"""
src/collectors/mastodon/main.py

Cloud Run — Conector Mastodon para TrendAnalyzer
=================================================
Endpoint:  POST /ingest
           Body (opcional): {"hashtag": "colombia"}

Flujo:
  Mastodon API → mapeo a esquema común → Pub/Sub topic (raw-events)
                                       → GCS  RAW_BUCKET_NAME/mastodon/YYYY/MM/DD/

Variables de entorno inyectadas por la infra (cloud-run.tf):
  MASTODON_ACCESS_TOKEN_SECRET  — ID del secreto en Secret Manager (la infra
                                  inyecta el *valor* del secreto directamente
                                  como variable de entorno plain-text en el
                                  contenedor; el nombre de la var es el key del
                                  secreto, igual que lo hace locals.tf).
  PUBSUB_TOPIC                  — Nombre del topic Pub/Sub (no el ID completo)
  RAW_BUCKET_NAME               — Nombre del bucket GCS
  GCP_PROJECT_ID                — ID del proyecto GCP
  SOURCE_NAME                   — "mastodon" (inyectado por cloud-run.tf)
"""

import json
import logging
import os
import re
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from google.cloud import pubsub_v1, storage

# ---------------------------------------------------------------------------
# Dependencias opcionales — BeautifulSoup para limpiar HTML
# ---------------------------------------------------------------------------
try:
    from bs4 import BeautifulSoup
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

MASTODON_BASE = "https://mastodon.social"
MASTODON_RATE_LIMIT = 300          # req / 5 min por token
DEFAULT_HASHTAG = "colombia"
PAGE_SIZE = 40                     # max que acepta la API

# Inyectados por la infra
ACCESS_TOKEN   = os.environ["MASTODON_ACCESS_TOKEN_SECRET"]
PUBSUB_TOPIC   = os.environ["PUBSUB_TOPIC"]
RAW_BUCKET     = os.environ["RAW_BUCKET_NAME"]
GCP_PROJECT    = os.environ["GCP_PROJECT_ID"]
SOURCE_NAME    = os.environ.get("SOURCE_NAME", "mastodon")

app = Flask(__name__)

# Clientes GCP (se inicializan una sola vez — Cloud Run reutiliza la instancia)
_publisher = None
_gcs_client = None


def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


def get_gcs() -> storage.Client:
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client(project=GCP_PROJECT)
    return _gcs_client


# ---------------------------------------------------------------------------
# Limpieza de HTML
# ---------------------------------------------------------------------------
def strip_html(raw: str) -> str:
    """Convierte HTML → texto plano.

    Usa BeautifulSoup si está disponible; si no, regex básico.
    """
    if not raw:
        return ""
    if _BS4_AVAILABLE:
        return BeautifulSoup(raw, "html.parser").get_text(separator=" ").strip()
    return re.sub(r"<[^>]+>", " ", raw).strip()


# ---------------------------------------------------------------------------
# Mapeo al esquema común
# ---------------------------------------------------------------------------
def map_toot(toot: dict) -> dict:
    """Mapea un toot crudo de Mastodon al esquema común de ingesta."""
    account = toot.get("account") or {}
    engagement = {
        "replies": toot.get("replies_count", 0),
        "reposts":  toot.get("reblogs_count", 0),
        "likes":    toot.get("favourites_count", 0),
    }
    tags = [t["name"] for t in (toot.get("tags") or [])]

    return {
        "source":          SOURCE_NAME,
        "id":              toot["id"],
        "created_at":      toot.get("created_at"),          # ya ISO8601 UTC
        "text":            strip_html(toot.get("content", "")),
        "url":             toot.get("url"),
        "language":        toot.get("language"),
        "tags":            tags,
        "author_id":       account.get("id"),
        "author_name":     account.get("username"),
        "author_followers": account.get("followers_count"),
        "engagement":      engagement,
        # Campos extra específicos de Mastodon (no forman parte del esquema común)
        "raw_metadata": {
            "visibility":  toot.get("visibility"),
            "sensitive":   toot.get("sensitive"),
            "spoiler_text": toot.get("spoiler_text"),
            "quotes_count": toot.get("quotes_count"),
        },
    }


# ---------------------------------------------------------------------------
# Polling con paginación
# ---------------------------------------------------------------------------
def fetch_toots(hashtag: str, max_pages: int = 5) -> list[dict]:
    """Pagina el endpoint de timeline por hashtag.

    Maneja paginación via parámetro max_id (el header Link: rel=next también
    lo expone pero es más fácil leer el id del último toot).
    """
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = f"{MASTODON_BASE}/api/v1/timelines/tag/{hashtag}"
    params: dict = {"limit": PAGE_SIZE, "local": "false"}
    toots: list[dict] = []

    for page_num in range(max_pages):
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        toots.extend(page)
        log.info("Página %d: %d toots (acumulado %d)", page_num + 1, len(page), len(toots))
        # Paginación: el id más pequeño de la página es el cursor
        params["max_id"] = page[-1]["id"]

    return toots


# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------
def publish_event(mapped: dict) -> None:
    """Publica un evento JSON al topic Pub/Sub configurado."""
    publisher = get_publisher()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
    data = json.dumps(mapped, ensure_ascii=False).encode("utf-8")
    future = publisher.publish(topic_path, data=data, source=SOURCE_NAME)
    future.result()   # bloquea hasta confirmación del broker


# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------
def write_raw_to_gcs(toots: list[dict]) -> str:
    """Escribe el JSON crudo al bucket GCS bajo mastodon/YYYY/MM/DD/."""
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S")
    blob_name = f"{SOURCE_NAME}/{date_path}/raw_{timestamp}.json"

    bucket = get_gcs().bucket(RAW_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(toots, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8",
    )
    gcs_uri = f"gs://{RAW_BUCKET}/{blob_name}"
    log.info("Raw escrito en GCS: %s", gcs_uri)
    return gcs_uri


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------
@app.route("/ingest", methods=["POST"])
def ingest():
    body = request.get_json(silent=True) or {}
    hashtag = body.get("hashtag", DEFAULT_HASHTAG)

    log.info("Iniciando ingesta Mastodon — hashtag: #%s", hashtag)

    try:
        raw_toots = fetch_toots(hashtag)
    except requests.HTTPError as exc:
        log.error("Error consultando Mastodon API: %s", exc)
        return jsonify({"error": str(exc)}), 502

    if not raw_toots:
        log.info("Sin toots nuevos para #%s", hashtag)
        return jsonify({"published": 0, "hashtag": hashtag}), 200

    # Guardar raw antes de procesar (raw = datos sin transformar)
    gcs_uri = write_raw_to_gcs(raw_toots)

    published = 0
    errors = 0
    for toot in raw_toots:
        try:
            mapped = map_toot(toot)
            publish_event(mapped)
            published += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Error publicando toot %s: %s", toot.get("id"), exc)
            errors += 1

    log.info("Ingesta completada: %d publicados, %d errores", published, errors)
    return jsonify({
        "source":    SOURCE_NAME,
        "hashtag":   hashtag,
        "fetched":   len(raw_toots),
        "published": published,
        "errors":    errors,
        "raw_gcs":   gcs_uri,
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "source": SOURCE_NAME}), 200


# ---------------------------------------------------------------------------
# Entrypoint local
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
