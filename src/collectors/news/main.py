"""
src/collectors/news/main.py

Cloud Run — Conector RSS / News para TrendAnalyzer
====================================================
Endpoint:  POST /ingest
           Body (opcional): {"feeds": ["url1", "url2"]}   ← sobreescribe la
                            lista por defecto si se pasa en el body.

Flujo:
  Lista de feeds RSS → feedparser → mapeo al esquema común
                     → Pub/Sub topic (raw-events)
                     → GCS  RAW_BUCKET_NAME/news/YYYY/MM/DD/

Variables de entorno inyectadas por la infra (cloud-run.tf):
  NEWS_API_KEY_SECRET   — Clave de News API (disponible aunque se use RSS puro)
  PUBSUB_TOPIC          — Nombre del topic Pub/Sub
  RAW_BUCKET_NAME       — Nombre del bucket GCS
  GCP_PROJECT_ID        — ID del proyecto GCP
  SOURCE_NAME           — "news"

Hallazgos del tanteo (output.txt):
  - published viene en "-05:00" (Colombia) → hay que convertir a UTC
  - summary puede contener HTML → aplicar strip_html igual que en Mastodon
  - tags[].term es el campo correcto de feedparser
  - id = URL completa → clave de deduplicación
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import feedparser
import requests
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1, storage

try:
    from bs4 import BeautifulSoup
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Inyectados por la infra
NEWS_API_KEY = os.environ.get("NEWS_API_KEY_SECRET", "")   # opcional en RSS puro
PUBSUB_TOPIC = os.environ["PUBSUB_TOPIC"]
RAW_BUCKET   = os.environ["RAW_BUCKET_NAME"]
GCP_PROJECT  = os.environ["GCP_PROJECT_ID"]
SOURCE_NAME  = os.environ.get("SOURCE_NAME", "news")

# Zona horaria de Colombia (fuentes RSS colombianas)
TZ_COLOMBIA = ZoneInfo("America/Bogota")

# ---------------------------------------------------------------------------
# Feeds RSS colombianos relevantes (sector bebidas / consumo masivo)
# Confirmado en output.txt que El Tiempo funciona con feedparser
# ---------------------------------------------------------------------------
DEFAULT_FEEDS: list[str] = [
    # Economía / sectores — relevante para marcas y consumo
    "https://www.eltiempo.com/rss/economia.xml",
    "https://www.elcolombiano.com/rss/economia.xml",
    "https://www.portafolio.co/rss/portafolio.xml",
    # Negocios y empresas
    "https://www.elespectador.com/rss/economia/",
    # Agroindustria / bebidas (sector café, jugos, lácteos)
    "https://www.agronegocios.co/feed/",
]

app = Flask(__name__)

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
# Utilidades
# ---------------------------------------------------------------------------
def strip_html(raw: str | None) -> str:
    """Convierte HTML → texto plano (mismo helper que Mastodon)."""
    if not raw:
        return ""
    if _BS4_AVAILABLE:
        return BeautifulSoup(raw, "html.parser").get_text(separator=" ").strip()
    return re.sub(r"<[^>]+>", " ", raw).strip()


def normalize_date(entry: feedparser.FeedParserDict) -> str | None:
    """Convierte published a ISO8601 UTC.

    feedparser expone published_parsed (struct_time UTC) y published (string).
    Si published_parsed está disponible es la vía más segura.
    Si no, intenta parsear published como RFC2822 y lo convierte a UTC.
    """
    if entry.get("published_parsed"):
        # struct_time en UTC — feedparser ya normaliza a UTC internamente
        dt_utc = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return dt_utc.isoformat()

    raw_date: str | None = entry.get("published")
    if not raw_date:
        return None

    try:
        # RFC2822 (ej. "Mon, 25 Aug 2026 05:30:00 -0500")
        dt = parsedate_to_datetime(raw_date)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        log.warning("No se pudo parsear la fecha: %s", raw_date)
        return raw_date


def make_dedup_key(entry_id: str) -> str:
    """Retorna un hash corto del id (URL) para usar como clave de dedup."""
    return hashlib.sha256(entry_id.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Mapeo al esquema común
# ---------------------------------------------------------------------------
def map_entry(entry: feedparser.FeedParserDict, feed_url: str) -> dict:
    """Mapea una entrada feedparser al esquema común de ingesta."""
    title   = strip_html(entry.get("title", ""))
    summary = strip_html(entry.get("summary", ""))
    text    = f"{title} {summary}".strip()

    tags = [t.get("term", "") for t in (entry.get("tags") or []) if t.get("term")]

    entry_id = entry.get("id") or entry.get("link", "")

    return {
        "source":      SOURCE_NAME,
        "id":          entry_id,
        "created_at":  normalize_date(entry),
        "text":        text,
        "url":         entry.get("link"),
        "language":    None,   # RSS no siempre lo expone; inferir en capa Trusted
        "tags":        tags,
        "author_name": entry.get("author") or None,
        "engagement":  {
            "replies": None,   # RSS no tiene métricas de engagement
            "reposts": None,
            "likes":   None,
        },
        "raw_metadata": {
            "feed_url":    feed_url,
            "dedup_key":   make_dedup_key(entry_id),
            "slash_comments": entry.get("slash_comments"),
        },
    }


# ---------------------------------------------------------------------------
# Pub/Sub + GCS
# ---------------------------------------------------------------------------
def publish_event(mapped: dict) -> None:
    publisher = get_publisher()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
    data = json.dumps(mapped, ensure_ascii=False).encode("utf-8")
    publisher.publish(topic_path, data=data, source=SOURCE_NAME).result()


def write_raw_to_gcs(raw_entries: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S")
    blob_name = f"{SOURCE_NAME}/{date_path}/raw_{timestamp}.json"

    bucket = get_gcs().bucket(RAW_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(raw_entries, ensure_ascii=False, indent=2),
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
    feeds = body.get("feeds", DEFAULT_FEEDS)

    log.info("Iniciando ingesta RSS/News — %d feeds", len(feeds))

    seen_ids: set[str] = set()   # deduplicación dentro de la ejecución
    raw_entries: list[dict] = []
    mapped_entries: list[dict] = []

    for feed_url in feeds:
        try:
            feed = feedparser.parse(feed_url)
            log.info("Feed %s → %d entradas", feed_url, len(feed.entries))
            for entry in feed.entries:
                entry_id = entry.get("id") or entry.get("link", "")
                if entry_id in seen_ids:
                    continue
                seen_ids.add(entry_id)
                raw_entries.append(dict(entry))
                mapped_entries.append(map_entry(entry, feed_url))
        except Exception as exc:  # noqa: BLE001
            log.error("Error procesando feed %s: %s", feed_url, exc)

    if not mapped_entries:
        return jsonify({"published": 0, "feeds": len(feeds)}), 200

    gcs_uri = write_raw_to_gcs(raw_entries)

    published = 0
    errors = 0
    for mapped in mapped_entries:
        try:
            publish_event(mapped)
            published += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Error publicando entrada %s: %s", mapped.get("id"), exc)
            errors += 1

    log.info("Ingesta completada: %d publicados, %d errores", published, errors)
    return jsonify({
        "source":    SOURCE_NAME,
        "feeds":     len(feeds),
        "fetched":   len(mapped_entries),
        "published": published,
        "errors":    errors,
        "raw_gcs":   gcs_uri,
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "source": SOURCE_NAME}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
