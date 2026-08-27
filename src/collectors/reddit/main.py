"""
src/collectors/reddit/main.py

Cloud Run — Conector Reddit para TrendAnalyzer
================================================
Endpoint:  POST /ingest
           Body (opcional): {"subreddits": ["colombia", "bogota"]}

Flujo:
  Reddit OAuth (client_credentials) → GET /r/{subreddit}/new
  → mapeo al esquema común → Pub/Sub topic (raw-events)
                           → GCS RAW_BUCKET_NAME/reddit/YYYY/MM/DD/

Variables de entorno inyectadas por la infra (cloud-run.tf):
  REDDIT_CLIENT_ID_SECRET      — Client ID de la app Reddit (tipo "script")
  REDDIT_CLIENT_SECRET_SECRET  — Client secret
  REDDIT_USER_AGENT_SECRET     — User-Agent (ej. "trend-analyzer/1.0 (EAFIT)")
  PUBSUB_TOPIC                 — Nombre del topic Pub/Sub
  RAW_BUCKET_NAME              — Nombre del bucket GCS
  GCP_PROJECT_ID               — ID del proyecto GCP
  SOURCE_NAME                  — "reddit"

NOTA IMPORTANTE — Reddit cerró el endpoint .json sin autenticación en mayo 2026
(confirmado en tanteo_fuentes.py:90). Se requiere OAuth client_credentials.
El prototipo de _obtener_token_reddit() viene de tanteo_fuentes.py:63-85.

⚠️ Plan B: si la aprobación OAuth no llega, activar la rama Pushshift
   (ver PlanB al final del archivo) y documentarlo en la matriz de riesgos.
"""

import json
import logging
import os
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1, storage

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Credenciales — inyectadas por la infra desde Secret Manager
CLIENT_ID     = os.environ["REDDIT_CLIENT_ID_SECRET"]
CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET_SECRET"]
USER_AGENT    = os.environ["REDDIT_USER_AGENT_SECRET"]

PUBSUB_TOPIC  = os.environ["PUBSUB_TOPIC"]
RAW_BUCKET    = os.environ["RAW_BUCKET_NAME"]
GCP_PROJECT   = os.environ["GCP_PROJECT_ID"]
SOURCE_NAME   = os.environ.get("SOURCE_NAME", "reddit")

REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_BASE  = "https://oauth.reddit.com"
PAGE_SIZE = 25       # Reddit devuelve max 100; usamos 25 para no saturar el rate limit
RATE_LIMIT_HEADER = "X-Ratelimit-Remaining"

# Subreddits por defecto — relevantes para bebidas / consumo masivo en Colombia
DEFAULT_SUBREDDITS = [
    "colombia",
    "bogota",
    "medellin",
    "cafe",           # r/cafe — café de especialidad
    "Colombia_news",
]

app = Flask(__name__)

_publisher  = None
_gcs_client = None
_reddit_token: dict = {}   # {"access_token": str, "expires_at": float}


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
# OAuth — prototipo de tanteo_fuentes.py adaptado a producción
# ---------------------------------------------------------------------------
def _obtener_token_reddit() -> str:
    """Pide un token OAuth de solo lectura vía client_credentials.

    Cachea el token en memoria hasta que expire (Cloud Run reutiliza
    la instancia entre llamadas del Scheduler, ~15 min en dev).
    """
    import time
    global _reddit_token

    now = time.time()
    if _reddit_token.get("access_token") and _reddit_token.get("expires_at", 0) > now + 60:
        return _reddit_token["access_token"]

    log.info("Solicitando nuevo token OAuth a Reddit...")
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    resp = requests.post(
        REDDIT_TOKEN_URL,
        auth=auth,
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": USER_AGENT},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    _reddit_token = {
        "access_token": data["access_token"],
        "expires_at":   now + data.get("expires_in", 3600),
    }
    log.info("Token Reddit obtenido (expira en %ds)", data.get("expires_in", 3600))
    return _reddit_token["access_token"]


# ---------------------------------------------------------------------------
# Fetch posts con rate-limit awareness
# ---------------------------------------------------------------------------
def fetch_posts(subreddit: str, token: str, limit: int = PAGE_SIZE) -> list[dict]:
    """Trae los posts más recientes de un subreddit vía OAuth.

    Respeta el rate limit leyendo el header X-Ratelimit-Remaining.
    Si queda < 5 requests disponibles, loguea una advertencia.
    """
    url = f"{REDDIT_API_BASE}/r/{subreddit}/new"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }
    params = {"limit": limit}

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    remaining = resp.headers.get(RATE_LIMIT_HEADER)
    if remaining and float(remaining) < 5:
        log.warning("Rate limit Reddit bajo: %.0f requests restantes", float(remaining))

    data = resp.json()
    return [child["data"] for child in data["data"]["children"]]


# ---------------------------------------------------------------------------
# Mapeo al esquema común
# ---------------------------------------------------------------------------
def map_post(post: dict) -> dict:
    """Mapea un post crudo de Reddit al esquema común de ingesta.

    Campos de origen: child.data (tal como lo devuelve Reddit API y
    como lo procesa tanteo_fuentes.py:103).
    """
    # created_utc es un timestamp UNIX en UTC
    created_utc = post.get("created_utc")
    created_at = None
    if created_utc:
        created_at = datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()

    title    = post.get("title", "") or ""
    selftext = post.get("selftext", "") or ""
    text     = f"{title} {selftext}".strip()

    return {
        "source":      SOURCE_NAME,
        "id":          post.get("name"),         # fullname: t3_abc123
        "created_at":  created_at,
        "text":        text,
        "url":         post.get("url"),
        "language":    None,                     # Reddit no lo expone en tier gratuito
        "tags":        [post.get("subreddit")],  # subreddit como único tag
        "author_name": post.get("author"),
        "engagement": {
            "replies": post.get("num_comments", 0),
            "reposts": 0,                        # Reddit no tiene reposts directos
            "likes":   post.get("score", 0),
        },
        "raw_metadata": {
            "subreddit":      post.get("subreddit"),
            "subreddit_id":   post.get("subreddit_id"),
            "permalink":      post.get("permalink"),
            "is_self":        post.get("is_self"),
            "upvote_ratio":   post.get("upvote_ratio"),
            "over_18":        post.get("over_18"),
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


def write_raw_to_gcs(raw_posts: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S")
    blob_name = f"{SOURCE_NAME}/{date_path}/raw_{timestamp}.json"

    bucket = get_gcs().bucket(RAW_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(raw_posts, ensure_ascii=False, indent=2),
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
    subreddits = body.get("subreddits", DEFAULT_SUBREDDITS)

    log.info("Iniciando ingesta Reddit — subreddits: %s", subreddits)

    try:
        token = _obtener_token_reddit()
    except requests.HTTPError as exc:
        log.error("Error obteniendo token Reddit OAuth: %s", exc)
        return jsonify({"error": "reddit_auth_failed", "detail": str(exc)}), 502

    seen_ids: set[str] = set()
    raw_posts: list[dict] = []
    mapped_posts: list[dict] = []

    for subreddit in subreddits:
        try:
            posts = fetch_posts(subreddit, token)
            log.info("r/%s → %d posts", subreddit, len(posts))
            for post in posts:
                post_id = post.get("name", "")
                if post_id in seen_ids:
                    continue
                seen_ids.add(post_id)
                raw_posts.append(post)
                mapped_posts.append(map_post(post))
        except requests.HTTPError as exc:
            log.error("Error consultando r/%s: %s", subreddit, exc)

    if not mapped_posts:
        return jsonify({"published": 0, "subreddits": subreddits}), 200

    gcs_uri = write_raw_to_gcs(raw_posts)

    published = 0
    errors = 0
    for mapped in mapped_posts:
        try:
            publish_event(mapped)
            published += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Error publicando post %s: %s", mapped.get("id"), exc)
            errors += 1

    log.info("Ingesta completada: %d publicados, %d errores", published, errors)
    return jsonify({
        "source":      SOURCE_NAME,
        "subreddits":  subreddits,
        "fetched":     len(mapped_posts),
        "published":   published,
        "errors":      errors,
        "raw_gcs":     gcs_uri,
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "source": SOURCE_NAME}), 200


# ---------------------------------------------------------------------------
# ⚠️ PLAN B — Pushshift (activar si el OAuth de Reddit no es aprobado)
# ---------------------------------------------------------------------------
# Si el acceso OAuth de Reddit no llega a tiempo para el Sprint 1:
#
# 1. Descargar el dump de Pushshift (archivo tipo NDJSON comprimido) desde
#    el mirror académico acordado con el equipo (ver FUENTES.md).
# 2. Implementar un endpoint alternativo POST /ingest-pushshift que:
#    - Lea el archivo desde GCS (ya subido manualmente al bucket)
#    - Itere línea a línea con ijson para no cargar todo en memoria
#    - Aplique el mismo map_post() o un map_pushshift() compatible
#    - Publique al mismo topic Pub/Sub y escriba raw en GCS
# 3. Documentar la decisión en la matriz de riesgos del proyecto.
# 4. Actualizar la variable subreddit_source en Cloud SQL (checkpoints)
#    para que el pipeline downstream sepa que esta fuente es batch.
#
# Esquema Pushshift difiere en algunos campos:
#   created_utc → igual
#   name        → "t3_" + id
#   score       → puede estar desactualizado
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
