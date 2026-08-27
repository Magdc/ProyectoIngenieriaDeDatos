"""
tanteo_fuentes.py

Script exploratorio (NO productivo) para traer 1-2 registros de cada
fuente propuesta en Trend Analyzer y ver su estructura real de datos:

  1. Mastodon  -> timeline público de una instancia (con token de app)
  2. Reddit    -> vía OAuth (client credentials, script app)
  3. RSS       -> feed de un medio de noticias (sin API key)

Uso:
    pip install requests feedparser

    # Nunca hardcodees credenciales en el archivo. Expórtalas como
    # variables de entorno antes de correr el script:
    export MASTODON_TOKEN="tu_token_regenerado"
    export REDDIT_CLIENT_ID="tu_client_id"
    export REDDIT_CLIENT_SECRET="tu_client_secret"

    python tanteo_fuentes.py

Nota: esto es solo para inspección manual (imprime el JSON crudo de
1-2 elementos). Si vas a usarlo en producción, además de lo anterior
hay que manejar rate limits, paginación, reintentos y rotación de
tokens como corresponde.
"""

import json
import os

import feedparser
import requests

MASTODON_TOKEN = "CVg9FgkxIoHOvDVMIfMq5b_OAMU9goNKu721_VzBCgg"
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")

REDDIT_USER_AGENT = "trend-analyzer-tanteo/0.2 (universidad EAFIT, uso academico)"


def tantear_mastodon(instancia="https://mastodon.social", limite=2):
    """Trae 'limite' toots del timeline público de una instancia Mastodon.

    mastodon.social exige un usuario autenticado en este endpoint, así
    que se necesita un access token (Preferencias > Desarrollo > tu app).
    """
    if not MASTODON_TOKEN:
        raise RuntimeError(
            "Falta MASTODON_TOKEN. Exporta la variable de entorno antes de correr el script."
        )

    url = f"{instancia}/api/v1/timelines/tag/colombia"
    params = {"limit": limite, "local": "false"}
    headers = {"Authorization": f"Bearer {MASTODON_TOKEN}"}

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    print("Status code:", resp.status_code)
    print("Respuesta cruda (primeros 500 chars):", resp.text[:500])
    resp.raise_for_status()
    return resp.json()


def _obtener_token_reddit():
    """Pide un token OAuth de solo lectura vía client_credentials.
    Requiere una app tipo 'script' registrada en reddit.com/prefs/apps.
    """
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
        raise RuntimeError(
            "Faltan REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET. "
            "Regístralos como variables de entorno."
        )

    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": REDDIT_USER_AGENT}

    resp = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def tantear_reddit(subreddit="colombia", limite=2):
    """Trae 'limite' posts recientes de un subreddit vía OAuth
    (Reddit cerró el endpoint .json sin autenticación en mayo de 2026).
    """
    token = _obtener_token_reddit()
    url = f"https://oauth.reddit.com/r/{subreddit}/new"
    params = {"limit": limite}
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": REDDIT_USER_AGENT,
    }

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return [child["data"] for child in data["data"]["children"]]


def tantear_rss(feed_url="https://www.eltiempo.com/rss/economia.xml", limite=2):
    """Trae 'limite' entradas de un feed RSS (sin necesidad de API key)."""
    feed = feedparser.parse(feed_url)
    entradas = feed.entries[:limite]
    return [dict(e) for e in entradas]


def imprimir(titulo, registros):
    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)
    for i, r in enumerate(registros, 1):
        print(f"\n--- Registro {i} ---")
        print(json.dumps(r, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    try:
        toots = tantear_mastodon()
        imprimir("MASTODON - timeline público", toots)
    except Exception as e:
        print(f"[Mastodon] Error: {e}")

    try:
        posts = tantear_reddit()
        imprimir("REDDIT - posts recientes (OAuth)", posts)
    except Exception as e:
        print(f"[Reddit] Error: {e}")

    try:
        noticias = tantear_rss()
        imprimir("RSS - noticias", noticias)
    except Exception as e:
        print(f"[RSS] Error: {e}")