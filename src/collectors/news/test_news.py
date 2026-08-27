"""
tests/collectors/test_news.py

Pruebas unitarias del conector RSS / News.
Cubre: normalización de fecha, strip_html, mapeo al esquema común, deduplicación.
No requiere credenciales reales ni conexión a GCP ni a internet.
"""

import sys
import os
from datetime import timezone
from unittest.mock import patch

os.environ.setdefault("NEWS_API_KEY_SECRET", "test-key")
os.environ.setdefault("PUBSUB_TOPIC", "test-topic")
os.environ.setdefault("RAW_BUCKET_NAME", "test-bucket")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("SOURCE_NAME", "news")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "collectors", "news"))

import main  # noqa: E402


# ---------------------------------------------------------------------------
# Helper: simular una entrada feedparser
# ---------------------------------------------------------------------------
def make_entry(overrides: dict | None = None):
    """Devuelve un objeto feedparser-like (dict con atributos del tanteo real)."""
    import types
    base = {
        "title": "Drummond renuncia a su regasificadora",
        "title_detail": {"type": "text/plain"},
        "summary": "<p>Detalles de la empresa en Colombia.</p>",
        "summary_detail": {"type": "text/html"},
        "published": "2026-08-25T05:30:00-05:00",
        "published_parsed": None,   # se testea con y sin este campo
        "link": "https://www.eltiempo.com/economia/sectores/nota-3580745",
        "id":   "https://www.eltiempo.com/economia/sectores/nota-3580745",
        "tags": [{"term": "Sectores", "scheme": None, "label": None}],
        "author": None,
        "slash_comments": "0",
    }
    if overrides:
        base.update(overrides)
    # feedparser devuelve FeedParserDict; para tests usamos un SimpleNamespace
    # con .get() compatible
    class FakeEntry(dict):
        pass
    entry = FakeEntry(base)
    return entry


# ---------------------------------------------------------------------------
# Tests: strip_html
# ---------------------------------------------------------------------------
class TestStripHtml:
    def test_elimina_parrafos(self):
        assert main.strip_html("<p>Hola</p>") == "Hola"

    def test_vacio(self):
        assert main.strip_html("") == ""

    def test_none(self):
        assert main.strip_html(None) == ""


# ---------------------------------------------------------------------------
# Tests: normalize_date
# ---------------------------------------------------------------------------
class TestNormalizeDate:
    def test_convierte_offset_colombia_a_utc(self):
        """2026-08-25T05:30:00-05:00 debe quedar como 2026-08-25T10:30:00+00:00"""
        entry = make_entry({"published_parsed": None,
                            "published": "2026-08-25T05:30:00-05:00"})
        result = main.normalize_date(entry)
        assert result is not None
        assert "10:30:00" in result or "10:30" in result

    def test_published_parsed_tiene_prioridad(self):
        """published_parsed (struct_time UTC de feedparser) tiene prioridad."""
        import time
        # struct_time correspondiente a 2026-08-25 10:30:00 UTC
        st = time.strptime("2026-08-25 10:30:00", "%Y-%m-%d %H:%M:%S")
        entry = make_entry({"published_parsed": st,
                            "published": "2026-08-25T05:30:00-05:00"})
        result = main.normalize_date(entry)
        assert "2026-08-25" in result
        assert "10:30" in result

    def test_sin_fecha_retorna_none(self):
        entry = make_entry({"published": None, "published_parsed": None})
        assert main.normalize_date(entry) is None


# ---------------------------------------------------------------------------
# Tests: map_entry
# ---------------------------------------------------------------------------
class TestMapEntry:
    def test_source_es_news(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert mapped["source"] == "news"

    def test_text_concatena_titulo_y_resumen(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert "Drummond" in mapped["text"]
        assert "Colombia" in mapped["text"]

    def test_text_sin_html(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert "<" not in mapped["text"]

    def test_tags_es_lista(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert mapped["tags"] == ["Sectores"]

    def test_engagement_es_null(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert mapped["engagement"]["replies"] is None
        assert mapped["engagement"]["likes"] is None

    def test_language_es_none(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert mapped["language"] is None

    def test_raw_metadata_contiene_feed_url(self):
        url = "https://www.eltiempo.com/rss/economia.xml"
        mapped = main.map_entry(make_entry(), url)
        assert mapped["raw_metadata"]["feed_url"] == url

    def test_dedup_key_presente_y_es_string(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        assert isinstance(mapped["raw_metadata"]["dedup_key"], str)
        assert len(mapped["raw_metadata"]["dedup_key"]) == 16

    def test_campos_obligatorios_presentes(self):
        mapped = main.map_entry(make_entry(), "https://feed.url/rss")
        required = ["source", "id", "created_at", "text", "url",
                    "language", "tags", "engagement", "raw_metadata"]
        for field in required:
            assert field in mapped, f"Campo faltante: {field}"


# ---------------------------------------------------------------------------
# Tests: make_dedup_key (idempotencia)
# ---------------------------------------------------------------------------
class TestDedupKey:
    def test_misma_url_mismo_hash(self):
        url = "https://www.eltiempo.com/economia/sectores/nota-3580745"
        assert main.make_dedup_key(url) == main.make_dedup_key(url)

    def test_distintas_urls_distintos_hashes(self):
        assert main.make_dedup_key("https://a.com") != main.make_dedup_key("https://b.com")
