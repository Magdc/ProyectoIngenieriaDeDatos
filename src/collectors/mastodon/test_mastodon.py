"""
tests/collectors/test_mastodon.py

Pruebas unitarias del conector Mastodon.
Cubre: limpieza HTML, mapeo al esquema común, deduplicación de campos.
No requiere credenciales reales ni conexión a GCP.
"""

import sys
import os

# ---------------------------------------------------------------------------
# Stub de variables de entorno antes de importar el módulo
# ---------------------------------------------------------------------------
os.environ.setdefault("MASTODON_ACCESS_TOKEN_SECRET", "test-token")
os.environ.setdefault("PUBSUB_TOPIC", "test-topic")
os.environ.setdefault("RAW_BUCKET_NAME", "test-bucket")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("SOURCE_NAME", "mastodon")

# Añadir src al path para importar directamente
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "collectors", "mastodon"))

import main  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_toot(overrides: dict | None = None) -> dict:
    """Construye un toot de prueba con la estructura real de la API."""
    toot = {
        "id": "117156446756584402",
        "created_at": "2026-08-25T13:45:08.445Z",
        "visibility": "public",
        "sensitive": False,
        "spoiler_text": "",
        "language": "es",
        "url": "https://mastodon.social/@desdeabajo/117156446756584402",
        "content": "<p>Texto de prueba con <b>HTML</b> y &amp; entidades.</p>",
        "replies_count": 2,
        "reblogs_count": 5,
        "favourites_count": 10,
        "quotes_count": 0,
        "tags": [
            {"name": "colombia", "url": "https://mastodon.social/tags/colombia"},
            {"name": "cafe",     "url": "https://mastodon.social/tags/cafe"},
        ],
        "account": {
            "id": "98765",
            "username": "desdeabajo",
            "followers_count": 1234,
        },
    }
    if overrides:
        toot.update(overrides)
    return toot


# ---------------------------------------------------------------------------
# Tests: strip_html
# ---------------------------------------------------------------------------
class TestStripHtml:
    def test_elimina_etiquetas_simples(self):
        assert main.strip_html("<p>Hola mundo</p>") == "Hola mundo"

    def test_elimina_etiquetas_anidadas(self):
        result = main.strip_html("<p>Texto con <b>negrita</b> y <a href='x'>link</a>.</p>")
        assert "<" not in result
        assert "Texto con" in result

    def test_string_vacio(self):
        assert main.strip_html("") == ""

    def test_none_retorna_vacio(self):
        assert main.strip_html(None) == ""

    def test_sin_html_pasa_sin_cambios(self):
        assert main.strip_html("Texto plano sin etiquetas") == "Texto plano sin etiquetas"

    def test_contenido_unicode(self):
        resultado = main.strip_html("<p>Café y niño con ñoño carácter</p>")
        assert "Café" in resultado
        assert "ñoño" in resultado


# ---------------------------------------------------------------------------
# Tests: map_toot
# ---------------------------------------------------------------------------
class TestMapToot:
    def test_source_es_mastodon(self):
        mapped = main.map_toot(make_toot())
        assert mapped["source"] == "mastodon"

    def test_id_se_preserva(self):
        mapped = main.map_toot(make_toot())
        assert mapped["id"] == "117156446756584402"

    def test_text_no_tiene_html(self):
        mapped = main.map_toot(make_toot())
        assert "<" not in mapped["text"]
        assert "Texto de prueba" in mapped["text"]

    def test_tags_es_lista_de_strings(self):
        mapped = main.map_toot(make_toot())
        assert mapped["tags"] == ["colombia", "cafe"]

    def test_engagement_completo(self):
        mapped = main.map_toot(make_toot())
        assert mapped["engagement"]["replies"] == 2
        assert mapped["engagement"]["reposts"] == 5
        assert mapped["engagement"]["likes"] == 10

    def test_author_fields(self):
        mapped = main.map_toot(make_toot())
        assert mapped["author_id"] == "98765"
        assert mapped["author_name"] == "desdeabajo"
        assert mapped["author_followers"] == 1234

    def test_sin_tags_retorna_lista_vacia(self):
        toot = make_toot({"tags": []})
        assert main.map_toot(toot)["tags"] == []

    def test_sin_account_no_falla(self):
        toot = make_toot({"account": {}})
        mapped = main.map_toot(toot)
        assert mapped["author_id"] is None
        assert mapped["author_name"] is None

    def test_raw_metadata_presente(self):
        mapped = main.map_toot(make_toot())
        assert "raw_metadata" in mapped
        assert mapped["raw_metadata"]["visibility"] == "public"

    def test_campos_obligatorios_presentes(self):
        mapped = main.map_toot(make_toot())
        required = ["source", "id", "created_at", "text", "url", "language",
                    "tags", "engagement", "raw_metadata"]
        for field in required:
            assert field in mapped, f"Campo faltante: {field}"

    def test_created_at_es_iso8601(self):
        mapped = main.map_toot(make_toot())
        # Debe conservar el formato ISO8601 que devuelve la API
        assert "T" in mapped["created_at"]
        assert "Z" in mapped["created_at"]
