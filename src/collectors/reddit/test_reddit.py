"""
tests/collectors/test_reddit.py

Pruebas unitarias del conector Reddit.
Cubre: mapeo al esquema común, conversión de created_utc → ISO8601 UTC,
       deduplicación por fullname, casos borde (sin selftext, autor deleted).
No requiere credenciales reales ni conexión a GCP.
"""

import sys
import os
from datetime import timezone, datetime

os.environ.setdefault("REDDIT_CLIENT_ID_SECRET", "test-id")
os.environ.setdefault("REDDIT_CLIENT_SECRET_SECRET", "test-secret")
os.environ.setdefault("REDDIT_USER_AGENT_SECRET", "test-agent/1.0")
os.environ.setdefault("PUBSUB_TOPIC", "test-topic")
os.environ.setdefault("RAW_BUCKET_NAME", "test-bucket")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("SOURCE_NAME", "reddit")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "collectors", "reddit"))

import main  # noqa: E402


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def make_post(overrides: dict | None = None) -> dict:
    """Simula un child.data de la Reddit API (estructura real confirmada en
    tanteo_fuentes.py:103: [child['data'] for child in data['data']['children']]).
    """
    post = {
        "name":          "t3_abc123",    # fullname — clave de dedup
        "id":            "abc123",
        "title":         "Nuevo café colombiano conquista mercados",
        "selftext":      "Detalle del artículo sobre el café.",
        "author":        "usuario_test",
        "subreddit":     "colombia",
        "subreddit_id":  "t5_2qh61",
        "url":           "https://www.reddit.com/r/colombia/comments/abc123/",
        "permalink":     "/r/colombia/comments/abc123/nuevo_cafe/",
        "created_utc":   1724589600.0,   # 2026-08-25 18:00:00 UTC
        "score":         42,
        "num_comments":  7,
        "upvote_ratio":  0.91,
        "is_self":       True,
        "over_18":       False,
    }
    if overrides:
        post.update(overrides)
    return post


# ---------------------------------------------------------------------------
# Tests: map_post
# ---------------------------------------------------------------------------
class TestMapPost:
    def test_source_es_reddit(self):
        assert main.map_post(make_post())["source"] == "reddit"

    def test_id_es_fullname(self):
        assert main.map_post(make_post())["id"] == "t3_abc123"

    def test_created_at_es_utc_iso8601(self):
        mapped = main.map_post(make_post())
        # 1724589600 → 2026-08-25T18:00:00+00:00
        assert "2026-08-25" in mapped["created_at"]
        assert "+00:00" in mapped["created_at"] or "Z" in mapped["created_at"]

    def test_text_concatena_title_y_selftext(self):
        mapped = main.map_post(make_post())
        assert "Nuevo café colombiano" in mapped["text"]
        assert "Detalle del artículo" in mapped["text"]

    def test_text_sin_selftext_usa_solo_titulo(self):
        post = make_post({"selftext": ""})
        mapped = main.map_post(post)
        assert "Nuevo café colombiano" in mapped["text"]

    def test_tags_contiene_subreddit(self):
        assert main.map_post(make_post())["tags"] == ["colombia"]

    def test_language_es_none(self):
        assert main.map_post(make_post())["language"] is None

    def test_engagement_correcto(self):
        mapped = main.map_post(make_post())
        assert mapped["engagement"]["replies"] == 7
        assert mapped["engagement"]["reposts"] == 0   # Reddit no tiene reposts
        assert mapped["engagement"]["likes"] == 42

    def test_raw_metadata_presente(self):
        mapped = main.map_post(make_post())
        assert mapped["raw_metadata"]["subreddit"] == "colombia"
        assert mapped["raw_metadata"]["upvote_ratio"] == 0.91

    def test_campos_obligatorios_presentes(self):
        mapped = main.map_post(make_post())
        required = ["source", "id", "created_at", "text", "url",
                    "language", "tags", "engagement", "raw_metadata"]
        for field in required:
            assert field in mapped, f"Campo faltante: {field}"

    def test_author_deleted_no_falla(self):
        post = make_post({"author": "[deleted]"})
        mapped = main.map_post(post)
        assert mapped["author_name"] == "[deleted]"

    def test_sin_created_utc_retorna_none(self):
        post = make_post({"created_utc": None})
        mapped = main.map_post(post)
        assert mapped["created_at"] is None

    def test_created_at_timestamp_correcto(self):
        """Verificación precisa: 1724589600 UTC debe ser 2026-08-25T18:00:00."""
        mapped = main.map_post(make_post({"created_utc": 1724589600.0}))
        dt = datetime.fromisoformat(mapped["created_at"])
        assert dt.year == 2026
        assert dt.month == 8
        assert dt.day == 25
        assert dt.hour == 18
        assert dt.tzinfo is not None
