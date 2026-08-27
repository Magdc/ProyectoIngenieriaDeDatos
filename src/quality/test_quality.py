"""
src/quality/test_quality.py

Pruebas unitarias de las reglas de calidad (US-13).
Cubre cada regla con casos validos e invalidos.
No requiere GCP, pandas ni Great Expectations instalado.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from expectations import (
    rule_id, rule_source, rule_created_at, rule_text,
    rule_url, rule_engagement, rule_privacy, ALL_RULES,
)
from validator import validate_record, anonymize_record


# ---------------------------------------------------------------------------
# Fixture base — registro completamente valido
# ---------------------------------------------------------------------------
def valid_record(overrides: dict | None = None) -> dict:
    r = {
        "source": "mastodon",
        "id": "117156446756584402",
        "created_at": "2026-08-25T13:45:08.445Z",
        "text": "Cafe colombiano conquista mercados europeos",
        "url": "https://mastodon.social/@user/117156446756584402",
        "language": "es",
        "tags": ["colombia", "cafe"],
        "author_name": "usuario_test",
        "author_id": "98765",
        "engagement": {"replies": 2, "reposts": 5, "likes": 10},
        "raw_metadata": {},
    }
    if overrides:
        r.update(overrides)
    return r


# ---------------------------------------------------------------------------
# R1: id
# ---------------------------------------------------------------------------
class TestRuleId:
    def test_id_valido(self):
        ok, msg = rule_id(valid_record())
        assert ok and msg is None

    def test_id_nulo(self):
        ok, msg = rule_id(valid_record({"id": None}))
        assert not ok and "R1" in msg

    def test_id_vacio(self):
        ok, msg = rule_id(valid_record({"id": ""}))
        assert not ok and "R1" in msg

    def test_id_solo_espacios(self):
        ok, msg = rule_id(valid_record({"id": "   "}))
        assert not ok and "R1" in msg

    def test_id_numerico_como_string(self):
        ok, _ = rule_id(valid_record({"id": "0"}))
        assert ok


# ---------------------------------------------------------------------------
# R2: source
# ---------------------------------------------------------------------------
class TestRuleSource:
    def test_mastodon_valido(self):
        assert rule_source(valid_record({"source": "mastodon"}))[0]

    def test_reddit_valido(self):
        assert rule_source(valid_record({"source": "reddit"}))[0]

    def test_news_valido(self):
        assert rule_source(valid_record({"source": "news"}))[0]

    def test_twitter_invalido(self):
        ok, msg = rule_source(valid_record({"source": "twitter"}))
        assert not ok and "R2" in msg

    def test_none_invalido(self):
        ok, msg = rule_source(valid_record({"source": None}))
        assert not ok and "R2" in msg

    def test_mayusculas_invalido(self):
        # El esquema comun define minusculas
        ok, msg = rule_source(valid_record({"source": "Mastodon"}))
        assert not ok and "R2" in msg


# ---------------------------------------------------------------------------
# R3: created_at
# ---------------------------------------------------------------------------
class TestRuleCreatedAt:
    def test_iso8601_utc_con_z(self):
        assert rule_created_at(valid_record())[0]

    def test_iso8601_con_offset(self):
        r = valid_record({"created_at": "2026-08-25T08:45:08.445+00:00"})
        assert rule_created_at(r)[0]

    def test_fecha_nula(self):
        ok, msg = rule_created_at(valid_record({"created_at": None}))
        assert not ok and "R3" in msg

    def test_fecha_no_iso8601(self):
        ok, msg = rule_created_at(valid_record({"created_at": "25-08-2026"}))
        assert not ok and "R3" in msg

    def test_fecha_anterior_a_2020(self):
        ok, msg = rule_created_at(valid_record({"created_at": "2019-12-31T23:59:59Z"}))
        assert not ok and "R3" in msg

    def test_fecha_en_futuro(self):
        ok, msg = rule_created_at(valid_record({"created_at": "2099-01-01T00:00:00Z"}))
        assert not ok and "R3" in msg


# ---------------------------------------------------------------------------
# R4: text
# ---------------------------------------------------------------------------
class TestRuleText:
    def test_texto_valido(self):
        assert rule_text(valid_record())[0]

    def test_texto_nulo(self):
        ok, msg = rule_text(valid_record({"text": None}))
        assert not ok and "R4" in msg

    def test_texto_vacio(self):
        ok, msg = rule_text(valid_record({"text": ""}))
        assert not ok and "R4" in msg

    def test_texto_espacios(self):
        ok, msg = rule_text(valid_record({"text": "   "}))
        assert not ok and "R4" in msg

    def test_texto_un_caracter(self):
        ok, _ = rule_text(valid_record({"text": "a"}))
        assert ok


# ---------------------------------------------------------------------------
# R5: url
# ---------------------------------------------------------------------------
class TestRuleUrl:
    def test_url_https_valida(self):
        assert rule_url(valid_record())[0]

    def test_url_http_valida(self):
        r = valid_record({"url": "http://example.com/articulo"})
        assert rule_url(r)[0]

    def test_url_nula(self):
        ok, msg = rule_url(valid_record({"url": None}))
        assert not ok and "R5" in msg

    def test_url_sin_esquema(self):
        ok, msg = rule_url(valid_record({"url": "www.example.com"}))
        assert not ok and "R5" in msg

    def test_url_solo_texto(self):
        ok, msg = rule_url(valid_record({"url": "no-es-url"}))
        assert not ok and "R5" in msg


# ---------------------------------------------------------------------------
# R6: engagement
# ---------------------------------------------------------------------------
class TestRuleEngagement:
    def test_engagement_valido(self):
        assert rule_engagement(valid_record())[0]

    def test_engagement_con_nulos(self):
        # RSS no tiene engagement — todos null es valido
        r = valid_record({"engagement": {"replies": None, "reposts": None, "likes": None}})
        assert rule_engagement(r)[0]

    def test_engagement_negativo(self):
        r = valid_record({"engagement": {"replies": -1, "reposts": 0, "likes": 0}})
        ok, msg = rule_engagement(r)
        assert not ok and "R6" in msg

    def test_engagement_no_numerico(self):
        r = valid_record({"engagement": {"likes": "muchos"}})
        ok, msg = rule_engagement(r)
        assert not ok and "R6" in msg

    def test_engagement_ausente(self):
        r = valid_record({"engagement": {}})
        assert rule_engagement(r)[0]   # sin campos → sin errores

    def test_engagement_cero_valido(self):
        r = valid_record({"engagement": {"replies": 0, "reposts": 0, "likes": 0}})
        assert rule_engagement(r)[0]


# ---------------------------------------------------------------------------
# R7: privacidad
# ---------------------------------------------------------------------------
class TestRulePrivacy:
    def test_handle_normal_valido(self):
        assert rule_privacy(valid_record())[0]

    def test_email_en_author_name(self):
        r = valid_record({"author_name": "usuario@gmail.com"})
        ok, msg = rule_privacy(r)
        assert not ok and "R7" in msg

    def test_telefono_colombiano_en_author_id(self):
        r = valid_record({"author_id": "3001234567"})
        ok, msg = rule_privacy(r)
        assert not ok and "R7" in msg

    def test_nulos_validos(self):
        r = valid_record({"author_name": None, "author_id": None})
        assert rule_privacy(r)[0]


# ---------------------------------------------------------------------------
# validate_record: integracion
# ---------------------------------------------------------------------------
class TestValidateRecord:
    def test_registro_valido_pasa(self):
        result = validate_record(valid_record())
        assert result.is_valid
        assert len(result.errors) == 0

    def test_registro_con_multiples_errores(self):
        r = {
            "source": "twitter",       # R2
            "id": "",                  # R1
            "created_at": "bad-date",  # R3
            "text": "",                # R4
            "url": "no-url",           # R5
            "engagement": {"likes": -1},  # R6
            "raw_metadata": {},
        }
        result = validate_record(r)
        assert not result.is_valid
        assert len(result.errors) >= 5

    def test_dead_letter_payload_tiene_errores(self):
        r = valid_record({"id": None})
        result = validate_record(r)
        payload = result.to_dead_letter_payload()
        assert "validation_errors" in payload
        assert len(payload["validation_errors"]) > 0
        assert "original_record" in payload


# ---------------------------------------------------------------------------
# anonymize_record
# ---------------------------------------------------------------------------
class TestAnonymize:
    def test_author_name_queda_nulo(self):
        anon = anonymize_record(valid_record())
        assert anon["author_name"] is None

    def test_author_id_queda_nulo(self):
        anon = anonymize_record(valid_record())
        assert anon.get("author_id") is None

    def test_original_no_muta(self):
        original = valid_record()
        anonymize_record(original)
        assert original["author_name"] == "usuario_test"

    def test_resto_de_campos_intactos(self):
        original = valid_record()
        anon = anonymize_record(original)
        assert anon["source"] == original["source"]
        assert anon["text"] == original["text"]
        assert anon["tags"] == original["tags"]
