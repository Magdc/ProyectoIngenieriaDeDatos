"""Anonimización, normalización y fila plana (Lote 2.2, HU-020, HU-021)."""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from common import enriched_row as er
from common.parquet_rows import arrow_schema, parquet_bytes_to_rows, rows_to_parquet_bytes
from streaming.enriched_testing import TEST_KEY, make_envelope

PROJECT_DIR = Path(__file__).resolve().parents[3]
BQ_SCHEMA_FILE = PROJECT_DIR / "infra" / "gcp" / "modules" / "bigquery_stream" / "schemas" / "posts_enriched.json"
PT = "2026-10-17T14:03:13.900Z"


@pytest.mark.parametrize("source,kind", [("reddit", "post"), ("reddit", "comment"), ("facebook", "post"), ("facebook", "comment"), ("instagram", "comment")])
def test_fila_sin_identidad_y_con_columnas_del_contrato(source, kind):
    env = make_envelope(source, 3, kind=kind)
    row = er.envelope_to_row(env, TEST_KEY, PT)
    assert tuple(row) == er.COLUMN_NAMES
    assert "user_id" not in row and "handle" not in row
    assert not er.find_identity_paths(row)
    assert not er.find_identity_paths(json.loads(row["payload"]))
    assert er.USER_HASH_RE.match(row["user_hash"])
    assert er.row_problems(row) == []
    # el handle no aparece en ningún valor
    assert env["author"]["handle"] not in json.dumps(row, ensure_ascii=False)


def test_envelope_enriquecido_quita_identidad_y_conserva_lo_demas():
    env = make_envelope("facebook", 4, kind="comment")
    out = er.enrich_envelope(env, TEST_KEY, PT)
    assert out["author"] == {"user_hash": out["author"]["user_hash"], "followers": 412}
    assert out["process_time"] == PT
    for k in ("schema_version", "event_id", "origin", "source", "generator", "seq", "event_time", "ingest_time", "text", "lang", "brands", "hashtags", "payload", "sim"):
        assert out[k] == env[k]
    assert (out["likes"], out["shares"], out["comments"]) == (14, 2, 3)


def test_no_modifica_el_envelope_de_entrada():
    env = make_envelope("reddit", 5)
    before = copy.deepcopy(env)
    er.envelope_to_row(env, TEST_KEY, PT)
    assert env == before


def test_hash_determinista_y_distinto_por_usuario_y_por_llave():
    a1 = er.envelope_to_row(make_envelope("reddit", 1, user_id="u_000481"), TEST_KEY, PT)["user_hash"]
    a2 = er.envelope_to_row(make_envelope("facebook", 9, user_id="u_000481"), TEST_KEY, PT)["user_hash"]
    b = er.envelope_to_row(make_envelope("reddit", 2, user_id="u_000482"), TEST_KEY, PT)["user_hash"]
    other_key = er.envelope_to_row(make_envelope("reddit", 1, user_id="u_000481"), TEST_KEY + "x", PT)["user_hash"]
    assert a1 == a2 != b
    assert a1 != other_key


def test_hash_usa_common_hashing():
    from common import hashing

    assert er.hash_user("u_000481", TEST_KEY) == hashing.user_hash("u_000481", TEST_KEY)


def test_hash_es_hmac_sha256_del_user_id():
    """Referencia de §5.2: HMAC-SHA256 hex con la llave en UTF-8."""
    expected = hmac.new(TEST_KEY.encode("utf-8"), b"u_000481", hashlib.sha256).hexdigest()
    assert er.hash_user("u_000481", TEST_KEY) == expected


@pytest.mark.parametrize(
    "source,kind,expected",
    [
        ("reddit", "post", ("score", None, "num_comments")),
        ("reddit", "comment", ("score", None, None)),
        ("facebook", "comment", ("reactions_total", "shares_count", "comments_count")),
        ("instagram", "comment", ("like_count", None, "comments_count")),
    ],
)
def test_normalizacion_de_engagement(source, kind, expected):
    env = make_envelope(source, 7, kind=kind)
    row = er.envelope_to_row(env, TEST_KEY, PT)
    p = env["payload"]
    want = tuple(p[k] if k else None for k in expected)
    assert (row["likes"], row["shares"], row["comments"]) == want


def test_engagement_acepta_tupla(monkeypatch):
    monkeypatch.setattr(er._envelope, "normalize_engagement", lambda s, p: (1, None, 2))
    assert er.engagement("reddit", {}) == (1, None, 2)


def test_brand_primary_label_sentiment_y_nlp_en_nulo():
    env = make_envelope("instagram", 2, brand="pepsi", label_sentiment=None)
    env["brands"] = ["pepsi", "postobon"]
    row = er.envelope_to_row(env, TEST_KEY, PT)
    assert row["brand_primary"] == "pepsi"
    assert row["label_sentiment"] is None
    assert all(row[k] is None for k in er.NLP_FIELDS)
    env["brands"] = []
    assert er.envelope_to_row(env, TEST_KEY, PT)["brand_primary"] is None


def test_followers_nulo_se_conserva():
    row = er.envelope_to_row(make_envelope("reddit", 2, followers=None), TEST_KEY, PT)
    assert row["followers"] is None


def test_errores_claros():
    env = make_envelope("reddit", 1)
    del env["author"]["user_id"]
    with pytest.raises(ValueError):
        er.envelope_to_row(env, TEST_KEY, PT)
    with pytest.raises(ValueError):
        er.hash_user("u_1", "")


def test_process_time_formato():
    assert er.format_process_time(datetime(2026, 10, 17, 14, 3, 13, 900123, tzinfo=timezone.utc)) == PT
    with pytest.raises(ValueError):
        er.format_process_time(datetime(2026, 1, 1))


def test_row_problems_detecta_violaciones():
    row = er.envelope_to_row(make_envelope("reddit", 1), TEST_KEY, PT)
    bad = dict(row, shares=3, user_hash="xyz", brand_primary="otra")
    problems = er.row_problems(bad)
    assert "shares debe ser nulo en reddit" in problems
    assert "user_hash no es hex de 64" in problems
    assert "brand_primary distinto de brands[0]" in problems
    assert "columna de identidad 'handle'" in er.row_problems(dict(row, handle="u/x"))
    assert "identidad dentro de payload" in er.row_problems(dict(row, payload=json.dumps({"a": {"user_id": "u"}})))


def test_schema_de_bigquery_de_terraform_es_el_de_common():
    on_disk = json.loads(BQ_SCHEMA_FILE.read_text(encoding="utf-8"))
    assert on_disk == er.bigquery_schema_fields()


def test_parquet_ida_y_vuelta_y_bytes_deterministas():
    rows = [er.envelope_to_row(make_envelope(s, i), TEST_KEY, PT) for i, s in enumerate(["reddit", "facebook", "instagram"] * 3)]
    a = rows_to_parquet_bytes(rows)
    b = rows_to_parquet_bytes(list(reversed(rows)))
    assert a == b  # se ordena por event_id
    back = parquet_bytes_to_rows(a)
    by_id = {r["event_id"]: r for r in rows}
    for r in back:
        assert r == by_id[r["event_id"]]


def test_parquet_tipos():
    import pyarrow as pa

    schema = arrow_schema()
    assert schema.names == list(er.COLUMN_NAMES)
    assert schema.field("event_time").type == pa.timestamp("ms", tz="UTC")
    assert schema.field("brands").type.value_type == pa.string()
    assert schema.field("likes").type == pa.int64()
    assert schema.field("payload").type == pa.string()
