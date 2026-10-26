"""Comprobaciones por fila (``quality.checks``): cada una cumple con una fila válida y falla con su defecto."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from quality import checks as C
from quality.testing import make_row

AS_OF = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
T0 = datetime(2026, 10, 23, 3, 20, 5, tzinfo=timezone.utc)


def to_arrow_like(row: dict) -> dict:
    """Las filas que lee pyarrow traen timestamps como ``datetime``; ``make_row`` los trae en texto."""
    out = dict(row)
    for k in ("event_time", "ingest_time", "process_time"):
        out[k] = datetime.strptime(out[k], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return out


def meta(layer="raw", origin="stream", source="reddit", dt="2026-10-23", hr="03", file="part-20261023T032000Z-p000.parquet"):
    return C.RowMeta(layer, origin, source, dt, hr, file, AS_OF)


def run_all(row, m):
    return {c.id: c.fn(row, m) for c in C.checks_for(m.layer)}


@pytest.mark.parametrize("source", ["reddit", "facebook", "instagram"])
def test_fila_valida_cumple_todo(source):
    row = to_arrow_like(make_row("stream", source, 3, T0))
    row["process_time"] = T0 + timedelta(seconds=10)
    res = run_all(row, meta(source=source))
    failed = sorted(k for k, v in res.items() if v is False)
    allowed = {f"not_null:{c}" for c in C.NULLABLE_BY_DESIGN}
    assert set(failed) <= allowed, failed


def test_ids_unicos_y_severidades_validas():
    ids = [c.id for c in C.CHECKS]
    assert len(ids) == len(set(ids))
    assert {c.severity for c in C.CHECKS} <= {C.ERROR, C.WARNING, C.INFO}
    assert C.CHECKS_BY_ID["not_null:shares"].severity == C.INFO
    assert C.CHECKS_BY_ID["not_null:event_id"].severity == C.ERROR
    assert "not_null:source" not in {c.id for c in C.checks_for("trusted")}
    assert "not_null:text_clean" not in {c.id for c in C.checks_for("raw")}


@pytest.mark.parametrize("check, mutate", [
    ("uuid:event_id", lambda r: r.update(event_id="ABC")),
    ("enum:origin", lambda r: r.update(origin="otro")),
    ("partition:origin", lambda r: r.update(origin="historical")),
    ("partition:source", lambda r: r.update(source="facebook")),
    ("regex:generator", lambda r: r.update(generator="gen")),
    ("set:generator", lambda r: r.update(generator="ta-verify@2.1")),
    ("regex:lang", lambda r: r.update(lang="ES")),
    ("set:lang", lambda r: r.update(lang="en")),
    ("regex:user_hash", lambda r: r.update(user_hash="xyz")),
    ("payload:no_identity", lambda r: r.update(payload=json.dumps({"kind": "post", "author": {"handle": "u/x"}}))),
    ("payload:json", lambda r: r.update(payload="no es json")),
    ("payload:kind", lambda r: r.update(payload=json.dumps({"kind": "story"}))),
    ("enum:sentiment_label", lambda r: r.update(sentiment_label="POS")),
    ("enum:emotion_label", lambda r: r.update(emotion_label="others")),
    ("enum:label_sentiment", lambda r: r.update(label_sentiment="mixed")),
    ("range:sentiment_score", lambda r: r.update(sentiment_score=1.5)),
    ("range:emotion_score", lambda r: r.update(emotion_score=-0.1)),
    ("regex:nlp_model_version", lambda r: r.update(nlp_model_version="otro@1")),
    ("nlp:complete", lambda r: r.update(emotion_label=None)),
    ("nlp:complete_since", lambda r: r.update(emotion_label=None)),
    ("regex:brands", lambda r: r.update(brands=["Pepsi"])),
    ("set:brands", lambda r: r.update(brands=["cocacola"], brand_primary="cocacola")),
    ("unique:brands", lambda r: r.update(brands=["pepsi", "pepsi"], brand_primary="pepsi")),
    ("set:brand_primary", lambda r: r.update(brand_primary="cocacola")),
    ("pair:brand_primary", lambda r: r.update(brand_primary="sprite" if r["brands"][0] != "sprite" else "pepsi")),
    ("regex:hashtags", lambda r: r.update(hashtags=["Con Espacio"])),
    ("unique:hashtags", lambda r: r.update(hashtags=["a", "a"])),
    ("consistency:hashtags_text", lambda r: r.update(hashtags=["inventado"])),
    ("range:followers", lambda r: r.update(followers=-1)),
    ("engagement:shares", lambda r: r.update(shares=3)),
    ("engagement:comments", lambda r: r.update(comments=-2)),
    ("text:not_empty", lambda r: r.update(text="   ")),
    ("time:ingest_ge_event", lambda r: r.update(ingest_time=r["event_time"] - timedelta(seconds=1))),
    ("time:ingest_lag", lambda r: r.update(event_time=r["ingest_time"] - timedelta(minutes=10))),
    ("time:process_ge_ingest", lambda r: r.update(process_time=r["ingest_time"] - timedelta(seconds=1))),
    ("time:not_future", lambda r: r.update(event_time=AS_OF + timedelta(hours=1))),
    ("partition:stream_process_hour", lambda r: r.update(process_time=T0 + timedelta(hours=3))),
])
def test_cada_defecto_falla_su_comprobacion(check, mutate):
    row = to_arrow_like(make_row("stream", "reddit", 7, T0))
    row["process_time"] = T0 + timedelta(seconds=10)
    row["label_sentiment"] = "positive"  # en la base, sin nulos (los nulos no aplican a las de formato)
    assert C.CHECKS_BY_ID[check].fn(row, meta()) is True, "la fila base debería cumplir"
    mutate(row)
    assert C.CHECKS_BY_ID[check].fn(row, meta()) is False


def test_likes_negativos_solo_en_reddit():
    row = to_arrow_like(make_row("stream", "facebook", 1, T0))
    row["likes"] = -5
    assert C.CHECKS_BY_ID["engagement:likes"].fn(row, meta(source="facebook")) is False
    row = dict(row, source="reddit", shares=None)
    assert C.CHECKS_BY_ID["engagement:likes"].fn(row, meta(source="reddit")) is True


def test_shares_en_facebook_es_obligatorio():
    row = to_arrow_like(make_row("stream", "facebook", 1, T0))
    assert C.CHECKS_BY_ID["engagement:shares"].fn(dict(row, shares=None), meta(source="facebook")) is False


def test_nulos_no_aplican_a_las_de_formato():
    row = to_arrow_like(make_row("stream", "reddit", 1, T0))
    row.update(lang=None, sentiment_score=None, user_hash=None)
    assert C.CHECKS_BY_ID["regex:lang"].fn(row, meta()) is None
    assert C.CHECKS_BY_ID["range:sentiment_score"].fn(row, meta()) is None
    assert C.CHECKS_BY_ID["regex:user_hash"].fn(row, meta()) is None
    assert C.CHECKS_BY_ID["not_null:user_hash"].fn(row, meta()) is False


def test_nlp_since_ignora_filas_anteriores():
    row = to_arrow_like(make_row("stream", "reddit", 1, T0))
    row.update(process_time=C.NLP_SINCE - timedelta(minutes=1), sentiment_label=None)
    assert C.CHECKS_BY_ID["nlp:complete_since"].fn(row, meta()) is None
    assert C.CHECKS_BY_ID["nlp:complete"].fn(row, meta()) is False


def test_particiones_del_historico_y_de_trusted():
    e = datetime(2026, 5, 1, 13, 45, tzinfo=timezone.utc)
    row = to_arrow_like(make_row("historical", "facebook", 1, e))
    m = meta(origin="historical", source="facebook", dt="2026-05-01", hr="13", file="part-historical-s3101-20260501T13.parquet")
    assert C.CHECKS_BY_ID["partition:hist_event_hour"].fn(row, m) is True
    assert C.CHECKS_BY_ID["partition:file_name"].fn(row, m) is True
    m2 = meta(origin="historical", source="facebook", dt="2026-05-01", hr="14", file="part-historical-s3101-20260501T14.parquet")
    assert C.CHECKS_BY_ID["partition:hist_event_hour"].fn(row, m2) is False
    assert C.CHECKS_BY_ID["partition:projection_range"].fn(row, meta(dt="2025-12-31")) is False
    t = C.RowMeta("trusted", "historical", "facebook", "2026-05-01", None, "part-0.parquet", AS_OF)
    assert C.CHECKS_BY_ID["partition:trusted_event_date"].fn(row, t) is True
    t2 = C.RowMeta("trusted", "historical", "facebook", "2026-05-02", None, "part-0.parquet", AS_OF)
    assert C.CHECKS_BY_ID["partition:trusted_event_date"].fn(row, t2) is False
    assert C.CHECKS_BY_ID["text_clean:not_longer"].fn(dict(row, text_clean=row["text"] + "x"), t) is False


def test_timestamp_sin_zona_se_interpreta_utc():
    naive = datetime(2026, 5, 1, 13, 0)
    assert C.as_utc(naive).tzinfo is timezone.utc
    assert C.as_utc("2026") is None
