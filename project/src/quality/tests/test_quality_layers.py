"""Capas, claves y patrones del profiler frente a los contratos del repositorio (sin red)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.enriched_row import COLUMN_NAMES
from quality import layers as L

PROJECT = Path(__file__).resolve().parents[3]
CONTRACTS = PROJECT / "contracts"


def ddl(name: str) -> tuple[list[str], list[str]]:
    from ops.validate51 import ddl_columns

    return ddl_columns((CONTRACTS / "athena" / f"{name}.sql").read_text(encoding="utf-8"))


def test_raw_son_las_24_de_posts_enriched_y_la_ddl():
    cols, parts = ddl("posts_raw")
    assert list(L.RAW_COLUMNS) == list(COLUMN_NAMES) == cols
    assert len(cols) == 24
    assert parts == ["part_origin", "part_source", "part_dt", "part_hr"]


def test_trusted_sin_source_con_text_clean_y_la_ddl():
    cols, parts = ddl("posts")
    assert list(L.TRUSTED_COLUMNS) == cols
    assert "source" not in L.TRUSTED_COLUMNS
    assert L.TRUSTED_COLUMNS.index("text_clean") == L.TRUSTED_COLUMNS.index("text") + 1
    assert parts == ["source", "dt"]


def _schema_patterns() -> dict[str, str]:
    found: dict[str, str] = {}

    def walk(obj, path=""):
        if isinstance(obj, dict):
            if "pattern" in obj and path:
                found.setdefault(path.rsplit(".", 1)[-1], obj["pattern"])
            for k, v in obj.items():
                walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for v in obj:
                walk(v, path)

    for name in ("envelope.v1.schema.json", "envelope.enriched.v1.schema.json"):
        walk(json.loads((CONTRACTS / name).read_text(encoding="utf-8")))
    return found


@pytest.mark.parametrize("key, rx", [
    ("uuid", L.UUID_RE), ("generator", L.GENERATOR_RE), ("lang", L.LANG_RE), ("brand_key", L.BRAND_KEY_RE),
    ("hashtag", L.HASHTAG_RE), ("nlp_model_version", L.NLP_VERSION_RE), ("user_hash", L.HEX64_RE),
])
def test_patrones_iguales_a_contracts(key, rx):
    patterns = _schema_patterns()
    assert key in patterns, f"no hay patrón {key} en contracts/"
    assert rx.pattern == patterns[key]


def test_parse_key_raw_y_trusted():
    k = "raw/origin=stream/source=reddit/dt=2026-10-23/hr=03/part-20261023T032000Z-p000.parquet"
    assert L.parse_key("raw", k) == {"origin": "stream", "source": "reddit", "dt": "2026-10-23", "hr": "03",
                                     "file": "part-20261023T032000Z-p000.parquet"}
    assert L.parse_key("raw", "raw/origin=stream/source=reddit/dt=2026-10-23/x.parquet") is None
    t = L.parse_key("trusted", "trusted/posts/source=facebook/dt=2026-05-01/part-00000-abc.snappy.parquet")
    assert t == {"source": "facebook", "dt": "2026-05-01", "file": "part-00000-abc.snappy.parquet"}
    with pytest.raises(ValueError):
        L.layer_spec("curated")


def test_nombre_del_archivo_frente_a_la_particion():
    ok = {"file": "part-20261023T032000Z-p000.parquet", "dt": "2026-10-23", "hr": "03"}
    assert L.file_name_matches_partition("stream", ok) is True
    assert L.file_name_matches_partition("stream", dict(ok, hr="04")) is False
    hist = {"file": "part-historical-s3101-20260427T00.parquet", "dt": "2026-04-27", "hr": "00"}
    assert L.file_name_matches_partition("historical", hist) is True
    assert L.file_name_matches_partition("historical", dict(hist, dt="2026-04-28")) is False
    assert L.historical_seed(hist["file"]) == "3101"
    assert L.stream_window_start(ok["file"]).isoformat() == "2026-10-23T03:20:00+00:00"


def test_brands_yaml_real_y_respaldo(tmp_path):
    brands = L.load_brand_aliases()
    assert set(brands) == {"postobon", "pepsi", "sprite"}
    assert "Postobón" in brands["postobon"]
    assert set(L.load_brand_aliases(tmp_path / "no-existe.yaml")) == set(L.BRAND_KEYS_V1)


def test_tipos_logicos():
    assert L.COLUMN_KINDS["event_time"] == "timestamp"
    assert L.COLUMN_KINDS["brands"] == "list"
    assert L.COLUMN_KINDS["payload"] == "json"
    assert L.COLUMN_KINDS["followers"] == "int"
    assert L.COLUMN_KINDS["sentiment_score"] == "float"
    assert L.COLUMN_KINDS["text_clean"] == "string"
    assert L.SENSITIVE_COLUMNS >= {"user_hash", "event_id", "text"}
