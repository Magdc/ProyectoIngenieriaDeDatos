"""Profiling de punta a punta sobre un Raw sintético escrito con el escritor real (sin nube)."""

from __future__ import annotations

import io
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quality import layers as L
from quality.profiler import arrow_kind, find_group, profile_layer
from quality.testing import make_row, make_rows, write_raw_layer

AS_OF = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
HIST_START = datetime(2026, 5, 1, 0, 0, 30, tzinfo=timezone.utc)
STREAM_START = datetime(2026, 10, 23, 3, 0, 30, tzinfo=timezone.utc)
HEX64 = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


def build_raw(root: Path, n: int = 40) -> None:
    rows = []
    for source in L.SOURCES:
        rows += make_rows("historical", source, n, HIST_START, step=timedelta(minutes=37))
        rows += make_rows("stream", source, n, STREAM_START, step=timedelta(seconds=40))
    write_raw_layer(root, rows)


@pytest.fixture(scope="module")
def clean_report(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    build_raw(root)
    return profile_layer(root, "raw", as_of=AS_OF, source_uri="test")


def test_grupos_y_conteos(clean_report):
    rep = clean_report
    keys = [(g["key"]["origin"], g["key"]["source"]) for g in rep["groups"]]
    assert keys[0] == ("*", "*")
    assert {("historical", "*"), ("stream", "*")} <= set(keys)
    assert {(o, s) for o in L.ORIGINS for s in L.SOURCES} <= set(keys)
    assert find_group(rep)["rows"] == 240
    assert find_group(rep, "historical", "reddit")["rows"] == 40
    assert rep["files"]["unrecognized"] == 0 and rep["files"]["read_error_count"] == 0
    assert rep["summary"]["rows"]["historical/*"] == 120


def test_cada_columna_tiene_nulos_cardinalidad_tipo_y_distribucion(clean_report):
    g = find_group(clean_report, "stream")
    for name in L.RAW_COLUMNS:
        col = g["columns"][name]
        assert {"kind", "rows", "nulls", "null_ratio", "distinct"} <= set(col)
        assert any(k in col for k in ("top", "numeric", "length", "timestamp", "list", "frequency_histogram")), name
    assert g["columns"]["source"]["top_complete"] is True
    assert g["columns"]["followers"]["numeric"]["p50"] is not None
    assert g["columns"]["event_time"]["timestamp"]["by_hour_utc"][3] > 0
    assert "daily" in g["columns"]["event_time"]["timestamp"]
    assert g["columns"]["brands"]["list"]["length_histogram"]["1"] == 120
    phys = clean_report["files"]["physical_types"]
    assert phys["event_time"] == {"timestamp[ms, tz=UTC]": clean_report["files"]["recognized"]}


def test_columnas_sensibles_sin_valores(clean_report):
    g = find_group(clean_report)
    for name in ("event_id", "user_hash", "text", "payload"):
        col = g["columns"][name]
        assert "top" not in col and "frequency_histogram" in col
    blob = json.dumps(clean_report, ensure_ascii=False, default=str)
    assert not HEX64.search(blob), "el informe no puede llevar valores de user_hash"


def test_esquema_igual_al_contrato_y_sin_anomalias_de_error(clean_report):
    variants = clean_report["files"]["schema_variants"]
    assert len(variants) == 1 and variants[0]["matches_contract"], variants
    errors = [a for a in clean_report["anomalies"] if a["severity"] == "error"]
    assert errors == []
    kpi = clean_report["summary"]["kpi"]
    for origin in ("historical", "stream"):
        assert kpi[origin]["completitud"] == 1.0
        assert kpi[origin]["unicidad"] == 1.0
        assert kpi[origin]["anonimizacion"] == 1.0


def test_texto_y_comparacion(clean_report):
    g = find_group(clean_report, "historical")
    t = g["text"]
    assert t["texts"] == 120 and t["types"] > 5 and 0 < t["mattr"] <= 1
    assert t["repeated"]["distinct_ratio"] == 1.0
    comp = clean_report["comparison"]
    assert comp["available"] and "brand_primary" in comp["categorical"]
    assert comp["overlap"]["users_shared"] > 0  # los dos orígenes usan el mismo pool de usuarios
    assert comp["numeric"]["latency_s_p50"]["stream"] < 60


def test_by_dt_y_desfase_de_particion(clean_report):
    h = find_group(clean_report, "historical")
    assert sum(v["rows"] for v in h["by_dt"].values()) == 120
    off = h["derived"]["partition_offset_s"]["numeric"]
    assert 0 <= off["min"] and off["max"] < 3600
    s = find_group(clean_report, "stream")
    assert s["checks"]["partition:stream_process_hour"]["failed"] == 0
    assert h["checks"]["partition:hist_event_hour"]["failed"] == 0


def _write_parquet(path: Path, rows: list[dict], schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pydict(cols, schema=schema), buf)
    path.write_bytes(buf.getvalue())


def test_detecta_defectos_en_raw(tmp_path):
    from common.parquet_rows import arrow_schema, rows_to_parquet_bytes

    build_raw(tmp_path, n=10)
    # 1. el mismo event_id en otro archivo del stream (copia de un reinicio)
    dup = make_row("stream", "reddit", 0, STREAM_START)
    other = tmp_path / "raw/origin=stream/source=reddit/dt=2026-10-23/hr=05/part-20261023T050000Z-p000.parquet"
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_bytes(rows_to_parquet_bytes([dup]))
    # 2. una columna de identidad y un user_hash inválido
    bad = make_row("stream", "facebook", 500, STREAM_START)
    bad["user_hash"] = "no-es-hex"
    schema = arrow_schema().append(pa.field("handle", pa.string()))
    rows = [dict(bad, handle="u/alguien")]
    for k in ("event_time", "ingest_time", "process_time"):
        rows[0][k] = datetime.strptime(rows[0][k], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    _write_parquet(tmp_path / "raw/origin=stream/source=facebook/dt=2026-10-23/hr=03/part-20261023T031000Z-p001.parquet", rows, schema)
    # 3. histórico en una partición que no es la de su event_time
    wrong = make_row("historical", "instagram", 900, HIST_START)
    p = tmp_path / "raw/origin=historical/source=instagram/dt=2026-05-03/hr=07/part-historical-s990051-20260503T07.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(rows_to_parquet_bytes([wrong]))
    # 4. una clave fuera del layout, un archivo ilegible y un auxiliar
    (tmp_path / "raw/origin=stream/suelto.parquet").write_bytes(b"x")
    roto = tmp_path / "raw/origin=stream/source=reddit/dt=2026-10-23/hr=06/part-20261023T060000Z-p000.parquet"
    roto.parent.mkdir(parents=True, exist_ok=True)
    roto.write_bytes(b"no es parquet")
    (tmp_path / "raw/origin=stream/source=reddit/dt=2026-10-23/hr=06/_SUCCESS").write_bytes(b"")

    rep = profile_layer(tmp_path, "raw", as_of=AS_OF)
    kinds = {(a["kind"], a.get("check")) for a in rep["anomalies"]}
    assert rep["duplicates"]["other_file_same_origin"] == 1
    assert ("duplicado", None) in kinds
    assert any(a["kind"] == "esquema" and a["identity"] == ["handle"] for a in rep["anomalies"])
    assert ("regla", "regex:user_hash") in kinds
    assert ("regla", "partition:hist_event_hour") in kinds
    assert rep["files"]["unrecognized"] == 1 and rep["files"]["read_error_count"] == 1 and rep["files"]["auxiliary"] == 1
    assert find_group(rep, "stream")["kpi"]["anonimizacion"] < 1.0
    assert not HEX64.search(json.dumps(rep, default=str))
    statuses = {r["id"]: r["status"] for r in rep["rules"]}
    assert statuses["DQ-02"] == "no_cumple" and statuses["DQ-03"] == "no_cumple" and statuses["DQ-51"] == "no_cumple"


def test_arrow_kind():
    assert arrow_kind("timestamp[ms, tz=UTC]") == "timestamp"
    assert arrow_kind("list<element: string>") == "list"
    assert arrow_kind("large_string") == "string"
    assert arrow_kind("int32") == "int"
    assert arrow_kind("double") == "float"


def test_capa_vacia(tmp_path):
    rep = profile_layer(tmp_path, "raw", as_of=AS_OF)
    assert rep["files"]["listed"] == 0
    assert any(a["kind"] == "sin_datos" for a in rep["anomalies"])
    assert all(r["status"] == "sin_dato" for r in rep["rules"])
