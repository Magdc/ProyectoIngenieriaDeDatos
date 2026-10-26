"""Profiling de Trusted con el contrato de §5.7, sobre un Trusted simulado (mientras no exista el Lote 4.2)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from quality import layers as L
from quality.profiler import find_group, iter_layer_files, profile_layer
from quality.testing import build_trusted, make_row, make_rows, placeholder_clean, write_raw_layer

AS_OF = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    rows = []
    for s in L.SOURCES:
        rows += make_rows("historical", s, 25, datetime(2026, 5, 1, 22, tzinfo=timezone.utc), step=timedelta(minutes=17))
        rows += make_rows("stream", s, 25, datetime(2026, 10, 23, 23, 40, tzinfo=timezone.utc), step=timedelta(seconds=90))
    rows.append(make_row("stream", "reddit", 0, datetime(2026, 10, 23, 23, 40, tzinfo=timezone.utc)))  # copia: Trusted la quita
    write_raw_layer(root, rows)
    raw = profile_layer(root, "raw", as_of=AS_OF)
    return root, raw


@pytest.fixture(scope="module")
def trusted(lake, tmp_path_factory):
    root, raw = lake
    out = tmp_path_factory.mktemp("trusted")
    info = build_trusted(root, out)
    return out, info, profile_layer(out, "trusted", as_of=AS_OF, raw_report=raw)


def test_layout_y_esquema_de_trusted(trusted):
    out, info, rep = trusted
    keys = [k for k, _, _ in iter_layer_files(out, "trusted")]
    assert keys and all(L.parse_key("trusted", k) for k in keys)
    assert info["distinct_event_ids"] == 150 and info["raw_rows"] == 151
    names = pq.read_schema(out / keys[0]).names
    assert names == list(L.TRUSTED_COLUMNS) and "source" not in names
    assert rep["files"]["schema_variants"][0]["matches_contract"] is True
    assert {(g["key"]["origin"], g["key"]["source"]) for g in rep["groups"]} >= {("*", "reddit"), ("historical", "facebook"), ("stream", "*")}


def test_reglas_de_trusted_cumplen(trusted):
    _, _, rep = trusted
    g = find_group(rep)
    assert g["rows"] == 150
    assert g["kpi"]["unicidad"] == 1.0 and g["kpi"]["duplicados"] == 0.0
    assert g["checks"]["partition:trusted_event_date"]["failed"] == 0
    assert g["checks"]["not_null:text_clean"]["failed"] == 0
    assert g["columns"]["source"]["distinct"] == 3  # source sale de la partición
    assert "text_clean" in g and g["text_clean"]["texts"] == 150
    by = {r["id"]: r for r in rep["rules"]}
    for rid in ("DQ-01", "DQ-49", "DQ-54", "DQ-55", "DQ-56", "DQ-58"):
        assert by[rid]["status"] == "cumple", (rid, by[rid]["observed"])
    assert "DQ-51" not in by  # solo de Raw
    assert [a for a in rep["anomalies"] if a["severity"] == "error"] == []


def test_cruza_la_medianoche_por_event_time(trusted):
    out, _, _ = trusted
    dts = {L.parse_key("trusted", k)["dt"] for k, _, _ in iter_layer_files(out, "trusted")}
    assert {"2026-05-01", "2026-05-02", "2026-10-23", "2026-10-24"} <= dts


def test_detecta_los_defectos_inyectados(lake, tmp_path):
    root, raw = lake
    info = build_trusted(root, tmp_path, defects=True)
    assert info["defects"] == ["duplicate_event_id", "dt_mismatch", "null_text_clean", "identity_column"]
    rep = profile_layer(tmp_path, "trusted", as_of=AS_OF, raw_report=raw)
    from ops.validate51 import detected_defects

    found = detected_defects(rep)
    assert all(found.values()), found
    by = {r["id"]: r for r in rep["rules"]}
    assert by["DQ-49"]["status"] == "no_cumple"
    assert by["DQ-54"]["status"] == "no_cumple"
    assert by["DQ-56"]["status"] == "no_cumple"
    assert by["DQ-02"]["status"] == "no_cumple"


def test_text_clean_de_marcador():
    assert placeholder_clean("Me ENCANTA #Postobón  hoy") == "me encanta hoy"
    assert placeholder_clean(None) is None
