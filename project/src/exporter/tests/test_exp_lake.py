"""Parte F: ambos orígenes en S3 y el esquema de un Parquet de cada uno (necesita pyarrow)."""

import io

import pytest

from exporter.lake_check import DOC_COLUMNS, inspect_parquet, lake_report, posts_enriched_column_names

HASH = "a" * 64


@pytest.fixture
def arrow():
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    return pa, pq


def _parquet(arrow, origin="stream", rows=3, columns=DOC_COLUMNS, **override):
    pa, pq = arrow
    data = {}
    for col in columns:
        if col == "origin":
            data[col] = [origin] * rows
        elif col == "user_hash":
            data[col] = [HASH] * rows
        elif col in ("brands", "hashtags"):
            data[col] = [["postobon"]] * rows
        elif col in ("likes", "followers", "shares", "comments"):
            data[col] = [1] * rows
        elif col == "payload":
            data[col] = ['{"kind":"comment"}'] * rows
        else:
            data[col] = [f"{col}-{i}" for i in range(rows)]
    data.update(override)
    buf = io.BytesIO()
    pq.write_table(pa.table(data), buf)
    return buf.getvalue()


@pytest.mark.pyarrow
def test_parquet_bueno_con_las_columnas_en_orden(arrow):
    info = inspect_parquet(_parquet(arrow), "stream", list(DOC_COLUMNS))
    assert info == {"rows": 3, "columns": 24, "problems": []}
    assert inspect_parquet(_parquet(arrow, "historical"), "historical", None)["problems"] == []


@pytest.mark.pyarrow
def test_parquet_con_problemas(arrow):
    body = _parquet(arrow, columns=(*DOC_COLUMNS, "user_id"), user_id=["u_1"] * 3,
                    user_hash=["zz", HASH, None], payload=['{"handle":"u/x"}', "{}", "{}"])
    problems = " | ".join(inspect_parquet(body, "historical", list(DOC_COLUMNS))["problems"])
    for word in ("columnas distintas", "user_id en claro", "origin", "hex de 64", "payload"):
        assert word in problems


@pytest.mark.pyarrow
def test_reporte_de_ambos_origenes(arrow):
    objects = {
        "raw/origin=historical/source=reddit/dt=2026-05-01/hr=01/part-historical-s3101-20260501T01.parquet": _parquet(arrow, "historical"),
        "raw/origin=stream/source=facebook/dt=2026-10-24/hr=12/part-20261024T120000Z-p000.parquet": _parquet(arrow, "stream"),
    }

    def count(prefix):
        hits = [len(v) for k, v in objects.items() if k.startswith(prefix)]
        return len(hits), sum(hits)

    def first(prefix):
        return next((k for k in sorted(objects) if k.startswith(prefix)), None)

    report = lake_report(count, first, objects.__getitem__, expected_columns=list(DOC_COLUMNS), columns_source="doc")
    assert report["origins"]["historical"]["objects"] == 1 and report["origins"]["stream"]["objects"] == 1
    assert report["origins"]["stream"]["by_source"]["facebook"]["objects"] == 1
    assert report["origins"]["historical"]["sample"]["problems"] == []


def test_adaptador_de_columnas_acepta_varias_formas(monkeypatch):
    import sys
    import types

    for shape in (list(DOC_COLUMNS), [(c, "STRING", "NULLABLE") for c in DOC_COLUMNS],
                  [{"name": c, "type": "STRING"} for c in DOC_COLUMNS], {c: "STRING" for c in DOC_COLUMNS},
                  [types.SimpleNamespace(name=c) for c in DOC_COLUMNS]):
        mod = types.ModuleType("common.enriched_row")
        mod.POSTS_ENRICHED_COLUMNS = shape
        monkeypatch.setitem(sys.modules, "common.enriched_row", mod)
        names, source = posts_enriched_column_names()
        assert names == list(DOC_COLUMNS), source
    mod.POSTS_ENRICHED_COLUMNS = [42]
    names, source = posts_enriched_column_names()
    assert names is None and "no sé leer" in source
