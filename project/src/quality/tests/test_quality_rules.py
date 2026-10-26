"""Catálogo de reglas propuestas (``quality.rules``) frente al código y a ``ARCHITECTURE.md`` §5.10."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.stream_tables import DQ_KPIS, DQ_LAYERS, DQ_RESULTS_COLUMNS
from quality import rules as R
from quality.checks import CHECKS_BY_ID
from quality.profiler import profile_layer
from quality.testing import make_rows, write_raw_layer

REPO = Path(__file__).resolve().parents[4]
METRICS = {"schema", "anonimizacion", "completitud", "unicidad", "duplicados", "nlp_single_version", "label_sentiment_null",
           "text_max_length", "latency_p95", "source_share", "text_distinct_ratio", "nlp_agreement", "rows_vs_raw"}


def test_ids_consecutivos_y_unicos():
    ids = list(R.RULE_IDS)
    assert ids == [f"DQ-{i:02d}" for i in range(1, len(ids) + 1)]
    assert len(set(ids)) == len(ids)


@pytest.mark.parametrize("rule", R.RULES, ids=lambda r: r.id)
def test_cada_regla_esta_completa(rule):
    assert rule.layers and set(rule.layers) <= {"raw", "trusted"}
    assert rule.kpi in DQ_KPIS
    assert rule.severity in ("error", "warning")
    assert rule.threshold
    assert rule.scope in ("*", "stream", "historical")
    if rule.check.startswith("metric:"):
        assert rule.check.split(":", 1)[1] in METRICS
    else:
        check = CHECKS_BY_ID[rule.check]
        assert any(lay in check.layers for lay in rule.layers)
        assert rule.mostly is not None and 0 < rule.mostly <= 1


def test_cubren_los_kpi_de_5_10_en_las_dos_capas():
    for layer in ("raw", "trusted"):
        kpis = {r.kpi for r in R.RULES if layer in r.layers}
        assert {"completitud", "unicidad", "duplicados", "anonimizacion"} <= kpis, layer
    assert set(DQ_LAYERS) >= {"raw", "trusted"}


def test_architecture_5_10_lista_el_mismo_catalogo():
    text = (REPO / "docs" / "ARCHITECTURE.md").read_text(encoding="utf-8")
    from ops.validate51 import doc_rule_ids

    assert doc_rule_ids(text) == list(R.RULE_IDS)
    section = text[text.find("### 5.10"):text.find("### 5.11")]
    for rule in R.RULES:
        row = next(line for line in section.splitlines() if re.match(rf"^\|\s*{rule.id}\s*\|", line))
        assert f"`{rule.check}`" in row, f"{rule.id}: §5.10 no nombra la comprobación {rule.check}"
        assert rule.severity in row, f"{rule.id}: §5.10 no dice la gravedad {rule.severity}"


@pytest.fixture(scope="module")
def report(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    rows = []
    for s in ("reddit", "facebook", "instagram"):
        rows += make_rows("historical", s, 30, datetime(2026, 6, 1, tzinfo=timezone.utc), step=timedelta(hours=1))
        rows += make_rows("stream", s, 30, datetime(2026, 10, 23, 4, tzinfo=timezone.utc), step=timedelta(seconds=50))
    write_raw_layer(root, rows)
    return profile_layer(root, "raw", as_of=datetime(2026, 10, 26, tzinfo=timezone.utc))


def test_evaluate_solo_reglas_de_la_capa(report):
    ids = {r["id"] for r in report["rules"]}
    assert "DQ-49" not in ids and "DQ-54" not in ids  # de Trusted
    assert "DQ-14" in ids and "DQ-51" in ids
    by = {r["id"]: r for r in report["rules"]}
    assert set(by["DQ-51"]["observed"]) == {"historical"}  # alcance historical
    assert set(by["DQ-59"]["observed"]) == {"stream"}
    assert by["DQ-06"]["status"] == "cumple"
    assert by["DQ-01"]["status"] == "cumple"
    assert by["DQ-60"]["status"] == "cumple"


def test_dq_preview_con_las_columnas_de_dq_results(report):
    names = {c.name for c in DQ_RESULTS_COLUMNS}
    rows = report["dq_results_preview"]
    assert rows and all(set(r) == names for r in rows)
    assert {r["kpi"] for r in rows} == {"completitud", "unicidad", "duplicados", "anonimizacion"}
    assert all(r["layer"] == "raw" and r["table_name"] == "ta_raw.posts_raw" and r["suite"] == "profiling_5_1" for r in rows)
    assert all(r["passed"] for r in rows)


def test_rows_vs_raw_con_y_sin_informe_de_raw():
    trusted = {"layer": "trusted", "files": {"schema_variants": []},
               "groups": [{"key": {"origin": "*", "source": "*"}, "rows": 99, "checks": {}, "kpi": {}, "columns": {}, "derived": {}}]}
    by = {r["id"]: r for r in R.evaluate(trusted)}
    assert by["DQ-58"]["status"] == "sin_dato"
    by = {r["id"]: r for r in R.evaluate(trusted, {"duplicates": {"distinct_event_ids": 100}})}
    assert by["DQ-58"]["status"] == "cumple"
    by = {r["id"]: r for r in R.evaluate(trusted, {"duplicates": {"distinct_event_ids": 98}})}
    assert by["DQ-58"]["status"] == "no_cumple"
