"""La tabla de KPI del informe sale de la evidencia real (Lote 8.1, HU-044)."""

import json

import pytest

from report import kpis


def test_the_real_evidence_meets_every_kpi():
    rows = kpis.build()
    assert len(rows) == 9
    assert [k.name for k in rows if k.ok is False] == []
    assert sum(1 for k in rows if k.ok is None) == 1  # la precisión del NLP es informativa
    names = " ".join(k.name for k in rows)
    for needed in ("Latencia de ingesta", "detección de tendencias", "Completitud", "Unicidad", "Duplicados", "tablero", "anonimizados"):
        assert needed in names


def test_the_informe_table_is_in_sync():
    text = kpis.INFORME.read_text(encoding="utf-8")
    assert kpis.splice(text, kpis.render(kpis.build())) == text, "corra: python project/scripts/kpi_report.py --write"


def test_quality_takes_the_worst_value():
    rows = [{"kpi": "completitud", "layer": "trusted", "observed": 0.97}, {"kpi": "completitud", "layer": "trusted", "observed": 0.99},
            {"kpi": "duplicados", "layer": "trusted", "observed": 0.001}, {"kpi": "duplicados", "layer": "trusted", "observed": 0.02},
            {"kpi": "completitud", "layer": "raw", "observed": 0.5}]
    assert kpis.quality(rows, "completitud") == (0.97, 2)
    assert kpis.quality(rows, "duplicados") == (0.02, 2)
    with pytest.raises(ValueError):
        kpis.quality(rows, "unicidad")


def test_a_failing_measurement_is_marked(tmp_path):
    for name in ("smoke_e2e_5_4.json", "validacion_5_2.json", "validacion_6_1.json", "validacion_6_2.json", "validacion_6_3.json"):
        (tmp_path / name).write_text((kpis.EVIDENCE / name).read_text(encoding="utf-8"), encoding="utf-8")
    smoke = json.loads((tmp_path / "smoke_e2e_5_4.json").read_text(encoding="utf-8"))
    smoke["stages"][0]["latency_s"] = 75.0
    (tmp_path / "smoke_e2e_5_4.json").write_text(json.dumps(smoke), encoding="utf-8")
    rows = kpis.build(tmp_path)
    assert rows[0].ok is False and rows[0].measured == "75,0 s"
    assert "❌" in kpis.render(rows)


def test_splice_needs_the_markers():
    with pytest.raises(ValueError):
        kpis.splice("# sin marcadores", "| tabla |")
    text = f"a\n{kpis.START}\nviejo\n{kpis.END}\nb"
    assert kpis.splice(text, "nuevo") == f"a\n{kpis.START}\nnuevo\n{kpis.END}\nb"
