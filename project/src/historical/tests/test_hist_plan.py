"""Plan del histórico: configuración, reparto en el tiempo y aceptación (lógica pura)."""

from __future__ import annotations

import collections
import datetime as dt
import subprocess
import sys

import pytest

from historical import plan
from historical.plan import (UTC, PlanError, TrendModel, accept_draw, config_from_dict,
                             format_ts, load_config, parse_ts, pearson, resolve_as_of,
                             slot_times, split_counts)


def test_configuracion_por_defecto_valida():
    cfg = load_config()
    assert cfg.n == 50_000 and cfg.days == 180 and cfg.seed == 3101
    assert cfg.seed != 7, "la semilla del histórico debe ser distinta de SIM_SEED"
    assert set(cfg.sources) == {"reddit", "facebook", "instagram"}
    assert {e.name for e in cfg.events} == {"campana_sprite", "crisis_postobon"}
    assert set(cfg.brands) == {"postobon", "pepsi", "sprite"}


@pytest.mark.parametrize("bad, msg", [
    ({"n": 0}, "n debe"),
    ({"weekly": [1] * 6}, "weekly"),
    ({"hourly": [1] * 23}, "hourly"),
    ({"sources": {"twitter": 1}}, "fuente desconocida"),
    ({"growth": -1}, "growth"),
    ({"otra": 1}, "claves desconocidas"),
    ({"events": [{"name": "x", "brand": "pepsi", "start_days_ago": 5, "duration_days": 2,
                  "sentiment": {"feliz": 2}}]}, "sentimiento desconocido"),
    ({"brands": {"pepsi": {"trend": 0.1}},
      "events": [{"name": "x", "brand": "sprite", "start_days_ago": 5, "duration_days": 2}]},
     "no está en brands"),
])
def test_configuracion_invalida(bad, msg):
    with pytest.raises(PlanError, match=msg):
        config_from_dict(bad)


def test_evento_fuera_de_la_ventana_se_admite():
    cfg = config_from_dict({"days": 30, "brands": {"pepsi": {"trend": 0}},
                            "events": [{"name": "viejo", "brand": "pepsi", "start_days_ago": 90,
                                        "duration_days": 3, "volume": 2}]})
    model = TrendModel(cfg, dt.datetime(2026, 10, 23, tzinfo=UTC))
    assert model.brand_factor("pepsi", dt.datetime(2026, 10, 1, tzinfo=UTC)) == pytest.approx(1.0)


def test_split_counts_suma_exacta_y_determinista():
    assert split_counts(50_000, {"reddit": 1, "facebook": 1, "instagram": 1}) == {
        "reddit": 16_667, "facebook": 16_667, "instagram": 16_666}
    assert sum(split_counts(1001, {"reddit": 2, "instagram": 1}).values()) == 1001


def test_timestamps_del_contrato():
    ts = dt.datetime(2026, 5, 1, 13, 2, 3, 45000, tzinfo=UTC)
    assert format_ts(ts) == "2026-05-01T13:02:03.045Z"
    assert parse_ts("2026-05-01T13:02:03.045Z") == ts


def test_as_of_por_defecto_es_hoy_a_medianoche_y_no_futuro():
    now = dt.datetime(2026, 10, 23, 17, 5, tzinfo=UTC)
    assert resolve_as_of(None, now) == dt.datetime(2026, 10, 23, tzinfo=UTC)
    assert resolve_as_of("2026-10-01", now) == dt.datetime(2026, 10, 1, tzinfo=UTC)
    assert resolve_as_of("2026-10-01T05:00:00Z", now).hour == 5
    with pytest.raises(PlanError, match="futuro"):
        resolve_as_of("2026-10-25", now)
    with pytest.raises(PlanError, match="inválido"):
        resolve_as_of("ayer", now)


def test_sha256_cambia_con_el_plan(cfg_factory):
    a = cfg_factory()
    assert a.sha256() == cfg_factory().sha256()
    assert a.sha256() != cfg_factory(n=1000).sha256()
    assert a.sha256() != cfg_factory(seed=1).sha256()
    assert a.sha256() != a.replace(as_of="2026-10-22T00:00:00.000Z").sha256()


def test_slot_times_exactos_ordenados_y_en_la_ventana(cfg_factory, as_of):
    cfg = cfg_factory()
    model = TrendModel(cfg, as_of)
    times = slot_times(model, "reddit", 16_667)
    assert len(times) == 16_667
    assert times == sorted(times)
    assert times[0] >= model.window.start and times[-1] < as_of
    assert all(t.microsecond % 1000 == 0 for t in times[:100])
    assert slot_times(model, "reddit", 16_667) == times
    assert slot_times(model, "facebook", 16_667) != times


def test_slot_times_siguen_la_estacionalidad(cfg_factory, as_of):
    cfg = cfg_factory()
    model = TrendModel(cfg, as_of)
    times = slot_times(model, "instagram", 30_000)
    off = dt.timedelta(hours=cfg.local_utc_offset_hours)
    by_hour = collections.Counter((t + off).hour for t in times)
    assert pearson([by_hour[h] for h in range(24)], list(cfg.hourly)) > 0.97
    first = sum(1 for t in times if t < model.window.start + dt.timedelta(days=30))
    last = sum(1 for t in times if t >= as_of - dt.timedelta(days=30))
    assert last / first > 1.15  # crecimiento del 30 % (más tendencias)


def test_aceptacion_sigue_tendencias_y_episodios(cfg_factory, as_of):
    model = TrendModel(cfg_factory(), as_of)
    start = model.window.start + dt.timedelta(hours=1)
    end = as_of - dt.timedelta(hours=1)
    # pepsi gana participación (+35 %) y postobon la pierde (-10 %): relativa a pepsi, la
    # aceptación de postobon baja de 1 a 0,9 / 1,35.
    assert model.acceptance("pepsi", "neutral", end) == pytest.approx(1.0)
    assert model.acceptance("postobon", "neutral", start) == pytest.approx(1.0, abs=0.01)
    assert model.acceptance("postobon", "neutral", end) == pytest.approx(0.9 / 1.35, abs=0.01)
    crisis = as_of - dt.timedelta(days=38)
    calm = as_of - dt.timedelta(days=60)
    assert model.acceptance("postobon", "negative", crisis) > model.acceptance("postobon", "positive", crisis)
    assert model.acceptance("postobon", "negative", calm) == pytest.approx(
        model.acceptance("postobon", "positive", calm))
    for brand in ("postobon", "pepsi", "sprite", None):
        for label in ("positive", "negative", "neutral", None):
            assert 0 < model.acceptance(brand, label, crisis) <= 1


def test_accept_draw_independiente_de_pythonhashseed():
    code = ("import sys; sys.path.insert(0, 'src'); from historical.plan import accept_draw;"
            "print(repr(accept_draw(3101, 'reddit', 42)))")
    from historical.work import PROJECT_DIR

    outs = {subprocess.run([sys.executable, "-c", code], cwd=PROJECT_DIR, capture_output=True,
                           text=True, env={"PYTHONHASHSEED": str(h), "PATH": ""}).stdout.strip()
            for h in (0, 1, 12345)}
    assert outs == {repr(accept_draw(3101, "reddit", 42))}


def test_resumen_del_plan(cfg_factory, as_of):
    summary = plan.plan_summary(cfg_factory(), as_of)
    assert summary["sources"] == {"reddit": 16_667, "facebook": 16_667, "instagram": 16_666}
    assert summary["start"] == "2026-04-26T00:00:00.000Z"
    assert [e["name"] for e in summary["events"]] == ["campana_sprite", "crisis_postobon"]


def test_pearson():
    assert pearson([1, 2, 3], [2, 4, 6]) == pytest.approx(1.0)
    assert pearson([1, 1, 1], [1, 2, 3]) is None
