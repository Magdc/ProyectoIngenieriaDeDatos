"""Rama de alertas (Lote 6.2): candidatos, configuración, cooldown y la rama completa con ``TestStream``.

Como en ``test_trends_pipeline.py``, los disparadores tardíos van con ``late_delay_seconds = 0`` y los
contadores se prueban llamando a los DoFn directamente.
"""

from __future__ import annotations

import logging

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_stream import TestStream

from common import alert_message as am
from streaming.alerts import AlertCooldownFn, AlertsRuntime, TrendAlerts, cooldown_step, resolve_alerts_config
from streaming.trends import TrendWindows, attach_trends
from streaming.trends_core import TrendConfig, TrendScore, WindowAggregate, alert_candidates, format_ts, score_window

T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)
_COLLECTED: dict[str, list] = {}


class _Collect(beam.DoFn):
    def __init__(self, name):
        self._name = name

    def process(self, element):
        _COLLECTED.setdefault(self._name, []).append(element)


class _State:
    def __init__(self):
        self.value = None

    def read(self):
        return self.value

    def write(self, value):
        self.value = value


class _Counter:
    def __init__(self):
        self.value = 0

    def inc(self, n=1):
        self.value += n


@pytest.fixture(autouse=True)
def _clean():
    _COLLECTED.clear()
    yield
    _COLLECTED.clear()


def _row(ms, brand="postobon", source="instagram", label="positive", score=0.9, eid=None):
    return {"event_id": eid or f"e{ms}{brand}{source}", "event_time": format_ts(ms), "brand_primary": brand,
            "source": source, "sentiment_label": label, "sentiment_score": score, "emotion_label": "joy",
            "text": "x", "user_hash": "a" * 64}


def _agg(n, sentiment_sum, start=T0):
    return WindowAggregate("postobon", "instagram", start, start + 300_000, n, 0, n, 0, sentiment_sum, n)


def test_cooldown_step():
    cd = 900_000
    assert cooldown_step(None, T0, cd) == (True, T0)
    assert cooldown_step(T0, T0 + 60_000, cd) == (False, T0)
    assert cooldown_step(T0, T0 - 840_000, cd) == (False, T0)          # panel tardío de una ventana anterior
    assert cooldown_step(T0, T0 + 900_000, cd) == (True, T0 + 900_000)
    assert cooldown_step(T0 + 900_000, T0, cd) == (True, T0 + 900_000)  # una más vieja sale, el estado no retrocede


def test_configuracion():
    on = resolve_alerts_config({}, {}, is_dataflow=True, project="p", trends_enabled=True)
    assert on == AlertsRuntime(True, "activa", "projects/p/topics/ta-alerts", 900)
    off = resolve_alerts_config({}, {}, is_dataflow=False, project="p", trends_enabled=True)
    assert (off.enabled, off.reason, off.topic) == (False, "solo_dataflow", None)
    env = resolve_alerts_config({}, {"TA_ALERTS_ENABLED": "false"}, is_dataflow=True, project="p", trends_enabled=True)
    assert (env.enabled, env.reason) == (False, "deshabilitada")
    local = resolve_alerts_config({"alerts_enabled": "true", "alerts_topic": "projects/x/topics/t", "alert_cooldown_seconds": 60},
                                  {}, is_dataflow=False, project=None, trends_enabled=True)
    assert local == AlertsRuntime(True, "activa", "projects/x/topics/t", 60)
    assert resolve_alerts_config({}, {}, is_dataflow=True, project="p", trends_enabled=False).reason == "sin_tendencias"
    assert resolve_alerts_config({}, {}, is_dataflow=True, project=None, trends_enabled=True).reason == "sin_topic"
    with pytest.raises(ValueError, match="alerts_topic"):
        resolve_alerts_config({"alerts_topic": "ta-alerts"}, {}, is_dataflow=True, project="p", trends_enabled=True)
    with pytest.raises(ValueError, match="cooldown"):
        resolve_alerts_config({"alert_cooldown_seconds": -1}, {}, is_dataflow=True, project="p", trends_enabled=True)


def test_candidatos_por_metrica():
    cfg = TrendConfig(baseline_windows=3, min_posts=1)
    state = None
    for k in range(9):  # 8 ventanas de 10 posts con sentimiento 0,5
        state, score = score_window(state, _agg(10 + k % 2, 5.0 + 0.5 * (k % 2), T0 + k * 60_000), cfg)
    assert score.warm and score.volume_mean is not None and score.sentiment_mean is not None
    spike = _agg(60, -30.0, T0 + 9 * 60_000)
    state, score = score_window(state, spike, cfg)
    alerts = alert_candidates(spike, score, cfg, detected_ms=T0 + 15 * 60_000, cooldown_seconds=900)
    assert [a["alert_type"] for a in alerts] == ["volume_up", "sentiment_down"]
    assert all(am.alert_problems(a) == [] for a in alerts)
    vol = alerts[0]
    assert vol["value"] == 60 and vol["baseline_mean"] == pytest.approx(score.volume_mean, abs=1e-6)
    assert vol["zscore"] == score.zscore_volume and vol["baseline_windows"] == 3
    quiet = TrendScore(1.0, 0.5, False, 60, True, 10.0, 1.0, 0.5, 0.1)
    assert alert_candidates(spike, quiet, cfg, detected_ms=0, cooldown_seconds=900) == []
    only_volume = TrendScore(9.0, 1.0, True, 60, True, 10.0, 1.0, 0.5, 0.1)
    assert [a["alert_type"] for a in alert_candidates(spike, only_volume, cfg, detected_ms=T0, cooldown_seconds=900)] == ["volume_up"]


def test_cooldown_fn_contadores_y_logs(caplog):
    caplog.set_level(logging.INFO)
    dofn = AlertCooldownFn(900)
    dofn._published, dofn._suppressed = _Counter(), _Counter()
    state = _State()

    def alert(minutes):
        return am.build_alert(metric="volume", brand="postobon", source="instagram",
                              window_start_ms=T0 + minutes * 60_000, window_end_ms=T0 + minutes * 60_000 + 300_000,
                              value=60, baseline_mean=10.0, baseline_std=1.0, zscore=50.0, z_threshold=3.0,
                              baseline_windows=60, n_posts=60, avg_sentiment=0.2, n_negative=5, n_positive=30,
                              detected_ms=T0, cooldown_seconds=900)

    out = []
    for minutes in (0, 1, 2, 14, 15, 16):
        out += list(dofn.process(("postobon|instagram|volume_up", alert(minutes)), last=state))
    assert [am.to_ms(a["window_end"]) - T0 for a in out] == [300_000, 1_200_000]
    assert dofn._published.value == 2 and dofn._suppressed.value == 4
    assert "trend_alert_published" in caplog.text and "trend_alert_suppressed" in caplog.text


def test_rama_completa_ola_con_cooldown():
    """Baseline de 36 min, ola de 30 posts negativos en un minuto: varias ventanas con is_alert, un mensaje por tipo.

    Con un baseline largo (30 ventanas) la ola sigue alertando en las ventanas siguientes que la contienen,
    como en Dataflow con 60; el cooldown deja salir una sola por marca × fuente × tipo.
    """
    # El reloj es fijo (fin de la ola): con 30 min de lateness, los posts del principio se descartarían.
    cfg = TrendConfig(late_delay_seconds=0, baseline_windows=30, min_posts=1, allowed_lateness_seconds=3 * 3600)
    stream = TestStream().advance_watermark_to(T0 / 1000)
    for minute in range(36):
        stream = stream.add_elements([_row(T0 + minute * 60_000 + 5000, eid=f"base{minute}")])
        stream = stream.advance_watermark_to((T0 + (minute + 1) * 60_000) / 1000)
    spike = T0 + 36 * 60_000
    stream = stream.add_elements([_row(spike + 1000 + i, label="negative", eid=f"s{i}") for i in range(30)])
    stream = stream.advance_watermark_to_infinity()
    clock = lambda: (spike + 6 * 60_000) / 1000  # noqa: E731 - todas las ventanas de la ola ya terminaron

    p = beam.Pipeline(options=PipelineOptions(streaming=True, runner="BundleBasedDirectRunner"))
    scored = p | stream | TrendWindows(cfg, clock=clock, with_alerts=True, cooldown_seconds=900)
    _ = scored["rows"] | "Rows" >> beam.ParDo(_Collect("rows"))
    runtime = AlertsRuntime(True, "activa", "projects/p/topics/ta-alerts", 900)
    published = scored["alerts"] | TrendAlerts(runtime, sink=beam.ParDo(_Collect("pubsub")))
    _ = published | "Published" >> beam.ParDo(_Collect("published"))
    p.run().wait_until_finish()

    alert_rows = [r for r in _COLLECTED["rows"] if r["is_alert"]]
    assert len({r["window_start"] for r in alert_rows}) >= 3, "la ola alerta en varias ventanas deslizantes"
    messages = _COLLECTED["pubsub"]
    decoded = [am.decode(m.data) for m in messages]
    types = sorted(a["alert_type"] for a in decoded)
    assert types == sorted(set(types)) and "volume_up" in types, types
    for m, a in zip(messages, decoded):
        assert m.attributes == am.attributes_for(a) and a["brand"] == "postobon" and a["source"] == "instagram"
        assert a["detected_at"] == format_ts(spike + 6 * 60_000)
    assert {a["window_end"] for a in decoded} <= {r["window_end"] for r in alert_rows}
    assert len(_COLLECTED["published"]) == len(messages)


def test_attach_trends_en_dataflow_engancha_las_alertas():
    stream = TestStream().advance_watermark_to(T0 / 1000).add_elements([_row(T0 + 1000)]).advance_watermark_to_infinity()
    p = beam.Pipeline(options=PipelineOptions(streaming=True, runner="BundleBasedDirectRunner"))
    topics = []

    def alerts_sink(topic):
        topics.append(topic)
        return beam.ParDo(_Collect("pubsub"))

    out = attach_trends(p | stream, options=PipelineOptions(["--runner", "DataflowRunner", "--project", "proj-x"]), env={},
                        bq_sink=lambda t: beam.ParDo(_Collect("bq")), dlq_read=beam.Create([]),
                        dlq_sink=lambda t: beam.ParDo(_Collect("dlq")), alerts_sink=alerts_sink)
    assert out["alert_config"] == AlertsRuntime(True, "activa", "projects/proj-x/topics/ta-alerts", 900)
    assert out["alerts"] is not None and topics == ["projects/proj-x/topics/ta-alerts"]
    p2 = beam.Pipeline(options=PipelineOptions(streaming=True, runner="BundleBasedDirectRunner"))
    out2 = attach_trends(p2 | "Rows" >> beam.Create([_row(T0 + 1000)]),
                         options=PipelineOptions(["--runner", "DataflowRunner", "--project", "proj-x",
                                                  "--alerts_enabled", "false", "--dlq_bq_enabled", "false"]),
                         env={}, bq_sink=lambda t: beam.ParDo(_Collect("bq")))
    assert out2["alerts"] is None and out2["alert_config"].reason == "deshabilitada"
    local = attach_trends(beam.Pipeline() | "Local" >> beam.Create([_row(T0)]), options=PipelineOptions([]), env={})
    assert local["alerts"] is None and local["alert_config"].reason == "solo_dataflow"
