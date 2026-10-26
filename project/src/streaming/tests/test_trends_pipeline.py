"""Rama de tendencias en el DirectRunner con ``TestStream`` (Lote 2.4).

Los disparadores tardíos van con ``late_delay_seconds = 0`` (``AfterCount(1)``):
el DirectRunner de Beam 2.77 no avanza con ``AfterProcessingTime`` en paneles
tardíos (ver ``streaming.trends._late_delay``). En Dataflow se usa 60 s.

Los contadores se prueban llamando a los DoFn directamente: en modo streaming,
el DirectRunner pierde las métricas de algunos bundles (comprobado con Beam 2.77),
así que no son fiables en estas pruebas.
"""

from __future__ import annotations

import json
from pathlib import Path

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_stream import TestStream

from streaming.trends import TrendWindows, attach_trends
from streaming.trends_core import TrendConfig, format_ts, row_problems

T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)
NOW_S = (T0 + 10 * 60_000) / 1000  # reloj fijo: 10 min después de T0
_COLLECTED: dict[str, list] = {}


def _clock():
    return NOW_S


class _Collect(beam.DoFn):
    def __init__(self, name):
        self._name = name

    def process(self, element):
        _COLLECTED.setdefault(self._name, []).append(element)


def _row(ms, brand="postobon", source="reddit", label="positive", score=0.9, emotion="joy", eid=None):
    return {"event_id": eid or f"e{ms}{brand}{source}", "event_time": format_ts(ms), "brand_primary": brand,
            "source": source, "sentiment_label": label, "sentiment_score": score, "emotion_label": emotion,
            "text": "x", "user_hash": "a" * 64}


def _options():
    return PipelineOptions(streaming=True, runner="BundleBasedDirectRunner")


@pytest.fixture(autouse=True)
def _clean():
    _COLLECTED.clear()
    yield
    _COLLECTED.clear()


def _run_windows(stream, cfg):
    p = beam.Pipeline(options=_options())
    rows = p | stream | TrendWindows(cfg, clock=_clock)
    _ = rows | beam.ParDo(_Collect("rows"))
    result = p.run()
    result.wait_until_finish()
    return result, _COLLECTED.get("rows", [])


def _by_window(rows, brand="postobon", source="reddit"):
    out: dict[str, list[dict]] = {}
    for row in rows:
        if row["brand"] == brand and row["source"] == source:
            out.setdefault(row["window_start"], []).append(row)
    return out


def test_ventanas_a_tiempo_tardias_y_descartadas():
    cfg = TrendConfig(late_delay_seconds=0, baseline_windows=3, min_posts=1)
    stream = (TestStream()
              .advance_watermark_to(T0 / 1000)
              .add_elements([_row(T0 + 1000), _row(T0 + 2000, label="negative", score=0.5, emotion="anger")])
              .advance_watermark_to(T0 / 1000 + 400)
              # Tardío dentro de los 30 min (actualiza las 5 ventanas) y otro con más de 30 min
              # de atraso respecto al reloj (no recalcula ventanas).
              .add_elements([_row(T0 + 3000, label="neutral", score=0.7, emotion=None),
                             _row(T0 - 40 * 60_000)])
              .advance_watermark_to_infinity())
    result, rows = _run_windows(stream, cfg)
    windows = _by_window(rows)
    assert len(windows) == 5
    for start, versions in windows.items():
        counts = sorted(r["n_posts"] for r in versions)
        assert counts == [2, 3], (start, counts)
        latest = max(versions, key=lambda r: r["n_posts"])
        assert (latest["n_negative"], latest["n_positive"]) == (1, 1)
        assert latest["avg_sentiment"] == pytest.approx((0.9 - 0.5 + 0.0) / 3, abs=1e-6)
        emotions = json.loads(latest["emotion_counts"])
        assert emotions["joy"] == 1 and emotions["anger"] == 1 and sum(emotions.values()) == 2
        assert row_problems(latest) == []
    assert len(rows) == 10  # 5 paneles a tiempo + 5 tardíos


def test_claves_por_marca_y_fuente_y_filas_sin_marca():
    cfg = TrendConfig(late_delay_seconds=0)
    stream = (TestStream()
              .advance_watermark_to(T0 / 1000)
              .add_elements([_row(T0 + 1000), _row(T0 + 1000, brand="pepsi"),
                             _row(T0 + 1000, source="instagram"), _row(T0 + 1000, brand=None),
                             "no es una fila"])
              .advance_watermark_to_infinity())
    result, rows = _run_windows(stream, cfg)
    keys = {(r["brand"], r["source"]) for r in rows}
    assert keys == {("postobon", "reddit"), ("pepsi", "reddit"), ("postobon", "instagram")}
    assert len(rows) == 15


def test_alerta_de_punta_a_punta_tras_el_baseline():
    cfg = TrendConfig(late_delay_seconds=0, baseline_windows=3, min_posts=1)
    stream = TestStream().advance_watermark_to(T0 / 1000)
    # Un post por minuto durante 12 minutos y luego una ola de 30 en un minuto.
    for minute in range(12):
        stream = stream.add_elements([_row(T0 + minute * 60_000 + 5000, eid=f"base{minute}")])
        stream = stream.advance_watermark_to((T0 + (minute + 1) * 60_000) / 1000)
    spike = T0 + 12 * 60_000
    stream = stream.add_elements([_row(spike + 1000 + i, label="negative", score=0.9, eid=f"s{i}") for i in range(30)])
    stream = stream.advance_watermark_to_infinity()
    clock_late = (spike + 6 * 60_000) / 1000  # todas las ventanas de la ola ya terminaron

    p = beam.Pipeline(options=_options())
    rows = p | stream | TrendWindows(cfg, clock=lambda: clock_late)
    _ = rows | beam.ParDo(_Collect("rows"))
    result = p.run()
    result.wait_until_finish()
    rows = _COLLECTED["rows"]
    alerts = [r for r in rows if r["is_alert"]]
    assert alerts, "la ola debería generar al menos una alerta"
    assert all(r["n_posts"] >= 30 for r in alerts)
    assert all(r["zscore_volume"] > 3 for r in alerts)
    first_start = min(r["window_start"] for r in rows)
    # Arranque en frío: ninguna alerta antes de min_start + ventana + baseline.
    cold_until = format_ts(T0 - 240_000 + 300_000 + 3 * 60_000)
    assert all(r["window_start"] >= cold_until for r in alerts), (first_start, cold_until)


def test_drenado_no_genera_alertas_falsas():
    """Al drenar, la marca de agua salta al infinito y las ventanas abiertas salen con datos parciales.

    Reproduce lo visto en la validación (v2.26): 48 alertas del job anterior en las ventanas
    que se cerraron al drenarlo. Las ventanas incompletas no deben alertar.
    """
    cfg = TrendConfig(late_delay_seconds=0, baseline_windows=3, min_posts=1)
    stream = TestStream().advance_watermark_to(T0 / 1000)
    for minute in range(15):
        stream = stream.add_elements([_row(T0 + minute * 60_000 + 1000 + i * 5000, eid=f"m{minute}-{i}")
                                      for i in range(10)])
        stream = stream.advance_watermark_to((T0 + (minute + 1) * 60_000) / 1000)
    drain_at = T0 + 15 * 60_000 + 5000
    stream = stream.add_elements([_row(drain_at - 2000, eid="ultimo")]).advance_watermark_to_infinity()
    p = beam.Pipeline(options=_options())
    _ = p | stream | TrendWindows(cfg, clock=lambda: drain_at / 1000) | beam.ParDo(_Collect("rows"))
    p.run().wait_until_finish()
    rows = _COLLECTED["rows"]
    partial = [r for r in rows if r["window_end"] > format_ts(drain_at)]
    assert partial and min(r["n_posts"] for r in partial) < 50
    assert any(r["zscore_volume"] is not None and r["zscore_volume"] <= -3 for r in partial)
    assert not any(r["is_alert"] for r in rows)


def test_attach_trends_local_escribe_jsonl(tmp_path: Path):
    stream = (TestStream()
              .advance_watermark_to(T0 / 1000)
              .add_elements([_row(T0 + 1000), _row(T0 + 2000, brand="sprite", source="facebook")])
              .advance_watermark_to_infinity())
    p = beam.Pipeline(options=_options())
    rows = p | stream
    launch = PipelineOptions(["--output_root", str(tmp_path)])
    out = attach_trends(rows, options=launch, env={}, clock=_clock)
    assert out["config"].local_root == str(tmp_path / "trend_windows")
    assert out["config"].bq_table is None and out["dlq_rows"] is None
    p.run().wait_until_finish()
    files = sorted((tmp_path / "trend_windows").rglob("*.jsonl"))
    assert files and all("dt=" in str(f) and "hr=" in str(f) for f in files)
    lines = [json.loads(line) for f in files for line in f.read_text().splitlines()]
    assert len(lines) == 10
    assert all(row_problems(r) == [] for r in lines)


def test_attach_trends_en_dataflow_usa_bigquery_y_la_dlq():
    from apache_beam.io.gcp.pubsub import PubsubMessage

    stream = (TestStream()
              .advance_watermark_to(T0 / 1000)
              .add_elements([_row(T0 + 1000)])
              .advance_watermark_to_infinity())
    dlq_messages = [
        PubsubMessage(b'{"truncado": ', {"dlq_error_type": "json", "dlq_source": "reddit",
                                         "dlq_error_detail": "Expecting value", "dlq_received_at": format_ts(T0)}),
        PubsubMessage(b"\xff\xfe x", {"dlq_error_type": "encoding", "dlq_source": "facebook",
                                       "dlq_received_at": format_ts(T0 + 1)}),
    ]
    p = beam.Pipeline(options=_options())
    rows = p | stream
    launch = PipelineOptions(["--runner", "DataflowRunner", "--project", "proj-x"])
    seen_tables = []

    def bq_sink(table):
        seen_tables.append(table)
        return beam.ParDo(_Collect("bq"))

    def dlq_sink(table):
        seen_tables.append(table)
        return beam.ParDo(_Collect("dlq"))

    out = attach_trends(rows, options=launch, env={}, clock=_clock, bq_sink=bq_sink,
                        dlq_read=beam.Create(dlq_messages), dlq_sink=dlq_sink)
    cfg = out["config"]
    assert cfg.is_dataflow and cfg.trend.late_delay_seconds == 60
    assert cfg.local_root is None
    assert seen_tables == ["proj-x:ta_stream.trend_windows", "proj-x:ta_stream.dlq_events"]
    assert cfg.dlq_subscription == "projects/proj-x/subscriptions/ta-dlq-df"
    # En el DirectRunner no se puede usar AfterProcessingTime: la prueba solo arma el grafo de
    # Dataflow y luego corre la DLQ por separado.
    p2 = beam.Pipeline(options=_options())
    out2 = attach_trends(p2 | "Rows" >> beam.Create([_row(T0 + 1000)]),
                         options=PipelineOptions(["--runner", "DataflowRunner", "--project", "proj-x",
                                                  "--trends_enabled", "false"]),
                         env={}, dlq_read=beam.Create(dlq_messages), dlq_sink=dlq_sink)
    assert out2["trend_rows"] is None and out2["dlq_rows"] is not None
    p2.run().wait_until_finish()
    dlq = sorted(_COLLECTED["dlq"], key=lambda r: r["received_at"])
    assert [r["error_type"] for r in dlq] == ["json", "encoding"]
    assert dlq[1]["raw_message"] == "\\xff\\xfe x"
    assert dlq[0]["source"] == "reddit" and dlq[0]["error_detail"] == "Expecting value"


def test_attach_trends_sin_filas():
    out = attach_trends(None, options=PipelineOptions([]), env={})
    assert out["trend_rows"] is None and out["dlq_rows"] is None
    with pytest.raises(RuntimeError, match="attach_enriched"):
        attach_trends(None, options=PipelineOptions(["--runner", "DataflowRunner", "--project", "p"]), env={})
