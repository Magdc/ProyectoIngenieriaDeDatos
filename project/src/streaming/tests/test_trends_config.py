"""Configuración, enganche y DoFn de la rama de tendencias, sin correr un pipeline (Lote 2.4)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window as beam_window
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfoTiming

from streaming import trends
from streaming.dlq_bq import DlqMessageToRow, decode_body, dlq_row
from streaming.trends import (
    AppendTrendJsonl,
    AttachWindow,
    ExtractTrendFeatures,
    ScoreTrendsFn,
    TrendOptions,
    resolve_rows,
    resolve_trends_config,
)
from streaming.trends_core import TrendConfig, WindowAggregate, format_ts, new_accumulator, add_feature

T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)


class _Counter:
    def __init__(self):
        self.value = 0

    def inc(self, n=1):
        self.value += n

    def update(self, _):
        self.value += 1


def _stub_counters(dofn):
    for name, value in list(vars(dofn).items()):
        if hasattr(value, "inc") or hasattr(value, "update"):
            setattr(dofn, name, _Counter())
    return dofn


def _opts(*args):
    # --output_root es una opción del Lote 2.1; en estas pruebas no está registrada.
    return PipelineOptions(list(args)).get_all_options(retain_unknown_options=True)


# --------------------------------------------------------------------------- opciones

def test_opciones_por_defecto():
    view = PipelineOptions([]).view_as(TrendOptions)
    assert view.trends_enabled == "auto" and view.trend_windows_table == "auto"
    assert view.trend_window_seconds == 300 and view.trend_period_seconds == 60
    assert view.trend_late_delay_seconds == "auto" and view.dlq_bq_enabled == "auto"


def test_dataflow_resuelve_tablas_y_suscripcion():
    cfg = resolve_trends_config(_opts("--runner", "DataflowRunner", "--project", "proj"), env={})
    assert cfg.enabled and cfg.is_dataflow
    assert cfg.bq_table == "proj:ta_stream.trend_windows"
    assert cfg.local_root is None
    assert cfg.dlq_enabled and cfg.dlq_table == "proj:ta_stream.dlq_events"
    assert cfg.dlq_subscription == "projects/proj/subscriptions/ta-dlq-df"
    assert cfg.trend.late_delay_seconds == 60


def test_dataflow_toma_el_proyecto_del_entorno():
    cfg = resolve_trends_config({"runner": "DataflowRunner"}, env={"GCP_PROJECT": "otro"})
    assert cfg.bq_table == "otro:ta_stream.trend_windows"


def test_local_escribe_en_el_lake_y_no_en_bigquery(tmp_path):
    cfg = resolve_trends_config(_opts("--output_root", str(tmp_path)), env={})
    assert cfg.enabled and not cfg.is_dataflow
    assert cfg.local_root == str(tmp_path / "trend_windows")
    assert cfg.bq_table is None
    assert not cfg.dlq_enabled and cfg.dlq_reason == "solo_dataflow"
    assert cfg.trend.late_delay_seconds == 0


def test_sin_salida_se_apaga():
    cfg = resolve_trends_config(_opts(), env={})
    assert not cfg.enabled and cfg.reason == "sin_salida"


def test_output_root_en_gcs_no_escribe_jsonl():
    cfg = resolve_trends_config(_opts("--output_root", "gs://b"), env={})
    assert cfg.local_root is None


@pytest.mark.parametrize("env,flag,expected", [
    ({"TA_TRENDS_ENABLED": "false"}, "auto", False),
    ({"TA_TRENDS_ENABLED": "true"}, "auto", True),
    ({}, "false", False),
    ({"TA_TRENDS_ENABLED": "true"}, "false", False),
])
def test_encendido_por_opcion_y_entorno(env, flag, expected):
    cfg = resolve_trends_config(_opts("--runner", "DataflowRunner", "--project", "p", "--trends_enabled", flag), env=env)
    assert cfg.enabled is expected


def test_dlq_se_apaga_por_entorno_o_por_dlq_table_del_2_1():
    cfg = resolve_trends_config(_opts("--runner", "DataflowRunner", "--project", "p"), env={"TA_DLQ_BQ_ENABLED": "false"})
    assert not cfg.dlq_enabled and cfg.dlq_reason == "deshabilitada"
    cfg = resolve_trends_config({"runner": "DataflowRunner", "project": "p", "dlq_table": "p:ta_stream.dlq_events"}, env={})
    assert not cfg.dlq_enabled and cfg.dlq_reason == "dlq_table_del_lote_2_1"


def test_tablas_explicitas_y_none():
    cfg = resolve_trends_config(_opts("--runner", "DataflowRunner", "--project", "p",
                                      "--trend_windows_table", "q:ds.tw", "--dlq_events_table", "none"), env={})
    assert cfg.bq_table == "q:ds.tw"
    assert not cfg.dlq_enabled


@pytest.mark.parametrize("args,fragment", [
    (("--trend_windows_table", "sin_punto", "--runner", "DataflowRunner", "--project", "p"), "inválida"),
    (("--trends_root", "gs://b/x"), "carpetas locales"),
    (("--trend_late_delay_seconds", "30"), "solo funciona en Dataflow"),
    (("--trend_window_seconds", "290", "--output_root", "/tmp/x"), "múltiplo"),
    (("--trends_enabled", "quizas", "--output_root", "/tmp/x"), "no reconocido"),
])
def test_configuraciones_invalidas(args, fragment):
    with pytest.raises(ValueError, match=fragment):
        resolve_trends_config(_opts(*args), env={})


def test_sin_proyecto_en_dataflow_falla():
    with pytest.raises(ValueError, match="proyecto"):
        resolve_trends_config({"runner": "DataflowRunner"}, env={})


def test_as_log_es_serializable(tmp_path):
    cfg = resolve_trends_config(_opts("--output_root", str(tmp_path)), env={})
    json.dumps(cfg.as_log())


# --------------------------------------------------------------------------- resolve_rows

def test_resolve_rows_acepta_varias_formas():
    p = beam.Pipeline()
    pc = p | beam.Create([1])
    assert resolve_rows(pc) is pc
    assert resolve_rows({"rows": pc}) is pc
    assert resolve_rows(SimpleNamespace(nlp_rows=pc)) is pc
    assert resolve_rows((pc, "otra cosa")) is pc
    assert resolve_rows(None) is None


def test_resolve_rows_rechaza_lo_desconocido():
    with pytest.raises(TypeError, match="TRENDS.md"):
        resolve_rows(SimpleNamespace(otra=1))


# --------------------------------------------------------------------------- DoFn

def _row(**over):
    row = {"event_id": "e", "event_time": format_ts(T0 + 1000), "brand_primary": "postobon",
           "source": "reddit", "sentiment_label": "positive", "sentiment_score": 0.9, "emotion_label": "joy"}
    row.update(over)
    return row


def test_extract_features_cuenta_y_marca_el_timestamp():
    cfg = TrendConfig()
    dofn = _stub_counters(ExtractTrendFeatures(cfg, clock=lambda: (T0 + 600_000) / 1000))
    out = list(dofn.process(_row()))
    assert len(out) == 1
    assert out[0].value == ("postobon|reddit", (T0 + 1000, "positive", 0.9, "joy"))
    assert out[0].timestamp == Timestamp(micros=(T0 + 1000) * 1000)
    assert list(dofn.process(_row(event_time=format_ts(T0 - 40 * 60_000)))) == []
    assert list(dofn.process(_row(brand_primary=None))) == []
    assert list(dofn.process("basura")) == []
    assert list(dofn.process(("clave", _row()))) != []  # pares (clave, fila)
    assert dofn._late.value == 1
    assert dofn._no_brand.value == 1
    assert dofn._bad.value == 1
    assert dofn._rows.value == 5


def test_attach_window_y_paneles():
    dofn = _stub_counters(AttachWindow(clock=lambda: (T0 + 330_000) / 1000))
    acc = add_feature(new_accumulator(), (T0 + 1000, "positive", 0.5, "joy"))
    win = beam_window.IntervalWindow(T0 / 1000, (T0 + 300_000) / 1000)
    on_time = SimpleNamespace(timing=PaneInfoTiming.ON_TIME)
    late = SimpleNamespace(timing=PaneInfoTiming.LATE)
    [(key, agg)] = list(dofn.process(("postobon|reddit", acc), win=win, pane=on_time))
    assert key == "postobon|reddit"
    assert (agg.window_start_ms, agg.window_end_ms, agg.n_posts) == (T0, T0 + 300_000, 1)
    assert agg.complete is True
    list(dofn.process(("postobon|reddit", acc), win=win, pane=late))
    assert dofn._on_time.value == 1 and dofn._late.value == 1 and dofn._delay.value == 1
    assert dofn._incomplete.value == 0


def test_attach_window_marca_incompleta_la_ventana_emitida_antes_de_terminar():
    dofn = _stub_counters(AttachWindow(clock=lambda: (T0 + 200_000) / 1000))  # drenado a mitad de ventana
    acc = add_feature(new_accumulator(), (T0 + 1000, "positive", 0.5, "joy"))
    win = beam_window.IntervalWindow(T0 / 1000, (T0 + 300_000) / 1000)
    [(_key, agg)] = list(dofn.process(("postobon|reddit", acc), win=win,
                                      pane=SimpleNamespace(timing=PaneInfoTiming.ON_TIME)))
    assert agg.complete is False and dofn._incomplete.value == 1


class _State:
    def __init__(self):
        self.value = None

    def read(self):
        return self.value

    def write(self, value):
        self.value = value


def test_score_guarda_el_baseline_en_estado():
    cfg = TrendConfig(baseline_windows=2, min_posts=1)
    dofn = _stub_counters(ScoreTrendsFn(cfg))
    state = _State()
    rows = []
    for i, n in enumerate([5, 5, 5, 5, 5, 5, 5, 40]):
        agg = WindowAggregate("postobon", "reddit", T0 + i * 60_000, T0 + i * 60_000 + 300_000,
                              n, 0, 0, n, 0.0, n, {})
        rows.extend(dofn.process(("postobon|reddit", agg), baseline=state))
    assert rows[0]["zscore_volume"] is None
    assert rows[-1]["is_alert"] is True
    assert dofn._alerts.value == 1 and dofn._windows.value == 8
    assert state.value["max_start"] == T0 + 7 * 60_000


def test_sumidero_local_por_hora_de_la_ventana(tmp_path: Path):
    dofn = _stub_counters(AppendTrendJsonl(str(tmp_path)))
    row = {"window_start": "2026-10-22T19:05:00.000Z", "n_posts": 1}
    dofn.process(row)
    dofn.process(dict(row, n_posts=2))
    path = tmp_path / "dt=2026-10-22" / "hr=19" / "part-20261022T19.jsonl"
    lines = path.read_text().splitlines()
    assert [json.loads(line)["n_posts"] for line in lines] == [1, 2]


# --------------------------------------------------------------------------- DLQ

def test_dlq_row_completo():
    row, warnings = dlq_row(b'{"x": ', {"dlq_error_type": "json", "dlq_source": "instagram",
                                        "dlq_error_detail": "Expecting value",
                                        "dlq_received_at": "2026-10-22T19:05:00.123Z"}, 0)
    assert row == {"received_at": "2026-10-22T19:05:00.123Z", "source": "instagram", "error_type": "json",
                   "error_detail": "Expecting value", "raw_message": '{"x": '}
    assert warnings == []


def test_dlq_row_con_atributos_faltantes():
    row, warnings = dlq_row(b"\xff ok", {"source": "reddit"}, T0)
    assert row["received_at"] == format_ts(T0)
    assert row["error_type"] == "unknown" and row["source"] == "reddit"
    assert row["raw_message"] == "\\xff ok"
    assert set(warnings) == {"received_at_fallback", "rows_without_type"}


def test_dlq_tipo_inesperado_se_conserva():
    row, warnings = dlq_row(b"x", {"dlq_error_type": "otro", "dlq_received_at": format_ts(T0)}, 0)
    assert row["error_type"] == "otro" and warnings == ["unexpected_error_type"]


def test_dlq_dofn_cuenta():
    from apache_beam.io.gcp.pubsub import PubsubMessage
    dofn = _stub_counters(DlqMessageToRow(clock=lambda: T0 / 1000))
    out = list(dofn.process(PubsubMessage(b"x", {})))
    assert out[0]["received_at"] == format_ts(T0)
    assert dofn._rows.value == 1 and dofn._fallback.value == 1 and dofn._untyped.value == 1


def test_decode_body():
    assert decode_body(None) == "" and decode_body("ya") == "ya" and decode_body(b"\xc3\xa1") == "á"


def test_el_modulo_exporta_attach_trends():
    assert callable(trends.attach_trends)
