"""Rama de tendencias: ``trend_windows`` en ventanas deslizantes (Lote 2.4, HU-023).

Se engancha en ``pipeline.py`` justo después de la rama enriched::

    from streaming.trends import attach_trends          # Lote 2.4
    ...
    enriched_rows = attach_enriched(unique)
    attach_trends(enriched_rows)                        # Lote 2.4

``attach_trends`` recibe lo que devuelve ``attach_enriched()``: la ``PCollection``
de filas de ``posts_enriched`` ya con NLP, o un objeto que la contenga (atributo
o clave ``rows``, ``nlp_rows``, ``enriched_rows``…). Lee las opciones del propio
pipeline, así que ``dataflow_job.sh`` no cambia.

Flujo::

    filas con NLP ─► TrendFeatures (event_time, marca, fuente; descarta > 30 min tarde)
      ─► SlidingWindows(300 s, 60 s) · AfterWatermark + tardíos cada 60 s · ACCUMULATING
      ─► CombinePerKey(marca|fuente) ─► ScoreTrends (estado: baseline de 60 ventanas)
      ─┬─► BigQuery ta_stream.trend_windows   (Dataflow)
       └─► {output_root}/trend_windows/*.jsonl (solo local)

    ta-dlq-df ─► DlqEventsToBigQuery ─► ta_stream.dlq_events   (solo Dataflow; streaming.dlq_bq)

    ScoreTrends ─(is_alert)─► TrendAlerts (cooldown) ─► Pub/Sub ta-alerts   (Lote 6.2; streaming.alerts)

Cada panel de una ventana es una fila nueva: un panel tardío lleva los conteos
acumulados. La versión vigente de cada ventana × marca × fuente es la de mayor
``n_posts`` (vista ``trend_windows_latest``, §5.5).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Tuple

import apache_beam as beam
from apache_beam.coders import PickleCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import PCollection, TaggedOutput
from apache_beam.transforms import trigger as beam_trigger
from apache_beam.transforms import window as beam_window
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.utils.timestamp import Duration, Timestamp
from apache_beam.utils.windowed_value import PaneInfoTiming

from streaming.alerts import (  # noqa: F401 (AlertOptions registra las opciones del Lote 6.2)
    ALERTS_TAG,
    DEFAULT_COOLDOWN_SECONDS,
    AlertOptions,
    TrendAlerts,
    resolve_alerts_config,
)
from streaming.bq_sink import WriteRowsToBigQuery, log_json
from streaming.trends_core import (
    TrendConfig,
    WindowAggregate,
    add_feature,
    alert_candidates,
    feature_from_row,
    format_ts,
    merge_accumulators,
    new_accumulator,
    score_window,
    split_key,
    trend_row,
)

DEFAULT_DATASET = "ta_stream"
TREND_TABLE = "trend_windows"
DLQ_TABLE = "dlq_events"
DLQ_SUBSCRIPTION = "ta-dlq-df"

_TRUE = {"1", "true", "yes", "si", "sí", "on"}
_FALSE = {"0", "false", "no", "off"}
_ROW_ATTRS = ("rows", "nlp_rows", "enriched_rows", "rows_with_nlp", "with_nlp", "enriched", "output", "result")


# --------------------------------------------------------------------------- opciones

class TrendOptions(PipelineOptions):
    """Opciones del Lote 2.4. Todas tienen valor por defecto."""

    @classmethod
    def _add_argparse_args(cls, parser):
        group = parser.add_argument_group("Tendencias (Lote 2.4)")
        group.add_argument("--trends_enabled", default="auto",
                           help="auto | true | false. Con auto manda TA_TRENDS_ENABLED (por defecto, encendida).")
        group.add_argument("--trend_windows_table", default="auto",
                           help="auto (Dataflow: proyecto:ta_stream.trend_windows) | none | tabla.")
        group.add_argument("--trends_root", default="auto",
                           help="auto (lake local: {output_root}/trend_windows) | none | carpeta local.")
        group.add_argument("--trend_window_seconds", type=int, default=300)
        group.add_argument("--trend_period_seconds", type=int, default=60)
        group.add_argument("--trend_allowed_lateness_seconds", type=int, default=1800)
        group.add_argument("--trend_late_delay_seconds", default="auto",
                           help="auto (Dataflow: 60; local: 0 = un panel por dato tardío) | segundos.")
        group.add_argument("--trend_baseline_windows", type=int, default=60)
        group.add_argument("--trend_z_threshold", type=float, default=3.0)
        group.add_argument("--trend_min_posts", type=int, default=5)
        group.add_argument("--dlq_bq_enabled", default="auto",
                           help="auto (solo Dataflow; TA_DLQ_BQ_ENABLED) | true | false.")
        group.add_argument("--dlq_events_table", default="auto",
                           help="auto (Dataflow: proyecto:ta_stream.dlq_events) | none | tabla.")
        group.add_argument("--dlq_df_subscription", default="auto",
                           help="auto (projects/{proyecto}/subscriptions/ta-dlq-df) | suscripción.")


@dataclass(frozen=True)
class TrendsRuntime:
    """Configuración resuelta de la rama (se registra como ``trends_config``)."""

    enabled: bool
    reason: str
    is_dataflow: bool
    project: str | None
    bq_table: str | None
    local_root: str | None
    dlq_enabled: bool
    dlq_reason: str
    dlq_table: str | None
    dlq_subscription: str | None
    trend: TrendConfig = field(default_factory=TrendConfig)

    def as_log(self) -> dict:
        data = asdict(self)
        data["trend"] = asdict(self.trend)
        return data


def _flag(value: Any, env_value: str | None, default: bool) -> bool:
    text = str(value if value is not None else "auto").strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    if text != "auto":
        raise ValueError(f"valor booleano no reconocido: {value!r} (use auto, true o false)")
    if env_value is not None and env_value.strip():
        env_text = env_value.strip().lower()
        if env_text in _TRUE:
            return True
        if env_text in _FALSE:
            return False
        raise ValueError(f"variable de entorno con valor no reconocido: {env_value!r}")
    return default


def _table_spec(value: str | None, project: str | None, table: str, is_dataflow: bool) -> str | None:
    text = (value or "auto").strip()
    if text.lower() in {"none", ""}:
        return None
    if text.lower() == "auto":
        if not is_dataflow:
            return None
        if not project:
            raise ValueError(f"no hay proyecto para resolver la tabla {table} (pase --project o GCP_PROJECT)")
        return f"{project}:{DEFAULT_DATASET}.{table}"
    if "." not in text:
        raise ValueError(f"tabla {text!r} inválida: use proyecto:dataset.tabla o dataset.tabla")
    return text


def _local_root(value: str | None, output_root: str | None) -> str | None:
    text = (value or "auto").strip()
    if text.lower() in {"none", ""}:
        return None
    if text.lower() == "auto":
        if output_root and not str(output_root).startswith("gs://"):
            return str(Path(output_root) / "trend_windows")
        return None
    if text.startswith("gs://"):
        raise ValueError("--trends_root solo admite carpetas locales: en GCS no se escribe trend_windows (§5.4)")
    return text


def resolve_trends_config(all_options: Mapping[str, Any], env: Mapping[str, str] | None = None) -> TrendsRuntime:
    """Resuelve la rama a partir de ``PipelineOptions.get_all_options()`` y el entorno."""
    env = os.environ if env is None else env
    runner = str(all_options.get("runner") or "")
    is_dataflow = "dataflow" in runner.lower()
    project = all_options.get("project") or env.get("GCP_PROJECT") or None
    trend = TrendConfig(
        window_seconds=int(all_options.get("trend_window_seconds") or 300),
        period_seconds=int(all_options.get("trend_period_seconds") or 60),
        allowed_lateness_seconds=int(_default(all_options.get("trend_allowed_lateness_seconds"), 1800)),
        late_delay_seconds=_late_delay(all_options.get("trend_late_delay_seconds"), is_dataflow),
        baseline_windows=int(all_options.get("trend_baseline_windows") or 60),
        z_threshold=float(all_options.get("trend_z_threshold") or 3.0),
        min_posts=int(_default(all_options.get("trend_min_posts"), 5)),
    )

    enabled = _flag(all_options.get("trends_enabled"), env.get("TA_TRENDS_ENABLED"), True)
    reason = "activa" if enabled else "deshabilitada"
    bq_table = _table_spec(all_options.get("trend_windows_table"), project, TREND_TABLE, is_dataflow)
    local_root = _local_root(all_options.get("trends_root"), all_options.get("output_root"))
    if enabled and not bq_table and not local_root:
        enabled, reason = False, "sin_salida"
    if not enabled:
        bq_table = local_root = None

    dlq_enabled = _flag(all_options.get("dlq_bq_enabled"), env.get("TA_DLQ_BQ_ENABLED"), is_dataflow)
    dlq_reason = "activa" if dlq_enabled else ("deshabilitada" if is_dataflow else "solo_dataflow")
    dlq_table = _table_spec(all_options.get("dlq_events_table"), project, DLQ_TABLE, is_dataflow) if dlq_enabled else None
    subscription = (all_options.get("dlq_df_subscription") or "auto").strip()
    if subscription.lower() == "auto":
        subscription = f"projects/{project}/subscriptions/{DLQ_SUBSCRIPTION}" if project else None
    if dlq_enabled and all_options.get("dlq_table"):
        dlq_enabled, dlq_reason = False, "dlq_table_del_lote_2_1"
    if dlq_enabled and (not dlq_table or not subscription):
        dlq_enabled, dlq_reason = False, "sin_tabla_o_suscripcion"
    if not dlq_enabled:
        dlq_table = None
        subscription = None
    return TrendsRuntime(enabled, reason, is_dataflow, project, bq_table, local_root,
                         dlq_enabled, dlq_reason, dlq_table, subscription, trend)


def _default(value, default):
    return default if value is None else value


def _late_delay(value, is_dataflow: bool) -> int:
    """Espera de los paneles tardíos.

    En Dataflow, ``AfterProcessingTime(60)`` agrupa los datos tardíos de un minuto en
    un panel. En el DirectRunner ese disparador por tiempo de proceso deja el runner
    girando sin avanzar (comprobado con Beam 2.77), así que en local cada dato tardío
    dispara su propio panel (``AfterCount(1)``).
    """
    text = str(value if value is not None else "auto").strip().lower()
    if text == "auto":
        return 60 if is_dataflow else 0
    seconds = int(text)
    if seconds and not is_dataflow:
        raise ValueError("--trend_late_delay_seconds > 0 solo funciona en Dataflow (en local use 0 o auto)")
    return seconds


def resolve_rows(source: Any) -> PCollection | None:
    """Encuentra la ``PCollection`` de filas enriquecidas en lo que devuelve ``attach_enriched()``."""
    if source is None:
        return None
    if isinstance(source, PCollection):
        return source
    if isinstance(source, Mapping):
        for name in _ROW_ATTRS:
            value = source.get(name)
            if isinstance(value, PCollection):
                return value
    for name in _ROW_ATTRS:
        try:
            value = getattr(source, name)
        except Exception:  # noqa: BLE001 - DoOutputsTuple y otros lanzan con nombres desconocidos
            continue
        if isinstance(value, PCollection):
            return value
    if isinstance(source, (tuple, list)):
        found = [item for item in source if isinstance(item, PCollection)]
        if len(found) == 1:
            return found[0]
    raise TypeError(
        f"attach_trends no reconoce {type(source).__name__}: pásele la PCollection de filas de "
        "posts_enriched con NLP (la que reciben los sumideros de enriched/ y posts_enriched). "
        "Ver src/streaming/TRENDS.md §2.")


# --------------------------------------------------------------------------- transforms

def _unwrap(element: Any) -> Any:
    if isinstance(element, Mapping):
        return element
    if isinstance(element, (tuple, list)) and len(element) == 2 and isinstance(element[1], Mapping):
        return element[1]
    for name in ("row", "value"):
        value = getattr(element, name, None)
        if isinstance(value, Mapping):
            return value
    return element


class ExtractTrendFeatures(beam.DoFn):
    """Fila enriquecida → ``(marca|fuente, rasgos)`` con el timestamp en ``event_time``.

    Descarta los posts con más de ``allowed_lateness`` de atraso respecto a la hora
    actual (contador ``trends/late_events``): siguen en ``posts_enriched``, pero no
    recalculan ventanas (HU-023, §5.12).
    """

    def __init__(self, cfg: TrendConfig, clock: Callable[[], float] = time.time):
        self._cfg = cfg
        self._clock = clock
        self._rows = Metrics.counter("trends", "trend_rows_in")
        self._late = Metrics.counter("trends", "late_events")
        self._no_brand = Metrics.counter("trends", "trend_rows_without_brand")
        self._bad = Metrics.counter("trends", "trend_rows_unrecognized")
        self._logged_bad = 0

    def process(self, element):
        self._rows.inc()
        feature, reason = feature_from_row(_unwrap(element))
        if feature is None:
            if reason == "no_brand":
                self._no_brand.inc()
                return
            self._bad.inc()
            if self._logged_bad < 5:
                self._logged_bad += 1
                log_json("trend_row_unrecognized", reason=reason, type=type(element).__name__,
                         sample=str(element)[:300])
            return
        now_ms = int(self._clock() * 1000)
        if now_ms - feature.event_ms > self._cfg.lateness_ms:
            self._late.inc()
            return
        yield beam_window.TimestampedValue(
            (feature.key, feature.as_tuple()), Timestamp(micros=feature.event_ms * 1000))


class TrendCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return new_accumulator()

    def add_input(self, accumulator, element):
        return add_feature(accumulator, element)

    def merge_accumulators(self, accumulators):
        return merge_accumulators(accumulators)

    def extract_output(self, accumulator):
        return accumulator


class AttachWindow(beam.DoFn):
    """``(clave, acumulador)`` → ``(clave, WindowAggregate)`` con el inicio y fin de la ventana.

    Marca ``complete = False`` si el panel sale antes de que la ventana termine en tiempo real:
    pasa al drenar el job, cuando la marca de agua salta al infinito y Dataflow cierra todas las
    ventanas abiertas con los datos que tenga (contador ``panes_incomplete``).
    """

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._on_time = Metrics.counter("trends", "panes_on_time")
        self._late = Metrics.counter("trends", "panes_late")
        self._incomplete = Metrics.counter("trends", "panes_incomplete")
        self._delay = Metrics.distribution("trends", "window_emit_delay_ms")

    def process(self, element, win=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        key, acc = element
        start_ms = win.start.micros // 1000
        end_ms = win.end.micros // 1000
        now_ms = int(self._clock() * 1000)
        complete = now_ms >= end_ms
        timing = getattr(pane, "timing", None)
        if timing == PaneInfoTiming.LATE:
            self._late.inc()
        else:
            self._on_time.inc()
            self._delay.update(max(0, now_ms - end_ms))
        if not complete:
            self._incomplete.inc()
        yield key, WindowAggregate.from_accumulator(key, start_ms, end_ms, acc, complete=complete)


class ScoreTrendsFn(beam.DoFn):
    """z-scores y alertas con el baseline de cada marca × fuente guardado en estado.

    Con ``emit_alerts`` (Lote 6.2), cada fila con ``is_alert`` deja además en la salida ``alerts`` un
    mensaje ``ta-alert`` por métrica que pasa el umbral (``trends_core.alert_candidates``).
    """

    BASELINE = ReadModifyWriteStateSpec("baseline", PickleCoder())

    def __init__(self, cfg: TrendConfig, emit_alerts: bool = False,
                 cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS, clock: Callable[[], float] = time.time):
        self._cfg = cfg
        self._emit_alerts = emit_alerts
        self._cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._windows = Metrics.counter("trends", "windows_emitted")
        self._alerts = Metrics.counter("trends", "alerts")

    def process(self, element, baseline=beam.DoFn.StateParam(BASELINE)):
        _key, agg = element
        state, score = score_window(baseline.read(), agg, self._cfg)
        baseline.write(state)
        row = trend_row(agg, score)
        self._windows.inc()
        if row["is_alert"]:
            self._alerts.inc()
            log_json("trend_alert", **{k: row[k] for k in (
                "window_start", "window_end", "brand", "source", "n_posts",
                "avg_sentiment", "zscore_volume", "zscore_sentiment")})
        yield row
        if self._emit_alerts and row["is_alert"]:
            detected_ms = int(self._clock() * 1000)
            for alert in alert_candidates(agg, score, self._cfg, detected_ms=detected_ms,
                                          cooldown_seconds=self._cooldown_seconds):
                yield TaggedOutput(ALERTS_TAG, alert)


class TrendWindows(beam.PTransform):
    """Filas enriquecidas → filas de ``trend_windows`` (sin escribir).

    Con ``with_alerts`` devuelve ``{"rows": …, "alerts": …}`` (las alertas, antes del *cooldown*).
    """

    def __init__(self, cfg: TrendConfig, clock: Callable[[], float] = time.time, with_alerts: bool = False,
                 cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS):
        super().__init__()
        self._cfg = cfg
        self._clock = clock
        self._with_alerts = with_alerts
        self._cooldown_seconds = cooldown_seconds

    def expand(self, rows):
        cfg = self._cfg
        late_trigger = beam_trigger.AfterProcessingTime(cfg.late_delay_seconds) if cfg.late_delay_seconds else beam_trigger.AfterCount(1)
        keyed = (
            rows
            | "Features" >> beam.ParDo(ExtractTrendFeatures(cfg, self._clock))
            | "Sliding" >> beam.WindowInto(
                beam_window.SlidingWindows(cfg.window_seconds, cfg.period_seconds),
                trigger=beam_trigger.AfterWatermark(late=late_trigger),
                accumulation_mode=beam_trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=cfg.allowed_lateness_seconds))
            | "Combine" >> beam.CombinePerKey(TrendCombineFn())
            | "WindowInfo" >> beam.ParDo(AttachWindow(self._clock))
            | "Global" >> beam.WindowInto(beam_window.GlobalWindows())
        )
        score = ScoreTrendsFn(cfg, self._with_alerts, self._cooldown_seconds, self._clock)
        if not self._with_alerts:
            return keyed | "Score" >> beam.ParDo(score).with_input_types(Tuple[str, Any])
        scored = keyed | "Score" >> beam.ParDo(score).with_input_types(Tuple[str, Any]).with_outputs(ALERTS_TAG, main="rows")
        return {"rows": scored.rows, "alerts": scored[ALERTS_TAG]}


class AppendTrendJsonl(beam.DoFn):
    """Sumidero local: ``{raíz}/dt=AAAA-MM-DD/hr=HH/part-AAAAMMDDTHH.jsonl`` por hora de ``window_start``.

    Solo para el lake local (el verificador lo lee). Cada panel es una línea; una
    ventana puede aparecer varias veces (paneles tardíos) y vale la de mayor ``n_posts``.
    """

    def __init__(self, root: str):
        self._root = root
        self._lines = Metrics.counter("trends", "local_rows")

    def process(self, row):
        start = row["window_start"]  # 2026-10-22T19:05:00.000Z
        day, hour = start[:10], start[11:13]
        folder = Path(self._root) / f"dt={day}" / f"hr={hour}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"part-{day.replace('-', '')}T{hour}.jsonl"
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._lines.inc()


# --------------------------------------------------------------------------- enganche

def _print_launcher(event: str, **fields) -> None:
    """Se ve en la terminal de quien lanza el job (no en Cloud Logging)."""
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def _all_options(pipeline_options) -> dict:
    try:
        return pipeline_options.get_all_options(drop_default=False, retain_unknown_options=True)
    except TypeError:  # versiones sin retain_unknown_options
        return pipeline_options.get_all_options()


def attach_trends(source: Any, *, options: PipelineOptions | None = None,
                  env: Mapping[str, str] | None = None, bq_sink=None, local_sink=None,
                  dlq_read=None, dlq_sink=None, alerts_sink=None, clock: Callable[[], float] = time.time) -> dict:
    """Engancha la rama de tendencias (y ``ta-dlq-df`` → ``dlq_events``) al pipeline.

    ``source`` es lo que devuelve ``attach_enriched()``. Devuelve un dict con las
    ``PCollection`` creadas (``trend_rows``, ``dlq_rows`` y, desde el Lote 6.2, ``alerts``:
    las alertas que pasan el *cooldown*) y la configuración (``config``, ``alert_config``);
    lo usan las pruebas. ``bq_sink``, ``local_sink``, ``dlq_read``, ``dlq_sink`` y
    ``alerts_sink`` (recibe el topic) son fábricas inyectables para las pruebas.
    """
    rows = resolve_rows(source)
    pipeline = rows.pipeline if rows is not None else getattr(source, "pipeline", None)
    if options is None and pipeline is not None:
        options = pipeline.options
    all_options = _all_options(options) if options is not None else {}
    runtime = resolve_trends_config(all_options, env)
    alert_rt = resolve_alerts_config(all_options, env, is_dataflow=runtime.is_dataflow, project=runtime.project,
                                     trends_enabled=runtime.enabled)
    out: dict[str, Any] = {"config": runtime, "alert_config": alert_rt, "trend_rows": None, "dlq_rows": None,
                           "alerts": None}

    if rows is None:
        if runtime.is_dataflow and runtime.enabled:
            raise RuntimeError(
                "attach_trends recibió None en Dataflow: attach_enriched() debe devolver la "
                "PCollection de filas con NLP (ver src/streaming/TRENDS.md §2) o lance con "
                "TA_TRENDS_ENABLED=false.")
        _print_launcher("trends_disabled", reason="sin_filas_enriched")
        return out

    _print_launcher("trends_config", **runtime.as_log())
    _print_launcher("alerts_config", **alert_rt.as_log())
    if runtime.enabled:
        if alert_rt.enabled:
            scored = rows | "TrendWindows" >> TrendWindows(runtime.trend, clock, with_alerts=True,
                                                         cooldown_seconds=alert_rt.cooldown_seconds)
            trend_rows = scored["rows"]
            sink = alerts_sink(alert_rt.topic) if alerts_sink else None
            out["alerts"] = scored["alerts"] | "TrendAlerts" >> TrendAlerts(alert_rt, sink=sink)
        else:
            trend_rows = rows | "TrendWindows" >> TrendWindows(runtime.trend, clock)
        if runtime.bq_table:
            sink = bq_sink(runtime.bq_table) if bq_sink else WriteRowsToBigQuery(
                runtime.bq_table, namespace="trends", failed_event="trend_bq_insert_failed",
                key_fields=("window_start", "brand", "source", "n_posts"))
            _ = trend_rows | "WriteTrendWindows" >> sink
        if runtime.local_root:
            local = local_sink(runtime.local_root) if local_sink else beam.ParDo(AppendTrendJsonl(runtime.local_root))
            _ = trend_rows | "WriteTrendJsonl" >> local
        out["trend_rows"] = trend_rows

    if runtime.dlq_enabled and pipeline is not None:
        from streaming.dlq_bq import DlqEventsToBigQuery
        out["dlq_rows"] = pipeline | "DlqEventsToBigQuery" >> DlqEventsToBigQuery(
            runtime.dlq_subscription, runtime.dlq_table, read=dlq_read,
            sink=dlq_sink(runtime.dlq_table) if dlq_sink else None)
    return out


__all__ = [
    "TrendOptions", "TrendsRuntime", "resolve_trends_config", "resolve_rows", "ExtractTrendFeatures",
    "TrendCombineFn", "AttachWindow", "ScoreTrendsFn", "TrendWindows", "AppendTrendJsonl",
    "attach_trends", "format_ts", "split_key", "AlertOptions",
]
