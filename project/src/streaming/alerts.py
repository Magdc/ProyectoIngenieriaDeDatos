"""Rama de alertas: ``trend_windows.is_alert`` → *cooldown* → ``ta-alerts`` (Lote 6.2, HU-039).

Se engancha sola desde ``attach_trends`` (``streaming.trends``): no cambia ``pipeline.py`` ni
``dataflow_job.sh``. Flujo::

    ScoreTrends ─(salida «alerts»: un mensaje ta-alert por métrica con |z| ≥ umbral, §5.3)
      ─► AlertKey (marca|fuente|alert_type) ─► Cooldown (estado: fin de la última ventana avisada)
      ─► Reshuffle ─► ToPubsub ─► Pub/Sub ta-alerts (atributos de §5.3)

* *Cooldown* de 15 min por marca × fuente × tipo (§5.12): una alerta se publica si su ventana termina a
  15 min o más de la última publicada para esa clave. Las ventanas deslizantes se solapan, así que una
  ola de un minuto deja hasta 5 ventanas con ``is_alert``; solo la primera sale a ``ta-alerts``. Se mide
  en tiempo del evento (``window_end``), así que da lo mismo en qué orden lleguen los paneles.
* El ``Reshuffle`` separa la publicación del resto del grafo: si Pub/Sub falla, Dataflow reintenta ese
  paso sin frenar ``trend_windows``.
* Solo en Dataflow por defecto (``--alerts_enabled auto``); el estado vive en el job (uno nuevo arranca
  sin *cooldown* previo).

Métricas (espacio ``alerts``): ``alerts_published``, ``alerts_suppressed``. Logs de los workers:
``trend_alert_published`` y ``trend_alert_suppressed``.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Tuple

import apache_beam as beam
from apache_beam.coders import PickleCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from common.alert_message import encode, to_ms
from streaming.bq_sink import log_json

ALERTS_TAG = "alerts"
ALERTS_TOPIC = "ta-alerts"
DEFAULT_COOLDOWN_SECONDS = 900

_TRUE = {"1", "true", "yes", "si", "sí", "on"}
_FALSE = {"0", "false", "no", "off"}


class AlertOptions(PipelineOptions):
    """Opciones del Lote 6.2. Todas tienen valor por defecto."""

    @classmethod
    def _add_argparse_args(cls, parser):
        group = parser.add_argument_group("Alertas (Lote 6.2)")
        group.add_argument("--alerts_enabled", default="auto",
                           help="auto (solo Dataflow; TA_ALERTS_ENABLED) | true | false.")
        group.add_argument("--alerts_topic", default="auto",
                           help="auto (projects/{proyecto}/topics/ta-alerts) | none | projects/P/topics/T.")
        group.add_argument("--alert_cooldown_seconds", type=int, default=DEFAULT_COOLDOWN_SECONDS,
                           help="Cooldown por marca × fuente × tipo de alerta (§5.12: 900).")


@dataclass(frozen=True)
class AlertsRuntime:
    """Configuración resuelta de la rama (se registra como ``alerts_config``)."""

    enabled: bool
    reason: str
    topic: str | None
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS

    def as_log(self) -> dict:
        return asdict(self)


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
        if env_text in _TRUE | _FALSE:
            return env_text in _TRUE
        raise ValueError(f"TA_ALERTS_ENABLED con valor no reconocido: {env_value!r}")
    return default


def _topic(value: str | None, project: str | None) -> str | None:
    text = (value or "auto").strip()
    if text.lower() in {"none", ""}:
        return None
    if text.lower() == "auto":
        return f"projects/{project}/topics/{ALERTS_TOPIC}" if project else None
    parts = text.split("/")
    if len(parts) != 4 or parts[0] != "projects" or parts[2] != "topics" or not parts[1] or not parts[3]:
        raise ValueError(f"--alerts_topic {text!r} inválido: use projects/PROYECTO/topics/TOPIC")
    return text


def resolve_alerts_config(all_options: Mapping[str, Any], env: Mapping[str, str] | None = None, *,
                          is_dataflow: bool, project: str | None, trends_enabled: bool) -> AlertsRuntime:
    """Resuelve la rama a partir de las opciones del pipeline, el entorno y la rama de tendencias."""
    env = os.environ if env is None else env
    cooldown = int(all_options.get("alert_cooldown_seconds") or DEFAULT_COOLDOWN_SECONDS)
    if cooldown <= 0:
        raise ValueError("--alert_cooldown_seconds debe ser > 0")
    enabled = _flag(all_options.get("alerts_enabled"), env.get("TA_ALERTS_ENABLED"), is_dataflow)
    reason = "activa" if enabled else ("deshabilitada" if is_dataflow else "solo_dataflow")
    topic = _topic(all_options.get("alerts_topic"), project) if enabled else None
    if enabled and not trends_enabled:
        enabled, reason = False, "sin_tendencias"
    if enabled and not topic:
        enabled, reason = False, "sin_topic"
    return AlertsRuntime(enabled, reason, topic if enabled else None, cooldown)


# --------------------------------------------------------------------------- cooldown

def cooldown_key(alert: Mapping[str, Any]) -> str:
    return f"{alert['brand']}|{alert['source']}|{alert['alert_type']}"


def cooldown_step(last_end_ms: int | None, window_end_ms: int, cooldown_ms: int) -> tuple[bool, int | None]:
    """``(publicar, nuevo estado)``: se publica si la ventana termina a ≥ ``cooldown_ms`` de la última avisada."""
    if last_end_ms is not None and abs(window_end_ms - last_end_ms) < cooldown_ms:
        return False, last_end_ms
    return True, window_end_ms if last_end_ms is None else max(last_end_ms, window_end_ms)


def _with_key(alert: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    return cooldown_key(alert), alert


class AlertCooldownFn(beam.DoFn):
    """Deja pasar una alerta por clave cada ``cooldown_seconds`` (estado por marca|fuente|tipo)."""

    LAST = ReadModifyWriteStateSpec("last_alert_end", PickleCoder())

    def __init__(self, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS):
        self._cooldown_ms = int(cooldown_seconds) * 1000
        self._published = Metrics.counter("alerts", "alerts_published")
        self._suppressed = Metrics.counter("alerts", "alerts_suppressed")

    def process(self, element, last=beam.DoFn.StateParam(LAST)):
        _key, alert = element
        fields = {k: alert[k] for k in ("alert_id", "alert_type", "brand", "source", "window_end", "zscore")}
        emit, state = cooldown_step(last.read(), to_ms(alert["window_end"]), self._cooldown_ms)
        if not emit:
            self._suppressed.inc()
            log_json("trend_alert_suppressed", **fields)
            return
        last.write(state)
        self._published.inc()
        log_json("trend_alert_published", **fields)
        yield alert


def to_pubsub_message(alert: Mapping[str, Any]):
    from apache_beam.io.gcp.pubsub import PubsubMessage

    data, attributes = encode(alert)
    return PubsubMessage(data, attributes)


class TrendAlerts(beam.PTransform):
    """Mensajes ``ta-alert`` → *cooldown* → ``ta-alerts``. Devuelve los que pasan el *cooldown*.

    ``sink`` es inyectable para las pruebas (recibe ``PubsubMessage``); por defecto ``WriteToPubSub``.
    """

    def __init__(self, runtime: AlertsRuntime, sink=None):
        super().__init__()
        self._runtime = runtime
        self._sink = sink

    def expand(self, alerts):
        published = (
            alerts
            | "AlertKey" >> beam.Map(_with_key).with_output_types(Tuple[str, Any])
            | "Cooldown" >> beam.ParDo(AlertCooldownFn(self._runtime.cooldown_seconds)).with_input_types(Tuple[str, Any])
        )
        sink = self._sink or beam.io.WriteToPubSub(self._runtime.topic, with_attributes=True)
        _ = (published
             | "Separate" >> beam.Reshuffle()
             | "ToPubsub" >> beam.Map(to_pubsub_message)
             | "PublishAlerts" >> sink)
        return published


__all__ = [
    "AlertOptions", "AlertsRuntime", "resolve_alerts_config", "cooldown_key", "cooldown_step",
    "AlertCooldownFn", "to_pubsub_message", "TrendAlerts", "ALERTS_TAG", "DEFAULT_COOLDOWN_SECONDS",
]
