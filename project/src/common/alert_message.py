"""Mensaje de alerta de tendencia ``ta-alert`` v1.0 (Lote 6.2, HU-039; ARCHITECTURE.md §5.3 y §3.28).

Lo arma la rama de alertas de Dataflow (``streaming.alerts``) cuando una fila de ``trend_windows`` tiene
``is_alert = true``, se publica en ``ta-alerts`` y lo lee el notificador de Cloud Run (``alerts.notifier``),
que escribe el log estructurado del que la política basada en logs saca el correo (§5.12).

Solo usa la biblioteca estándar: también va en la imagen del notificador.

* Un mensaje por **métrica** que pasa el umbral: una ventana puede dar ``volume_*`` y ``sentiment_*``.
* ``alert_type`` = métrica + dirección (``up``/``down`` según el signo de z). El *cooldown* de 15 min es
  por marca × fuente × ``alert_type`` (§5.12).
* ``alert_id`` es determinista (uuid5 de marca, fuente, tipo e inicio de la ventana): el mismo panel
  reenviado da el mismo id.
* ``origin``: ``dataflow`` (el pipeline) o ``check`` (alerta sintética que publica el validador).
* Horas en UTC (§5.1); el texto del correo muestra además la hora de Bogotá (UTC−5, sin horario de
  verano desde 1993: se usa un desfase fijo para no depender de ``tzdata`` en la imagen).
"""

from __future__ import annotations

import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

SCHEMA = "ta-alert"
SCHEMA_VERSION = "1.0"
METRICS: tuple[str, ...] = ("volume", "sentiment")
DIRECTIONS: tuple[str, ...] = ("up", "down")
ALERT_TYPES: tuple[str, ...] = ("volume_up", "volume_down", "sentiment_up", "sentiment_down")
ORIGINS: tuple[str, ...] = ("dataflow", "check")
SOURCES: tuple[str, ...] = ("reddit", "facebook", "instagram")
ATTRIBUTE_KEYS: tuple[str, ...] = ("alert_id", "alert_type", "brand", "source", "window_end", "schema_version")
BOGOTA = timezone(timedelta(hours=-5), "America/Bogota")

#: Campos del cuerpo JSON, en orden (§5.3).
FIELDS: tuple[str, ...] = (
    "schema", "schema_version", "alert_id", "alert_type", "metric", "direction", "origin",
    "brand", "source", "window_start", "window_end",
    "value", "baseline_mean", "baseline_std", "zscore", "change", "change_pct",
    "z_threshold", "baseline_windows", "n_posts", "avg_sentiment", "n_negative", "n_positive",
    "detected_at", "cooldown_seconds",
)
_NULLABLE = {"baseline_std", "change_pct", "avg_sentiment"}
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_TYPE_TEXT = {
    "volume_up": "Volumen al alza",
    "volume_down": "Volumen a la baja",
    "sentiment_up": "Sentimiento al alza",
    "sentiment_down": "Sentimiento a la baja",
}


# --------------------------------------------------------------------------- tiempo

def format_ts(epoch_ms: int) -> str:
    """UTC con milisegundos y ``Z`` (§5.1), igual que ``streaming.trends_core.format_ts``."""
    epoch_ms = int(round(epoch_ms))
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{epoch_ms % 1000:03d}Z"


def parse_ts(text: str) -> datetime:
    if not isinstance(text, str) or not _TS.match(text):
        raise ValueError(f"marca de tiempo inválida: {text!r} (se espera AAAA-MM-DDThh:mm:ss.mmmZ)")
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def to_ms(text: str) -> int:
    return int(round(parse_ts(text).timestamp() * 1000))


# --------------------------------------------------------------------------- construcción

def alert_type_for(metric: str, zscore: float) -> str:
    if metric not in METRICS:
        raise ValueError(f"métrica desconocida: {metric!r}")
    return f"{metric}_{'up' if zscore >= 0 else 'down'}"


def alert_id(brand: str, source: str, alert_type: str, window_start: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SCHEMA}|{brand}|{source}|{alert_type}|{window_start}"))


def _round(value: float | None, digits: int) -> float | None:
    return None if value is None else round(float(value), digits)


def build_alert(*, metric: str, brand: str, source: str, window_start_ms: int, window_end_ms: int,
                value: float, baseline_mean: float, baseline_std: float | None, zscore: float,
                z_threshold: float, baseline_windows: int, n_posts: int, avg_sentiment: float | None,
                n_negative: int, n_positive: int, detected_ms: int, cooldown_seconds: int,
                origin: str = "dataflow") -> dict[str, Any]:
    """Arma el cuerpo de una alerta con los campos de ``FIELDS`` (en orden)."""
    alert_type = alert_type_for(metric, zscore)
    start = format_ts(window_start_ms)
    digits = 0 if metric == "volume" else 6
    change = float(value) - float(baseline_mean)
    # El cambio porcentual solo tiene sentido en volumen (el sentimiento es una media con signo).
    pct = round(change / abs(baseline_mean) * 100, 1) if metric == "volume" and baseline_mean else None
    msg = {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "alert_id": alert_id(brand, source, alert_type, start),
        "alert_type": alert_type,
        "metric": metric,
        "direction": alert_type.rsplit("_", 1)[1],
        "origin": origin,
        "brand": brand,
        "source": source,
        "window_start": start,
        "window_end": format_ts(window_end_ms),
        "value": int(value) if digits == 0 else _round(value, 6),
        "baseline_mean": _round(baseline_mean, 6),
        "baseline_std": _round(baseline_std, 6),
        "zscore": _round(zscore, 6),
        "change": _round(change, 6),
        "change_pct": pct,
        "z_threshold": float(z_threshold),
        "baseline_windows": int(baseline_windows),
        "n_posts": int(n_posts),
        "avg_sentiment": _round(avg_sentiment, 6),
        "n_negative": int(n_negative),
        "n_positive": int(n_positive),
        "detected_at": format_ts(detected_ms),
        "cooldown_seconds": int(cooldown_seconds),
    }
    return msg


# --------------------------------------------------------------------------- validación

def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def alert_problems(msg: Any) -> list[str]:
    """Problemas de un cuerpo ``ta-alert`` (vacía = válido)."""
    if not isinstance(msg, Mapping):
        return ["el mensaje no es un objeto JSON"]
    out = [f"falta {k}" for k in FIELDS if k not in msg]
    extra = sorted(set(msg) - set(FIELDS))
    if extra:
        out.append(f"campos no previstos: {', '.join(extra)}")
    if out:
        return out
    if msg["schema"] != SCHEMA or msg["schema_version"] != SCHEMA_VERSION:
        out.append(f"schema {msg['schema']!r} {msg['schema_version']!r} (se espera {SCHEMA} {SCHEMA_VERSION})")
    if msg["alert_type"] not in ALERT_TYPES:
        out.append(f"alert_type {msg['alert_type']!r}")
    if msg["metric"] not in METRICS or msg["direction"] not in DIRECTIONS:
        out.append(f"metric/direction {msg['metric']!r}/{msg['direction']!r}")
    elif msg["alert_type"] != f"{msg['metric']}_{msg['direction']}":
        out.append("alert_type no coincide con metric y direction")
    if msg["origin"] not in ORIGINS:
        out.append(f"origin {msg['origin']!r}")
    if msg["source"] not in SOURCES:
        out.append(f"source {msg['source']!r}")
    if not isinstance(msg["brand"], str) or not msg["brand"]:
        out.append("brand vacío")
    for key in ("window_start", "window_end", "detected_at"):
        try:
            parse_ts(msg[key])
        except ValueError as exc:
            out.append(f"{key}: {exc}")
    for key in FIELDS[11:]:
        if key in ("detected_at",):
            continue
        value = msg[key]
        if value is None and key in _NULLABLE:
            continue
        if not _is_number(value):
            out.append(f"{key} no es un número: {value!r}")
    if out:
        return out
    if to_ms(msg["window_end"]) <= to_ms(msg["window_start"]):
        out.append("window_end <= window_start")
    if (msg["zscore"] >= 0) != (msg["direction"] == "up"):
        out.append("el signo de zscore no coincide con direction")
    if abs(msg["zscore"]) < msg["z_threshold"]:
        out.append(f"|zscore| {abs(msg['zscore'])} < z_threshold {msg['z_threshold']}")
    if msg["alert_id"] != alert_id(msg["brand"], msg["source"], msg["alert_type"], msg["window_start"]):
        out.append("alert_id no coincide con marca, fuente, tipo y ventana")
    return out


# --------------------------------------------------------------------------- Pub/Sub

def attributes_for(msg: Mapping[str, Any]) -> dict[str, str]:
    return {k: str(msg[k]) for k in ATTRIBUTE_KEYS}


def encode(msg: Mapping[str, Any]) -> tuple[bytes, dict[str, str]]:
    """Cuerpo JSON UTF-8 compacto y atributos de §5.3."""
    problems = alert_problems(msg)
    if problems:
        raise ValueError("alerta inválida: " + "; ".join(problems))
    body = {k: msg[k] for k in FIELDS}
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), attributes_for(msg)


def decode(data: bytes) -> dict[str, Any]:
    """Cuerpo de un mensaje de ``ta-alerts``; ``ValueError`` si no es una alerta válida."""
    try:
        msg = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"el cuerpo no es JSON UTF-8: {exc}") from exc
    problems = alert_problems(msg)
    if problems:
        raise ValueError("; ".join(problems))
    return msg


# --------------------------------------------------------------------------- texto del correo

def _num(value: float | None, digits: int) -> str:
    if value is None:
        return "—"
    text = f"{value:.{digits}f}".replace(".", ",")
    return text.replace("-", "−")


def _signed(value: float | None, digits: int) -> str:
    if value is None:
        return "—"
    return ("+" if value >= 0 else "") + _num(value, digits)


def _hhmm(text: str, tz: timezone) -> str:
    return parse_ts(text).astimezone(tz).strftime("%H:%M")


def window_text(msg: Mapping[str, Any]) -> str:
    s, e = msg["window_start"], msg["window_end"]
    day = parse_ts(s).strftime("%Y-%m-%d")
    return (f"{day} {_hhmm(s, timezone.utc)}–{_hhmm(e, timezone.utc)} UTC "
            f"({_hhmm(s, BOGOTA)}–{_hhmm(e, BOGOTA)} en Bogotá)")


def alert_text(msg: Mapping[str, Any]) -> str:
    """Una línea en español con marca, fuente, ventana y magnitud del cambio (HU-039)."""
    head = ("[prueba] " if msg["origin"] == "check" else "") + _TYPE_TEXT[msg["alert_type"]]
    if msg["metric"] == "volume":
        what = (f"{msg['value']} posts frente a una media de {_num(msg['baseline_mean'], 1)} "
                f"({_signed(msg['change_pct'], 0)} %, z = {_num(msg['zscore'], 1)})" if msg["change_pct"] is not None else
                f"{msg['value']} posts frente a una media de {_num(msg['baseline_mean'], 1)} (z = {_num(msg['zscore'], 1)})")
    else:
        what = (f"sentimiento medio {_num(msg['value'], 2)} frente a {_num(msg['baseline_mean'], 2)} "
                f"({_signed(msg['change'], 2)}, z = {_num(msg['zscore'], 1)}; {msg['n_posts']} posts)")
    return f"{head} de {msg['brand']} en {msg['source']}: {what} en la ventana {window_text(msg)}."


def notification_fields(msg: Mapping[str, Any]) -> dict[str, str]:
    """Textos cortos que la política de alertas extrae del log (``jsonPayload.aviso.*``) para el correo."""
    if msg["metric"] == "volume":
        value, mean, change = str(msg["value"]), _num(msg["baseline_mean"], 1), (
            f"{_signed(msg['change_pct'], 0)} %" if msg["change_pct"] is not None else _signed(msg["change"], 0))
    else:
        value, mean, change = _num(msg["value"], 2), _num(msg["baseline_mean"], 2), _signed(msg["change"], 2)
    return {
        "marca": msg["brand"],
        "fuente": msg["source"],
        "tipo": msg["alert_type"],
        "ventana": window_text(msg),
        "valor": value,
        "media": mean,
        "cambio": change,
        "z": _num(msg["zscore"], 1),
        "origen": msg["origin"],
        "alerta_id": msg["alert_id"],
    }


def log_record(msg: Mapping[str, Any], *, received_ms: int, publish_time: str | None = None,
               message_id: str | None = None) -> dict[str, Any]:
    """La línea JSON que escribe el notificador (Cloud Logging la guarda como ``jsonPayload``)."""
    end_ms = to_ms(msg["window_end"])
    detected_ms = to_ms(msg["detected_at"])
    return {
        "severity": "WARNING",
        "alert": "trend",
        "message": alert_text(msg),
        "aviso": notification_fields(msg),
        **{k: msg[k] for k in FIELDS if k not in ("schema", "schema_version")},
        "message_id": message_id,
        "publish_time": publish_time,
        "received_at": format_ts(received_ms),
        "detection_s": round((received_ms - end_ms) / 1000, 3),
        "notify_delay_s": round((received_ms - detected_ms) / 1000, 3),
    }


__all__ = [
    "SCHEMA", "SCHEMA_VERSION", "METRICS", "ALERT_TYPES", "ORIGINS", "ATTRIBUTE_KEYS", "FIELDS", "BOGOTA",
    "format_ts", "parse_ts", "to_ms", "alert_type_for", "alert_id", "build_alert", "alert_problems",
    "attributes_for", "encode", "decode", "alert_text", "window_text", "notification_fields", "log_record",
]
