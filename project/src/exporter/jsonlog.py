"""Logs JSON de una línea a stdout (Cloud Logging los lee como ``jsonPayload``)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any, Callable, TextIO

LogFn = Callable[..., None]

# Claves que nunca deben aparecer en un log (tokens y credenciales).
_SECRET_KEYS = {"token", "id_token", "web_identity_token", "secret_access_key", "session_token", "SecretAccessKey", "SessionToken"}


def _default(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_ts(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    return str(value)


def format_ts(value: datetime | None) -> str | None:
    """UTC, ISO-8601 con milisegundos y ``Z`` (§5.1)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def parse_ts(text: str | None) -> datetime | None:
    if not text:
        return None
    raw = str(text).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def make_logger(stream: TextIO | None = None) -> LogFn:
    out = stream or sys.stdout

    def log(event: str, severity: str = "INFO", **fields: Any) -> None:
        payload = {"severity": severity, "event": event, "message": event}
        for key, value in fields.items():
            if key in _SECRET_KEYS:
                continue
            payload[key] = value
        payload["time"] = format_ts(datetime.now(timezone.utc))
        out.write(json.dumps(payload, ensure_ascii=False, default=_default) + "\n")
        out.flush()

    return log


def null_logger(event: str, severity: str = "INFO", **fields: Any) -> None:  # pragma: no cover - trivial
    return None
