"""Registros que viajan por el pipeline y rutas de partición del lake (§5.4).

Sin dependencias de Beam: lo usan el pipeline, el verificador y las pruebas.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

# Tipos de error de la DLQ (valor de `error_type` en dlq_events y en dlq/).
ERROR_ENCODING = "encoding"      # el cuerpo no es UTF-8
ERROR_JSON = "json"              # no es JSON o no es un objeto
ERROR_SCHEMA = "schema"          # no cumple envelope.v1.schema.json
ERROR_ATTRIBUTES = "attributes"  # atributos ausentes, mal tipados o distintos del cuerpo
ERROR_ROUTING = "routing"        # evento de otra fuente en esta suscripción
ERROR_TYPES = (ERROR_ENCODING, ERROR_JSON, ERROR_SCHEMA, ERROR_ATTRIBUTES, ERROR_ROUTING)

RAW_PREFIX = "raw"
DLQ_PREFIX = "dlq"

# Límites de Pub/Sub para atributos (bytes).
_ATTR_VALUE_MAX = 1024
_ATTR_MAX_COUNT = 100
ERROR_DETAIL_MAX = 2000  # caracteres guardados en dlq/ y dlq_events


def format_ts(epoch_seconds: float) -> str:
    """UTC ISO-8601 con milisegundos y `Z` (§5.1)."""
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def raw_partition(source: str, process_ts: float) -> str:
    """`raw/source={s}/dt={YYYY-MM-DD}/hr={HH}` por fecha/hora de proceso (§5.4)."""
    dt = _utc(process_ts)
    return f"{RAW_PREFIX}/source={source}/dt={dt:%Y-%m-%d}/hr={dt:%H}"


def dlq_partition(process_ts: float) -> str:
    """`dlq/dt={YYYY-MM-DD}` por fecha de proceso (§5.4)."""
    return f"{DLQ_PREFIX}/dt={_utc(process_ts):%Y-%m-%d}"


def truncate_utf8(text: str, max_bytes: int) -> str:
    """Corta `text` a `max_bytes` bytes UTF-8 sin partir un carácter."""
    data = text.encode("utf-8")
    if len(data) <= max_bytes:
        return text
    return data[:max_bytes].decode("utf-8", errors="ignore")


@dataclass(frozen=True)
class ValidEvent:
    """Evento que pasó la validación.

    `body` es el JSON tal como llegó, en una sola línea (si traía saltos de
    línea se re-serializa compacto, sin cambiar su contenido).
    """

    source: str
    event_id: str
    body: str
    process_ts: float


@dataclass(frozen=True)
class DlqRecord:
    """Mensaje rechazado. Los cinco primeros campos son las columnas de dlq_events (§5.5)."""

    received_at: str
    source: str
    error_type: str
    error_detail: str
    raw_message: str
    process_ts: float
    data: bytes = b""
    attributes: dict = field(default_factory=dict)

    def row(self) -> dict:
        """Fila de `ta_stream.dlq_events` y línea de `dlq/`."""
        return {
            "received_at": self.received_at,
            "source": self.source,
            "error_type": self.error_type,
            "error_detail": self.error_detail,
            "raw_message": self.raw_message,
        }

    def line(self) -> str:
        return json.dumps(self.row(), ensure_ascii=False, separators=(",", ":"))

    def pubsub_attributes(self) -> dict:
        """Atributos del mensaje re-publicado en `ta-dlq`.

        Conserva los atributos originales (para poder re-inyectarlo) y suma
        `dlq_error_type`, `dlq_error_detail`, `dlq_source` y `dlq_received_at`.
        """
        extra = {
            "dlq_error_type": self.error_type,
            "dlq_error_detail": truncate_utf8(self.error_detail, _ATTR_VALUE_MAX),
            "dlq_source": self.source,
            "dlq_received_at": self.received_at,
        }
        original: dict = {}
        for key in sorted(self.attributes or {}):
            if key in extra or key.startswith("dlq_"):
                continue
            if len(original) >= _ATTR_MAX_COUNT - len(extra):
                break
            if len(key.encode("utf-8")) > 256:
                continue
            original[key] = truncate_utf8(str(self.attributes[key]), _ATTR_VALUE_MAX)
        return {**original, **extra}
