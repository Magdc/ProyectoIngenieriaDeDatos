"""``ta-dlq`` → ``ta_stream.dlq_events`` (Lote 2.4, HU-018 y HU-005).

El pipeline del Lote 2.1 ya publica cada mensaje rechazado en ``ta-dlq`` con el
cuerpo original y los atributos ``dlq_error_type``, ``dlq_error_detail``,
``dlq_source`` y ``dlq_received_at`` (§5.3). Esta rama lee la suscripción
``ta-dlq-df`` y escribe una fila de ``dlq_events`` por mensaje (§5.5), sin tocar
el código del Lote 2.1. No se usa a la vez que ``--dlq_table`` (escribiría dos veces).

Formato de la fila:

* ``received_at`` = ``dlq_received_at`` normalizado a UTC con milisegundos
  (si falta o no se entiende, la hora de lectura; contador ``dlq_bq/received_at_fallback``).
* ``source`` = ``dlq_source`` (o ``source``); ``error_type`` = ``dlq_error_type``
  (o ``unknown``, contador ``dlq_bq/rows_without_type``); ``error_detail`` = ``dlq_error_detail``.
* ``raw_message`` = cuerpo en UTF-8; los bytes inválidos van como ``\\xNN``.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Mapping

import apache_beam as beam
from apache_beam.metrics import Metrics

from common.stream_tables import DLQ_ERROR_TYPES
from streaming.bq_sink import WriteRowsToBigQuery
from streaming.trends_core import format_ts, to_epoch_ms

DLQ_COLUMNS = ("received_at", "source", "error_type", "error_detail", "raw_message")


def decode_body(data: Any) -> str:
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    return bytes(data).decode("utf-8", errors="backslashreplace")


def dlq_row(data: Any, attributes: Mapping[str, str] | None, now_ms: int) -> tuple[dict, list[str]]:
    """Fila de ``dlq_events`` desde un mensaje de ``ta-dlq``. Devuelve ``(fila, avisos)``."""
    attrs = dict(attributes or {})
    warnings: list[str] = []
    received_ms = to_epoch_ms(attrs.get("dlq_received_at"))
    if received_ms is None:
        received_ms = now_ms
        warnings.append("received_at_fallback")
    error_type = attrs.get("dlq_error_type") or None
    if not error_type:
        error_type = "unknown"
        warnings.append("rows_without_type")
    elif error_type not in DLQ_ERROR_TYPES:
        warnings.append("unexpected_error_type")
    row = {
        "received_at": format_ts(received_ms),
        "source": attrs.get("dlq_source") or attrs.get("source") or None,
        "error_type": error_type,
        "error_detail": attrs.get("dlq_error_detail") or None,
        "raw_message": decode_body(data),
    }
    return row, warnings


class DlqMessageToRow(beam.DoFn):
    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._rows = Metrics.counter("dlq_bq", "dlq_rows")
        self._fallback = Metrics.counter("dlq_bq", "received_at_fallback")
        self._untyped = Metrics.counter("dlq_bq", "rows_without_type")
        self._unexpected = Metrics.counter("dlq_bq", "unexpected_error_type")

    def process(self, message):
        data = getattr(message, "data", message)
        attributes = getattr(message, "attributes", None)
        row, warnings = dlq_row(data, attributes, int(self._clock() * 1000))
        for warning in warnings:
            {"received_at_fallback": self._fallback, "rows_without_type": self._untyped,
             "unexpected_error_type": self._unexpected}[warning].inc()
        self._rows.inc()
        yield row


class DlqEventsToBigQuery(beam.PTransform):
    """Lee ``ta-dlq-df`` y escribe ``dlq_events``. ``read``/``sink`` se inyectan en las pruebas."""

    def __init__(self, subscription: str, table: str, *, read=None, sink=None):
        super().__init__()
        self._subscription = subscription
        self._table = table
        self._read = read
        self._sink = sink

    def expand(self, pbegin):
        if self._read is not None:
            messages = pbegin | "ReadDlq" >> self._read
        else:
            from apache_beam.io.gcp.pubsub import ReadFromPubSub
            messages = pbegin | "ReadDlq" >> ReadFromPubSub(
                subscription=self._subscription, with_attributes=True)
        rows = messages | "ToDlqRow" >> beam.ParDo(DlqMessageToRow())
        sink = self._sink or WriteRowsToBigQuery(
            self._table, namespace="dlq_bq", failed_event="dlq_bq_insert_failed",
            key_fields=("received_at", "source", "error_type"))
        _ = rows | "WriteDlqEvents" >> sink
        # Se imprime al construir el grafo: aparece en la terminal de quien lanza el job.
        print(json.dumps({"event": "dlq_bq_attached", "subscription": self._subscription,
                          "table": self._table}), flush=True)
        return rows
