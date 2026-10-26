"""Escritura genérica en BigQuery por *streaming inserts* (Lote 2.4).

Mismo método que ``posts_enriched`` (Lote 2.2, ADR-18): ``STREAMING_INSERTS``,
``CREATE_NEVER`` y ``RETRY_ON_TRANSIENT_ERROR``. Las filas rechazadas suben el
contador ``{namespace}/bq_failed_rows`` y dejan un log ``{failed_event}`` con
las claves de la fila y el motivo; el pipeline sigue.
"""

from __future__ import annotations

import json
import logging
from typing import Iterable, Sequence

import apache_beam as beam
from apache_beam.metrics import Metrics

LOG = logging.getLogger("streaming.bq_sink")


def log_json(event: str, *, level: int = logging.INFO, **fields) -> None:
    """Log estructurado en JSON (en los workers va a Cloud Logging)."""
    LOG.log(level, json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))


class _LogFailedRows(beam.DoFn):
    def __init__(self, namespace: str, failed_event: str, key_fields: Sequence[str]):
        self._namespace = namespace
        self._failed_event = failed_event
        self._key_fields = tuple(key_fields)
        self._counter = Metrics.counter(namespace, "bq_failed_rows")

    def process(self, element):
        self._counter.inc()
        row, errors = _unpack_failed(element)
        keys = {k: row.get(k) for k in self._key_fields} if isinstance(row, dict) else {"row": str(row)[:200]}
        log_json(self._failed_event, level=logging.WARNING, **keys, errors=str(errors)[:1000])


def _unpack_failed(element):
    # FailedRowsWithErrors: (destino, fila, errores); versiones antiguas: (destino, fila).
    if isinstance(element, (tuple, list)):
        if len(element) >= 3:
            return element[1], element[2]
        if len(element) == 2:
            return element[1], None
    return element, None


def _failed_rows(result):
    for getter in (
        lambda r: r.failed_rows_with_errors,
        lambda r: r["FailedRowsWithErrors"],
        lambda r: r.failed_rows,
        lambda r: r["FailedRows"],
    ):
        try:
            value = getter(result)
        except Exception:  # noqa: BLE001 - la forma del resultado cambia entre versiones de Beam
            continue
        if value is not None:
            return value
    return None


class WriteRowsToBigQuery(beam.PTransform):
    """Inserta dicts con exactamente las columnas de la tabla (que ya existe)."""

    def __init__(self, table: str, *, namespace: str, failed_event: str,
                 key_fields: Iterable[str] = ()):
        super().__init__()
        self._table = table
        self._namespace = namespace
        self._failed_event = failed_event
        self._key_fields = tuple(key_fields)

    def expand(self, rows):
        from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
        from apache_beam.io.gcp.bigquery_tools import RetryStrategy

        result = rows | "Insert" >> WriteToBigQuery(
            table=self._table,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
        )
        failed = _failed_rows(result)
        if failed is not None:
            _ = failed | "LogFailedRows" >> beam.ParDo(
                _LogFailedRows(self._namespace, self._failed_event, self._key_fields))
        return result
