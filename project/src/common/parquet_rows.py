"""Parquet de ``enriched/`` con el esquema plano de ``posts_enriched`` (§5.4, §5.5).

Lo escriben Dataflow (``enriched/`` en GCS) y el loader histórico (``raw/origin=historical/``
en S3, Lote 3.1); lo leen Athena y Glue (``posts_raw``, §5.7). ``pyarrow`` se importa
solo al usarse, para que ``common`` siga importándose sin él.

Tipos: timestamps ``timestamp[ms, UTC]`` (INT64 TIMESTAMP_MILLIS, ajustado a UTC),
enteros ``int64``, scores ``double``, listas ``list<string>`` y ``payload`` como texto JSON.
Compresión Snappy (§5.7). Mismo conjunto de filas ⇒ mismos bytes.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from common.enriched_row import COLUMN_NAMES, POSTS_ENRICHED_COLUMNS

_ARROW_TYPES = {
    "STRING": "string",
    "JSON": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "TIMESTAMP": "timestamp",
}


def arrow_schema():
    """Esquema de pyarrow en el orden de ``POSTS_ENRICHED_COLUMNS``."""
    import pyarrow as pa

    fields = []
    for c in POSTS_ENRICHED_COLUMNS:
        kind = _ARROW_TYPES[c.bq_type]
        if kind == "timestamp":
            t = pa.timestamp("ms", tz="UTC")
        elif kind == "int64":
            t = pa.int64()
        elif kind == "float64":
            t = pa.float64()
        else:
            t = pa.string()
        if c.repeated:
            t = pa.list_(pa.field("element", t))
        fields.append(pa.field(c.name, t, nullable=not c.required))
    return pa.schema(fields)


def parse_ts(value: Any) -> datetime | None:
    """``2026-10-17T14:03:11.482Z`` → ``datetime`` en UTC (o ``None``)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def rows_to_table(rows: Iterable[Mapping[str, Any]]):
    """Filas de ``posts_enriched`` → ``pyarrow.Table``."""
    import pyarrow as pa

    rows = list(rows)
    cols: dict[str, list[Any]] = {name: [] for name in COLUMN_NAMES}
    ts_cols = {c.name for c in POSTS_ENRICHED_COLUMNS if c.bq_type == "TIMESTAMP"}
    for r in rows:
        for name in COLUMN_NAMES:
            v = r.get(name)
            cols[name].append(parse_ts(v) if name in ts_cols else v)
    return pa.Table.from_pydict(cols, schema=arrow_schema())


def rows_to_parquet_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    """Filas → bytes de un archivo Parquet (Snappy), ordenadas por ``event_id``."""
    import pyarrow.parquet as pq

    ordered = sorted(rows, key=lambda r: str(r.get("event_id")))
    table = rows_to_table(ordered)
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        use_dictionary=True,
        coerce_timestamps="ms",
        allow_truncated_timestamps=False,
        use_deprecated_int96_timestamps=False,
        write_statistics=True,
    )
    return buf.getvalue()


def _format_ts(value: Any) -> Any:
    if isinstance(value, datetime):
        value = value.astimezone(timezone.utc)
        return f"{value:%Y-%m-%dT%H:%M:%S}.{value.microsecond // 1000:03d}Z"
    return value


def parquet_bytes_to_rows(data: bytes) -> list[dict[str, Any]]:
    """Lee un Parquet de ``enriched/`` y devuelve filas con timestamps en texto ISO."""
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(data))
    out = []
    for rec in table.to_pylist():
        out.append({k: _format_ts(v) for k, v in rec.items()})
    return out
