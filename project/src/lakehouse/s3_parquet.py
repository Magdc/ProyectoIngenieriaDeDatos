"""Filas de los Parquet de un prefijo de S3 leyendo solo su pie (Lote 4.1, ARCHITECTURE.md §3.19.1).

Lo usa ``validate_4_1.py athena`` (E2–E3) para conciliar lo que Athena cuenta en
``ta_raw.posts_raw`` con lo que hay en S3, y sirve igual para Trusted y Curated (4.2/4.3).

Por cada objeto ``*.parquet`` pide los últimos ``tail_bytes`` (``Range: bytes=-N``): el pie del
Parquet (metadatos con ``num_rows``) está al final; si el pie no cabe, baja el objeto entero. Con
los Parquet del lake (KB) eso es un GET por objeto. Necesita ``s3:ListBucket``, ``s3:GetObject`` y
``kms:Decrypt`` (las credenciales del desarrollador) y ``pyarrow``.

    count_parquet_rows(client, bucket, "raw/origin=stream/") -> {"objects", "bytes", "rows",
        "by_source": {fuente: {"objects", "rows"}}, "errors": [...], "seconds"}
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

RAW_KEY = re.compile(r"^raw/origin=(?P<origin>[^/]+)/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hr=(?P<hr>\d{2})/[^/]+\.parquet$")
SOURCE_IN_KEY = re.compile(r"(?:^|/)source=(?P<source>[^/]+)/")
MAGIC = b"PAR1"


def parse_raw_key(key: str) -> dict[str, str] | None:
    """``raw/origin=…/source=…/dt=…/hr=…/x.parquet`` → ``{origin, source, dt, hr}``; si no, ``None``."""
    m = RAW_KEY.match(key)
    return m.groupdict() if m else None


def footer_length(tail: bytes) -> int:
    """Largo del pie (metadatos) leído de los últimos 8 bytes; lanza ``ValueError`` si no es Parquet."""
    if len(tail) < 12 or tail[-4:] != MAGIC:
        raise ValueError("no termina en PAR1")
    return int.from_bytes(tail[-8:-4], "little")


def rows_from_tail(tail: bytes) -> int:
    """``num_rows`` del pie, con solo el final del archivo (incluido el pie completo)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if footer_length(tail) + 8 > len(tail):
        raise ValueError("el pie no cabe en lo leído")
    return int(pq.read_metadata(pa.BufferReader(tail)).num_rows)


def list_parquet(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            if obj["Key"].endswith(".parquet"):
                out.append({"key": obj["Key"], "size": int(obj.get("Size") or 0)})
    return out


def object_rows(client: Any, bucket: str, key: str, size: int, tail_bytes: int = 262144) -> int:
    n = min(size, tail_bytes) if size else tail_bytes
    tail = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{n}")["Body"].read()
    try:
        return rows_from_tail(tail)
    except ValueError:
        if size and n >= size:
            raise
        whole = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        return rows_from_tail(whole)


def count_parquet_rows(client: Any, bucket: str, prefix: str, *, workers: int = 16, tail_bytes: int = 262144,
                       progress: Callable[[int, int], None] | None = None) -> dict[str, Any]:
    start = time.monotonic()
    objects = list_parquet(client, bucket, prefix)
    by_source: dict[str, dict[str, int]] = {}
    errors: list[dict[str, str]] = []
    total_rows = 0

    def one(obj: dict[str, Any]) -> tuple[dict[str, Any], int | None, str | None]:
        try:
            return obj, object_rows(client, bucket, obj["key"], obj["size"], tail_bytes), None
        except Exception as exc:  # noqa: BLE001 - se informa por objeto
            return obj, None, f"{type(exc).__name__}: {exc}"[:300]

    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for obj, rows, error in pool.map(one, objects):
            done += 1
            m = SOURCE_IN_KEY.search(obj["key"])
            src = m.group("source") if m else "?"
            entry = by_source.setdefault(src, {"objects": 0, "rows": 0})
            entry["objects"] += 1
            if rows is None:
                errors.append({"key": obj["key"], "error": error or "?"})
            else:
                entry["rows"] += rows
                total_rows += rows
            if progress and done % 1000 == 0:
                progress(done, len(objects))
    return {
        "prefix": prefix,
        "objects": len(objects),
        "bytes": sum(o["size"] for o in objects),
        "rows": total_rows,
        "by_source": dict(sorted(by_source.items())),
        "errors": errors[:50],
        "error_count": len(errors),
        "seconds": round(time.monotonic() - start, 1),
    }
