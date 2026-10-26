"""Ambos orígenes en ``s3://…/raw/`` (HU-025 y "Listo cuando" de la Fase 3).

Cuenta los objetos de ``raw/origin=historical/`` y ``raw/origin=stream/`` por fuente y lee
un Parquet de cada origen (con las credenciales del desarrollador) para comprobar que
tiene las columnas de ``posts_enriched`` (§5.5), su ``origin`` y ninguna identidad en claro.
pyarrow y ``common/`` se cargan al usarse: no hacen falta en la imagen del job.
"""

from __future__ import annotations

import io
import re
from typing import Any, Callable

from exporter.keys import SOURCES

ORIGINS = ("historical", "stream")
# Columnas de posts_enriched según §5.5, por si no se puede importar common.enriched_row.
DOC_COLUMNS = (
    "event_id", "origin", "source", "generator", "event_time", "ingest_time", "process_time",
    "user_hash", "followers", "text", "lang", "brands", "brand_primary", "hashtags",
    "likes", "shares", "comments", "sentiment_label", "sentiment_score", "emotion_label",
    "emotion_score", "label_sentiment", "nlp_model_version", "payload",
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def posts_enriched_column_names() -> tuple[list[str] | None, str]:
    """Nombres de ``common.enriched_row.POSTS_ENRICHED_COLUMNS`` sin suponer su forma
    (texto, tupla/lista con el nombre primero, dict con ``name`` u objeto con ``.name``)."""
    try:
        from common.enriched_row import POSTS_ENRICHED_COLUMNS  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return None, f"no se pudo importar common.enriched_row ({type(exc).__name__}: {exc})"
    cols = POSTS_ENRICHED_COLUMNS
    if isinstance(cols, dict):
        return [str(k) for k in cols.keys()], "common.enriched_row.POSTS_ENRICHED_COLUMNS (dict)"
    names: list[str] = []
    for col in cols:
        if isinstance(col, str):
            names.append(col)
        elif isinstance(col, dict) and "name" in col:
            names.append(str(col["name"]))
        elif isinstance(col, (tuple, list)) and col and isinstance(col[0], str):
            names.append(col[0])
        elif hasattr(col, "name"):
            names.append(str(getattr(col, "name")))
        else:
            return None, f"no sé leer el nombre de la columna {col!r} (forma {type(col).__name__})"
    return names, "common.enriched_row.POSTS_ENRICHED_COLUMNS"


def inspect_parquet(body: bytes, origin: str, expected: list[str] | None) -> dict[str, Any]:
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(body))
    names = list(table.schema.names)
    problems: list[str] = []
    if expected is not None:
        if names != list(expected):
            missing = [c for c in expected if c not in names]
            extra = [c for c in names if c not in expected]
            problems.append(f"columnas distintas de posts_enriched (faltan {missing}, sobran {extra}, orden {'igual' if not missing and not extra else '—'})")
    else:
        if set(names) != set(DOC_COLUMNS):
            problems.append(f"columnas distintas de §5.5: {sorted(set(names) ^ set(DOC_COLUMNS))}")
    for bad in ("user_id", "handle"):
        if bad in names:
            problems.append(f"columna {bad} en claro")
    rows = table.num_rows
    if "origin" in names:
        origins = {v for v in table.column("origin").to_pylist()}
        if origins != {origin}:
            problems.append(f"origin = {sorted(map(str, origins))} (se esperaba {origin})")
    if "user_hash" in names:
        bad_hash = sum(1 for v in table.column("user_hash").to_pylist() if v is not None and not _HEX64.match(str(v)))
        if bad_hash:
            problems.append(f"{bad_hash} user_hash que no son hex de 64")
    if "payload" in names:
        leaked = sum(1 for v in table.column("payload").to_pylist() if v and ('"user_id"' in str(v) or '"handle"' in str(v)))
        if leaked:
            problems.append(f"{leaked} payload con user_id o handle")
    return {"rows": rows, "columns": len(names), "problems": problems}


def lake_report(
    count_prefix: Callable[[str], tuple[int, int]],
    first_key: Callable[[str], str | None],
    get_bytes: Callable[[str], bytes],
    *,
    expected_columns: list[str] | None,
    columns_source: str,
) -> dict[str, Any]:
    origins: dict[str, Any] = {}
    for origin in ORIGINS:
        per_source: dict[str, dict[str, int]] = {}
        total = size = 0
        for source in SOURCES:
            n, b = count_prefix(f"raw/origin={origin}/source={source}/")
            per_source[source] = {"objects": n, "bytes": b}
            total += n
            size += b
        sample: dict[str, Any] = {}
        key = first_key(f"raw/origin={origin}/") if total else None
        if key:
            try:
                sample = {"key": key, **inspect_parquet(get_bytes(key), origin, expected_columns)}
            except Exception as exc:  # noqa: BLE001
                sample = {"key": key, "problems": [f"no se pudo leer: {type(exc).__name__}: {exc}"]}
        origins[origin] = {"objects": total, "bytes": size, "by_source": per_source, "sample": sample}
    return {"origins": origins, "columns_source": columns_source, "expected_columns": len(expected_columns or DOC_COLUMNS)}
