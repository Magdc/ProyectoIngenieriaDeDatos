"""Etapa 3 · ``write``: Parquet por ``source/dt/hr`` en el espejo local del bucket.

Lee ``stage2/``, valida cada envelope enriquecido contra ``envelope.enriched.v1``
(opcional), lo aplana con ``to_posts_enriched_row()`` y escribe con
``rows_to_parquet_bytes()`` (las 24 columnas de ``posts_enriched`` en orden, el mismo
formato de ``enriched/``, §5.4).

Particiones del histórico (§5.7, ADR-21): ``dt``/``hr`` = fecha y hora UTC de
``event_time``. Un archivo por fuente y hora, con nombre determinista
``part-historical-s{seed}-{AAAAMMDD}T{HH}.parquet`` y filas ordenadas por ``event_id``:
reejecutar reescribe las mismas claves (no duplica). Los Parquet de la misma semilla
que ya no correspondan al plan se borran del espejo local.
"""

from __future__ import annotations

import collections
import datetime as dt
import time
from typing import Any, Callable, Mapping, Sequence

from historical.adapters import NLP_FIELDS
from historical.plan import UTC, format_ts
from historical.work import (WorkDir, atomic_write_bytes, atomic_write_json, iter_jsonl_gz,
                             log_event, part_key, sha256_bytes, sha256_file)


class WriteError(RuntimeError):
    pass


def partition_of(event_time: str) -> tuple[str, str]:
    """``(AAAA-MM-DD, HH)`` de un ``event_time`` del contrato (``…T14:03:11.482Z``)."""
    if not (isinstance(event_time, str) and len(event_time) >= 13 and event_time[10] == "T"):
        raise WriteError(f"event_time inesperado: {event_time!r}")
    return event_time[:10], event_time[11:13]


def write_output(work: WorkDir, *, to_row: Callable[[Mapping[str, Any]], dict[str, Any]],
                 rows_to_parquet: Callable[[Sequence[Mapping[str, Any]]], bytes],
                 validate_enriched: Callable[[Mapping[str, Any]], list[str]] | None = None,
                 allow_missing_nlp: bool = False) -> dict[str, Any]:
    gen = work.read_json(work.generate_json)
    if not gen:
        raise WriteError("falta generate.json: corra `generate` primero")
    stage1 = [p.name for p in work.stage1_files()]
    stage2 = {p.name for p in work.stage2_files()}
    pending = [n for n in stage1 if n not in stage2]
    if pending:
        raise WriteError(f"faltan {len(pending)} archivos de NLP (p. ej. {pending[:3]}): "
                         "corra `nlp` hasta terminar")
    t0 = time.monotonic()
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    seen: set[str] = set()
    missing_nlp = 0
    invalid: list[dict[str, Any]] = []
    n_invalid = 0
    for e in iter_jsonl_gz(work.stage2_files()):
        eid = e["event_id"]
        if eid in seen:
            raise WriteError(f"event_id repetido en stage2: {eid}")
        seen.add(eid)
        if e.get("sentiment_label") is None or e.get("emotion_label") is None:
            missing_nlp += 1
        elif validate_enriched is not None:
            errors = validate_enriched(e)
            if errors:
                n_invalid += 1
                if len(invalid) < 10:
                    invalid.append({"event_id": eid, "errors": errors})
        day, hour = partition_of(e["event_time"])
        groups[(e["source"], day, hour)].append(e)
    if missing_nlp and not allow_missing_nlp:
        raise WriteError(f"{missing_nlp} filas sin NLP: repita `nlp` (reintenta las que faltan) "
                         "o use --allow-missing-nlp solo para pruebas")
    if n_invalid:
        raise WriteError(f"{n_invalid} envelopes enriquecidos no cumplen envelope.enriched.v1: "
                         f"{invalid[:2]}")
    if len(seen) != int(gen.get("rows", -1)):
        raise WriteError(f"stage2 tiene {len(seen)} filas y generate.json {gen.get('rows')}")
    files: list[dict[str, Any]] = []
    written = unchanged = 0
    keep: set[str] = set()
    for (source, day, hour) in sorted(groups):
        items = sorted(groups[(source, day, hour)], key=lambda r: r["event_id"])
        rows = []
        for e in items:
            row = to_row(e)
            for f in NLP_FIELDS:
                row[f] = e.get(f)
            rows.append(row)
        data = rows_to_parquet(rows)
        key = part_key(work.seed, source, day, hour)
        path = work.local_path(key)
        digest = sha256_bytes(data)
        if path.exists() and path.stat().st_size == len(data) and sha256_file(path) == digest:
            unchanged += 1
        else:
            atomic_write_bytes(path, data)
            written += 1
        keep.add(str(path.resolve()))
        files.append({"key": key, "source": source, "dt": day, "hr": hour,
                      "rows": len(rows), "size": len(data), "sha256": digest})
    removed = []
    for path in work.own_parts():
        if str(path.resolve()) not in keep:
            removed.append(str(path.relative_to(work.root)))
            path.unlink()
    per_source = collections.Counter()
    for f in files:
        per_source[f["source"]] += f["rows"]
    result = {"digest": gen.get("digest"), "seed": work.seed, "rows": len(seen),
              "files": files, "n_files": len(files), "written": written, "unchanged": unchanged,
              "removed": removed, "rows_per_source": dict(per_source),
              "bytes": sum(f["size"] for f in files), "missing_nlp": missing_nlp,
              "written_at": format_ts(dt.datetime.now(UTC)),
              "seconds": round(time.monotonic() - t0, 1)}
    atomic_write_json(work.output_json, result)
    log_event("hist_write_done", files=len(files), written=written, unchanged=unchanged,
              removed=len(removed), rows=len(seen), bytes=result["bytes"])
    return result
