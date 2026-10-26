"""Verificador de la exportación (lo corre el desarrollador con sus credenciales).

Compara ``enriched/`` en GCS con ``raw/origin=stream/`` en S3: lo que el watermark dice
que ya salió debe estar en S3 con el mismo tamaño; en una muestra, S3 debe tener SSE-KMS
con la llave del lake y el mismo CRC32C que GCS (el exportador lo manda en el PUT).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from exporter.core import GcsObject
from exporter.keys import parse_gcs_key, parse_s3_key

OK, AVISO, FALLA = "OK", "AVISO", "FALLA"


def _check(cid: str, what: str, status: str, detail: str, **data: Any) -> dict[str, Any]:
    return {"id": cid, "what": what, "status": status, "detail": detail, **data}


def mapped(gcs_objects: Iterable[GcsObject]) -> dict[str, GcsObject]:
    out: dict[str, GcsObject] = {}
    for obj in gcs_objects:
        parsed = parse_gcs_key(obj.name)
        if parsed is not None:
            out[parsed.s3_key] = obj
    return out


def sample_keys(keys: Iterable[str], n: int) -> list[str]:
    """Muestra determinista y repartida (cada k-ésima clave en orden)."""
    ordered = sorted(keys)
    if n <= 0 or not ordered:
        return []
    if len(ordered) <= n:
        return ordered
    step = len(ordered) / n
    return [ordered[int(i * step)] for i in range(n)]


def verify_export(
    gcs_objects: list[GcsObject],
    s3_index: dict[str, int],
    *,
    cursor: datetime | None,
    heads: dict[str, dict[str, Any]],
    kms_key_arn: str | None,
) -> dict[str, Any]:
    by_key = mapped(gcs_objects)
    checks: list[dict[str, Any]] = []

    exported = {k: o for k, o in by_key.items() if cursor is not None and o.time_created <= cursor}
    missing = sorted(k for k in exported if k not in s3_index)
    size_diff = sorted(k for k, o in exported.items() if k in s3_index and s3_index[k] != o.size)
    if cursor is None:
        checks.append(_check("V1", "lo exportado según el watermark está en S3", FALLA,
                             "no hay watermark (el exportador no ha terminado ninguna corrida)"))
    else:
        status = OK if not missing and not size_diff else FALLA
        checks.append(_check(
            "V1", "lo exportado según el watermark está en S3", status,
            f"{len(exported) - len(missing) - len(size_diff)} de {len(exported)} con el mismo tamaño"
            + (f"; faltan {len(missing)}" if missing else "") + (f"; tamaño distinto {len(size_diff)}" if size_diff else ""),
            expected=len(exported), missing=missing[:20], size_mismatch=size_diff[:20],
        ))

    later = sorted(k for k, o in by_key.items() if cursor is None or o.time_created > cursor)
    later_in_s3 = sum(1 for k in later if k in s3_index)
    checks.append(_check("V2", "objetos posteriores al cursor", OK,
                         f"{len(later)} en GCS después del cursor ({later_in_s3} ya en S3); salen en la próxima corrida",
                         after_cursor=len(later)))

    extra = sorted(k for k in s3_index if k not in by_key)
    bad_format = sorted(k for k in s3_index if parse_s3_key(k) is None)
    status = FALLA if bad_format else (AVISO if extra else OK)
    checks.append(_check(
        "V3", "S3 solo tiene claves de enriched/ con su formato", status,
        f"{len(s3_index)} objetos en raw/origin=stream/; sin par en GCS: {len(extra)}; formato distinto: {len(bad_format)}",
        extra=extra[:20], bad_format=bad_format[:20],
    ))

    sample_problems: list[str] = []
    no_bucket_key = 0
    for key, head in sorted(heads.items()):
        obj = by_key.get(key)
        if head.get("error"):
            sample_problems.append(f"{key}: {head['error']}")
            continue
        if head.get("sse") != "aws:kms":
            sample_problems.append(f"{key}: cifrado {head.get('sse')!r}")
        if kms_key_arn and head.get("kms_key_id") != kms_key_arn:
            sample_problems.append(f"{key}: llave {head.get('kms_key_id')!r}")
        if not head.get("bucket_key"):
            no_bucket_key += 1
        if obj is not None:
            if head.get("size") != obj.size:
                sample_problems.append(f"{key}: tamaño {head.get('size')} vs {obj.size}")
            crc = head.get("checksum_crc32c") or (head.get("metadata") or {}).get("gcs-crc32c")
            if obj.crc32c and crc != obj.crc32c:
                sample_problems.append(f"{key}: CRC32C {crc!r} vs GCS {obj.crc32c!r}")
    if not heads:
        checks.append(_check("V4", "SSE-KMS y CRC32C (muestra)", AVISO, "sin muestra (S3 vacío)"))
    else:
        status = FALLA if sample_problems else (AVISO if no_bucket_key else OK)
        detail = f"{len(heads) - len(sample_problems)} de {len(heads)} con SSE-KMS, llave del lake y CRC32C igual a GCS"
        if no_bucket_key:
            detail += f"; {no_bucket_key} sin bucket key"
        checks.append(_check("V4", "SSE-KMS y CRC32C (muestra)", status, detail, problems=sample_problems[:20], sample=len(heads)))

    worst = FALLA if any(c["status"] == FALLA for c in checks) else (AVISO if any(c["status"] == AVISO for c in checks) else OK)
    sources: dict[str, dict[str, int]] = {}
    for key in s3_index:
        parsed = parse_s3_key(key)
        if parsed:
            sources.setdefault(parsed.partition.source, {"objects": 0})["objects"] += 1
    return {
        "status": worst,
        "gcs_enriched": len(by_key),
        "s3_stream": len(s3_index),
        "cursor": cursor.isoformat() if cursor else None,
        "s3_by_source": sources,
        "checks": checks,
    }
