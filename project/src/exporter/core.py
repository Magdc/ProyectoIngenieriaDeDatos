"""Corrida del exportador: listar, decidir, subir y mover el watermark (§5.6).

La lógica no conoce los clientes reales: recibe un ``GcsPort`` y un ``S3Port``
(``gcs_io.GcsLake`` y ``s3_io.S3Lake`` en producción; dobles en ``testing``).
"""

from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Protocol

from exporter import EXPORTER_ID
from exporter.config import ExporterConfig
from exporter.jsonlog import LogFn, format_ts, null_logger
from exporter.keys import Partition, parse_gcs_key, source_prefix, start_offset
from exporter.watermark import (
    Watermark,
    WatermarkError,
    candidate_threshold,
    listing_since,
    next_cursor,
)

UPLOAD = "upload"
OVERWRITE = "overwrite"
SKIP = "skip_same_size"
MAX_LISTED_NAMES = 20


class StateConflict(Exception):
    """Otra corrida cambió el watermark entre la lectura y la escritura."""


class EncryptionMismatch(RuntimeError):
    """S3 respondió con un cifrado distinto del esperado."""


@dataclass(frozen=True)
class GcsObject:
    name: str
    size: int
    generation: int
    time_created: datetime
    crc32c: str | None = None
    md5: str | None = None


class GcsPort(Protocol):
    def list_objects(self, prefix: str, start_offset: str | None = None) -> Iterable[GcsObject]: ...

    def download(self, obj: GcsObject) -> bytes: ...

    def read_state(self, key: str) -> tuple[dict | None, int]: ...

    def write_state(self, key: str, data: dict, if_generation_match: int) -> int: ...


class S3Port(Protocol):
    def list_prefix(self, prefix: str) -> dict[str, int]: ...

    def put_object(
        self, key: str, body: bytes, *, crc32c: str | None, md5: str | None, metadata: dict[str, str]
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class Decision:
    obj: GcsObject
    s3_key: str
    action: str
    s3_size: int | None = None


@dataclass
class ExportReport:
    run_id: str
    dry_run: bool
    started_at: datetime
    finished_at: datetime | None = None
    duration_s: float = 0.0
    listed: int = 0
    unrecognized: int = 0
    unrecognized_names: list[str] = field(default_factory=list)
    candidates: int = 0
    partitions_checked: int = 0
    to_upload: int = 0
    uploaded: int = 0
    overwritten: int = 0
    skipped_same_size: int = 0
    pending: int = 0
    truncated: bool = False
    bytes_uploaded: int = 0
    failed: list[dict[str, str]] = field(default_factory=list)
    uploaded_keys: list[str] = field(default_factory=list)
    planned_keys: list[str] = field(default_factory=list)
    watermark_before: str | None = None
    watermark_after: str | None = None
    watermark_written: bool = False
    watermark_conflicts: int = 0
    by_source: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failed

    def to_json(self, *, keys_limit: int = 200) -> dict[str, Any]:
        return {
            "exporter": EXPORTER_ID,
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "ok": self.ok,
            "started_at": format_ts(self.started_at),
            "finished_at": format_ts(self.finished_at),
            "duration_s": round(self.duration_s, 3),
            "listed": self.listed,
            "unrecognized": self.unrecognized,
            "unrecognized_names": self.unrecognized_names,
            "candidates": self.candidates,
            "partitions_checked": self.partitions_checked,
            "to_upload": self.to_upload,
            "uploaded": self.uploaded,
            "overwritten": self.overwritten,
            "skipped_same_size": self.skipped_same_size,
            "pending": self.pending,
            "truncated": self.truncated,
            "bytes_uploaded": self.bytes_uploaded,
            "failed": self.failed[:50],
            "failed_count": len(self.failed),
            "uploaded_keys": self.uploaded_keys[:keys_limit],
            "planned_keys": self.planned_keys[:keys_limit],
            "watermark_before": self.watermark_before,
            "watermark_after": self.watermark_after,
            "watermark_written": self.watermark_written,
            "watermark_conflicts": self.watermark_conflicts,
            "by_source": self.by_source,
        }


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%S}Z-{uuid.uuid4().hex[:8]}"


def build_plan(candidates: Iterable[GcsObject], s3_index: dict[str, int]) -> list[Decision]:
    """Qué hacer con cada candidato según lo que ya está en S3 (mismo nombre y tamaño)."""
    plan: list[Decision] = []
    for obj in candidates:
        parsed = parse_gcs_key(obj.name)
        if parsed is None:  # pragma: no cover - los candidatos ya vienen filtrados
            continue
        key = parsed.s3_key
        existing = s3_index.get(key)
        if existing is None:
            plan.append(Decision(obj, key, UPLOAD))
        elif existing == obj.size:
            plan.append(Decision(obj, key, SKIP, existing))
        else:
            plan.append(Decision(obj, key, OVERWRITE, existing))
    return plan


def _metadata(obj: GcsObject, run_id: str) -> dict[str, str]:
    meta = {
        "gcs-generation": str(obj.generation),
        "exporter": EXPORTER_ID,
        "export-run-id": run_id,
    }
    if obj.crc32c:
        meta["gcs-crc32c"] = obj.crc32c
    if obj.md5:
        meta["gcs-md5"] = obj.md5
    return meta


def _check_encryption(resp: dict[str, Any], kms_key_arn: str | None) -> None:
    sse = resp.get("sse")
    if sse != "aws:kms":
        raise EncryptionMismatch(f"S3 respondió ServerSideEncryption={sse!r} (se esperaba aws:kms)")
    got = resp.get("kms_key_id")
    if kms_key_arn and got and got != kms_key_arn:
        raise EncryptionMismatch(f"S3 cifró con {got} y no con la llave del lake {kms_key_arn}")


def _upload_one(gcs: GcsPort, s3: S3Port, decision: Decision, cfg: ExporterConfig, run_id: str) -> int:
    body = gcs.download(decision.obj)
    if len(body) != decision.obj.size:
        raise RuntimeError(f"descargados {len(body)} bytes y GCS dice {decision.obj.size}")
    resp = s3.put_object(
        decision.s3_key,
        body,
        crc32c=decision.obj.crc32c,
        md5=decision.obj.md5,
        metadata=_metadata(decision.obj, run_id),
    )
    _check_encryption(resp, cfg.kms_key_arn)
    return len(body)


def _write_watermark(
    gcs: GcsPort,
    cfg: ExporterConfig,
    generation: int,
    cursor: datetime | None,
    run_id: str,
    last_run: dict[str, Any],
    now: datetime,
    log: LogFn,
    attempts: int = 3,
) -> tuple[datetime | None, int]:
    """Escribe con precondición de generación; si otra corrida escribió antes, se queda con
    el cursor mayor y reintenta. Devuelve (cursor escrito, conflictos)."""
    conflicts = 0
    gen = generation
    for _ in range(attempts):
        wm = Watermark(cursor=cursor, updated_at=now, run_id=run_id, exporter=EXPORTER_ID, last_run=last_run)
        try:
            gcs.write_state(cfg.state_key, wm.to_json(), if_generation_match=gen)
            return cursor, conflicts
        except StateConflict:
            conflicts += 1
            data, gen = gcs.read_state(cfg.state_key)
            other = Watermark.from_json(data)
            if other.cursor is not None and (cursor is None or other.cursor > cursor):
                cursor = other.cursor
            log("export_watermark_conflict", "WARNING", run_id=run_id, generation=gen)
    raise StateConflict(f"el watermark cambió {conflicts} veces seguidas; no se escribió")


def run_export(
    cfg: ExporterConfig,
    gcs: GcsPort,
    s3: S3Port,
    *,
    dry_run: bool = False,
    log: LogFn = null_logger,
    now: Callable[[], datetime] | None = None,
    run_id: str | None = None,
) -> ExportReport:
    clock = now or (lambda: datetime.now(timezone.utc))
    t0 = time.monotonic()
    started = clock()
    run_id = run_id or new_run_id(started)
    report = ExportReport(run_id=run_id, dry_run=dry_run, started_at=started)
    log("export_start", run_id=run_id, dry_run=dry_run, config=cfg.public())

    data, generation = gcs.read_state(cfg.state_key)
    try:
        wm = Watermark.from_json(data)
    except WatermarkError as exc:
        log("export_watermark_invalid", "ERROR", run_id=run_id, detail=str(exc))
        raise
    report.watermark_before = format_ts(wm.cursor)

    since = listing_since(wm, cfg.overlap_seconds, cfg.margin_seconds)
    threshold = candidate_threshold(wm, cfg.overlap_seconds)
    candidates: list[GcsObject] = []
    for source in cfg.sources:
        prefix = source_prefix(source)
        offset = start_offset(source, since) if since else None
        stats = report.by_source.setdefault(source, {"listed": 0, "candidates": 0, "uploaded": 0})
        for obj in gcs.list_objects(prefix, offset):
            report.listed += 1
            stats["listed"] += 1
            if parse_gcs_key(obj.name) is None:
                report.unrecognized += 1
                if len(report.unrecognized_names) < MAX_LISTED_NAMES:
                    report.unrecognized_names.append(obj.name)
                continue
            if threshold is not None and obj.time_created <= threshold:
                continue
            candidates.append(obj)
            stats["candidates"] += 1
    report.candidates = len(candidates)

    partitions = sorted({parse_gcs_key(o.name).partition for o in candidates})  # type: ignore[union-attr]
    s3_index: dict[str, int] = {}
    for part in partitions:
        s3_index.update(s3.list_prefix(part.s3_prefix()))
    report.partitions_checked = len(partitions)

    plan = build_plan(candidates, s3_index)
    report.skipped_same_size = sum(1 for d in plan if d.action == SKIP)
    todo = sorted((d for d in plan if d.action != SKIP), key=lambda d: (d.obj.time_created, d.obj.name))
    report.to_upload = len(todo)
    batch = todo[: cfg.max_objects]
    pending = todo[cfg.max_objects :]
    report.truncated = bool(pending)
    report.pending = len(pending)
    report.planned_keys = [d.s3_key for d in batch]
    log(
        "export_plan",
        run_id=run_id,
        listed=report.listed,
        candidates=report.candidates,
        partitions=report.partitions_checked,
        to_upload=report.to_upload,
        skipped_same_size=report.skipped_same_size,
        truncated=report.truncated,
        unrecognized=report.unrecognized,
    )

    if dry_run:
        report.finished_at = clock()
        report.duration_s = time.monotonic() - t0
        log("export_done", run_id=run_id, **_summary(report))
        return report

    if batch:
        with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as pool:
            futures = {pool.submit(_upload_one, gcs, s3, d, cfg, run_id): d for d in batch}
            for future in as_completed(futures):
                decision = futures[future]
                source = parse_gcs_key(decision.obj.name).partition.source  # type: ignore[union-attr]
                try:
                    size = future.result()
                except Exception as exc:  # noqa: BLE001 - cada objeto falla por separado
                    detail = f"{type(exc).__name__}: {exc}"[:500]
                    report.failed.append({"name": decision.obj.name, "error": detail})
                    log("export_object_failed", "ERROR", run_id=run_id, name=decision.obj.name, error=detail)
                    continue
                report.bytes_uploaded += size
                report.uploaded_keys.append(decision.s3_key)
                report.by_source[source]["uploaded"] += 1
                if decision.action == OVERWRITE:
                    report.overwritten += 1
                    log(
                        "export_object_overwritten",
                        "WARNING",
                        run_id=run_id,
                        key=decision.s3_key,
                        s3_size=decision.s3_size,
                        gcs_size=decision.obj.size,
                    )
                else:
                    report.uploaded += 1
        report.uploaded_keys.sort()

    if report.failed:
        report.watermark_after = report.watermark_before
        report.finished_at = clock()
        report.duration_s = time.monotonic() - t0
        log("export_done", "ERROR", run_id=run_id, **_summary(report))
        return report

    cursor = next_cursor(
        wm.cursor,
        [o.time_created for o in candidates],
        [d.obj.time_created for d in pending],
    )
    finished = clock()
    report.duration_s = time.monotonic() - t0
    last_run = {
        "run_id": run_id,
        "finished_at": format_ts(finished),
        "listed": report.listed,
        "candidates": report.candidates,
        "uploaded": report.uploaded,
        "overwritten": report.overwritten,
        "skipped_same_size": report.skipped_same_size,
        "pending": report.pending,
        "bytes_uploaded": report.bytes_uploaded,
        "duration_s": round(report.duration_s, 3),
    }
    written, conflicts = _write_watermark(gcs, cfg, generation, cursor, run_id, last_run, finished, log)
    report.watermark_conflicts = conflicts
    report.watermark_written = True
    report.watermark_after = format_ts(written)
    report.finished_at = clock()
    report.duration_s = time.monotonic() - t0
    log("export_watermark_updated", run_id=run_id, cursor=report.watermark_after, conflicts=conflicts)
    log("export_done", run_id=run_id, **_summary(report))
    return report


def _summary(report: ExportReport) -> dict[str, Any]:
    return {
        "ok": report.ok,
        "dry_run": report.dry_run,
        "listed": report.listed,
        "candidates": report.candidates,
        "to_upload": report.to_upload,
        "uploaded": report.uploaded,
        "overwritten": report.overwritten,
        "skipped_same_size": report.skipped_same_size,
        "pending": report.pending,
        "failed": len(report.failed),
        "bytes_uploaded": report.bytes_uploaded,
        "duration_s": round(report.duration_s, 3),
        "watermark_before": report.watermark_before,
        "watermark_after": report.watermark_after,
        "by_source": report.by_source,
    }


def partitions_of(objs: Iterable[GcsObject]) -> list[Partition]:
    return sorted({p.partition for p in (parse_gcs_key(o.name) for o in objs) if p is not None})
