"""Lectura de `raw/` y `dlq/` del lake (GCS o carpeta local) para verificar el pipeline.

No depende de Beam: usa `google-cloud-storage` para `gs://` y el sistema de
archivos para rutas locales. Solo lee.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from streaming.records import DLQ_PREFIX, RAW_PREFIX


class LocalLake:
    def __init__(self, root: str):
        self.root = Path(root)
        self.label = str(self.root)

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.is_dir():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*.jsonl.gz") if p.is_file())

    def read(self, key: str) -> bytes:
        return (self.root / key).read_bytes()


class GcsLake:
    def __init__(self, root: str, client=None):
        bucket, _, base = root[len("gs://"):].partition("/")
        if not bucket:
            raise ValueError(f"ruta de GCS inválida: {root}")
        if client is None:
            from google.cloud import storage

            client = storage.Client()
        self._bucket = client.bucket(bucket)
        self._client = client
        self._bucket_name = bucket
        self._base = base.strip("/")
        self.label = root.rstrip("/")

    def _key(self, rel: str) -> str:
        return f"{self._base}/{rel}" if self._base else rel

    def list(self, prefix: str) -> list[str]:
        full = self._key(prefix).rstrip("/") + "/"
        cut = len(self._base) + 1 if self._base else 0
        return sorted(b.name[cut:] for b in self._client.list_blobs(self._bucket_name, prefix=full)
                      if b.name.endswith(".jsonl.gz"))

    def read(self, key: str) -> bytes:
        return self._bucket.blob(self._key(key)).download_as_bytes()


def open_lake(root: str, client=None):
    if root.startswith("gs://"):
        return GcsLake(root, client=client)
    return LocalLake(root)


def hours_back(now: datetime, hours: int) -> list[datetime]:
    """Horas UTC (truncadas) desde `now - hours` hasta `now`, ambas incluidas."""
    now = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [now - timedelta(hours=h) for h in range(hours, -1, -1)]


def _lines(blob: bytes) -> list[str]:
    return [ln for ln in gzip.decompress(blob).decode("utf-8").split("\n") if ln.strip()]


@dataclass
class RawStats:
    files: int = 0
    lines: int = 0
    bad_lines: int = 0
    ids: Counter = field(default_factory=Counter)

    def summary(self) -> dict:
        repeated = {k: v for k, v in self.ids.items() if v > 1}
        return {"files": self.files, "lines": self.lines, "unique_event_ids": len(self.ids),
                "duplicate_event_ids": len(repeated),
                "extra_copies": sum(v - 1 for v in repeated.values()),
                "bad_lines": self.bad_lines}


@dataclass
class LakeScan:
    root: str
    hours: list[str]
    raw: dict[str, RawStats]
    dlq_files: int
    dlq_rows: list[dict]

    def raw_lines_with(self, needle: str) -> int:
        return sum(stats.ids.get(needle, 0) for stats in self.raw.values())

    def summary(self) -> dict:
        by_type = Counter(r.get("error_type") for r in self.dlq_rows)
        by_source = Counter(r.get("source") for r in self.dlq_rows)
        return {
            "root": self.root,
            "hours_scanned": self.hours,
            "raw": {s: st.summary() for s, st in self.raw.items()},
            "raw_total_lines": sum(st.lines for st in self.raw.values()),
            "raw_total_extra_copies": sum(st.summary()["extra_copies"] for st in self.raw.values()),
            "dlq": {"files": self.dlq_files, "records": len(self.dlq_rows),
                    "by_error_type": dict(sorted(by_type.items())),
                    "by_source": dict(sorted(by_source.items()))},
        }


def scan(lake, sources, hours: int = 2, now: datetime | None = None) -> LakeScan:
    """Lee las particiones de las últimas `hours` horas (por fecha/hora de proceso)."""
    now = now or datetime.now(timezone.utc)
    slots = hours_back(now, hours)
    raw: dict[str, RawStats] = {s: RawStats() for s in sources}
    for source in sources:
        stats = raw[source]
        for slot in slots:
            prefix = f"{RAW_PREFIX}/source={source}/dt={slot:%Y-%m-%d}/hr={slot:%H}"
            for key in lake.list(prefix):
                stats.files += 1
                for line in _lines(lake.read(key)):
                    stats.lines += 1
                    try:
                        obj = json.loads(line)
                        if obj.get("source") != source:
                            raise ValueError("fuente distinta de la partición")
                        stats.ids[obj["event_id"]] += 1
                    except (ValueError, KeyError, AttributeError):
                        stats.bad_lines += 1

    dlq_rows: list[dict] = []
    dlq_files = 0
    for day in sorted({f"{s:%Y-%m-%d}" for s in slots}):
        for key in lake.list(f"{DLQ_PREFIX}/dt={day}"):
            dlq_files += 1
            for line in _lines(lake.read(key)):
                try:
                    dlq_rows.append(json.loads(line))
                except ValueError:
                    dlq_rows.append({"error_type": "?", "raw_message": line})
    return LakeScan(root=lake.label, hours=[f"{s:%Y-%m-%dT%H}" for s in slots], raw=raw,
                    dlq_files=dlq_files, dlq_rows=dlq_rows)
