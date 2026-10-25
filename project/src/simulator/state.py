"""Cursor persistente del simulador (§3.6, HU-013).

Formato del objeto ``sim-state/{source}.json`` (en ``gs://{GCP_PROJECT}-ta-datasets/``):

    {"seed": 7, "seq": 1207, "updated_at": "2026-10-17T14:03:13.020Z"}

``seq`` es el **siguiente** evento a emitir. Si la semilla guardada no coincide con
``SIM_SEED`` se arranca en ``seq = 0`` (los ``event_id`` nuevos no chocan con los viejos
porque la semilla forma parte del id).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Protocol

STATE_PREFIX = "sim-state"


def _now_iso() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class Cursor:
    seed: int
    seq: int
    updated_at: str = ""

    def to_json(self) -> str:
        return json.dumps({"seed": self.seed, "seq": self.seq, "updated_at": self.updated_at or _now_iso()})

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Cursor":
        d = json.loads(raw)
        seed, seq = int(d["seed"]), int(d["seq"])
        if seq < 0:
            raise ValueError("seq negativo en el cursor")
        return cls(seed=seed, seq=seq, updated_at=str(d.get("updated_at", "")))


def object_name(source: str) -> str:
    return f"{STATE_PREFIX}/{source}.json"


class StateStore(Protocol):
    def load(self, source: str) -> Optional[Cursor]: ...
    def save(self, source: str, cursor: Cursor) -> None: ...


def resume_seq(store: StateStore, source: str, seed: int, log=None) -> int:
    """``seq`` desde el que continuar: el del cursor si la semilla coincide, si no 0."""
    try:
        cur = store.load(source)
    except (ValueError, KeyError) as exc:  # cursor corrupto: se empieza de cero
        if log:
            log("cursor_invalid", source=source, error=str(exc))
        return 0
    if cur is None:
        return 0
    if cur.seed != seed:
        if log:
            log("cursor_seed_mismatch", source=source, cursor_seed=cur.seed, seed=seed)
        return 0
    return cur.seq


class MemoryStateStore:
    """Para pruebas."""

    def __init__(self) -> None:
        self.objects: dict[str, str] = {}
        self.saves = 0

    def load(self, source: str) -> Optional[Cursor]:
        raw = self.objects.get(object_name(source))
        return None if raw is None else Cursor.from_json(raw)

    def save(self, source: str, cursor: Cursor) -> None:
        self.objects[object_name(source)] = cursor.to_json()
        self.saves += 1


class LocalStateStore:
    """Carpeta local con la misma estructura que el bucket (desarrollo, Lote 1.5)."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)

    def _path(self, source: str) -> Path:
        return self.root / object_name(source)

    def load(self, source: str) -> Optional[Cursor]:
        p = self._path(source)
        if not p.exists():
            return None
        return Cursor.from_json(p.read_text(encoding="utf-8"))

    def save(self, source: str, cursor: Cursor) -> None:
        p = self._path(source)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=p.parent, prefix=f".{source}.", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(cursor.to_json())
        os.replace(tmp, p)  # escritura atómica


class GcsStateStore:
    """Bucket ``{GCP_PROJECT}-ta-datasets`` (o el que se configure)."""

    def __init__(self, bucket: str, client=None) -> None:
        if not bucket:
            raise ValueError("falta el bucket de estado (SIM_STATE_BUCKET)")
        if client is None:
            from google.cloud import storage  # import diferido

            client = storage.Client()
        self._bucket = client.bucket(bucket)

    def load(self, source: str) -> Optional[Cursor]:
        blob = self._bucket.blob(object_name(source))
        if not blob.exists():
            return None
        return Cursor.from_json(blob.download_as_bytes())

    def save(self, source: str, cursor: Cursor) -> None:
        blob = self._bucket.blob(object_name(source))
        blob.upload_from_string(cursor.to_json(), content_type="application/json")


def store_from_env(env: dict | None = None) -> StateStore:
    """``SIM_STATE_BACKEND`` = ``gcs`` (por defecto) | ``local`` | ``memory``."""
    env = dict(os.environ if env is None else env)
    backend = (env.get("SIM_STATE_BACKEND") or "gcs").lower()
    if backend == "memory":
        return MemoryStateStore()
    if backend == "local":
        return LocalStateStore(env.get("SIM_STATE_DIR") or ".sim-state")
    if backend == "gcs":
        bucket = env.get("SIM_STATE_BUCKET") or (
            f"{env['GCP_PROJECT']}-ta-datasets" if env.get("GCP_PROJECT") else ""
        )
        return GcsStateStore(bucket)
    raise ValueError(f"SIM_STATE_BACKEND desconocido: {backend!r}")
