"""Lectura del cursor del simulador (``sim-state/{source}.json``, §3.6).

Solo lee: el verificador nunca escribe ni borra el cursor.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from ingest_check.stats import CursorState

CURSOR_PREFIX = "sim-state"


def parse_cursor(raw: bytes | str) -> CursorState:
    """Interpreta ``{"seed": 7, "seq": 1207, "updated_at": "…"}``. Lanza ``ValueError`` si no cuadra."""
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"cursor no es JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("cursor no es un objeto JSON")
    seed, seq = data.get("seed"), data.get("seq")
    for name, value in (("seed", seed), ("seq", seq)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"cursor sin '{name}' entero")
    updated = data.get("updated_at")
    return CursorState(seed=seed, seq=seq, updated_at=updated if isinstance(updated, str) else None)


class CursorReader(Protocol):
    def read(self, source: str) -> CursorState | None: ...

    def describe(self) -> str: ...


class NullCursorReader:
    def read(self, source: str) -> CursorState | None:
        return None

    def describe(self) -> str:
        return "sin lectura de cursor"


class LocalCursorReader:
    """Backend ``local`` del simulador (``SIM_STATE_DIR``).

    Acepta tanto ``{dir}/{source}.json`` como ``{dir}/sim-state/{source}.json``.
    """

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    def read(self, source: str) -> CursorState | None:
        for path in (self.directory / f"{source}.json", self.directory / CURSOR_PREFIX / f"{source}.json"):
            if path.is_file():
                return parse_cursor(path.read_bytes())
        return None

    def describe(self) -> str:
        return f"local: {self.directory}"


class GcsCursorReader:
    """Backend ``gcs`` del simulador: ``gs://{bucket}/sim-state/{source}.json``."""

    def __init__(self, bucket: str, project: str | None = None) -> None:
        from google.cloud import storage  # import diferido: solo si se usa GCS

        self.bucket_name = bucket
        self._bucket = storage.Client(project=project).bucket(bucket)

    def read(self, source: str) -> CursorState | None:
        blob = self._bucket.blob(f"{CURSOR_PREFIX}/{source}.json")
        if not blob.exists():
            return None
        return parse_cursor(blob.download_as_bytes())

    def describe(self) -> str:
        return f"gs://{self.bucket_name}/{CURSOR_PREFIX}/"


def read_all(reader: CursorReader, sources: list[str]) -> dict[str, CursorState | None]:
    """Lee el cursor de cada fuente; un cursor ilegible cuenta como ausente."""
    out: dict[str, CursorState | None] = {}
    for s in sources:
        try:
            out[s] = reader.read(s)
        except ValueError:
            out[s] = None
    return out
