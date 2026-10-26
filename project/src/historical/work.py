"""Carpetas, archivos de etapa y escrituras atómicas del loader histórico.

Estructura (``root`` es el espejo local del bucket de S3; por defecto
``project/.local-s3/``):

    {root}/raw/origin=historical/source={s}/dt={AAAA-MM-DD}/hr={HH}/part-historical-s{seed}-{AAAAMMDD}T{HH}.parquet
    {root}/_historical/s{seed}/generate.json        plan, estadísticas, digest y seq aceptados
    {root}/_historical/s{seed}/stage1/{source}-{k:04d}.jsonl.gz       enriquecido sin NLP
    {root}/_historical/s{seed}/stage2/{source}-{k:04d}.jsonl.gz(.json) con NLP (+ resumen)
    {root}/_historical/s{seed}/output.json          archivos Parquet escritos
    {root}/_historical/s{seed}/upload.json          última subida a S3

Solo ``raw/`` se sube. ``_historical/`` no contiene identidad en claro: las etapas
guardan el envelope **enriquecido** (sin ``user_id`` ni ``handle``).
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Iterable, Iterator

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_ROOT = PROJECT_DIR / ".local-s3"
HIST_PREFIX = "raw/origin=historical/"
PART_RE = re.compile(
    r"^raw/origin=historical/source=(?P<source>[a-z]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/"
    r"hr=(?P<hr>\d{2})/part-historical-s(?P<seed>\d+)-(?P<day>\d{8})T(?P<hour>\d{2})\.parquet$")
STAGE_RE = re.compile(r"^(?P<source>[a-z]+)-(?P<chunk>\d{4})\.jsonl\.gz$")
HEX64_CHECK = re.compile(r"^[0-9a-f]{64}$")


def part_key(seed: int, source: str, day: str, hour: str) -> str:
    """Clave S3 (y ruta relativa local) de un archivo histórico."""
    return (f"{HIST_PREFIX}source={source}/dt={day}/hr={hour}/"
            f"part-historical-s{seed}-{day.replace('-', '')}T{hour}.parquet")


class WorkDir:
    def __init__(self, root: str | Path, seed: int):
        self.root = Path(root)
        self.seed = int(seed)
        self.dir = self.root / "_historical" / f"s{self.seed}"
        self.stage1 = self.dir / "stage1"
        self.stage2 = self.dir / "stage2"
        self.generate_json = self.dir / "generate.json"
        self.output_json = self.dir / "output.json"
        self.upload_json = self.dir / "upload.json"
        self.nlp_json = self.dir / "nlp.json"

    @property
    def hist_root(self) -> Path:
        return self.root / HIST_PREFIX

    def local_path(self, key: str) -> Path:
        return self.root / key

    def stage1_files(self) -> list[Path]:
        return sorted(p for p in self.stage1.glob("*.jsonl.gz") if STAGE_RE.match(p.name))

    def stage2_files(self) -> list[Path]:
        return sorted(p for p in self.stage2.glob("*.jsonl.gz") if STAGE_RE.match(p.name))

    def own_parts(self) -> list[Path]:
        """Parquet históricos de esta semilla que hay en el espejo local."""
        if not self.hist_root.exists():
            return []
        pattern = f"part-historical-s{self.seed}-*.parquet"
        return sorted(self.hist_root.rglob(pattern))

    def read_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- escrituras

def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Escribe en un temporal y renombra: nunca queda un archivo a medias.

    Sin ``fsync``: con ~12.000 Parquet en ``/mnt/c`` (WSL) cada ``fsync`` cuesta decenas de
    milisegundos, y una caída a mitad se arregla repitiendo la etapa.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def atomic_write_json(path: Path, data: Any) -> None:
    text = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False, default=str)
    atomic_write_bytes(path, (text + "\n").encode("utf-8"))


def gzip_bytes(data: bytes) -> bytes:
    """gzip determinista (``mtime = 0``, sin nombre de archivo)."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=6) as gz:
        gz.write(data)
    return buf.getvalue()


def jsonl_gz_bytes(records: Iterable[Any]) -> bytes:
    lines = [json.dumps(r, ensure_ascii=False, sort_keys=False, separators=(",", ":"))
             for r in records]
    body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
    return gzip_bytes(body)


def read_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def iter_jsonl_gz(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        yield from read_jsonl_gz(path)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def find_identity_keys(value: Any, path: str = "") -> list[str]:
    """Rutas donde aparece una clave ``user_id`` o ``handle`` (a cualquier profundidad)."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, sub in value.items():
            here = f"{path}.{key}" if path else str(key)
            if key in ("user_id", "handle"):
                found.append(here)
            found.extend(find_identity_keys(sub, here))
    elif isinstance(value, list):
        for i, sub in enumerate(value):
            found.extend(find_identity_keys(sub, f"{path}[{i}]"))
    elif isinstance(value, str) and path.endswith("payload") and value[:1] in "{[":
        try:
            found.extend(find_identity_keys(json.loads(value), path))
        except ValueError:
            pass
    return found


def log_event(event: str, **fields: Any) -> None:
    """Log JSON de una línea a stdout (§5.1)."""
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)
