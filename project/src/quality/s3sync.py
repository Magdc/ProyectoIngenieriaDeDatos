"""Caché local de los Parquet del lake (solo lectura de S3) para el *profiling* (§3.20).

    {cache}/s3/{bucket}/{clave de S3}            # mismo layout que el bucket
    {cache}/s3/{bucket}/_sync/{capa}.json         # manifiesto del último listado

``cache`` = ``--cache`` > ``TA_QUALITY_CACHE`` > ``~/.cache/trendanalyzer/quality``: **fuera del
repositorio** (``resolve_cache`` se niega a usar una carpeta dentro del repo: los datos nunca van a
Git). Solo usa ``ListObjectsV2`` y ``GetObject`` (con SSE-KMS, S3 descifra con ``kms:Decrypt`` del
perfil). Un objeto ya presente con el mismo tamaño no se vuelve a bajar: repetir cuesta solo el
listado. Nunca borra nada en S3; en local tampoco (los archivos que ya no están en S3 se cuentan
como ``stale``).
"""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CACHE = Path("~/.cache/trendanalyzer/quality")
DEFAULT_WORKERS = 16
LAYER_PREFIXES = {"raw": "raw/", "trusted": "trusted/posts/"}


class CacheError(RuntimeError):
    """Configuración de la caché inválida (p. ej. dentro del repositorio)."""


class S3ReadPort(Protocol):
    def list_objects(self, bucket: str, prefix: str) -> list[dict[str, Any]]: ...  # [{key, size, etag}]

    def download(self, bucket: str, key: str, dest: Path) -> None: ...


def resolve_cache(cache: str | Path | None = None, *, env: dict[str, str] | None = None, repo_root: Path = REPO_ROOT) -> Path:
    env = os.environ if env is None else env
    raw = cache or env.get("TA_QUALITY_CACHE") or DEFAULT_CACHE
    path = Path(raw).expanduser().resolve()
    try:
        path.relative_to(repo_root.resolve())
    except ValueError:
        return path
    raise CacheError(f"la caché {path} está dentro del repositorio: use una carpeta fuera (TA_QUALITY_CACHE o --cache); "
                     "los datos nunca se versionan")


def bucket_root(cache: Path, bucket: str) -> Path:
    return cache / "s3" / bucket


@dataclass
class SyncReport:
    bucket: str
    prefix: str
    cache_root: str
    started_at: str
    finished_at: str = ""
    duration_s: float = 0.0
    objects: int = 0
    bytes: int = 0
    to_download: int = 0
    downloaded: int = 0
    downloaded_bytes: int = 0
    skipped_same_size: int = 0
    failed: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)
    stale_local: int = 0
    dry_run: bool = False
    by_prefix: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["ok"] = self.ok
        return d


def _group_key(key: str, prefix: str) -> str:
    rest = key[len(prefix):] if key.startswith(prefix) else key
    parts = rest.split("/")
    # raw/: origin=…/source=…  ·  trusted/posts/: source=…
    head = [p for p in parts[:2] if "=" in p and not p.startswith(("dt=", "hr="))]
    return "/".join(head) or "(raíz)"


def sync_prefix(
    port: S3ReadPort,
    bucket: str,
    prefix: str,
    cache: Path,
    *,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
    progress: Callable[[str], None] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> SyncReport:
    """Baja a ``{cache}/s3/{bucket}/`` lo que falte de ``s3://{bucket}/{prefix}`` y escribe el manifiesto."""
    started = time.monotonic()
    root = bucket_root(cache, bucket)
    rep = SyncReport(bucket, prefix, str(root), now().strftime("%Y-%m-%dT%H:%M:%SZ"), dry_run=dry_run)
    objects = port.list_objects(bucket, prefix)
    rep.objects = len(objects)
    rep.bytes = sum(int(o["size"]) for o in objects)
    todo = []
    for o in objects:
        key, size = o["key"], int(o["size"])
        g = rep.by_prefix.setdefault(_group_key(key, prefix), {"objects": 0, "bytes": 0})
        g["objects"] += 1
        g["bytes"] += size
        dest = root / key
        if dest.exists() and dest.stat().st_size == size:
            rep.skipped_same_size += 1
        else:
            todo.append((key, size, dest))
    rep.to_download = len(todo)
    remote = {o["key"] for o in objects}
    base = root / prefix
    if base.exists():
        rep.stale_local = sum(1 for p in base.rglob("*") if p.is_file() and p.relative_to(root).as_posix() not in remote)
    if progress:
        progress(f"{rep.objects} objetos en s3://{bucket}/{prefix} ({rep.bytes / 1e6:.1f} MB); por bajar {len(todo)}")
    if not dry_run and todo:
        lock = threading.Lock()
        done = [0]

        def fetch(item):
            key, size, dest = item
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name(dest.name + ".part")
            try:
                port.download(bucket, key, tmp)
                os.replace(tmp, dest)
            finally:
                if tmp.exists():
                    tmp.unlink()
            return size

        with ThreadPoolExecutor(max_workers=max(1, min(workers, 32))) as pool:
            futures = {pool.submit(fetch, item): item for item in todo}
            for fut in as_completed(futures):
                key, size, _ = futures[fut]
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        rep.failed += 1
                        if len(rep.failures) < 20:
                            rep.failures.append({"key": key, "error": f"{type(exc).__name__}: {str(exc)[:160]}"})
                    continue
                with lock:
                    rep.downloaded += 1
                    rep.downloaded_bytes += size
                    done[0] += 1
                    if progress and done[0] % 1000 == 0:
                        progress(f"bajados {done[0]} de {len(todo)}")
    rep.finished_at = now().strftime("%Y-%m-%dT%H:%M:%SZ")
    rep.duration_s = round(time.monotonic() - started, 1)
    if not dry_run:
        manifest = root / "_sync" / f"{_layer_of(prefix)}.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(rep.to_json(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return rep


def _layer_of(prefix: str) -> str:
    """Nombre del manifiesto: la capa si se listó entera (``raw/``); si no, el prefijo saneado."""
    for layer, p in LAYER_PREFIXES.items():
        if prefix == p:
            return layer
    return prefix.strip("/").replace("/", "_").replace("=", "-") or "root"


def read_manifest(cache: Path, bucket: str, layer: str) -> dict[str, Any] | None:
    path = bucket_root(cache, bucket) / "_sync" / f"{layer}.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


class Boto3S3:
    """Adaptador real (boto3). ``profile`` = perfil de AWS CLI (solo lectura del lake)."""

    def __init__(self, *, profile: str | None = None, region: str | None = None, client: Any = None):
        if client is None:
            import boto3
            from botocore.config import Config

            session = boto3.Session(profile_name=profile) if profile else boto3.Session()
            client = session.client("s3", region_name=region or os.environ.get("AWS_REGION", "us-east-1"),
                                    config=Config(max_pool_connections=DEFAULT_WORKERS + 4, retries={"max_attempts": 5, "mode": "standard"}))
        self.client = client

    def list_objects(self, bucket: str, prefix: str) -> list[dict[str, Any]]:
        out = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []) or []:
                out.append({"key": o["Key"], "size": int(o["Size"]), "etag": str(o.get("ETag", "")).strip('"')})
        return out

    def download(self, bucket: str, key: str, dest: Path) -> None:
        self.client.download_file(bucket, key, str(dest))


def account_id(profile: str | None = None, client: Any = None) -> str:
    """Cuenta de AWS del perfil (``sts get-caller-identity``)."""
    if client is None:
        import boto3

        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        client = session.client("sts")
    return str(client.get_caller_identity()["Account"])


def resolve_bucket(bucket: str | None = None, *, env: dict[str, str] | None = None, account: Callable[[], str] | None = None) -> str:
    """``--bucket`` > ``TA_LAKE_BUCKET`` > ``ta-{ENV}-{cuenta}-lake`` (nombre de §5.7 con la cuenta del perfil)."""
    env = os.environ if env is None else env
    if bucket:
        return bucket
    if env.get("TA_LAKE_BUCKET"):
        return env["TA_LAKE_BUCKET"]
    if account is None:
        raise CacheError("sin bucket: --bucket, TA_LAKE_BUCKET o credenciales de AWS para derivarlo")
    return f"ta-{env.get('ENV', 'dev')}-{account()}-lake"
