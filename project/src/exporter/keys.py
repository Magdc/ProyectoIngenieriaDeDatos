"""Claves de GCS y S3 del exportador (§5.4 y §5.6).

``enriched/source={s}/dt={AAAA-MM-DD}/hr={HH}/{archivo}.parquet`` en GCS se copia a
``raw/origin=stream/source={s}/dt={AAAA-MM-DD}/hr={HH}/{archivo}.parquet`` en S3: mismas
particiones (hora de proceso) y mismo nombre de archivo.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

SOURCES: tuple[str, ...] = ("reddit", "facebook", "instagram")
GCS_PREFIX = "enriched/"
S3_PREFIX = "raw/origin=stream/"

_GCS_KEY_RE = re.compile(
    r"^enriched/source=(?P<source>reddit|facebook|instagram)"
    r"/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hr=(?P<hr>[01]\d|2[0-3])"
    r"/(?P<name>part-[A-Za-z0-9_.\-]+\.parquet)$"
)
_S3_KEY_RE = re.compile(
    r"^raw/origin=stream/source=(?P<source>reddit|facebook|instagram)"
    r"/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hr=(?P<hr>[01]\d|2[0-3])"
    r"/(?P<name>part-[A-Za-z0-9_.\-]+\.parquet)$"
)


@dataclass(frozen=True, order=True)
class Partition:
    source: str
    dt: str
    hr: str

    def gcs_prefix(self) -> str:
        return f"{GCS_PREFIX}source={self.source}/dt={self.dt}/hr={self.hr}/"

    def s3_prefix(self) -> str:
        return f"{S3_PREFIX}source={self.source}/dt={self.dt}/hr={self.hr}/"


@dataclass(frozen=True)
class ParsedKey:
    partition: Partition
    name: str

    @property
    def s3_key(self) -> str:
        return self.partition.s3_prefix() + self.name

    @property
    def gcs_key(self) -> str:
        return self.partition.gcs_prefix() + self.name


def _valid_date(text: str) -> bool:
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _parse(regex: re.Pattern, key: str) -> ParsedKey | None:
    match = regex.match(key or "")
    if not match or not _valid_date(match.group("dt")):
        return None
    part = Partition(match.group("source"), match.group("dt"), match.group("hr"))
    return ParsedKey(part, match.group("name"))


def parse_gcs_key(key: str) -> ParsedKey | None:
    """La clave reconocida de ``enriched/`` o ``None`` (otros objetos no se exportan)."""
    return _parse(_GCS_KEY_RE, key)


def parse_s3_key(key: str) -> ParsedKey | None:
    return _parse(_S3_KEY_RE, key)


def to_s3_key(gcs_key: str) -> str:
    parsed = parse_gcs_key(gcs_key)
    if parsed is None:
        raise ValueError(f"no es una clave de enriched/ exportable: {gcs_key!r}")
    return parsed.s3_key


def source_prefix(source: str) -> str:
    if source not in SOURCES:
        raise ValueError(f"fuente desconocida: {source!r}")
    return f"{GCS_PREFIX}source={source}/"


def start_offset(source: str, since: datetime) -> str:
    """Primer nombre a listar para la partición que contiene ``since`` (orden lexicográfico).

    Las claves ``dt=AAAA-MM-DD/hr=HH`` ordenan igual que el tiempo, así que listar desde
    aquí devuelve esa partición y todas las posteriores.
    """
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    since = since.astimezone(timezone.utc)
    return f"{source_prefix(source)}dt={since:%Y-%m-%d}/hr={since:%H}/"
