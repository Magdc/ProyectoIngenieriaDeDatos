"""Capas del lake de S3 que perfila el Lote 5.1 (``ARCHITECTURE.md`` §5.7 y §3.20).

Lógica pura (sin pyarrow ni red): columnas y orden de cada capa, claves de S3, enums y
expresiones regulares de §5.2 (las mismas de ``contracts/*.schema.json``; una prueba lo exige).

* **Raw** (``ta_raw.posts_raw``): ``raw/origin={o}/source={s}/dt={AAAA-MM-DD}/hr={HH}/*.parquet``
  con las 24 columnas de ``posts_enriched`` (``common.enriched_row.COLUMN_NAMES``).
* **Trusted** (``ta_trusted.posts``): ``trusted/posts/source={s}/dt={AAAA-MM-DD}/*.parquet``,
  ``dt = DATE(event_time)`` UTC; ``source`` es partición (no columna) y se añade ``text_clean``
  justo después de ``text`` (orden de ``contracts/athena/posts.sql``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.enriched_row import COLUMN_NAMES, POSTS_ENRICHED_COLUMNS, USER_HASH_RE

ORIGINS: tuple[str, ...] = ("stream", "historical")
SOURCES: tuple[str, ...] = ("reddit", "facebook", "instagram")
SENTIMENTS: tuple[str, ...] = ("negative", "neutral", "positive")
EMOTIONS: tuple[str, ...] = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
KINDS: tuple[str, ...] = ("post", "comment")
LANGS: tuple[str, ...] = ("es",)
BRAND_KEYS_V1: tuple[str, ...] = ("postobon", "pepsi", "sprite")
IDENTITY_COLUMNS: tuple[str, ...] = ("user_id", "handle")

#: Primer día que ve la *partition projection* de Athena (``projection.part_dt.range`` y
#: ``projection.dt.range`` en ``contracts/athena/``): una partición anterior no se consulta.
PROJECTION_START = "2026-01-01"

# Mismos patrones que contracts/envelope*.schema.json (test_quality_layers lo comprueba).
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
GENERATOR_RE = re.compile(r"^[a-z0-9][a-z0-9-]*@[0-9]+\.[0-9]+(\.[0-9]+)?$")
LANG_RE = re.compile(r"^[a-z]{2}$")
BRAND_KEY_RE = re.compile(r"^[a-z0-9_]+$")
HASHTAG_RE = re.compile(r"^[a-z0-9_]{1,100}$")
NLP_VERSION_RE = re.compile(r"^robertuito-sentiment@\S+\+robertuito-emotion@\S+$")
HEX64_RE = USER_HASH_RE

RAW_COLUMNS: tuple[str, ...] = tuple(COLUMN_NAMES)


def _trusted_columns() -> tuple[str, ...]:
    out: list[str] = []
    for name in RAW_COLUMNS:
        if name == "source":
            continue
        out.append(name)
        if name == "text":
            out.append("text_clean")
    return tuple(out)


TRUSTED_COLUMNS: tuple[str, ...] = _trusted_columns()

_BQ_KIND = {"STRING": "string", "JSON": "json", "INT64": "int", "FLOAT64": "float", "TIMESTAMP": "timestamp"}

#: Tipo lógico de cada columna: string, json, int, float, timestamp o list (de string).
COLUMN_KINDS: dict[str, str] = {
    c.name: ("list" if c.repeated else _BQ_KIND[c.bq_type]) for c in POSTS_ENRICHED_COLUMNS
}
COLUMN_KINDS["text_clean"] = "string"

#: Columnas cuyos valores nunca se escriben en el informe (solo conteos y cardinalidad).
SENSITIVE_COLUMNS: frozenset[str] = frozenset({"event_id", "user_hash", "text", "text_clean", "payload"})

#: Campos clave del KPI de completitud (§5.10).
KEY_FIELDS: tuple[str, ...] = ("event_id", "event_time", "source", "text", "user_hash")
NLP_COLUMNS: tuple[str, ...] = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version")

RAW_KEY_RE = re.compile(
    r"^raw/origin=(?P<origin>[^/]+)/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hr=(?P<hr>\d{2})/(?P<file>[^/]+\.parquet)$"
)
TRUSTED_KEY_RE = re.compile(r"^trusted/posts/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/(?P<file>[^/]+\.parquet)$")
STREAM_FILE_RE = re.compile(r"^part-(?P<start>\d{8}T\d{6})Z-p(?P<panel>\d{3})\.parquet$")
HISTORICAL_FILE_RE = re.compile(r"^part-historical-s(?P<seed>\d+)-(?P<day>\d{8})T(?P<hh>\d{2})\.parquet$")


@dataclass(frozen=True)
class LayerSpec:
    name: str
    table: str
    prefix: str
    columns: tuple[str, ...]
    key_re: re.Pattern
    partition_keys: tuple[str, ...]


LAYERS: dict[str, LayerSpec] = {
    "raw": LayerSpec("raw", "ta_raw.posts_raw", "raw/", RAW_COLUMNS, RAW_KEY_RE, ("origin", "source", "dt", "hr")),
    "trusted": LayerSpec("trusted", "ta_trusted.posts", "trusted/posts/", TRUSTED_COLUMNS, TRUSTED_KEY_RE, ("source", "dt")),
}


def layer_spec(name: str) -> LayerSpec:
    try:
        return LAYERS[name]
    except KeyError:
        raise ValueError(f"capa desconocida: {name!r} (raw | trusted)") from None


def parse_key(layer: str, key: str) -> dict[str, str] | None:
    """Clave de S3 (relativa al bucket) → particiones y nombre del archivo; ``None`` si no encaja."""
    m = layer_spec(layer).key_re.match(key)
    return m.groupdict() if m else None


def file_name_matches_partition(origin: str | None, part: dict[str, str]) -> bool | None:
    """¿El nombre del archivo coincide con su partición ``dt``/``hr``? (solo Raw; ``None`` si no aplica).

    Stream: ``part-{AAAAMMDD}T{hhmmss}Z-p{panel}.parquet`` con el inicio de la ventana de proceso
    (§5.4). Histórico: ``part-historical-s{seed}-{AAAAMMDD}T{HH}.parquet`` (§5.7).
    """
    name = part.get("file", "")
    dt = part.get("dt", "").replace("-", "")
    hr = part.get("hr")
    if hr is None:
        return None
    if origin == "stream":
        m = STREAM_FILE_RE.match(name)
        return bool(m) and m.group("start")[:8] == dt and m.group("start")[9:11] == hr
    if origin == "historical":
        m = HISTORICAL_FILE_RE.match(name)
        return bool(m) and m.group("day") == dt and m.group("hh") == hr
    return None


def stream_window_start(name: str):
    """Inicio de la ventana de proceso del nombre de un archivo del stream (``datetime`` UTC) o ``None``."""
    from datetime import datetime, timezone

    m = STREAM_FILE_RE.match(name)
    if not m:
        return None
    return datetime.strptime(m.group("start"), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


def historical_seed(name: str) -> str | None:
    m = HISTORICAL_FILE_RE.match(name)
    return m.group("seed") if m else None


DEFAULT_BRANDS_YAML = Path(__file__).resolve().parents[1] / "simulator" / "config" / "brands.yaml"


def load_brand_aliases(path: Path | None = None) -> dict[str, list[str]]:
    """Marcas de ``brands.yaml`` (§3.2.1) → ``{clave: [alias…]}``; si no se puede leer, las 3 de v1."""
    path = path or DEFAULT_BRANDS_YAML
    try:
        import yaml

        doc: Any = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        brands = doc.get("brands") or {}
        out = {}
        for key, info in brands.items():
            aliases = list((info or {}).get("aliases") or [])
            display = (info or {}).get("display")
            if display and display not in aliases:
                aliases.append(display)
            out[str(key)] = aliases or [str(key)]
        if out:
            return out
    except Exception:  # noqa: BLE001 - sin YAML o sin archivo: valores de v1
        pass
    return {k: [k] for k in BRAND_KEYS_V1}
