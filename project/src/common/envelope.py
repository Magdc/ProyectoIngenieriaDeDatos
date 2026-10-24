"""Envelope canónico v1.0 (ARCHITECTURE.md §3.6, §5.2 y §5.3).

Funciones puras y sin estado: las usan el simulador, el loader histórico y las pruebas.
No hace nada de red ni de disco.
"""
from __future__ import annotations

import random
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

from .hashtags import CANONICAL_RE as HASHTAG_RE
from .hashtags import extract_hashtags

SCHEMA_VERSION = "1.0"
ORIGINS = ("stream", "historical")
SOURCES = ("reddit", "facebook", "instagram")
SENTIMENTS = ("positive", "negative", "neutral")
CHAOS_VALUES = ("duplicate", "late", "null_fields", "malformed")

_BRAND_RE = re.compile(r"^[a-z0-9_]+$")
_GENERATOR_RE = re.compile(r"^[^@\s]+@[^@\s]+$")
_LANG_RE = re.compile(r"^[a-z]{2}$")


# ------------------------------------------------------------------ identidad (§3.6)
def event_id(origin: str, source: str, seed: int | str, seq: int) -> str:
    """``uuid5(NAMESPACE_URL, "{origin}|{source}|{seed}|{seq}")``: determinista y en minúsculas."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def event_rng(seed: int | str, source: str, seq: int) -> random.Random:
    """RNG propio de un evento: ``Random(f"{seed}|{source}|{seq}")``.

    Con semilla de tipo ``str`` Python usa SHA-512, no ``hash()``: es idéntico entre procesos
    (§3.6). Misma semilla + fuente + ``seq`` => mismo comentario.
    """
    return random.Random(f"{seed}|{source}|{seq}")


# ------------------------------------------------------------------ tiempo (§5.1)
def format_ts(dt: datetime) -> str:
    """UTC, ISO-8601 con milisegundos y ``Z``: ``2026-10-17T14:03:11.482Z``."""
    if dt.tzinfo is None:
        raise ValueError("datetime sin zona horaria; use UTC")
    s = dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


def parse_ts(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"timestamp sin 'Z': {value!r}")
    return datetime.fromisoformat(value[:-1] + "+00:00")


# ------------------------------------------------------------------ construcción
def build_envelope(
    *,
    origin: str,
    source: str,
    seed: int | str,
    seq: int,
    event_time: datetime,
    ingest_time: datetime,
    user_id: str,
    handle: str,
    followers: Optional[int],
    text: str,
    payload: Mapping[str, Any],
    brands: Sequence[str],
    generator: str,
    hashtags: Optional[Sequence[str]] = None,
    lang: Optional[str] = "es",
    label_sentiment: Optional[str] = None,
    scenario: Optional[str] = None,
    chaos: Iterable[str] = (),
) -> dict[str, Any]:
    """Arma el envelope de §5.2 (orden de claves igual al del documento).

    ``hashtags=None`` => se extraen del texto en forma canónica (§3.8). Lanza ``ValueError``
    si algún campo incumple las reglas de §5.2 que se pueden comprobar sin el schema.
    """
    if origin not in ORIGINS:
        raise ValueError(f"origin inválido: {origin!r}")
    if source not in SOURCES:
        raise ValueError(f"source inválido: {source!r}")
    if not text or not text.strip():
        raise ValueError("text vacío")
    if not _GENERATOR_RE.match(generator):
        raise ValueError(f"generator debe ser 'nombre@versión': {generator!r}")
    if lang is not None and not _LANG_RE.match(lang):
        raise ValueError(f"lang inválido: {lang!r}")
    if followers is not None and (not isinstance(followers, int) or followers < 0):
        raise ValueError(f"followers debe ser entero >= 0 o None: {followers!r}")
    if label_sentiment is not None and label_sentiment not in SENTIMENTS:
        raise ValueError(f"label_sentiment inválido: {label_sentiment!r}")
    brands = list(brands)
    if len(set(brands)) != len(brands) or not all(_BRAND_RE.match(b) for b in brands):
        raise ValueError(f"brands inválido (claves canónicas, sin repetidos): {brands!r}")
    chaos = list(chaos)
    if len(set(chaos)) != len(chaos) or not set(chaos) <= set(CHAOS_VALUES):
        raise ValueError(f"chaos inválido: {chaos!r}")
    tags = extract_hashtags(text) if hashtags is None else list(hashtags)
    if len(set(tags)) != len(tags) or not all(HASHTAG_RE.match(h) for h in tags):
        raise ValueError(f"hashtags no canónicos: {tags!r}")

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id(origin, source, seed, seq),
        "origin": origin,
        "source": source,
        "generator": generator,
        "seq": seq,
        "event_time": format_ts(event_time),
        "ingest_time": format_ts(ingest_time),
        "author": {"user_id": user_id, "handle": handle, "followers": followers},
        "text": text,
        "lang": lang,
        "brands": brands,
        "hashtags": tags,
        "payload": dict(payload),
        "sim": {"label_sentiment": label_sentiment, "scenario": scenario, "chaos": chaos},
    }


def pubsub_attributes(envelope: Mapping[str, Any]) -> dict[str, str]:
    """Atributos del mensaje Pub/Sub (§5.3); Dataflow usa ``event_id`` como ``id_label``."""
    return {
        "event_id": envelope["event_id"],
        "source": envelope["source"],
        "origin": envelope["origin"],
        "schema_version": envelope["schema_version"],
    }


def normalize_engagement(source: str, payload: Mapping[str, Any]) -> dict[str, Optional[int]]:
    """``likes/shares/comments`` normalizados según §5.2."""
    if source == "reddit":
        return {"likes": payload["score"], "shares": None, "comments": payload.get("num_comments")}
    if source == "facebook":
        return {
            "likes": payload["reactions_total"],
            "shares": payload["shares_count"],
            "comments": payload["comments_count"],
        }
    if source == "instagram":
        return {"likes": payload["like_count"], "shares": None, "comments": payload["comments_count"]}
    raise ValueError(f"source inválido: {source!r}")
