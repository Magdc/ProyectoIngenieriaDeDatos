"""Dobles de prueba de la rama enriched (Lote 2.2). Sin red ni credenciales.

Construye envelopes de §5.2 a mano (sin depender de la firma de ``build_envelope``) que
cumplen las invariantes por fuente y ``kind``, y lakes locales de ejemplo.
"""

from __future__ import annotations

import gzip
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from common.enriched_row import envelope_to_row, format_process_time
from common.parquet_rows import rows_to_parquet_bytes

TEST_KEY = "clave-de-prueba-del-lote-2-2-no-es-secreta"
BRAND_DISPLAY = {"postobon": "Postobón", "pepsi": "Pepsi", "sprite": "Sprite"}


def ts(dt: datetime) -> str:
    return format_process_time(dt)


def event_id(source: str, seq: int, seed: int = 7, origin: str = "stream") -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def make_payload(source: str, seq: int, kind: str, brand: str) -> dict[str, Any]:
    if source == "reddit":
        if kind == "post":
            return {
                "reddit_id": f"t3_{seq:07d}"[-10:],
                "kind": "post",
                "subreddit": "r/colombia",
                "title": "Opinión del día",
                "parent_id": None,
                "score": 12 + seq % 7,
                "upvote_ratio": 0.91,
                "num_comments": 3,
                "permalink": f"/r/colombia/comments/{seq:07d}/_/",
                "flair": None,
            }
        return {
            "reddit_id": f"t1_{seq:07d}",
            "kind": "comment",
            "subreddit": "r/medellin",
            "title": None,
            "parent_id": "t3_0000001",
            "score": -2 + seq % 5,
            "upvote_ratio": None,
            "num_comments": None,
            "permalink": f"/r/medellin/comments/0000001/_/{seq:07d}/",
            "flair": None,
        }
    if source == "facebook":
        return {
            "comment_id": f"1029384756_{seq:010d}",
            "kind": kind,
            "page_id": f"sim-page-{brand}",
            "page_name": f"{BRAND_DISPLAY[brand]} (sim)",
            "post_id": "sim-post-000812",
            "parent_id": None if kind == "post" else "sim-post-000812",
            "reactions_total": 14,
            "reactions": {"like": 9, "love": 3, "haha": 1, "wow": 0, "sad": 0, "angry": 1},
            "shares_count": 2,
            "comments_count": 3,
            "permalink": f"https://sim.facebook.local/{brand}/posts/000812",
        }
    return {
        "comment_id": f"{17912345678900000 + seq}",
        "kind": "comment",
        "media_id": "17855544433322211",
        "media_type": "IMAGE",
        "account_username": f"{brand}.sim",
        "parent_id": None,
        "like_count": 31,
        "comments_count": 4,
        "permalink": "https://sim.instagram.local/p/000812/",
    }


def make_envelope(
    source: str = "reddit",
    seq: int = 1,
    *,
    kind: str | None = None,
    user_id: str | None = None,
    brand: str = "postobon",
    event_time: datetime | None = None,
    followers: int | None = 412,
    label_sentiment: str | None = "positive",
) -> dict[str, Any]:
    kind = kind or ("comment" if source == "instagram" or seq % 2 else "post")
    et = event_time or datetime(2026, 10, 17, 14, 3, 11, 482000, tzinfo=timezone.utc)
    handle = {"reddit": "u/mariag_89", "facebook": "María Gómez", "instagram": "@mariag_89"}[source]
    return {
        "schema_version": "1.0",
        "event_id": event_id(source, seq),
        "origin": "stream",
        "source": source,
        "generator": "comment-gen@1.0",
        "seq": seq,
        "event_time": ts(et),
        "ingest_time": ts(et + timedelta(milliseconds=1538)),
        "author": {"user_id": user_id or f"u_{seq % 5:06d}", "handle": handle, "followers": followers},
        "text": f"Opinión personal: soy fan de {BRAND_DISPLAY[brand]} y no pienso cambiar.",
        "lang": "es",
        "brands": [brand],
        "hashtags": [],
        "payload": make_payload(source, seq, kind, brand),
        "sim": {"label_sentiment": label_sentiment, "scenario": None, "chaos": []},
    }


class FakeValidEvent:
    """Imita un ``ValidEvent`` del Lote 2.1 que guarda el cuerpo original en ``body``."""

    def __init__(self, envelope: dict[str, Any], source: str | None = None):
        self.body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        self.source = source or envelope["source"]
        self.event_id = envelope["event_id"]


def write_local_lake(root: str, envelopes: list[dict], *, now: datetime, key: str = TEST_KEY, window_seconds: int = 60) -> None:
    """Escribe ``raw/`` (JSONL gzip) y ``enriched/`` (Parquet) como lo haría el pipeline."""
    start = now.replace(second=0, microsecond=0) - timedelta(minutes=30)
    start = start - timedelta(seconds=start.timestamp() % window_seconds)
    by_source: dict[str, list[dict]] = {}
    for env in envelopes:
        by_source.setdefault(env["source"], []).append(env)
    for source, envs in by_source.items():
        part = f"source={source}/dt={start:%Y-%m-%d}/hr={start:%H}"
        name = f"part-{start:%Y%m%dT%H%M%S}Z-p000"
        raw_dir = os.path.join(root, "raw", part)
        enr_dir = os.path.join(root, "enriched", part)
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(enr_dir, exist_ok=True)
        lines = "\n".join(json.dumps(e, ensure_ascii=False) for e in envs) + "\n"
        with open(os.path.join(raw_dir, name + ".jsonl.gz"), "wb") as fh:
            fh.write(gzip.compress(lines.encode("utf-8"), mtime=0))
        rows = [envelope_to_row(e, key, ts(start + timedelta(seconds=5))) for e in envs]
        with open(os.path.join(enr_dir, name + ".parquet"), "wb") as fh:
            fh.write(rows_to_parquet_bytes(rows))
