"""Dobles de prueba del loader histórico (sin simulador, nube, torch ni Beam).

Imitan la **forma** documentada de las piezas reales (§5.2, §5.5): envelopes de las tres
fuentes con sus invariantes por ``kind``, anonimización HMAC-SHA256, la fila de
``posts_enriched`` y su Parquet, y un NLP determinista. Solo sirven para probar el
orquestador; las pruebas de ``test_hist_repo.py`` usan el código real.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import random
import uuid
from typing import Any, Mapping, Sequence

from historical.adapters import NLP_FIELDS
from historical.plan import UTC, format_ts

BRANDS = ("postobon", "pepsi", "sprite")
DISPLAY = {"postobon": "Postobón", "pepsi": "Pepsi", "sprite": "Sprite"}
USER_BASE = {"reddit": 0, "facebook": 2000, "instagram": 4000}
FAKE_VERSION = "robertuito-sentiment@aaaaaaaaaaaa+robertuito-emotion@bbbbbbbbbbbb"
COLUMNS = ("event_id", "origin", "source", "generator", "event_time", "ingest_time",
           "process_time", "user_hash", "followers", "text", "lang", "brands",
           "brand_primary", "hashtags", "likes", "shares", "comments", "sentiment_label",
           "sentiment_score", "emotion_label", "emotion_score", "label_sentiment",
           "nlp_model_version", "payload")


def fake_event_id(origin: str, source: str, seed: int, seq: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def _digits(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("0123456789") for _ in range(n))


def _b36(rng: random.Random, n: int = 7) -> str:
    return "".join(rng.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(n))


class FakeEnvelopes:
    """Envelopes deterministas con la forma de §5.2 (como ``EventFactory.make``)."""

    kind = "fake"

    def __init__(self, seed: int, brand_weights: Mapping[str, float] | None = None):
        self.seed = seed
        self.brand_weights = dict(brand_weights or {b: 1.0 for b in BRANDS})
        self.calls = 0

    def envelope(self, source: str, seq: int, now: dt.datetime) -> dict[str, Any]:
        self.calls += 1
        rng = random.Random(f"{self.seed}|{source}|{seq}")
        brands = list(self.brand_weights)
        brand = rng.choices(brands, weights=[self.brand_weights[b] for b in brands])[0]
        label = rng.choices(["positive", "negative", "neutral", None], weights=[28, 24, 18, 30])[0]
        user_n = USER_BASE[source] + min(1999, int(rng.paretovariate(1.1)) - 1)
        user_id = f"u_{user_n:06d}"
        lag = rng.uniform(0, 5)
        event_time = now - dt.timedelta(seconds=lag)
        text = f"Opinión {label or 'ambigua'} sobre {DISPLAY[brand]} #{DISPLAY[brand]}"
        payload = self._payload(source, brand, rng)
        return {
            "schema_version": "1.0",
            "event_id": fake_event_id("historical", source, self.seed, seq),
            "origin": "historical", "source": source, "generator": "comment-gen@1.0",
            "seq": seq, "event_time": format_ts(event_time), "ingest_time": format_ts(now),
            "author": {"user_id": user_id, "handle": f"u/{user_id}",
                       "followers": int(rng.lognormvariate(5, 1))},
            "text": text, "lang": "es", "brands": [brand], "hashtags": [brand],
            "payload": payload,
            "sim": {"label_sentiment": label, "scenario": None, "chaos": []},
        }

    @staticmethod
    def _payload(source: str, brand: str, rng: random.Random) -> dict[str, Any]:
        if source == "reddit":
            if rng.random() < 0.3:
                rid = f"t3_{_b36(rng)}"
                return {"reddit_id": rid, "kind": "post", "subreddit": "r/colombia",
                        "title": "Hablemos de gaseosas", "parent_id": None,
                        "score": rng.randint(-5, 300), "upvote_ratio": round(rng.uniform(0.5, 1), 2),
                        "num_comments": rng.randint(0, 40),
                        "permalink": f"/r/colombia/comments/{rid[3:]}/_/", "flair": None}
            rid = f"t1_{_b36(rng)}"
            parent = f"t3_{_b36(rng)}"
            return {"reddit_id": rid, "kind": "comment", "subreddit": "r/colombia",
                    "title": None, "parent_id": parent, "score": rng.randint(-5, 100),
                    "upvote_ratio": None, "num_comments": None,
                    "permalink": f"/r/colombia/comments/{parent[3:]}/_/{rid[3:]}/", "flair": None}
        if source == "facebook":
            is_post = rng.random() < 0.15
            reactions = {k: rng.randint(0, 9) for k in ("like", "love", "haha", "wow", "sad", "angry")}
            post_id = f"sim-post-{rng.randint(0, 999999):06d}"
            return {"comment_id": f"{_digits(rng, 10)}_{_digits(rng, 10)}",
                    "kind": "post" if is_post else "comment", "page_id": f"sim-page-{brand}",
                    "page_name": f"{DISPLAY[brand]} (sim)", "post_id": post_id,
                    "parent_id": None if is_post else post_id,
                    "reactions_total": sum(reactions.values()), "reactions": reactions,
                    "shares_count": rng.randint(0, 9), "comments_count": rng.randint(0, 9),
                    "permalink": f"https://sim.facebook.local/{brand}/posts/{post_id[9:]}"}
        return {"comment_id": _digits(rng, 17), "kind": "comment", "media_id": _digits(rng, 17),
                "media_type": rng.choice(["IMAGE", "VIDEO", "CAROUSEL_ALBUM"]),
                "account_username": f"{brand}.sim", "parent_id": None,
                "like_count": rng.randint(0, 200), "comments_count": rng.randint(0, 20),
                "permalink": f"https://sim.instagram.local/p/{_digits(rng, 6)}/"}

    def describe(self) -> dict[str, Any]:
        return {"how": "FakeEnvelopes", "calls": self.calls}


def fake_engagement(source: str, payload: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    if source == "reddit":
        return payload["score"], None, payload["num_comments"]
    if source == "facebook":
        return payload["reactions_total"], payload["shares_count"], payload["comments_count"]
    return payload["like_count"], None, payload["comments_count"]


def make_fake_enricher(key: str, process_time: dt.datetime):
    def enrich(envelope: Mapping[str, Any]) -> dict[str, Any]:
        env = json.loads(json.dumps(envelope))
        author = env.pop("author")
        uh = hmac.new(key.encode(), author["user_id"].encode(), hashlib.sha256).hexdigest()
        env["author"] = {"user_hash": uh, "followers": author.get("followers")}
        likes, shares, comments = fake_engagement(env["source"], env["payload"])
        env.update({"likes": likes, "shares": shares, "comments": comments})
        for f in NLP_FIELDS:
            env[f] = None
        env["process_time"] = format_ts(process_time)
        return env

    return enrich


def fake_to_row(enriched: Mapping[str, Any]) -> dict[str, Any]:
    e = enriched
    row = {
        "event_id": e["event_id"], "origin": e["origin"], "source": e["source"],
        "generator": e["generator"], "event_time": e["event_time"],
        "ingest_time": e["ingest_time"], "process_time": e["process_time"],
        "user_hash": e["author"]["user_hash"], "followers": e["author"].get("followers"),
        "text": e["text"], "lang": e.get("lang"), "brands": list(e.get("brands") or []),
        "brand_primary": (e.get("brands") or [None])[0], "hashtags": list(e.get("hashtags") or []),
        "likes": e.get("likes"), "shares": e.get("shares"), "comments": e.get("comments"),
        "sentiment_label": e.get("sentiment_label"), "sentiment_score": e.get("sentiment_score"),
        "emotion_label": e.get("emotion_label"), "emotion_score": e.get("emotion_score"),
        "label_sentiment": (e.get("sim") or {}).get("label_sentiment"),
        "nlp_model_version": e.get("nlp_model_version"),
        "payload": json.dumps(e.get("payload"), ensure_ascii=False, separators=(",", ":"),
                              sort_keys=True),
    }
    return row


def fake_rows_to_parquet(rows: Sequence[Mapping[str, Any]]) -> bytes:
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    ts = pa.timestamp("ms", tz="UTC")
    schema = pa.schema([
        ("event_id", pa.string()), ("origin", pa.string()), ("source", pa.string()),
        ("generator", pa.string()), ("event_time", ts), ("ingest_time", ts),
        ("process_time", ts), ("user_hash", pa.string()), ("followers", pa.int64()),
        ("text", pa.string()), ("lang", pa.string()), ("brands", pa.list_(pa.string())),
        ("brand_primary", pa.string()), ("hashtags", pa.list_(pa.string())),
        ("likes", pa.int64()), ("shares", pa.int64()), ("comments", pa.int64()),
        ("sentiment_label", pa.string()), ("sentiment_score", pa.float64()),
        ("emotion_label", pa.string()), ("emotion_score", pa.float64()),
        ("label_sentiment", pa.string()), ("nlp_model_version", pa.string()),
        ("payload", pa.string())])

    def conv(row: Mapping[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for c in ("event_time", "ingest_time", "process_time"):
            out[c] = dt.datetime.fromisoformat(row[c][:-1]).replace(tzinfo=UTC)
        return out

    table = pa.Table.from_pylist([conv(r) for r in rows], schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


class FakeNlpEngine:
    """NLP determinista. ``fail_first`` deja sin NLP esas filas en la primera llamada."""

    name = "fake"

    def __init__(self, fail_first: int = 0, version: str = FAKE_VERSION):
        self.fail_first = fail_first
        self.version = version
        self.calls = 0
        self.rows_seen = 0

    def annotate(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        self.calls += 1
        out = {}
        for i, r in enumerate(rows):
            self.rows_seen += 1
            if self.calls == 1 and i < self.fail_first:
                out[r["event_id"]] = {f: None for f in NLP_FIELDS}
                continue
            gt = r.get("label_sentiment") or "neutral"
            emo = {"positive": "joy", "negative": "anger"}.get(gt, "neutral")
            out[r["event_id"]] = {"sentiment_label": gt, "sentiment_score": 0.9,
                                  "emotion_label": emo, "emotion_score": 0.8,
                                  "nlp_model_version": self.version}
        return out

    def describe(self) -> dict[str, Any]:
        return {"engine": self.name, "calls": self.calls}
