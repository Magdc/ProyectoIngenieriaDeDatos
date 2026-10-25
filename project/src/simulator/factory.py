"""Construcción determinista de eventos del simulador (Lote 1.2, §3.3, §3.5, §3.8, §5.2).

``EventFactory.make(source, seq, now)`` arma el envelope de un evento y los mensajes
Pub/Sub que le corresponden (uno, dos si hay ``duplicate``, o un cuerpo defectuoso si
hay ``malformed``). Es una función pura salvo por ``now``:

- misma semilla + fuente + ``seq`` ⇒ mismo ``event_id``, mismo texto, misma marca,
  mismo usuario, mismo payload. Solo ``event_time`` e ``ingest_time`` dependen de ``now``;
- encender el caos no cambia nada de lo anterior (usa su propio ``Random``, §3.9).

**Orden de extracciones** del ``Random`` del evento (``common.envelope.event_rng``):
1. tipo (``post``/``comment``) · 2. marca · 3. contexto (subreddit / media) — estas tres
las repite :meth:`EventFactory.peek` para saber, sin estado, qué posts hubo antes
y así enlazar ``parent_id`` con un post reciente real · 4. usuario · 5. generador de
comentarios · 6. hashtags añadidos · 7. lag · 8. padre · 9. engagement · 10. flair.
No reordenar sin aceptar que cambian todos los eventos.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from common.envelope import event_id as make_event_id
from common.envelope import event_rng, pubsub_attributes
from common.hashtags import extract_hashtags
from simulator.enrich import chaos as chaos_mod
from simulator.enrich import engagement
from simulator.enrich.brands import (
    BrandSelector,
    Brands,
    add_hashtags,
    brands_in_text,
    first_sentence,
    load_brands,
)
from simulator.enrich.users import UserPool
from simulator.generator.comment_generator import GENERATOR_ID, generate
from simulator.sim_config import SOURCES, SimConfig

SCHEMA_VERSION = "1.0"
_SENTIMENTS = ("positive", "negative", "neutral")


def iso_ms(dt: datetime) -> str:
    """UTC, ISO-8601 con milisegundos y ``Z`` (§5.1)."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class Scenario:
    """Escenario de demo (§3.7). El endpoint que lo activa llega en el Lote 6.3."""

    name: str
    brand: str
    sentiment: Optional[str] = None
    source: Optional[str] = None

    def applies_to(self, source: str) -> bool:
        return self.source is None or self.source == source


@dataclass(frozen=True)
class Message:
    data: bytes
    attributes: dict


@dataclass(frozen=True)
class Emission:
    source: str
    seq: int
    envelope: dict  # el evento tal como debería llegar (con late/null_fields aplicados)
    messages: tuple[Message, ...]
    chaos: tuple[str, ...] = field(default=())


@dataclass(frozen=True)
class _Peek:
    kind: str
    brand: str
    ctx: float


def _digest(*parts: object) -> int:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(h[:16], "big")


def _base36(n: int, width: int) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = []
    for _ in range(width):
        n, r = divmod(n, 36)
        out.append(alphabet[r])
    return "".join(reversed(out))


def _digits(n: int, width: int) -> str:
    s = str(n % (10**width)).rjust(width, "0")
    return ("1" + s[1:]) if s[0] == "0" else s  # sin ceros a la izquierda


class EventFactory:
    """Arma eventos de las tres fuentes simuladas con una semilla dada."""

    def __init__(
        self,
        config: SimConfig,
        brands: Optional[Brands] = None,
        *,
        origin: str = "stream",
    ) -> None:
        if origin not in ("stream", "historical"):
            raise ValueError(f"origin inválido: {origin!r}")
        self.config = config
        self.seed = config.seed
        self.origin = origin
        self.brands = brands or load_brands()
        self.selector = BrandSelector(self.brands, config)
        self._pools: dict[str, UserPool] = {}
        self._peek_cache: dict[tuple[str, int], _Peek] = {}

    # -- piezas reutilizables --------------------------------------------------------

    def pool(self, source: str) -> UserPool:
        if source not in self._pools:
            self._pools[source] = UserPool(source, self.config)
        return self._pools[source]

    def peek(self, source: str, seq: int) -> _Peek:
        """Tipo, marca y contexto de ``seq`` (las 3 primeras extracciones), sin escenario."""
        key = (source, seq)
        hit = self._peek_cache.get(key)
        if hit is not None:
            return hit
        rng = event_rng(self.seed, source, seq)
        p = self._draw_head(rng, source, seq)
        if len(self._peek_cache) > 20000:
            self._peek_cache.clear()
        self._peek_cache[key] = p
        return p

    def _draw_head(self, rng: random.Random, source: str, seq: int) -> _Peek:
        kind = "post" if rng.random() < self.config.post_probability[source] else "comment"
        brand = self.selector.choose(rng, seq)
        ctx = rng.random()
        return _Peek(kind, brand, ctx)

    def _recent_posts(self, source: str, seq: int, brand: Optional[str]) -> list[int]:
        found: list[int] = []
        lo = max(-1, seq - 1 - self.config.parent_lookback)
        for s in range(seq - 1, lo, -1):
            p = self.peek(source, s)
            if p.kind == "post" and (brand is None or p.brand == brand):
                found.append(s)
                if len(found) == self.config.parent_candidates:
                    break
        return found

    def _subreddit(self, ctx: float) -> str:
        subs = self.config.subreddits
        return subs[min(int(ctx * len(subs)), len(subs) - 1)]

    def _reddit_id(self, seq: int) -> str:
        return _base36(_digest(self.seed, "reddit", seq, "id"), 7)

    # -- evento ----------------------------------------------------------------------

    def make(self, source: str, seq: int, now: datetime, scenario: Optional[Scenario] = None) -> Emission:
        if source not in SOURCES:
            raise ValueError(f"fuente desconocida: {source!r}")
        if seq < 0:
            raise ValueError("seq debe ser >= 0")
        if now.tzinfo is None:
            raise ValueError("now debe tener zona horaria (UTC)")
        cfg = self.config
        rng = event_rng(self.seed, source, seq)

        head = self._draw_head(rng, source, seq)  # 1–3
        kind, brand = head.kind, head.brand
        forced_sentiment: Optional[str] = None
        scenario_name: Optional[str] = None
        if scenario is not None and scenario.applies_to(source):
            brand = self.brands.canonical(scenario.brand)
            forced_sentiment = scenario.sentiment
            scenario_name = scenario.name
            if forced_sentiment is not None and forced_sentiment not in _SENTIMENTS:
                raise ValueError(f"sentimiento de escenario inválido: {forced_sentiment!r}")

        user = self.pool(source).pick(rng)  # 4
        comentario = generate(rng, marca=brand, sentimiento=forced_sentiment, ruido=cfg.ruido, brands=self.brands)  # 5
        brand = comentario.marca or brand
        info = self.selector.info(brand)
        text = add_hashtags(  # 6
            rng,
            comentario.texto,
            brand_display=str(info.get("display", brand)),
            probability=cfg.hashtag_probability[source],
            count=cfg.hashtag_count,
            sector=cfg.sector_hashtags,
        )
        lag = rng.uniform(*cfg.event_lag_s)  # 7
        event_dt = now - timedelta(seconds=lag)
        label = comentario.sentimiento if comentario.sentimiento in _SENTIMENTS else None

        payload = self._payload(rng, source, seq, kind, brand, head.ctx, info, text, user.followers, label)  # 8–10

        env = {
            "schema_version": SCHEMA_VERSION,
            "event_id": make_event_id(self.origin, source, self.seed, seq),
            "origin": self.origin,
            "source": source,
            "generator": GENERATOR_ID,
            "seq": seq,
            "event_time": iso_ms(event_dt),
            "ingest_time": iso_ms(now),
            "author": {"user_id": user.user_id, "handle": user.handle, "followers": user.followers},
            "text": text,
            "lang": cfg.lang,
            "brands": brands_in_text(brand, text, self.brands),
            "hashtags": extract_hashtags(text),
            "payload": payload,
            "sim": {"label_sentiment": label, "scenario": scenario_name, "chaos": []},
        }
        return self._with_chaos(env, source, seq, event_dt)

    def _with_chaos(self, env: dict, source: str, seq: int, event_dt: datetime) -> Emission:
        plan = chaos_mod.plan(self.config.chaos, self.seed, source, seq)
        attrs = pubsub_attributes(env)
        if plan.malformed:
            body = chaos_mod.malformed_bytes(env, plan.malformed_mode)
            return Emission(source, seq, env, (Message(body, dict(attrs)),), plan.labels)
        out = chaos_mod.apply_to_envelope(env, plan, event_dt=event_dt, format_ts=iso_ms)
        msgs = [Message(_dumps(out), dict(attrs))]
        if plan.duplicate:
            msgs.append(Message(_dumps(chaos_mod.duplicate_of(out)), dict(attrs)))
        return Emission(source, seq, out, tuple(msgs), plan.labels)

    # -- payload por red (§5.2) --------------------------------------------------------

    def _payload(self, rng, source, seq, kind, brand, ctx, info, text, followers, label) -> dict:
        if source == "reddit":
            return self._reddit(rng, seq, kind, ctx, text, followers, label)
        if source == "facebook":
            return self._facebook(rng, seq, kind, brand, info, followers, label)
        return self._instagram(rng, seq, brand, info, followers, label)

    def _reddit(self, rng, seq, kind, ctx, text, followers, label) -> dict:
        own = self._reddit_id(seq)
        pick = rng.random()  # 8
        if kind == "post":
            sub = self._subreddit(ctx)
            parent, title = None, first_sentence(text)
            permalink = f"/{sub}/comments/{own}/_/"
            rid = f"t3_{own}"
        else:
            posts = self._recent_posts("reddit", seq, None)
            if posts:
                pseq = posts[min(int(pick * len(posts)), len(posts) - 1)]
                sub = self._subreddit(self.peek("reddit", pseq).ctx)
                post_id = self._reddit_id(pseq)
            else:  # aún no hay posts: hilo preexistente simulado
                sub = self._subreddit(ctx)
                post_id = _base36(_digest(self.seed, "reddit", "pre", sub), 7)
            parent, title = f"t3_{post_id}", None
            permalink = f"/{sub}/comments/{post_id}/_/{own}/"
            rid = f"t1_{own}"
        eng = engagement.reddit(rng, kind, followers, label)  # 9
        flair_roll, flair_idx = rng.random(), rng.random()  # 10
        flair = None
        if self.config.flairs and flair_roll < self.config.flair_probability:
            flair = self.config.flairs[min(int(flair_idx * len(self.config.flairs)), len(self.config.flairs) - 1)]
        return {
            "reddit_id": rid,
            "kind": kind,
            "subreddit": sub,
            "title": title,
            "parent_id": parent,
            "score": eng["score"],
            "upvote_ratio": eng["upvote_ratio"],
            "num_comments": eng["num_comments"],
            "permalink": permalink,
            "flair": flair,
        }

    def _facebook(self, rng, seq, kind, brand, info, followers, label) -> dict:
        page_num = _digits(_digest(self.seed, "facebook", "page", brand), 10)
        own_num = _digits(_digest(self.seed, "facebook", seq, "id"), 10)
        pick = rng.random()  # 8
        if kind == "post":
            post_id, parent = f"sim-post-{seq:06d}", None
            post_ref = f"{seq:06d}"
        else:
            posts = self._recent_posts("facebook", seq, brand)
            if posts:
                pseq = posts[min(int(pick * len(posts)), len(posts) - 1)]
                post_ref = f"{pseq:06d}"
            else:
                post_ref = f"pre-{brand}"
            post_id = parent = f"sim-post-{post_ref}"
        eng = engagement.facebook(rng, kind, followers, label)  # 9
        rng.random()  # 10 (sin flair; se mantiene el conteo)
        permalink = f"https://sim.facebook.local/{brand}/posts/{post_ref}"
        if kind == "comment":
            permalink += f"?comment_id={own_num}"
        return {
            "comment_id": f"{page_num}_{own_num}",
            "kind": kind,
            "page_id": f"sim-page-{brand}",
            "page_name": str(info.get("facebook_page") or f"{info.get('display', brand)} (sim)"),
            "post_id": post_id,
            "parent_id": parent,
            "reactions_total": eng["reactions_total"],
            "reactions": eng["reactions"],
            "shares_count": eng["shares_count"],
            "comments_count": eng["comments_count"],
            "permalink": permalink,
        }

    def _instagram(self, rng, seq, brand, info, followers, label) -> dict:
        cfg = self.config
        account = str(info.get("instagram_account") or f"{brand}.sim")
        pick = rng.random()  # 8
        newest = seq // cfg.media_every
        idx = max(0, newest - min(int(pick * cfg.recent_media), cfg.recent_media - 1))
        media_id = _digits(_digest(self.seed, "instagram", account, idx, "media"), 17)
        types = list(cfg.media_types)
        mrng = random.Random(f"{self.seed}|instagram|{account}|{idx}|media_type")
        media_type = mrng.choices(types, weights=[cfg.media_types[t] for t in types], k=1)[0]
        eng = engagement.instagram(rng, "comment", followers, label)  # 9
        rng.random()  # 10
        return {
            "comment_id": _digits(_digest(self.seed, "instagram", seq, "id"), 17),
            "kind": "comment",
            "media_id": media_id,
            "media_type": media_type,
            "account_username": account,
            "parent_id": None,
            "like_count": eng["like_count"],
            "comments_count": eng["comments_count"],
            "permalink": f"https://sim.instagram.local/p/{media_id}/",
        }


def _dumps(env: dict) -> bytes:
    return json.dumps(env, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
