"""Engagement sintético por red (§3.8, HU-014).

Se interpreta como *snapshot* al momento de la recolección. Escala con los seguidores
del autor, con un leve impulso para el sentimiento extremo y, en Facebook, un sesgo del
desglose de reacciones según ``label_sentiment`` cuando existe.

Cada función hace un número fijo de extracciones de ``rng`` para el mismo ``kind``,
así el resto del evento no depende de los valores que salgan.
"""

from __future__ import annotations

import math
import random
from typing import Optional

REACTIONS = ("like", "love", "haha", "wow", "sad", "angry")

_BASE_MIX = {"like": 0.62, "love": 0.12, "haha": 0.08, "wow": 0.05, "sad": 0.05, "angry": 0.08}
_BIAS = {
    "positive": {"love": 2.0, "haha": 1.2, "sad": 0.4, "angry": 0.3},
    "negative": {"love": 0.3, "haha": 1.1, "sad": 2.0, "angry": 3.0},
    "neutral": {"wow": 1.3},
}


def _scale(followers: Optional[int]) -> float:
    """Factor ~1 para 150 seguidores, crece despacio (raíz cúbica)."""
    f = 150 if followers is None else max(0, followers)
    return max(0.3, ((f + 1) / 150.0) ** (1 / 3))


def _boost(label_sentiment: Optional[str]) -> float:
    return 1.25 if label_sentiment in ("positive", "negative") else 1.0


def _poisson_like(rng: random.Random, mean: float) -> int:
    """Entero ≥ 0 con media ~``mean`` (lognormal discretizada; 1 extracción)."""
    if mean <= 0:
        rng.random()
        return 0
    sigma = 0.8
    mu = math.log(mean) - sigma * sigma / 2
    return int(rng.lognormvariate(mu, sigma))


def reddit(rng: random.Random, kind: str, followers: Optional[int], label_sentiment: Optional[str]) -> dict:
    """``score`` (puede ser negativo), ``upvote_ratio`` y ``num_comments`` (solo en post)."""
    s = _scale(followers) * _boost(label_sentiment)
    mean = (40.0 if kind == "post" else 9.0) * s
    score = _poisson_like(rng, mean)
    down = rng.random()
    if label_sentiment == "negative" and down < 0.25:
        score = -int(score * 0.3) - 1
    elif down < 0.05:
        score = -rng.randint(1, 5)
    else:
        rng.random()  # mantener número de extracciones estable
    ratio = round(0.55 + 0.43 * rng.random(), 2)
    n_comments = _poisson_like(rng, 12.0 * s)
    if kind == "post":
        return {"score": score, "upvote_ratio": ratio, "num_comments": n_comments}
    return {"score": score, "upvote_ratio": None, "num_comments": None}


def facebook(rng: random.Random, kind: str, followers: Optional[int], label_sentiment: Optional[str]) -> dict:
    """``reactions_total``, desglose ``reactions``, ``shares_count`` y ``comments_count``."""
    s = _scale(followers) * _boost(label_sentiment)
    total = _poisson_like(rng, (60.0 if kind == "post" else 8.0) * s)
    mix = dict(_BASE_MIX)
    for k, f in _BIAS.get(label_sentiment or "", {}).items():
        mix[k] *= f
    z = sum(mix.values())
    probs = [mix[r] / z for r in REACTIONS]
    counts = dict.fromkeys(REACTIONS, 0)
    # Reparto multinomial con a lo sumo 200 extracciones; el resto, proporcional.
    sampled = min(total, 200)
    for r in rng.choices(REACTIONS, weights=probs, k=sampled):
        counts[r] += 1
    rest = total - sampled
    if rest:
        assigned = 0
        for r, p in zip(REACTIONS, probs):
            add = int(rest * p)
            counts[r] += add
            assigned += add
        counts["like"] += rest - assigned
    shares = _poisson_like(rng, (6.0 if kind == "post" else 0.6) * s)
    comments = _poisson_like(rng, (14.0 if kind == "post" else 1.2) * s)
    return {
        "reactions_total": sum(counts.values()),
        "reactions": counts,
        "shares_count": shares,
        "comments_count": comments,
    }


def instagram(rng: random.Random, kind: str, followers: Optional[int], label_sentiment: Optional[str]) -> dict:
    """``like_count`` y ``comments_count`` (Instagram no tiene shares)."""
    s = _scale(followers) * _boost(label_sentiment)
    likes = _poisson_like(rng, 14.0 * s)
    replies = _poisson_like(rng, 1.5 * s)
    return {"like_count": likes, "comments_count": replies}


BY_SOURCE = {"reddit": reddit, "facebook": facebook, "instagram": instagram}
