"""Carga y valida ``config/sim.yaml`` (Lote 1.2, ARCHITECTURE.md §3.5, §3.8, §3.9).

La configuración es un objeto inmutable que se arma una vez al arrancar. Dos variables
de entorno de §5.1 la pisan: ``SIM_SEED`` y ``SIM_CHAOS_ENABLED``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "sim.yaml"
DEFAULT_SEED = 7
SOURCES: tuple[str, ...] = ("reddit", "facebook", "instagram")


def _pair(value: Any, name: str) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"{name} debe ser una lista [min, max]")
    lo, hi = float(value[0]), float(value[1])
    if lo < 0 or hi < lo:
        raise ValueError(f"{name}: rango inválido {value!r}")
    return lo, hi


def _prob(value: Any, name: str) -> float:
    p = float(value)
    if not 0.0 <= p <= 1.0:
        raise ValueError(f"{name} debe estar entre 0 y 1 (recibido {value!r})")
    return p


def _bool_env(raw: str) -> bool:
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "si", "sí", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"valor booleano inválido: {raw!r}")


@dataclass(frozen=True)
class BrandCycle:
    base: float
    amplitude: float
    period: float
    phase: float


@dataclass(frozen=True)
class ChaosConfig:
    enabled: bool
    rates: Mapping[str, float]
    late_minutes: tuple[float, float]


@dataclass(frozen=True)
class SimConfig:
    seed: int
    interval_s: tuple[float, float]
    checkpoint_every: int
    event_lag_s: tuple[float, float]
    lang: str
    post_probability: Mapping[str, float]
    parent_candidates: int
    parent_lookback: int
    brand_cycles: Mapping[str, BrandCycle]
    drift_period: float
    drift_amplitude: float
    ruido: float
    hashtag_probability: Mapping[str, float]
    hashtag_count: tuple[int, int]
    sector_hashtags: tuple[str, ...]
    pool_seed: int
    pool_size: int
    zipf_s: float
    followers_mu: float
    followers_sigma: float
    subreddits: tuple[str, ...]
    flairs: tuple[str, ...]
    flair_probability: float
    media_every: int
    recent_media: int
    media_types: Mapping[str, float]
    chaos: ChaosConfig
    raw: Mapping[str, Any] = field(repr=False, default_factory=dict)


CHAOS_KINDS = ("duplicate", "late", "null_fields", "malformed")
MEDIA_TYPES = ("IMAGE", "VIDEO", "CAROUSEL_ALBUM")


def load_config(
    path: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> SimConfig:
    """Lee ``sim.yaml`` y aplica ``SIM_SEED`` / ``SIM_CHAOS_ENABLED`` del entorno."""
    env = os.environ if env is None else env
    p = Path(path) if path else Path(env.get("SIM_CONFIG") or DEFAULT_CONFIG_PATH)
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return config_from_dict(data, env)


def config_from_dict(data: Mapping[str, Any], env: Mapping[str, str] | None = None) -> SimConfig:
    env = {} if env is None else env
    em = data.get("emission", {})
    kinds = data.get("kinds", {})
    post_probability = {}
    for s in SOURCES:
        post_probability[s] = _prob(kinds.get(s, {}).get("post", 0.0), f"kinds.{s}.post")
    if post_probability["instagram"] != 0.0:
        raise ValueError("kinds.instagram.post debe ser 0: Instagram es siempre comment (§3.3)")

    bw = data.get("brand_weights", {})
    cycles = {
        k: BrandCycle(
            base=float(v["base"]),
            amplitude=float(v.get("amplitude", 0.0)),
            period=float(v.get("period", 1.0)),
            phase=float(v.get("phase", 0.0)),
        )
        for k, v in (bw.get("brands") or {}).items()
    }
    for k, c in cycles.items():
        if c.base <= 0 or not 0 <= c.amplitude < 1 or c.period <= 0:
            raise ValueError(f"brand_weights.brands.{k}: base > 0, 0 <= amplitude < 1 y period > 0")
    drift_amp = float(bw.get("drift_amplitude", 0.0))
    if not 0 <= drift_amp < 1:
        raise ValueError("brand_weights.drift_amplitude debe estar en [0, 1)")

    ht = data.get("hashtags", {})
    hprob = {s: _prob((ht.get("probability") or {}).get(s, 0.0), f"hashtags.probability.{s}") for s in SOURCES}
    cmin, cmax = (int(x) for x in ht.get("count", [1, 2]))
    if cmin < 0 or cmax < cmin:
        raise ValueError("hashtags.count inválido")

    us = data.get("users", {})
    fl = us.get("followers_lognormal", {})
    pool_size = int(us.get("pool_size", 2000))
    if pool_size < 1:
        raise ValueError("users.pool_size debe ser >= 1")

    rd = data.get("reddit", {})
    ig = data.get("instagram", {})
    media_types = {k: float(v) for k, v in (ig.get("media_types") or {"IMAGE": 1.0}).items()}
    if not set(media_types) <= set(MEDIA_TYPES) or sum(media_types.values()) <= 0:
        raise ValueError(f"instagram.media_types solo admite {MEDIA_TYPES}")

    ch = data.get("chaos", {})
    rates = {k: _prob((ch.get("rates") or {}).get(k, 0.0), f"chaos.rates.{k}") for k in CHAOS_KINDS}
    unknown = set(ch.get("rates") or {}) - set(CHAOS_KINDS)
    if unknown:
        raise ValueError(f"chaos.rates: defectos desconocidos {sorted(unknown)}")
    chaos_enabled = bool(ch.get("enabled", False))
    if "SIM_CHAOS_ENABLED" in env:
        chaos_enabled = _bool_env(env["SIM_CHAOS_ENABLED"])

    seed = int(env.get("SIM_SEED") or data.get("seed", DEFAULT_SEED))
    checkpoint_every = int(em.get("checkpoint_every", 50))
    if checkpoint_every < 1:
        raise ValueError("emission.checkpoint_every debe ser >= 1")

    return SimConfig(
        seed=seed,
        interval_s=_pair(em.get("interval_s", [1.0, 3.0]), "emission.interval_s"),
        checkpoint_every=checkpoint_every,
        event_lag_s=_pair(em.get("event_lag_s", [0.0, 5.0]), "emission.event_lag_s"),
        lang=str(em.get("lang", "es")),
        post_probability=post_probability,
        parent_candidates=max(1, int((data.get("parents") or {}).get("candidates", 3))),
        parent_lookback=max(1, int((data.get("parents") or {}).get("lookback", 400))),
        brand_cycles=cycles,
        drift_period=float(bw.get("drift_period", 20000)),
        drift_amplitude=drift_amp,
        ruido=_prob((data.get("generator") or {}).get("ruido", 0.20), "generator.ruido"),
        hashtag_probability=hprob,
        hashtag_count=(cmin, cmax),
        sector_hashtags=tuple(ht.get("sector", ["Colombia"])),
        pool_seed=int(us.get("pool_seed", 1581)),
        pool_size=pool_size,
        zipf_s=float(us.get("zipf_s", 1.1)),
        followers_mu=float(fl.get("mu", 5.0)),
        followers_sigma=float(fl.get("sigma", 1.4)),
        subreddits=tuple(rd.get("subreddits", ["r/colombia"])),
        flairs=tuple(rd.get("flairs", [])),
        flair_probability=_prob(rd.get("flair_probability", 0.0), "reddit.flair_probability"),
        media_every=max(1, int(ig.get("media_every", 40))),
        recent_media=max(1, int(ig.get("recent_media", 3))),
        media_types=media_types,
        chaos=ChaosConfig(
            enabled=chaos_enabled,
            rates=rates,
            late_minutes=_pair(ch.get("late_minutes", [2, 30]), "chaos.late_minutes"),
        ),
        raw=data,
    )
