"""Selección de marca con pesos que varían con ``seq`` y hashtags de marca (§3.8).

Reutiliza ``generator.brands`` para leer ``brands.yaml`` (no hay una segunda lectura
del archivo, §3.2.2). Los pesos son una función **determinista** de ``seq``: no usan
azar, así que dos corridas con la misma semilla producen las mismas tendencias.
"""

from __future__ import annotations

import math
import random
from typing import Iterable, Sequence

from common.hashtags import extract_hashtags, normalize_hashtag
from simulator.generator.brands import Brands, load_brands, normalize_text
from simulator.sim_config import SimConfig

__all__ = ["BrandSelector", "Brands", "load_brands", "brand_keys", "brands_in_text", "add_hashtags", "first_sentence"]


class BrandSelector:
    """Elige la marca de cada evento según ``brand_weights`` de ``sim.yaml``."""

    def __init__(self, brands: Brands, config: SimConfig) -> None:
        keys = brand_keys(brands)
        missing = [k for k in keys if k not in config.brand_cycles]
        extra = [k for k in config.brand_cycles if k not in keys]
        if extra:
            raise ValueError(f"sim.yaml define pesos para marcas que no están en brands.yaml: {extra}")
        self.brands = brands
        self.keys: tuple[str, ...] = keys
        self._cycles = config.brand_cycles
        self._missing = set(missing)  # sin ciclo: peso plano 1/n
        self._drift_period = config.drift_period
        self._drift_amp = config.drift_amplitude

    def weights(self, seq: int) -> dict[str, float]:
        """Pesos normalizados (suman 1) para la posición ``seq``."""
        raw: dict[str, float] = {}
        flat = 1.0 / len(self.keys)
        for k in self.keys:
            if k in self._missing:
                raw[k] = flat
                continue
            c = self._cycles[k]
            cyc = 1.0 + c.amplitude * math.sin(2 * math.pi * seq / c.period + c.phase)
            drift = 1.0 + self._drift_amp * math.sin(2 * math.pi * seq / self._drift_period + c.phase)
            raw[k] = c.base * cyc * drift
        total = sum(raw.values())
        return {k: v / total for k, v in raw.items()}

    def choose(self, rng: random.Random, seq: int) -> str:
        """Una sola extracción de ``rng`` (orden estable para ``peek``)."""
        w = self.weights(seq)
        return rng.choices(self.keys, weights=[w[k] for k in self.keys], k=1)[0]

    def info(self, key: str) -> dict:
        """Entrada de ``brands.yaml`` (``display``, ``facebook_page``, ``instagram_account``…)."""
        if key not in self.keys:
            raise KeyError(f"marca desconocida: {key!r}")
        if self._info is None:
            self._info = _brand_entries(self.brands)
        return self._info[key]

    _info: dict | None = None


def _brand_entries(brands: Brands) -> dict:
    """Entradas completas de ``brands.yaml``.

    Usa lo que ya cargó ``generator.brands`` si lo expone (``data``/``raw``/``entries``);
    si no, lee el mismo archivo que ``load_brands`` usa por defecto.
    """
    for attr in ("data", "raw", "entries", "_data", "_raw"):
        val = getattr(brands, attr, None)
        if isinstance(val, dict) and val and all(isinstance(v, dict) for v in val.values()):
            return dict(val.get("brands", val)) if "brands" in val and isinstance(val["brands"], dict) else dict(val)
    import os
    from pathlib import Path

    import yaml

    path = getattr(brands, "path", None) or os.environ.get("SIM_BRANDS") or (
        Path(__file__).resolve().parent.parent / "config" / "brands.yaml"
    )
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return dict(data.get("brands", data))


def brand_keys(brands: Brands) -> tuple[str, ...]:
    """Claves canónicas de ``brands.yaml`` (``Brands.keys`` puede ser atributo o método)."""
    keys = brands.keys
    return tuple(keys() if callable(keys) else keys)


def brands_in_text(chosen: str, text: str, brands: Brands) -> list[str]:
    """``brands[]`` = marca elegida + otras detectadas por alias, sin repetidos (§3.8)."""
    out = [chosen]
    found = brands.detect(text) or ()
    if isinstance(found, str):
        found = (found,)
    for b in found:
        if b not in out:
            out.append(b)
    return out


def _brand_tag(display: str) -> str:
    """``Postobón`` -> ``Postobon`` (hashtag legible; la forma canónica la da normalize_hashtag)."""
    plain = normalize_text(display)
    words = [w for w in "".join(ch if ch.isalnum() else " " for ch in plain).split() if w]
    return "".join(w[:1].upper() + w[1:] for w in words) or plain


def add_hashtags(
    rng: random.Random,
    text: str,
    *,
    brand_display: str,
    probability: float,
    count: tuple[int, int],
    sector: Sequence[str],
) -> str:
    """Con probabilidad ``probability`` añade 1–2 hashtags que falten al final del texto.

    Siempre hace las mismas extracciones de ``rng`` (aunque no añada nada), para que
    el resto del evento no dependa de si se añadieron o no.
    """
    roll = rng.random()
    n = rng.randint(count[0], count[1]) if count[1] > 0 else 0
    pool = [_brand_tag(brand_display), *sector]
    order = rng.sample(range(len(pool)), len(pool))
    if roll >= probability or n == 0:
        return text
    present = set(extract_hashtags(text))
    picked: list[str] = []
    for i in order:
        tag = pool[i]
        canon = normalize_hashtag(tag)
        if not canon or canon in present or canon in {normalize_hashtag(t) for t in picked}:
            continue
        picked.append(tag)
        if len(picked) == n:
            break
    if not picked:
        return text
    return text.rstrip() + " " + " ".join(f"#{t}" for t in picked)


def first_sentence(text: str, max_len: int = 300) -> str:
    """Primera frase (título de un post de Reddit, §3.8)."""
    t = text.strip()
    for i, ch in enumerate(t):
        if ch in ".!?…" and i > 0:
            cand = t[: i + 1].strip()
            if len(cand) >= 3:
                return cand[:max_len]
    return t[:max_len] or t


def unique(seq: Iterable[str]) -> list[str]:
    out: list[str] = []
    for x in seq:
        if x not in out:
            out.append(x)
    return out
