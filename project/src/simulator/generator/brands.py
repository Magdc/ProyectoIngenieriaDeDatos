"""Registro de marcas (``brands.yaml``): clave canónica, alias y detección en texto.

La detección compara texto y alias **normalizados** (minúsculas, sin tildes), de modo que el
ruido de redes (``POSTOBÓN``, ``postobon``) no hace perder la marca (§3.2.1 #4).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional, Sequence

import yaml

DEFAULT_PATH = Path(__file__).resolve().parent.parent / "config" / "brands.yaml"
_KEY_RE = re.compile(r"^[a-z0-9_]+$")
ROLES = ("propia", "competidor")


def normalize_text(s: str) -> str:
    """Minúsculas y sin tildes (``Postobón`` -> ``postobon``)."""
    d = unicodedata.normalize("NFKD", s)
    return "".join(c for c in d if not unicodedata.combining(c)).lower().strip()


@dataclass(frozen=True)
class Brand:
    key: str
    display: str
    aliases: tuple[str, ...]
    role: str
    facebook_page: Optional[str] = None
    instagram_account: Optional[str] = None


class Brands:
    def __init__(self, brands: Sequence[Brand]):
        if not brands:
            raise ValueError("brands.yaml no define ninguna marca")
        self._by_key = {b.key: b for b in brands}
        self._alias_to_key: dict[str, str] = {}
        self._patterns: list[tuple[str, re.Pattern[str]]] = []
        for b in brands:
            norm = {normalize_text(a) for a in (*b.aliases, b.display, b.key)}
            for a in norm:
                if self._alias_to_key.setdefault(a, b.key) != b.key:
                    raise ValueError(f"alias {a!r} repetido entre marcas")
            alt = "|".join(re.escape(a) for a in sorted(norm, key=len, reverse=True))
            self._patterns.append((b.key, re.compile(rf"(?<![a-z0-9])(?:{alt})(?![a-z0-9])")))

    @property
    def keys(self) -> list[str]:
        return list(self._by_key)

    def __getitem__(self, key: str) -> Brand:
        return self._by_key[key]

    def __contains__(self, key: object) -> bool:
        return key in self._by_key

    def canonical(self, name: str) -> str:
        """Clave canónica a partir de clave, display o alias (``'Postobón'`` -> ``'postobon'``)."""
        k = self._alias_to_key.get(normalize_text(name))
        if k is None:
            raise ValueError(f"marca desconocida: {name!r}; válidas: {self.keys}")
        return k

    def detect(self, text: str) -> list[str]:
        """Marcas mencionadas en ``text``, en orden de aparición y sin repetidos."""
        norm = normalize_text(text)
        found = []
        for key, pat in self._patterns:
            m = pat.search(norm)
            if m:
                found.append((m.start(), key))
        return [k for _, k in sorted(found)]


def _parse(data: dict) -> Brands:
    out = []
    for key, v in data["brands"].items():
        if not _KEY_RE.match(key):
            raise ValueError(f"clave de marca no canónica: {key!r}")
        if v.get("role") not in ROLES:
            raise ValueError(f"{key}: role debe ser uno de {ROLES}")
        if not v.get("aliases"):
            raise ValueError(f"{key}: aliases vacío")
        out.append(
            Brand(
                key=key,
                display=v["display"],
                aliases=tuple(v["aliases"]),
                role=v["role"],
                facebook_page=v.get("facebook_page"),
                instagram_account=v.get("instagram_account"),
            )
        )
    return Brands(out)


@lru_cache(maxsize=8)
def _load_cached(path: str) -> Brands:
    return _parse(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def load_brands(path: Optional[str | Path] = None) -> Brands:
    """Lee ``brands.yaml`` (por defecto el de ``simulator/config/``)."""
    return _load_cached(str(Path(path or DEFAULT_PATH).resolve()))
