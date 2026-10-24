"""Forma canónica de hashtags (ARCHITECTURE.md §3.8 y §5.2).

Canónico = minúsculas, sin ``#``, sin tildes, solo ``[a-z0-9_]``, 1-100 caracteres,
sin repetidos y en orden de aparición en el texto. El texto conserva la forma original.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

MAX_LEN = 100
CANONICAL_RE = re.compile(r"^[a-z0-9_]{1,100}$")
_HASHTAG_RE = re.compile(r"#(\w+)")


def normalize_hashtag(raw: str) -> Optional[str]:
    """Devuelve la forma canónica de ``raw`` o ``None`` si no queda nada válido.

    >>> normalize_hashtag("#Postobón")
    'postobon'
    >>> normalize_hashtag("#Año_Nuevo")
    'ano_nuevo'
    """
    s = unicodedata.normalize("NFKD", raw.strip().lstrip("#"))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9_]", "", s)[:MAX_LEN]
    return s or None


def extract_hashtags(text: str) -> list[str]:
    """Hashtags del texto en forma canónica, sin repetidos y en orden de aparición."""
    vistos: dict[str, None] = {}
    for m in _HASHTAG_RE.finditer(text):
        h = normalize_hashtag(m.group(1))
        if h:
            vistos.setdefault(h, None)
    return list(vistos)
