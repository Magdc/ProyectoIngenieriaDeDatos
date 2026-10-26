"""Diversidad léxica y texto repetido del generador (HU-031, riesgo de §9).

Lógica pura, sin dependencias. Los duplicados de **evento** se miden por ``event_id``
(``profile_core``); aquí solo se mide cuánto se repite el **texto**, que es esperable porque
el generador usa plantillas (§3.5: el mismo texto en dos eventos no es un duplicado).

Definiciones (todas sobre el texto en minúsculas y sin tildes, para que el ruido del
generador —quitar tildes, mayúsculas— no cuente como palabra nueva):

* **token**: secuencia de letras (``[^\\W\\d_]+``); números, emojis y signos no cuentan.
* **TTR** = tipos / tokens; **Herdan C** = log(tipos) / log(tokens); **Guiraud R** = tipos / √tokens.
* **MATTR** (ventana ``window`` tokens, por defecto 500): media del TTR en ventanas deslizantes sobre
  los tokens en el orden de lectura (archivos por clave y filas por ``event_id``). No depende del tamaño.
* **distinct-2** = bigramas distintos / bigramas (dentro de cada texto).
* **hapax** = tipos que aparecen una sola vez / tipos.
* **texto repetido**: textos idénticos (tal cual) y **esqueletos** (minúsculas, sin tildes, sin
  hashtags, alias de marca → ``<m>``, sin signos): el esqueleto mide la repetición de plantillas.
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import Counter, deque
from typing import Iterable

TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)
_HASHTAG_RE = re.compile(r"#\w+", re.UNICODE)
_NON_WORD_RE = re.compile(r"[^\w<> ]+", re.UNICODE)
_SPACES_RE = re.compile(r"\s+")
PREVIEW_CHARS = 60
TOP_REPEATED = 5
TOP_TOKENS = 15
DEFAULT_WINDOW = 500


def strip_accents(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize(text: str) -> str:
    return strip_accents(text.lower())


def tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(normalize(text))


def digest(text: str) -> int:
    """Huella de 64 bits (blake2b) para contar sin guardar el texto."""
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest(), "big")


class Skeletonizer:
    """Texto → esqueleto de plantilla (alias de marca enmascarados)."""

    def __init__(self, brand_aliases: dict[str, list[str]] | None = None):
        aliases: set[str] = set()
        for key, values in (brand_aliases or {}).items():
            aliases.add(normalize(key))
            aliases.update(normalize(v) for v in values)
        ordered = sorted((a for a in aliases if a), key=len, reverse=True)
        self._brand_re = re.compile(r"\b(" + "|".join(re.escape(a) for a in ordered) + r")\b") if ordered else None

    def __call__(self, text: str) -> str:
        t = _HASHTAG_RE.sub(" ", normalize(text))
        if self._brand_re is not None:
            t = self._brand_re.sub(" <m> ", t)
        t = _NON_WORD_RE.sub(" ", t)
        return _SPACES_RE.sub(" ", t).strip()


def _frequency_histogram(counts: Iterable[int]) -> dict[str, int]:
    hist = {"1": 0, "2": 0, "3-5": 0, "6-10": 0, "11-100": 0, ">100": 0}
    for c in counts:
        if c <= 1:
            hist["1"] += 1
        elif c == 2:
            hist["2"] += 1
        elif c <= 5:
            hist["3-5"] += 1
        elif c <= 10:
            hist["6-10"] += 1
        elif c <= 100:
            hist["11-100"] += 1
        else:
            hist[">100"] += 1
    return hist


class TextDiversity:
    """Acumula métricas de un conjunto de textos (``add`` por fila, ``result`` al final)."""

    def __init__(self, skeletonizer: Skeletonizer | None = None, *, window: int = DEFAULT_WINDOW):
        self.skeleton = skeletonizer or Skeletonizer()
        self.window = window
        self.n_texts = 0
        self.empty = 0
        self.vocab: Counter[str] = Counter()
        self.n_tokens = 0
        self.bigrams: set[tuple[str, str]] = set()
        self.n_bigrams = 0
        self.text_counts: Counter[int] = Counter()
        self.previews: dict[int, str] = {}
        self.skeleton_counts: Counter[int] = Counter()
        self._win: deque[str] = deque()
        self._win_counts: Counter[str] = Counter()
        self._ttr_sum = 0.0
        self._ttr_n = 0

    def add(self, text: str | None) -> None:
        if text is None:
            return
        self.n_texts += 1
        if not text.strip():
            self.empty += 1
        d = digest(text)
        self.text_counts[d] += 1
        if d not in self.previews and len(self.previews) < 200_000:
            self.previews[d] = text[:PREVIEW_CHARS]
        self.skeleton_counts[digest(self.skeleton(text))] += 1
        toks = tokens(text)
        self.vocab.update(toks)
        self.n_tokens += len(toks)
        for a, b in zip(toks, toks[1:]):
            self.bigrams.add((a, b))
            self.n_bigrams += 1
        for t in toks:
            self._push(t)

    def _push(self, tok: str) -> None:
        self._win.append(tok)
        self._win_counts[tok] += 1
        if len(self._win) > self.window:
            old = self._win.popleft()
            self._win_counts[old] -= 1
            if not self._win_counts[old]:
                del self._win_counts[old]
        if len(self._win) == self.window:
            self._ttr_sum += len(self._win_counts) / self.window
            self._ttr_n += 1

    def text_digests(self) -> set[int]:
        return set(self.text_counts)

    def result(self) -> dict:
        types = len(self.vocab)
        n = self.n_tokens
        distinct_texts = len(self.text_counts)
        repeated = [(d, c) for d, c in self.text_counts.items() if c > 1]
        rows_in_repeated = sum(c for _, c in repeated)
        top = sorted(repeated, key=lambda dc: (-dc[1], self.previews.get(dc[0], "")))[:TOP_REPEATED]
        distinct_skeletons = len(self.skeleton_counts)
        hapax = sum(1 for c in self.vocab.values() if c == 1)
        mattr = (self._ttr_sum / self._ttr_n) if self._ttr_n else (types / n if n else None)
        return {
            "texts": self.n_texts,
            "empty": self.empty,
            "tokens": n,
            "types": types,
            "tokens_per_text": round(n / self.n_texts, 2) if self.n_texts else None,
            "ttr": round(types / n, 5) if n else None,
            "herdan_c": round(math.log(types) / math.log(n), 4) if types > 1 and n > 1 else None,
            "guiraud_r": round(types / math.sqrt(n), 3) if n else None,
            "mattr": round(mattr, 4) if mattr is not None else None,
            "mattr_window": self.window,
            "hapax_ratio": round(hapax / types, 4) if types else None,
            "distinct_2": round(len(self.bigrams) / self.n_bigrams, 4) if self.n_bigrams else None,
            "top_tokens": [{"token": t, "count": c} for t, c in self.vocab.most_common(TOP_TOKENS)],
            "repeated": {
                "distinct_texts": distinct_texts,
                "distinct_ratio": round(distinct_texts / self.n_texts, 5) if self.n_texts else None,
                "duplicate_text_ratio": round(1 - distinct_texts / self.n_texts, 5) if self.n_texts else None,
                "texts_repeated": len(repeated),
                "rows_in_repeated": rows_in_repeated,
                "max_repetition": max((c for _, c in repeated), default=1 if self.n_texts else 0),
                "frequency_histogram": _frequency_histogram(self.text_counts.values()),
                "top": [{"preview": self.previews.get(d, ""), "count": c} for d, c in top],
            },
            "skeletons": {
                "distinct": distinct_skeletons,
                "distinct_ratio": round(distinct_skeletons / self.n_texts, 5) if self.n_texts else None,
                "max_repetition": max(self.skeleton_counts.values(), default=0),
                "frequency_histogram": _frequency_histogram(self.skeleton_counts.values()),
            },
        }
