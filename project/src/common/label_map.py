"""Carga y uso de ``label_map.yaml`` (ARCHITECTURE.md §3.3 y ADR-13)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml

SENTIMENT_LABELS = ("negative", "neutral", "positive")
EMOTION_LABELS = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
_PATH = Path(__file__).with_name("label_map.yaml")


@lru_cache(maxsize=1)
def _load() -> dict:
    data = yaml.safe_load(_PATH.read_text(encoding="utf-8"))
    for raw, label in data["sentiment"].items():
        if label not in SENTIMENT_LABELS:
            raise ValueError(f"label_map.yaml: sentimiento inválido {raw!r} -> {label!r}")
    for raw, label in data["emotion"].items():
        if label not in EMOTION_LABELS:
            raise ValueError(f"label_map.yaml: emoción inválida {raw!r} -> {label!r}")
    for cat, label in data["categoria_sentimiento"].items():
        if label is not None and label not in SENTIMENT_LABELS:
            raise ValueError(f"label_map.yaml: categoría {cat!r} -> {label!r}")
    return data


def map_sentiment(raw: str) -> str:
    """``NEG/NEU/POS`` (modelo) -> ``negative/neutral/positive`` (contrato)."""
    try:
        return _load()["sentiment"][raw]
    except KeyError:
        raise ValueError(f"etiqueta de sentimiento desconocida: {raw!r}") from None


def map_emotion(raw: str) -> str:
    """Etiqueta de emoción del modelo -> una de las 7 clases del contrato."""
    try:
        return _load()["emotion"][raw.lower()]
    except KeyError:
        raise ValueError(f"etiqueta de emoción desconocida: {raw!r}") from None


def categoria_sentimiento() -> dict[str, Optional[str]]:
    """Categoría del generador -> sentimiento (``None`` si es ambigua)."""
    return dict(_load()["categoria_sentimiento"])


def categorias_para(sentimiento: str) -> list[str]:
    """Categorías que producen ese sentimiento (inverso de ADR-13)."""
    if sentimiento not in SENTIMENT_LABELS:
        raise ValueError(f"sentimiento inválido: {sentimiento!r}; use {SENTIMENT_LABELS}")
    return [c for c, s in _load()["categoria_sentimiento"].items() if s == sentimiento]
