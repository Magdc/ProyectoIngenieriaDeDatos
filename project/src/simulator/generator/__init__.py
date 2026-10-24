"""Generador de comentarios sintéticos (ARCHITECTURE.md §3.2)."""
from .brands import Brands, load_brands, normalize_text
from .comment_generator import (
    GENERATOR_ID,
    GENERATOR_NAME,
    GENERATOR_VERSION,
    Comentario,
    generate,
)

__all__ = [
    "Brands", "load_brands", "normalize_text",
    "GENERATOR_ID", "GENERATOR_NAME", "GENERATOR_VERSION", "Comentario", "generate",
]
