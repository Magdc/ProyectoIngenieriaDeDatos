"""Plantillas del generador, una por categoría (ARCHITECTURE.md §3.2.1)."""
from . import adiccion, asco, gusto, informativo, nostalgia
from .huecos import (
    ADJ_NEG, ADJ_POS, ALIAS_TEXTO, CLIMAS, COMIDAS, ENVASES, EVENTOS, LUGARES, MOMENTOS,
    PERSONAS, PRECIOS, VARIANTES, VARIANTES_GENERICAS,
)

CATEGORIAS = {
    "gusto": gusto.DATA,
    "asco": asco.DATA,
    "informativo": informativo.DATA,
    "nostalgia": nostalgia.DATA,
    "adiccion": adiccion.DATA,
}

__all__ = [
    "CATEGORIAS", "ADJ_NEG", "ADJ_POS", "ALIAS_TEXTO", "CLIMAS", "COMIDAS", "ENVASES", "EVENTOS",
    "LUGARES", "MOMENTOS", "PERSONAS", "PRECIOS", "VARIANTES", "VARIANTES_GENERICAS",
]
