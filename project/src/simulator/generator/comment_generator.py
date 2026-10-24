"""Generador de comentarios sintéticos sobre gaseosas colombianas (ARCHITECTURE.md §3.2).

Adaptación del script v3 (``generar_comentarios.py``) como **función pura**:

    generate(rng, marca=None, sentimiento=None) -> Comentario

* Todo el azar sale del ``random.Random`` inyectado (nada global, ni ``hash()``): misma semilla
  de ``rng`` => mismo comentario, en cualquier proceso (§3.6).
* No duerme, no imprime, no escribe archivos ni mantiene estado: el ritmo, el cursor y la elección
  de marca/sentimiento los controla el simulador (§3.5, §3.8).
* La marca se devuelve en forma canónica (``postobon``); el texto usa variantes (``Postobón``,
  ``Postobon``...). Con ruido, los tokens de marca no se dañan (ver ``noise.py``).
* ``sentimiento`` (``positive|negative|neutral``) restringe la categoría; el ground truth de cada
  categoría sale de ``common/label_map.yaml`` (ADR-13).

Pensado para ``PYTHONPATH=project/src``.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from common.label_map import categoria_sentimiento, categorias_para

from .brands import Brands, load_brands
from .noise import aplicar_ruido
from .templates import (
    ADJ_NEG, ADJ_POS, ALIAS_TEXTO, CATEGORIAS, CLIMAS, COMIDAS, ENVASES, EVENTOS, LUGARES,
    MOMENTOS, PERSONAS, PRECIOS, VARIANTES, VARIANTES_GENERICAS,
)

GENERATOR_NAME = "comment-gen"
GENERATOR_VERSION = "1.0"
GENERATOR_ID = f"{GENERATOR_NAME}@{GENERATOR_VERSION}"  # valor de `generator` en el envelope (§5.2)

DEFAULT_RUIDO = 0.20

# Peso de cada categoría cuando no se fija sentimiento ni categoría. El simulador puede forzar
# el sentimiento (escenarios, §3.7); esto solo define la mezcla "natural".
PESOS_CATEGORIA = {"gusto": 0.28, "asco": 0.24, "informativo": 0.18, "nostalgia": 0.15, "adiccion": 0.15}

P_COMPARATIVA = 0.30   # dentro de "informativo", si hay >= 2 marcas
P_GENERATIVA = 0.55    # "{verbo} {m} {complemento}." frente a plantilla fija
P_CONTEXTO = 0.40


@dataclass(frozen=True)
class Comentario:
    marca: str                    # clave canónica de brands.yaml (p. ej. "postobon")
    texto: str
    categoria: str                # gusto | asco | informativo | nostalgia | adiccion
    sentimiento: Optional[str]    # positive | negative | neutral | None (ADR-13)


class _Huecos(dict):
    """Rellena ``{envase}``, ``{lugar}``... con valores al azar, una vez por comentario."""

    def __init__(self, marca: str, brands: Brands, rng: random.Random):
        self.marca, self.brands, self.rng = marca, brands, rng
        self.alias = rng.choice(_alias_texto(marca, brands))
        super().__init__(m=self.alias)
        self.marca2: Optional[str] = None
        self.alias2: Optional[str] = None
        self.listas = {
            "envase": ENVASES, "comida": COMIDAS, "lugar": LUGARES, "momento": MOMENTOS,
            "persona": PERSONAS, "variante": VARIANTES.get(marca, VARIANTES_GENERICAS),
            "precio": PRECIOS, "clima": CLIMAS, "evento": EVENTOS, "adjpos": ADJ_POS,
            "adjneg": ADJ_NEG,
        }

    def __missing__(self, clave: str) -> str:
        if clave == "m2":
            otras = [k for k in self.brands.keys if k != self.marca]
            if not otras:
                raise KeyError("m2: el registro solo tiene una marca")
            self.marca2 = self.rng.choice(otras)
            self.alias2 = valor = self.rng.choice(_alias_texto(self.marca2, self.brands))
        else:
            valor = self.rng.choice(self.listas[clave])
        self[clave] = valor
        return valor

    @property
    def marcas(self) -> list[str]:
        return [self.marca] + ([self.marca2] if self.marca2 else [])

    @property
    def aliases(self) -> list[str]:
        return [a for a in (self.alias, self.alias2) if a]


def _alias_texto(marca: str, brands: Brands) -> list[str]:
    return ALIAS_TEXTO.get(marca) or list(brands[marca].aliases)


def _llenar(plantilla: str, huecos: _Huecos) -> str:
    # Dos pasadas: ENVASES contiene {m}.
    return plantilla.format_map(huecos).format_map(huecos)


def _plantilla_nucleo(rng: random.Random, d: dict, hay_otras_marcas: bool) -> str:
    if d["comparativos"] and hay_otras_marcas and rng.random() < P_COMPARATIVA:
        return rng.choice(d["comparativos"])
    if rng.random() < P_GENERATIVA:
        return f"{rng.choice(d['gen_verbos'])} {{m}} {rng.choice(d['gen_compl'])}."
    return rng.choice(d["nucleos"])


def _resolver_categoria(rng: random.Random, sentimiento: Optional[str],
                        categoria: Optional[str]) -> str:
    if categoria is not None and categoria not in CATEGORIAS:
        raise ValueError(f"categoría inválida: {categoria!r}; use {list(CATEGORIAS)}")
    if sentimiento is not None:
        permitidas = categorias_para(sentimiento)  # ValueError si el sentimiento no existe
        if categoria is not None:
            if categoria not in permitidas:
                raise ValueError(f"la categoría {categoria!r} no produce sentimiento {sentimiento!r}")
            return categoria
        return rng.choice(permitidas)
    if categoria is not None:
        return categoria
    cats = list(PESOS_CATEGORIA)
    return rng.choices(cats, weights=[PESOS_CATEGORIA[c] for c in cats], k=1)[0]


def generate(
    rng: random.Random,
    marca: Optional[str] = None,
    sentimiento: Optional[str] = None,
    *,
    categoria: Optional[str] = None,
    ruido: float = DEFAULT_RUIDO,
    brands: Optional[Brands] = None,
) -> Comentario:
    """Genera un comentario.

    ``marca``: clave, display o alias de brands.yaml (se normaliza a la clave canónica); ``None``
    => una al azar. ``sentimiento``: ``positive|negative|neutral`` o ``None``. ``categoria`` fija
    la categoría (debe ser coherente con ``sentimiento``). ``ruido`` = probabilidad de aplicar
    ruido de redes (0 lo apaga). Lanza ``ValueError`` ante argumentos inválidos.
    """
    brands = brands or load_brands()
    marca = brands.canonical(marca) if marca is not None else rng.choice(brands.keys)
    cat = _resolver_categoria(rng, sentimiento, categoria)
    d = CATEGORIAS[cat]

    h = _Huecos(marca, brands, rng)
    apertura = rng.choice(d["aperturas"])
    nucleo = _llenar(_plantilla_nucleo(rng, d, len(brands.keys) > 1), h)
    # Tras una apertura la frase central empieza en minúscula (salvo que arranque con la marca).
    if apertura and not nucleo.startswith(h.alias):
        nucleo = nucleo[0].lower() + nucleo[1:]
    partes = [apertura + nucleo]
    if rng.random() < P_CONTEXTO:
        partes.append(_llenar(rng.choice(d["contextos"]), h))
    r = rng.random()
    n_reac = 0 if r < 0.25 else (1 if r < 0.85 else 2)
    if n_reac:
        partes += rng.sample(d["reacciones"], n_reac)
    texto = (" ".join(partes) + _llenar(rng.choice(d["cierres"]), h)).strip()
    if texto[0].islower():
        texto = texto[0].upper() + texto[1:]

    if ruido and rng.random() < ruido:
        ruidoso = aplicar_ruido(texto, d, rng, protegidos=h.aliases)
        # Garantía final: el ruido nunca puede hacer perder una marca (§3.2.1 #4).
        if set(h.marcas) <= set(brands.detect(ruidoso)):
            texto = ruidoso

    return Comentario(marca, texto, cat, categoria_sentimiento()[cat])
