"""Ruido de redes sociales (typos, mayúsculas, alargues, jajaja...).

Port del ``ruido_texto`` del script v3 con una diferencia: los tokens de marca se **enmascaran**
antes de aplicar el ruido y se restauran después, así un *typo* nunca convierte
"Postobón" en "Pstoobón" (ARCHITECTURE.md §3.2.1 #4). Todo el azar sale del ``rng`` recibido.
"""
from __future__ import annotations

import random
import re
from typing import Iterable, Mapping

ACENTOS = str.maketrans("áéíóúÁÉÍÓÚ", "aeiouAEIOU")
ALARGUES = {"rico": "ricooo", "buena": "buenaaa", "buenísima": "buenísimaaa", "delicia": "deliciaaa",
            "asco": "ascooo", "feo": "feooo", "qué": "queee", "horrible": "horribleee",
            "amo": "amooo", "extraño": "extrañooo", "no": "nooo", "ya": "yaaa"}
ABREV = [(" porque ", " pq "), (" porque ", " xq "), (" también ", " tmb "), (" pero ", " pero "),
         (" qué ", " k "), (" que ", " k "), (" muy ", " muuy "), (" más ", " mas "),
         (" con ", " c "), (" mucho ", " muxo "), (" nada ", " na "), (" todo ", " too "),
         (" estaba ", " staba "), (" para ", " pa "), (" tomé ", " tome ")]

# Caracteres de uso privado: no son letras, dígitos ni puntuación, así que ningún filtro de
# ruido (isalpha, regex de puntuación, \b) los toca.
_MASK_A, _MASK_B = "\ue000", "\ue001"


def _typo(texto: str, rng: random.Random) -> str:
    palabras = texto.split(" ")
    idx = [i for i, p in enumerate(palabras) if len(p) > 4 and p.isalpha()]
    if not idx:
        return texto
    i = rng.choice(idx)
    p = palabras[i]
    j = rng.randrange(1, len(p) - 1)
    palabras[i] = p[:j] + p[j + 1] + p[j] + p[j + 2:]
    return " ".join(palabras)


def _alargue(texto: str, rng: random.Random) -> str:
    for k, v in ALARGUES.items():
        if re.search(rf"\b{k}\b", texto, flags=re.I) and rng.random() < 0.7:
            return re.sub(rf"\b{k}\b", v, texto, count=1, flags=re.I)
    return texto


def _ruido_clasico(texto: str, rng: random.Random, prefijo: str) -> str:
    for _ in range(rng.choice([1, 1, 2])):
        r = rng.random()
        if r < 0.20:
            texto = texto.lower()
        elif r < 0.38:
            texto = texto.translate(ACENTOS)
        elif r < 0.46:
            texto = texto.rstrip(".") + "!!!"
        elif r < 0.56:
            texto = (texto.replace(" que ", " q ").replace(" de ", " d ")
                     .replace(" para ", " pa ").replace(" por ", " x "))
        elif r < 0.62:
            texto = texto.upper()
        elif r < 0.74:
            texto = _typo(texto, rng)
        elif r < 0.82:
            texto = _alargue(texto, rng)
        elif r < 0.90:
            texto = re.sub(r"[.,;:!¡¿?]", "", texto)
        elif r < 0.96:
            texto = prefijo + texto[0].lower() + texto[1:]
        else:
            texto = texto + " " + rng.choice(["xd", "jeje", "jaja", "😂", "😭", "🙏"])
    return texto


def _ruido_extra(texto: str, rng: random.Random, interj: list[str], emojis: list[str]) -> str:
    r = rng.randrange(9)
    if r == 0:
        return rng.choice(interj) + texto[0].lower() + texto[1:]
    if r == 1:
        return texto + " " + rng.choice(emojis) * rng.choice([1, 1, 2, 3])
    if r == 2:
        a, b = rng.choice(ABREV)
        return texto.replace(a, b, 1)
    if r == 3:
        return texto.replace(". ", "... ", 1) if ". " in texto else texto + "..."
    if r == 4:
        p = texto.split(" ")
        if p[-1].rstrip(".!?").isalpha() and p[-1][-1] in "aeiouAEIOU":
            p[-1] = p[-1] + p[-1][-1] * rng.randint(2, 4)
        return " ".join(p)
    if r == 5:
        return texto.replace("ll", "y", 1).replace("v", "b", 1)
    if r == 6:
        return texto.replace("h", "", 1) if "h" in texto else texto
    if r == 7:
        return texto.rstrip(".") + rng.choice(["??", "?!", "!!", " ...", " jajaja"])
    return re.sub(r"\s+", " ", texto.replace(", ", ",  ", 1))


def _proteger(texto: str, protegidos: Iterable[str]) -> tuple[str, dict[str, str]]:
    mapa: dict[str, str] = {}
    for i, p in enumerate(sorted({p for p in protegidos if p}, key=len, reverse=True)):
        if p in texto:
            ph = f"{_MASK_A}{i}{_MASK_B}"
            texto = texto.replace(p, ph)
            mapa[ph] = p
    return texto, mapa


def aplicar_ruido(texto: str, datos_cat: Mapping, rng: random.Random,
                  protegidos: Iterable[str] = ()) -> str:
    """Aplica el ruido de redes; ``protegidos`` (alias de marca usados) quedan intactos.

    ``datos_cat`` aporta las interjecciones, emojis y prefijo propios de la categoría.
    """
    texto, mapa = _proteger(texto, protegidos)
    if rng.random() < 0.6:
        texto = _ruido_clasico(texto, rng, datos_cat["ruido_prefijo"])
    if rng.random() < 0.55 and texto:
        for _ in range(rng.choice([1, 1, 2])):
            texto = _ruido_extra(texto, rng, datos_cat["interjecciones"], datos_cat["emojis"])
    # Si el ruido dejó todo el texto en minúsculas (o mayúsculas), la marca sigue ese caso.
    letras = [c for c in re.sub(f"{_MASK_A}\\d+{_MASK_B}", "", texto) if c.isalpha()]
    caso = str.lower if letras and all(c.islower() for c in letras) else \
        str.upper if letras and all(c.isupper() for c in letras) else (lambda s: s)
    for ph, p in mapa.items():
        texto = texto.replace(ph, caso(p))
    return texto
