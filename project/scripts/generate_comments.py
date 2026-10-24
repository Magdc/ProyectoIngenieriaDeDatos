#!/usr/bin/env python3
"""CLI del generador de comentarios: snapshots CSV e histórico (ARCHITECTURE.md §3.2, §3.4).

Uso (desde ``project/``):
    python scripts/generate_comments.py --n 50000 --seed 7 --out comentarios.csv
    python scripts/generate_comments.py --n 1000 --sentimiento negative --marca postobon
    python scripts/generate_comments.py --n 5000 --unico --categoria --ruido 0.25

Salida CSV (UTF-8; los comentarios con comas se entrecomillan con el módulo ``csv``):
    marca,comentario,sentimiento[,categoria]
``marca`` es la clave canónica de brands.yaml (``postobon``); ``sentimiento`` queda vacío
para las categorías ambiguas (``nostalgia``, ``adiccion``; ADR-13).

Reproducible: la fila ``i`` sale de ``Random(f"{seed}|batch|{i}")`` (no depende del orden ni de
``hash()``); misma semilla y mismos argumentos => mismo archivo, byte a byte.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.envelope import SENTIMENTS, event_rng  # noqa: E402
from simulator.generator import GENERATOR_ID, generate, load_brands  # noqa: E402
from simulator.generator.comment_generator import DEFAULT_RUIDO  # noqa: E402
from simulator.generator.templates import CATEGORIAS  # noqa: E402

LOTE = 50_000


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-n", "--n", "--filas", dest="n", type=int, default=1000,
                    help="número de filas (por defecto 1000)")
    ap.add_argument("-o", "--out", "--salida", dest="out", default="comentarios.csv")
    ap.add_argument("--seed", type=int, default=int(os.environ.get("SIM_SEED", "7")),
                    help="semilla (por defecto SIM_SEED o 7)")
    ap.add_argument("--ruido", type=float, default=DEFAULT_RUIDO,
                    help="probabilidad (0-1) de aplicar ruido de redes (0 lo apaga)")
    ap.add_argument("--marca", action="append",
                    help="limita a estas marcas (clave o alias); se puede repetir")
    ap.add_argument("--sentimiento", choices=SENTIMENTS,
                    help="fuerza el sentimiento (restringe la categoría, ADR-13)")
    ap.add_argument("--solo-categoria", choices=list(CATEGORIAS), dest="solo_categoria",
                    help="fuerza la categoría")
    ap.add_argument("--categoria", action="store_true",
                    help="agrega una columna con la categoría")
    ap.add_argument("--unico", action="store_true",
                    help="evita duplicados exactos (hash estable con hashlib; usa más memoria)")
    ap.add_argument("--brands", help="ruta a un brands.yaml distinto del de simulator/config/")
    ap.add_argument("--bom", action="store_true",
                    help="escribe UTF-8 con BOM (para abrir en Excel de Windows)")
    return ap


def _clave_unica(marca: str, texto: str) -> bytes:
    return hashlib.blake2b(f"{marca}\x00{texto}".encode("utf-8"), digest_size=8).digest()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.n < 0:
        print("error: --n debe ser >= 0", file=sys.stderr)
        return 2
    if not 0.0 <= args.ruido <= 1.0:
        print("error: --ruido debe estar en [0, 1]", file=sys.stderr)
        return 2

    brands = load_brands(args.brands)
    try:
        marcas = [brands.canonical(m) for m in args.marca] if args.marca else brands.keys
        # Valida la combinación de argumentos una vez, con un error legible.
        generate(event_rng(args.seed, "batch", 0), marcas[0], args.sentimiento,
                 categoria=args.solo_categoria, ruido=0, brands=brands)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    encabezado = ["marca", "comentario", "sentimiento"] + (["categoria"] if args.categoria else [])
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    vistos: set[bytes] = set()
    escritas = intentos = 0
    max_intentos = args.n * 20
    t0 = time.time()
    with open(out, "w", newline="", encoding="utf-8-sig" if args.bom else "utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(encabezado)
        lote: list[list[str]] = []
        while escritas < args.n:
            if args.unico and intentos >= max_intentos:
                print(f"\nAviso: ya no hay combinaciones únicas; se detiene en {escritas:,} filas.",
                      file=sys.stderr)
                break
            rng = event_rng(args.seed, "batch", intentos)
            intentos += 1
            c = generate(rng, rng.choice(marcas), args.sentimiento,
                         categoria=args.solo_categoria, ruido=args.ruido, brands=brands)
            if args.unico:
                k = _clave_unica(c.marca, c.texto)
                if k in vistos:
                    continue
                vistos.add(k)
            fila = [c.marca, c.texto, c.sentimiento or ""] + ([c.categoria] if args.categoria else [])
            lote.append(fila)
            escritas += 1
            if len(lote) >= LOTE:
                w.writerows(lote)
                lote.clear()
                print(f"\r{escritas:,} / {args.n:,} filas", end="", file=sys.stderr, flush=True)
        if lote:
            w.writerows(lote)

    print(f"Listo: {escritas:,} filas en {out} ({time.time() - t0:.1f} s) · generador {GENERATOR_ID}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
