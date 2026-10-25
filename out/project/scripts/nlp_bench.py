#!/usr/bin/env python3
"""Mide la velocidad del NLP dentro de la imagen dataflow-nlp (Lote 2.3).

Se corre DENTRO de la imagen local (tiene torch y los modelos), con el código montado:

    docker run --rm -v "$PWD/project:/work" -w /work --entrypoint python ta-dataflow-nlp-local:local scripts/nlp_bench.py

Para cada número de hilos (1 y todos los núcleos) y cada tamaño de lote (1 y 16), clasifica
64 comentarios con los dos modelos y reporta comentarios por segundo. El simulador emite unos
1,5 comentarios por segundo en total: el NLP necesita bastante margen sobre eso.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import torch  # noqa: E402

from streaming import nlp  # noqa: E402

TEXTS = [
    "Me encanta Postobón, la mejor gaseosa para el almuerzo #Colombia",
    "La Pepsi de hoy estaba sin gas, qué decepción 😞",
    "Sprite bien fría en la playa, no hay nada mejor jajaja",
    "Pepsi vs Postobón: me quedo con la de toda la vida",
] * 16


def bench(clf, batch: int) -> float:
    pre = [nlp.preprocess(t) for t in TEXTS]
    start = time.perf_counter()
    for i in range(0, len(pre), batch):
        clf.classify(pre[i:i + batch])
    return len(pre) / (time.perf_counter() - start)


def main() -> int:
    cores = os.cpu_count() or 1
    print(f"núcleos visibles: {cores} · torch {torch.__version__}")
    d = nlp.models_dir()
    for threads in sorted({1, cores}):
        torch.set_num_threads(threads)
        sent = nlp.load_classifier(d, "sentiment")
        emo = nlp.load_classifier(d, "emotion")
        for batch in (1, 16):
            rs, re_ = bench(sent, batch), bench(emo, batch)
            both = 1 / (1 / rs + 1 / re_)
            print(f"hilos={threads:<2} lote={batch:<2}  sentimiento {rs:6.1f}/s  emoción {re_:6.1f}/s  ambos {both:6.1f} comentarios/s")
    print("Referencia: el simulador emite ~1,5 comentarios/s; se busca ≥ 5/s con lote 1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
