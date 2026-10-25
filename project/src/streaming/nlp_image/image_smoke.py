"""Prueba de humo de la imagen ``dataflow-nlp`` (Lote 2.3). Falla la construcción si algo no cuadra.

* ``models.json`` existe y trae un commit por modelo.
* Las etiquetas de cada modelo son exactamente las que espera ``streaming/nlp.py``.
* El preprocesado de pysentimiento carga sin importar el paquete completo.
* Dos frases evidentes se clasifican bien (positiva y negativa) y los scores están en [0, 1].
"""

import importlib.util
import json
import os
import sys

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

MODELS = os.environ.get("TA_NLP_MODELS_DIR", "/opt/ta-nlp/models")
EXPECTED = {
    "sentiment": {"NEG", "NEU", "POS"},
    "emotion": {"others", "joy", "sadness", "anger", "surprise", "disgust", "fear"},
}
CASES = [
    ("sentiment", "Me encanta Postobón, es deliciosa y la mejor gaseosa del mundo 😍", "POS"),
    ("sentiment", "Odio esta gaseosa, es horrible y me cayó pésimo, qué asco", "NEG"),
]


def load_preprocess():
    spec = importlib.util.find_spec("pysentimiento")
    path = os.path.join(list(spec.submodule_search_locations)[0], "preprocessing.py")
    ms = importlib.util.spec_from_file_location("ta_pp", path)
    mod = importlib.util.module_from_spec(ms)
    ms.loader.exec_module(mod)
    return mod.preprocess_tweet


def main() -> int:
    manifest = json.load(open(os.path.join(MODELS, "models.json"), encoding="utf-8"))
    pre = load_preprocess()
    clfs = {}
    for task, labels in EXPECTED.items():
        entry = manifest[task]
        path = os.path.join(MODELS, entry["path"])
        tok = AutoTokenizer.from_pretrained(path, local_files_only=True)
        tok.model_max_length = 128
        model = AutoModelForSequenceClassification.from_pretrained(path, local_files_only=True)
        got = set(model.config.id2label.values())
        print(f"{task}: {entry['repo']}@{entry['revision'][:12]} etiquetas {sorted(got)}")
        if got != labels:
            print(f"ERROR: etiquetas de {task} distintas de {sorted(labels)}")
            return 1
        clfs[task] = pipeline("text-classification", model=model, tokenizer=tok, device=-1)
    for task, text, want in CASES:
        out = clfs[task](pre(text, lang="es"), truncation=True, max_length=128)[0]
        print(f"{task}: {text!r} → {out}")
        if out["label"] != want or not 0.0 <= out["score"] <= 1.0:
            print(f"ERROR: se esperaba {want}")
            return 1
    out = clfs["emotion"](pre("¡Qué alegría tomarme una Sprite bien fría!", lang="es"), truncation=True, max_length=128)[0]
    print(f"emotion: ejemplo → {out}")
    print("Prueba de humo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
