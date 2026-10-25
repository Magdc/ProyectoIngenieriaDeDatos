"""Descarga los modelos de RoBERTuito en la imagen ``dataflow-nlp`` (Lote 2.3).

Se ejecuta al construir la imagen: resuelve cada revisión a su commit, descarga solo los
archivos necesarios y escribe ``models.json`` (repo, commit y carpeta de cada modelo), que es
de donde sale ``nlp_model_version``. En tiempo de ejecución no hay acceso a Hugging Face.

Uso: python fetch_models.py DESTINO [--sentiment-revision REV] [--emotion-revision REV]
"""

import argparse
import json
import os

from huggingface_hub import HfApi, snapshot_download

REPOS = {
    "sentiment": "pysentimiento/robertuito-sentiment-analysis",
    "emotion": "pysentimiento/robertuito-emotion-analysis",
}
PATTERNS = ["*.json", "*.txt", "*.model", "*.safetensors", "pytorch_model.bin", "*.md"]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("dest")
    p.add_argument("--sentiment-revision", default="main")
    p.add_argument("--emotion-revision", default="main")
    args = p.parse_args()
    api = HfApi()
    manifest = {}
    for task, repo in REPOS.items():
        rev = getattr(args, f"{task}_revision")
        info = api.model_info(repo, revision=rev)
        sha = info.sha
        target = os.path.join(args.dest, task)
        snapshot_download(repo, revision=sha, local_dir=target, allow_patterns=PATTERNS)
        files = sorted(os.listdir(target))
        if not any(f in files for f in ("model.safetensors", "pytorch_model.bin")):
            raise SystemExit(f"{repo}@{sha}: no se descargaron los pesos ({files})")
        manifest[task] = {"repo": repo, "revision": sha, "requested": rev, "path": task}
        print(f"{task}: {repo}@{sha} → {target} ({len(files)} archivos)")
    with open(os.path.join(args.dest, "models.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)


if __name__ == "__main__":
    main()
