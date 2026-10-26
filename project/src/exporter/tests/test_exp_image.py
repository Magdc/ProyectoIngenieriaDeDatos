"""Imagen del job: Dockerfile, Cloud Build y dependencias fijadas."""

import re
from pathlib import Path

PKG = Path(__file__).resolve().parents[1]


def test_requirements_con_versiones_exactas():
    lines = [ln.strip() for ln in (PKG / "requirements.txt").read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    assert lines and all(re.fullmatch(r"[a-z0-9-]+==\d+(\.\d+)+", ln) for ln in lines), lines
    names = {ln.split("==")[0] for ln in lines}
    assert names == {"google-cloud-storage", "boto3"}
    assert "google-cloud-storage==3.17.1" in lines  # la misma del simulador y del stream


def test_dockerfile_publica_runtime_sin_privilegios_y_prueba_sin_omitidas():
    text = (PKG / "Dockerfile").read_text()
    stages = re.findall(r"^FROM\s+\S+\s+AS\s+(\w+)", text, flags=re.M)
    assert stages == ["deps", "runtime", "test"]  # runtime antes de test: el builder clásico no corre test
    runtime = text.split("AS runtime", 1)[1].split("AS test", 1)[0]
    assert "USER 10001" in runtime and 'ENTRYPOINT ["python", "-m", "exporter.cli"]' in runtime and 'CMD ["run"]' in runtime
    assert "rm -rf exporter/tests" in runtime
    test = text.split("AS test", 1)[1]
    assert '-m "not repo and not pyarrow"' in test and "SKIPPED" in test
    assert "python:3.11" in text


def test_cloudbuild_prueba_antes_de_publicar():
    text = (PKG / "cloudbuild.yaml").read_text()
    assert text.index("--target\", \"test") < text.index("--target\", \"runtime")
    assert "${_IMAGE}:${_TAG}" in text and "${_IMAGE}:latest" in text


def test_el_paquete_no_importa_otros_lotes():
    for path in PKG.glob("*.py"):
        text = path.read_text()
        for mod in ("streaming", "simulator", "historical", "ops", "common"):
            imports = re.findall(rf"^(\s*)(?:from|import)\s+{mod}\b", text, flags=re.M)
            if mod == "common" and path.name == "lake_check.py":
                # solo dentro de una función (parte F), nunca al importar el módulo
                assert imports and all(indent for indent in imports), path.name
                continue
            assert not imports, (path.name, mod)
