"""Punto de entrada de la plantilla Flex de Dataflow (Lote 2.4, HU-005).

El lanzador de la plantilla (imagen ``dataflow-launcher``) ejecuta este archivo con
los parámetros de la plantilla como ``--clave=valor``. Prepara el entorno y corre
``streaming.pipeline`` como ``__main__``, igual que ``dataflow_job.sh start``, así
que no depende de la firma de ``run()``.

* Trabaja desde ``src/``, como ``dataflow_job.sh``: las opciones del Lote 2.1 usan
  rutas relativas a esa carpeta (``--contracts_dir=../contracts``, ``--setup_file=./setup.py``).
  El lanzador arranca en otra carpeta (corrección de v2.26, parte F).
* ``--nlp_container_image`` (parámetro de la plantilla) pasa a ``TA_NLP_IMAGE``, que es
  lo que lee la etapa de NLP del Lote 2.3 (§5.1), y se quita de los argumentos.
* El lanzador pasa las etiquetas dos veces: juntas en un valor (``--labels=a=b,c=d``) y
  sueltas. Beam tomaría el valor junto como la etiqueta ``a`` con valor ``b,c=d`` (inválido);
  solo funciona si las sueltas vienen después. Se separan y se dejan una por clave.
* ``GCP_PROJECT`` y ``GCP_REGION`` salen de ``--project`` y ``--region`` si no
  están en el entorno (el código del Lote 2.1 los lee de ahí).
* Añade ``--streaming`` y ``--runner=DataflowRunner`` si faltan.
* El lanzador pasa ``--template_location``: con esa opción Beam solo escribe la
  descripción del job (la envía el servicio) y el resultado **no tiene job**. Se
  parchea ``DataflowPipelineResult`` para que ``job_id()`` devuelva ``None`` y
  ``wait_until_finish()`` no espere, en vez de fallar con ``AttributeError``
  (comprobado con Beam 2.77; corrección de v2.26, parte F).
"""

from __future__ import annotations

import json
import os
import runpy
import sys
from pathlib import Path
from typing import MutableMapping, Sequence

SRC_DIR = Path(__file__).resolve().parents[1]
_RENAMED_TO_ENV = {"nlp_container_image": "TA_NLP_IMAGE"}


def _value(argv: Sequence[str], name: str) -> str | None:
    flag = f"--{name}"
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(flag + "="):
            return arg.split("=", 1)[1]
    return None


def _has(argv: Sequence[str], name: str) -> bool:
    flag = f"--{name}"
    return any(arg == flag or arg.startswith(flag + "=") for arg in argv)


def _without(argv: Sequence[str], name: str) -> list[str]:
    flag = f"--{name}"
    out, skip = [], False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg == flag:
            skip = True
            continue
        if arg.startswith(flag + "="):
            continue
        out.append(arg)
    return out


def normalize_labels(argv: Sequence[str]) -> list[str]:
    """Una ``--labels=clave=valor`` por clave; separa los valores con comas (la última gana)."""
    labels: dict[str, str] = {}
    out: list[str] = []
    position = None
    args = list(argv)
    i = 0
    while i < len(args):
        arg = args[i]
        value = None
        if arg == "--labels" and i + 1 < len(args):
            value = args[i + 1]
            i += 1
        elif arg.startswith("--labels="):
            value = arg.split("=", 1)[1]
        if value is None:
            out.append(arg)
        else:
            if position is None:
                position = len(out)
            if value.lstrip().startswith("{"):
                out.append(f"--labels={value}")  # formato JSON de Beam: se deja igual
            else:
                for pair in value.split(","):
                    key, sep, val = pair.strip().partition("=")
                    if key and sep:
                        labels[key] = val
        i += 1
    if labels:
        at = len(out) if position is None else position
        out[at:at] = [f"--labels={k}={v}" for k, v in labels.items()]
    return out


def prepare_argv(argv: Sequence[str], env: MutableMapping[str, str]) -> list[str]:
    """Argumentos para ``streaming.pipeline``; ajusta ``env`` en el mismo paso."""
    out = normalize_labels(argv)
    for option, variable in _RENAMED_TO_ENV.items():
        value = _value(out, option)
        if value is not None:
            if value:
                env[variable] = value
            out = _without(out, option)
    project = _value(out, "project")
    region = _value(out, "region")
    if project and not env.get("GCP_PROJECT"):
        env["GCP_PROJECT"] = project
    if region and not env.get("GCP_REGION"):
        env["GCP_REGION"] = region
    if not _has(out, "streaming"):
        out.append("--streaming")
    if not _has(out, "runner"):
        out.append("--runner=DataflowRunner")
    return out


def patch_dataflow_result() -> bool:
    """Hace que un ``DataflowPipelineResult`` sin job (``--template_location``) no falle."""
    try:
        from apache_beam.runners.dataflow import dataflow_runner
    except ImportError:  # pragma: no cover - sin Beam no hay nada que parchear
        return False
    result_cls = getattr(dataflow_runner, "DataflowPipelineResult", None)
    if result_cls is None:  # pragma: no cover
        return False
    if getattr(result_cls, "_ta_flex_patched", False):
        return True
    original_job_id = result_cls.job_id

    def job_id(self):
        if getattr(self, "_job", None) is None:
            return None
        return original_job_id(self)

    def wait_until_finish(self, duration=None):  # noqa: ARG001
        print(json.dumps({"event": "flex_template_submitted", "job_id": job_id(self)}), flush=True)
        try:
            return self.state
        except Exception:  # noqa: BLE001
            return None

    result_cls.job_id = job_id
    result_cls.wait_until_finish = wait_until_finish
    result_cls._ta_flex_patched = True
    return True


def main() -> None:
    os.chdir(SRC_DIR)
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    sys.argv = [sys.argv[0], *prepare_argv(sys.argv[1:], os.environ)]
    print(json.dumps({"event": "flex_template_launch", "cwd": str(SRC_DIR), "argv": sys.argv[1:],
                      "ta_nlp_image": os.environ.get("TA_NLP_IMAGE")}), flush=True)
    patch_dataflow_result()
    runpy.run_module("streaming.pipeline", run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
