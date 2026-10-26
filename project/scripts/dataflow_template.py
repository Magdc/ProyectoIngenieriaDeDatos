#!/usr/bin/env python3
"""Plantilla Flex de Dataflow parametrizada con los outputs de Terraform (Lote 2.4, HU-005).

Desde project/, con el entorno de Python del Lote 2.1 (~/.venvs/ta-streaming) y gcloud:

    python scripts/dataflow_template.py params            # qué se toma de Terraform
    python scripts/dataflow_template.py build [--tag X]   # imagen dataflow-launcher + plantilla
    python scripts/dataflow_template.py run               # job ta-stream-{env} desde la plantilla

Todos admiten --dry-run (build y run) y --json-out. Ver src/streaming/TRENDS.md §5.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.template_tools import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
