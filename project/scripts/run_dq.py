#!/usr/bin/env python3
"""Calidad de datos con Great Expectations (Lote 5.2; ARCHITECTURE.md §3.24).

    ~/.venvs/ta-gx/bin/python project/scripts/run_dq.py sync|run|rules [opciones]

La lógica está en project/src/dq/. Corre en el venv de GX (project/src/dq/requirements.txt), no en ta-streaming.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dq.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
