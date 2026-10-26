#!/usr/bin/env python3
"""Carga histórica (Lote 3.1, HU-024): plan, doctor, generate, nlp, write, verify, upload, status.

Se corre desde ``project/`` o desde la raíz del repo; guía en ``src/historical/HISTORICAL.md``
y validación en ``docs/validation/validation-3-1.md``.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from historical.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
