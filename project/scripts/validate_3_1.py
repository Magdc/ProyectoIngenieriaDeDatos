#!/usr/bin/env python3
"""Validación automática del Lote 3.1 (HU-024): partes A a F de docs/validation/validation-3-1.md.

Desde la raíz del repo, con ~/.venvs/ta-streaming activado:
    python project/scripts/validate_3_1.py preflight | tests | terraform [--apply] | local [--build] | load | upload
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate31 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
