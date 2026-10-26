#!/usr/bin/env python3
"""Validación automática del Lote 5.1 (docs/validation/validation-5-1.md).

    python project/scripts/validate_5_1.py preflight|tests|raw|trusted|all|clean

La lógica está en project/src/ops/validate51.py.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate51 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
