#!/usr/bin/env python3
"""Validación automática del Lote 4.1 (docs/validation/validation-4-1.md).

    python project/scripts/validate_4_1.py preflight|tests|terraform|catalog|athena|workflow|status|off|on

La lógica está en project/src/ops/validate41.py.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate41 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
