#!/usr/bin/env python3
"""Validación automática del Lote 5.4 (docs/validation/validation-5-4.md).

    python project/scripts/validate_5_4.py preflight|tests|local|prepare|smoke|off|status

La lógica está en project/src/ops/validate54.py; la prueba de humo, en project/scripts/smoke_e2e.py. prepare enciende
Dataflow (cobra hasta off); smoke publica dos mensajes y dispara el exportador y el workflow una vez.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate54 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
