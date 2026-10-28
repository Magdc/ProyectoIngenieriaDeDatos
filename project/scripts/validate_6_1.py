#!/usr/bin/env python3
"""Validación automática del Lote 6.1 (docs/validation/validation-6-1.md).

    python project/scripts/validate_6_1.py preflight|tests|local|terraform|deploy|checks|latency|status|unsubscribe

La lógica está en project/src/ops/validate61.py; el despliegue, en project/scripts/quicksight.py. terraform --apply
crea los roles IAM de acceso; deploy crea o actualiza el tablero en QuickSight; unsubscribe borra la suscripción.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate61 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
