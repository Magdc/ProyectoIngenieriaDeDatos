#!/usr/bin/env python3
"""Validación automática del Lote 4.3 (docs/validation/validation-4-3.md).

    python project/scripts/validate_4_3.py preflight|tests|local|terraform|run|athena|rerun|status|off|on

La lógica está en project/src/ops/validate43.py. El horario del lakehouse queda ENCENDIDO al terminar
(decisión del usuario): off mientras se valida y on --yes al final.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate43 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
