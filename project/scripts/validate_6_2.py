#!/usr/bin/env python3
"""Validación automática del Lote 6.2 (docs/validation/validation-6-2.md).

    python project/scripts/validate_6_2.py preflight|tests|local|image|terraform|notify|prepare|flow|email|off|status

La lógica está en project/src/ops/validate62.py; las comprobaciones de las alertas, en project/scripts/alerts_check.py.
image construye el notificador; terraform --apply crea sa-alerts, el notificador, la push, los canales y la política;
prepare enciende el simulador y Dataflow (cobran hasta el off); flow mide una alerta real con una ola de prueba.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate62 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
