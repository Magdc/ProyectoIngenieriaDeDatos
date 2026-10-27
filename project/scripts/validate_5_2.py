#!/usr/bin/env python3
"""Validación automática del Lote 5.2 (docs/validation/validation-5-2.md).

    python project/scripts/validate_5_2.py preflight|venv|tests|sync|local|publish|status

La lógica está en project/src/ops/validate52.py; el trabajo, en project/scripts/run_dq.py con el venv de Great
Expectations (~/.venvs/ta-gx). No cambia infraestructura ni horarios.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate52 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
