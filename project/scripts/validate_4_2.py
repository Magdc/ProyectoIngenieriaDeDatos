#!/usr/bin/env python3
"""Validación automática del Lote 4.2 (docs/validation/validation-4-2.md).

    python project/scripts/validate_4_2.py preflight|tests|local|terraform|run|athena|rerun|status|off

La lógica está en project/src/ops/validate42.py.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate42 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
