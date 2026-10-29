"""Validación del Lote 8.1 (HU-044 y HU-045). Ver ``src/ops/validate81.py`` y ``docs/validation/validation-8-1.md``.

    python project/scripts/validate_8_1.py preflight | tests | local | ensayo | status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate81 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
