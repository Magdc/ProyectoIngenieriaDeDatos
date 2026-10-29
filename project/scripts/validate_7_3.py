"""Validación del Lote 7.3 (HU-043, control de costos). Ver ``src/ops/validate73.py`` y ``docs/validation/validation-7-3.md``.

    python project/scripts/validate_7_3.py preflight | tests | local | terraform [--apply] | cloud | fire | status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate73 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
