"""Validación del Lote 7.2 (HU-042, observabilidad). Ver ``src/ops/validate72.py`` y ``docs/validation/validation-7-2.md``.

    python project/scripts/validate_7_2.py preflight | tests | local | terraform [--apply] | cloud | prepare | metrics | fire | off | status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate72 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
