"""Validación del Lote 6.3 (HU-040, escenario de demo). Ver ``src/ops/validate63.py`` y ``docs/validation/validation-6-3.md``.

    python project/scripts/validate_6_3.py preflight | tests | local | image | prepare | auth | flow | off | status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate63 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
