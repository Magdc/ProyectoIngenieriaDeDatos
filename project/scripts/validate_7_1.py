"""Validación del Lote 7.1 (HU-041, CI/CD). Ver ``src/ops/validate71.py`` y ``docs/validation/validation-7-1.md``.

    python project/scripts/validate_7_1.py preflight | tests | local | emulate | plan | github | status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate71 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
