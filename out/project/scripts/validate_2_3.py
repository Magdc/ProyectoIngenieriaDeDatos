#!/usr/bin/env python3
"""Valida el Lote 2.3 (NLP) siguiendo docs/validation/validation-2-3.md y deja la evidencia en evidencia/.

Uso (desde la raíz del repo, con ~/.venvs/ta-streaming activo):

    python project/scripts/validate_2_3.py                 # tests + local + cloud
    python project/scripts/validate_2_3.py tests local     # solo lo gratis
    python project/scripts/validate_2_3.py cloud --off-after
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate23 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
