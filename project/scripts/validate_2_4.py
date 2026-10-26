#!/usr/bin/env python3
"""Validación automática del Lote 2.4 (docs/validation/validation-2-4.md).

Desde la raíz del repositorio, con ~/.venvs/ta-streaming activado:

    python project/scripts/validate_2_4.py preflight
    python project/scripts/validate_2_4.py tests
    python project/scripts/validate_2_4.py terraform [--apply] [--yes]
    python project/scripts/validate_2_4.py local [--down]
    python project/scripts/validate_2_4.py cloud [--minutes 12]
    python project/scripts/validate_2_4.py template [--skip-build]

Resumen OK / AVISO / FALLA y evidencia en project/evidencia/validacion_2_4.json.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.validate24 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
