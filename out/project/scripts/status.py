#!/usr/bin/env python3
"""Muestra qué está encendido, en local y en GCP (solo lectura).

Uso (desde la raíz del repo o desde project/, con ~/.venvs/ta-streaming activo):

    python project/scripts/on.py [all|local|cloud] [--nlp auto|on|off] [--restart-job] [--yes]
    python project/scripts/off.py [all|local|cloud] [--keep-simulator] [--force] [--yes]
    python project/scripts/status.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ops.services import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main("status"))
