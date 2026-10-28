#!/usr/bin/env python3
"""Tablero de QuickSight sobre ta_curated (HU-037 y HU-038; ARCHITECTURE.md §3.27 y docs/validation/validation-6-1.md).

    python project/scripts/quicksight.py plan            # definiciones y comprobaciones, sin nube
    python project/scripts/quicksight.py deploy --yes    # fuente Athena, 4 datasets SPICE, análisis y tablero
    python project/scripts/quicksight.py status          # solo lectura

La lógica está en project/src/serving/ (definiciones en quicksight_spec.py, SQL en sql/).
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from serving.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
