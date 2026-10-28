"""Comprobación de las alertas de tendencia (Lote 6.2, HU-039). Ver ``src/alerts/cli.py`` y ARCHITECTURE.md §3.28.

    python project/scripts/alerts_check.py plan | publish-test | burst [opciones]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alerts.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
