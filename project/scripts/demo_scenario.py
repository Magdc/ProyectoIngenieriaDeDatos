"""Escenario de demo del simulador (Lote 6.3, HU-040). Ver ``src/demo/cli.py``, ``docs/DEMO.md`` y ARCHITECTURE.md §3.29.

    python project/scripts/demo_scenario.py start | status | stop | check [opciones]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from demo.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
