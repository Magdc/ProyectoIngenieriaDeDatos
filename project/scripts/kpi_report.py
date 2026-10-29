"""Tabla de KPI medidos del informe (Lote 8.1, HU-044). Ver ``src/report/kpis.py`` y ``docs/INFORME.md``.

    python project/scripts/kpi_report.py [--write | --check]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from report.kpis import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
