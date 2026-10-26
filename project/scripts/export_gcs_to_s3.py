#!/usr/bin/env python3
"""CLI del exportador gcs-to-s3 (Lote 3.2): run, plan, probe, token, doctor, verify, status,
lake-report. Guía en src/exporter/EXPORTER.md; contrato en ARCHITECTURE.md §3.18 y §5.6."""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\.api_core")

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from exporter.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
