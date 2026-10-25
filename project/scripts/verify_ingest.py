#!/usr/bin/env python3
"""Verificación de la ingesta del simulador (Lote 1.4, HU-017).

Uso (desde ``project/``, con ``gcloud auth application-default login`` hecho):

    python scripts/verify_ingest.py measure --duration 120
    python scripts/verify_ingest.py restart-check --restart manual
    python scripts/verify_ingest.py measure --json-out evidencia/ingesta.json

Crea una suscripción temporal por topic (``ta-verify-{source}-{run}``), mide y
la borra. No toca ``ta-raw-{source}-df`` (Dataflow). Ver ``ARCHITECTURE.md`` §3.11
y ``python scripts/verify_ingest.py <comando> --help``.
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingest_check.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
