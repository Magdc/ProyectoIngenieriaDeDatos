#!/usr/bin/env python3
"""Crea en el emulador de Pub/Sub los topics y suscripciones de §5.3 (Lote 1.5).

Lo ejecuta el servicio ``pubsub-init`` de ``docker-compose.yml``. Se niega a
correr sin ``PUBSUB_EMULATOR_HOST``: en GCP la topología la crea Terraform.
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingest_check.emulator import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
