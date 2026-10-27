#!/usr/bin/env python3
"""Prueba de humo de extremo a extremo (HU-048; ARCHITECTURE.md §3.26 y docs/validation/validation-5-4.md).

    python project/scripts/smoke_e2e.py plan     # los eventos que publicaría, sin nube
    python project/scripts/smoke_e2e.py run      # necesita Dataflow encendido (validate_5_4.py prepare)

Publica un comentario centinela con event_id conocido, lo sigue hasta ta_curated.fact_posts y escribe el informe en
project/evidencia/smoke_e2e_5_4.{json,md}. La lógica está en project/src/smoke/.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from smoke.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
