#!/usr/bin/env python3
"""Verificador del Lote 2.4: trend_windows, dlq_events, latencia y dq_results.

Desde project/:

    python scripts/verify_trends.py report --local --since 2026-10-22T19:00:00Z
    python scripts/verify_trends.py report --since <arranque del job> --watch-minutes 10 --write-dq
    python scripts/verify_trends.py dlq-check --allow-cloud

Salida: 0 pasa (con o sin avisos), 1 falla, 2 error de uso o de infraestructura.
Ver docs/validation/validation-2-4.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.trends_cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
