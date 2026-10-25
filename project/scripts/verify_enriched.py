#!/usr/bin/env python3
"""Verifica la rama enriched (Lote 2.2): ``enriched/`` en el lake y ``ta_stream.posts_enriched``.

Uso (desde ``project/``, con el entorno de ``src/streaming/requirements.txt``)::

    python scripts/verify_enriched.py report --local --hours 1 --json-out evidencia/local_enriched_report.json
    python scripts/verify_enriched.py report --hours 1 --json-out evidencia/enriched_report.json   # GCP

Ver ``src/streaming/ENRICHED.md``.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.enriched_cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
