#!/usr/bin/env python3
"""Verifica el pipeline de streaming (Lote 2.1). Ver `src/streaming/cli.py` y ARCHITECTURE.md §3.13.

    python scripts/verify_streaming.py report --root gs://PROYECTO-ta-lake
    python scripts/verify_streaming.py check --allow-cloud --json-out evidencia/streaming_check.json
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
