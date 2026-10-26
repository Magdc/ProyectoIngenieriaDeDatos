#!/usr/bin/env python3
"""Profiling de Raw y Trusted del lake de S3 (Lote 5.1, HU-031; ARCHITECTURE.md §3.20).

    python project/scripts/profile_data.py sync    --layer raw
    python project/scripts/profile_data.py profile --layer raw --json-out project/evidencia/profiling_raw_5_1.json \
        --md-out project/evidencia/profiling_raw_5_1.md
    python project/scripts/profile_data.py rules   --catalog
    python project/scripts/profile_data.py synth-trusted [--defects]
    python project/scripts/profile_data.py publish --from project/evidencia/profiling_raw_5_1.json   # plan; --apply sube

La lógica está en project/src/quality/.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quality.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
