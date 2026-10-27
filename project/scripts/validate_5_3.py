#!/usr/bin/env python3
"""Validación automática del Lote 5.3 (docs/validation/validation-5-3.md).

    python project/scripts/validate_5_3.py preflight|tests|local|terraform|glue|controls|status

La lógica está en project/src/ops/validate53.py. Solo `terraform --apply` cambia la nube (descripciones y comentarios
del catálogo de Glue); el resto lee. No toca datos, jobs ni el horario.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ops.validate53 import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
