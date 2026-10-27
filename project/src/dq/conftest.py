"""Las pruebas de ``dq`` que usan Great Expectations (``tests/test_dq_gx*.py``) solo corren en el venv de GX
(``~/.venvs/ta-gx``); en ``ta-streaming`` no se recogen (no son omitidas: no existen ahí). ``test_dq_identity.py``
(sin GX) corre en los dos."""

import importlib.util

collect_ignore_glob = [] if importlib.util.find_spec("great_expectations") else ["tests/test_dq_gx*.py"]
