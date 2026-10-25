"""La rama enriched está enganchada en ``pipeline.py`` (Lote 2.2).

Falla hasta que se agreguen las dos líneas de ``src/streaming/ENRICHED.md`` §1:
``from streaming.enriched import attach_enriched`` y ``attach_enriched(unique)``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PIPELINE = Path(__file__).resolve().parents[1] / "pipeline.py"


@pytest.mark.skipif(not PIPELINE.exists(), reason="falta streaming/pipeline.py (Lote 2.1)")
def test_pipeline_engancha_attach_enriched():
    text = PIPELINE.read_text(encoding="utf-8")
    assert re.search(r"from\s+streaming\.enriched\s+import\s+[^\n]*attach_enriched", text), (
        "pipeline.py no importa attach_enriched (ver src/streaming/ENRICHED.md §1)"
    )
    assert re.search(r"attach_enriched\(", text), (
        "pipeline.py no llama attach_enriched(...) tras build_from_messages (ver ENRICHED.md §1)"
    )
