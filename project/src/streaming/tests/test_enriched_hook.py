"""La rama enriched está enganchada en ``pipeline.py`` (Lote 2.2).

Falla hasta que se agreguen las dos líneas de ``src/streaming/ENRICHED.md`` §1:
el import de ``attach_enriched`` y la llamada ``attach_enriched(unique)`` dentro de
``build_from_messages()``. Acepta el import en una línea, entre paréntesis o relativo.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PIPELINE = Path(__file__).resolve().parents[1] / "pipeline.py"

_IMPORT = re.compile(
    r"from\s+(?:streaming)?\.enriched\s+import\s+(?:\([^)]*\battach_enriched\b[^)]*\)|[^\n]*\battach_enriched\b)",
    re.S,
)
_MODULE_IMPORT = re.compile(r"(?:from\s+streaming\s+import\s+[^\n]*\benriched\b|import\s+streaming\.enriched)")


@pytest.mark.skipif(not PIPELINE.exists(), reason="falta streaming/pipeline.py (Lote 2.1)")
def test_pipeline_engancha_attach_enriched():
    text = PIPELINE.read_text(encoding="utf-8")
    assert _IMPORT.search(text) or _MODULE_IMPORT.search(text), (
        "pipeline.py no importa attach_enriched: agregue 'from streaming.enriched import attach_enriched' "
        "(ver src/streaming/ENRICHED.md §1)"
    )
    assert re.search(r"\battach_enriched\(", text), (
        "pipeline.py no llama attach_enriched(unique) dentro de build_from_messages (ver ENRICHED.md §1)"
    )


def test_regex_del_import():
    assert _IMPORT.search("from streaming.enriched import attach_enriched")
    assert _IMPORT.search("from streaming.enriched import (\n    EnrichedOptions,\n    attach_enriched,\n)")
    assert _IMPORT.search("from .enriched import attach_enriched")
    assert _MODULE_IMPORT.search("from streaming import enriched")
    assert not _IMPORT.search("from streaming.records import attach_enriched_x")
