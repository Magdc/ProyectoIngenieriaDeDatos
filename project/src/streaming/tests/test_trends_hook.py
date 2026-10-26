"""Enganche del Lote 2.4 en ``pipeline.py`` y forma de ``attach_enriched()``.

``pipeline.py`` y ``enriched.py`` son de los lotes 2.1 y 2.2; fuera del repositorio
las pruebas se omiten. Si fallan, ver ``src/streaming/TRENDS.md`` §2.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

STREAMING = Path(__file__).resolve().parents[1]
PIPELINE = STREAMING / "pipeline.py"
ENRICHED = STREAMING / "enriched.py"


def test_pipeline_engancha_attach_trends():
    if not PIPELINE.exists():
        pytest.skip("no está streaming/pipeline.py (Lote 2.1)")
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    imported = any(isinstance(n, ast.ImportFrom) and n.module == "streaming.trends"
                   and any(a.name == "attach_trends" for a in n.names) for n in ast.walk(tree))
    called = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
              and getattr(n.func, "id", getattr(n.func, "attr", None)) == "attach_trends"]
    assert imported, "falta 'from streaming.trends import attach_trends' en pipeline.py"
    assert called, "falta la llamada attach_trends(...) en pipeline.py, después de attach_enriched(unique)"
    assert called[0].args, "attach_trends necesita lo que devuelve attach_enriched(unique)"


def test_attach_trends_recibe_el_resultado_de_attach_enriched():
    if not PIPELINE.exists():
        pytest.skip("no está streaming/pipeline.py (Lote 2.1)")
    tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
    assigned = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) \
                and getattr(node.value.func, "id", getattr(node.value.func, "attr", None)) == "attach_enriched":
            assigned.update(t.id for t in node.targets if isinstance(t, ast.Name))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", getattr(n.func, "attr", None)) == "attach_trends"]
    first = calls[0].args[0] if calls and calls[0].args else None
    nested = isinstance(first, ast.Call) and getattr(first.func, "id", getattr(first.func, "attr", None)) == "attach_enriched"
    named = isinstance(first, ast.Name) and first.id in assigned
    assert nested or named, (
        "attach_trends debe recibir lo que devuelve attach_enriched(unique): "
        "'enriched_rows = attach_enriched(unique)' y luego 'attach_trends(enriched_rows)'")


def test_attach_enriched_devuelve_algo():
    if not ENRICHED.exists():
        pytest.skip("no está streaming/enriched.py (Lote 2.2)")
    tree = ast.parse(ENRICHED.read_text(encoding="utf-8"))
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "attach_enriched"]
    assert funcs, "no está attach_enriched en enriched.py"
    returns = [n for n in ast.walk(funcs[0]) if isinstance(n, ast.Return)]
    with_value = [r for r in returns if r.value is not None
                  and not (isinstance(r.value, ast.Constant) and r.value.value is None)]
    assert with_value, (
        "attach_enriched() no devuelve nada: añada 'return rows' al final (la PCollection de filas con NLP "
        "que reciben los sumideros). Ver src/streaming/TRENDS.md §2.")
