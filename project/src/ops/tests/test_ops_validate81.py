"""Pruebas del validador del Lote 8.1 con los documentos reales (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ops import validate81 as v

NOW = datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc)


def test_the_real_documents_have_what_hu044_and_hu045_ask():
    assert v.missing((v.DOCS / "INFORME.md").read_text(encoding="utf-8"), v.INFORME_NEEDS) == []
    assert v.missing((v.DOCS / "SUSTENTACION.md").read_text(encoding="utf-8"), v.GUION_NEEDS) == []
    assert v.missing((v.DOCS / "RUNBOOK.md").read_text(encoding="utf-8"), v.RUNBOOK_NEEDS) == []
    assert v.missing("solo arquitectura", {"a": "## 3. Arquitectura", "b": "x"}) == ["a", "b"]


def test_cited_paths_exist():
    for name in ("INFORME.md", "SUSTENTACION.md", "RUNBOOK.md"):
        text = (v.DOCS / name).read_text(encoding="utf-8")
        assert v.cited_paths(text), name
        assert v.absent_paths(text) == [], name
    assert v.absent_paths("ver `project/scripts/no_existe.py` y [x](NOPE.md)") == ["docs/NOPE.md", "project/scripts/no_existe.py"]


def test_ensayo_block():
    text = f"x\n{v.ENSAYO_START}\nPendiente: algo\n{v.ENSAYO_END}\n"
    out = v.ensayo_block(text, "**Ensayo del 29-oct:** todo OK")
    assert "Pendiente" not in out and "todo OK" in out and out.endswith(f"{v.ENSAYO_END}\n")


class FakeRunner:
    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, **kw):
        from ops.validate32 import Result

        return Result(1, "", "inesperado")


def test_local_with_the_real_documents(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    code = v.main(["local"], runner=FakeRunner(), printer=lambda s: None, now=lambda: NOW, evidence_path=tmp_path / "ev.json")
    steps = {s["id"]: s for s in json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))["parts"]["L"]["steps"]}
    assert code == 0, {k: s["detail"] for k, s in steps.items() if s["status"] == "FALLA"}
    assert all(steps[k]["status"] == "OK" for k in ("L1", "L2", "L3", "L4", "L5"))


def test_ensayo_needs_confirmation(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    code = v.main(["ensayo"], runner=FakeRunner(), printer=lambda s: None, confirm=lambda q: False, now=lambda: NOW,
                  evidence_path=tmp_path / "ev.json")
    step = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))["parts"]["E"]["steps"][0]
    assert code == 0 and step["status"] == "AVISO"
