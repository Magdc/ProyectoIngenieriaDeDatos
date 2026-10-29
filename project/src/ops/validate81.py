"""Validación automática del Lote 8.1 (HU-044 y HU-045, informe, sustentación y runbook): ``docs/validation/validation-8-1.md``.

    python project/scripts/validate_8_1.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: el informe tiene lo que pide HU-044 y su tabla de KPI coincide con
la evidencia; el guion y el runbook tienen lo que pide HU-045; los comandos y documentos citados existen), ``ensayo`` (E:
el ensayo completo en la nube: enciende el simulador y Dataflow, espera el baseline, lanza la ola de demo y mide la
alerta (las partes del 6.3), corre la prueba de humo con el exportador y el workflow forzados, refresca SPICE, apaga y
anota el resultado en ``docs/SUSTENTACION.md``) y ``status``. Evidencia en ``project/evidencia/validacion_8_1.json``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ops import validate62 as v62
from ops import validate63 as v63
from ops.validate32 import AVISO, FALLA, OK, WSL_HINT, Part, Runner, on_windows, pytest_counts
from report import kpis

REPO = v62.REPO
PROJECT = v62.PROJECT
DOCS = REPO / "docs"
EVIDENCE = PROJECT / "evidencia" / "validacion_8_1.json"
ENSAYO_START = "<!-- ensayo:inicio · lo completa `python project/scripts/validate_8_1.py ensayo` -->"
ENSAYO_END = "<!-- ensayo:fin -->"

#: HU-044: lo que el informe debe tener → texto que lo muestra.
INFORME_NEEDS = {
    "arquitectura": "## 3. Arquitectura",
    "decisiones (ADR)": "## 4. Decisiones",
    "contratos": "## 5. Contratos",
    "justificación de las fuentes simuladas": "## 2. Por qué las fuentes son simuladas",
    "KPI: latencia < 5 min": "< 5 min",
    "KPI: completitud ≥ 95 %": "≥ 95 %",
    "KPI: unicidad ≥ 99 %": "≥ 99 %",
    "KPI: duplicados < 1 %": "< 1 %",
    "KPI: tablero < 3 s": "< 3 s",
    "KPI: anonimización 100 %": "anonimizados antes de la capa analítica",
    "limitación: datos sintéticos": "Datos sintéticos",
    "limitación: plantillas con vocabulario acotado": "vocabulario acotado",
    "limitación: marca → sentimiento artificial": "marca → sentimiento artificial",
    "limitación: NLP optimista": "Métricas de NLP optimistas",
    "modelos: RoBERTuito y pysentimiento": "RoBERTuito",
    "modelos: corpus TASS": "TASS 2020",
    "modelos: licencia": "licencia",
    "KPI reproducibles": "## 9. Cómo reproducir",
}
#: HU-045: guion de punta a punta con pico y alerta, y plan B.
GUION_NEEDS = ["simulador", "Pub/Sub", "Dataflow", "BigQuery", "S3", "Athena", "QuickSight", "demo_scenario.py start",
               "alerta", "Plan B", "Capturas", "Ejecución grabada", "Simulador local", "Registro del ensayo"]
RUNBOOK_NEEDS = {"encendido y apagado": "## 1. Encender y apagar", "reinicio del simulador": "## 2. Reiniciar el simulador",
                 "reproceso desde raw/": "## 3. Reprocesar desde `raw/`", "atención de la DLQ": "## 4. Atender la DLQ",
                 "respuesta a cada alerta": "## 5. Responder a cada alerta"}

LOT_FILES = ["docs/INFORME.md", "docs/RUNBOOK.md", "docs/SUSTENTACION.md", "project/src/report/__init__.py",
             "project/src/report/kpis.py", "project/src/report/tests/test_kpis.py", "project/scripts/kpi_report.py",
             "project/src/ops/validate81.py", "project/src/ops/tests/test_ops_validate81.py", "project/scripts/validate_8_1.py"]
B1_EXPECTED = 5
B2_EXPECTED = 5


@dataclass
class Ctx(v62.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "8.1", "hu": ["HU-044", "HU-045"], "parts": {}}


# --- utilidades puras ----------------------------------------------------------------------------


def missing(text: str, needs: dict[str, str] | list[str]) -> list[str]:
    items = needs.items() if isinstance(needs, dict) else ((n, n) for n in needs)
    return [name for name, marker in items if marker not in text]


def cited_paths(text: str) -> list[str]:
    """Rutas `project/…` y `docs/…` citadas entre comillas invertidas o como enlace relativo."""
    found = set(re.findall(r"`((?:project|docs)/[\w./-]+\.(?:py|sh|md|json|sql|yaml|tf))`", text))
    found |= {f"project/scripts/{m}" for m in re.findall(r"project/scripts/([\w]+\.(?:py|sh))", text)}
    found |= {f"docs/{m}" for m in re.findall(r"\]\(([\w-]+\.md)\)", text)}
    return sorted(found)


def absent_paths(text: str, root: Path = REPO) -> list[str]:
    return [p for p in cited_paths(text) if not (root / p).exists() and "*" not in p]


def ensayo_block(text: str, body: str) -> str:
    i, j = text.find(ENSAYO_START), text.find(ENSAYO_END)
    if i < 0 or j < i:
        raise ValueError("docs/SUSTENTACION.md no tiene los marcadores del registro del ensayo")
    return text[: i + len(ENSAYO_START)] + "\n" + body.strip() + "\n" + text[j:]


# --- partes -----------------------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    absent = [f for f in LOT_FILES if not (REPO / f).exists()]
    part.add("A1", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK, ", ".join(absent) or "presentes")
    return part.finish()


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    for sid, path, expected in (("B1", "src/report/tests/test_kpis.py", B1_EXPECTED), ("B2", "src/ops/tests/test_ops_validate81.py", B2_EXPECTED)):
        res = ctx.pytest(path, "-q", "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and not c["failed"]
        part.add(sid, path, OK if good else FALLA, f"{c['passed']} passed (esperado {expected})" + ("" if good else "\n" + res.tail(8)), **c)
    return part.finish()


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Informe, guion y runbook (sin nube)")
    informe = (DOCS / "INFORME.md").read_text(encoding="utf-8")
    gaps = missing(informe, INFORME_NEEDS)
    part.add("L1", "INFORME.md: lo que pide HU-044 (arquitectura, ADR, contratos, fuentes simuladas, 6 KPI, 4 limitaciones, modelos)",
             FALLA if gaps else OK, f"falta: {gaps}" if gaps else f"{len(INFORME_NEEDS)} puntos")
    rows = kpis.build()
    table = kpis.render(rows)
    synced = kpis.splice(informe, table) == informe
    failing = [k.name for k in rows if k.ok is False]
    part.add("L2", "tabla de KPI = evidencia (kpi_report.py --check) y cada KPI con su evidencia y su comando",
             OK if synced and not failing else FALLA,
             f"{len(rows)} KPI, {sum(1 for k in rows if k.ok)} cumplen, {sum(1 for k in rows if k.ok is None)} informativo"
             + ("" if synced else "; la tabla del informe no coincide: kpi_report.py --write") + (f"; no cumplen: {failing}" if failing else ""),
             kpis=[{"kpi": k.name, "medido": k.measured, "cumple": k.ok} for k in rows])
    guion = (DOCS / "SUSTENTACION.md").read_text(encoding="utf-8")
    gaps = missing(guion, GUION_NEEDS)
    part.add("L3", "SUSTENTACION.md: guion de punta a punta con pico y alerta, plan B y registro del ensayo",
             FALLA if gaps else OK, f"falta: {gaps}" if gaps else "las 7 etapas, el pico, la alerta, el plan B y el registro")
    runbook = (DOCS / "RUNBOOK.md").read_text(encoding="utf-8")
    gaps = missing(runbook, RUNBOOK_NEEDS)
    part.add("L4", "RUNBOOK.md: encendido y apagado, reinicio del simulador, reproceso desde raw/, DLQ y alertas",
             FALLA if gaps else OK, f"falta: {gaps}" if gaps else ", ".join(RUNBOOK_NEEDS))
    absent = {name: absent_paths(text) for name, text in (("INFORME", informe), ("SUSTENTACION", guion), ("RUNBOOK", runbook))}
    absent = {k: v for k, v in absent.items() if v}
    total = sum(len(cited_paths(t)) for t in (informe, guion, runbook))
    part.add("L5", "comandos y documentos citados existen", FALLA if absent else OK, json.dumps(absent) if absent else f"{total} rutas")
    done = ENSAYO_START in guion and "Pendiente:" not in guion.split(ENSAYO_START, 1)[1].split(ENSAYO_END, 1)[0]
    part.add("L6", "ensayo completo registrado", OK if done else AVISO, "registrado" if done else "pendiente (parte ensayo, con el OK del usuario)")
    return part.finish()


def part_ensayo(ctx: Ctx) -> int:
    """El ensayo de la sustentación con las partes ya probadas: P (6.2), F (6.3), la prueba de humo, SPICE y O (6.2)."""
    part = Part(ctx, "E", "Ensayo completo en la nube")
    if not ctx.ask("¿Ensayo completo: simulador y Dataflow ~1,5 h (≈ 0,6 USD), ola de demo con su alerta y correo, prueba de "
                   "humo con el exportador y el workflow forzados, refresco de SPICE y apagado?"):
        part.add("E0", "ensayo", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    steps: dict[str, Any] = {}
    try:
        steps["prepare"] = v62.part_prepare(ctx)
        steps["flow"] = v63.part_flow(ctx) if steps["prepare"] == 0 else 1
        res = ctx.run([sys.executable, "scripts/smoke_e2e.py", "run"], cwd=PROJECT, timeout=3600, stream=True)
        steps["smoke"] = res.code
        res = ctx.run([sys.executable, "scripts/quicksight.py", "refresh"], cwd=PROJECT, timeout=1800)
        steps["refresh"] = res.code
    finally:
        steps["off"] = v62.part_off(ctx)
    f7, f11 = ctx.step_data("F", "F7"), ctx.step_data("F", "F11")
    good = all(code == 0 for code in steps.values())
    minutes = round((ctx.now() - started).total_seconds() / 60)
    body = (f"**Ensayo del {started:%d-%b-%Y} (UTC {started:%H:%M}, {minutes} min):** "
            f"{'todo OK' if good else 'con fallas'} · ola ×{f11.get('rate_ratio')} → alerta notificada a los "
            f"{f7.get('notification_s')} s del fin de la ventana · prueba de humo "
            f"{'OK' if steps.get('smoke') == 0 else 'FALLA'} · SPICE {'refrescado' if steps.get('refresh') == 0 else 'sin refrescar'} · "
            f"apagado {'OK' if steps.get('off') == 0 else 'revisar status.py'}. Evidencia: `project/evidencia/validacion_8_1.json` "
            "y `project/evidencia/smoke_e2e_5_4.json`.")
    path = DOCS / "SUSTENTACION.md"
    text = path.read_text(encoding="utf-8")
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(ensayo_block(text, body))
    part.add("E1", "ensayo completo", OK if good else FALLA, body, steps=steps, minutes=minutes)
    return part.finish()


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    rows = kpis.build()
    part.add("S1", "KPI", OK if all(k.ok is not False for k in rows) else AVISO,
             "; ".join(f"{k.name.split(':')[0]}: {k.measured}" for k in rows[:3]))
    return part.finish()


PARTS: dict[str, Callable[[Ctx], int]] = {"preflight": part_preflight, "tests": part_tests, "local": part_local,
                                         "ensayo": part_ensayo, "status": part_status}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_8_1.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    for name, kind, default in (("--brand", str, "postobon"), ("--sentiment", str, "negative"), ("--multiplier", float, 3.0),
                                ("--duration", int, 300), ("--source", str, "instagram"), ("--max-wait-minutes", float, 90),
                                ("--watch-minutes", float, 8)):
        parser.add_argument(name, type=kind, default=default)
    parser.add_argument("--force", action="store_true")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm, sleep=sleep or time.sleep,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 8.1 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    return PARTS[args.part](ctx)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
