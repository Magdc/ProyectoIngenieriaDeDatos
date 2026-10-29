"""KPI medidos frente a su objetivo (Lote 8.1, HU-044; ARCHITECTURE.md §3.33).

La tabla de ``docs/INFORME.md`` no se escribe a mano: sale de la evidencia de las validaciones en ``project/evidencia/``
(la prueba de humo de HU-048 para la latencia por etapa, Great Expectations del 5.2 para la calidad, el 6.1 para el
tablero, el 6.2 y el 6.3 para la detección de tendencias). Cada fila dice de qué archivo sale y con qué comando se
vuelve a medir.

    python project/scripts/kpi_report.py            # imprime la tabla
    python project/scripts/kpi_report.py --write    # la reemplaza en docs/INFORME.md
    python project/scripts/kpi_report.py --check    # 1 si docs/INFORME.md no coincide con la evidencia
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parents[2]
REPO = PROJECT.parent
EVIDENCE = PROJECT / "evidencia"
INFORME = REPO / "docs" / "INFORME.md"
START = "<!-- kpis:inicio · generado por `python project/scripts/kpi_report.py --write`; no editar a mano -->"
END = "<!-- kpis:fin -->"


@dataclass(frozen=True)
class Kpi:
    name: str
    target: str
    measured: str
    ok: bool | None  # None: informativo, sin objetivo
    source: str
    how: str


def _load(folder: Path, name: str) -> dict[str, Any]:
    return json.loads((folder / name).read_text(encoding="utf-8"))


def _steps(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {s["id"]: s for p in doc.get("parts", {}).values() for s in p.get("steps", [])}


def _pct(x: float) -> str:
    if x in (0.0, 1.0):
        return f"{x * 100:.0f} %"
    return f"{x * 100:.4f} %".replace(".", ",")


def _num(x: float, nd: int = 1) -> str:
    return f"{x:.{nd}f}".replace(".", ",")


def quality(kpis: list[dict[str, Any]], kpi: str, layer: str = "trusted") -> tuple[float, int]:
    """El peor valor de un KPI de HU-032 en una capa (y en cuántas mediciones se basa)."""
    vals = [float(k["observed"]) for k in kpis if k.get("kpi") == kpi and k.get("layer") == layer]
    if not vals:
        raise ValueError(f"sin {kpi} en {layer}")
    worst = max(vals) if kpi == "duplicados" else min(vals)
    return worst, len(vals)


def build(folder: Path = EVIDENCE) -> list[Kpi]:
    smoke = _load(folder, "smoke_e2e_5_4.json")
    stages = {s["stage"]: s for s in smoke["stages"]}
    v52 = _steps(_load(folder, "validacion_5_2.json"))
    v61 = _steps(_load(folder, "validacion_6_1.json"))
    v62 = _steps(_load(folder, "validacion_6_2.json"))
    v63 = _steps(_load(folder, "validacion_6_3.json"))
    q = v52["P2"]["data"]["kpis"]
    out = []

    bq = stages["bigquery"]
    out.append(Kpi("Latencia de ingesta: Pub/Sub → BigQuery `posts_enriched`", "< 60 s (§5.12)", f"{_num(bq['latency_s'])} s",
                   bq["latency_s"] < 60, "smoke_e2e_5_4.json (HU-048)", "`python project/scripts/smoke_e2e.py run`"))
    det = [(lot, d["F7"]["data"].get("notification_s")) for lot, d in (("6.2", v62), ("6.3", v63))]
    det = [(lot, s) for lot, s in det if s is not None]
    worst = max(s for _, s in det)
    out.append(Kpi("Latencia de detección de tendencias: fin de la ventana → notificación", "< 5 min (300 s)",
                   " y ".join(f"{_num(s)} s ({lot})" for lot, s in det), worst < 300,
                   "validacion_6_2.json, validacion_6_3.json (F7)", "`python project/scripts/validate_6_3.py flow`"))
    all_ok = all(s.get("ok") for s in smoke["stages"])
    out.append(Kpi("Punta a punta: Pub/Sub → `ta_curated.fact_posts` con el exportador y el workflow forzados",
                   "cada etapa en su objetivo (§5.12)", f"{_num(smoke['total_s'])} s; " + ", ".join(
                       f"{s['stage']} {_num(s['latency_s'])} s" for s in smoke["stages"]), all_ok,
                   "smoke_e2e_5_4.json (HU-048)", "`python project/scripts/smoke_e2e.py run`"))
    for kpi, name, target, test in (("completitud", "Completitud (capa confiable)", "≥ 95 %", lambda x: x >= 0.95),
                                    ("unicidad", "Unicidad (capa confiable)", "≥ 99 %", lambda x: x >= 0.99),
                                    ("duplicados", "Duplicados (capa confiable)", "< 1 %", lambda x: x < 0.01)):
        value, n = quality(q, kpi)
        out.append(Kpi(name, target, f"{_pct(value)} (peor de {n})", test(value), "validacion_5_2.json (Great Expectations)",
                       "`python project/scripts/run_dq.py run` (venv `~/.venvs/ta-gx`)"))
    m = re.search(r"peor ([\d.]+) s", v61["E1"]["detail"])
    worst_s = float(m.group(1)) if m else None
    m = re.search(r"máximo (\d+) ms", v61["E5"]["detail"])
    visual = int(m.group(1)) if m else None
    out.append(Kpi("Respuesta del tablero de QuickSight (SPICE)", "< 3 s", f"{_num(worst_s, 2)} s el peor clic; carga de visual máx. "
                   f"{_num(visual / 1000, 2)} s", worst_s is not None and worst_s < 3, "validacion_6_1.json (E1, E5)",
                   "`python project/scripts/validate_6_1.py latency`"))
    anon_rows = [k for k in q if k.get("kpi") == "anonimizacion"]
    anon = min(float(k["observed"]) for k in anon_rows)
    layers = ", ".join(sorted({str(k.get("layer")).capitalize() for k in anon_rows}))
    ident = smoke["identity"]
    clean = not ident.get("problems") and ident.get("same_user_hash")
    out.append(Kpi("Identificadores de usuario anonimizados antes de la capa analítica", "100 %",
                   f"{_pct(anon)} en {layers}; 0 `user_id`/`handle` en las {len(ident.get('checked', []))} etapas de la prueba de humo",
                   anon == 1.0 and bool(clean), "validacion_5_2.json, smoke_e2e_5_4.json", "`python project/scripts/run_dq.py run` y `smoke_e2e.py run`"))
    nlp = v52["L5"]["data"]["nlp"]
    out.append(Kpi("Precisión del NLP frente a la verdad del simulador (HU-033)", "informativo",
                   f"accuracy {_num(nlp['accuracy'], 4)}, F1 macro {_num(nlp['macro_f1'], 4)} "
                   f"({format(nlp['evaluated'], ',').replace(',', '.')} filas)",
                   None, "validacion_5_2.json, precision_nlp_5_2.md", "`python project/scripts/run_dq.py run`"))
    return out


def render(rows: list[Kpi]) -> str:
    mark = {True: "✅", False: "❌", None: "—"}
    lines = ["| KPI | Objetivo | Medido | Cumple | Evidencia | Cómo volver a medirlo |", "|---|---|---|---|---|---|"]
    lines += [f"| {k.name} | {k.target} | {k.measured} | {mark[k.ok]} | `project/evidencia/` {k.source} | {k.how} |" for k in rows]
    return "\n".join(lines)


def splice(text: str, table: str) -> str:
    """Reemplaza el bloque entre los marcadores de ``docs/INFORME.md``."""
    i, j = text.find(START), text.find(END)
    if i < 0 or j < i:
        raise ValueError("docs/INFORME.md no tiene los marcadores de la tabla de KPI")
    return text[: i + len(START)] + "\n" + table + "\n" + text[j:]


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    p = argparse.ArgumentParser(prog="kpi_report.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--write", action="store_true", help="reemplaza la tabla en docs/INFORME.md")
    p.add_argument("--check", action="store_true", help="falla si docs/INFORME.md no coincide con la evidencia")
    args = p.parse_args(argv)
    rows = build()
    table = render(rows)
    if args.write or args.check:
        text = INFORME.read_text(encoding="utf-8")
        new = splice(text, table)
        if args.check:
            ok = new == text
            print("docs/INFORME.md: tabla de KPI al día" if ok else "docs/INFORME.md: la tabla de KPI no coincide (--write)")
            return 0 if ok else 1
        with open(INFORME, "w", encoding="utf-8", newline="") as f:
            f.write(new)
        print(f"docs/INFORME.md: {len(rows)} KPI escritos")
        return 0
    sys.stdout.write(table + "\n")
    return 0 if all(k.ok is not False for k in rows) else 1
