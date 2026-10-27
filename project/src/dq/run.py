"""Una corrida de calidad (Lote 5.2): suites de GX sobre Raw, Trusted y Curated, filas de ``dq_results`` y salidas.

``run(root, out_dir, …)`` lee la copia local del lake (``root`` = ``{caché}/s3/{bucket}``), valida, arma las filas de
``dq_results`` (§5.5, una por expectativa y origen), el resumen con los KPI de HU-032 y el informe de precisión del
NLP (HU-033), los escribe en ``out_dir/{run_id}/`` y, si se pide, los publica en S3 (``quality/``) y en BigQuery.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from common.stream_tables import DQ_KPIS, DQ_LAYERS, DQ_RESULTS_COLUMNS
from dq import frames as F
from dq.catalog import TABLE_NAMES, Spec, curated_specs, layer_specs
from dq.nlp import precision_report, render_md
from quality.rules import NLP_AGREEMENT_MIN

LAYERS = ("raw", "trusted", "curated")
#: KPI de HU-032 (§5.10): regla de cada capa → (kpi, objetivo, sentido).
KPI_RULES = {
    "raw": {"DQ-05": "completitud", "DQ-14": "unicidad", "DQ-15": "duplicados", "DQ-02": "anonimizacion"},
    "trusted": {"DQ-05": "completitud", "DQ-49": "unicidad", "DQ-48": "duplicados", "DQ-02": "anonimizacion"},
    "curated": {"DQ-67": "completitud", "DQ-66": "unicidad", "DQ-65": "anonimizacion"},
}
KPI_TARGETS = {"completitud": (0.95, "≥"), "unicidad": (0.99, "≥"), "duplicados": (0.01, "<"), "anonimizacion": (1.0, "=")}
S3_PREFIX = "quality/"
LATEST_SUMMARY = "quality/_latest/ge/summary.json"
DQ_TABLE = "ta_stream.dq_results"
PROJECT = Path(__file__).resolve().parents[2]


def new_run_id(now: datetime) -> str:
    return f"dq-{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def dq_row(run_id: str, run_ts: str, spec: Spec, result: dict | None, *, origin: str | None, suite: str,
           metrics: pd.DataFrame | None = None) -> dict:
    """Una fila de ``dq_results`` (§5.5) para una expectativa: sin datos de filas en ``details``."""
    r = (result or {}).get("result") or {}
    values = None
    if spec.kind == "metric" and metrics is not None:
        sub = metrics[metrics["rule"] == spec.rule]
        values = {str(k): _num(v) for k, v in zip(sub["key"], sub["value"])}
    if values and len(values) == 1:
        observed = next(iter(values.values()))
    elif _num(r.get("observed_value")) is not None:
        observed = _num(r.get("observed_value"))
    elif r.get("element_count") and _num(r.get("unexpected_percent")) is not None:
        observed = 1 - _num(r.get("unexpected_percent")) / 100
    else:
        observed = None
    if spec.layer == "curated":
        table = spec.table if spec.table != "metrics" else (spec.label.split(".")[0] if "." in spec.label else "")
        table_name = f"ta_curated.{table}" if table and table != "*" else "ta_curated"
        check = f"{spec.rule}[{spec.label}]"
    else:
        table_name = TABLE_NAMES[spec.layer]
        check = f"{spec.rule}[origin={origin}]"
    details = {"rule": spec.rule, "origin": origin, "ge": spec.ge, "kind": spec.kind, "severity": spec.severity,
               "label": spec.label, "element_count": r.get("element_count"), "unexpected_count": r.get("unexpected_count"),
               "missing_count": r.get("missing_count")}
    if values and len(values) > 1:
        details["values"] = values
    if result is None:
        details["error"] = "sin resultado de GX"
    return {"run_id": run_id, "run_ts": run_ts, "layer": spec.layer, "table_name": table_name, "suite": suite,
            "check_name": check, "kpi": spec.kpi if spec.kpi in DQ_KPIS else "otro", "observed_value": observed,
            "threshold": spec.threshold, "passed": bool(result and result.get("success")), "details": details}


def _for_origin(specs: list[Spec], origin: str, metrics: pd.DataFrame) -> tuple[list[Spec], list[Spec], list[str]]:
    rows, mets, skipped = [], [], []
    have = set(metrics["rule"]) if len(metrics) else set()
    for s in specs:
        if s.scope not in ("*", origin):
            continue
        if s.kind != "metric":
            rows.append(s)
        elif s.rule in have:
            mets.append(s)
        else:
            skipped.append(f"{s.rule}[origin={origin}]: sin datos para la métrica")
    return rows, mets, skipped


def nlp_rows(run_id: str, run_ts: str, rep: dict) -> list[dict]:
    acc = rep.get("accuracy")
    base = {"run_id": run_id, "run_ts": run_ts, "layer": "curated", "table_name": "ta_curated.fact_posts",
            "suite": "nlp_precision_5_2", "kpi": "precision_nlp"}
    out = [dict(base, check_name="NLP[accuracy]", observed_value=acc, threshold=NLP_AGREEMENT_MIN,
                passed=bool(acc is not None and acc >= NLP_AGREEMENT_MIN),
                details={"evaluated": rep["evaluated"], "without_label": rep["without_label"], "confusion": rep["confusion"]})]
    out.append(dict(base, check_name="NLP[macro_f1]", observed_value=rep.get("macro_f1"), threshold=None, passed=True,
                    details={"per_class": rep["per_class"], "informativo": True}))
    return out


def summarize(run_id: str, rows: list[dict], *, started: float, extra: dict) -> dict:
    errors = [r["check_name"] + " " + r["table_name"] for r in rows if not r["passed"] and r["details"].get("severity") == "error"]
    warnings = [r["check_name"] + " " + r["table_name"] for r in rows if not r["passed"] and r["details"].get("severity") != "error"]
    kpis = []
    for r in rows:
        kpi = KPI_RULES.get(r["layer"], {}).get(r["details"].get("rule"))
        if kpi:
            target, sense = KPI_TARGETS[kpi]
            obs = r["observed_value"]
            ok = obs is not None and ((obs >= target) if sense == "≥" else (obs < target) if sense == "<" else obs >= target)
            kpis.append({"layer": r["layer"], "origin": r["details"].get("origin"), "kpi": kpi, "rule": r["details"]["rule"],
                         "observed": obs, "target": f"{sense} {target:g}", "passed": bool(ok)})
    suites: dict[str, dict] = {}
    for r in rows:
        s = suites.setdefault(r["suite"], {"checks": 0, "passed": 0, "failed_error": 0, "failed_warning": 0})
        s["checks"] += 1
        s["passed"] += 1 if r["passed"] else 0
        if not r["passed"]:
            s["failed_error" if r["details"].get("severity") == "error" else "failed_warning"] += 1
    return {"run_id": run_id, "rows": len(rows), "passed": sum(1 for r in rows if r["passed"]), "errors_failed": errors,
            "warnings_failed": warnings, "ok": not errors, "kpis": kpis, "suites": suites,
            "duration_s": round(time.monotonic() - started, 1), **extra}


def run(root: Path, out_dir: Path, *, layers: tuple[str, ...] = LAYERS, now: datetime | None = None,
        log: Callable[[str], None] = print) -> dict:
    """Valida y escribe ``out_dir/{run_id}/``; devuelve el resumen (``ok`` = ninguna regla de gravedad ``error`` falló)."""
    from dq.suites import Validator

    started = time.monotonic()
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_id, run_ts = new_run_id(now), now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    v = Validator()
    rows: list[dict] = []
    ge_results: dict[str, dict] = {}
    skipped: list[str] = []
    inputs: dict[str, Any] = {}

    def suite(name, df, specs, origin, metrics=None, value_sets=None, dq_suite=None):
        if not specs:
            return
        res, pairs = v.validate(name, df, specs, value_sets)
        ge_results[name] = res
        for spec, r in pairs:
            rows.append(dq_row(run_id, run_ts, spec, r, origin=origin, suite=dq_suite or name, metrics=metrics))

    raw_distinct = None
    trusted_all = None
    for layer in ("raw", "trusted"):
        needed = layer in layers or (layer == "trusted" and "curated" in layers)
        if not needed:
            continue
        log(f"{layer}: leyendo la copia local y evaluando las comprobaciones por fila…")
        t0 = time.monotonic()
        data = F.load_layer(root, layer, as_of=now)
        inputs[layer] = {"rows": {o: len(df) for o, df in data.frames.items()}, "files": data.files, "skipped": len(data.skipped),
                         "read_errors": data.read_errors[:5], "rows_without_origin": data.rows_without_origin,
                         "load_seconds": round(time.monotonic() - t0, 1)}
        if layer == "raw":
            raw_distinct = F.distinct_event_ids(data)
        if layer == "trusted":
            trusted_all = F.all_rows(data)
        if layer not in layers:
            continue
        specs = layer_specs(layer)
        for origin, df in data.frames.items():
            metrics = F.layer_metrics(data, origin, raw_distinct=raw_distinct if layer == "trusted" else None)
            row_specs, metric_specs, miss = _for_origin(specs, origin, metrics)
            skipped += miss
            suite(f"ge_{layer}_posts__{origin}", df, row_specs, origin, dq_suite=f"ge_{layer}_posts")
            suite(f"ge_{layer}_posts_metrics__{origin}", metrics, metric_specs, origin, metrics=metrics,
                  dq_suite=f"ge_{layer}_posts_metrics")
        log(f"{layer}: {sum(inputs[layer]['rows'].values())} filas validadas")
    nlp = None
    if "curated" in layers:
        log("curated: leyendo las nueve tablas y recalculando agregados y aristas…")
        cur = F.load_curated(root)
        inputs["curated"] = {"rows": {t: len(df) for t, df in cur.tables.items()}, "files": cur.files}
        metrics = F.curated_metrics(cur, trusted_all if trusted_all is not None else pd.DataFrame())
        specs = curated_specs()
        values = F.dimension_values(cur)
        by_table: dict[str, list[Spec]] = {}
        for s in specs:
            by_table.setdefault(s.table, []).append(s)
        have = set(metrics["rule"])
        for table, ss in by_table.items():
            if table == "metrics":
                ok = [s for s in ss if s.rule in have]
                skipped += [f"{s.rule}: sin datos para la métrica" for s in ss if s.rule not in have]
                suite("ge_curated_metrics", metrics, ok, None, metrics=metrics)
            else:
                suite(f"ge_curated_{table}", cur.tables[table], ss, None, value_sets=values)
        nlp = precision_report(cur.tables["fact_posts"])
        rows += nlp_rows(run_id, run_ts, nlp)
    for r in rows:
        assert r["layer"] in DQ_LAYERS, r["layer"]
    summary = summarize(run_id, rows, started=started, extra={
        "run_ts": run_ts, "layers": list(layers), "great_expectations": v.version, "inputs": inputs, "skipped": skipped,
        "nlp": {k: nlp[k] for k in ("evaluated", "accuracy", "macro_f1")} if nlp else None})
    write_outputs(out_dir / run_id, summary, rows, ge_results, nlp, as_of=run_ts)
    return summary


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=1, default=str)


def write_outputs(folder: Path, summary: dict, rows: list[dict], ge_results: dict, nlp: dict | None, *, as_of: str) -> list[Path]:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "ge").mkdir(exist_ok=True)
    paths = []
    for name, res in sorted(ge_results.items()):
        p = folder / "ge" / f"{name}.json"
        p.write_text(_json(res), encoding="utf-8")
        paths.append(p)
    names = [c.name for c in DQ_RESULTS_COLUMNS]
    ordered = [{n: r.get(n) for n in names} for r in rows]  # columnas en el orden de §5.5
    (folder / "dq_results.json").write_text(json.dumps(ordered, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    (folder / "summary.json").write_text(_json(summary), encoding="utf-8")
    paths += [folder / "dq_results.json", folder / "summary.json"]
    if nlp:
        (folder / "nlp_precision.json").write_text(_json(nlp), encoding="utf-8")
        (folder / "nlp_precision.md").write_text(render_md(nlp, run_id=summary["run_id"], as_of=as_of), encoding="utf-8")
        paths += [folder / "nlp_precision.json", folder / "nlp_precision.md"]
    return paths


# --- publicación -----------------------------------------------------------------------------------


def s3_items(folder: Path, run_id: str, run_ts: str) -> list[tuple[str, Path]]:
    """``[(clave, archivo)]``: ``quality/{AAAA-MM-DD}/ge/{run_id}/…`` y el resumen en ``quality/_latest/ge/summary.json``."""
    day = run_ts[:10]
    items = [(f"{S3_PREFIX}{day}/ge/{run_id}/{p.relative_to(folder).as_posix()}", p) for p in sorted(folder.rglob("*")) if p.is_file()]
    items.append((LATEST_SUMMARY, folder / "summary.json"))
    return items


def publish_s3(client, bucket: str, items: list[tuple[str, Path]]) -> int:
    for key, path in items:
        ctype = "text/markdown" if path.suffix == ".md" else "application/json"
        client.put_object(Bucket=bucket, Key=key, Body=path.read_bytes(), ContentType=ctype)
    return len(items)


def bq_rows(rows: list[dict]) -> list[dict]:
    """Filas para un *streaming insert*: ``details`` como texto JSON (como ``verify_trends.py``, §3.16)."""
    return [dict(r, details=json.dumps(r["details"], ensure_ascii=False, sort_keys=True, default=str)) for r in rows]


def publish_bq(client, table: str, rows: list[dict], *, chunk: int = 500) -> list:
    errors = []
    data = bq_rows(rows)
    for i in range(0, len(data), chunk):
        errors += list(client.insert_rows_json(table, data[i:i + chunk]))
    return errors


def default_gcp_project(env: dict | None = None) -> str | None:
    import os

    env = os.environ if env is None else env
    if env.get("TA_GCP_PROJECT"):
        return env["TA_GCP_PROJECT"]
    tfvars = PROJECT / "infra" / "gcp" / "environments" / "dev" / "terraform.tfvars"
    try:
        m = re.search(r'^\s*project_id\s*=\s*"([^"]+)"', tfvars.read_text(encoding="utf-8"), re.M)
    except OSError:
        m = None
    return m.group(1) if m else env.get("GOOGLE_CLOUD_PROJECT")
