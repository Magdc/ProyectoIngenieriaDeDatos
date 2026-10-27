"""Validación automática del Lote 5.2 (HU-032, HU-033): ``docs/validation/validation-5-2.md``.

    python project/scripts/validate_5_2.py <parte> [opciones]      (desde la raíz del repo, en WSL, con ta-streaming)

Partes: ``preflight`` (A), ``venv`` (V: crea o actualiza ``~/.venvs/ta-gx`` con ``uv``), ``tests`` (B), ``sync`` (S: baja
de S3 a la caché lo que falte de Raw, Trusted y Curated; solo lee), ``local`` (L: las suites de Great Expectations sobre la
caché, sin publicar), ``publish`` (P: la misma corrida publicada en ``s3://…/quality/`` y en BigQuery ``dq_results``) y
``status`` (solo lectura). El trabajo lo hace ``project/scripts/run_dq.py`` (paquete ``src/dq``) con el venv de GX
(``--gx-python``); este validador corre en ``ta-streaming``, lee sus líneas JSON y comprueba S3 y BigQuery.
Reutiliza de ``ops/validate41.py`` y ``ops/validate42.py`` el contexto, ``s3_keys``, ``read_json_object`` y
``services_status``; de ``ops/validate32.py`` el ejecutor y los pasos.
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

from ops import validate41 as v41
from ops import validate42 as v42
from ops.validate32 import AVISO, FALLA, OK, WSL_HINT, Part, Runner, on_windows, pytest_counts

REPO = v41.REPO
PROJECT = v41.PROJECT
SRC = v41.SRC
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_5_2.json"
NLP_EVIDENCE = EVIDENCE_DIR / "precision_nlp_5_2.md"
RUN_DQ = PROJECT / "scripts" / "run_dq.py"
REQUIREMENTS = SRC / "dq" / "requirements.txt"
DEFAULT_GX_PYTHON = "~/.venvs/ta-gx/bin/python"
DEFAULT_BUCKET = "ta-dev-648617469176-lake"
DQ_TABLE = "ta_stream.dq_results"
LATEST_SUMMARY = "quality/_latest/ge/summary.json"
GX_VERSION = "1.24.1"
LAYERS = ("raw", "trusted", "curated")
#: La única regla que hoy falla (aviso): los 6 centinelas ta-verify@2.1 de Raw stream (§3.20, §5.10).
EXPECTED_WARNINGS = {"DQ-63[origin=stream] ta_raw.posts_raw"}
#: Lo medido el 27-oct-2026 con la copia local (solo AVISO si cambia).
TODAY = {"rows": 239, "passed": 238, "raw_rows": 221637, "trusted_rows": 221629, "fact_rows": 221629, "nlp_evaluated": 109365}

B1_EXPECTED = 13  # src/dq con el venv de GX
B2_EXPECTED = 20  # test_ops_validate52.py
B3_EXPECTED = 3  # src/dq en ta-streaming (la prueba de CI sin GX)
B5_EXPECTED = 165  # src/quality (regresión: el catálogo DQ-01…DQ-63 y quality.checks)

LOT_FILES = [
    "src/dq/__init__.py", "src/dq/catalog.py", "src/dq/frames.py", "src/dq/suites.py", "src/dq/run.py", "src/dq/nlp.py",
    "src/dq/cli.py", "src/dq/conftest.py", "src/dq/requirements.txt", "src/dq/tests/test_dq_gx_suites.py",
    "src/dq/tests/test_dq_identity.py", "scripts/run_dq.py", "src/ops/validate52.py", "src/ops/tests/test_ops_validate52.py",
    "scripts/validate_5_2.py",
]
PREVIOUS_FILES = [
    "src/quality/rules.py", "src/quality/checks.py", "src/quality/layers.py", "src/quality/profiler.py", "src/quality/s3sync.py",
    "src/quality/testing.py", "src/common/stream_tables.py", "src/lakehouse/catalog_spec.py",
    "src/lakehouse/glue_jobs/raw_to_trusted.py", "src/lakehouse/glue_jobs/trusted_to_curated.py",
    "src/ops/validate42.py", "src/ops/validate41.py", "src/ops/validate32.py", "src/ops/services.py",
]
HEX64 = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v42.Ctx):
    evidence_path: Path = EVIDENCE
    bq: Callable[[str, dict], list[dict]] | None = None

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "5.2", "hu": ["HU-032", "HU-033"], "parts": {}}


def gx_python(ctx: Ctx) -> str:
    return str(Path(getattr(ctx.options, "gx_python", None) or DEFAULT_GX_PYTHON).expanduser())


def bucket_of(ctx: Ctx) -> str:
    return getattr(ctx.options, "bucket", None) or DEFAULT_BUCKET


def events(text: str, name: str | None = None) -> list[dict]:
    return [e for e in v41.json_lines([text]) if name is None or e.get("event") == name]


def run_dq(ctx: Ctx, *args: str, timeout: int = 3600):
    cmd = [gx_python(ctx), str(RUN_DQ), *args]
    for opt in ("cache", "bucket", "aws_profile"):
        val = getattr(ctx.options, opt, None)
        if val and args and args[0] in ("sync", "run"):
            cmd += [f"--{opt.replace('_', '-')}", val]
    return ctx.run(cmd, cwd=REPO, timeout=timeout)


def cache_root(ctx: Ctx) -> tuple[Path, Path]:
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from quality.s3sync import bucket_root, resolve_cache

    cache = Path(resolve_cache(getattr(ctx.options, "cache", None)))
    return cache, Path(bucket_root(cache, bucket_of(ctx)))


def gcp_project(ctx: Ctx) -> str | None:
    if getattr(ctx.options, "gcp_project", None):
        return ctx.options.gcp_project
    tfvars = PROJECT / "infra" / "gcp" / "environments" / "dev" / "terraform.tfvars"
    try:
        m = re.search(r'^\s*project_id\s*=\s*"([^"]+)"', tfvars.read_text(encoding="utf-8"), re.M)
    except OSError:
        return None
    return m.group(1) if m else None


def default_bq(project: str) -> Callable[[str, dict], list[dict]]:
    def query(sql: str, params: dict) -> list[dict]:
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        qp = [bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()]
        return [dict(r.items()) for r in client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()]
    return query


def bq_query(ctx: Ctx, sql: str, params: dict) -> list[dict]:
    project = gcp_project(ctx)
    return (ctx.bq or default_bq(project))(sql.replace("{table}", f"`{project}.{DQ_TABLE}`"), params)


COUNT_SQL = """SELECT layer, COUNT(*) AS n, COUNTIF(passed) AS ok, COUNTIF(kpi != 'otro') AS kpi_rows
FROM {table} WHERE run_id = @run_id AND DATE(run_ts) = PARSE_DATE('%Y-%m-%d', @day) GROUP BY layer ORDER BY layer"""
LAST_RUNS_SQL = """SELECT run_id, MIN(run_ts) AS run_ts, COUNT(*) AS n, COUNTIF(passed) AS ok
FROM {table} WHERE STARTS_WITH(run_id, 'dq-') AND DATE(run_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY run_id ORDER BY run_ts DESC LIMIT 5"""


def privacy_problems(folder: Path) -> list[str]:
    """Busca en las salidas cualquier ``user_hash`` (hex de 64) o ``event_id`` (UUID): no deben estar."""
    bad = []
    for p in sorted(folder.rglob("*")):
        if p.is_file():
            text = p.read_text(encoding="utf-8", errors="replace")
            for rx, what in ((HEX64, "hex de 64 (¿user_hash?)"), (UUID, "UUID (¿event_id?)")):
                n = len(rx.findall(text))
                if n:
                    bad.append(f"{p.name}: {n} × {what}")
    return bad


def summarize_done(done: dict) -> str:
    kpis = ", ".join(f"{k['layer']}/{k['origin'] or '*'} {k['kpi']} {k['observed']:.6g}" for k in done.get("kpis") or []
                     if k.get("observed") is not None)
    return (f"{done.get('rows')} comprobaciones, {done.get('passed')} OK · errores {len(done.get('errors_failed') or [])} · avisos "
            f"{len(done.get('warnings_failed') or [])} · {done.get('duration_s')} s · GX {done.get('great_expectations')} · KPI: {kpis}")


# --- A · preflight --------------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código, del venv de GX, de la caché y de las credenciales")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    part.add("A1", "Python de ta-streaming", OK if (py.major, py.minor) == (3, 11) else FALLA, f"Python {py.major}.{py.minor}.{py.micro}")
    absent = [f for f in LOT_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(LOT_FILES)} presentes")
    absent = [f for f in PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A3", "archivos de lotes anteriores que usa", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(PREVIOUS_FILES)} presentes")
    res = ctx.run([gx_python(ctx), "-c", "import json, great_expectations as g, pandas, pyarrow; "
                   "print(json.dumps({'gx': g.__version__, 'pandas': pandas.__version__, 'pyarrow': pyarrow.__version__}))"],
                  cwd=REPO, timeout=300)
    ver = (v41.json_lines([res.out]) or [{}])[-1] if res.ok else {}
    part.add("A4", f"venv de Great Expectations ({gx_python(ctx)})", OK if ver.get("gx") == GX_VERSION else FALLA,
             (f"great_expectations {ver.get('gx')}, pandas {ver.get('pandas')}, pyarrow {ver.get('pyarrow')}"
              + ("" if ver.get("gx") == GX_VERSION else f" (esperado {GX_VERSION}: python project/scripts/validate_5_2.py venv)"))
             if ver else f"no responde: {res.tail(3)} (créelo: python project/scripts/validate_5_2.py venv)", versions=ver)
    try:
        _, root = cache_root(ctx)
        have = {lay: (root / pre).is_dir() for lay, pre in (("raw", "raw"), ("trusted", "trusted/posts"), ("curated", "curated"))}
        missing = [k for k, v in have.items() if not v]
        part.add("A5", "copia local del lake (caché)", AVISO if missing else OK,
                 f"{root}: " + (f"faltan {missing} (python project/scripts/validate_5_2.py sync)" if missing else "raw, trusted y curated"))
    except Exception as exc:  # noqa: BLE001 - CacheError (caché dentro del repo) u otro
        part.add("A5", "copia local del lake (caché)", FALLA, f"{type(exc).__name__}: {exc}")
    tools = {t: ctx.runner.which(t) for t in ("aws", "uv")}
    need = [t for t in tools if not tools[t]]
    part.add("A6", "herramientas", AVISO if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {need}" if need else ""))
    adc = Path("~/.config/gcloud/application_default_credentials.json").expanduser()
    project = gcp_project(ctx)
    part.add("A7", "GCP para dq_results (ADC y proyecto)", OK if adc.exists() and project else AVISO,
             f"proyecto {project or '—'} · ADC {'presente' if adc.exists() else 'falta (gcloud auth application-default login)'}"
             f" · tabla {DQ_TABLE}")
    lines = v41.services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("A8", "lo que cobra (services.status)", AVISO if charging else OK, "\n".join(lines))
    return part.finish()


# --- V · venv ---------------------------------------------------------------------------------------


def part_venv(ctx: Ctx) -> int:
    part = Part(ctx, "V", "Venv de Great Expectations (~/.venvs/ta-gx, uv)")
    py = Path(gx_python(ctx))
    uv = ctx.runner.which("uv") or str(Path("~/.local/bin/uv").expanduser())
    if not py.exists():
        res = ctx.run([uv, "venv", "--python", "3.11", str(py.parent.parent)], cwd=REPO, timeout=600)
        part.add("V1", "crear el venv", OK if res.ok else FALLA, res.tail(3))
        if not res.ok:
            return part.finish()
    else:
        part.add("V1", "crear el venv", OK, f"ya existe: {py}")
    res = ctx.run([uv, "pip", "install", "--python", str(py), "-r", str(REQUIREMENTS)], cwd=REPO, timeout=1800)
    part.add("V2", f"uv pip install -r {REQUIREMENTS.relative_to(PROJECT)}", OK if res.ok else FALLA, res.tail(4))
    return part.finish() if not res.ok else part_preflight_versions(ctx, part)


def part_preflight_versions(ctx: Ctx, part: Part) -> int:
    res = ctx.run([gx_python(ctx), "-c", "import great_expectations as g; print(g.__version__)"], cwd=REPO, timeout=300)
    got = res.out.strip().splitlines()[-1] if res.ok and res.out.strip() else ""
    part.add("V3", "versión de Great Expectations", OK if got == GX_VERSION else FALLA, f"{got or res.tail(3)} (esperado {GX_VERSION})")
    return part.finish()


# --- B · pruebas -------------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    gx = gx_python(ctx)
    plan = [
        ("B1", "pytest src/dq (venv de GX)", [gx, "-m", "pytest", "src/dq", "-q", "-p", "no:cacheprovider", "-W", "ignore"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate52.py", None, B2_EXPECTED),
        ("B3", "pytest src/dq en ta-streaming (prueba de CI de identidad, sin GX)", None, B3_EXPECTED),
        ("B4", "pytest contracts/tests", None, 18),
        ("B5", "regresión: pytest src/quality (catálogo y comprobaciones del 5.1)", None, B5_EXPECTED),
    ]
    args = {"B2": ["src/ops/tests/test_ops_validate52.py", "-q"], "B3": ["src/dq", "-q"], "B4": ["contracts/tests", "-q"],
            "B5": ["src/quality", "-q"]}
    for sid, title, cmd, expected in plan:
        res = ctx.run(cmd, cwd=PROJECT, timeout=1800) if cmd else ctx.pytest(*args[sid], "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- S · sync ----------------------------------------------------------------------------------------


def part_sync(ctx: Ctx) -> int:
    part = Part(ctx, "S", "Copia local de Raw, Trusted y Curated (solo lee S3)")
    for sid, title in (("S1", "sync"), ("S2", "segundo sync (no debe bajar nada)")):
        part.note(f"{sid}: {title}…")
        res = run_dq(ctx, "sync")
        evs = {e.get("layer"): e for e in events(res.out, "dq_sync")}
        if not evs:
            part.add(sid, title, FALLA, res.tail(6))
            return part.finish()
        failed = sum(int(e.get("failed") or 0) for e in evs.values())
        down = sum(int(e.get("downloaded") or 0) for e in evs.values())
        detail = " · ".join(f"{lay}: {e.get('objects')} objetos, {e.get('downloaded')} bajados" for lay, e in evs.items())
        good = res.ok and not failed and set(evs) == set(LAYERS) and (sid == "S1" or down == 0)
        part.add(sid, title, OK if good else (AVISO if sid == "S2" and not failed else FALLA), detail, sync=evs)
    return part.finish()


# --- L · local y P · publish ---------------------------------------------------------------------------


def _check_done(ctx: Ctx, part: Part, prefix: str, res) -> dict | None:
    done = (events(res.out, "dq_done") or [None])[-1]
    if not done:
        err = (events(res.out, "dq_error") or [{}])[-1].get("error")
        part.add(f"{prefix}1", "corrida de las suites", FALLA, err or res.tail(8))
        return None
    errors = done.get("errors_failed") or []
    part.add(f"{prefix}1", "corrida de las suites (Great Expectations)", FALLA if errors else OK,
             summarize_done(done) + ("\nerrores: " + "; ".join(errors[:10]) if errors else ""),
             **{k: done.get(k) for k in ("run_id", "rows", "passed", "duration_s", "great_expectations", "suites", "skipped")})
    kpis = done.get("kpis") or []
    bad = [k for k in kpis if not k.get("passed")]
    part.add(f"{prefix}2", "KPI de HU-032 (completitud ≥ 95 %, unicidad ≥ 99 %, duplicados < 1 % en Trusted, anonimización 100 %)",
             FALLA if bad or not kpis else OK,
             "; ".join(f"{k['layer']}/{k['origin'] or '*'} {k['kpi']} {k['observed']} ({k['target']})" for k in (bad or kpis)),
             kpis=kpis)
    warnings = set(done.get("warnings_failed") or [])
    extra = sorted(warnings - EXPECTED_WARNINGS - {w for w in warnings if w.startswith("NLP[")})
    part.add(f"{prefix}3", "avisos (warning) registrados", AVISO if extra else OK,
             (f"{len(warnings)} avisos: " + "; ".join(sorted(warnings))) if warnings else "ningún aviso",
             warnings=sorted(warnings), unexpected=extra)
    return done


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Suites de Great Expectations sobre la copia local (sin publicar)")
    part.note("L1: leyendo Raw, Trusted y Curated y validando (≈ 1–2 min, ~2,5 GB de memoria)…")
    res = run_dq(ctx, "run")
    done = _check_done(ctx, part, "L", res)
    if not done:
        return part.finish()
    folder = Path(done.get("folder") or "")
    problems = privacy_problems(folder) if folder.is_dir() else [f"no existe {folder}"]
    part.add("L4", "salidas sin user_hash ni event_id", FALLA if problems else OK,
             "\n".join(problems[:10]) if problems else f"{sum(1 for p in folder.rglob('*') if p.is_file())} archivos en {folder} sin hex de 64 "
             "ni UUID")
    nlp = done.get("nlp") or {}
    md = folder / "nlp_precision.md"
    if md.exists():
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        NLP_EVIDENCE.write_text(md.read_text(encoding="utf-8"), encoding="utf-8")
    acc = nlp.get("accuracy")
    where = NLP_EVIDENCE.relative_to(REPO) if NLP_EVIDENCE.is_relative_to(REPO) else NLP_EVIDENCE
    part.add("L5", "precisión del NLP (HU-033)", OK if acc is not None else FALLA,
             f"{nlp.get('evaluated')} filas evaluadas · accuracy {acc:.4f} · F1 macro {nlp.get('macro_f1'):.4f} · informe en "
             f"{where}" if acc is not None else "sin informe", nlp=nlp)
    diffs = []
    rows = (done.get("inputs") or {})
    seen = {"rows": done.get("rows"), "passed": done.get("passed"), "nlp_evaluated": nlp.get("evaluated"),
            "raw_rows": sum((rows.get("raw") or {}).get("rows", {}).values()) or None,
            "trusted_rows": sum((rows.get("trusted") or {}).get("rows", {}).values()) or None,
            "fact_rows": ((rows.get("curated") or {}).get("rows") or {}).get("fact_posts")}
    diffs = [f"{k} {seen[k]} (el 27-oct: {TODAY[k]})" for k in TODAY if seen.get(k) is not None and seen[k] != TODAY[k]]
    part.add("L6", "cifras frente a las del 27-oct", AVISO if diffs else OK, "; ".join(diffs) if diffs else
             f"{seen['raw_rows']} filas de Raw, {seen['trusted_rows']} de Trusted, {seen['fact_rows']} de fact_posts", seen=seen)
    return part.finish()


def part_publish(ctx: Ctx) -> int:
    part = Part(ctx, "P", "Corrida publicada: s3://…/quality/ y BigQuery dq_results")
    project = gcp_project(ctx)
    if not project:
        part.add("P0", "proyecto de GCP", FALLA, "sin project_id en infra/gcp/environments/dev/terraform.tfvars (o --gcp-project)")
        return part.finish()
    if not ctx.ask(f"¿Correr las suites y publicar en s3://{bucket_of(ctx)}/quality/ (~20 PUT) y en {project}.{DQ_TABLE} "
                   "(~240 filas, streaming insert; centavos)?"):
        part.add("P1", "corrida publicada", AVISO, "no confirmado")
        return part.finish()
    res = run_dq(ctx, "run", "--publish", "s3,bq", "--gcp-project", project)
    done = _check_done(ctx, part, "P", res)
    if not done:
        return part.finish()
    pubs = {e.get("target"): e for e in events(res.out, "dq_published")}
    s3, bq = pubs.get("s3") or {}, pubs.get("bigquery") or {}
    run_id = done.get("run_id")
    m = re.match(r"dq-(\d{4})(\d{2})(\d{2})T", run_id or "")  # el día sale del run_id: dq-AAAAMMDDThhmmssZ-…
    day = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""
    prefix = f"quality/{day}/ge/{run_id}/"
    keys = v41.s3_keys(ctx, bucket_of(ctx), prefix) or []
    latest = v42.read_json_object(ctx, bucket_of(ctx), LATEST_SUMMARY) or {}
    good4 = bool(s3) and len(keys) == int(s3.get("objects") or 0) - 1 and latest.get("run_id") == run_id
    part.add("P4", "S3: resultados de GX, dq_results.json, resumen e informe del NLP", OK if good4 else FALLA,
             f"s3://{bucket_of(ctx)}/{prefix}: {len(keys)} objetos ({s3.get('objects')} subidos con el resumen de _latest) · "
             f"{LATEST_SUMMARY} → {latest.get('run_id')}", objects=len(keys))
    try:
        got = bq_query(ctx, COUNT_SQL, {"run_id": run_id, "day": day})
    except Exception as exc:  # noqa: BLE001
        got, err = [], f"{type(exc).__name__}: {str(exc)[:200]}"
    else:
        err = ""
    total = sum(int(r.get("n") or 0) for r in got)
    good5 = not bq.get("errors") and total == int(bq.get("rows") or -1) == int(done.get("rows") or -2)
    part.add("P5", f"BigQuery {DQ_TABLE}: filas de la corrida", OK if good5 else FALLA,
             (f"insertadas {bq.get('rows')} (errores {len(bq.get('errors') or [])}); consultadas {total}: "
              + "; ".join(f"{r.get('layer')} {r.get('n')} ({r.get('ok')} OK, {r.get('kpi_rows')} con KPI)" for r in got)) if not err else err,
             by_layer=got)
    part.add("P6", "costo", OK, f"S3 ≈ {len(keys) + 1} PUT ≈ {(len(keys) + 1) * 0.005 / 1000:.5f} USD · BigQuery streaming insert de "
             f"{bq.get('rows')} filas (~0,1 MB) ≈ 0,000005 USD y una consulta de < 10 MB (primer TB del mes gratis) · lectura de S3 "
             "solo en sync")
    return part.finish()


# --- status -------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Última corrida de calidad (solo lectura)")
    latest = v42.read_json_object(ctx, bucket_of(ctx), LATEST_SUMMARY) or {}
    part.add("S1", f"s3://…/{LATEST_SUMMARY}", OK if latest.get("ok") else (AVISO if latest else AVISO),
             (f"{latest.get('run_id')} · {latest.get('rows')} comprobaciones, {latest.get('passed')} OK · errores "
              f"{len(latest.get('errors_failed') or [])} · avisos {latest.get('warnings_failed')}") if latest else
             "sin corridas publicadas (python project/scripts/validate_5_2.py publish)", run_id=latest.get("run_id"))
    try:
        runs = bq_query(ctx, LAST_RUNS_SQL, {})
        part.add("S2", f"BigQuery {DQ_TABLE}: últimas corridas del 5.2", OK if runs else AVISO,
                 "; ".join(f"{r.get('run_id')} {r.get('n')} filas ({r.get('ok')} OK)" for r in runs) or "ninguna", runs=runs)
    except Exception as exc:  # noqa: BLE001
        part.add("S2", f"BigQuery {DQ_TABLE}", AVISO, f"{type(exc).__name__}: {str(exc)[:200]}")
    return part.finish()


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight, "venv": part_venv, "tests": part_tests, "sync": part_sync, "local": part_local,
    "publish": part_publish, "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="validate_5_2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("part", choices=list(PARTS))
    p.add_argument("--yes", action="store_true", help="no pedir confirmación")
    p.add_argument("--project", default=None, help="proyecto de GCP (solo para services.status)")
    p.add_argument("--gcp-project", default=None, help="proyecto de GCP de dq_results (si no, project_id de infra/gcp)")
    p.add_argument("--aws-profile", default=None)
    p.add_argument("--bucket", default=None, help=f"bucket del lake (si no, {DEFAULT_BUCKET})")
    p.add_argument("--cache", default=None, help="caché (TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
    p.add_argument("--gx-python", default=None, help=f"Python del venv de GX (si no, {DEFAULT_GX_PYTHON})")
    return p


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None,
         bq: Callable[[str, dict], list[dict]] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm, sleep=sleep or time.sleep,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE, bq=bq)
    printer(f"Lote 5.2 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    return PARTS[args.part](ctx)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
