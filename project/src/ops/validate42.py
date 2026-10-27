"""Validación automática del Lote 4.2 (HU-026): ``docs/validation/validation-4-2.md``.

    python project/scripts/validate_4_2.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: el job contra la caché local de Raw del 5.1, sin
nube), ``terraform`` (C: script y capacidad del job; ``--apply``), ``run`` (D: una corrida real del job y
la conciliación Raw → Trusted), ``athena`` (E: ``ta_trusted.posts`` en Athena, duplicados < 1 % y reglas
de Trusted), ``rerun`` (F: segunda corrida, dentro del workflow, que demuestra la idempotencia, y el
costo); además ``status`` (solo lectura) y ``off`` (estado de ahorro, el mismo del 4.1). Cada parte
imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su resumen en
``project/evidencia/validacion_4_2.json``.

Reutiliza de ``ops/validate41.py`` el contexto, el horario, las corridas del workflow, Athena, los logs, el
conteo de filas de S3 y el apagado; de ``ops/validate32.py`` el ejecutor, los pasos y el plan de Terraform
leído en JSON; y de ``ops/services.py`` (vía ``validate41.services_status``) el estado de lo que cobra.
Lo que cambia la nube (``terraform apply``, correr el job o el workflow) pide confirmación salvo con
``--yes``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ops import validate41 as v41
from ops.validate32 import (
    AVISO,
    FALLA,
    OK,
    OMITIDO,
    WSL_HINT,
    Part,
    Result,
    Runner,
    diff_text,
    on_windows,
    parse_json,
    plan_line,
    pytest_counts,
    tf_apply,
    tf_plan,
    tfvars_value,
)

REPO = v41.REPO
PROJECT = v41.PROJECT
SRC = v41.SRC
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_4_2.json"
AWS_ENV = v41.AWS_ENV
GLUE_JOBS_DIR = v41.GLUE_JOBS_DIR
JOB_FILE = GLUE_JOBS_DIR / "raw_to_trusted.py"

AWS_REGION = v41.AWS_REGION
WORKFLOW = v41.WORKFLOW
JOB_KEY = "raw_to_trusted"
JOB = v41.JOBS[JOB_KEY]
NEXT_JOB = v41.JOBS["trusted_to_curated"]
SCHEDULE = v41.SCHEDULE
GLUE_OUTPUT_LOG = v41.GLUE_OUTPUT_LOG
GLUE_ERROR_LOG = v41.GLUE_ERROR_LOG
TRUSTED_PREFIX = "trusted/posts/"
PLACEHOLDER_PREFIX = "trusted/_ta_placeholder/"
LATEST_REPORT = "quality/_latest/raw_to_trusted.json"
SOURCES = ("reddit", "facebook", "instagram")
TRUSTED_KEY = re.compile(r"^trusted/posts/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/(?P<file>[^/]+)$")
PART_FILE = "part-00000.parquet"
PROJECTION_START = "2026-01-01"
NLP_SINCE = "2026-10-22T23:00:00Z"

# Capacidad del job (lakehouse.tf, ADR-25) y lo que cambia el plan del lote.
EXPECTED_CAPACITY = 1.0
TERRAFORM_TARGETS = ["module.glue_workflow"]
EXPECTED_UPDATES = {
    f'module.glue_workflow.aws_s3_object.script["{JOB_KEY}"]',
    f'module.glue_workflow.aws_glue_job.this["{JOB_KEY}"]',
}

# Lo medido con el Raw del 26-oct-2026 (exportador en pausa): si Raw cambia, la conciliación manda y
# estas cifras solo dan AVISO.
TODAY = {"raw_rows": 221637, "raw_stream": 171637, "raw_historical": 50000, "sentinels": 6, "duplicates": 2,
         "rows_written": 221629, "nlp_missing": 65263, "partitions": 543, "raw_distinct": 221635}

# Pruebas esperadas (B): las cuenta docs/validation/validation-4-2.md.
B1_EXPECTED = 90
B2_EXPECTED = 30
B4_EXPECTED = 48
B5_EXPECTED = 52

# Precios de us-east-1 (oct-2026, a confirmar con la factura).
DPU_USD_PER_HOUR = 0.44
S3_GET_USD = 0.0004 / 1000
S3_PUT_USD = 0.005 / 1000
RUNS_PER_DAY = 24

LOT_FILES = [
    "src/lakehouse/glue_jobs/raw_to_trusted.py",
    "src/lakehouse/tests/test_lake_raw_to_trusted.py",
    "src/lakehouse/tests/test_lake_glue_jobs.py",
    "infra/aws/environments/dev/lakehouse.tf",
    "src/ops/validate42.py",
    "src/ops/tests/test_ops_validate42.py",
    "scripts/validate_4_2.py",
]
PREVIOUS_FILES = [
    "infra/aws/modules/glue_workflow/main.tf",
    "infra/aws/modules/glue_role/main.tf",
    "infra/aws/environments/dev/variables_lakehouse.tf",
    "src/lakehouse/catalog_spec.py",
    "src/lakehouse/s3_parquet.py",
    "src/lakehouse/glue_jobs/trusted_to_curated.py",
    "contracts/athena/posts.sql",
    "src/quality/layers.py",
    "src/quality/testing.py",
    "src/quality/s3sync.py",
    "src/ops/validate41.py",
    "src/ops/validate32.py",
    "src/ops/services.py",
]


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE
    s3_get: Callable[[str, str], bytes] | None = None

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "4.2", "hu": ["HU-026"], "parts": {}}


# --- utilidades puras ---------------------------------------------------------------------------


def normalized(data: bytes | str) -> bytes:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return raw.replace(b"\r\n", b"\n")


def layout_problems(keys: list[str]) -> list[str]:
    """Claves de ``trusted/posts/`` que no son ``source={s}/dt={AAAA-MM-DD}/part-00000.parquet``."""
    bad, seen = [], {}
    for key in keys:
        if key.endswith("/"):
            continue
        m = TRUSTED_KEY.match(key)
        if not m:
            bad.append(f"fuera del layout: {key}")
            continue
        if m.group("source") not in SOURCES:
            bad.append(f"source desconocida: {key}")
        if m.group("dt") < PROJECTION_START:
            bad.append(f"dt antes de la proyección: {key}")
        if m.group("file") != PART_FILE:
            bad.append(f"archivo de más: {key}")
        seen[(m.group("source"), m.group("dt"))] = seen.get((m.group("source"), m.group("dt")), 0) + 1
    bad += [f"{n} archivos en source={s}/dt={d}" for (s, d), n in seen.items() if n > 1]
    return bad


def job_cost(execution_s: float | None, dpu: float) -> float:
    return v41.billable_seconds(execution_s) * dpu * DPU_USD_PER_HOUR / 3600


def s3_request_cost(report: dict[str, Any]) -> float:
    objects = int(((report or {}).get("input") or {}).get("objects") or 0)
    puts = int(((report or {}).get("output") or {}).get("objects_written") or 0) + 2
    lists = objects // 1000 + 3
    return objects * S3_GET_USD + (puts + lists) * S3_PUT_USD


def report_summary(report: dict[str, Any]) -> dict[str, Any]:
    """El reporte del job sin las listas por archivo (para la evidencia)."""
    out = {k: v for k, v in (report or {}).items() if k != "output"}
    out["output"] = {k: v for k, v in ((report or {}).get("output") or {}).items()
                     if k not in ("files", "changed_keys", "removed_keys")}
    return out


def files_by_key(report: dict[str, Any]) -> dict[str, str]:
    return {f.get("key"): f.get("sha256") for f in ((report or {}).get("output") or {}).get("files") or []}


def conciliation(report: dict[str, Any]) -> tuple[bool, int, dict[str, int]]:
    c = (report or {}).get("counts") or {}
    rejected = int((c.get("rejected") or {}).get("total") or 0)
    sentinels = int((c.get("sentinels") or {}).get("rows") or 0)
    dups = int((c.get("duplicates") or {}).get("rows_removed") or 0)
    read, written = int(c.get("rows_read") or 0), int(c.get("rows_written") or 0)
    parts = {"rows_read": read, "rejected": rejected, "sentinels": sentinels, "duplicates": dups, "rows_written": written}
    return read == written + rejected + sentinels + dups, read - written, parts


QUALITY_SQL = """SELECT
  count_if(generator LIKE 'ta-verify@%') AS centinelas,
  count_if(date_format(event_time, '%Y-%m-%d') <> dt) AS dt_distinto,
  count_if(text_clean IS NULL) AS text_clean_nulo,
  count_if(length(text_clean) > length(text)) AS text_clean_mas_largo,
  count_if(event_id IS NULL OR event_time IS NULL OR text IS NULL OR user_hash IS NULL) AS clave_nula,
  count_if(sentiment_label IS NULL AND sentiment_score IS NULL AND emotion_label IS NULL AND emotion_score IS NULL
           AND nlp_model_version IS NULL) AS sin_nlp,
  count_if(process_time >= from_iso8601_timestamp('{since}') AND (sentiment_label IS NULL OR sentiment_score IS NULL
           OR emotion_label IS NULL OR emotion_score IS NULL OR nlp_model_version IS NULL)) AS sin_nlp_desde
FROM ta_trusted.posts""".format(since=NLP_SINCE)

COUNT_SQL = "SELECT COUNT(*) AS filas, COUNT(DISTINCT event_id) AS event_ids FROM ta_trusted.posts"

BY_SOURCE_SQL = """SELECT source, origin, COUNT(*) AS filas, COUNT(DISTINCT dt) AS dts, MIN(dt) AS desde, MAX(dt) AS hasta
FROM ta_trusted.posts GROUP BY source, origin ORDER BY source, origin"""

RAW_DISTINCT_SQL = "SELECT COUNT(DISTINCT event_id) AS event_ids FROM ta_raw.posts_raw"


# --- piezas de AWS ---------------------------------------------------------------------------------


def default_s3_get(ctx: Ctx) -> Callable[[str, str], bytes]:
    def get(bucket: str, key: str) -> bytes:
        import boto3

        profile = getattr(ctx.options, "aws_profile", None) or os.environ.get("AWS_PROFILE") or None
        client = boto3.Session(profile_name=profile, region_name=AWS_REGION).client("s3")
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()

    return get


def s3_rows(ctx: Ctx, bucket: str, prefix: str) -> dict[str, Any]:
    return (ctx.s3_rows or v41.default_s3_rows(ctx))(bucket, prefix)


def read_json_object(ctx: Ctx, bucket: str, key: str) -> dict[str, Any] | None:
    res = ctx.aws("s3", "cp", f"s3://{bucket}/{key}", "-", timeout=300)
    data = parse_json(res.out) if res.ok else None
    return data if isinstance(data, dict) else None


# (v2.37) ``job`` (por defecto el del 4.2) deja reutilizar estas piezas con otro job (validate43.py).


def get_job(ctx: Ctx, job: str = JOB) -> dict[str, Any]:
    res = ctx.aws("glue", "get-job", "--job-name", job)
    return (parse_json(res.out) or {}).get("Job") or {} if res.ok else {}


def get_job_run(ctx: Ctx, run_id: str, job: str = JOB) -> dict[str, Any]:
    res = ctx.aws("glue", "get-job-run", "--job-name", job, "--run-id", run_id)
    return (parse_json(res.out) or {}).get("JobRun") or {} if res.ok else {}


def wait_job_run(ctx: Ctx, part: Part, run_id: str, timeout_s: float = 2400, job: str = JOB) -> dict[str, Any]:
    waited, jr = 0.0, {}
    while waited <= timeout_s:
        jr = get_job_run(ctx, run_id, job)
        state = jr.get("JobRunState")
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"):
            return jr
        if waited and int(waited) % 120 == 0:
            part.note(f"{run_id}: {state or '?'} ({int(waited)} s)")
        ctx.sleep(15)
        waited += 15
    jr["_timeout"] = True
    return jr


def start_job_run(ctx: Ctx, arguments: dict[str, str], part: Part, job: str = JOB) -> tuple[str | None, str, int]:
    """StartJobRun de ``job`` con el workflow y los jobs libres y reintento ante ConcurrentRunsExceededException."""
    busy = v41.wait_idle(ctx, part)
    if busy:
        return None, f"el workflow sigue con corridas en curso: {', '.join(busy)}", 0
    waited = 0
    while any(v41.running_job_runs(ctx).values()) and waited < v41.IDLE_TIMEOUT_S:
        part.note("esperando a que terminen los job runs en curso…")
        ctx.sleep(15)
        waited += 15
    res = Result(1, "", "")
    for attempt in range(1, v41.CONCURRENT_TRIES + 1):
        res = ctx.aws("glue", "start-job-run", "--job-name", job, "--arguments", json.dumps(arguments))
        run_id = (parse_json(res.out) or {}).get("JobRunId") if res.ok else None
        if run_id:
            return run_id, "", attempt
        if v41.aws_error(res) != "ConcurrentRunsExceededException":
            return None, res.tail(4), attempt
        if attempt < v41.CONCURRENT_TRIES:
            part.note(f"Glue todavía cuenta otra corrida (ConcurrentRunsExceededException); reintento en {v41.CONCURRENT_RETRY_S} s…")
            ctx.sleep(v41.CONCURRENT_RETRY_S)
    return None, f"ConcurrentRunsExceededException tras {v41.CONCURRENT_TRIES} intentos: " + res.tail(2), v41.CONCURRENT_TRIES


def job_done(ctx: Ctx, job_run_id: str, started: datetime, wait_s: float = 180) -> dict[str, Any] | None:
    lines = v41.json_lines(v41.find_log_lines(ctx, GLUE_OUTPUT_LOG, job_run_id, "glue_job_done", started, wait_s=wait_s))
    return next((x for x in lines if x.get("event") == "glue_job_done"), None)


def trusted_listing(ctx: Ctx, bucket: str) -> dict[str, int] | None:
    keys = v41.s3_keys(ctx, bucket, TRUSTED_PREFIX)
    if keys is None:
        return None
    return {k.get("Key"): int(k.get("Size") or 0) for k in keys}


def placeholder_count(ctx: Ctx, bucket: str) -> int | None:
    keys = v41.s3_keys(ctx, bucket, PLACEHOLDER_PREFIX)
    return None if keys is None else len(keys)


def bucket_name(ctx: Ctx) -> str:
    if getattr(ctx.options, "bucket", None):
        return ctx.options.bucket
    return f"ta-dev-{v41.account_id(ctx)}-lake"


def schema_problems(data: bytes) -> tuple[list[str], int]:
    """Un Parquet de Trusted frente al contrato (columnas, orden y tipos de §5.7)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakehouse import catalog_spec
    from quality.testing import trusted_arrow_schema

    table = pq.read_table(pa.BufferReader(data))
    expected = trusted_arrow_schema()
    names = list(catalog_spec.table("ta_trusted.posts").column_names)
    problems = []
    if table.column_names != names:
        problems.append(f"columnas {table.column_names} ≠ contrato {names}")
    for field in expected:
        if field.name in table.column_names and not table.schema.field(field.name).type.equals(field.type):
            problems.append(f"{field.name}: {table.schema.field(field.name).type} (esperado {field.type})")
    return problems, table.num_rows


# --- Parte A · preflight --------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    missing = []
    for mod in ("boto3", "botocore", "pyarrow"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    part.add("A1", "Python y dependencias", FALLA if missing or (py.major, py.minor) != (3, 11) else OK,
             f"Python {py.major}.{py.minor}.{py.micro}" + (f"; faltan: {', '.join(missing)}" if missing else "; boto3, botocore y pyarrow"))
    absent = [f for f in LOT_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(LOT_FILES)} presentes")
    absent = [f for f in PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A3", "archivos de lotes anteriores que usa", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(PREVIOUS_FILES)} presentes")
    try:
        if str(SRC) not in sys.path:
            sys.path.insert(0, str(SRC))
        from lakehouse import catalog_spec

        problems = catalog_spec.check()
    except Exception as exc:  # noqa: BLE001
        problems = [f"{type(exc).__name__}: {exc}"]
    part.add("A4", "tables.json al día con contracts/athena/", FALLA if problems else OK,
             "\n".join(problems) if problems else "ta_trusted.posts y las demás tablas = las DDL")
    tools = {t: ctx.runner.which(t) for t in ("terraform", "aws", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A5", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))

    state, sched = v41.get_schedule(ctx)
    runs = v41.workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    jobs = {k: v for k, v in v41.running_job_runs(ctx).items() if v}
    ok6 = state in ("DISABLED", "NO_EXISTE") and not running and not jobs
    part.add("A6", "AWS: horario, corridas y job runs", OK if ok6 else AVISO,
             f"horario {SCHEDULE}: {state or sched.get('error')}; "
             + (f"corridas en curso: {', '.join(running)}; " if running else "ninguna corrida en curso; ")
             + (f"job runs en curso: {jobs}" if jobs else "ningún job run en curso")
             + (" (apague con python project/scripts/validate_4_2.py off)" if not ok6 else ""),
             schedule=state, running=running, job_runs=jobs)
    lines = v41.services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("A7", "GCP y local (services.status)", AVISO if charging else OK,
             "\n".join(lines) + ("\neste lote no los necesita: python project/scripts/off.py" if charging else ""))
    try:
        if str(SRC) not in sys.path:
            sys.path.insert(0, str(SRC))
        from quality.s3sync import bucket_root, resolve_cache

        root = bucket_root(resolve_cache(getattr(ctx.options, "cache", None)), getattr(ctx.options, "bucket", None) or "ta-dev-648617469176-lake")
        has = (Path(root) / "raw").is_dir()
        detail = f"{root}/raw/ {'presente' if has else 'no existe'}" + ("" if has else
                 " (L la necesita: python project/scripts/profile_data.py sync --layer raw; solo lee S3)")
    except Exception as exc:  # noqa: BLE001
        has, detail = False, f"{type(exc).__name__}: {exc}"
    part.add("A8", "caché local de Raw del 5.1 (para la parte L)", OK if has else AVISO, detail)
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/lakehouse", ["src/lakehouse", "-q", "-rs"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate42.py", ["src/ops/tests/test_ops_validate42.py", "-q"], B2_EXPECTED),
        ("B3", "pytest contracts/tests", ["contracts/tests", "-q"], 18),
        ("B4", "regresión: test_ops_validate41.py (piezas reutilizadas)", ["src/ops/tests/test_ops_validate41.py", "-q"], B4_EXPECTED),
        ("B5", "test_ops_validate24.py (prueba inestable arreglada)", ["src/ops/tests/test_ops_validate24.py", "-q"], B5_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · el job en local contra la caché de Raw ----------------------------------------------


def run_local_job(ctx: Ctx, src: Path, dst: Path, bucket: str) -> tuple[Result, dict[str, Any] | None]:
    res = ctx.run([sys.executable, str(JOB_FILE), "--lake_bucket", bucket, "--ta_local_src", str(src),
                   "--ta_local_dst", str(dst)], cwd=PROJECT, timeout=3600)
    done = next((x for x in v41.json_lines([res.out]) if x.get("event") == "glue_job_done"), None)
    return res, done


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "El job en local contra la caché de Raw (sin nube)")
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from quality.s3sync import bucket_root, resolve_cache

    bucket = getattr(ctx.options, "bucket", None) or "ta-dev-648617469176-lake"
    try:
        cache = Path(resolve_cache(getattr(ctx.options, "cache", None)))
    except Exception as exc:  # noqa: BLE001 - CacheError (caché dentro del repo) u otro
        part.add("L0", "caché local", FALLA, f"{type(exc).__name__}: {exc}")
        return part.finish()
    src = Path(bucket_root(cache, bucket))
    dst = cache / "raw_to_trusted_local"
    if not (src / "raw").is_dir():
        part.add("L0", "caché local de Raw", AVISO, f"no existe {src}/raw/: python project/scripts/profile_data.py sync --layer raw "
                 "(solo lee S3, ~4 min la primera vez) y repita")
        return part.finish()
    part.add("L0", "caché local de Raw", OK, f"lee {src}/raw/ · escribe en {dst} (fuera del repo)")
    part.note("L1: el job lee los ~13.300 Parquet de la caché y escribe Trusted (≈ 25 s y ~0,8 GB de memoria)…")
    res, done = run_local_job(ctx, src, dst, bucket)
    if not res.ok or not done:
        part.add("L1", "corrida local", FALLA, res.tail(8))
        return part.finish()
    matches = done.get("rows_written") == TODAY["rows_written"] and done.get("sentinels") == TODAY["sentinels"] \
        and done.get("duplicates_removed") == TODAY["duplicates"]
    part.add("L1", "corrida local (conteos)", (OK if matches else AVISO) if done.get("conciliation_ok") else FALLA,
             f"leídas {done.get('rows_read')} · centinelas {done.get('sentinels')} · duplicadas {done.get('duplicates_removed')} · "
             f"sin NLP {done.get('nlp_missing')} · escritas {done.get('rows_written')} en {done.get('partitions')} particiones · "
             f"{done.get('duration_s')} s · pico {done.get('peak_rss_mb')} MB"
             + ("" if matches else f" (con el Raw del 26-oct: {TODAY['rows_written']} escritas; ¿cambió la caché?)"),
             done=done)
    res2, done2 = run_local_job(ctx, src, dst, bucket)
    idem = bool(res2.ok and done2 and done2.get("partitions_changed") == 0 and done2.get("objects_deleted") == 0
                and done2.get("rows_written") == done.get("rows_written"))
    part.add("L2", "segunda corrida local (idempotencia)", OK if idem else FALLA,
             f"particiones cambiadas {done2.get('partitions_changed') if done2 else '?'} · borradas "
             f"{done2.get('objects_deleted') if done2 else '?'} · escritas {done2.get('rows_written') if done2 else '?'}"
             + ("" if done2 else "\n" + res2.tail(6)), done=done2)
    if getattr(ctx.options, "profile", False):
        part.note("L3: profiling de la salida con el profiler del 5.1 (~4–5 min)…")
        out = cache / "raw_to_trusted_local_profile.json"
        raw_profile = PROJECT / "evidencia" / "profiling_raw_5_1.json"
        args = [sys.executable, str(PROJECT / "scripts" / "profile_data.py"), "profile", "--layer", "trusted", "--root", str(dst),
                "--bucket", bucket, "--json-out", str(out), "--quiet"]
        if raw_profile.exists():
            args += ["--raw-profile", str(raw_profile)]
        r = ctx.run(args, cwd=REPO, timeout=3600)
        try:
            prof = json.loads(out.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            prof = {}
        summary = prof.get("summary") or {}
        rules = summary.get("rules") or {}
        errors = (summary.get("anomalies") or {}).get("error")
        good = bool(prof) and not errors and not rules.get("no_cumple")
        part.add("L3", "profiling del Trusted local (5.1)", OK if good else FALLA,
                 f"reglas {rules} · anomalías {summary.get('anomalies')}" if prof else r.tail(6), summary=summary)
    else:
        part.add("L3", "profiling del Trusted local (5.1)", OMITIDO, "sin --profile")
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (script y capacidad de ta-job-raw-to-trusted)")
    apply = bool(getattr(ctx.options, "apply", False))
    res = ctx.aws("sts", "get-caller-identity")
    ident = parse_json(res.out) or {}
    if not res.ok:
        part.add("C0", "identidad de AWS", FALLA, "aws sts get-caller-identity falló: " + res.tail(3))
        return part.finish()
    tf_account = tfvars_value(AWS_ENV / "terraform.tfvars", "aws_account_id")
    if tf_account and ident.get("Account") != tf_account:
        part.add("C0", "identidad de AWS", FALLA, f"el perfil apunta a {ident.get('Account')} y terraform.tfvars a {tf_account}")
        return part.finish()
    arn = ident.get("Arn", "")
    part.add("C0", "identidad de AWS", AVISO if arn.endswith(":root") else OK,
             f"{arn} (cuenta raíz: sirve para aplicar; el job corre con ta-glue-role)" if arn.endswith(":root") else arn)
    missing = [n for n in ("backend.hcl", "terraform.tfvars") if not (AWS_ENV / n).exists()]
    part.add("C0b", "backend.hcl y terraform.tfvars de infra/aws", FALLA if missing else OK,
             "faltan: " + ", ".join(missing) if missing else "presentes (no se versionan)")
    if missing:
        return part.finish()
    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()
    res = ctx.tf(AWS_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
    part.add("C2a", "terraform init", OK if res.ok else FALLA, res.tail(3))
    if not res.ok:
        return part.finish()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / "tfplan_4_2"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", "plan (-target=module.glue_workflow)", FALLA, res.err or res.tail(8))
        return part.finish()
    updates = set(summary["update"])
    extra = (updates - EXPECTED_UPDATES) | set(summary["create"] + summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", "plan (-target=module.glue_workflow)", FALLA,
                 f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    diffs = summary.get("diffs") or {}
    job_addr = f'module.glue_workflow.aws_glue_job.this["{JOB_KEY}"]'
    job_diff = diffs.get(job_addr, [])
    bad_job = [d for d in job_diff if not d.startswith(("max_capacity", "description"))]
    if bad_job:
        part.add("C2", "plan (-target=module.glue_workflow)", FALLA,
                 f"el job cambia algo más que max_capacity/description: {bad_job}", **summary)
        return part.finish()
    part.add("C2", "plan (-target=module.glue_workflow)", OK,
             plan_line(summary) + (" (ya aplicado)" if not updates else
                                   " (esperado: el script y el job de raw_to_trusted)") + diff_text(summary, updates), **summary)
    applied = not updates
    if updates:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
        elif ctx.ask(f"¿Aplicar el plan de infra/aws (0 a crear, {len(updates)} a cambiar, 0 a destruir)?"):
            res = tf_apply(ctx, AWS_ENV, plan_path)
            applied = res.ok and f"Apply complete! Resources: 0 added, {len(updates)} changed, 0 destroyed." in res.out
            part.add("C3", "apply", OK if applied else FALLA, res.tail(4) if res.ok else res.err)
            if not res.ok:
                return part.finish()
        else:
            part.add("C3", "apply", AVISO, "no confirmado")
    else:
        part.add("C3", "apply", OK, "nada que aplicar")
    if applied:
        res, again = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, EVIDENCE_DIR / "tfplan_4_2_check")
        if again is None:
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA, res.err or res.tail(8))
        else:
            changes = again["create"] + again["update"] + again["delete"] + again["replace"]
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA if changes else OK,
                     (f"{plan_line(again)}: {sorted(changes)}" + diff_text(again)) if changes else "No changes", **again)
    else:
        part.add("C3b", "plan tras aplicar (sin deriva)", OMITIDO, "pendiente del apply")
    return part.finish()


# --- Parte D · corrida real y conciliación ---------------------------------------------------------


def check_job(ctx: Ctx, bucket: str) -> list[str]:
    job = get_job(ctx)
    if not job:
        return [f"{JOB} no existe (aplique el 4.1 y C)"]
    problems = []
    cmd = job.get("Command") or {}
    args = job.get("DefaultArguments") or {}
    if cmd.get("Name") != "pythonshell" or str(cmd.get("PythonVersion")) != "3.9":
        problems.append(f"comando {cmd.get('Name')} {cmd.get('PythonVersion')}")
    if cmd.get("ScriptLocation") != f"s3://{bucket}/_glue/scripts/{JOB}.py":
        problems.append(f"script en {cmd.get('ScriptLocation')}")
    if float(job.get("MaxCapacity") or 0) != EXPECTED_CAPACITY:
        problems.append(f"MaxCapacity {job.get('MaxCapacity')} (esperado {EXPECTED_CAPACITY}: aplique C)")
    if job.get("MaxRetries") != 0:
        problems.append(f"MaxRetries {job.get('MaxRetries')}")
    for k, want in (("--lake_bucket", bucket), ("--ta_input_prefix", "raw/"), ("--ta_output_prefix", "trusted/"),
                    ("library-set", "analytics")):
        if args.get(k) != want:
            problems.append(f"{k} = {args.get(k)!r} (esperado {want!r})")
    deployed = ctx.aws("s3", "cp", f"s3://{bucket}/_glue/scripts/{JOB}.py", "-")
    if not deployed.ok or hashlib.sha256(normalized(deployed.out)).hexdigest() != hashlib.sha256(normalized(JOB_FILE.read_bytes())).hexdigest():
        problems.append(f"el script de S3 no es src/lakehouse/glue_jobs/{JOB_KEY}.py (aplique C)")
    return problems


def part_run(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Corrida real de ta-job-raw-to-trusted y conciliación Raw → Trusted")
    bucket = bucket_name(ctx)
    problems = check_job(ctx, bucket)
    part.add("D1", f"{JOB}: Python shell 3.9, {EXPECTED_CAPACITY:g} DPU, script de S3 = repo", FALLA if problems else OK,
             "\n".join(problems) if problems else f"pythonshell 3.9 · {EXPECTED_CAPACITY:g} DPU (16 GB) · library-set analytics · "
             "script de S3 idéntico al repo")
    if problems:
        return part.finish()

    part.note("D2: contando las filas de Raw por los pies de sus Parquet (≈ 13.300 GET; 1–2 min)…")
    raw = {o: s3_rows(ctx, bucket, f"raw/origin={o}/") for o in ("stream", "historical")}
    raw_total = sum(r.get("rows", 0) for r in raw.values())
    raw_errors = sum(r.get("error_count", 0) for r in raw.values())
    placeholders_before = placeholder_count(ctx, bucket)
    part.add("D2", "Raw antes de la corrida", FALLA if raw_errors else OK,
             f"stream {raw['stream'].get('objects')} objetos y {raw['stream'].get('rows')} filas · histórico "
             f"{raw['historical'].get('objects')} objetos y {raw['historical'].get('rows')} filas · total {raw_total}"
             + ("" if raw_total == TODAY["raw_rows"] else f" (el 26-oct eran {TODAY['raw_rows']}: ¿corrió el exportador?)")
             + (f" · {raw_errors} ilegibles" if raw_errors else ""),
             raw={o: {k: r.get(k) for k in ("objects", "rows", "by_source", "error_count", "seconds")} for o, r in raw.items()},
             placeholders_before=placeholders_before)

    if not ctx.ask(f"¿Correr {JOB} una vez ({EXPECTED_CAPACITY:g} DPU, ≈ 0,01 USD; reescribe trusted/posts/)?"):
        part.add("D3", "corrida del job", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = start_job_run(ctx, {"--ta_validacion": "4.2"}, part)
    if not run_id:
        part.add("D3", "corrida del job", FALLA, err)
        return part.finish()
    part.note(f"{run_id}: esperando a que termine (2–5 min, la mayor parte es el arranque de Glue)…")
    jr = wait_job_run(ctx, part, run_id)
    good = jr.get("JobRunState") == "SUCCEEDED"
    part.add("D3", "corrida del job", OK if good else FALLA,
             f"{run_id}: {jr.get('JobRunState')}{' (tiempo agotado)' if jr.get('_timeout') else ''} · {jr.get('ExecutionTime')} s de "
             f"ejecución · {jr.get('MaxCapacity')} DPU" + (f" · arrancó al intento {tries}" if tries > 1 else "")
             + (f"\n{jr.get('ErrorMessage')}" if jr.get("ErrorMessage") else "")
             + ("" if good else f"\ntraceback en {GLUE_ERROR_LOG} (flujo {run_id})"),
             job_run_id=run_id, execution_s=jr.get("ExecutionTime"), max_capacity=jr.get("MaxCapacity"), state=jr.get("JobRunState"))
    if not good:
        return part.finish()

    done = job_done(ctx, run_id, started)
    env = (done or {}).get("environment") or {}
    part.add("D4", f"línea glue_job_done en {GLUE_OUTPUT_LOG}", OK if done else FALLA,
             (f"leídas {done.get('rows_read')} · escritas {done.get('rows_written')} · {done.get('partitions')} particiones · "
              f"lectura {done.get('read_seconds')} s · total {done.get('duration_s')} s · pico de memoria {done.get('peak_rss_mb')} MB · "
              f"Python {env.get('python')}, pyarrow {env.get('pyarrow')}, boto3 {env.get('boto3')}") if done else
             f"sin glue_job_done en el flujo {run_id} (¿tardan los logs?)", done=done)
    report_key = (done or {}).get("report") or LATEST_REPORT
    report = read_json_object(ctx, bucket, report_key)
    latest = read_json_object(ctx, bucket, LATEST_REPORT)
    same = bool(report and latest and report.get("run_id") == latest.get("run_id"))
    part.add("D5", "reporte de la corrida en quality/", OK if report and same else FALLA,
             f"{report_key} y {LATEST_REPORT} (corrida {report.get('run_id')}, esquema {report.get('schema')})" if report else
             f"no se pudo leer {report_key}", report=report_summary(report or {}))
    if not report:
        return part.finish()

    ok_eq, _, parts = conciliation(report)
    c = report.get("counts") or {}
    by_origin = (report.get("input") or {}).get("by_origin") or {}
    raw_match = parts["rows_read"] == raw_total and all(
        int((by_origin.get(o) or {}).get("rows") or 0) == int(raw[o].get("rows") or 0) for o in raw)
    today = (parts["rows_written"] == TODAY["rows_written"] and parts["sentinels"] == TODAY["sentinels"]
             and parts["duplicates"] == TODAY["duplicates"])
    status = FALLA if not (ok_eq and raw_match) else (OK if today or raw_total != TODAY["raw_rows"] else AVISO)
    part.add("D6", "conciliación Raw → Trusted", status,
             f"Raw {raw_total} (pies de S3) = leídas {parts['rows_read']} = escritas {parts['rows_written']} + centinelas "
             f"{parts['sentinels']} + duplicadas {parts['duplicates']} + descartadas {parts['rejected']}"
             + ("" if raw_match else f" · el job leyó {parts['rows_read']} y S3 tiene {raw_total}")
             + f"\nsin NLP conservadas {((c.get('nlp_missing') or {}).get('rows'))} (desde NLP_SINCE: "
             f"{(c.get('nlp_missing') or {}).get('since_nlp_since')}) · copias que difieren en algo más que process_time: "
             f"{(c.get('duplicates') or {}).get('groups_with_other_differences')}"
             + ("" if today or raw_total != TODAY["raw_rows"] else
                f"\ncon el Raw del 26-oct se esperaban {TODAY['rows_written']} escritas, {TODAY['sentinels']} centinelas y "
                f"{TODAY['duplicates']} duplicadas"), **parts)

    part.note("D7: contando las filas de trusted/posts/ por los pies de sus Parquet…")
    trusted = s3_rows(ctx, bucket, TRUSTED_PREFIX)
    listing = trusted_listing(ctx, bucket) or {}
    layout = layout_problems(list(listing))
    out = report.get("output") or {}
    by_src = {s: v.get("rows") for s, v in (out.get("by_source") or {}).items()}
    s3_by_src = {s: v.get("rows") for s, v in (trusted.get("by_source") or {}).items()}
    placeholders_after = placeholder_count(ctx, bucket)
    good7 = (trusted.get("rows") == parts["rows_written"] and trusted.get("objects") == out.get("partitions")
             and by_src == s3_by_src and not layout and not trusted.get("error_count")
             and placeholders_after == placeholders_before)
    part.add("D7", "trusted/posts/ en S3 (filas, layout, sin marcas nuevas)", OK if good7 else FALLA,
             f"{trusted.get('objects')} objetos ({out.get('partitions')} particiones en el reporte), {trusted.get('rows')} filas en los pies "
             f"· por fuente {s3_by_src} · dt {out.get('dt_min')} → {out.get('dt_max')} · {out.get('bytes')} bytes · "
             f"trusted/_ta_placeholder/: {placeholders_before} → {placeholders_after} objetos"
             + (("\n" + "\n".join(layout[:10])) if layout else ""),
             s3={k: trusted.get(k) for k in ("objects", "rows", "by_source", "error_count", "seconds")}, layout=layout[:20])

    smallest = min(listing.items(), key=lambda kv: kv[1])[0] if listing else None
    if smallest:
        try:
            problems, n = schema_problems((ctx.s3_get or default_s3_get(ctx))(bucket, smallest))
        except Exception as exc:  # noqa: BLE001
            problems, n = [f"{type(exc).__name__}: {exc}"], 0
        part.add("D8", "esquema de un Parquet de Trusted = contrato", FALLA if problems else OK,
                 "\n".join(problems) if problems else f"{smallest}: 24 columnas en el orden de contracts/athena/posts.sql, "
                 f"timestamp[ms, UTC], int64, list<string>, text_clean ({n} filas)")
    else:
        part.add("D8", "esquema de un Parquet de Trusted = contrato", FALLA, "trusted/posts/ vacío")
    return part.finish()


# --- Parte E · Athena ---------------------------------------------------------------------------


def latest_report(ctx: Ctx, bucket: str) -> dict[str, Any]:
    return read_json_object(ctx, bucket, LATEST_REPORT) or {}


def first_row(q: dict[str, Any]) -> dict[str, Any]:
    return (q.get("rows") or [{}])[0]


def part_athena(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Athena: ta_trusted.posts")
    bucket = bucket_name(ctx)
    report = latest_report(ctx, bucket)
    written = int(((report.get("counts") or {}).get("rows_written")) or 0)
    cutoff = v41.DEFAULT_CUTOFF
    scanned = 0
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["trusted"])
    scanned += q.get("scanned_bytes", 0)
    if q["state"] != "SUCCEEDED":
        part.add("E1", "COUNT(*) y event_id distintos", FALLA, f"{q['state']}: {q['reason']}")
        return part.finish()
    rows, ids = int(first_row(q).get("filas") or 0), int(first_row(q).get("event_ids") or 0)
    dup = (rows - ids) / rows if rows else 1.0
    part.add("E1", "duplicados < 1 % (KPI de §5.10, DQ-48/DQ-49)", FALLA if not rows or dup >= 0.01 else (OK if dup == 0 else AVISO),
             f"{rows} filas · {ids} event_id distintos · duplicados {dup:.4%} · el reporte del job dice {written} escritas"
             + ("" if rows == written else " (≠: ¿otra corrida en medio?)"), rows=rows, event_ids=ids, duplicates_ratio=dup)

    q = v41.athena_query(ctx, BY_SOURCE_SQL, v41.DATABASES["trusted"])
    scanned += q.get("scanned_bytes", 0)
    out = report.get("output") or {}
    by_src: dict[str, int] = {}
    for r in q.get("rows") or []:
        by_src[r.get("source")] = by_src.get(r.get("source"), 0) + int(r.get("filas") or 0)
    rep_src = {s: v.get("rows") for s, v in (out.get("by_source") or {}).items()}
    good = q["state"] == "SUCCEEDED" and by_src == rep_src and sum(by_src.values()) == rows
    part.add("E2", "por fuente y origen (particiones visibles)", OK if good else FALLA,
             "; ".join(f"{r.get('source')}/{r.get('origin')}: {r.get('filas')} filas en {r.get('dts')} días ({r.get('desde')} → {r.get('hasta')})"
                       for r in q.get("rows") or []) or f"{q['state']}: {q['reason']}", rows=q.get("rows"))

    q = v41.athena_query(ctx, QUALITY_SQL, v41.DATABASES["trusted"])
    scanned += q.get("scanned_bytes", 0)
    r = {k: int(v or 0) for k, v in first_row(q).items()} if q["state"] == "SUCCEEDED" else {}
    nlp_report = int((((report.get("counts") or {}).get("nlp_missing") or {}).get("rows")) or 0)
    zero = ("centinelas", "dt_distinto", "text_clean_nulo", "text_clean_mas_largo", "clave_nula", "sin_nlp_desde")
    bad = [f"{k} = {r.get(k)}" for k in zero if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    if r and r.get("sin_nlp") != nlp_report:
        bad.append(f"sin_nlp = {r.get('sin_nlp')} y el reporte dice {nlp_report}")
    part.add("E3", "reglas de Trusted (DQ-13, DQ-54, DQ-56, DQ-57, DQ-63, claves)", FALLA if bad else OK,
             "\n".join(bad) if bad else f"0 centinelas · 0 dt ≠ DATE(event_time) · 0 text_clean nulos o más largos que text · 0 claves "
             f"nulas · {r.get('sin_nlp')} filas sin NLP conservadas (= reporte; 0 desde {NLP_SINCE})", counts=r)

    part.note("E4: event_id distintos de Raw (DQ-58; la proyección de posts_raw enumera ~43.000 particiones: ~70 s)…")
    q = v41.athena_query(ctx, RAW_DISTINCT_SQL, v41.DATABASES["raw"])
    scanned += q.get("scanned_bytes", 0)
    raw_ids = int(first_row(q).get("event_ids") or 0) if q["state"] == "SUCCEEDED" else 0
    ratio = rows / raw_ids if raw_ids else 0.0
    part.add("E4", "filas de Trusted / event_id distintos de Raw (DQ-58: 0,99–1)", OK if 0.99 <= ratio <= 1 else FALLA,
             f"{rows} / {raw_ids} = {ratio:.5f}" + ("" if q["state"] == "SUCCEEDED" else f" ({q['state']}: {q['reason']})"),
             ratio=ratio, raw_distinct=raw_ids)
    part.add("E5", "bytes escaneados ≤ límite del workgroup", OK if scanned <= 4 * cutoff else FALLA,
             f"{scanned / 1e6:.1f} MB en 4 consultas (≈ {max(scanned, 4e7) / 1e12 * 5:.5f} USD; mínimo 10 MB por consulta)",
             scanned_bytes=scanned)
    return part.finish()


# --- Parte F · segunda corrida (idempotencia) y costo ----------------------------------------------


def part_rerun(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Segunda corrida dentro del workflow: idempotencia y costo")
    bucket = bucket_name(ctx)
    before_report = latest_report(ctx, bucket)
    before = trusted_listing(ctx, bucket)
    if not before_report or not before:
        part.add("F1", "Trusted antes de la segunda corrida", FALLA,
                 "no hay reporte en quality/_latest/ o trusted/posts/ está vacío: corra antes la parte D")
        return part.finish()
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["trusted"])
    rows0 = int(first_row(q).get("filas") or 0)
    part.add("F1", "Trusted antes de la segunda corrida", OK,
             f"{len(before)} objetos · reporte {before_report.get('run_id')} · {rows0} filas en Athena", objects=len(before), rows=rows0)
    state0, _ = v41.get_schedule(ctx)
    if not ctx.ask(f"¿Correr el workflow {WORKFLOW} una vez ({JOB} con {EXPECTED_CAPACITY:g} DPU y el marcador "
                   f"{NEXT_JOB}; ≈ 0,01 USD)?"):
        part.add("F2", "corrida del workflow", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = v41.start_run(ctx, {"ta_validacion": "4.2"}, part)
    if not run_id:
        part.add("F2", "corrida del workflow", FALLA, err)
        return part.finish()
    part.note(f"{run_id}: esperando a los dos jobs (3–6 min)…")
    run = v41.wait_run(ctx, part, run_id)
    jobs = v41.job_runs_by_name(run)
    jr1 = (jobs.get(JOB) or [{}])[0]
    jr2 = (jobs.get(NEXT_JOB) or [{}])[0]
    ok1 = jr1.get("JobRunState") == "SUCCEEDED"
    ok2 = jr2.get("JobRunState") == "SUCCEEDED"
    part.add("F2", "corrida del workflow", FALLA if not ok1 else (OK if ok2 else AVISO),
             f"{run_id}: {run.get('Status')} · {JOB} {jr1.get('JobRunState')} ({jr1.get('ExecutionTime')} s) → {NEXT_JOB} "
             f"{jr2.get('JobRunState')} ({jr2.get('ExecutionTime')} s; marcador de posición hasta el 4.3)"
             + (f" · arrancó al intento {tries}" if tries > 1 else "")
             + (f"\n{jr1.get('ErrorMessage')}" if jr1.get("ErrorMessage") else ""),
             run_id=run_id, job_runs={JOB: jr1.get("Id"), NEXT_JOB: jr2.get("Id")},
             durations=[jr1.get("ExecutionTime"), jr2.get("ExecutionTime")])
    if not ok1:
        return part.finish()

    done = job_done(ctx, jr1.get("Id"), started) if jr1.get("Id") else None
    report = read_json_object(ctx, bucket, (done or {}).get("report") or LATEST_REPORT) or {}
    out = report.get("output") or {}
    same_files = files_by_key(report) == files_by_key(before_report) and bool(files_by_key(report))
    good3 = (report.get("workflow_run_id") == run_id and out.get("partitions_changed") == 0 and out.get("objects_deleted") == 0
             and same_files and (report.get("counts") or {}).get("rows_written") == (before_report.get("counts") or {}).get("rows_written"))
    part.add("F3", "reporte de la segunda corrida: nada cambió", OK if good3 else FALLA,
             f"corrida {report.get('run_id')} · particiones cambiadas {out.get('partitions_changed')} · borradas {out.get('objects_deleted')} · "
             f"{'mismos' if same_files else 'DISTINTOS'} sha256 que la corrida anterior en {len(files_by_key(report))} archivos · "
             f"escritas {(report.get('counts') or {}).get('rows_written')}", report=report_summary(report))

    after = trusted_listing(ctx, bucket) or {}
    part.add("F4", "mismos objetos y tamaños en trusted/posts/", OK if after == before else FALLA,
             f"{len(before)} → {len(after)} objetos" + ("" if after == before else
                                                        f"; nuevos {sorted(set(after) - set(before))[:5]}, faltan {sorted(set(before) - set(after))[:5]}"))
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["trusted"])
    rows1, ids1 = int(first_row(q).get("filas") or 0), int(first_row(q).get("event_ids") or 0)
    part.add("F5", "Athena: mismas filas, sin duplicar", OK if rows1 == rows0 and ids1 == rows1 else FALLA,
             f"{rows0} → {rows1} filas · {ids1} event_id distintos")

    props_res = ctx.aws("glue", "get-workflow-run-properties", "--name", WORKFLOW, "--run-id", run_id)
    props = (parse_json(props_res.out) or {}).get("RunProperties") or {}
    good6 = props.get("ta_trusted_report") == report.get("report_key") and props.get("ta_trusted_partitions_changed") == "0"
    part.add("F6", "propiedades de la corrida para el 4.3", OK if good6 else FALLA,
             f"ta_trusted_report = {props.get('ta_trusted_report')} · ta_trusted_rows = {props.get('ta_trusted_rows')} · "
             f"ta_trusted_partitions_changed = {props.get('ta_trusted_partitions_changed')}", properties=props)

    deleted, problems = v41.delete_markers(ctx, bucket)
    busy = [r.get("WorkflowRunId") for r in v41.workflow_runs(ctx) or [] if r.get("Status") in ("RUNNING", "STOPPING")]
    state1, _ = v41.get_schedule(ctx)
    good7 = not problems and not busy and state1 == state0
    part.add("F7", "limpieza y estado de ahorro", OK if good7 else AVISO,
             f"marcas del marcador de posición borradas: {deleted} · corridas en curso: {busy or 'ninguna'} · horario {state1}"
             + (f"; {problems}" if problems else "") + ("" if state1 == "DISABLED" else " (python project/scripts/validate_4_2.py off)"))

    d_exec = ctx.step_data("D", "D3").get("execution_s")
    exec_s = jr1.get("ExecutionTime") or d_exec
    glue_usd = job_cost(exec_s, EXPECTED_CAPACITY)
    next_usd = job_cost(jr2.get("ExecutionTime"), v41.PYSHELL_DPU)
    s3_usd = s3_request_cost(report)
    per_run = glue_usd + next_usd + s3_usd
    part.add("F8", "costo con lo medido", OK,
             f"{JOB}: {exec_s} s × {EXPECTED_CAPACITY:g} DPU × {DPU_USD_PER_HOUR} USD/DPU-h (mínimo 60 s) ≈ {glue_usd:.5f} USD · "
             f"{NEXT_JOB} ≈ {next_usd:.5f} · S3 (≈ {((report.get('input') or {}).get('objects'))} GET y {out.get('objects_written')} PUT) ≈ "
             f"{s3_usd:.5f} → ≈ {per_run:.4f} USD por corrida del workflow; con el horario encendido ≈ {per_run * RUNS_PER_DAY:.3f} USD/día "
             f"(≈ {per_run * RUNS_PER_DAY * 30:.2f} USD/mes; KMS con bucket key y logs aparte, sin comprobar en la factura)",
             usd_per_run=round(per_run, 5), usd_per_day=round(per_run * RUNS_PER_DAY, 4), glue_usd=round(glue_usd, 5),
             s3_usd=round(s3_usd, 5), execution_s=exec_s)
    return part.finish()


# --- status y off ---------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del lakehouse y de Trusted (solo lectura)")
    state, sched = v41.get_schedule(ctx)
    part.add("S1", f"horario {SCHEDULE}", OK if state in ("DISABLED", "NO_EXISTE") else AVISO,
             f"{state or sched.get('error')}" + (" (cobra: arranca el workflow cada hora)" if state == "ENABLED" else ""))
    runs = v41.workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    jobs = {k: v for k, v in v41.running_job_runs(ctx).items() if v}
    part.add("S2", "corridas y job runs en curso", AVISO if running or jobs else OK,
             (f"corridas: {', '.join(running)}; " if running else "ninguna corrida; ") + (f"job runs: {jobs}" if jobs else "ningún job run"))
    bucket = bucket_name(ctx)
    listing = trusted_listing(ctx, bucket)
    report = latest_report(ctx, bucket)
    c = report.get("counts") or {}
    part.add("S3", "Trusted y su último reporte", OK if listing is not None else AVISO,
             f"trusted/posts/: {len(listing or {})} objetos · " + (
                 f"último reporte {report.get('run_id')} ({report.get('finished_at') or '—'}): {c.get('rows_written')} filas, "
                 f"{((report.get('output') or {}).get('partitions'))} particiones" if report else
                 f"sin reporte en {LATEST_REPORT} (el job todavía no corrió)"))
    temps = v41.temp_schedules(ctx)
    part.add("S4", "horarios de prueba del 4.1", AVISO if temps else OK, ", ".join(temps) if temps else "ninguno")
    return part.finish()


def part_off(ctx: Ctx) -> int:
    """El apagado del 4.1 (horario, corridas, job runs, horarios de prueba y marcas), con la evidencia del 4.2."""
    return v41.part_off(ctx)


# --- CLI ----------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "terraform": part_terraform,
    "run": part_run,
    "athena": part_athena,
    "rerun": part_rerun,
    "status": part_status,
    "off": part_off,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_4_2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (solo para el estado de status.py)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--bucket", default=None, help="bucket del lake (si no, ta-dev-{cuenta}-lake)")
    parser.add_argument("--cache", default=None, help="L/A8: caché del profiling (si no, TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan (pide confirmación)")
    parser.add_argument("--profile", action="store_true", help="L3: perfilar la salida local con el profiler del 5.1 (~5 min)")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None,
         s3_rows: Callable[[str, str], dict[str, Any]] | None = None,
         s3_get: Callable[[str, str], bytes] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE, s3_rows=s3_rows, s3_get=s3_get)
    printer(f"Lote 4.2 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Para volver al estado de ahorro: python project/scripts/validate_4_2.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
