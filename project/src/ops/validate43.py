"""Validación automática del Lote 4.3 (HU-027, HU-030): ``docs/validation/validation-4-3.md``.

    python project/scripts/validate_4_3.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: el job contra el Trusted real de la caché local, sin
nube), ``terraform`` (C: script y descripción del job y las consultas guardadas de Curated; ``--apply``),
``run`` (D: una corrida real del job y la conciliación Trusted → Curated), ``athena`` (E: el «Listo cuando»
de la Fase 4, la conciliación en SQL y las consultas guardadas con sus bytes y su latencia), ``rerun`` (F:
segunda corrida dentro del workflow, que demuestra la idempotencia, y el costo sumado al del 4.2); además
``status`` (solo lectura), ``off`` (el apagado del 4.1: deshabilita el horario mientras se valida) y ``on``
(vuelve a encender el horario: **el estado normal es encendido**, decisión del usuario). Cada parte imprime un
paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su resumen en ``project/evidencia/validacion_4_3.json``.

Reutiliza de ``ops/validate41.py`` el contexto, el horario, las corridas del workflow, Athena, los logs, el
conteo de filas de S3 y el apagado; de ``ops/validate42.py`` las corridas sueltas de un job
(``start_job_run``/``wait_job_run``/``get_job`` con ``job=``), ``job_done``, ``read_json_object``,
``normalized``, ``job_cost`` y ``s3_request_cost``; de ``ops/validate32.py`` el ejecutor, los pasos y el plan
de Terraform leído en JSON. Lo que cambia la nube (``terraform apply``, correr el job o el workflow, el
horario) pide confirmación salvo con ``--yes``.
"""

from __future__ import annotations

import argparse
import hashlib
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
EVIDENCE = EVIDENCE_DIR / "validacion_4_3.json"
AWS_ENV = v41.AWS_ENV
QUERIES = v41.QUERIES
JOB_FILE = v41.GLUE_JOBS_DIR / "trusted_to_curated.py"

WORKFLOW = v41.WORKFLOW
JOB_KEY = "trusted_to_curated"
JOB = v41.JOBS[JOB_KEY]
PREV_JOB = v41.JOBS["raw_to_trusted"]
SCHEDULE = v41.SCHEDULE
GLUE_OUTPUT_LOG = v41.GLUE_OUTPUT_LOG
GLUE_ERROR_LOG = v41.GLUE_ERROR_LOG
TRUSTED_PREFIX = "trusted/posts/"
CURATED_PREFIX = "curated/"
TABLES = ("fact_posts", "dim_date", "dim_source", "dim_brand", "dim_user", "agg_emotion_by_brand_hour", "agg_trend_daily")
EDGE_PREFIX = "curated/edges_"
PLACEHOLDER_PREFIX = "curated/_ta_placeholder/"
LATEST_REPORT = "quality/_latest/trusted_to_curated.json"
TRUSTED_LATEST_REPORT = v42.LATEST_REPORT
PART_FILE = "part-00000.parquet"
FACT_KEY = re.compile(r"^curated/fact_posts/dt=(?P<dt>\d{4}-\d{2}-\d{2})/(?P<file>[^/]+)$")
PROJECTION_START = "2026-01-01"

# Capacidades (lakehouse.tf: 0,0625 DPU por defecto del módulo para este job, ADR-26; 1 DPU el del 4.2).
EXPECTED_CAPACITY = v41.PYSHELL_DPU
PREV_CAPACITY = v42.EXPECTED_CAPACITY
PEAK_WARN_MB = 800  # de 1 GB (0,0625 DPU): por encima, AVISO para pasar a 1 DPU

CURATED_QUERIES = {
    "ta-curated-posts-por-origen": "curated_posts_por_origen.sql",
    "ta-curated-tendencia-diaria": "curated_tendencia_diaria.sql",
    "ta-curated-emociones-por-hora-bogota": "curated_emociones_por_hora_bogota.sql",
    "ta-curated-semana-vs-fin-de-semana": "curated_semana_vs_fin_de_semana.sql",
    "ta-curated-precision-nlp": "curated_precision_nlp.sql",
}
TERRAFORM_TARGETS = ["module.glue_workflow", "module.athena"]
EXPECTED_UPDATES = {
    f'module.glue_workflow.aws_s3_object.script["{JOB_KEY}"]',
    f'module.glue_workflow.aws_glue_job.this["{JOB_KEY}"]',
}
EXPECTED_CREATES = {f'module.athena.aws_athena_named_query.this["{n}"]' for n in CURATED_QUERIES}

# Lo medido con el Trusted del 26-oct-2026 (exportador en pausa): si Trusted cambia, la conciliación manda y
# estas cifras solo dan AVISO.
TODAY = {"trusted_rows": 221629, "stream": 171629, "historical": 50000, "fact_partitions": 181, "objects": 187,
         "dim_date": 181, "dim_source": 3, "dim_brand": 3, "dim_user": 5936, "agg_emotion_by_brand_hour": 39954,
         "agg_trend_daily": 543, "nlp_missing": 65263, "with_sentiment": 156366, "with_emotion": 156366,
         "trend_rows_without_sentiment": 65263, "trend_short_rows": 6}

# Pruebas esperadas (B): las cuenta docs/validation/validation-4-3.md.
B1_EXPECTED = 123
B2_EXPECTED = 34
B4_EXPECTED = 30
B5_EXPECTED = 48

# Precios de us-east-1 (oct-2026, a confirmar con la factura) y KPI de §4/§5.10.
DPU_USD_PER_HOUR = v42.DPU_USD_PER_HOUR
S3_GET_USD = v42.S3_GET_USD
S3_PUT_USD = v42.S3_PUT_USD
RUNS_PER_DAY = 24
KPI_LATENCY_MS = 3000
#: Estimado antes de validar: F8 del 4.2 (0,0196) − el marcador (0,00046) + este job (≈ 0,00074).
ESTIMATED_USD_PER_RUN = 0.0199

LOT_FILES = [
    "src/lakehouse/glue_jobs/trusted_to_curated.py",
    "src/lakehouse/tests/test_lake_trusted_to_curated.py",
    "src/lakehouse/tests/test_lake_glue_jobs.py",
    *[f"src/lakehouse/queries/{f}" for f in CURATED_QUERIES.values()],
    "infra/aws/environments/dev/lakehouse.tf",
    "src/ops/validate43.py",
    "src/ops/tests/test_ops_validate43.py",
    "scripts/validate_4_3.py",
]
PREVIOUS_FILES = [
    "infra/aws/modules/glue_workflow/main.tf",
    "infra/aws/modules/athena_workgroup/main.tf",
    "infra/aws/modules/glue_role/main.tf",
    "src/lakehouse/catalog_spec.py",
    "src/lakehouse/s3_parquet.py",
    "src/lakehouse/glue_jobs/raw_to_trusted.py",
    "contracts/athena/curated.sql",
    "contracts/athena/posts.sql",
    "src/simulator/config/brands.yaml",
    "src/quality/s3sync.py",
    "src/ops/validate42.py",
    "src/ops/validate41.py",
    "src/ops/validate32.py",
    "src/ops/services.py",
]


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v42.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "4.3", "hu": ["HU-027", "HU-030"], "parts": {}}


# --- utilidades puras ---------------------------------------------------------------------------


def table_of(key: str) -> str | None:
    """La tabla de Curated de una clave (``curated/{tabla}/…``) o ``None``."""
    m = re.match(r"^curated/([^/]+)/", key)
    return m.group(1) if m and m.group(1) in TABLES else None


def layout_problems(keys: list[str]) -> list[str]:
    """Claves de las tablas del 4.3 que no son ``fact_posts/dt={d}/part-00000.parquet`` o ``{tabla}/part-00000.parquet``."""
    bad, per_table = [], {}
    for key in keys:
        if key.endswith("/"):
            continue
        table = table_of(key)
        if table is None:
            continue
        if table == "fact_posts":
            m = FACT_KEY.match(key)
            if not m:
                bad.append(f"fuera del layout: {key}")
            elif m.group("file") != PART_FILE:
                bad.append(f"archivo de más: {key}")
            elif m.group("dt") < PROJECTION_START:
                bad.append(f"dt antes de la proyección: {key}")
        elif key != f"curated/{table}/{PART_FILE}":
            bad.append(f"archivo de más: {key}")
        per_table[table] = per_table.get(table, 0) + 1
    missing = [t for t in TABLES if t not in per_table]
    bad += [f"falta la tabla {t}" for t in missing]
    return bad


def curated_schema_problems(data: bytes, table: str) -> tuple[list[str], int]:
    """Un Parquet de Curated frente al contrato (columnas, orden y tipos de ``contracts/athena/curated.sql``)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakehouse import catalog_spec

    athena = {"string": pa.string(), "int": pa.int32(), "bigint": pa.int64(), "double": pa.float64(), "date": pa.date32(),
              "boolean": pa.bool_(), "timestamp": pa.timestamp("ms", tz="UTC"),
              "array<string>": pa.list_(pa.field("element", pa.string()))}
    spec = catalog_spec.table(f"ta_curated.{table}")
    got = pq.read_table(pa.BufferReader(data))
    problems = []
    names = list(spec.column_names)
    if got.column_names != names:
        problems.append(f"{table}: columnas {got.column_names} ≠ contrato {names}")
    for col in spec.columns:
        want = athena.get(col.type)
        if col.name in got.column_names and want is not None and not got.schema.field(col.name).type.equals(want):
            problems.append(f"{table}.{col.name}: {got.schema.field(col.name).type} (esperado {want}, Athena {col.type})")
    return problems, got.num_rows


def files_by_key(report: dict[str, Any]) -> dict[str, str]:
    return {f.get("key"): f.get("sha256") for f in ((report or {}).get("output") or {}).get("files") or []}


def report_summary(report: dict[str, Any]) -> dict[str, Any]:
    """El reporte del job sin las listas por archivo (para la evidencia)."""
    out = {k: v for k, v in (report or {}).items() if k != "output"}
    out["output"] = {k: v for k, v in ((report or {}).get("output") or {}).items()
                     if k not in ("files", "changed_keys", "removed_keys")}
    return out


def failed_checks(report: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in ((report or {}).get("checks") or {}).items() if not v.get("ok")}


def curated_s3_cost(report: dict[str, Any]) -> float:
    """S3 de una corrida del 4.3: un GET por Parquet de Trusted, los PUT de lo que cambió, dos reportes y ~10 LIST."""
    objects = int(((report or {}).get("input") or {}).get("objects") or 0)
    puts = int(((report or {}).get("output") or {}).get("objects_written") or 0) + 2
    return (objects + 1) * S3_GET_USD + (puts + 10) * S3_PUT_USD


def compare_today(values: dict[str, Any]) -> list[str]:
    return [f"{k} {values.get(k)} (el 26-oct: {TODAY[k]})" for k in values if k in TODAY and values.get(k) != TODAY[k]]


# --- SQL de la parte E -------------------------------------------------------------------------------

READY_SQL = """SELECT origin, COUNT(*) AS filas, COUNT(DISTINCT event_id) AS event_ids, MIN(dt) AS desde, MAX(dt) AS hasta,
  count_if(cardinality(brands) > 1) AS comparativas
FROM ta_curated.fact_posts GROUP BY origin ORDER BY origin"""

LINKS_SQL = """SELECT
  (SELECT COUNT(*) FROM ta_trusted.posts) AS trusted,
  (SELECT COUNT(*) FROM ta_curated.fact_posts) AS fact,
  (SELECT count_if(date_key <> CAST(replace(dt, '-', '') AS integer) OR date_format(event_time, '%Y-%m-%d') <> dt)
     FROM ta_curated.fact_posts) AS date_key_mal,
  (SELECT COUNT(*) FROM ta_curated.fact_posts f LEFT JOIN ta_curated.dim_date d ON d.date_key = f.date_key
     WHERE d.date_key IS NULL) AS sin_fecha,
  (SELECT COUNT(*) FROM ta_curated.fact_posts f LEFT JOIN ta_curated.dim_source s ON s.source = f.source
     WHERE s.source IS NULL) AS sin_fuente,
  (SELECT COUNT(*) FROM ta_curated.fact_posts f LEFT JOIN ta_curated.dim_brand b ON b.brand = f.brand_primary
     WHERE f.brand_primary IS NOT NULL AND b.brand IS NULL) AS sin_marca,
  (SELECT COUNT(*) FROM ta_curated.fact_posts f LEFT JOIN ta_curated.dim_user u ON u.user_hash = f.user_hash
     WHERE f.user_hash IS NOT NULL AND u.user_hash IS NULL) AS sin_usuario,
  (SELECT COUNT(*) FROM ta_curated.dim_date) AS dias,
  (SELECT date_diff('day', min(full_date), max(full_date)) + 1 FROM ta_curated.dim_date) AS dias_esperados"""

TREND_SQL = """WITH f AS (
  SELECT dt, brand_primary AS brand, COUNT(*) AS n,
    count_if(sentiment_label = 'negative') AS neg, count_if(sentiment_label = 'neutral') AS neu,
    count_if(sentiment_label = 'positive') AS pos,
    avg(CASE sentiment_label WHEN 'positive' THEN sentiment_score WHEN 'negative' THEN -sentiment_score
                             WHEN 'neutral' THEN 0 END) AS avg_s
  FROM ta_curated.fact_posts WHERE brand_primary IS NOT NULL GROUP BY dt, brand_primary),
a AS (SELECT date_format(full_date, '%Y-%m-%d') AS dt, brand, n_posts, n_negative, n_neutral, n_positive, avg_sentiment
      FROM ta_curated.agg_trend_daily)
SELECT
  count_if(a.dt IS NULL OR f.dt IS NULL) AS claves_distintas,
  count_if(a.n_posts <> f.n OR a.n_negative <> f.neg OR a.n_neutral <> f.neu OR a.n_positive <> f.pos) AS conteos_distintos,
  count_if(abs(coalesce(a.avg_sentiment, 9) - coalesce(f.avg_s, 9)) > 0.000001) AS promedios_distintos,
  count_if(a.n_posts <> a.n_negative + a.n_neutral + a.n_positive) AS filas_con_posts_sin_nlp,
  sum(a.n_posts) AS n_posts,
  sum(a.n_negative + a.n_neutral + a.n_positive) AS con_sentimiento
FROM a FULL OUTER JOIN f ON a.dt = f.dt AND a.brand = f.brand"""

EMOTION_SQL = """WITH f AS (
  SELECT date_trunc('hour', event_time) AS h, brand_primary AS brand, source, emotion_label, COUNT(*) AS n
  FROM ta_curated.fact_posts WHERE emotion_label IS NOT NULL AND brand_primary IS NOT NULL GROUP BY 1, 2, 3, 4),
e AS (SELECT hour_start AS h, brand, source, emotion_label, n_posts FROM ta_curated.agg_emotion_by_brand_hour)
SELECT
  count_if(e.h IS NULL OR f.h IS NULL) AS claves_distintas,
  count_if(e.n_posts <> f.n) AS conteos_distintos,
  sum(e.n_posts) AS n_posts,
  (SELECT count_if(emotion_label IS NULL) FROM ta_curated.fact_posts) AS sin_emocion
FROM e FULL OUTER JOIN f ON e.h = f.h AND e.brand = f.brand AND e.source = f.source AND e.emotion_label = f.emotion_label"""

USER_SQL = """WITH f AS (
  SELECT user_hash, min(event_time) AS fs, max(event_time) AS ls, COUNT(*) AS n, max(followers) AS mf
  FROM ta_curated.fact_posts WHERE user_hash IS NOT NULL GROUP BY user_hash)
SELECT
  count_if(u.user_hash IS NULL OR f.user_hash IS NULL) AS claves_distintas,
  count_if(u.first_seen <> f.fs OR u.last_seen <> f.ls OR u.n_posts <> f.n OR u.max_followers IS DISTINCT FROM f.mf)
    AS filas_distintas,
  sum(u.n_posts) AS n_posts,
  COUNT(*) AS usuarios
FROM ta_curated.dim_user u FULL OUTER JOIN f ON u.user_hash = f.user_hash"""

COUNT_SQL = "SELECT COUNT(*) AS filas, COUNT(DISTINCT event_id) AS event_ids FROM ta_curated.fact_posts"


# --- piezas de AWS ---------------------------------------------------------------------------------


def bucket_name(ctx: Ctx) -> str:
    return v42.bucket_name(ctx)


def curated_listing(ctx: Ctx, bucket: str) -> dict[str, int] | None:
    """Claves y tamaños de las siete tablas del 4.3 (sin aristas ni marcas)."""
    keys = v41.s3_keys(ctx, bucket, CURATED_PREFIX)
    if keys is None:
        return None
    return {k.get("Key"): int(k.get("Size") or 0) for k in keys if table_of(k.get("Key") or "")}


def prefix_listing(ctx: Ctx, bucket: str, prefix: str) -> dict[str, int] | None:
    keys = v41.s3_keys(ctx, bucket, prefix)
    return None if keys is None else {k.get("Key"): int(k.get("Size") or 0) for k in keys}


def latest_report(ctx: Ctx, bucket: str) -> dict[str, Any]:
    return v42.read_json_object(ctx, bucket, LATEST_REPORT) or {}


def first_row(q: dict[str, Any]) -> dict[str, Any]:
    return (q.get("rows") or [{}])[0]


def ints(row: dict[str, Any]) -> dict[str, int]:
    out = {}
    for k, v in (row or {}).items():
        try:
            out[k] = int(float(v)) if v not in (None, "") else 0
        except (TypeError, ValueError):
            out[k] = 0
    return out


def named_queries(ctx: Ctx) -> dict[str, dict[str, Any]]:
    """Consultas guardadas de ``ta-analytics``: nombre → ``{QueryString, Database, NamedQueryId}``."""
    res = ctx.aws("athena", "list-named-queries", "--work-group", v41.WORKGROUP)
    ids = (parse_json(res.out) or {}).get("NamedQueryIds") or [] if res.ok else []
    if not ids:
        return {}
    got = ctx.aws("athena", "batch-get-named-query", "--named-query-ids", *ids)
    items = (parse_json(got.out) or {}).get("NamedQueries") or [] if got.ok else []
    return {q.get("Name"): q for q in items}


# --- Parte A · preflight --------------------------------------------------------------------------


def local_trusted_root(ctx: Ctx) -> tuple[Path | None, str]:
    """El Trusted local para L: la copia de S3 del 5.1 (``{caché}/s3/{bucket}``) o la salida local del 4.2."""
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from quality.s3sync import bucket_root, resolve_cache

    cache = Path(resolve_cache(getattr(ctx.options, "cache", None)))
    bucket = getattr(ctx.options, "bucket", None) or "ta-dev-648617469176-lake"
    for root, label in ((Path(bucket_root(cache, bucket)), "copia de S3 (profile_data.py sync --layer trusted)"),
                        (cache / "raw_to_trusted_local", "salida local del 4.2 (validate_4_2.py local)")):
        if (root / "trusted" / "posts").is_dir():
            return root, label
    return None, str(cache)


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
             "\n".join(problems) if problems else "las tablas de ta_curated y las demás = las DDL")
    tools = {t: ctx.runner.which(t) for t in ("terraform", "aws", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A5", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))

    state, sched = v41.get_schedule(ctx)
    runs = v41.workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    jobs = {k: v for k, v in v41.running_job_runs(ctx).items() if v}
    quiet = state in ("DISABLED", "NO_EXISTE") and not running and not jobs
    note = ""
    if state == "ENABLED":
        note = (" (ENABLED es el estado normal por decisión del usuario, pero mientras valida apáguelo: "
                "python project/scripts/validate_4_3.py off; al terminar: validate_4_3.py on --yes)")
    elif running or jobs:
        note = " (espere a que terminen o: python project/scripts/validate_4_3.py off)"
    part.add("A6", "AWS: horario, corridas y job runs", OK if quiet else AVISO,
             f"horario {SCHEDULE}: {state or sched.get('error')}; "
             + (f"corridas en curso: {', '.join(running)}; " if running else "ninguna corrida en curso; ")
             + (f"job runs en curso: {jobs}" if jobs else "ningún job run en curso") + note,
             schedule=state, running=running, job_runs=jobs)
    lines = v41.services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("A7", "GCP y local (services.status)", AVISO if charging else OK,
             "\n".join(lines) + ("\neste lote no los necesita: python project/scripts/off.py" if charging else ""))
    try:
        root, label = local_trusted_root(ctx)
        detail = (f"{root}/trusted/posts/ ({label})" if root else
                  f"no hay trusted/posts/ en {label} (L la necesita: python project/scripts/profile_data.py sync --layer trusted; "
                  "solo lee S3)")
    except Exception as exc:  # noqa: BLE001
        root, detail = None, f"{type(exc).__name__}: {exc}"
    part.add("A8", "Trusted local para la parte L", OK if root else AVISO, detail)
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/lakehouse", ["src/lakehouse", "-q", "-rs"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate43.py", ["src/ops/tests/test_ops_validate43.py", "-q"], B2_EXPECTED),
        ("B3", "pytest contracts/tests", ["contracts/tests", "-q"], 18),
        ("B4", "regresión: test_ops_validate42.py (piezas reutilizadas)", ["src/ops/tests/test_ops_validate42.py", "-q"], B4_EXPECTED),
        ("B5", "regresión: test_ops_validate41.py (piezas reutilizadas)", ["src/ops/tests/test_ops_validate41.py", "-q"], B5_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · el job en local contra el Trusted real ----------------------------------------------


def run_local_job(ctx: Ctx, python: str, src: Path, dst: Path, bucket: str) -> tuple[Result, dict[str, Any] | None]:
    res = ctx.run([python, str(JOB_FILE), "--lake_bucket", bucket, "--ta_local_src", str(src), "--ta_local_dst", str(dst)],
                  cwd=PROJECT, timeout=3600)
    done = next((x for x in v41.json_lines([res.out]) if x.get("event") == "glue_job_done"), None)
    return res, done


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "El job en local contra el Trusted real (sin nube)")
    bucket = getattr(ctx.options, "bucket", None) or "ta-dev-648617469176-lake"
    try:
        src, label = local_trusted_root(ctx)
    except Exception as exc:  # noqa: BLE001 - CacheError (caché dentro del repo) u otro
        part.add("L0", "Trusted local", FALLA, f"{type(exc).__name__}: {exc}")
        return part.finish()
    if src is None:
        part.add("L0", "Trusted local", AVISO, f"no hay trusted/posts/ en {label}: python project/scripts/profile_data.py sync "
                 "--layer trusted (solo lee S3, ~1 min) o validate_4_2.py local, y repita")
        return part.finish()
    python = getattr(ctx.options, "python", None) or sys.executable
    from quality.s3sync import resolve_cache

    dst = Path(resolve_cache(getattr(ctx.options, "cache", None))) / "trusted_to_curated_local"
    part.add("L0", "Trusted local", OK, f"lee {src}/trusted/posts/ ({label}) · escribe en {dst} (fuera del repo) · {python}")
    part.note("L1: el job lee los Parquet de Trusted y construye Curated (≈ 5 s y ~0,4–0,6 GB)…")
    res, done = run_local_job(ctx, python, src, dst, bucket)
    if not res.ok or not done:
        part.add("L1", "corrida local", FALLA, res.tail(8))
        return part.finish()
    tables = done.get("tables") or {}
    values = {"trusted_rows": done.get("rows_read"), "fact_partitions": done.get("fact_partitions"),
              "nlp_missing": done.get("nlp_missing"), **{k: tables.get(k) for k in TABLES if k != "fact_posts"}}
    diffs = compare_today(values)
    env = done.get("environment") or {}
    part.add("L1", "corrida local (conteos y conciliación)", (AVISO if diffs else OK) if done.get("conciliation_ok") else FALLA,
             f"Trusted {done.get('rows_read')} → fact_posts {done.get('fact_rows')} en {done.get('fact_partitions')} particiones · "
             f"tablas {tables} · sin NLP {done.get('nlp_missing')} · {done.get('duration_s')} s, {done.get('cpu_seconds')} s de CPU, "
             f"pico {done.get('peak_rss_mb')} MB · Python {env.get('python')}, pyarrow {env.get('pyarrow')}"
             + ("\ndistinto del Trusted del 26-oct: " + "; ".join(diffs) if diffs else ""), done=done)
    res2, done2 = run_local_job(ctx, python, src, dst, bucket)
    idem = bool(res2.ok and done2 and done2.get("objects_written") == 0 and done2.get("files_changed") == 0
                and done2.get("objects_deleted") == 0 and done2.get("fact_rows") == done.get("fact_rows"))
    part.add("L2", "segunda corrida local (idempotencia)", OK if idem else FALLA,
             (f"escritos {done2.get('objects_written')} · sin cambios {done2.get('objects_unchanged')} · archivos cambiados "
              f"{done2.get('files_changed')} · borrados {done2.get('objects_deleted')}") if done2 else res2.tail(6), done=done2)
    problems, counted = [], {}
    for table in TABLES:
        files = sorted((dst / "curated" / table).rglob("*.parquet"))
        if not files:
            problems.append(f"{table}: sin archivos")
            continue
        p, n = curated_schema_problems(files[0].read_bytes(), table)
        problems += p
        counted[table] = len(files)
    part.add("L3", "esquema de cada tabla = contracts/athena/curated.sql", FALLA if problems else OK,
             "\n".join(problems[:12]) if problems else f"7 tablas (archivos: {counted}); int → int32, date → date32, "
             "boolean, timestamp[ms, UTC], bigint → int64, array<string>")
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (script y descripción del job y consultas guardadas de Curated)")
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
    plan_path = EVIDENCE_DIR / "tfplan_4_3"
    title = "plan (-target=module.glue_workflow -target=module.athena)"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", title, FALLA, res.err or res.tail(8))
        return part.finish()
    updates, creates = set(summary["update"]), set(summary["create"])
    extra = (updates - EXPECTED_UPDATES) | (creates - EXPECTED_CREATES) | set(summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", title, FALLA, f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    job_addr = f'module.glue_workflow.aws_glue_job.this["{JOB_KEY}"]'
    bad_job = [d for d in (summary.get("diffs") or {}).get(job_addr, []) if not d.startswith("description")]
    if bad_job:
        part.add("C2", title, FALLA, f"el job cambia algo más que description: {bad_job}", **summary)
        return part.finish()
    pending = updates | creates
    part.add("C2", title, OK, plan_line(summary) + (" (ya aplicado)" if not pending else
             f" (esperado: {len(EXPECTED_CREATES)} consultas guardadas a crear; el script y la descripción del job a cambiar)")
             + diff_text(summary, updates), **summary)
    applied = not pending
    if pending:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
        elif ctx.ask(f"¿Aplicar el plan de infra/aws ({len(creates)} a crear, {len(updates)} a cambiar, 0 a destruir)?"):
            res = tf_apply(ctx, AWS_ENV, plan_path)
            applied = res.ok and f"Apply complete! Resources: {len(creates)} added, {len(updates)} changed, 0 destroyed." in res.out
            part.add("C3", "apply", OK if applied else FALLA, res.tail(4) if res.ok else res.err)
            if not res.ok:
                return part.finish()
        else:
            part.add("C3", "apply", AVISO, "no confirmado")
    else:
        part.add("C3", "apply", OK, "nada que aplicar")
    if applied:
        res, again = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, EVIDENCE_DIR / "tfplan_4_3_check")
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
    job = v42.get_job(ctx, JOB)
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
        problems.append(f"MaxCapacity {job.get('MaxCapacity')} (esperado {EXPECTED_CAPACITY})")
    if job.get("MaxRetries") != 0:
        problems.append(f"MaxRetries {job.get('MaxRetries')}")
    for k, want in (("--lake_bucket", bucket), ("--ta_input_prefix", "trusted/"), ("--ta_output_prefix", "curated/"),
                    ("library-set", "analytics")):
        if args.get(k) != want:
            problems.append(f"{k} = {args.get(k)!r} (esperado {want!r})")
    deployed = ctx.aws("s3", "cp", f"s3://{bucket}/_glue/scripts/{JOB}.py", "-")
    if not deployed.ok or hashlib.sha256(v42.normalized(deployed.out)).hexdigest() != \
            hashlib.sha256(v42.normalized(JOB_FILE.read_bytes())).hexdigest():
        problems.append(f"el script de S3 no es src/lakehouse/glue_jobs/{JOB_KEY}.py (aplique C)")
    return problems


def part_run(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Corrida real de ta-job-trusted-to-curated y conciliación Trusted → Curated")
    bucket = bucket_name(ctx)
    state, _ = v41.get_schedule(ctx)
    if state == "ENABLED":
        part.add("D0", "horario", AVISO, "ENABLED: una corrida del horario puede caer en medio (la parte espera y reintenta); "
                 "mientras valida: python project/scripts/validate_4_3.py off, y al terminar on --yes")
    problems = check_job(ctx, bucket)
    part.add("D1", f"{JOB}: Python shell 3.9, {EXPECTED_CAPACITY:g} DPU, script de S3 = repo", FALLA if problems else OK,
             "\n".join(problems) if problems else f"pythonshell 3.9 · {EXPECTED_CAPACITY:g} DPU (1 GB) · library-set analytics · "
             "script de S3 idéntico al repo")
    if problems:
        return part.finish()

    part.note("D2: contando las filas de Trusted por los pies de sus Parquet (≈ 543 GET)…")
    trusted = v42.s3_rows(ctx, bucket, TRUSTED_PREFIX)
    placeholders_before = prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    edges_before = prefix_listing(ctx, bucket, EDGE_PREFIX)
    part.add("D2", "Trusted antes de la corrida", FALLA if trusted.get("error_count") else OK,
             f"trusted/posts/: {trusted.get('objects')} objetos y {trusted.get('rows')} filas"
             + ("" if trusted.get("rows") == TODAY["trusted_rows"] else f" (el 26-oct eran {TODAY['trusted_rows']}: ¿cambió Raw?)")
             + f" · curated/_ta_placeholder/: {len(placeholders_before or {})} · curated/edges_*: {len(edges_before or {})}",
             trusted={k: trusted.get(k) for k in ("objects", "rows", "error_count", "seconds")},
             placeholders_before=len(placeholders_before or {}), edges_before=len(edges_before or {}))

    if not ctx.ask(f"¿Correr {JOB} una vez ({EXPECTED_CAPACITY:g} DPU, ≈ 0,001 USD; reescribe lo que cambie en curated/)?"):
        part.add("D3", "corrida del job", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = v42.start_job_run(ctx, {"--ta_validacion": "4.3"}, part, job=JOB)
    if not run_id:
        part.add("D3", "corrida del job", FALLA, err)
        return part.finish()
    part.note(f"{run_id}: esperando a que termine (1–3 min, la mayor parte es el arranque de Glue)…")
    jr = v42.wait_job_run(ctx, part, run_id, job=JOB)
    good = jr.get("JobRunState") == "SUCCEEDED"
    part.add("D3", "corrida del job", OK if good else FALLA,
             f"{run_id}: {jr.get('JobRunState')}{' (tiempo agotado)' if jr.get('_timeout') else ''} · {jr.get('ExecutionTime')} s de "
             f"ejecución · {jr.get('MaxCapacity')} DPU" + (f" · arrancó al intento {tries}" if tries > 1 else "")
             + (f"\n{jr.get('ErrorMessage')}" if jr.get("ErrorMessage") else "")
             + ("" if good else f"\ntraceback en {GLUE_ERROR_LOG} (flujo {run_id})"),
             job_run_id=run_id, execution_s=jr.get("ExecutionTime"), max_capacity=jr.get("MaxCapacity"), state=jr.get("JobRunState"))
    if not good:
        return part.finish()

    done = v42.job_done(ctx, run_id, started)
    env = (done or {}).get("environment") or {}
    peak = (done or {}).get("peak_rss_mb") or 0
    part.add("D4", f"línea glue_job_done en {GLUE_OUTPUT_LOG}", (AVISO if peak > PEAK_WARN_MB else OK) if done else FALLA,
             (f"leídas {done.get('rows_read')} · fact_posts {done.get('fact_rows')} en {done.get('fact_partitions')} particiones · "
              f"tablas {done.get('tables')} · lectura {done.get('read_seconds')} s · total {done.get('duration_s')} s · CPU "
              f"{done.get('cpu_seconds')} s · pico de memoria {peak} MB de 1024 · Python {env.get('python')}, pyarrow "
              f"{env.get('pyarrow')}, boto3 {env.get('boto3')}"
              + (f"\npico > {PEAK_WARN_MB} MB: considere 1 DPU (glue_job_settings)" if peak > PEAK_WARN_MB else "")) if done else
             f"sin glue_job_done en el flujo {run_id} (¿tardan los logs?)", done=done)
    report_key = (done or {}).get("report") or LATEST_REPORT
    report = v42.read_json_object(ctx, bucket, report_key)
    latest = v42.read_json_object(ctx, bucket, LATEST_REPORT)
    same = bool(report and latest and report.get("run_id") == latest.get("run_id"))
    part.add("D5", "reporte de la corrida en quality/", OK if report and same else FALLA,
             f"{report_key} y {LATEST_REPORT} (corrida {report.get('run_id')}, esquema {report.get('schema')})" if report else
             f"no se pudo leer {report_key}", report=report_summary(report or {}))
    if not report:
        return part.finish()

    c, i = report.get("counts") or {}, report.get("input") or {}
    checks = report.get("checks") or {}
    bad = failed_checks(report)
    rows_ok = trusted.get("rows") == i.get("rows") == c.get("fact_rows")
    tables = {k: v.get("rows") for k, v in (report.get("tables") or {}).items()}
    values = {"trusted_rows": i.get("rows"), "nlp_missing": c.get("nlp_missing"), "with_sentiment": c.get("with_sentiment"),
              "with_emotion": c.get("with_emotion"),
              "trend_rows_without_sentiment": (checks.get("trend_rows_without_sentiment") or {}).get("observed"),
              **{k: tables.get(k) for k in TABLES if k != "fact_posts"}}
    diffs = compare_today(values) if trusted.get("rows") == TODAY["trusted_rows"] else []
    status = FALLA if (bad or not rows_ok or not checks) else (AVISO if diffs else OK)
    part.add("D6", "conciliación Trusted → Curated", status,
             f"Trusted {trusted.get('rows')} (pies de S3) = leídas {i.get('rows')} = fact_posts {c.get('fact_rows')}"
             + f"\nagg_trend_daily: n_posts {(checks.get('trend_n_posts_eq_rows_with_brand') or {}).get('observed')} = filas con marca; "
             f"con sentimiento {(checks.get('trend_sentiment_eq_rows_with_sentiment') or {}).get('observed')}; sin sentimiento "
             f"{(checks.get('trend_rows_without_sentiment') or {}).get('observed')} (= sin NLP {c.get('nlp_missing')}: decisión 2)"
             + f"\nagg_emotion_by_brand_hour: {(checks.get('emotion_n_posts_eq_rows_with_emotion') or {}).get('observed')} = filas con "
             f"emoción · dim_user: {(checks.get('user_n_posts_eq_rows_with_user') or {}).get('observed')} = filas con usuario · "
             f"llaves foráneas y dim_date continua: {'OK' if not bad else 'FALLA'}"
             + (f"\nno cuadra: {json.dumps(bad, sort_keys=True)}" if bad else "")
             + ("" if rows_ok else f"\nTrusted {trusted.get('rows')}, leídas {i.get('rows')}, fact_posts {c.get('fact_rows')}")
             + ("\ndistinto del Trusted del 26-oct: " + "; ".join(diffs) if diffs else ""),
             checks=checks, tables=tables)

    part.note("D7: contando las filas de curated/ por los pies de sus Parquet…")
    listing = curated_listing(ctx, bucket) or {}
    layout = layout_problems(list(listing))
    s3_tables = {t: v42.s3_rows(ctx, bucket, f"{CURATED_PREFIX}{t}/").get("rows") for t in TABLES}
    placeholders_after = prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    edges_after = prefix_listing(ctx, bucket, EDGE_PREFIX)
    out = report.get("output") or {}
    good7 = (s3_tables == tables and len(listing) == out.get("objects") and not layout
             and (placeholders_after or {}).keys() <= (placeholders_before or {}).keys() and edges_after == edges_before)
    part.add("D7", "curated/ en S3 (filas, layout; marcas y aristas sin tocar)", OK if good7 else FALLA,
             f"{len(listing)} objetos ({out.get('objects')} en el reporte; {tables.get('fact_posts')} filas de fact_posts en "
             f"{(report.get('tables') or {}).get('fact_posts', {}).get('files')} particiones) · filas por tabla en los pies {s3_tables} · "
             f"dt {i.get('dt_min')} → {i.get('dt_max')} · {out.get('bytes')} bytes · escritos {out.get('objects_written')}, "
             f"sin cambios {out.get('objects_unchanged')}, borrados {out.get('objects_deleted')} · curated/_ta_placeholder/: "
             f"{len(placeholders_before or {})} → {len(placeholders_after or {})} · curated/edges_*: "
             f"{len(edges_before or {})} → {len(edges_after or {})}"
             + (("\n" + "\n".join(layout[:10])) if layout else "")
             + ("" if s3_tables == tables else f"\nel reporte dice {tables}"), s3_tables=s3_tables, layout=layout[:20])

    problems, sizes = [], {}
    for table in TABLES:
        keys = sorted((k for k in listing if table_of(k) == table), key=lambda k: listing[k])
        if not keys:
            problems.append(f"{table}: sin objetos")
            continue
        try:
            p, n = curated_schema_problems((ctx.s3_get or v42.default_s3_get(ctx))(bucket, keys[0]), table)
        except Exception as exc:  # noqa: BLE001
            p, n = [f"{table}: {type(exc).__name__}: {exc}"], 0
        problems += p
        sizes[table] = n
    part.add("D8", "esquema de un Parquet de cada tabla = contrato", FALLA if problems else OK,
             "\n".join(problems[:12]) if problems else f"7 tablas con las columnas y tipos de contracts/athena/curated.sql "
             f"(filas del archivo leído: {sizes})")
    return part.finish()


# --- Parte E · Athena ---------------------------------------------------------------------------


def part_athena(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Athena: ta_curated (Listo cuando, conciliación, consultas guardadas)")
    bucket = bucket_name(ctx)
    report = latest_report(ctx, bucket)
    c = report.get("counts") or {}
    fact_report = int(c.get("fact_rows") or 0)
    db = v41.DATABASES["curated"]
    scanned: list[int] = []

    def query(sql: str) -> dict[str, Any]:
        q = v41.athena_query(ctx, sql, db)
        scanned.append(q.get("scanned_bytes", 0))
        return q

    q = query(READY_SQL)
    rows = {r.get("origin"): ints(r) for r in q.get("rows") or []}
    total = sum(r.get("filas", 0) for r in rows.values())
    distinct = sum(r.get("event_ids", 0) for r in rows.values())
    good = q["state"] == "SUCCEEDED" and set(rows) == {"stream", "historical"} and all(r.get("filas") for r in rows.values()) \
        and total == distinct and (not fact_report or total == fact_report)
    part.add("E1", "Listo cuando de la Fase 4: SELECT sobre ta_curated.fact_posts con stream e histórico", OK if good else FALLA,
             ("; ".join(f"{o}: {r.get('filas')} filas, {r.get('event_ids')} event_id, comparativas {r.get('comparativas')} "
                        f"({(q_row or {}).get('desde')} → {(q_row or {}).get('hasta')})"
                        for (o, r), q_row in zip(rows.items(), q.get("rows") or []))
              + f" · total {total} (reporte {fact_report})") if q["state"] == "SUCCEEDED" else f"{q['state']}: {q['reason']}",
             rows=q.get("rows"), total=total)

    q = query(LINKS_SQL)
    r = ints(first_row(q)) if q["state"] == "SUCCEEDED" else {}
    zero = ("date_key_mal", "sin_fecha", "sin_fuente", "sin_marca", "sin_usuario")
    bad = [f"{k} = {r.get(k)}" for k in zero if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    if r and r.get("trusted") != r.get("fact"):
        bad.append(f"ta_trusted.posts {r.get('trusted')} ≠ fact_posts {r.get('fact')}")
    if r and r.get("dias") != r.get("dias_esperados"):
        bad.append(f"dim_date {r.get('dias')} filas, esperadas {r.get('dias_esperados')}")
    part.add("E2", "Trusted = fact_posts; date_key; llaves foráneas; dim_date continua", FALLA if bad else OK,
             "\n".join(bad) if bad else f"ta_trusted.posts {r.get('trusted')} = fact_posts {r.get('fact')} · 0 date_key ≠ dt ≠ "
             f"DATE(event_time) · 0 filas sin su fecha, fuente, marca o usuario en las dimensiones · dim_date {r.get('dias')} días "
             "seguidos", counts=r)

    q = query(TREND_SQL)
    r = ints(first_row(q)) if q["state"] == "SUCCEEDED" else {}
    nlp = int(c.get("nlp_missing") or 0)
    bad = [f"{k} = {r.get(k)}" for k in ("claves_distintas", "conteos_distintos", "promedios_distintos") if r.get(k)] if r \
        else [f"{q['state']}: {q['reason']}"]
    if r and r.get("n_posts", 0) - r.get("con_sentimiento", 0) != nlp:
        bad.append(f"n_posts − con sentimiento = {r.get('n_posts', 0) - r.get('con_sentimiento', 0)} y el reporte dice {nlp} sin NLP")
    part.add("E3", "agg_trend_daily = recalculado desde fact_posts (decisión 2: sin NLP en n_posts, no en el sentimiento)",
             FALLA if bad else OK,
             "\n".join(bad) if bad else f"0 claves, conteos o promedios distintos · n_posts {r.get('n_posts')} − con sentimiento "
             f"{r.get('con_sentimiento')} = {nlp} filas sin NLP · {r.get('filas_con_posts_sin_nlp')} filas (día × marca) con "
             "n_posts ≠ n_negative + n_neutral + n_positive", counts=r)

    q = query(EMOTION_SQL)
    r = ints(first_row(q)) if q["state"] == "SUCCEEDED" else {}
    bad = [f"{k} = {r.get(k)}" for k in ("claves_distintas", "conteos_distintos") if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    if r and r.get("n_posts", 0) + r.get("sin_emocion", 0) != (fact_report or r.get("n_posts", 0) + r.get("sin_emocion", 0)):
        bad.append(f"n_posts {r.get('n_posts')} + sin emoción {r.get('sin_emocion')} ≠ fact_posts {fact_report}")
    part.add("E4", "agg_emotion_by_brand_hour = recalculado (sin las filas sin emoción)", FALLA if bad else OK,
             "\n".join(bad) if bad else f"0 claves o conteos distintos · n_posts {r.get('n_posts')} + {r.get('sin_emocion')} filas sin "
             f"emoción = {r.get('n_posts', 0) + r.get('sin_emocion', 0)}", counts=r)

    q = query(USER_SQL)
    r = ints(first_row(q)) if q["state"] == "SUCCEEDED" else {}
    bad = [f"{k} = {r.get(k)}" for k in ("claves_distintas", "filas_distintas") if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    part.add("E5", "dim_user = recalculado (first_seen, last_seen, n_posts, max_followers)", FALLA if bad else OK,
             "\n".join(bad) if bad else f"{r.get('usuarios')} usuarios, 0 distintos · suma de n_posts {r.get('n_posts')}", counts=r)

    stored = named_queries(ctx)
    results, problems = [], []
    for name, fname in CURATED_QUERIES.items():
        nq = stored.get(name)
        if not nq:
            problems.append(f"{name}: no está en {v41.WORKGROUP} (aplique C)")
            continue
        want = v42.normalized((QUERIES / fname).read_text(encoding="utf-8")).strip()
        if v42.normalized(nq.get("QueryString") or "").strip() != want:
            problems.append(f"{name}: el texto guardado no es src/lakehouse/queries/{fname} (aplique C)")
        if nq.get("Database") != db:
            problems.append(f"{name}: base {nq.get('Database')}")
        q = query(nq.get("QueryString") or "")
        results.append({"name": name, "state": q["state"], "rows": len(q.get("rows") or []), "scanned_bytes": q.get("scanned_bytes", 0),
                        "engine_ms": q.get("engine_ms", 0), "total_ms": q.get("total_ms", 0)})
        if q["state"] != "SUCCEEDED":
            problems.append(f"{name}: {q['state']}: {q['reason']}")
        elif not q.get("rows"):
            problems.append(f"{name}: 0 filas")
    part.add("E6", f"las {len(CURATED_QUERIES)} consultas guardadas de Curated (HU-030)", FALLA if problems else OK,
             "\n".join(problems) if problems else "; ".join(f"{x['name']}: {x['rows']} filas, {x['scanned_bytes'] / 1e6:.1f} MB, "
                                                           f"{x['total_ms']} ms" for x in results), queries=results)
    cutoff = v41.DEFAULT_CUTOFF
    over = [s for s in scanned if s > cutoff]
    billed = sum(max(s, 10_000_000) for s in scanned)
    part.add("E7", "bytes escaneados ≤ límite del workgroup", FALLA if over else OK,
             f"{sum(scanned) / 1e6:.1f} MB en {len(scanned)} consultas; la mayor {max(scanned or [0]) / 1e6:.1f} MB (límite "
             f"{cutoff / 2**30:.0f} GiB) · ≈ {billed / 1e12 * 5:.5f} USD (mínimo 10 MB por consulta)",
             scanned_bytes=sum(scanned), queries=len(scanned), usd=round(billed / 1e12 * 5, 6))
    slow = [x for x in results if x["total_ms"] > KPI_LATENCY_MS]
    worst = max((x["total_ms"] for x in results), default=0)
    part.add("E8", f"latencia de las consultas guardadas frente al KPI de < {KPI_LATENCY_MS / 1000:g} s (§4)",
             OK if results and not slow else (AVISO if results else OMITIDO),
             (f"la más lenta {worst} ms (total, con la cola de Athena); motor: "
              + ", ".join(f"{x['name']} {x['engine_ms']} ms" for x in results)
              + ("" if not slow else f"\n{len(slow)} pasan de {KPI_LATENCY_MS} ms: el KPI es del dashboard con SPICE (Lote 6.1); "
                 "Athena directo es la referencia")) if results else "sin consultas", worst_ms=worst)
    return part.finish()


# --- Parte F · segunda corrida (idempotencia) y costo ----------------------------------------------


def part_rerun(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Segunda corrida dentro del workflow: idempotencia y costo (sumado al del 4.2)")
    bucket = bucket_name(ctx)
    before_report = latest_report(ctx, bucket)
    before = curated_listing(ctx, bucket)
    if not before_report or not before:
        part.add("F1", "Curated antes de la segunda corrida", FALLA,
                 "no hay reporte en quality/_latest/ o curated/ está vacío: corra antes la parte D")
        return part.finish()
    placeholders0 = prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX) or {}
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["curated"])
    rows0 = int(first_row(q).get("filas") or 0)
    state0, _ = v41.get_schedule(ctx)
    part.add("F1", "Curated antes de la segunda corrida", OK,
             f"{len(before)} objetos · reporte {before_report.get('run_id')} · {rows0} filas de fact_posts en Athena · horario {state0}",
             objects=len(before), rows=rows0, schedule=state0)
    if not ctx.ask(f"¿Correr el workflow {WORKFLOW} una vez ({PREV_JOB} con {PREV_CAPACITY:g} DPU y {JOB} con "
                   f"{EXPECTED_CAPACITY:g} DPU; ≈ 0,02 USD)?"):
        part.add("F2", "corrida del workflow", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = v41.start_run(ctx, {"ta_validacion": "4.3"}, part)
    if not run_id:
        part.add("F2", "corrida del workflow", FALLA, err)
        return part.finish()
    part.note(f"{run_id}: esperando a los dos jobs (4–7 min)…")
    run = v41.wait_run(ctx, part, run_id)
    jobs = v41.job_runs_by_name(run)
    jr1 = (jobs.get(PREV_JOB) or [{}])[0]
    jr2 = (jobs.get(JOB) or [{}])[0]
    ok1, ok2 = jr1.get("JobRunState") == "SUCCEEDED", jr2.get("JobRunState") == "SUCCEEDED"
    part.add("F2", "corrida del workflow", OK if ok1 and ok2 else FALLA,
             f"{run_id}: {run.get('Status')} · {PREV_JOB} {jr1.get('JobRunState')} ({jr1.get('ExecutionTime')} s) → {JOB} "
             f"{jr2.get('JobRunState')} ({jr2.get('ExecutionTime')} s)" + (f" · arrancó al intento {tries}" if tries > 1 else "")
             + "".join(f"\n{j.get('ErrorMessage')}" for j in (jr1, jr2) if j.get("ErrorMessage")),
             run_id=run_id, job_runs={PREV_JOB: jr1.get("Id"), JOB: jr2.get("Id")},
             durations=[jr1.get("ExecutionTime"), jr2.get("ExecutionTime")])
    if not (ok1 and ok2):
        return part.finish()

    done = v42.job_done(ctx, jr2.get("Id"), started) if jr2.get("Id") else None
    report = v42.read_json_object(ctx, bucket, (done or {}).get("report") or LATEST_REPORT) or {}
    out, i = report.get("output") or {}, report.get("input") or {}
    same = files_by_key(report) == files_by_key(before_report) and bool(files_by_key(report))
    good3 = (report.get("workflow_run_id") == run_id and out.get("files_changed") == 0 and out.get("objects_written") == 0
             and out.get("objects_deleted") == 0 and same and not failed_checks(report)
             and (report.get("counts") or {}).get("fact_rows") == (before_report.get("counts") or {}).get("fact_rows"))
    part.add("F3", "reporte de la segunda corrida: nada cambió", OK if good3 else FALLA,
             f"corrida {report.get('run_id')} · archivos cambiados {out.get('files_changed')} · escritos {out.get('objects_written')} · "
             f"borrados {out.get('objects_deleted')} · {'mismos' if same else 'DISTINTOS'} sha256 que la corrida anterior en "
             f"{len(files_by_key(report))} archivos · fact_posts {(report.get('counts') or {}).get('fact_rows')} · Trusted de la misma "
             f"corrida: {i.get('trusted_rows_expected')} filas (coincide: {i.get('trusted_rows_match')})", report=report_summary(report))

    after = curated_listing(ctx, bucket) or {}
    part.add("F4", "mismos objetos y tamaños en curated/ (siete tablas)", OK if after == before else FALLA,
             f"{len(before)} → {len(after)} objetos" + ("" if after == before else
                                                        f"; nuevos {sorted(set(after) - set(before))[:5]}, faltan {sorted(set(before) - set(after))[:5]}"))
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["curated"])
    rows1, ids1 = int(first_row(q).get("filas") or 0), int(first_row(q).get("event_ids") or 0)
    part.add("F5", "Athena: mismas filas, sin duplicar", OK if rows1 == rows0 and ids1 == rows1 else FALLA,
             f"{rows0} → {rows1} filas · {ids1} event_id distintos")

    props_res = ctx.aws("glue", "get-workflow-run-properties", "--name", WORKFLOW, "--run-id", run_id)
    props = (parse_json(props_res.out) or {}).get("RunProperties") or {}
    good6 = (props.get("ta_curated_report") == report.get("report_key") and props.get("ta_curated_files_changed") == "0"
             and bool(props.get("ta_trusted_report")))
    part.add("F6", "propiedades de la corrida (para el 4.4 y el 5.2)", OK if good6 else FALLA,
             f"ta_trusted_report = {props.get('ta_trusted_report')} · ta_trusted_rows = {props.get('ta_trusted_rows')} · "
             f"ta_curated_report = {props.get('ta_curated_report')} · ta_curated_rows = {props.get('ta_curated_rows')} · "
             f"ta_curated_files_changed = {props.get('ta_curated_files_changed')}", properties=props)

    placeholders1 = prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX) or {}
    busy = [r.get("WorkflowRunId") for r in v41.workflow_runs(ctx) or [] if r.get("Status") in ("RUNNING", "STOPPING")]
    state1, _ = v41.get_schedule(ctx)
    new_marks = sorted(set(placeholders1) - set(placeholders0))
    good7 = not new_marks and not busy and state1 == state0
    part.add("F7", "sin marcas nuevas, nada en curso, horario sin cambios", OK if good7 else AVISO,
             f"curated/_ta_placeholder/: {len(placeholders0)} → {len(placeholders1)} · corridas en curso: {busy or 'ninguna'} · horario "
             f"{state0} → {state1}" + (f"; marcas nuevas {new_marks[:3]}" if new_marks else "")
             + ("" if state1 == "ENABLED" else " (al terminar: python project/scripts/validate_4_3.py on --yes)"))

    trusted_report = v42.read_json_object(ctx, bucket, props.get("ta_trusted_report") or TRUSTED_LATEST_REPORT) or {}
    exec1 = jr1.get("ExecutionTime")
    exec2 = jr2.get("ExecutionTime") or ctx.step_data("D", "D3").get("execution_s")
    trusted_usd = v42.job_cost(exec1, PREV_CAPACITY) + v42.s3_request_cost(trusted_report)
    curated_usd = v42.job_cost(exec2, EXPECTED_CAPACITY) + curated_s3_cost(report)
    marker_usd = v42.job_cost(None, EXPECTED_CAPACITY)
    per_run = trusted_usd + curated_usd
    per_day = per_run * RUNS_PER_DAY
    part.add("F8", "costo con lo medido (4.2 + 4.3, horario cada hora)", OK,
             f"{PREV_JOB}: {exec1} s × {PREV_CAPACITY:g} DPU + S3 ≈ {trusted_usd:.5f} USD · {JOB}: {exec2} s × {EXPECTED_CAPACITY:g} DPU "
             f"(mínimo 60 s) + S3 (≈ {i.get('objects')} GET, {out.get('objects_written')} PUT de datos) ≈ {curated_usd:.5f} USD "
             f"(el marcador costaba ≈ {marker_usd:.5f}) → ≈ {per_run:.4f} USD por corrida del workflow; con el horario cada hora "
             f"≈ {per_day:.3f} USD/día, ≈ {per_day * 30:.2f} USD/mes (KMS con bucket key y logs aparte, sin comprobar en la factura)",
             usd_per_run=round(per_run, 5), usd_per_day=round(per_day, 4), usd_per_month=round(per_day * 30, 2),
             trusted_usd=round(trusted_usd, 5), curated_usd=round(curated_usd, 5), execution_s=[exec1, exec2])
    return part.finish()


# --- status, off y on -------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del lakehouse y de Curated (solo lectura)")
    state, sched = v41.get_schedule(ctx)
    if state == "ENABLED":
        s1 = (OK, "ENABLED (estado normal por decisión del usuario: cada hora corren Trusted y Curated)")
    elif state == "DISABLED":
        s1 = (AVISO, "DISABLED (el estado normal es encendido: python project/scripts/validate_4_3.py on --yes)")
    else:
        s1 = (AVISO, f"{state or sched.get('error')}")
    part.add("S1", f"horario {SCHEDULE}", *s1, schedule=state)
    runs = v41.workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    jobs = {k: v for k, v in v41.running_job_runs(ctx).items() if v}
    part.add("S2", "corridas y job runs en curso", AVISO if running or jobs else OK,
             (f"corridas: {', '.join(running)}; " if running else "ninguna corrida; ") + (f"job runs: {jobs}" if jobs else "ningún job run"))
    bucket = bucket_name(ctx)
    listing = curated_listing(ctx, bucket)
    report = latest_report(ctx, bucket)
    per_table = {t: sum(1 for k in (listing or {}) if table_of(k) == t) for t in TABLES}
    part.add("S3", "Curated y su último reporte", OK if listing is not None else AVISO,
             f"curated/: {len(listing or {})} objetos {per_table} · " + (
                 f"último reporte {report.get('run_id')} ({report.get('finished_at') or '—'}): fact_posts "
                 f"{(report.get('counts') or {}).get('fact_rows')} filas, {((report.get('output') or {}).get('files_changed'))} archivos "
                 "cambiados" if report else f"sin reporte en {LATEST_REPORT} (el job todavía no corrió)"))
    temps = v41.temp_schedules(ctx)
    part.add("S4", "horarios de prueba del 4.1", AVISO if temps else OK, ", ".join(temps) if temps else "ninguno")
    marks = prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    part.add("S5", "marcas del marcador de posición en curated/_ta_placeholder/", OK if not marks else AVISO,
             f"{len(marks or {})} objetos" + (" (restos del marcador del 4.1: off los borra)" if marks else ""))
    return part.finish()


def part_off(ctx: Ctx) -> int:
    """El apagado del 4.1 (horario, corridas, job runs, horarios de prueba y marcas), con la evidencia del 4.3.

    Se usa **mientras se valida**: al terminar, ``on`` deja el horario encendido (estado normal)."""
    return v41.part_off(ctx)


def part_on(ctx: Ctx) -> int:
    part = Part(ctx, "on", "Encender el horario del lakehouse (estado normal, decisión del usuario)")
    f8 = ctx.step_data("F", "F8")
    per_day = f8.get("usd_per_day") or round(ESTIMATED_USD_PER_RUN * RUNS_PER_DAY, 3)
    source = "medido en F8" if f8.get("usd_per_day") else "estimado"
    if not ctx.ask(f"¿Encender {SCHEDULE} (arranca {WORKFLOW} cada hora: Trusted y Curated; ≈ {per_day} USD/día, "
                   f"≈ {round(per_day * 30, 2)} USD/mes, {source})?"):
        part.add("N1", "encender el horario", AVISO, "no confirmado")
        return part.finish()
    ok, detail = v41.set_schedule_state(ctx, "ENABLED")
    part.add("N1", "encender el horario", OK if ok else FALLA, detail + "; se apaga con: python project/scripts/validate_4_3.py off")
    state, sched = v41.get_schedule(ctx)
    target = sched.get("Target") or {}
    retries = (target.get("RetryPolicy") or {}).get("MaximumRetryAttempts")
    good = state == "ENABLED" and sched.get("ScheduleExpression") == "rate(1 hour)" and retries == 0
    part.add("N2", "definición del horario intacta", OK if good else FALLA,
             f"{state} · {sched.get('ScheduleExpression')} · {sched.get('ScheduleExpressionTimezone')} · reintentos {retries}",
             schedule=state, usd_per_day=per_day)
    return part.finish()


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
    "on": part_on,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_4_3.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (solo para el estado de status.py)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--bucket", default=None, help="bucket del lake (si no, ta-dev-{cuenta}-lake)")
    parser.add_argument("--cache", default=None, help="L/A8: caché del profiling (si no, TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
    parser.add_argument("--python", default=None, help="L: intérprete del job (p. ej. un venv con Python 3.9 y pyarrow 7)")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan (pide confirmación)")
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
    printer(f"Lote 4.3 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si quedó algo corriendo: python project/scripts/validate_4_3.py off; y al terminar: "
                "validate_4_3.py on --yes (el horario va encendido)")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
