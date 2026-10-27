"""Validación automática del Lote 4.4 (HU-028, aristas): ``docs/validation/validation-4-4.md``.

    python project/scripts/validate_4_4.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: el job contra el Trusted real de la caché local y las
aristas recalculadas aparte con pyarrow, sin nube), ``terraform`` (C: script y descripción del job;
``--apply``), ``run`` (D: una corrida real del job y la conciliación de las aristas), ``athena`` (E: las dos
tablas de aristas recalculadas en SQL, sus llaves foráneas y dos consultas de ejemplo), ``rerun`` (F: segunda
corrida dentro del workflow, idempotencia y costo); además ``status`` (solo lectura), ``off`` (el apagado del
4.1, mientras se valida) y ``on`` (vuelve a encender el horario: **el estado normal es encendido**). Cada
parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su resumen en
``project/evidencia/validacion_4_4.json``.

Las aristas las escribe el mismo job del 4.3 (``ta-job-trusted-to-curated``, versión 1.1; ADR-27), así que
este validador reutiliza casi todo de ``ops/validate43.py`` (``check_job``, ``run_local_job``,
``local_trusted_root``, ``curated_schema_problems``, ``layout_problems``, ``prefix_listing``,
``failed_checks``, ``report_summary``, ``files_by_key``, ``curated_s3_cost``, ``first_row``, ``ints``) y,
a través de él, de ``validate41``/``validate42``/``validate32``. Lo propio: las nueve tablas (las siete del
4.3 más las dos aristas), el recálculo de las aristas y su SQL. Lo que cambia la nube (``terraform apply``,
correr el job o el workflow, el horario) pide confirmación salvo con ``--yes``.
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
from ops import validate43 as v43
from ops.validate32 import (
    AVISO,
    FALLA,
    OK,
    OMITIDO,
    WSL_HINT,
    Part,
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

PROJECT = v43.PROJECT
SRC = v43.SRC
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_4_4.json"
AWS_ENV = v43.AWS_ENV
JOB_FILE = v43.JOB_FILE
WORKFLOW = v43.WORKFLOW
JOB_KEY = v43.JOB_KEY
JOB = v43.JOB
PREV_JOB = v43.PREV_JOB
SCHEDULE = v43.SCHEDULE
GLUE_OUTPUT_LOG = v43.GLUE_OUTPUT_LOG
GLUE_ERROR_LOG = v43.GLUE_ERROR_LOG
TRUSTED_PREFIX = v43.TRUSTED_PREFIX
CURATED_PREFIX = v43.CURATED_PREFIX
EDGE_TABLES = ("edges_user_brand", "edges_hashtag_cooccurrence")
TABLES = v43.TABLES + EDGE_TABLES
EDGE_PREFIX = v43.EDGE_PREFIX
PLACEHOLDER_PREFIX = v43.PLACEHOLDER_PREFIX
LATEST_REPORT = v43.LATEST_REPORT
TRUSTED_LATEST_REPORT = v43.TRUSTED_LATEST_REPORT
PART_FILE = v43.PART_FILE
JOB_VERSION = "trusted-to-curated@1.1"
N_CHECKS = 16  # las 11 del 4.3 y 5 de las aristas
EXPECTED_CAPACITY = v43.EXPECTED_CAPACITY
PREV_CAPACITY = v43.PREV_CAPACITY
PEAK_WARN_MB = v43.PEAK_WARN_MB

# Terraform: solo cambian el script y la descripción del job (el catálogo y la proyección ya existían, 4.1).
TERRAFORM_TARGETS = ["module.glue_workflow"]
EXPECTED_UPDATES = set(v43.EXPECTED_UPDATES)

# Lo medido con el Trusted del 26-oct-2026 (exportador en pausa) en local: si Trusted cambia, la conciliación
# manda y estas cifras solo dan AVISO.
TODAY = dict(v43.TODAY, objects=189, edges_user_brand=15395, edges_hashtag_cooccurrence=136, mentions=233395,
             cooccurrences=44578, hashtags_distinct=25)

# Pruebas esperadas (B): las cuenta docs/validation/validation-4-4.md.
B1_EXPECTED = 127
B2_EXPECTED = 27
B4_EXPECTED = v43.B2_EXPECTED
B5_EXPECTED = v43.B4_EXPECTED
B6_EXPECTED = v43.B5_EXPECTED

RUNS_PER_DAY = v43.RUNS_PER_DAY
KPI_LATENCY_MS = v43.KPI_LATENCY_MS
#: Estimado antes de validar: lo medido en F8 del 4.3 (el job sigue por debajo del mínimo de 60 s cobrados).
ESTIMATED_USD_PER_RUN = 0.0196

LOT_FILES = [
    "src/lakehouse/glue_jobs/trusted_to_curated.py",
    "src/lakehouse/tests/test_lake_trusted_to_curated.py",
    "src/lakehouse/tests/test_lake_glue_jobs.py",
    "infra/aws/environments/dev/lakehouse.tf",
    "src/ops/validate44.py",
    "src/ops/tests/test_ops_validate44.py",
    "scripts/validate_4_4.py",
]
PREVIOUS_FILES = [*v43.PREVIOUS_FILES, "src/ops/validate43.py", "src/ops/tests/test_ops_validate43.py"]


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v42.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "4.4", "hu": ["HU-028"], "parts": {}}


# --- utilidades puras ---------------------------------------------------------------------------


def table_of(key: str) -> str | None:
    """La tabla de Curated de una clave (``curated/{tabla}/…``, las nueve) o ``None``."""
    m = re.match(r"^curated/([^/]+)/", key)
    return m.group(1) if m and m.group(1) in TABLES else None


def layout_problems(keys: list[str]) -> list[str]:
    """``validate43.layout_problems`` (las siete del 4.3) más las aristas: un solo ``curated/{arista}/part-00000.parquet``."""
    bad = v43.layout_problems(keys)
    for table in EDGE_TABLES:
        mine = [k for k in keys if k.startswith(f"curated/{table}/") and not k.endswith("/")]
        bad += [f"archivo de más: {k}" for k in mine if k != f"curated/{table}/{PART_FILE}"]
        if f"curated/{table}/{PART_FILE}" not in mine:
            bad.append(f"falta la tabla {table}")
    return bad


def compare_today(values: dict[str, Any]) -> list[str]:
    return [f"{k} {values.get(k)} (el 26-oct: {TODAY[k]})" for k in values if k in TODAY and values.get(k) != TODAY[k]]


def edge_checks(report: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in ((report or {}).get("checks") or {}).items() if "edge" in k}


def edges_from_trusted(root: Path) -> dict[str, Any]:
    """Las aristas recalculadas desde un Trusted local con pyarrow (``group_by``/``join``), aparte del job.

    ``{user_brand: {(user_hash, marca): filas}, pairs: {(a, b): filas}, mentions, cooccurrences, hashtags, rows}``.
    Mismas reglas (ADR-27): cada marca de ``brands`` y cada hashtag distinto cuentan una vez por fila; los pares
    solo con ``a < b``; las filas sin ``user_hash`` no tienen arista usuario → marca."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    files = sorted((root / "trusted" / "posts").rglob("*.parquet"))
    if not files:
        return {"user_brand": {}, "pairs": {}, "mentions": 0, "cooccurrences": 0, "hashtags": 0, "rows": 0}
    t = pa.concat_tables([pq.read_table(f, columns=["user_hash", "brands", "hashtags"]) for f in files])
    t = t.append_column("row", pa.array(range(t.num_rows), pa.int64()))
    u = t.filter(pc.is_valid(t["user_hash"]))
    idx = pc.list_parent_indices(u["brands"])
    flat = pa.table({"row": pc.take(u["row"], idx), "user_hash": pc.take(u["user_hash"], idx),
                     "brand": pc.list_flatten(u["brands"])})
    flat = flat.filter(pc.is_valid(flat["brand"])).group_by(["row", "user_hash", "brand"]).aggregate([])
    ub = flat.group_by(["user_hash", "brand"]).aggregate([("row", "count")])
    user_brand = {(r["user_hash"], r["brand"]): r["row_count"] for r in ub.to_pylist()}
    idx = pc.list_parent_indices(t["hashtags"])
    tags = pa.table({"row": pc.take(t["row"], idx), "tag": pc.list_flatten(t["hashtags"])})
    tags = tags.filter(pc.and_(pc.is_valid(tags["tag"]), pc.not_equal(tags["tag"], "")))
    tags = tags.group_by(["row", "tag"]).aggregate([])
    joined = tags.join(tags, keys="row", join_type="inner", right_suffix="_b")
    joined = joined.filter(pc.less(joined["tag"], joined["tag_b"]))
    co = joined.group_by(["tag", "tag_b"]).aggregate([("row", "count")])
    pairs = {(r["tag"], r["tag_b"]): r["row_count"] for r in co.to_pylist()}
    return {"user_brand": user_brand, "pairs": pairs, "mentions": sum(user_brand.values()),
            "cooccurrences": sum(pairs.values()), "hashtags": tags.group_by("tag").aggregate([]).num_rows, "rows": t.num_rows}


def edges_from_parquet(user_brand: bytes, pairs: bytes) -> tuple[dict, dict]:
    """Los Parquet de las aristas del job → ``({(user_hash, marca): weight}, {(a, b): weight})``."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    ub = pq.read_table(pa.BufferReader(user_brand)).to_pylist()
    ht = pq.read_table(pa.BufferReader(pairs)).to_pylist()
    return ({(r["user_hash"], r["brand"]): r["weight"] for r in ub},
            {(r["hashtag_a"], r["hashtag_b"]): r["weight"] for r in ht})


def edge_diff(want: dict, got: dict) -> list[str]:
    """Diferencias sin mostrar ``user_hash`` (el repositorio es público): solo cuántas."""
    missing = len(set(want) - set(got))
    extra = len(set(got) - set(want))
    weight = sum(1 for k in set(want) & set(got) if want[k] != got[k])
    return [f"{n} {what}" for n, what in ((missing, "faltan"), (extra, "sobran"), (weight, "con otro peso")) if n]


# --- SQL de la parte E -------------------------------------------------------------------------------

# edges_user_brand recalculada desde fact_posts (tiene user_hash y brands): una mención = una marca distinta de brands.
UB_SQL = """WITH m AS (
  SELECT f.user_hash, b AS brand, COUNT(*) AS n
  FROM ta_curated.fact_posts f CROSS JOIN UNNEST(array_distinct(f.brands)) AS t (b)
  WHERE f.user_hash IS NOT NULL AND b IS NOT NULL GROUP BY 1, 2),
e AS (SELECT user_hash, brand, weight FROM ta_curated.edges_user_brand)
SELECT
  count_if(e.user_hash IS NULL OR m.user_hash IS NULL) AS claves_distintas,
  count_if(e.weight <> m.n) AS pesos_distintos,
  count(e.user_hash) AS pares,
  sum(e.weight) AS peso_total,
  sum(m.n) AS menciones,
  (SELECT sum(cardinality(brands)) FROM ta_curated.fact_posts WHERE user_hash IS NOT NULL) AS marcas_en_brands
FROM e FULL OUTER JOIN m ON e.user_hash = m.user_hash AND e.brand = m.brand"""

# Llaves foráneas, nulos, orden canónico y unicidad de las dos aristas.
LINKS_SQL = """SELECT
  (SELECT COUNT(*) FROM ta_curated.edges_user_brand e LEFT JOIN ta_curated.dim_user u ON u.user_hash = e.user_hash
     WHERE u.user_hash IS NULL) AS sin_usuario,
  (SELECT COUNT(*) FROM ta_curated.edges_user_brand e LEFT JOIN ta_curated.dim_brand b ON b.brand = e.brand
     WHERE b.brand IS NULL) AS sin_marca,
  (SELECT count_if(user_hash IS NULL OR brand IS NULL OR weight IS NULL OR weight < 1) FROM ta_curated.edges_user_brand)
    AS nulos_usuario_marca,
  (SELECT COUNT(*) - COUNT(DISTINCT concat(user_hash, '|', brand)) FROM ta_curated.edges_user_brand) AS pares_repetidos_ub,
  (SELECT count_if(hashtag_a IS NULL OR hashtag_b IS NULL OR weight IS NULL OR weight < 1)
     FROM ta_curated.edges_hashtag_cooccurrence) AS nulos_hashtags,
  (SELECT count_if(NOT hashtag_a < hashtag_b) FROM ta_curated.edges_hashtag_cooccurrence) AS fuera_de_orden,
  (SELECT count_if(NOT regexp_like(hashtag_a, '^[a-z0-9_]{1,100}$') OR NOT regexp_like(hashtag_b, '^[a-z0-9_]{1,100}$'))
     FROM ta_curated.edges_hashtag_cooccurrence) AS no_canonicos,
  (SELECT COUNT(*) - COUNT(DISTINCT concat(hashtag_a, '|', hashtag_b)) FROM ta_curated.edges_hashtag_cooccurrence)
    AS pares_repetidos_ht"""

# edges_hashtag_cooccurrence recalculada desde ta_trusted.posts: fact_posts no tiene hashtags (§3.22.1, v2.38).
HT_SQL = """WITH t AS (
  SELECT p.event_id, h FROM ta_trusted.posts p CROSS JOIN UNNEST(array_distinct(p.hashtags)) AS x (h)
  WHERE h IS NOT NULL AND h <> ''),
c AS (SELECT a.h AS hashtag_a, b.h AS hashtag_b, COUNT(*) AS n
      FROM t a JOIN t b ON a.event_id = b.event_id AND a.h < b.h GROUP BY 1, 2),
e AS (SELECT hashtag_a, hashtag_b, weight FROM ta_curated.edges_hashtag_cooccurrence)
SELECT
  count_if(e.hashtag_a IS NULL OR c.hashtag_a IS NULL) AS claves_distintas,
  count_if(e.weight <> c.n) AS pesos_distintos,
  count(e.hashtag_a) AS pares,
  sum(e.weight) AS peso_total,
  sum(c.n) AS coocurrencias,
  (SELECT sum(k * (k - 1) / 2) FROM (SELECT cardinality(array_distinct(filter(hashtags, x -> x IS NOT NULL AND x <> '')))
     AS k FROM ta_trusted.posts)) AS pares_por_fila,
  (SELECT COUNT(DISTINCT h) FROM t) AS hashtags
FROM e FULL OUTER JOIN c ON e.hashtag_a = c.hashtag_a AND e.hashtag_b = c.hashtag_b"""

# Dos consultas de ejemplo de HU-028 («análisis de relaciones» sin base de grafos; sin mostrar user_hash).
EXAMPLE_SQL = {
    "pares-de-hashtags-mas-frecuentes": """SELECT hashtag_a, hashtag_b, weight
FROM ta_curated.edges_hashtag_cooccurrence ORDER BY weight DESC, hashtag_a, hashtag_b LIMIT 10""",
    "usuarios-por-numero-de-marcas": """SELECT n_marcas, COUNT(*) AS usuarios, SUM(menciones) AS menciones
FROM (SELECT user_hash, COUNT(*) AS n_marcas, SUM(weight) AS menciones FROM ta_curated.edges_user_brand GROUP BY user_hash)
GROUP BY n_marcas ORDER BY n_marcas""",
}

COUNT_SQL = """SELECT
  (SELECT COUNT(*) FROM ta_curated.fact_posts) AS fact,
  (SELECT COUNT(*) FROM ta_curated.edges_user_brand) AS pares_ub,
  (SELECT sum(weight) FROM ta_curated.edges_user_brand) AS peso_ub,
  (SELECT COUNT(*) FROM ta_curated.edges_hashtag_cooccurrence) AS pares_ht,
  (SELECT sum(weight) FROM ta_curated.edges_hashtag_cooccurrence) AS peso_ht"""


# --- piezas de AWS ---------------------------------------------------------------------------------


def bucket_name(ctx: Ctx) -> str:
    return v42.bucket_name(ctx)


def curated_listing(ctx: Ctx, bucket: str) -> dict[str, int] | None:
    """Claves y tamaños de las nueve tablas (sin marcas ni otras carpetas de ``curated/``)."""
    keys = v41.s3_keys(ctx, bucket, CURATED_PREFIX)
    if keys is None:
        return None
    return {k.get("Key"): int(k.get("Size") or 0) for k in keys if table_of(k.get("Key") or "")}


def latest_report(ctx: Ctx, bucket: str) -> dict[str, Any]:
    return v42.read_json_object(ctx, bucket, LATEST_REPORT) or {}


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
        names = {s.name for s in catalog_spec.load_contract_tables() if s.database == "ta_curated"}
        problems += [f"falta ta_curated.{t} en contracts/athena/curated.sql" for t in EDGE_TABLES if t not in names]
    except Exception as exc:  # noqa: BLE001
        problems = [f"{type(exc).__name__}: {exc}"]
    part.add("A4", "tables.json al día con contracts/athena/ (aristas incluidas)", FALLA if problems else OK,
             "\n".join(problems) if problems else "edges_user_brand y edges_hashtag_cooccurrence en el catálogo = las DDL")
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
                "python project/scripts/validate_4_4.py off; al terminar: validate_4_4.py on --yes)")
    elif running or jobs:
        note = " (espere a que terminen o: python project/scripts/validate_4_4.py off)"
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
        root, label = v43.local_trusted_root(ctx)
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
        ("B1", "pytest src/lakehouse (job con aristas)", ["src/lakehouse", "-q", "-rs"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate44.py", ["src/ops/tests/test_ops_validate44.py", "-q"], B2_EXPECTED),
        ("B3", "pytest contracts/tests", ["contracts/tests", "-q"], 18),
        ("B4", "regresión: test_ops_validate43.py (piezas reutilizadas)", ["src/ops/tests/test_ops_validate43.py", "-q"], B4_EXPECTED),
        ("B5", "regresión: test_ops_validate42.py", ["src/ops/tests/test_ops_validate42.py", "-q"], B5_EXPECTED),
        ("B6", "regresión: test_ops_validate41.py", ["src/ops/tests/test_ops_validate41.py", "-q"], B6_EXPECTED),
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


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "El job en local contra el Trusted real y las aristas recalculadas aparte (sin nube)")
    bucket = getattr(ctx.options, "bucket", None) or "ta-dev-648617469176-lake"
    try:
        src, label = v43.local_trusted_root(ctx)
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
    part.note("L1: el job lee los Parquet de Trusted y construye Curated con las aristas (≈ 5 s y ~0,4–0,6 GB)…")
    res, done = v43.run_local_job(ctx, python, src, dst, bucket)
    if not res.ok or not done:
        part.add("L1", "corrida local", FALLA, res.tail(8))
        return part.finish()
    try:
        report = json.loads((dst / "quality" / "_latest" / "trusted_to_curated.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        report = {}
    checks = report.get("checks") or {}
    bad = v43.failed_checks(report)
    tables = done.get("tables") or {}
    values = {"trusted_rows": done.get("rows_read"), "mentions": done.get("mentions"), "cooccurrences": done.get("cooccurrences"),
              "hashtags_distinct": done.get("hashtags_distinct"), **{k: tables.get(k) for k in EDGE_TABLES}}
    diffs = compare_today(values)
    env = done.get("environment") or {}
    good = done.get("conciliation_ok") and len(checks) == N_CHECKS and not bad and report.get("version") == JOB_VERSION
    part.add("L1", "corrida local (aristas y conciliación)", (AVISO if diffs else OK) if good else FALLA,
             f"Trusted {done.get('rows_read')} → fact_posts {done.get('fact_rows')} · edges_user_brand {tables.get('edges_user_brand')} "
             f"pares, {done.get('mentions')} menciones · edges_hashtag_cooccurrence {tables.get('edges_hashtag_cooccurrence')} pares, "
             f"{done.get('cooccurrences')} coocurrencias, {done.get('hashtags_distinct')} hashtags · {len(checks)} comprobaciones "
             f"({'todas OK' if not bad else 'FALLAN ' + ', '.join(bad)}) · versión {report.get('version')} · archivos cambiados "
             f"{done.get('files_changed')} (2 = solo las aristas, la primera vez sobre la salida del 4.3) · {done.get('duration_s')} s, "
             f"{done.get('cpu_seconds')} s de CPU, pico {done.get('peak_rss_mb')} MB · Python {env.get('python')}, pyarrow "
             f"{env.get('pyarrow')}" + ("\ndistinto del Trusted del 26-oct: " + "; ".join(diffs) if diffs else ""),
             done=done, edge_checks=edge_checks(report))
    res2, done2 = v43.run_local_job(ctx, python, src, dst, bucket)
    idem = bool(res2.ok and done2 and done2.get("objects_written") == 0 and done2.get("files_changed") == 0
                and done2.get("objects_deleted") == 0 and done2.get("mentions") == done.get("mentions"))
    part.add("L2", "segunda corrida local (idempotencia)", OK if idem else FALLA,
             (f"escritos {done2.get('objects_written')} · sin cambios {done2.get('objects_unchanged')} · archivos cambiados "
              f"{done2.get('files_changed')} · borrados {done2.get('objects_deleted')}") if done2 else res2.tail(6), done=done2)
    problems, counted = [], {}
    for table in TABLES:
        files = sorted((dst / "curated" / table).rglob("*.parquet"))
        if not files:
            problems.append(f"{table}: sin archivos")
            continue
        p, _ = v43.curated_schema_problems(files[0].read_bytes(), table)
        problems += p
        counted[table] = len(files)
    part.add("L3", "esquema de las nueve tablas = contracts/athena/curated.sql", FALLA if problems else OK,
             "\n".join(problems[:12]) if problems else f"9 tablas (archivos: {counted}); aristas: string, string, bigint → int64")
    part.note("L4: recalculando las aristas desde el Trusted local con pyarrow (group_by y join)…")
    try:
        want = edges_from_trusted(src)
        got_ub, got_ht = edges_from_parquet((dst / "curated" / "edges_user_brand" / PART_FILE).read_bytes(),
                                            (dst / "curated" / "edges_hashtag_cooccurrence" / PART_FILE).read_bytes())
        diffs4 = [f"edges_user_brand: {d}" for d in edge_diff(want["user_brand"], got_ub)]
        diffs4 += [f"edges_hashtag_cooccurrence: {d}" for d in edge_diff(want["pairs"], got_ht)]
    except Exception as exc:  # noqa: BLE001
        want, diffs4 = {}, [f"{type(exc).__name__}: {exc}"]
    part.add("L4", "aristas = recalculadas aparte con pyarrow", FALLA if diffs4 else OK,
             "\n".join(diffs4) if diffs4 else f"{len(want['user_brand'])} pares usuario–marca con los mismos pesos (Σ "
             f"{want['mentions']} = menciones del job {done.get('mentions')}) · {len(want['pairs'])} pares de hashtags con los mismos "
             f"pesos (Σ {want['cooccurrences']}) · {want['hashtags']} hashtags · {want['rows']} filas de Trusted",
             recomputed={k: want.get(k) for k in ("mentions", "cooccurrences", "hashtags", "rows")} if want else {})
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (script y descripción del job)")
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
    plan_path = EVIDENCE_DIR / "tfplan_4_4"
    title = "plan (-target=module.glue_workflow)"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", title, FALLA, res.err or res.tail(8))
        return part.finish()
    updates = set(summary["update"])
    extra = (updates - EXPECTED_UPDATES) | set(summary["create"] + summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", title, FALLA, f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    job_addr = f'module.glue_workflow.aws_glue_job.this["{JOB_KEY}"]'
    bad_job = [d for d in (summary.get("diffs") or {}).get(job_addr, []) if not d.startswith("description")]
    if bad_job:
        part.add("C2", title, FALLA, f"el job cambia algo más que description: {bad_job}", **summary)
        return part.finish()
    part.add("C2", title, OK, plan_line(summary) + (" (ya aplicado)" if not updates else
             " (esperado: el script y la descripción del job a cambiar; nada a crear ni a destruir)")
             + diff_text(summary, updates), **summary)
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
        res, again = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, EVIDENCE_DIR / "tfplan_4_4_check")
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


def part_run(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Corrida real de ta-job-trusted-to-curated con las aristas y su conciliación")
    bucket = bucket_name(ctx)
    state, _ = v41.get_schedule(ctx)
    if state == "ENABLED":
        part.add("D0", "horario", AVISO, "ENABLED: una corrida del horario puede caer en medio (la parte espera y reintenta); "
                 "mientras valida: python project/scripts/validate_4_4.py off, y al terminar on --yes")
    problems = v43.check_job(ctx, bucket)
    part.add("D1", f"{JOB}: Python shell 3.9, {EXPECTED_CAPACITY:g} DPU, script de S3 = repo (versión 1.1)", FALLA if problems else OK,
             "\n".join(problems) if problems else f"pythonshell 3.9 · {EXPECTED_CAPACITY:g} DPU (1 GB) · library-set analytics · "
             "script de S3 idéntico al repo")
    if problems:
        return part.finish()

    part.note("D2: contando las filas de Trusted por los pies de sus Parquet (≈ 543 GET)…")
    trusted = v42.s3_rows(ctx, bucket, TRUSTED_PREFIX)
    placeholders_before = v43.prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    before = curated_listing(ctx, bucket) or {}
    edges_before = {k: s for k, s in before.items() if k.startswith(EDGE_PREFIX)}
    part.add("D2", "Trusted y aristas antes de la corrida", FALLA if trusted.get("error_count") else OK,
             f"trusted/posts/: {trusted.get('objects')} objetos y {trusted.get('rows')} filas"
             + ("" if trusted.get("rows") == TODAY["trusted_rows"] else f" (el 26-oct eran {TODAY['trusted_rows']}: ¿cambió Raw?)")
             + f" · curated/ (nueve tablas): {len(before)} objetos, aristas {len(edges_before)} · curated/_ta_placeholder/: "
             f"{len(placeholders_before or {})}",
             trusted={k: trusted.get(k) for k in ("objects", "rows", "error_count", "seconds")},
             objects_before=len(before), edges_before=len(edges_before))

    if not ctx.ask(f"¿Correr {JOB} una vez ({EXPECTED_CAPACITY:g} DPU, ≈ 0,001 USD; reescribe lo que cambie en curated/, "
                   "aristas incluidas)?"):
        part.add("D3", "corrida del job", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = v42.start_job_run(ctx, {"--ta_validacion": "4.4"}, part, job=JOB)
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
    tables_done = (done or {}).get("tables") or {}
    part.add("D4", f"línea glue_job_done en {GLUE_OUTPUT_LOG}", (AVISO if peak > PEAK_WARN_MB else OK) if done else FALLA,
             (f"leídas {done.get('rows_read')} · fact_posts {done.get('fact_rows')} · edges_user_brand "
              f"{tables_done.get('edges_user_brand')} ({done.get('mentions')} menciones) · edges_hashtag_cooccurrence "
              f"{tables_done.get('edges_hashtag_cooccurrence')} ({done.get('cooccurrences')} coocurrencias, {done.get('hashtags_distinct')} "
              f"hashtags) · lectura {done.get('read_seconds')} s · total {done.get('duration_s')} s · CPU {done.get('cpu_seconds')} s · "
              f"pico de memoria {peak} MB de 1024 · Python {env.get('python')}, pyarrow {env.get('pyarrow')}, boto3 {env.get('boto3')}"
              + (f"\npico > {PEAK_WARN_MB} MB: considere 1 DPU (glue_job_settings)" if peak > PEAK_WARN_MB else "")) if done else
             f"sin glue_job_done en el flujo {run_id} (¿tardan los logs?)", done=done)
    report_key = (done or {}).get("report") or LATEST_REPORT
    report = v42.read_json_object(ctx, bucket, report_key)
    latest = v42.read_json_object(ctx, bucket, LATEST_REPORT)
    same = bool(report and latest and report.get("run_id") == latest.get("run_id"))
    good5 = bool(report and same and report.get("version") == JOB_VERSION)
    part.add("D5", "reporte de la corrida en quality/ (versión 1.1)", OK if good5 else FALLA,
             f"{report_key} y {LATEST_REPORT} (corrida {report.get('run_id')}, {report.get('version')}, esquema {report.get('schema')})"
             if report else f"no se pudo leer {report_key}", report=v43.report_summary(report or {}))
    if not report:
        return part.finish()

    c, i = report.get("counts") or {}, report.get("input") or {}
    checks = report.get("checks") or {}
    bad = v43.failed_checks(report)
    rows_ok = trusted.get("rows") == i.get("rows") == c.get("fact_rows")
    tables = {k: v.get("rows") for k, v in (report.get("tables") or {}).items()}
    values = {"trusted_rows": i.get("rows"), "mentions": c.get("mentions"), "cooccurrences": c.get("cooccurrences"),
              "hashtags_distinct": c.get("hashtags_distinct"), **{k: tables.get(k) for k in TABLES if k != "fact_posts"}}
    diffs = compare_today(values) if trusted.get("rows") == TODAY["trusted_rows"] else []
    status = FALLA if (bad or not rows_ok or len(checks) != N_CHECKS) else (AVISO if diffs else OK)
    ec = edge_checks(report)
    part.add("D6", f"conciliación ({len(checks)} comprobaciones; {len(ec)} de aristas)", status,
             f"Trusted {trusted.get('rows')} (pies de S3) = leídas {i.get('rows')} = fact_posts {c.get('fact_rows')}"
             + f"\nedges_user_brand: Σ weight {(checks.get('edges_user_brand_weight_eq_mentions') or {}).get('observed')} = menciones "
             f"{c.get('mentions')} ({tables.get('edges_user_brand')} pares; por marca {c.get('mentions_by_brand')}) · llaves en dim_user "
             f"y dim_brand OK · filas con usuario sin marcas {c.get('rows_without_brands')}"
             + f"\nedges_hashtag_cooccurrence: Σ weight {(checks.get('edges_hashtag_weight_eq_pairs') or {}).get('observed')} = Σ C(k, 2) "
             f"{c.get('cooccurrences')} ({tables.get('edges_hashtag_cooccurrence')} pares, {c.get('hashtags_distinct')} hashtags; filas por "
             f"nº de hashtags {c.get('rows_by_hashtag_count')}) · hashtag_a < hashtag_b en todas"
             + (f"\nno cuadra: {json.dumps(bad, sort_keys=True)}" if bad else "")
             + ("" if len(checks) == N_CHECKS else f"\n{len(checks)} comprobaciones (esperadas {N_CHECKS}: ¿script viejo?)")
             + ("" if rows_ok else f"\nTrusted {trusted.get('rows')}, leídas {i.get('rows')}, fact_posts {c.get('fact_rows')}")
             + ("\ndistinto del Trusted del 26-oct: " + "; ".join(diffs) if diffs else ""),
             checks=ec, tables=tables)

    part.note("D7: contando las filas de curated/ por los pies de sus Parquet…")
    listing = curated_listing(ctx, bucket) or {}
    layout = layout_problems(list(listing))
    s3_tables = {t: v42.s3_rows(ctx, bucket, f"{CURATED_PREFIX}{t}/").get("rows") for t in TABLES}
    placeholders_after = v43.prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    out = report.get("output") or {}
    changed = out.get("changed_keys") or []
    not_edges = [k for k in changed if not k.startswith(EDGE_PREFIX)]
    good7 = (s3_tables == tables and len(listing) == out.get("objects") and not layout and not not_edges
             and (placeholders_after or {}).keys() <= (placeholders_before or {}).keys())
    part.add("D7", "curated/ en S3 (filas, layout; solo cambian las aristas)", OK if good7 else FALLA,
             f"{len(before)} → {len(listing)} objetos ({out.get('objects')} en el reporte) · filas por tabla en los pies {s3_tables} · "
             f"archivos cambiados {len(changed)} ({'solo aristas' if not not_edges else f'{len(not_edges)} fuera de las aristas'}) · "
             f"escritos {out.get('objects_written')}, sin cambios {out.get('objects_unchanged')}, borrados {out.get('objects_deleted')} · "
             f"{out.get('bytes')} bytes · curated/_ta_placeholder/: {len(placeholders_before or {})} → {len(placeholders_after or {})}"
             + (("\n" + "\n".join(layout[:10])) if layout else "")
             + (("\ncambiaron tablas del 4.3: " + ", ".join(not_edges[:5])) if not_edges else "")
             + ("" if s3_tables == tables else f"\nel reporte dice {tables}"),
             s3_tables=s3_tables, layout=layout[:20], files_changed=len(changed), changed_outside_edges=len(not_edges))

    problems, sizes = [], {}
    for table in TABLES:
        keys = sorted((k for k in listing if table_of(k) == table), key=lambda k: listing[k])
        if not keys:
            problems.append(f"{table}: sin objetos")
            continue
        try:
            p, n = v43.curated_schema_problems((ctx.s3_get or v42.default_s3_get(ctx))(bucket, keys[0]), table)
        except Exception as exc:  # noqa: BLE001
            p, n = [f"{table}: {type(exc).__name__}: {exc}"], 0
        problems += p
        sizes[table] = n
    part.add("D8", "esquema de un Parquet de cada tabla = contrato (nueve)", FALLA if problems else OK,
             "\n".join(problems[:12]) if problems else f"9 tablas con las columnas y tipos de contracts/athena/curated.sql "
             f"(filas del archivo leído: {sizes})")
    return part.finish()


# --- Parte E · Athena ---------------------------------------------------------------------------


def part_athena(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Athena: aristas recalculadas, llaves y consultas de ejemplo (HU-028)")
    bucket = bucket_name(ctx)
    report = latest_report(ctx, bucket)
    c = report.get("counts") or {}
    db = v41.DATABASES["curated"]
    scanned: list[int] = []

    def query(sql: str) -> dict[str, Any]:
        q = v41.athena_query(ctx, sql, db)
        scanned.append(q.get("scanned_bytes", 0))
        return q

    q = query(UB_SQL)
    r = v43.ints(v43.first_row(q)) if q["state"] == "SUCCEEDED" else {}
    bad = [f"{k} = {r.get(k)}" for k in ("claves_distintas", "pesos_distintos") if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    if r and not (r.get("peso_total") == r.get("menciones") == r.get("marcas_en_brands")):
        bad.append(f"Σ weight {r.get('peso_total')}, menciones recalculadas {r.get('menciones')}, Σ cardinality(brands) "
                   f"{r.get('marcas_en_brands')}")
    if r and c.get("mentions") is not None and r.get("peso_total") != c.get("mentions"):
        bad.append(f"Σ weight {r.get('peso_total')} ≠ menciones del reporte {c.get('mentions')}")
    part.add("E1", "edges_user_brand = recalculada desde fact_posts (cada marca de brands, una vez por fila)", FALLA if bad else OK,
             "\n".join(bad) if bad else f"{r.get('pares')} pares, 0 claves o pesos distintos · Σ weight {r.get('peso_total')} = "
             f"menciones {r.get('menciones')} = Σ cardinality(brands) {r.get('marcas_en_brands')} (= reporte)", counts=r)

    q = query(LINKS_SQL)
    r = v43.ints(v43.first_row(q)) if q["state"] == "SUCCEEDED" else {}
    zero = ("sin_usuario", "sin_marca", "nulos_usuario_marca", "pares_repetidos_ub", "nulos_hashtags", "fuera_de_orden",
            "no_canonicos", "pares_repetidos_ht")
    bad = [f"{k} = {r.get(k)}" for k in zero if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    part.add("E2", "llaves foráneas, nulos, orden hashtag_a < hashtag_b y pares únicos", FALLA if bad else OK,
             "\n".join(bad) if bad else "0 aristas sin su usuario en dim_user o su marca en dim_brand · 0 nulos o pesos < 1 · 0 pares "
             "fuera de orden, no canónicos o repetidos", counts=r)

    q = query(HT_SQL)
    r = v43.ints(v43.first_row(q)) if q["state"] == "SUCCEEDED" else {}
    bad = [f"{k} = {r.get(k)}" for k in ("claves_distintas", "pesos_distintos") if r.get(k)] if r else [f"{q['state']}: {q['reason']}"]
    if r and not (r.get("peso_total") == r.get("coocurrencias") == r.get("pares_por_fila")):
        bad.append(f"Σ weight {r.get('peso_total')}, recalculadas {r.get('coocurrencias')}, Σ C(k, 2) {r.get('pares_por_fila')}")
    if r and c.get("cooccurrences") is not None and r.get("peso_total") != c.get("cooccurrences"):
        bad.append(f"Σ weight {r.get('peso_total')} ≠ coocurrencias del reporte {c.get('cooccurrences')}")
    part.add("E3", "edges_hashtag_cooccurrence = recalculada desde ta_trusted.posts (fact_posts no tiene hashtags)",
             FALLA if bad else OK,
             "\n".join(bad) if bad else f"{r.get('pares')} pares, 0 claves o pesos distintos · Σ weight {r.get('peso_total')} = "
             f"recalculadas {r.get('coocurrencias')} = Σ C(k, 2) por fila {r.get('pares_por_fila')} · {r.get('hashtags')} hashtags",
             counts=r)

    results, problems = [], []
    for name, sql in EXAMPLE_SQL.items():
        q = query(sql)
        results.append({"name": name, "state": q["state"], "rows": q.get("rows") or [], "scanned_bytes": q.get("scanned_bytes", 0),
                        "engine_ms": q.get("engine_ms", 0), "total_ms": q.get("total_ms", 0)})
        if q["state"] != "SUCCEEDED":
            problems.append(f"{name}: {q['state']}: {q['reason']}")
        elif not q.get("rows"):
            problems.append(f"{name}: 0 filas")
    top = (results[0]["rows"] or [{}])[0] if results else {}
    by_n = {row.get("n_marcas"): row.get("usuarios") for row in (results[1]["rows"] if len(results) > 1 else [])}
    part.add("E4", "consultas de ejemplo de HU-028 (sin user_hash)", FALLA if problems else OK,
             "\n".join(problems) if problems else
             f"par de hashtags más frecuente: {top.get('hashtag_a')} + {top.get('hashtag_b')} ({top.get('weight')}) · usuarios por nº de "
             f"marcas mencionadas: {by_n} · " + "; ".join(f"{x['name']} {len(x['rows'])} filas, {x['total_ms']} ms" for x in results),
             queries=[{k: (v if k != "rows" else len(v)) for k, v in x.items()} for x in results], top_pair=top, users_by_n_brands=by_n)
    cutoff = v41.DEFAULT_CUTOFF
    over = [s for s in scanned if s > cutoff]
    billed = sum(max(s, 10_000_000) for s in scanned)
    part.add("E5", "bytes escaneados ≤ límite del workgroup", FALLA if over else OK,
             f"{sum(scanned) / 1e6:.1f} MB en {len(scanned)} consultas; la mayor {max(scanned or [0]) / 1e6:.1f} MB (límite "
             f"{cutoff / 2**30:.0f} GiB) · ≈ {billed / 1e12 * 5:.5f} USD (mínimo 10 MB por consulta)",
             scanned_bytes=sum(scanned), queries=len(scanned), usd=round(billed / 1e12 * 5, 6))
    slow = [x for x in results if x["total_ms"] > KPI_LATENCY_MS]
    worst = max((x["total_ms"] for x in results), default=0)
    part.add("E6", f"latencia de las consultas de ejemplo frente al KPI de < {KPI_LATENCY_MS / 1000:g} s (§4)",
             OK if results and not slow else (AVISO if results else OMITIDO),
             (f"la más lenta {worst} ms (total, con la cola de Athena); motor: "
              + ", ".join(f"{x['name']} {x['engine_ms']} ms" for x in results)
              + ("" if not slow else f"\n{len(slow)} pasan de {KPI_LATENCY_MS} ms: el KPI es del dashboard con SPICE (Lote 6.1)"))
             if results else "sin consultas", worst_ms=worst)
    return part.finish()


# --- Parte F · segunda corrida (idempotencia) y costo ----------------------------------------------


def part_rerun(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Segunda corrida dentro del workflow: idempotencia (aristas incluidas) y costo")
    bucket = bucket_name(ctx)
    before_report = latest_report(ctx, bucket)
    before = curated_listing(ctx, bucket)
    if not before_report or not before or not any(k.startswith(EDGE_PREFIX) for k in before):
        part.add("F1", "Curated antes de la segunda corrida", FALLA,
                 "no hay reporte en quality/_latest/ o curated/ no tiene aristas: corra antes la parte D")
        return part.finish()
    placeholders0 = v43.prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX) or {}
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["curated"])
    counts0 = v43.ints(v43.first_row(q))
    state0, _ = v41.get_schedule(ctx)
    part.add("F1", "Curated antes de la segunda corrida", OK,
             f"{len(before)} objetos · reporte {before_report.get('run_id')} ({before_report.get('version')}) · Athena: fact_posts "
             f"{counts0.get('fact')}, edges_user_brand {counts0.get('pares_ub')} (Σ {counts0.get('peso_ub')}), "
             f"edges_hashtag_cooccurrence {counts0.get('pares_ht')} (Σ {counts0.get('peso_ht')}) · horario {state0}",
             objects=len(before), counts=counts0, schedule=state0)
    if not ctx.ask(f"¿Correr el workflow {WORKFLOW} una vez ({PREV_JOB} con {PREV_CAPACITY:g} DPU y {JOB} con "
                   f"{EXPECTED_CAPACITY:g} DPU; ≈ 0,02 USD)?"):
        part.add("F2", "corrida del workflow", AVISO, "no confirmado")
        return part.finish()
    started = ctx.now()
    run_id, err, tries = v41.start_run(ctx, {"ta_validacion": "4.4"}, part)
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
    same = v43.files_by_key(report) == v43.files_by_key(before_report) and bool(v43.files_by_key(report))
    edges_in = sum(1 for k in v43.files_by_key(report) if k.startswith(EDGE_PREFIX))
    good3 = (report.get("workflow_run_id") == run_id and out.get("files_changed") == 0 and out.get("objects_written") == 0
             and out.get("objects_deleted") == 0 and same and edges_in == len(EDGE_TABLES) and not v43.failed_checks(report)
             and (report.get("counts") or {}).get("mentions") == (before_report.get("counts") or {}).get("mentions"))
    part.add("F3", "reporte de la segunda corrida: nada cambió (aristas incluidas)", OK if good3 else FALLA,
             f"corrida {report.get('run_id')} · archivos cambiados {out.get('files_changed')} · escritos {out.get('objects_written')} · "
             f"borrados {out.get('objects_deleted')} · {'mismos' if same else 'DISTINTOS'} sha256 que la corrida anterior en "
             f"{len(v43.files_by_key(report))} archivos ({edges_in} de aristas) · menciones {(report.get('counts') or {}).get('mentions')} · "
             f"coocurrencias {(report.get('counts') or {}).get('cooccurrences')} · Trusted de la misma corrida: "
             f"{i.get('trusted_rows_expected')} filas (coincide: {i.get('trusted_rows_match')})", report=v43.report_summary(report))

    after = curated_listing(ctx, bucket) or {}
    part.add("F4", "mismos objetos y tamaños en curated/ (nueve tablas)", OK if after == before else FALLA,
             f"{len(before)} → {len(after)} objetos" + ("" if after == before else
                                                        f"; nuevos {sorted(set(after) - set(before))[:5]}, faltan {sorted(set(before) - set(after))[:5]}"))
    q = v41.athena_query(ctx, COUNT_SQL, v41.DATABASES["curated"])
    counts1 = v43.ints(v43.first_row(q))
    part.add("F5", "Athena: mismas filas y pesos", OK if counts1 == counts0 and counts1 else FALLA,
             f"{counts0} → {counts1}")

    props_res = ctx.aws("glue", "get-workflow-run-properties", "--name", WORKFLOW, "--run-id", run_id)
    props = (parse_json(props_res.out) or {}).get("RunProperties") or {}
    good6 = (props.get("ta_curated_report") == report.get("report_key") and props.get("ta_curated_files_changed") == "0"
             and bool(props.get("ta_trusted_report")))
    part.add("F6", "propiedades de la corrida (para el 5.2)", OK if good6 else FALLA,
             f"ta_trusted_report = {props.get('ta_trusted_report')} · ta_trusted_rows = {props.get('ta_trusted_rows')} · "
             f"ta_curated_report = {props.get('ta_curated_report')} · ta_curated_rows = {props.get('ta_curated_rows')} · "
             f"ta_curated_files_changed = {props.get('ta_curated_files_changed')}", properties=props)

    placeholders1 = v43.prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX) or {}
    busy = [r.get("WorkflowRunId") for r in v41.workflow_runs(ctx) or [] if r.get("Status") in ("RUNNING", "STOPPING")]
    state1, _ = v41.get_schedule(ctx)
    new_marks = sorted(set(placeholders1) - set(placeholders0))
    good7 = not new_marks and not busy and state1 == state0
    part.add("F7", "sin marcas nuevas, nada en curso, horario sin cambios", OK if good7 else AVISO,
             f"curated/_ta_placeholder/: {len(placeholders0)} → {len(placeholders1)} · corridas en curso: {busy or 'ninguna'} · horario "
             f"{state0} → {state1}" + (f"; marcas nuevas {new_marks[:3]}" if new_marks else "")
             + ("" if state1 == "ENABLED" else " (al terminar: python project/scripts/validate_4_4.py on --yes)"))

    trusted_report = v42.read_json_object(ctx, bucket, props.get("ta_trusted_report") or TRUSTED_LATEST_REPORT) or {}
    exec1 = jr1.get("ExecutionTime")
    exec2 = jr2.get("ExecutionTime") or ctx.step_data("D", "D3").get("execution_s")
    trusted_usd = v42.job_cost(exec1, PREV_CAPACITY) + v42.s3_request_cost(trusted_report)
    curated_usd = v42.job_cost(exec2, EXPECTED_CAPACITY) + v43.curated_s3_cost(report)
    per_run = trusted_usd + curated_usd
    per_day = per_run * RUNS_PER_DAY
    part.add("F8", "costo con lo medido (4.2 + 4.3/4.4, horario cada hora)", OK,
             f"{PREV_JOB}: {exec1} s × {PREV_CAPACITY:g} DPU + S3 ≈ {trusted_usd:.5f} USD · {JOB} (con aristas): {exec2} s × "
             f"{EXPECTED_CAPACITY:g} DPU (mínimo 60 s) + S3 (≈ {i.get('objects')} GET, {out.get('objects_written')} PUT de datos) ≈ "
             f"{curated_usd:.5f} USD → ≈ {per_run:.4f} USD por corrida del workflow; con el horario cada hora ≈ {per_day:.3f} USD/día, "
             f"≈ {per_day * 30:.2f} USD/mes (KMS con bucket key y logs aparte, sin comprobar en la factura)",
             usd_per_run=round(per_run, 5), usd_per_day=round(per_day, 4), usd_per_month=round(per_day * 30, 2),
             trusted_usd=round(trusted_usd, 5), curated_usd=round(curated_usd, 5), execution_s=[exec1, exec2])
    return part.finish()


# --- status, off y on -------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del lakehouse y de Curated con aristas (solo lectura)")
    state, sched = v41.get_schedule(ctx)
    if state == "ENABLED":
        s1 = (OK, "ENABLED (estado normal por decisión del usuario: cada hora corren Trusted y Curated con las aristas)")
    elif state == "DISABLED":
        s1 = (AVISO, "DISABLED (el estado normal es encendido: python project/scripts/validate_4_4.py on --yes)")
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
    has_edges = all(per_table.get(t) for t in EDGE_TABLES)
    c = report.get("counts") or {}
    tables = {k: v.get("rows") for k, v in (report.get("tables") or {}).items() if k in EDGE_TABLES}
    part.add("S3", "Curated (nueve tablas) y su último reporte", OK if listing is not None and has_edges else AVISO,
             f"curated/: {len(listing or {})} objetos {per_table} · " + (
                 f"último reporte {report.get('run_id')} ({report.get('version')}, {report.get('finished_at') or '—'}): fact_posts "
                 f"{c.get('fact_rows')} filas, aristas {tables}, menciones {c.get('mentions')}, coocurrencias {c.get('cooccurrences')}, "
                 f"{(report.get('output') or {}).get('files_changed')} archivos cambiados" if report else
                 f"sin reporte en {LATEST_REPORT} (el job todavía no corrió)")
             + ("" if has_edges else " · sin aristas todavía (aplique C y corra D)"))
    temps = v41.temp_schedules(ctx)
    part.add("S4", "horarios de prueba del 4.1", AVISO if temps else OK, ", ".join(temps) if temps else "ninguno")
    marks = v43.prefix_listing(ctx, bucket, PLACEHOLDER_PREFIX)
    part.add("S5", "marcas del marcador de posición en curated/_ta_placeholder/", OK if not marks else AVISO,
             f"{len(marks or {})} objetos" + (" (restos del marcador del 4.1: off los borra)" if marks else ""))
    return part.finish()


def part_off(ctx: Ctx) -> int:
    """El apagado del 4.1 (horario, corridas, job runs, horarios de prueba y marcas), con la evidencia del 4.4.

    Se usa **mientras se valida**: al terminar, ``on`` deja el horario encendido (estado normal)."""
    return v41.part_off(ctx)


def part_on(ctx: Ctx) -> int:
    part = Part(ctx, "on", "Encender el horario del lakehouse (estado normal, decisión del usuario)")
    f8 = ctx.step_data("F", "F8")
    per_day = f8.get("usd_per_day") or round(ESTIMATED_USD_PER_RUN * RUNS_PER_DAY, 3)
    source = "medido en F8" if f8.get("usd_per_day") else "estimado"
    if not ctx.ask(f"¿Encender {SCHEDULE} (arranca {WORKFLOW} cada hora: Trusted y Curated con aristas; ≈ {per_day} USD/día, "
                   f"≈ {round(per_day * 30, 2)} USD/mes, {source})?"):
        part.add("N1", "encender el horario", AVISO, "no confirmado")
        return part.finish()
    ok, detail = v41.set_schedule_state(ctx, "ENABLED")
    part.add("N1", "encender el horario", OK if ok else FALLA, detail + "; se apaga con: python project/scripts/validate_4_4.py off")
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
    parser = argparse.ArgumentParser(prog="validate_4_4.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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
    printer(f"Lote 4.4 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si quedó algo corriendo: python project/scripts/validate_4_4.py off; y al terminar: "
                "validate_4_4.py on --yes (el horario va encendido)")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
