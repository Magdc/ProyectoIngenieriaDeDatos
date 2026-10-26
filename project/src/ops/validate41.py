"""Validación automática del Lote 4.1 (HU-007, HU-029): ``docs/validation/validation-4-1.md``.

    python project/scripts/validate_4_1.py <parte> [opciones]      (desde la raíz del repo)

Partes: ``preflight`` (A), ``tests`` (B), ``terraform`` (C), ``catalog`` (D: Glue, workgroup, S3 e
IAM), ``athena`` (E: el SELECT de demostración sobre ``ta_raw.posts_raw``), ``workflow`` (F: el
workflow de Glue, los fallos en CloudWatch y el horario); además ``status`` (solo lectura),
``off`` (estado de ahorro: horario deshabilitado, sin corridas, sin restos) y ``on`` (encender el
horario; pide confirmación). Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y
guarda su resumen en ``project/evidencia/validacion_4_1.json``.

Reutiliza de ``ops/validate32.py`` el ejecutor, los pasos, el plan de Terraform leído en JSON
(qué cambia antes → después) y el ``apply`` vigilado, y de ``ops/services.py`` el estado de lo
que cobra en GCP (``status.py``). Lo que cambia la nube (``terraform apply``, correr el
workflow, encender el horario) pide confirmación, salvo con ``--yes``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from ops import services as sv
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
    policy_actions,
    pytest_counts,
    tf_apply,
    tf_plan,
    tfvars_value,
)

REPO = Path(__file__).resolve().parents[3]
PROJECT = REPO / "project"
SRC = PROJECT / "src"
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_4_1.json"
AWS_ENV = PROJECT / "infra" / "aws" / "environments" / "dev"
QUERIES = SRC / "lakehouse" / "queries"
GLUE_JOBS_DIR = SRC / "lakehouse" / "glue_jobs"

AWS_REGION = "us-east-1"
DEFAULT_PROJECT = "project-0568d9ba-848f-413c-88b"
DATABASES = {"raw": "ta_raw", "trusted": "ta_trusted", "curated": "ta_curated"}
WORKGROUP = "ta-analytics"
WORKFLOW = "ta-wf-raw-to-curated"
JOBS = {"raw_to_trusted": "ta-job-raw-to-trusted", "trusted_to_curated": "ta-job-trusted-to-curated"}
START_TRIGGER = f"{WORKFLOW}-inicio"
NEXT_TRIGGER = f"{WORKFLOW}-trusted-to-curated"
SCHEDULE = "ta-wf-raw-to-curated-cada-hora"
SCHEDULE_TEST_PREFIX = "ta-wf-raw-to-curated-validacion-"
SCHEDULER_TARGET = "arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun"
ROLES = {"glue": "ta-glue-role", "quicksight": "ta-quicksight-role", "scheduler": "ta-lakehouse-scheduler-role"}
POLICIES = {"glue": "ta-glue-role-lake", "quicksight": "ta-quicksight-role-athena", "scheduler": "ta-lakehouse-scheduler-role-glue"}
FAIL_RULE = "ta-glue-job-fallos"
FAIL_LOG_GROUP = "/aws/events/ta-glue-job-fallos"
FAIL_LOG_POLICY = "ta-glue-job-fallos-events"
GLUE_OUTPUT_LOG = "/aws-glue/python-jobs/output"
GLUE_ERROR_LOG = "/aws-glue/python-jobs/error"
LAYER_FOLDERS = ("trusted/", "curated/", "quality/")
PLACEHOLDER_PREFIXES = ("trusted/_ta_placeholder/", "curated/_ta_placeholder/")
NAMED_QUERIES = {"ta-raw-conteo-por-origen": "raw_conteo_por_origen.sql", "ta-raw-sentimiento-7-dias": "raw_sentimiento_7_dias.sql"}
DEFAULT_CUTOFF = 1073741824
HIST_ROWS = 50000
HIST_OBJECTS = 12102

# Pruebas esperadas (B1, B2): las cuenta docs/validation/validation-4-1.md.
B1_EXPECTED = 50
B2_EXPECTED = 48

# Glue Python shell (us-east-1, oct-2026): 0,44 USD por DPU-hora, por segundo con mínimo de 1 min.
DPU_USD_PER_HOUR = 0.44
PYSHELL_DPU = 0.0625
RUNS_PER_DAY = 24

TERRAFORM_TARGETS = [
    "module.glue_catalog",
    "aws_s3_object.lake_layer",
    "module.athena",
    "module.glue_role",
    "module.quicksight_role",
    "module.glue_workflow",
    "module.lakehouse_schedule",
    "module.glue_failure_events",
]

LOT_FILES = [
    "infra/aws/modules/glue_catalog/main.tf",
    "infra/aws/modules/glue_catalog/tables.json",
    "infra/aws/modules/athena_workgroup/main.tf",
    "infra/aws/modules/glue_role/main.tf",
    "infra/aws/modules/quicksight_role/main.tf",
    "infra/aws/modules/glue_workflow/main.tf",
    "infra/aws/modules/lakehouse_schedule/main.tf",
    "infra/aws/modules/glue_failure_events/main.tf",
    "infra/aws/environments/dev/lakehouse.tf",
    "infra/aws/environments/dev/variables_lakehouse.tf",
    "infra/aws/environments/dev/outputs_lakehouse.tf",
    "src/lakehouse/__init__.py",
    "src/lakehouse/catalog_spec.py",
    "src/lakehouse/s3_parquet.py",
    "src/lakehouse/glue_jobs/raw_to_trusted.py",
    "src/lakehouse/glue_jobs/trusted_to_curated.py",
    "src/lakehouse/queries/raw_conteo_por_origen.sql",
    "src/lakehouse/queries/raw_sentimiento_7_dias.sql",
    "src/lakehouse/tests/test_lake_catalog_spec.py",
    "src/lakehouse/tests/test_lake_glue_jobs.py",
    "src/lakehouse/tests/test_lake_s3_parquet.py",
    "src/lakehouse/tests/test_lake_infra.py",
    "src/ops/validate41.py",
    "src/ops/tests/test_ops_validate41.py",
    "scripts/validate_4_1.py",
]
PREVIOUS_FILES = [
    "infra/validate.sh",
    "infra/aws/environments/dev/main.tf",
    "infra/aws/environments/dev/variables.tf",
    "infra/aws/environments/dev/lake.tf",
    "infra/aws/environments/dev/outputs_lake.tf",
    "infra/aws/environments/dev/exporter.tf",
    "contracts/athena/posts_raw.sql",
    "contracts/athena/posts.sql",
    "contracts/athena/curated.sql",
    "src/common/enriched_row.py",
    "src/ops/services.py",
    "src/ops/validate32.py",
    "scripts/status.py",
]

EXPECTED_ACTIONS = {
    "glue": {
        "s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts", "kms:Decrypt", "kms:GenerateDataKey",
        "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition",
        "glue:GetPartitions", "glue:BatchGetPartition", "glue:GetWorkflowRunProperties",
        "glue:PutWorkflowRunProperties", "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
        "cloudwatch:PutMetricData",
    },
    "quicksight": {
        "athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:GetQueryResultsStream", "athena:BatchGetQueryExecution",
        "athena:ListQueryExecutions", "athena:GetWorkGroup", "athena:GetDataCatalog", "athena:ListDatabases",
        "athena:GetDatabase", "athena:ListTableMetadata", "athena:GetTableMetadata", "athena:ListWorkGroups",
        "athena:ListDataCatalogs", "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "s3:GetBucketLocation",
        "s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
        "kms:Decrypt", "kms:GenerateDataKey",
    },
    "scheduler": {"glue:StartWorkflowRun"},
}
TRUST_PRINCIPALS = {"glue": "glue.amazonaws.com", "quicksight": "quicksight.amazonaws.com", "scheduler": "scheduler.amazonaws.com"}
PREVIOUS_ROLES = {"ta-historical-loader": "ta-historical-loader-s3", "ta-gcp-exporter-role": "ta-gcp-exporter-role-s3"}
PREVIOUS_ACTIONS = {"s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey"}


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx:
    runner: Runner
    options: argparse.Namespace
    printer: Callable[[str], None] = print
    confirm: Callable[[str], bool] | None = None
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    sleep: Callable[[float], None] = time.sleep
    evidence_path: Path = EVIDENCE
    s3_rows: Callable[[str, str], dict[str, Any]] | None = None
    cache: dict[str, Any] = field(default_factory=dict)

    @property
    def project(self) -> str:
        return getattr(self.options, "project", None) or os.environ.get("GCP_PROJECT") or DEFAULT_PROJECT

    def env(self) -> dict[str, str]:
        env = {"AWS_REGION": AWS_REGION, "AWS_PAGER": "", "GCP_PROJECT": self.project, "CLOUDSDK_CORE_DISABLE_PROMPTS": "1"}
        profile = getattr(self.options, "aws_profile", None)
        if profile:
            env["AWS_PROFILE"] = profile
        return env

    def ask(self, question: str) -> bool:
        if getattr(self.options, "yes", False):
            return True
        if self.confirm is not None:
            return self.confirm(question)
        try:
            return input(f"{question} [s/N] ").strip().lower() in {"s", "si", "sí", "y", "yes"}
        except EOFError:
            return False

    # evidencia
    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "4.1", "hu": ["HU-007", "HU-029"], "parts": {}}

    def save_evidence(self, data: dict[str, Any]) -> None:
        data["updated_at"] = self.now().isoformat()
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    def save_part(self, part: Part) -> None:
        data = self.load_evidence()
        data.setdefault("parts", {})[part.key] = {
            "title": part.title,
            "status": part.worst,
            "started_at": part.started.isoformat(),
            "finished_at": self.now().isoformat(),
            "steps": [asdict(s) for s in part.steps],
        }
        self.save_evidence(data)

    def step_data(self, part_key: str, step_id: str) -> dict[str, Any]:
        for step in self.load_evidence().get("parts", {}).get(part_key, {}).get("steps", []):
            if step.get("id") == step_id:
                return step.get("data", {})
        return {}

    # comandos
    def run(self, cmd: list[str], **kw: Any) -> Result:
        env = self.env()
        env.update({k: v for k, v in (kw.pop("env", None) or {}).items() if v is not None})
        return self.runner.run(cmd, env=env, **kw)

    def aws(self, *args: str, timeout: float = 180) -> Result:
        return self.run(["aws", *args, "--output", "json", "--region", AWS_REGION], timeout=timeout)

    def tf(self, root: Path, *args: str, timeout: float = 900, stream: bool = False) -> Result:
        return self.run(["terraform", f"-chdir={root}", *args], timeout=timeout, stream=stream)

    def pytest(self, *args: str, timeout: float = 900) -> Result:
        return self.run([sys.executable, "-m", "pytest", *args], cwd=PROJECT, timeout=timeout)


# --- utilidades puras ---------------------------------------------------------------------------


AWS_ERROR = re.compile(r"An error occurred \((\w+)\)")


def aws_error(res: Result) -> str | None:
    m = AWS_ERROR.search(res.text or "")
    return m.group(1) if m else None


def parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def strip_sql_comments(sql: str) -> str:
    return "\n".join(ln for ln in sql.replace("\r\n", "\n").splitlines() if not ln.strip().startswith("--")).strip()


def load_tables() -> list[Any]:
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakehouse import catalog_spec

    return catalog_spec.load_tables_json()


def expected_creates(tables: list[Any]) -> set[str]:
    """Direcciones de Terraform que crea el lote (40 con las 11 tablas de contracts/athena/)."""
    addrs = {f'module.glue_catalog.aws_glue_catalog_database.this["{k}"]' for k in DATABASES}
    addrs |= {f'module.glue_catalog.aws_glue_catalog_table.this["{t.full_name}"]' for t in tables}
    addrs |= {f'aws_s3_object.lake_layer["{p}"]' for p in LAYER_FOLDERS}
    addrs.add("module.athena.aws_athena_workgroup.this")
    addrs |= {f'module.athena.aws_athena_named_query.this["{n}"]' for n in NAMED_QUERIES}
    for mod in ("glue_role", "quicksight_role", "lakehouse_schedule"):
        addrs |= {f"module.{mod}.aws_iam_role.this", f"module.{mod}.aws_iam_role_policy.this"}
    addrs.add("module.lakehouse_schedule.aws_scheduler_schedule.this")
    for key in JOBS:
        addrs |= {f'module.glue_workflow.aws_s3_object.script["{key}"]', f'module.glue_workflow.aws_glue_job.this["{key}"]'}
    addrs |= {"module.glue_workflow.aws_glue_workflow.this", "module.glue_workflow.aws_glue_trigger.start",
              'module.glue_workflow.aws_glue_trigger.next["trusted_to_curated"]'}
    addrs |= {f'module.glue_workflow.aws_cloudwatch_log_group.glue["{g}"]' for g in (GLUE_OUTPUT_LOG, GLUE_ERROR_LOG)}
    addrs |= {f"module.glue_failure_events.{r}" for r in (
        "aws_cloudwatch_log_group.this", "aws_cloudwatch_event_rule.this", "aws_cloudwatch_event_target.this",
        "aws_cloudwatch_log_resource_policy.this")}
    return addrs


def athena_rows(data: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    """Columnas y filas (como dict) de ``get-query-results``; la primera fila es el encabezado."""
    rs = (data or {}).get("ResultSet") or {}
    cols = [c.get("Name") or c.get("Label") for c in (rs.get("ResultSetMetadata") or {}).get("ColumnInfo") or []]
    rows = [[d.get("VarCharValue") for d in r.get("Data") or []] for r in rs.get("Rows") or []]
    if rows and rows[0] == cols:
        rows = rows[1:]
    return cols, [dict(zip(cols, r)) for r in rows]


def job_runs_by_name(run: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for node in ((run or {}).get("Graph") or {}).get("Nodes") or []:
        if node.get("Type") == "JOB":
            out[node.get("Name")] = list(((node.get("JobDetails") or {}).get("JobRuns")) or [])
    return out


def billable_seconds(execution_s: float | None) -> float:
    return max(60.0, float(execution_s or 0))


def job_cost(execution_s: float | None, dpu: float = PYSHELL_DPU) -> float:
    return billable_seconds(execution_s) * dpu * DPU_USD_PER_HOUR / 3600


def schedule_update_input(schedule: dict[str, Any], state: str) -> dict[str, Any]:
    """Lo que ``update-schedule`` necesita para cambiar solo el estado (la API reemplaza todo)."""
    keys = ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow", "Target",
            "Description", "StartDate", "EndDate", "KmsKeyArn", "ActionAfterCompletion")
    out = {k: schedule[k] for k in keys if schedule.get(k) not in (None, "")}
    out["State"] = state
    return out


def at_expression(now: datetime, delay_s: float = 90) -> tuple[str, datetime]:
    """Expresión ``at(...)`` del primer minuto entero que llega ``delay_s`` después de ``now`` (UTC)."""
    at = (now + timedelta(seconds=delay_s)).replace(second=0, microsecond=0) + timedelta(minutes=1)
    return f"at({at.strftime('%Y-%m-%dT%H:%M:%S')})", at


def schedule_problems(sched: dict[str, Any], role_arn: str | None) -> list[str]:
    problems = []
    target = sched.get("Target") or {}
    if sched.get("ScheduleExpression") != "rate(1 hour)":
        problems.append(f"expresión {sched.get('ScheduleExpression')}")
    if (sched.get("ScheduleExpressionTimezone") or "UTC") != "UTC":
        problems.append(f"zona {sched.get('ScheduleExpressionTimezone')}")
    if target.get("Arn") != SCHEDULER_TARGET:
        problems.append(f"destino {target.get('Arn')}")
    if role_arn and target.get("RoleArn") != role_arn:
        problems.append(f"rol {target.get('RoleArn')}")
    try:
        body = json.loads(target.get("Input") or "{}")
    except ValueError:
        body = {}
    if body.get("Name") != WORKFLOW:
        problems.append(f"Input {target.get('Input')}")
    if ((target.get("RetryPolicy") or {}).get("MaximumRetryAttempts")) != 0:
        problems.append(f"reintentos {target.get('RetryPolicy')}")
    if ((sched.get("FlexibleTimeWindow") or {}).get("Mode")) != "OFF":
        problems.append(f"ventana flexible {sched.get('FlexibleTimeWindow')}")
    return problems


def trust_services(doc: Any) -> set[str]:
    if isinstance(doc, str):
        from urllib.parse import unquote

        doc = json.loads(unquote(doc))
    out: set[str] = set()
    statements = (doc or {}).get("Statement", [])
    for st in statements if isinstance(statements, list) else [statements]:
        svc = (st.get("Principal") or {}).get("Service")
        out.update(svc if isinstance(svc, list) else [svc] if svc else [])
    return out


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def glue_arn(account: str, kind: str, name: str) -> str:
    return f"arn:aws:glue:{AWS_REGION}:{account}:{kind}/{name}"


def simulate_cases(role: str, outputs: dict[str, Any], account: str) -> list[tuple[str, str, str, list[tuple[str, str]], bool]]:
    bucket = outputs.get("lake_bucket") or ""
    key = str(outputs.get("kms_key_arn") or "")
    b = f"arn:aws:s3:::{bucket}"
    via = [("kms:ViaService", f"s3.{AWS_REGION}.amazonaws.com")]
    hist = f"{b}/raw/origin=historical/source=reddit/dt=2026-05-01/hr=03/part-historical-s3101-20260501T03.parquet"
    stream = f"{b}/raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-20261022T100000Z-p000.parquet"
    trusted = f"{b}/trusted/posts/source=reddit/dt=2026-10-22/part-0.parquet"
    curated = f"{b}/curated/fact_posts/dt=2026-10-22/part-0.parquet"
    wf = glue_arn(account, "workflow", WORKFLOW)
    wg = f"arn:aws:athena:{AWS_REGION}:{account}:workgroup/{WORKGROUP}"
    if role == "glue":
        # El simulador de IAM evalúa logs:PutLogEvents contra el ARN del grupo: con el de un flujo
        # (…:log-stream:jr_…) responde implicitDeny incluso con "Resource": "*" (comprobado en v2.32).
        # La escritura real en el flujo la prueba F5 (logs de cada job run).
        logs = f"arn:aws:logs:{AWS_REGION}:{account}:log-group:{GLUE_OUTPUT_LOG}"
        other_logs = f"arn:aws:logs:{AWS_REGION}:{account}:log-group:{FAIL_LOG_GROUP}"
        return [
            ("get raw/origin=historical/", "s3:GetObject", hist, [], True),
            ("list raw/", "s3:ListBucket", b, [("s3:prefix", "raw/origin=stream/")], True),
            ("put trusted/posts/", "s3:PutObject", trusted, [], True),
            ("delete curated/fact_posts/", "s3:DeleteObject", curated, [], True),
            ("put quality/", "s3:PutObject", f"{b}/quality/2026-10-26/ge.json", [], True),
            ("get _glue/scripts/", "s3:GetObject", f"{b}/_glue/scripts/{JOBS['raw_to_trusted']}.py", [], True),
            ("kms:Decrypt vía S3", "kms:Decrypt", key, via, True),
            ("kms:GenerateDataKey vía S3", "kms:GenerateDataKey", key, via, True),
            ("glue:GetTable ta_raw.posts_raw", "glue:GetTable", glue_arn(account, "table", "ta_raw/posts_raw"), [], True),
            ("glue:GetWorkflowRunProperties", "glue:GetWorkflowRunProperties", wf, [], True),
            ("logs:PutLogEvents en /aws-glue/", "logs:PutLogEvents", logs, [], True),
            ("put raw/origin=stream/", "s3:PutObject", stream, [], False),
            ("delete raw/origin=historical/", "s3:DeleteObject", hist, [], False),
            ("put athena-results/", "s3:PutObject", f"{b}/athena-results/x.csv", [], False),
            ("list export-state/", "s3:ListBucket", b, [("s3:prefix", "export-state/")], False),
            ("kms:Decrypt sin S3", "kms:Decrypt", key, [], False),
            ("glue:CreateTable en ta_curated", "glue:CreateTable", glue_arn(account, "database", "ta_curated"), [], False),
            ("glue:DeleteTable ta_raw.posts_raw", "glue:DeleteTable", glue_arn(account, "table", "ta_raw/posts_raw"), [], False),
            ("glue:StartWorkflowRun", "glue:StartWorkflowRun", wf, [], False),
            ("logs:PutLogEvents fuera de /aws-glue/", "logs:PutLogEvents", other_logs, [], False),
        ]
    if role == "quicksight":
        return [
            ("athena:StartQueryExecution en ta-analytics", "athena:StartQueryExecution", wg, [], True),
            ("athena:GetQueryResults en ta-analytics", "athena:GetQueryResults", wg, [], True),
            ("glue:GetTable ta_curated.fact_posts", "glue:GetTable", glue_arn(account, "table", "ta_curated/fact_posts"), [], True),
            ("get curated/", "s3:GetObject", curated, [], True),
            ("put athena-results/", "s3:PutObject", f"{b}/athena-results/x.csv", [], True),
            ("list curated/", "s3:ListBucket", b, [("s3:prefix", "curated/fact_posts/")], True),
            ("kms:Decrypt vía S3", "kms:Decrypt", key, via, True),
            ("athena:StartQueryExecution en primary", "athena:StartQueryExecution",
             f"arn:aws:athena:{AWS_REGION}:{account}:workgroup/primary", [], False),
            ("athena:DeleteWorkGroup ta-analytics", "athena:DeleteWorkGroup", wg, [], False),
            ("glue:GetTable ta_raw.posts_raw", "glue:GetTable", glue_arn(account, "table", "ta_raw/posts_raw"), [], False),
            ("get raw/", "s3:GetObject", stream, [], False),
            ("get trusted/", "s3:GetObject", trusted, [], False),
            ("put curated/", "s3:PutObject", curated, [], False),
        ]
    return [
        ("glue:StartWorkflowRun ta-wf-raw-to-curated", "glue:StartWorkflowRun", wf, [], True),
        ("glue:StartWorkflowRun otro workflow", "glue:StartWorkflowRun", glue_arn(account, "workflow", "otro"), [], False),
        ("glue:StartJobRun", "glue:StartJobRun", glue_arn(account, "job", JOBS["raw_to_trusted"]), [], False),
    ]


# --- piezas que usan varias partes -------------------------------------------------------------


def aws_outputs(ctx: Ctx, part: Part | None = None) -> dict[str, Any]:
    if "aws_outputs" in ctx.cache:
        return ctx.cache["aws_outputs"]
    if part:
        part.note("leyendo terraform output de infra/aws (en /mnt/c puede tardar un minuto)…")
    res = ctx.tf(AWS_ENV, "output", "-json", timeout=300)
    data = parse_json(res.out) if res.ok else None
    outputs = {k: v.get("value") for k, v in (data or {}).items() if isinstance(v, dict)}
    ctx.cache["aws_outputs"] = outputs
    return outputs


def account_id(ctx: Ctx) -> str:
    if "account" not in ctx.cache:
        res = ctx.aws("sts", "get-caller-identity")
        ident = parse_json(res.out) or {}
        ctx.cache["account"] = str(ident.get("Account") or "")
        ctx.cache["caller_arn"] = ident.get("Arn") or ""
    return ctx.cache["account"]


def lake_bucket(ctx: Ctx, outputs: dict[str, Any]) -> str:
    return outputs.get("lake_bucket") or f"ta-dev-{account_id(ctx)}-lake"


def get_schedule(ctx: Ctx, name: str = SCHEDULE) -> tuple[str | None, dict[str, Any]]:
    res = ctx.aws("scheduler", "get-schedule", "--name", name)
    if not res.ok:
        if aws_error(res) == "ResourceNotFoundException":
            return "NO_EXISTE", {}
        return None, {"error": res.tail(3)}
    data = parse_json(res.out) or {}
    return data.get("State"), data


def write_temp_json(data: dict[str, Any]) -> Path:
    fd, name = tempfile.mkstemp(prefix="ta41-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    return Path(name)


def aws_with_input(ctx: Ctx, args: list[str], data: dict[str, Any]) -> Result:
    path = write_temp_json(data)
    try:
        return ctx.aws(*args, "--cli-input-json", f"file://{path}")
    finally:
        try:
            path.unlink()
        except OSError:
            pass


def set_schedule_state(ctx: Ctx, state: str) -> tuple[bool, str]:
    current, data = get_schedule(ctx)
    if current in (None, "NO_EXISTE"):
        return False, f"horario {SCHEDULE}: {current or data.get('error')}"
    if current == state:
        return True, f"ya estaba {state}"
    res = aws_with_input(ctx, ["scheduler", "update-schedule"], schedule_update_input(data, state))
    after, _ = get_schedule(ctx)
    return after == state, f"{current} → {after}" + ("" if res.ok else ": " + res.tail(3))


def workflow_runs(ctx: Ctx, max_results: int = 25) -> list[dict[str, Any]] | None:
    res = ctx.aws("glue", "get-workflow-runs", "--name", WORKFLOW, "--max-results", str(max_results), "--no-include-graph")
    if not res.ok:
        return [] if aws_error(res) == "EntityNotFoundException" else None
    return list((parse_json(res.out) or {}).get("Runs") or [])


def get_run(ctx: Ctx, run_id: str) -> dict[str, Any]:
    res = ctx.aws("glue", "get-workflow-run", "--name", WORKFLOW, "--run-id", run_id, "--include-graph")
    return (parse_json(res.out) or {}).get("Run") or {} if res.ok else {}


def wait_run(ctx: Ctx, part: Part, run_id: str, timeout_s: float = 1800) -> dict[str, Any]:
    waited = 0.0
    run: dict[str, Any] = {}
    while waited <= timeout_s:
        run = get_run(ctx, run_id)
        status = run.get("Status")
        if status in ("COMPLETED", "STOPPED", "ERROR"):
            return run
        if waited and int(waited) % 120 == 0:
            part.note(f"corrida {run_id}: {status or '?'} ({int(waited)} s)")
        ctx.sleep(15)
        waited += 15
    run["_timeout"] = True
    return run


# Glue libera el cupo de MaxConcurrentRuns (1) unos segundos DESPUÉS de marcar la corrida COMPLETED:
# en la validación, StartWorkflowRun 9 s después del CompletedOn de la anterior dio
# ConcurrentRunsExceededException. Antes de arrancar se espera a que no haya corridas RUNNING/STOPPING
# y, si Glue todavía rechaza, se reintenta cada CONCURRENT_RETRY_S hasta CONCURRENT_TRIES veces.
CONCURRENT_RETRY_S = 20
CONCURRENT_TRIES = 9
IDLE_TIMEOUT_S = 1800


def wait_idle(ctx: Ctx, part: Part | None = None, timeout_s: float = IDLE_TIMEOUT_S) -> list[str]:
    """Espera a que el workflow no tenga corridas RUNNING/STOPPING; devuelve las que sigan al agotar."""
    waited = 0.0
    while True:
        busy = [r.get("WorkflowRunId") for r in workflow_runs(ctx) or [] if r.get("Status") in ("RUNNING", "STOPPING")]
        if not busy or waited >= timeout_s:
            return busy
        if part and int(waited) % 120 == 0:
            part.note(f"esperando a que terminen las corridas en curso: {', '.join(busy)}…")
        ctx.sleep(15)
        waited += 15


def start_run(ctx: Ctx, properties: dict[str, str], part: Part | None = None) -> tuple[str | None, str, int]:
    """StartWorkflowRun con espera a que el workflow esté libre y reintento ante ConcurrentRunsExceededException.

    Devuelve ``(run_id, error, intentos)``."""
    busy = wait_idle(ctx, part)
    if busy:
        return None, f"el workflow sigue con corridas en curso tras {IDLE_TIMEOUT_S // 60} min: {', '.join(busy)}", 0
    args = ["glue", "start-workflow-run", "--name", WORKFLOW]
    if properties:
        args += ["--run-properties", ",".join(f"{k}={v}" for k, v in properties.items())]
    res = Result(1, "", "")
    for attempt in range(1, CONCURRENT_TRIES + 1):
        res = ctx.aws(*args)
        run_id = (parse_json(res.out) or {}).get("RunId") if res.ok else None
        if run_id:
            return run_id, "", attempt
        if aws_error(res) != "ConcurrentRunsExceededException":
            return None, res.tail(4), attempt
        if attempt < CONCURRENT_TRIES:
            if part:
                part.note(f"Glue todavía cuenta la corrida anterior (ConcurrentRunsExceededException); reintento en {CONCURRENT_RETRY_S} s…")
            ctx.sleep(CONCURRENT_RETRY_S)
    return None, (f"ConcurrentRunsExceededException tras {CONCURRENT_TRIES} intentos "
                  f"(~{CONCURRENT_TRIES * CONCURRENT_RETRY_S} s): " + res.tail(2)), CONCURRENT_TRIES


def find_log_lines(ctx: Ctx, group: str, stream_prefix: str, needle: str, start: datetime, wait_s: float = 180) -> list[str]:
    start_ms = str(int((start - timedelta(minutes=5)).timestamp() * 1000))
    tries = int(wait_s // 20) + 1
    prefix = ["--log-stream-name-prefix", stream_prefix] if stream_prefix else []
    for attempt in range(tries):
        res = ctx.aws("logs", "filter-log-events", "--log-group-name", group, *prefix,
                      "--start-time", start_ms, "--filter-pattern", f'"{needle}"', timeout=120)
        events = (parse_json(res.out) or {}).get("events") or [] if res.ok else []
        if events:
            return [e.get("message", "") for e in events]
        if attempt < tries - 1:
            ctx.sleep(20)
    return []


def json_lines(messages: list[str]) -> list[dict[str, Any]]:
    out = []
    for msg in messages:
        for line in (msg or "").splitlines():
            line = line.strip()
            if line.startswith("{"):
                data = parse_json(line)
                if isinstance(data, dict):
                    out.append(data)
    return out


def s3_keys(ctx: Ctx, bucket: str, prefix: str, max_items: int | None = None) -> list[dict[str, Any]] | None:
    args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix]
    if max_items:
        args += ["--max-items", str(max_items)]
    res = ctx.aws(*args, timeout=600)
    if not res.ok:
        return None
    return list((parse_json(res.out) or {}).get("Contents") or [])


def delete_markers(ctx: Ctx, bucket: str) -> tuple[int, list[str]]:
    deleted, problems = 0, []
    for prefix in PLACEHOLDER_PREFIXES:
        keys = s3_keys(ctx, bucket, prefix)
        if keys is None:
            problems.append(f"no se pudo listar {prefix}")
            continue
        if keys:
            res = ctx.aws("s3", "rm", f"s3://{bucket}/{prefix}", "--recursive", timeout=600)
            if res.ok:
                deleted += len(keys)
            else:
                problems.append(f"{prefix}: {res.tail(2)}")
    return deleted, problems


def athena_query(ctx: Ctx, sql: str, database: str, *, timeout_s: float = 600) -> dict[str, Any]:
    res = ctx.aws("athena", "start-query-execution", "--work-group", WORKGROUP, "--query-string", strip_sql_comments(sql),
                  "--query-execution-context", f"Database={database}")
    qid = (parse_json(res.out) or {}).get("QueryExecutionId") if res.ok else None
    if not qid:
        return {"state": "NO_ARRANCÓ", "reason": res.tail(4)}
    waited, qe = 0.0, {}
    while waited <= timeout_s:
        r = ctx.aws("athena", "get-query-execution", "--query-execution-id", qid)
        qe = (parse_json(r.out) or {}).get("QueryExecution") or {}
        state = (qe.get("Status") or {}).get("State")
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        ctx.sleep(3)
        waited += 3
    status = qe.get("Status") or {}
    stats = qe.get("Statistics") or {}
    out = {
        "id": qid,
        "state": status.get("State") or "SIN_RESPUESTA",
        "reason": status.get("StateChangeReason") or "",
        "scanned_bytes": int(stats.get("DataScannedInBytes") or 0),
        "engine_ms": int(stats.get("EngineExecutionTimeInMillis") or 0),
        "total_ms": int(stats.get("TotalExecutionTimeInMillis") or 0),
        "output_location": (qe.get("ResultConfiguration") or {}).get("OutputLocation"),
        "columns": [],
        "rows": [],
    }
    if out["state"] == "SUCCEEDED":
        r = ctx.aws("athena", "get-query-results", "--query-execution-id", qid)
        out["columns"], out["rows"] = athena_rows(parse_json(r.out) or {})
    return out


def default_s3_rows(ctx: Ctx) -> Callable[[str, str], dict[str, Any]]:
    def count(bucket: str, prefix: str) -> dict[str, Any]:
        if str(SRC) not in sys.path:
            sys.path.insert(0, str(SRC))
        import boto3
        from botocore.config import Config

        from lakehouse.s3_parquet import count_parquet_rows

        profile = getattr(ctx.options, "aws_profile", None) or os.environ.get("AWS_PROFILE") or None
        session = boto3.Session(profile_name=profile, region_name=AWS_REGION)
        client = session.client("s3", config=Config(max_pool_connections=32, retries={"max_attempts": 5}))
        return count_parquet_rows(client, bucket, prefix, workers=16)

    return count


def services_status(ctx: Ctx) -> list[str]:
    """Lo que cobra en local y en GCP según ``ops/services.py`` (lo mismo que ``status.py``)."""

    def call(args: list[str], *, capture: bool = True, env: dict | None = None, check: bool = False) -> sv.Result:
        res = ctx.run(list(args), cwd=PROJECT, env=env, timeout=300)
        out = sv.Result(res.code, res.out, res.err)
        if check and not out.ok:
            raise sv.OpsError(f"falló: {' '.join(args)}")
        return out

    try:
        return sv.status(call, sv.Settings(project=ctx.project))
    except Exception as exc:  # noqa: BLE001 - el estado de GCP solo informa
        return [f"no se pudo leer el estado de GCP: {exc}"]


def simulate(ctx: Ctx, role_arn: str, cases: list[tuple[str, str, str, list[tuple[str, str]], bool]]
             ) -> tuple[list[str], list[dict[str, Any]]]:
    """Simula cada caso; devuelve las líneas de los que no dan lo esperado y el registro de todos."""
    bad, results = [], []
    for label, action, resource, context, expected in cases:
        args = ["iam", "simulate-principal-policy", "--policy-source-arn", role_arn, "--action-names", action,
                "--resource-arns", resource]
        for key, value in context:
            args += ["--context-entries", f"ContextKeyName={key},ContextKeyValues={value},ContextKeyType=string"]
        res = ctx.aws(*args)
        result = ((parse_json(res.out) or {}).get("EvaluationResults") or [{}])[0]
        decision = result.get("EvalDecision")
        matched = [m.get("SourcePolicyId") for m in result.get("MatchedStatements") or []]
        ok = (decision == "allowed") == expected
        results.append({"case": label, "action": action, "resource": resource, "context": dict(context),
                        "expected": "allowed" if expected else "denied", "decision": decision or res.tail(1),
                        "matched": matched, "ok": ok})
        if not ok:
            ctxs = ", ".join(f"{k}={v}" for k, v in context)
            bad.append(f"{label}: {decision or res.tail(1)} (esperado {'permitido' if expected else 'denegado'}) · "
                       f"{action} sobre {resource}" + (f" con {ctxs}" if ctxs else ""))
    return bad, results


# --- Parte A · preflight --------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA,
                 f"está corriendo en Windows ({sys.platform}, Python {sys.version.split()[0]}): {WSL_HINT}")
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
        n_tables = len(catalog_spec.load_tables_json())
    except Exception as exc:  # noqa: BLE001
        problems, n_tables = [f"{type(exc).__name__}: {exc}"], 0
    part.add("A4", "tables.json al día con contracts/athena/", FALLA if problems else OK,
             "\n".join(problems) + "\nregenere: cd project/src && python -m lakehouse.catalog_spec --write" if problems
             else f"{n_tables} tablas = las DDL de contracts/athena/")

    tools = {t: ctx.runner.which(t) for t in ("terraform", "aws", "bash", "gcloud")}
    need = [t for t in ("terraform", "aws", "bash") if not tools[t]]
    version = ""
    if tools["terraform"]:
        version = (parse_json(ctx.run(["terraform", "version", "-json"], timeout=60).out) or {}).get("terraform_version", "")
    part.add("A5", "herramientas", FALLA if need else OK,
             ", ".join(f"{t}{' ' + version if t == 'terraform' and version else ''}" for t in tools if tools[t])
             + (f"; faltan: {', '.join(need)}" if need else "") + ("" if tools["gcloud"] else "; sin gcloud (A7 no ve GCP)"))

    state, sched = get_schedule(ctx)
    runs = workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    detail = f"horario {SCHEDULE}: {state or 'sin dato (' + str(sched.get('error')) + ')'}; " + (
        "workflow todavía no existe" if runs == [] and state == "NO_EXISTE" else
        (f"corridas en curso: {', '.join(running)}" if running else "ninguna corrida en curso"))
    part.add("A6", "AWS: lo que cobra por disparo (horario y corridas)",
             OK if state in ("NO_EXISTE", "DISABLED") and not running else AVISO,
             detail + (" (cobra cada hora: python project/scripts/validate_4_1.py off)" if state == "ENABLED" else ""),
             schedule=state, running=running)

    lines = services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("A7", "GCP y local (services.status)", AVISO if charging else OK,
             "\n".join(lines) + ("\neste lote no los necesita: python project/scripts/off.py" if charging else ""))

    auto = AWS_ENV / "gcp_exporter.auto.tfvars"
    part.add("A8", "gcp_exporter.auto.tfvars (Lote 3.2)", OK if auto.exists() else AVISO,
             "presente: un plan completo de infra/aws no toca ta-gcp-exporter-role" if auto.exists() else
             "falta: un plan SIN -target propondría destruir ta-gcp-exporter-role (C usa -target y no lo toca); "
             "recréelo con python project/scripts/validate_3_2.py terraform")
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    res = ctx.pytest("src/lakehouse", "-q", "-rs", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B1_EXPECTED and c["skipped"] == 0
    part.add("B1", "pytest src/lakehouse", OK if ok else FALLA,
             f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {B1_EXPECTED} passed, 0 skipped)"
             + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("src/ops/tests/test_ops_validate41.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B2_EXPECTED
    part.add("B2", "pytest test_ops_validate41.py", OK if ok else FALLA,
             f"{c['passed']} passed, {c['failed']} failed (esperado {B2_EXPECTED})" + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("contracts/tests", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    part.add("B3", "pytest contracts/tests", OK if res.ok and c["passed"] == 18 else FALLA, f"{c['passed']} passed (esperado 18)", **c)
    previous = [p for p in ("src/historical/tests/test_hist_infra.py", "src/exporter/tests/test_exp_infra.py") if (PROJECT / p).exists()]
    if previous:
        res = ctx.pytest(*previous, "-q", "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        part.add("B4", "regresión Terraform AWS de 3.1 y 3.2", OK if res.ok else FALLA,
                 f"{c['passed']} passed, {c['failed']} failed ({', '.join(Path(p).name for p in previous)})"
                 + ("" if res.ok else "\n" + res.tail(10)), **c)
    else:
        part.add("B4", "regresión Terraform AWS de 3.1 y 3.2", AVISO, "no están test_hist_infra.py ni test_exp_infra.py")
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (infra/aws)")
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
             f"{arn} (cuenta raíz: sirve para aplicar y validar; los roles del lote los asumen servicios, no este perfil)"
             if arn.endswith(":root") else arn, account=ident.get("Account"))

    missing = [n for n in ("backend.hcl", "terraform.tfvars") if not (AWS_ENV / n).exists()]
    part.add("C0b", "backend.hcl y terraform.tfvars de infra/aws", FALLA if missing else OK,
             "faltan: " + ", ".join(missing) + " (cópielos de los *.example, §5.14)" if missing else "presentes (no se versionan)")
    if missing:
        return part.finish()

    try:
        tables = load_tables()
    except Exception as exc:  # noqa: BLE001
        part.add("C0c", "tablas del catálogo", FALLA, f"tables.json ilegible: {exc}")
        return part.finish()
    expected = expected_creates(tables)

    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()
    res = ctx.tf(AWS_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
    part.add("C2a", "terraform init (instala los 7 módulos del lote)", OK if res.ok else FALLA, res.tail(4))
    if not res.ok:
        return part.finish()

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / "tfplan_4_1"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", "plan (-target de los módulos del lote)", FALLA, res.err or res.tail(8))
        return part.finish()
    creates = set(summary["create"])
    extra = (creates - expected) | set(summary["update"] + summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", "plan (-target de los módulos del lote)", FALLA,
                 f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    note = (" (ya aplicado)" if not creates else
            (f"; faltan {len(expected) - len(creates)} de {len(expected)} porque ya existen" if creates != expected else ""))
    part.add("C2", "plan (-target de los módulos del lote)", OK, plan_line(summary) + f" (esperado {len(expected)} a crear)" + note,
             **summary)

    applied = not creates
    if creates:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
        elif ctx.ask(f"¿Aplicar el plan de infra/aws ({len(creates)} a crear, 0 a cambiar, 0 a destruir)?"):
            res = tf_apply(ctx, AWS_ENV, plan_path)
            applied = res.ok and f"Apply complete! Resources: {len(creates)} added, 0 changed, 0 destroyed." in res.out
            part.add("C3", "apply", OK if applied else FALLA, res.tail(4) if res.ok else res.err)
            if not res.ok:
                return part.finish()
        else:
            part.add("C3", "apply", AVISO, "no confirmado")
    else:
        part.add("C3", "apply", OK, "nada que aplicar")

    if applied:
        res, again = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, EVIDENCE_DIR / "tfplan_4_1_check")
        if again is None:
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA, res.err or res.tail(8))
        else:
            changes = again["create"] + again["update"] + again["delete"] + again["replace"]
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA if changes else OK,
                     (f"{plan_line(again)}: {sorted(changes)}" + diff_text(again)) if changes else "No changes", **again)
    else:
        part.add("C3b", "plan tras aplicar (sin deriva)", OMITIDO, "pendiente del apply")

    ctx.cache.pop("aws_outputs", None)
    outputs = aws_outputs(ctx, part)
    needed = ["lake_bucket", "kms_key_arn", "glue_db_names", "athena_workgroup", "athena_results_location",
              "athena_bytes_scanned_cutoff", "glue_role_arn", "quicksight_role_arn", "glue_workflow_name", "glue_job_names",
              "lakehouse_schedule_name", "lakehouse_scheduler_role_arn", "glue_failures_log_group"]
    absent = [k for k in needed if not outputs.get(k)]
    wrong = []
    if outputs.get("glue_db_names") and outputs["glue_db_names"] != DATABASES:
        wrong.append(f"glue_db_names = {outputs['glue_db_names']}")
    if outputs.get("athena_workgroup") and outputs["athena_workgroup"] != WORKGROUP:
        wrong.append(f"athena_workgroup = {outputs['athena_workgroup']}")
    exporter = outputs.get("exporter_role_arn")
    status = FALLA if wrong else (AVISO if absent or not exporter else OK)
    detail = ("faltan: " + ", ".join(absent) + " (aplique con --apply)") if absent else (
        f"glue_db_names {outputs['glue_db_names']} · {outputs['athena_workgroup']} · {outputs['glue_role_arn']} · "
        f"límite {outputs['athena_bytes_scanned_cutoff']} B · {outputs['glue_workflow_name']}")
    if not exporter:
        detail += "\nexporter_role_arn vacío (§5.9/HU-007: lo crea el Lote 3.2 con gcp_exporter.auto.tfvars)"
    part.add("C4", "outputs de infra/aws (§5.9)", status, detail + ("\n" + "\n".join(wrong) if wrong else ""),
             outputs={k: outputs.get(k) for k in needed + ["exporter_role_arn"]})
    return part.finish()


# --- Parte D · catálogo, workgroup, S3 e IAM -----------------------------------------------------


def part_catalog(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Catálogo de Glue, workgroup, carpetas del lake e IAM")
    outputs = aws_outputs(ctx, part)
    account = account_id(ctx)
    bucket = lake_bucket(ctx, outputs)
    kms = str(outputs.get("kms_key_arn") or "")
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakehouse import catalog_spec

    tables = catalog_spec.load_tables_json()

    res = ctx.aws("glue", "get-databases")
    dbs = {d.get("Name"): d for d in (parse_json(res.out) or {}).get("DatabaseList") or []}
    missing = [n for n in DATABASES.values() if n not in dbs]
    bad_loc = [f"{n}: {dbs[n].get('LocationUri')}" for layer, n in DATABASES.items()
               if n in dbs and dbs[n].get("LocationUri") != f"s3://{bucket}/{layer}/"]
    part.add("D1", "bases de Glue", FALLA if missing or not res.ok else (AVISO if bad_loc else OK),
             ("faltan: " + ", ".join(missing)) if missing else (", ".join(DATABASES.values()) + (f"; ubicación: {bad_loc}" if bad_loc else "")))
    if missing:
        return part.finish()

    problems, found = [], {}
    for db in DATABASES.values():
        r = ctx.aws("glue", "get-tables", "--database-name", db, timeout=300)
        for t in (parse_json(r.out) or {}).get("TableList") or []:
            found[f"{db}.{t.get('Name')}"] = t
    for spec in tables:
        glue_table = found.get(spec.full_name)
        if glue_table is None:
            problems.append(f"falta {spec.full_name}")
            continue
        problems.extend(f"{spec.full_name}: {p}" for p in catalog_spec.glue_table_problems(spec, glue_table, bucket))
    extra = sorted(set(found) - {t.full_name for t in tables})
    projected = [t.full_name for t in tables if t.projected]
    part.add("D2", "tablas = contracts/athena/ (columnas, particiones, ubicación, proyección)",
             FALLA if problems else (AVISO if extra else OK),
             ("\n".join(problems[:20]) if problems else f"{len(tables)} tablas iguales a las DDL; partition projection en "
              + ", ".join(projected)) + (f"\ntablas que no están en contracts/: {extra}" if extra else ""),
             tables=sorted(found))

    res = ctx.aws("athena", "get-work-group", "--work-group", WORKGROUP)
    wg = (parse_json(res.out) or {}).get("WorkGroup") or {}
    conf = wg.get("Configuration") or {}
    rc = conf.get("ResultConfiguration") or {}
    enc = rc.get("EncryptionConfiguration") or {}
    cutoff = outputs.get("athena_bytes_scanned_cutoff") or DEFAULT_CUTOFF
    wg_problems = []
    if wg.get("State") != "ENABLED":
        wg_problems.append(f"estado {wg.get('State')}")
    if conf.get("EnforceWorkGroupConfiguration") is not True:
        wg_problems.append("EnforceWorkGroupConfiguration no es true")
    if conf.get("BytesScannedCutoffPerQuery") != cutoff:
        wg_problems.append(f"límite {conf.get('BytesScannedCutoffPerQuery')} (esperado {cutoff})")
    if rc.get("OutputLocation") != f"s3://{bucket}/athena-results/":
        wg_problems.append(f"resultados en {rc.get('OutputLocation')}")
    if enc.get("EncryptionOption") != "SSE_KMS" or (kms and enc.get("KmsKey") != kms):
        wg_problems.append(f"cifrado {enc}")
    engine = (conf.get("EngineVersion") or {}).get("EffectiveEngineVersion")
    named = ctx.aws("athena", "list-named-queries", "--work-group", WORKGROUP)
    ids = (parse_json(named.out) or {}).get("NamedQueryIds") or []
    names = set()
    if ids:
        got = ctx.aws("athena", "batch-get-named-query", "--named-query-ids", *ids)
        names = {q.get("Name") for q in (parse_json(got.out) or {}).get("NamedQueries") or []}
    if not set(NAMED_QUERIES) <= names:
        wg_problems.append(f"consultas guardadas: {sorted(names)}")
    part.add("D3", f"workgroup {WORKGROUP}", FALLA if wg_problems or not res.ok else OK,
             "\n".join(wg_problems) if wg_problems else
             f"límite {cutoff} B ({cutoff / 2**30:.2f} GiB) forzado; resultados en athena-results/ con SSE-KMS de la llave del lake; "
             f"{engine}; consultas guardadas: {', '.join(sorted(NAMED_QUERIES))}")

    layers = []
    for prefix in ("raw/",) + LAYER_FOLDERS:
        keys = s3_keys(ctx, bucket, prefix, max_items=1)
        layers.append((prefix, bool(keys)))
    enc_res = ctx.aws("s3api", "get-bucket-encryption", "--bucket", bucket)
    rule = (((parse_json(enc_res.out) or {}).get("ServerSideEncryptionConfiguration") or {}).get("Rules") or [{}])[0]
    default = rule.get("ApplyServerSideEncryptionByDefault") or {}
    absent = [p for p, ok in layers if not ok]
    enc_ok = default.get("SSEAlgorithm") == "aws:kms" and (not kms or default.get("KMSMasterKeyID") == kms)
    part.add("D4", "carpetas del lake y cifrado (HU-007)", FALLA if absent or not enc_ok else OK,
             (f"sin objetos: {absent}" if absent else "raw/, trusted/, curated/ y quality/ existen; athena-results/ aparece con E1")
             + f"; cifrado por defecto {default.get('SSEAlgorithm')} con {'la llave del lake' if enc_ok else default.get('KMSMasterKeyID')}")

    role_problems, arns = [], {}
    for key, role in ROLES.items():
        r = ctx.aws("iam", "get-role", "--role-name", role)
        info = (parse_json(r.out) or {}).get("Role") or {}
        if not r.ok:
            role_problems.append(f"{role}: no existe")
            continue
        arns[key] = info.get("Arn")
        services = trust_services(info.get("AssumeRolePolicyDocument") or {})
        if services != {TRUST_PRINCIPALS[key]}:
            role_problems.append(f"{role}: confianza {sorted(services)}")
        p = ctx.aws("iam", "get-role-policy", "--role-name", role, "--policy-name", POLICIES[key])
        actions, _ = policy_actions((parse_json(p.out) or {}).get("PolicyDocument") or {}) if p.ok else (set(), [])
        if actions != EXPECTED_ACTIONS[key]:
            role_problems.append(f"{role}: acciones de más {sorted(actions - EXPECTED_ACTIONS[key])}, "
                                 f"de menos {sorted(EXPECTED_ACTIONS[key] - actions)}")
        if any(a.endswith(":*") for a in actions):
            role_problems.append(f"{role}: comodín en las acciones")
    part.add("D5", "roles ta-glue-role, ta-quicksight-role y del Scheduler", FALLA if role_problems else OK,
             "\n".join(role_problems) if role_problems else
             "confianza solo del servicio (glue, quicksight, scheduler) y exactamente las acciones de §5.8", arns=arns)

    for sid, key in (("D6", "glue"), ("D7", "quicksight"), ("D8", "scheduler")):
        if key not in arns:
            part.add(sid, f"política simulada de {ROLES[key]}", OMITIDO, "el rol no existe")
            continue
        cases = simulate_cases(key, outputs, account)
        bad, results = simulate(ctx, arns[key], cases)
        allowed = sum(1 for c in cases if c[4])
        part.add(sid, f"política simulada de {ROLES[key]} ({len(cases)} casos)", FALLA if bad else OK,
                 "\n".join(bad) if bad else f"{len(cases)} de {len(cases)} como se espera ({allowed} permitidos, {len(cases) - allowed} denegados)",
                 role_arn=arns[key], cases=results)

    prev = []
    for role, policy in PREVIOUS_ROLES.items():
        p = ctx.aws("iam", "get-role-policy", "--role-name", role, "--policy-name", policy)
        if not p.ok:
            prev.append(f"{role}: {aws_error(p) or 'sin política'}")
            continue
        actions, _ = policy_actions((parse_json(p.out) or {}).get("PolicyDocument") or {})
        if actions != PREVIOUS_ACTIONS:
            prev.append(f"{role}: acciones {sorted(actions)}")
    part.add("D9", "roles de 3.1 y 3.2 intactos", AVISO if prev else OK,
             "\n".join(prev) if prev else "ta-historical-loader y ta-gcp-exporter-role siguen con PutObject, ListBucket y GenerateDataKey")
    return part.finish()


# --- Parte E · Athena (SELECT de demostración) --------------------------------------------------


def part_athena(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Athena: SELECT sobre ta_raw.posts_raw con los dos orígenes")
    outputs = aws_outputs(ctx, part)
    bucket = lake_bucket(ctx, outputs)
    kms = str(outputs.get("kms_key_arn") or "")
    cutoff = int(outputs.get("athena_bytes_scanned_cutoff") or DEFAULT_CUTOFF)
    sql = (QUERIES / NAMED_QUERIES["ta-raw-conteo-por-origen"]).read_text(encoding="utf-8")
    part.note("E1: COUNT por origen y fuente en ta-analytics (la proyección enumera ~43.000 particiones; 10–60 s)…")
    q = athena_query(ctx, sql, DATABASES["raw"])
    rows = q["rows"]
    if q["state"] != "SUCCEEDED":
        part.add("E1", "SELECT de demostración (ta-raw-conteo-por-origen)", FALLA, f"{q['state']}: {q['reason']}", query=q)
        return part.finish()
    by = {(r.get("origin"), r.get("source")): int(r.get("filas") or 0) for r in rows}
    ids = {(r.get("origin"), r.get("source")): int(r.get("event_ids") or 0) for r in rows}
    totals = {o: sum(n for (oo, _), n in by.items() if oo == o) for o in ("historical", "stream")}
    table = "; ".join(f"{o}/{s}: {n}" for (o, s), n in sorted(by.items()))
    part.add("E1", "SELECT de demostración (ta-raw-conteo-por-origen)", OK if totals["historical"] and totals["stream"] else FALLA,
             f"{table}\nhistórico {totals['historical']} · stream {totals['stream']} · {q['engine_ms'] / 1000:.1f} s de motor",
             query={k: v for k, v in q.items() if k != "rows"}, rows=rows)

    hist_ids = sum(n for (o, _), n in ids.items() if o == "historical")
    detail = f"{totals['historical']} filas (esperado {HIST_ROWS}, §3.17) · {hist_ids} event_id distintos · " + ", ".join(
        f"{s} {n}" for (o, s), n in sorted(by.items()) if o == "historical")
    status = OK if totals["historical"] == HIST_ROWS and hist_ids == HIST_ROWS else FALLA
    data: dict[str, Any] = {"athena": totals["historical"]}
    if getattr(ctx.options, "reconcile_historical", False):
        part.note("conciliando el histórico con los pies de 12.102 Parquet (1–3 min)…")
        rep = (ctx.s3_rows or default_s3_rows(ctx))(bucket, "raw/origin=historical/")
        data["s3"] = rep
        match = all(by.get(("historical", s), 0) == v["rows"] for s, v in rep["by_source"].items()) and rep["rows"] == totals["historical"]
        detail += f"\nS3: {rep['objects']} objetos, {rep['rows']} filas en los pies ({rep['seconds']} s)" + ("" if match else " ≠ Athena")
        status = status if match and not rep["error_count"] else FALLA
    part.add("E2", "histórico = 50.000 filas", status, detail, **data)

    part.note("E3: conciliando el stream con los pies de los Parquet de raw/origin=stream/ (≈ 1.200 GET; 1–2 min)…")
    rep = (ctx.s3_rows or default_s3_rows(ctx))(bucket, "raw/origin=stream/")
    stream_by = {s: v["rows"] for s, v in rep.get("by_source", {}).items()}
    athena_stream = {s: n for (o, s), n in by.items() if o == "stream"}
    match = stream_by == athena_stream and rep.get("rows") == totals["stream"] and not rep.get("error_count")
    stream_ids = sum(n for (o, _), n in ids.items() if o == "stream")
    part.add("E3", "stream conciliado contra S3", OK if match else FALLA,
             f"Athena {totals['stream']} filas · S3 {rep.get('objects')} objetos y {rep.get('rows')} filas en los pies "
             f"({rep.get('seconds')} s) · por fuente {athena_stream}" + ("" if match else f" ≠ S3 {stream_by}")
             + f" · {stream_ids} event_id distintos (Raw no deduplica: lo hace Trusted)"
             + (f"\n{rep.get('error_count')} objetos ilegibles: {rep.get('errors')[:3]}" if rep.get("error_count") else ""),
             s3=rep, athena=athena_stream)

    scanned = q["scanned_bytes"]
    part.add("E4", "bytes escaneados ≤ límite del workgroup", OK if scanned <= cutoff else FALLA,
             f"{scanned / 1e6:.1f} MB de {cutoff / 1e6:.0f} MB ({100 * scanned / cutoff:.1f} %; ≈ {scanned / 1e12 * 5:.5f} USD a 5 USD/TB; "
             f"Athena cobra mínimo 10 MB por consulta)", scanned_bytes=scanned, cutoff=cutoff)

    loc = q.get("output_location") or ""
    m = re.match(r"s3://([^/]+)/(.+)$", loc)
    if m:
        head = ctx.aws("s3api", "head-object", "--bucket", m.group(1), "--key", m.group(2))
        h = parse_json(head.out) or {}
        good = m.group(2).startswith("athena-results/") and h.get("ServerSideEncryption") == "aws:kms" and (not kms or h.get("SSEKMSKeyId") == kms)
        part.add("E5", "resultado en athena-results/ con la llave del lake", OK if good else FALLA,
                 f"{loc} · {h.get('ServerSideEncryption')} · {'llave del lake' if kms and h.get('SSEKMSKeyId') == kms else h.get('SSEKMSKeyId')} "
                 "(expira a los 7 días, §5.12)")
    else:
        part.add("E5", "resultado en athena-results/ con la llave del lake", FALLA, f"OutputLocation {loc!r}")

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakehouse import catalog_spec

    others = [t for t in catalog_spec.load_tables_json() if t.full_name != "ta_raw.posts_raw"]
    results, failed, scanned_total = {}, [], 0
    for t in others:
        r = athena_query(ctx, f"SELECT COUNT(*) AS n FROM {t.full_name}", t.database, timeout_s=300)
        scanned_total += r.get("scanned_bytes", 0)
        if r["state"] != "SUCCEEDED":
            failed.append(f"{t.full_name}: {r['state']} {r['reason']}")
        else:
            results[t.full_name] = int((r["rows"] or [{}])[0].get("n") or 0)
    with_data = {k: v for k, v in results.items() if v}
    part.add("E6", f"las otras {len(others)} tablas se consultan (vacías hasta 4.2/4.3)", FALLA if failed else OK,
             ("\n".join(failed)) if failed else (f"{len(results)} consultas OK; " + (
                 f"con datos: {with_data}" if with_data else "todas en 0 filas") + f"; {scanned_total / 1e6:.1f} MB en total"),
             counts=results)

    nq = ctx.aws("athena", "list-named-queries", "--work-group", WORKGROUP)
    qids = (parse_json(nq.out) or {}).get("NamedQueryIds") or []
    text = None
    if qids:
        got = ctx.aws("athena", "batch-get-named-query", "--named-query-ids", *qids)
        for item in (parse_json(got.out) or {}).get("NamedQueries") or []:
            if item.get("Name") == "ta-raw-sentimiento-7-dias":
                text = item.get("QueryString")
    if not text:
        part.add("E7", "consulta guardada ta-raw-sentimiento-7-dias", FALLA, "no está en el workgroup")
    else:
        r = athena_query(ctx, text, DATABASES["raw"])
        n = sum(int(x.get("filas") or 0) for x in r["rows"])
        part.add("E7", "consulta guardada ta-raw-sentimiento-7-dias (poda por part_dt)",
                 FALLA if r["state"] != "SUCCEEDED" else (OK if n else AVISO),
                 f"{r['state']} {r['reason']}".strip() if r["state"] != "SUCCEEDED" else
                 f"{len(r['rows'])} grupos, {n} filas de los últimos 7 días, {r['scanned_bytes'] / 1e6:.1f} MB escaneados"
                 + ("" if n else " (sin datos recientes: el stream no ha corrido en la última semana)"))
    return part.finish()


# --- Parte F · workflow, fallos y horario ---------------------------------------------------------


def check_config(ctx: Ctx, outputs: dict[str, Any]) -> list[str]:
    problems = []
    t = (parse_json(ctx.aws("glue", "get-trigger", "--name", START_TRIGGER).out) or {}).get("Trigger") or {}
    if t.get("Type") != "ON_DEMAND" or [a.get("JobName") for a in t.get("Actions") or []] != [JOBS["raw_to_trusted"]] \
            or t.get("WorkflowName") != WORKFLOW:
        problems.append(f"{START_TRIGGER}: {t.get('Type')} → {[a.get('JobName') for a in t.get('Actions') or []]}")
    t = (parse_json(ctx.aws("glue", "get-trigger", "--name", NEXT_TRIGGER).out) or {}).get("Trigger") or {}
    conds = (t.get("Predicate") or {}).get("Conditions") or []
    if t.get("Type") != "CONDITIONAL" or t.get("State") != "ACTIVATED" or \
            [(c.get("JobName"), c.get("State")) for c in conds] != [(JOBS["raw_to_trusted"], "SUCCEEDED")] or \
            [a.get("JobName") for a in t.get("Actions") or []] != [JOBS["trusted_to_curated"]]:
        problems.append(f"{NEXT_TRIGGER}: {t.get('Type')} {t.get('State')} {conds}")
    role = outputs.get("glue_role_arn")
    bucket = outputs.get("lake_bucket")
    for key, name in JOBS.items():
        job = (parse_json(ctx.aws("glue", "get-job", "--job-name", name).out) or {}).get("Job") or {}
        cmd = job.get("Command") or {}
        args = job.get("DefaultArguments") or {}
        if cmd.get("Name") != "pythonshell" or cmd.get("ScriptLocation") != f"s3://{bucket}/_glue/scripts/{name}.py":
            problems.append(f"{name}: comando {cmd}")
        if role and job.get("Role") != role:
            problems.append(f"{name}: rol {job.get('Role')}")
        if job.get("MaxRetries") != 0 or args.get("--lake_bucket") != bucket:
            problems.append(f"{name}: MaxRetries {job.get('MaxRetries')} · --lake_bucket {args.get('--lake_bucket')}")
        deployed = ctx.aws("s3", "cp", f"s3://{bucket}/_glue/scripts/{name}.py", "-")
        local = (GLUE_JOBS_DIR / f"{key}.py").read_bytes()
        if not deployed.ok or hashlib.sha256(deployed.out.encode("utf-8")).hexdigest() != hashlib.sha256(local).hexdigest():
            problems.append(f"{name}: el script de S3 no es src/lakehouse/glue_jobs/{key}.py (aplique C)")
    return problems


def run_summary(run: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    return job_runs_by_name(run), run.get("Statistics") or {}


def part_workflow(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Workflow de Glue, fallos en CloudWatch y horario")
    outputs = aws_outputs(ctx, part)
    bucket = lake_bucket(ctx, outputs)
    scheduler_role = outputs.get("lakehouse_scheduler_role_arn")

    problems = check_config(ctx, outputs)
    part.add("F1", "workflow, disparadores y jobs (scripts de S3 = repo)", FALLA if problems else OK,
             "\n".join(problems) if problems else
             f"{START_TRIGGER} (ON_DEMAND) → {JOBS['raw_to_trusted']} → {NEXT_TRIGGER} (SUCCEEDED) → {JOBS['trusted_to_curated']}; "
             "pythonshell 3.9 con ta-glue-role; scripts de S3 idénticos al repo")
    if problems:
        return part.finish()

    state0, sched = get_schedule(ctx)
    sp = schedule_problems(sched, scheduler_role) if sched else ["no existe"]
    part.add("F2", f"horario {SCHEDULE}", FALLA if sp else (OK if state0 == "DISABLED" else AVISO),
             "\n".join(sp) if sp else f"{state0} · rate(1 hour) UTC → glue:StartWorkflowRun({WORKFLOW}) con "
             f"{ROLES['scheduler']}; sin reintentos" + ("" if state0 == "DISABLED" else " (encendido: cobra cada hora)"),
             state=state0)

    rule = parse_json(ctx.aws("events", "describe-rule", "--name", FAIL_RULE).out) or {}
    pattern = parse_json(rule.get("EventPattern") or "") or {}
    targets = (parse_json(ctx.aws("events", "list-targets-by-rule", "--rule", FAIL_RULE).out) or {}).get("Targets") or []
    groups = (parse_json(ctx.aws("logs", "describe-log-groups", "--log-group-name-prefix", FAIL_LOG_GROUP).out) or {}).get("logGroups") or []
    policies = (parse_json(ctx.aws("logs", "describe-resource-policies").out) or {}).get("resourcePolicies") or []
    fp = []
    if rule.get("State") != "ENABLED":
        fp.append(f"regla {rule.get('State')}")
    if pattern.get("detail-type") != ["Glue Job State Change"] or "FAILED" not in ((pattern.get("detail") or {}).get("state") or []):
        fp.append(f"patrón {pattern}")
    if not any(str(t.get("Arn", "")).endswith(f"log-group:{FAIL_LOG_GROUP}") for t in targets):
        fp.append(f"destinos {[t.get('Arn') for t in targets]}")
    if not any(g.get("logGroupName") == FAIL_LOG_GROUP for g in groups):
        fp.append(f"no existe {FAIL_LOG_GROUP}")
    if not any(p.get("policyName") == FAIL_LOG_POLICY for p in policies):
        fp.append(f"falta la política de recursos {FAIL_LOG_POLICY}")
    part.add("F3", f"regla {FAIL_RULE} → {FAIL_LOG_GROUP}", FALLA if fp else OK,
             "\n".join(fp) if fp else "FAILED/TIMEOUT/STOPPED/ERROR de ta-job-* → CloudWatch Logs (retención "
             f"{next((g.get('retentionInDays') for g in groups if g.get('logGroupName') == FAIL_LOG_GROUP), '?')} días)")

    tick = not getattr(ctx.options, "no_tick", False)
    if not ctx.ask(f"¿Correr el workflow dos veces (una con fallo provocado){' y un horario de prueba de una sola vez' if tick else ''}? "
                   "Cobra ≈ 0,003 USD (Glue Python shell) y deja marcas en trusted/ y curated/ que F9 borra"):
        part.add("F4", "corrida del workflow", AVISO, "no confirmado")
        return part.finish()

    known = {r.get("WorkflowRunId") for r in workflow_runs(ctx) or []}
    started = ctx.now()
    run_id, err, tries = start_run(ctx, {"ta_validacion": "4.1"}, part)
    if not run_id:
        part.add("F4", "corrida del workflow (Raw → Trusted → Curated)", FALLA, err)
        return part.finish()
    known.add(run_id)
    part.note(f"corrida {run_id}: esperando a que terminen los dos jobs (3–6 min)…")
    run = wait_run(ctx, part, run_id)
    jobs, stats = run_summary(run)
    jr1 = (jobs.get(JOBS["raw_to_trusted"]) or [{}])[0]
    jr2 = (jobs.get(JOBS["trusted_to_curated"]) or [{}])[0]
    t1_end, t2_start = parse_time(jr1.get("CompletedOn")), parse_time(jr2.get("StartedOn"))
    ordered = bool(t1_end and t2_start and t2_start >= t1_end)
    good = (run.get("Status") == "COMPLETED" and jr1.get("JobRunState") == "SUCCEEDED" and jr2.get("JobRunState") == "SUCCEEDED"
            and ordered and stats.get("FailedActions", 0) == 0)
    durations = [jr1.get("ExecutionTime"), jr2.get("ExecutionTime")]
    part.add("F4", "corrida del workflow (Raw → Trusted → Curated)", OK if good else FALLA,
             f"{run_id}: {run.get('Status')}{' (tiempo agotado)' if run.get('_timeout') else ''} · "
             f"{JOBS['raw_to_trusted']} {jr1.get('JobRunState')} ({jr1.get('ExecutionTime')} s) → "
             f"{JOBS['trusted_to_curated']} {jr2.get('JobRunState')} ({jr2.get('ExecutionTime')} s) · "
             f"{'en orden' if ordered else 'ORDEN NO COMPROBADO'} · {stats}"
             + (f" · arrancó al intento {tries} (ConcurrentRunsExceededException)" if tries > 1 else "")
             + (f"\n{jr1.get('ErrorMessage') or jr2.get('ErrorMessage')}" if not good and (jr1.get("ErrorMessage") or jr2.get("ErrorMessage")) else ""),
             run_id=run_id, job_runs={JOBS["raw_to_trusted"]: jr1.get("Id"), JOBS["trusted_to_curated"]: jr2.get("Id")},
             durations=durations, start_attempts=tries)

    found, details = [], []
    for name, jr in ((JOBS["raw_to_trusted"], jr1), (JOBS["trusted_to_curated"], jr2)):
        if not jr.get("Id"):
            continue
        lines = json_lines(find_log_lines(ctx, GLUE_OUTPUT_LOG, jr["Id"], "glue_placeholder_done", started, wait_s=120))
        done = next((x for x in lines if x.get("event") == "glue_placeholder_done"), None)
        if done:
            found.append(name)
            details.append(f"{name}: {done.get('input_objects')} objetos bajo {done.get('input_prefix')}, lectura {done.get('read_probe')}"
                           + (f", marca del anterior {'leída' if done.get('upstream_marker_found') else 'NO encontrada'}"
                              if done.get("upstream_marker_found") is not None else ""))
    markers = [p for p in (f"trusted/_ta_placeholder/{JOBS['raw_to_trusted']}/{run_id}.json",
                           f"curated/_ta_placeholder/{JOBS['trusted_to_curated']}/{run_id}.json")
               if ctx.aws("s3api", "head-object", "--bucket", bucket, "--key", p).ok]
    upstream_ok = all("NO encontrada" not in d for d in details)
    part.add("F5", "logs en CloudWatch y marcas en S3", OK if len(found) == 2 and len(markers) == 2 and upstream_ok else FALLA,
             "\n".join(details) + f"\nlogs en {GLUE_OUTPUT_LOG}: {len(found)} de 2 · marcas en S3: {len(markers)} de 2")

    run_id2, err, tries2 = start_run(ctx, {"ta_validacion": "4.1", "ta_force_fail": "raw_to_trusted"}, part)
    if not run_id2:
        part.add("F6", "corrida con fallo provocado (el segundo job no corre)", FALLA, err, start_attempts=tries2)
        part.add("F7", "el fallo queda en CloudWatch", OMITIDO, "no se comprobó: F6 no pudo arrancar la corrida con fallo provocado")
    else:
        known.add(run_id2)
        part.note(f"corrida {run_id2} con ta_force_fail=raw_to_trusted (1–3 min)…")
        run2 = wait_run(ctx, part, run_id2)
        jobs2, stats2 = run_summary(run2)
        f1 = (jobs2.get(JOBS["raw_to_trusted"]) or [{}])[0]
        never = not jobs2.get(JOBS["trusted_to_curated"])
        ok6 = f1.get("JobRunState") == "FAILED" and never
        part.add("F6", "corrida con fallo provocado (el segundo job no corre)", OK if ok6 else FALLA,
                 f"{run_id2}: {run2.get('Status')} · {JOBS['raw_to_trusted']} {f1.get('JobRunState')} "
                 f"(ErrorMessage: {(f1.get('ErrorMessage') or '—')[:160]}) · {JOBS['trusted_to_curated']} "
                 f"{'no corrió' if never else 'CORRIÓ'} · {stats2}"
                 + (f" · arrancó al intento {tries2} (ConcurrentRunsExceededException)" if tries2 > 1 else ""),
                 run_id=run_id2, job_run=f1.get("Id"), error_message=f1.get("ErrorMessage"), start_attempts=tries2)
        if f1.get("Id"):
            part.note("buscando el fallo en CloudWatch (EventBridge tarda hasta un par de minutos)…")
            events = find_log_lines(ctx, FAIL_LOG_GROUP, "", f1["Id"], started, wait_s=240)
            errors = find_log_lines(ctx, GLUE_ERROR_LOG, f1["Id"], "ta_force_fail", started, wait_s=60)
            if not errors:
                errors = find_log_lines(ctx, GLUE_OUTPUT_LOG, f1["Id"], "glue_job_forced_failure", started, wait_s=20)
            ev = json_lines(events)
            state = ((ev[0].get("detail") or {}).get("state")) if ev else None
            part.add("F7", "el fallo queda en CloudWatch", OK if events and errors else (AVISO if events else FALLA),
                     f"{FAIL_LOG_GROUP}: {'evento ' + str(state) + ' de ' + f1['Id'] if events else 'sin evento'} · "
                     f"logs del job: {'el fallo provocado aparece' if errors else 'sin rastro del fallo'} "
                     f"({GLUE_ERROR_LOG} o {GLUE_OUTPUT_LOG})")
        else:
            part.add("F7", "el fallo queda en CloudWatch", FALLA, "no hubo job run que buscar")

    busy = wait_idle(ctx, part) if tick else []
    if tick and busy:
        part.add("F8", "horario de prueba (EventBridge Scheduler → workflow)", FALLA,
                 f"no se creó: el workflow sigue con corridas en curso ({', '.join(busy)}) y el horario de prueba, "
                 "sin reintentos como el real, perdería su disparo por ConcurrentRunsExceededException")
    elif tick:
        # at() queda ≥ 90 s después: más que los segundos que tarda Glue en liberar el cupo de la corrida anterior.
        name = SCHEDULE_TEST_PREFIX + ctx.now().strftime("%Y%m%d%H%M%S")
        expr, at = at_expression(ctx.now())
        body = {"Name": name, "GroupName": "default", "ScheduleExpression": expr, "ScheduleExpressionTimezone": "UTC",
                "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED", "ActionAfterCompletion": "DELETE",
                "Description": "validate_4_1.py F8: horario de prueba de una sola vez (se borra solo)",
                "Target": {"Arn": SCHEDULER_TARGET, "RoleArn": scheduler_role, "Input": json.dumps({"Name": WORKFLOW}),
                           "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 600}}}
        res = aws_with_input(ctx, ["scheduler", "create-schedule"], body)
        ev = ctx.load_evidence()
        ev.setdefault("temp_schedules", []).append({"name": name, "at": at.isoformat(), "cleaned": False})
        ctx.save_evidence(ev)
        if not res.ok:
            part.add("F8", "horario de prueba (EventBridge Scheduler → workflow)", FALLA, res.tail(4))
        else:
            part.note(f"{name} = {expr} UTC con {ROLES['scheduler']}; esperando su corrida (hasta 6 min)…")
            new_run = None
            deadline = at + timedelta(minutes=6)
            polls = int(max(0.0, (deadline - ctx.now()).total_seconds()) // 20) + 2
            for _ in range(polls):
                if new_run is not None or ctx.now() > deadline:
                    break
                for r in workflow_runs(ctx) or []:
                    rid = r.get("WorkflowRunId")
                    st = parse_time(r.get("StartedOn"))
                    if rid not in known and st and st >= at - timedelta(seconds=60):
                        new_run = rid
                        break
                if new_run is None:
                    ctx.sleep(20)
            if new_run:
                run3 = wait_run(ctx, part, new_run)
                jobs3, _ = run_summary(run3)
                ok8 = all((jobs3.get(j) or [{}])[0].get("JobRunState") == "SUCCEEDED" for j in JOBS.values())
                gone, _ = get_schedule(ctx, name)
                auto = gone == "NO_EXISTE"
                if not auto:
                    ctx.aws("scheduler", "delete-schedule", "--name", name)
                    gone, _ = get_schedule(ctx, name)
                status8 = FALLA if not ok8 or gone != "NO_EXISTE" else (OK if auto else AVISO)
                part.add("F8", "horario de prueba (EventBridge Scheduler → workflow)", status8,
                         f"{expr}: corrida {new_run} creada por el Scheduler con {ROLES['scheduler']} · jobs "
                         f"{'SUCCEEDED' if ok8 else {j: (jobs3.get(j) or [{}])[0].get('JobRunState') for j in JOBS.values()}} · "
                         "horario de prueba " + ("borrado solo" if auto else (
                             "borrado por el validador (ActionAfterCompletion no lo borró)" if gone == "NO_EXISTE" else f"sigue: {gone}")),
                         run_id=new_run)
            else:
                ctx.aws("scheduler", "delete-schedule", "--name", name)
                part.add("F8", "horario de prueba (EventBridge Scheduler → workflow)", FALLA,
                         f"no apareció una corrida nueva hasta {deadline.strftime('%H:%M')} UTC: revise los permisos de "
                         f"{ROLES['scheduler']} (D8) y la métrica TargetErrorCount de AWS/Scheduler")
            gone, _ = get_schedule(ctx, name)
            ev = ctx.load_evidence()
            for item in ev.get("temp_schedules", []):
                if item.get("name") == name:
                    item["cleaned"] = gone == "NO_EXISTE"
            ctx.save_evidence(ev)
    else:
        part.add("F8", "horario de prueba (EventBridge Scheduler → workflow)", OMITIDO, "--no-tick")

    deleted, problems = delete_markers(ctx, bucket)
    part.add("F9", "marcas de los marcadores de posición borradas", AVISO if problems else OK,
             f"{deleted} objetos borrados de {' y '.join(PLACEHOLDER_PREFIXES)}" + (f"; {problems}" if problems else ""))

    state1, _ = get_schedule(ctx)
    part.add("F10", "el horario sigue como estaba", OK if state1 == state0 else AVISO,
             f"{state1}" + ("" if state1 == "DISABLED" else " (encendido: python project/scripts/validate_4_1.py off)"))

    costs = [job_cost(d) for d in durations if d is not None]
    per_run = sum(costs) if len(costs) == 2 else 2 * job_cost(None)
    part.add("F11", "costo con lo medido", OK,
             f"jobs de {', '.join(str(d) for d in durations)} s (se cobra ≥ 60 s × {PYSHELL_DPU} DPU × {DPU_USD_PER_HOUR} USD/DPU-h) → "
             f"≈ {per_run:.5f} USD por corrida y ≈ {per_run * RUNS_PER_DAY:.3f} USD/día con el horario encendido "
             f"(≈ {per_run * RUNS_PER_DAY * 30:.2f} USD/mes; con los jobs reales de 4.2/4.3, más)",
             usd_per_run=round(per_run, 6), usd_per_day=round(per_run * RUNS_PER_DAY, 4), durations=durations)
    return part.finish()


# --- status, off y on -----------------------------------------------------------------------------


def running_job_runs(ctx: Ctx) -> dict[str, list[str]]:
    out = {}
    for name in JOBS.values():
        res = ctx.aws("glue", "get-job-runs", "--job-name", name, "--max-results", "10")
        runs = (parse_json(res.out) or {}).get("JobRuns") or [] if res.ok else []
        out[name] = [r.get("Id") for r in runs if r.get("JobRunState") in ("STARTING", "RUNNING", "STOPPING", "WAITING")]
    return out


def temp_schedules(ctx: Ctx) -> list[str]:
    res = ctx.aws("scheduler", "list-schedules", "--name-prefix", SCHEDULE_TEST_PREFIX)
    return [s.get("Name") for s in (parse_json(res.out) or {}).get("Schedules") or []] if res.ok else []


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del lakehouse (solo lectura)")
    state, sched = get_schedule(ctx)
    part.add("S1", f"horario {SCHEDULE}", OK if state in ("DISABLED", "NO_EXISTE") else AVISO,
             f"{state or sched.get('error')}" + (" (cobra: arranca el workflow cada hora)" if state == "ENABLED" else ""))
    runs = workflow_runs(ctx, 10)
    running = [r.get("WorkflowRunId") for r in (runs or []) if r.get("Status") in ("RUNNING", "STOPPING")]
    recent = [f"{r.get('WorkflowRunId')} {r.get('Status')} {(r.get('StartedOn') or '')[:19]}" for r in (runs or [])[:3]]
    part.add("S2", "corridas del workflow", AVISO if running else OK,
             (f"en curso: {', '.join(running)}; " if running else "ninguna en curso; ")
             + ("últimas: " + "; ".join(recent) if recent else ("sin corridas" if runs is not None else "sin dato")))
    jobs = running_job_runs(ctx)
    busy = {k: v for k, v in jobs.items() if v}
    part.add("S3", "job runs en curso", AVISO if busy else OK, str(busy) if busy else "ninguno")
    temps = temp_schedules(ctx)
    part.add("S4", "horarios de prueba de F8", AVISO if temps else OK, ", ".join(temps) if temps else "ninguno")
    return part.finish()


def part_off(ctx: Ctx) -> int:
    part = Part(ctx, "off", "Apagado (estado de ahorro del lakehouse)")
    state, _ = get_schedule(ctx)
    if state == "ENABLED":
        ok, detail = set_schedule_state(ctx, "DISABLED")
        part.add("O1", "deshabilitar el horario", OK if ok else FALLA, detail)
    else:
        part.add("O1", "deshabilitar el horario", OK if state in ("DISABLED", "NO_EXISTE") else AVISO, f"estado {state}")

    stopped, still = [], []
    for r in workflow_runs(ctx, 25) or []:
        if r.get("Status") == "RUNNING":
            res = ctx.aws("glue", "stop-workflow-run", "--name", WORKFLOW, "--run-id", r.get("WorkflowRunId"))
            (stopped if res.ok else still).append(r.get("WorkflowRunId"))
    for name, ids in running_job_runs(ctx).items():
        if ids:
            res = ctx.aws("glue", "batch-stop-job-run", "--job-name", name, "--job-run-ids", *ids)
            (stopped if res.ok else still).extend(ids)
    part.add("O2", "corridas y job runs", OK if not still else FALLA,
             (f"detenidos: {', '.join(stopped)}; " if stopped else "") + ("nada en curso" if not still else f"siguen: {still}"))

    temps = temp_schedules(ctx)
    for name in temps:
        ctx.aws("scheduler", "delete-schedule", "--name", name)
    left = temp_schedules(ctx) if temps else []
    ev = ctx.load_evidence()
    for item in ev.get("temp_schedules", []):
        item["cleaned"] = item.get("name") not in left
    ctx.save_evidence(ev)
    part.add("O3", "horarios de prueba de F8", OK if not left else FALLA,
             (f"borrados: {', '.join(temps)}" if temps else "ninguno") + (f"; siguen: {left}" if left else ""))

    outputs = aws_outputs(ctx, part)
    deleted, problems = delete_markers(ctx, lake_bucket(ctx, outputs))
    part.add("O4", "marcas de los marcadores de posición", AVISO if problems else OK,
             f"{deleted} borradas" + (f"; {problems}" if problems else ""))

    lines = services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("O5", "status.py (lo demás que cobra)", AVISO if charging else OK,
             "\n".join(lines) + ("\nsigue cobrando; si ningún lote lo necesita: python project/scripts/off.py" if charging else ""))
    return part.finish()


def part_on(ctx: Ctx) -> int:
    part = Part(ctx, "on", "Encender el horario del lakehouse")
    cost = ctx.step_data("F", "F11").get("usd_per_day") or round(2 * job_cost(None) * RUNS_PER_DAY, 4)
    if not ctx.ask(f"¿Encender {SCHEDULE} (arranca {WORKFLOW} cada hora; ≈ {cost} USD/día con los jobs actuales)?"):
        part.add("N1", "encender el horario", AVISO, "no confirmado")
        return part.finish()
    ok, detail = set_schedule_state(ctx, "ENABLED")
    part.add("N1", "encender el horario", OK if ok else FALLA, detail + "; se apaga con: python project/scripts/validate_4_1.py off")
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "terraform": part_terraform,
    "catalog": part_catalog,
    "athena": part_athena,
    "workflow": part_workflow,
    "status": part_status,
    "off": part_off,
    "on": part_on,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_4_1.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (solo para el estado de status.py)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan (pide confirmación)")
    parser.add_argument("--reconcile-historical", action="store_true", help="E2: contar también los pies de los 12.102 Parquet del histórico")
    parser.add_argument("--no-tick", action="store_true", help="F8: sin el horario de prueba de una sola vez")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None,
         s3_rows: Callable[[str, str], dict[str, Any]] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE, s3_rows=s3_rows)
    printer(f"Lote 4.1 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Para volver al estado de ahorro: python project/scripts/validate_4_1.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
