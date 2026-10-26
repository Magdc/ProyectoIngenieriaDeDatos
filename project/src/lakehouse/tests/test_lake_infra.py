"""Terraform del Lote 4.1 sin nube: nombres de §5.7, mínimo privilegio (§5.8), estado de ahorro,
outputs de §5.9 y que los archivos nuevos encajen con la raíz de ``infra/aws/environments/dev``.

``terraform validate`` y el ``plan`` los corre ``validate_4_1.py terraform`` (parte C).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[3]
AWS = PROJECT / "infra" / "aws"
ENV = AWS / "environments" / "dev"
MODULES = ("glue_catalog", "athena_workgroup", "glue_role", "quicksight_role", "glue_workflow",
           "lakehouse_schedule", "glue_failure_events")
ENV_FILES = ("lakehouse.tf", "variables_lakehouse.tf", "outputs_lakehouse.tf")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def code(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def block(text: str, header_regex: str) -> str:
    m = re.search(header_regex + r"\s*\{", text)
    assert m, f"no está el bloque {header_regex}"
    depth, i = 1, m.end()
    while depth:
        depth += {"{": 1, "}": -1}.get(text[i], 0)
        i += 1
    return text[m.end():i - 1]


def module_text(name: str) -> str:
    return code(read(AWS / "modules" / name / "main.tf"))


def test_archivos_del_lote():
    for mod in MODULES:
        assert (AWS / "modules" / mod / "main.tf").is_file(), mod
    assert (AWS / "modules" / "glue_catalog" / "tables.json").is_file()
    for name in ENV_FILES:
        assert (ENV / name).is_file(), name
    for rel in ("glue_jobs/raw_to_trusted.py", "glue_jobs/trusted_to_curated.py",
                "queries/raw_conteo_por_origen.sql", "queries/raw_sentimiento_7_dias.sql"):
        assert (PROJECT / "src" / "lakehouse" / rel).is_file(), rel


def test_nombres_de_la_seccion_5_7():
    tf = code(read(ENV / "lakehouse.tf"))
    for name in ('"ta_raw"', '"ta_trusted"', '"ta_curated"', '"ta-analytics"', '"ta-glue-role"', '"ta-quicksight-role"',
                 '"ta-wf-raw-to-curated"', '"ta-job-raw-to-trusted"', '"ta-job-trusted-to-curated"', '"athena-results/"'):
        assert name in tf, name
    assert 'default     = "rate(1 hour)"' in read(ENV / "variables_lakehouse.tf")


def test_variables_declaradas_y_sin_nombres_repetidos():
    texts = {p.name: code(read(p)) for p in ENV.glob("*.tf")}
    declared = {v for t in texts.values() for v in re.findall(r'^variable\s+"([^"]+)"', t, flags=re.M)}
    used = set()
    for name in ENV_FILES:
        used |= set(re.findall(r"\bvar\.([a-z0-9_]+)", texts[name]))
    assert not used - declared, f"sin declarar: {sorted(used - declared)}"
    for kind in ("variable", "output", "module"):
        names = [n for t in texts.values() for n in re.findall(rf'^{kind}\s+"([^"]+)"', t, flags=re.M)]
        assert len(names) == len(set(names)), f"{kind} repetidos"


def test_outputs_de_la_seccion_5_9_existen():
    outputs = set()
    for p in ENV.glob("outputs*.tf"):
        outputs |= set(re.findall(r'^output\s+"([^"]+)"', read(p), flags=re.M))
    hu007 = {"lake_bucket", "glue_db_names", "athena_workgroup", "exporter_role_arn", "glue_role_arn", "kms_key_arn"}
    assert hu007 <= outputs, hu007 - outputs
    lote = {"quicksight_role_arn", "glue_workflow_name", "glue_job_names", "lakehouse_schedule_name",
            "lakehouse_scheduler_role_arn", "glue_failures_log_group", "athena_results_location"}
    assert lote <= outputs, lote - outputs
    # Los outputs del lote dependen de módulos (no solo de variables): se guardan con -target.
    for name, body in re.findall(r'output\s+"([^"]+)"\s*\{(.*?)\n\}', read(ENV / "outputs_lakehouse.tf"), flags=re.S):
        assert "module." in body, name


def test_horario_creado_deshabilitado_y_estado_ignorado():
    text = module_text("lakehouse_schedule")
    sched = block(text, r'resource\s+"aws_scheduler_schedule"\s+"this"')
    assert re.search(r"\bstate\s*=\s*var\.initial_state", sched)
    assert re.search(r"ignore_changes\s*=\s*\[state\]", sched)
    assert '"arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun"' in sched
    assert re.search(r'mode\s*=\s*"OFF"', sched) and re.search(r"maximum_retry_attempts\s*=\s*0\b", sched)
    assert re.search(r'initial_state\s*=\s*"DISABLED"', code(read(ENV / "lakehouse.tf")))
    assert re.search(r'variable "initial_state"[^}]*default\s*=\s*"DISABLED"', text, flags=re.S)
    policy = block(text, r'data\s+"aws_iam_policy_document"\s+"start"')
    assert '"glue:StartWorkflowRun"' in policy and "var.workflow_arn" in policy and '"*"' not in policy
    assert "aws:SourceAccount" in block(text, r'data\s+"aws_iam_policy_document"\s+"trust"')


def test_rol_de_glue_no_escribe_raw_y_usa_kms_via_s3():
    text = module_text("glue_role")
    assert re.search(r'default\s*=\s*\["trusted/", "curated/", "quality/", "_glue/temp/"\]', text)
    assert '!startswith(p, "raw/")' in text
    perms = block(text, r'data\s+"aws_iam_policy_document"\s+"permissions"')
    assert '"s3:prefix"' in perms and '"kms:ViaService"' in perms
    assert "s3:*" not in perms and "glue:*" not in perms and '"glue:CreateTable"' not in perms
    assert '"glue:DeleteTable"' not in perms and '"glue:UpdateTable"' not in perms
    assert '"glue.amazonaws.com"' in text


def test_rol_de_quicksight_solo_curated_y_su_workgroup():
    text = module_text("quicksight_role")
    perms = block(text, r'data\s+"aws_iam_policy_document"\s+"permissions"')
    assert "athena:*" not in perms and "s3:*" not in perms
    assert "workgroup/${var.workgroup_name}" in perms
    assert re.search(r'variable "read_prefixes"[^}]*default\s*=\s*\["curated/"\]', text, flags=re.S)
    assert '"athena:DeleteWorkGroup"' not in perms and '"athena:UpdateWorkGroup"' not in perms
    assert '"quicksight.amazonaws.com"' in text
    root = code(read(ENV / "lakehouse.tf"))
    assert "database_names = [local.lakehouse_databases.curated]" in root


def test_workgroup_forzado_con_limite_y_sse_kms():
    text = module_text("athena_workgroup")
    wg = block(text, r'resource\s+"aws_athena_workgroup"\s+"this"')
    assert re.search(r"enforce_workgroup_configuration\s*=\s*true", wg)
    assert re.search(r"bytes_scanned_cutoff_per_query\s*=\s*var\.bytes_scanned_cutoff_per_query", wg)
    assert '"SSE_KMS"' in wg and "var.kms_key_arn" in wg
    assert re.search(r'variable "athena_bytes_scanned_cutoff_per_query"[^}]*default\s*=\s*1073741824',
                     read(ENV / "variables_lakehouse.tf"), flags=re.S)


def test_workflow_en_orden_con_disparador_condicional():
    text = module_text("glue_workflow")
    start = block(text, r'resource\s+"aws_glue_trigger"\s+"start"')
    nxt = block(text, r'resource\s+"aws_glue_trigger"\s+"next"')
    assert '"ON_DEMAND"' in start and "local.first_key" in start
    # Con enabled = true el proveedor llama StartTrigger al crearlo y eso corre el workflow (v2.32).
    assert re.search(r"\benabled\s*=\s*false\b", start) and re.search(r"ignore_changes\s*=\s*\[enabled\]", start)
    assert '"CONDITIONAL"' in nxt and re.search(r'state\s*=\s*"SUCCEEDED"', nxt)
    job = block(text, r'resource\s+"aws_glue_job"\s+"this"')
    assert re.search(r"max_retries\s*=\s*0\b", job) and re.search(r"max_concurrent_runs\s*=\s*1\b", job)
    assert re.search(r"source_hash\s*=\s*filemd5\(each\.value\.script_path\)", text)  # con SSE-KMS el ETag no es el MD5
    assert not re.search(r"^\s*etag\s*=", text, flags=re.M)
    assert '"/aws-glue/python-jobs/output"' in text


def test_fallos_de_glue_a_cloudwatch():
    text = module_text("glue_failure_events")
    rule = block(text, r'resource\s+"aws_cloudwatch_event_rule"\s+"this"')
    assert '"Glue Job State Change"' in rule and '"aws.glue"' in rule and re.search(r'state\s*=\s*"ENABLED"', rule)
    assert re.search(r'default\s*=\s*\["FAILED", "TIMEOUT", "STOPPED", "ERROR"\]', text)
    assert 'resource "aws_cloudwatch_log_resource_policy" "this"' in text


def test_catalogo_lee_tables_json_y_sustituye_el_bucket():
    text = module_text("glue_catalog")
    assert 'jsondecode(file(' in text and "tables.json" in text
    assert text.count('"{{lake_bucket}}"') >= 3
    assert '"EXTERNAL_TABLE"' in text and "ParquetHiveSerDe" in text


def test_sin_cuenta_ni_secretos_en_el_codigo():
    paths = [ENV / n for n in ENV_FILES] + [AWS / "modules" / m / "main.tf" for m in MODULES]
    for path in paths:
        text = read(path)
        assert "648617469176" not in text, path
        assert not re.search(r"AKIA[0-9A-Z]{16}", text), path


def test_consultas_de_ejemplo_sobre_posts_raw():
    for name in ("raw_conteo_por_origen.sql", "raw_sentimiento_7_dias.sql"):
        sql = "\n".join(ln for ln in read(PROJECT / "src" / "lakehouse" / "queries" / name).splitlines()
                        if not ln.strip().startswith("--"))
        assert "ta_raw.posts_raw" in sql and ";" not in sql  # un solo enunciado, sin ';' (Athena)


@pytest.mark.skipif(shutil.which("terraform") is None, reason="sin terraform en el PATH")
def test_terraform_fmt():
    paths = [str(ENV / n) for n in ENV_FILES] + [str(AWS / "modules" / m) for m in MODULES]
    proc = subprocess.run(["terraform", "fmt", "-check", "-recursive", *paths], capture_output=True, text=True,
                          stdin=subprocess.DEVNULL)
    assert proc.returncode == 0, proc.stdout + proc.stderr
