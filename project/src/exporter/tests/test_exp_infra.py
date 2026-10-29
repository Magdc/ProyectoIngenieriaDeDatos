"""Terraform del Lote 3.2 (AWS y GCP): nombres, mínimo privilegio, estado de ahorro y el
encaje con las raíces reales de ``infra/*/environments/dev`` (sin red ni credenciales)."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from exporter.config import load_config

pytestmark = pytest.mark.repo

PROJECT = Path(__file__).resolve().parents[3]
INFRA = PROJECT / "infra"
AWS_MOD = INFRA / "aws/modules/gcp_exporter_role/main.tf"
AWS_ENV = INFRA / "aws/environments/dev"
GCP_MOD = INFRA / "gcp/modules/cloud_run_exporter/main.tf"
GCP_ENV = INFRA / "gcp/environments/dev"
LOT_FILES = [
    AWS_MOD,
    AWS_ENV / "exporter.tf",
    AWS_ENV / "variables_exporter.tf",
    AWS_ENV / "outputs_exporter.tf",
    GCP_MOD,
    GCP_ENV / "exporter.tf",
    GCP_ENV / "variables_exporter.tf",
    GCP_ENV / "outputs_exporter.tf",
]


def _read(path: Path) -> str:
    if not path.exists():
        pytest.fail(f"falta {path.relative_to(PROJECT)}")
    return path.read_text(encoding="utf-8")


def _strip_comments(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def block(text: str, header_regex: str) -> str:
    """Cuerpo del primer bloque cuyo encabezado coincide (llaves balanceadas)."""
    match = re.search(header_regex + r"\s*\{", text)
    assert match, f"no está el bloque {header_regex}"
    depth, i = 1, match.end()
    while depth:
        depth += {"{": 1, "}": -1}.get(text[i], 0)
        i += 1
    return text[match.end() : i - 1]


def declared_variables(root: Path) -> set[str]:
    names: set[str] = set()
    for tf in root.glob("*.tf"):
        names.update(re.findall(r'^variable\s+"([^"]+)"', tf.read_text(encoding="utf-8"), flags=re.M))
    return names


def test_estan_los_archivos_del_lote():
    for path in LOT_FILES:
        assert path.exists(), path


# --- AWS --------------------------------------------------------------------------------


def test_confianza_solo_para_sa_exporter_con_el_idp_integrado_de_google():
    text = _strip_comments(_read(AWS_MOD))
    trust = block(text, r'data\s+"aws_iam_policy_document"\s+"trust"')
    assert '"sts:AssumeRoleWithWebIdentity"' in trust
    assert re.search(r'type\s*=\s*"Federated"', trust) and '["accounts.google.com"]' in trust
    for key, values in (
        ("accounts.google.com:aud", "[var.gcp_sa_unique_id, var.gcp_sa_email, var.audience]"),
        ("accounts.google.com:oaud", "[var.audience]"),
        ("accounts.google.com:sub", "[var.gcp_sa_unique_id]"),
    ):
        cond = re.search(rf'test\s*=\s*"StringEquals"\s*variable\s*=\s*"{re.escape(key)}"\s*values\s*=\s*(\[[^\]]*\])', trust)
        assert cond and cond.group(1) == values, key
    assert "aws_iam_openid_connect_provider" not in text  # Google ya está integrado en AWS


def test_politica_de_minimo_privilegio_solo_en_raw_stream():
    text = _strip_comments(_read(AWS_MOD))
    policy = block(text, r'data\s+"aws_iam_policy_document"\s+"s3"')
    lists = re.findall(r"actions\s*=\s*\[([^\]]*)\]", policy)
    actions = sorted({a for chunk in lists for a in re.findall(r'"([^"]+)"', chunk)})
    assert actions == ["kms:GenerateDataKey", "s3:ListBucket", "s3:PutObject"], actions
    assert "${var.bucket_name}/${var.object_prefix}*" in policy
    assert re.search(r'variable\s*=\s*"s3:prefix"\s*values\s*=\s*\["\$\{var\.object_prefix\}\*"\]', policy)
    assert re.search(r'variable\s*=\s*"kms:ViaService"', policy)
    assert re.search(r'default\s*=\s*"raw/origin=stream/"', text)
    for forbidden in ("s3:GetObject", "s3:DeleteObject", "s3:HeadObject", '"s3:*"', '"kms:*"', "kms:Decrypt"):
        assert forbidden not in policy, forbidden


def test_raiz_aws_crea_el_rol_con_su_nombre_y_solo_con_el_uniqueid():
    text = _strip_comments(_read(AWS_ENV / "exporter.tf"))
    assert re.search(r'source\s*=\s*"\.\./\.\./modules/gcp_exporter_role"', text)  # terraform fmt alinea el =
    assert re.search(r'role_name\s*=\s*"ta-gcp-exporter-role"', text)
    assert re.search(r'count\s*=\s*local\.exporter_role_create \? 1 : 0', text)
    assert 'alias/ta-${var.env}-lake' in text and '"ta-${var.env}-${var.aws_account_id}-lake"' in text
    variables = _read(AWS_ENV / "variables_exporter.tf")
    assert re.search(r'variable "gcp_exporter_sa_unique_id"[^}]*default\s*=\s*""', variables, flags=re.S)


def test_raiz_aws_usa_variables_declaradas():
    used = set(re.findall(r"\bvar\.([a-z0-9_]+)", _read(AWS_ENV / "exporter.tf")))
    missing = used - declared_variables(AWS_ENV)
    assert not missing, f"variables sin declarar en infra/aws/environments/dev: {sorted(missing)}"


# --- GCP --------------------------------------------------------------------------------


def _resources(text: str, rtype: str) -> dict[str, str]:
    out = {}
    for name in re.findall(rf'^resource\s+"{rtype}"\s+"([^"]+)"', text, flags=re.M):
        out[name] = block(text, rf'resource\s+"{rtype}"\s+"{name}"')
    return out


def test_sa_exporter_ya_no_lee_todo_el_lake():
    """Ningún objectViewer/objectUser de sa-exporter en el lake sin condición (HU-025)."""
    for tf in GCP_ENV.glob("*.tf"):
        text = _strip_comments(tf.read_text(encoding="utf-8"))
        for name, body in _resources(text, "google_storage_bucket_iam_member").items():
            if "module.sa_exporter.member" in body:
                assert "condition" in body, f"{tf.name}: {name} sin condición"
    platform = _read(GCP_ENV / "platform.tf")
    assert 'resource "google_storage_bucket_iam_member" "exporter_lake_read"' not in platform


def test_condicion_de_lectura_solo_enriched():
    text = _strip_comments(_read(GCP_ENV / "exporter.tf"))
    body = _resources(text, "google_storage_bucket_iam_member")["exporter_lake_enriched"]
    assert re.search(r'role\s*=\s*"roles/storage.objectViewer"', body)
    assert 'storage.googleapis.com/Bucket' in body and 'storage.googleapis.com/Object' in body
    assert 'projects/_/buckets/${module.lake_bucket.name}/objects/enriched/' in text
    assert "raw/" not in body


def test_token_oidc_solo_sobre_sa_exporter():
    text = _strip_comments(_read(GCP_ENV / "exporter.tf"))
    body = _resources(text, "google_service_account_iam_member")["exporter_oidc_token_creator"]
    assert '"roles/iam.serviceAccountOpenIdTokenCreator"' in body
    assert "module.sa_exporter.email" in body and "var.exporter_oidc_token_creators" in body
    assert "serviceAccountTokenCreator" not in text  # no el rol amplio (access tokens)


def test_job_y_scheduler_en_estado_de_ahorro():
    text = _strip_comments(_read(GCP_MOD))
    sched = block(text, r'resource\s+"google_cloud_scheduler_job"\s+"this"')
    assert re.search(r"paused\s*=\s*true", sched)
    assert re.search(r"ignore_changes\s*=\s*\[paused\]", sched)
    assert ":run" in sched and "oauth_token" in sched and "service_account_email = var.service_account_email" in sched
    job = block(text, r'resource\s+"google_cloud_run_v2_job"\s+"this"')
    assert re.search(r"deletion_protection\s*=\s*false", job)
    assert "service_account = var.service_account_email" in job and 'args  = ["run"]' in job
    invoker = block(text, r'resource\s+"google_cloud_run_v2_job_iam_member"\s+"scheduler_invoker"')
    assert '"roles/run.invoker"' in invoker and "google_cloud_run_v2_job.this.name" in invoker
    assert re.search(r'variable "schedule"\s*\{\s*type\s*=\s*string\s*default\s*=\s*"\*/15 \* \* \* \*"', text)


def test_raiz_gcp_crea_el_job_solo_con_imagen_y_valores_de_aws():
    text = _strip_comments(_read(GCP_ENV / "exporter.tf"))
    assert re.search(r'count\s*=\s*local\.exporter_create_job \? 1 : 0', text)
    for var in ("exporter_image", "exporter_role_arn", "exporter_s3_bucket", "exporter_kms_key_arn"):
        assert f'var.{var} != ""' in text
    assert 'job_name              = "ta-gcs-to-s3"' in text


def test_variables_de_entorno_del_job_las_lee_el_exportador():
    text = _strip_comments(_read(GCP_ENV / "exporter.tf"))
    env_block = block(text, r"env_vars\s*=")
    names = set(re.findall(r"^\s*([A-Z][A-Z0-9_]+)\s*=", env_block, flags=re.M))
    import inspect

    from exporter import config

    source = inspect.getsource(config)
    read = set(re.findall(r'env\.get\("([A-Z0-9_]+)"\)', source)) | set(re.findall(r'_int\(env, "([A-Z0-9_]+)"', source))
    informative = {"ENV", "GCP_REGION"}
    assert names - informative <= read, sorted(names - informative - read)
    fake = {n: "x" for n in names}
    fake.update(
        GCP_PROJECT="p", TA_LAKE_BUCKET="b", TA_LAKE_KMS_KEY_ARN="arn:aws:kms:us-east-1:648617469176:key/1-2",
        TA_EXPORTER_ROLE_ARN="arn:aws:iam::648617469176:role/ta-gcp-exporter-role", TA_EXPORT_WORKERS="8",
        TA_EXPORT_MAX_OBJECTS="5000", TA_EXPORT_OVERLAP_SECONDS="3600", TA_EXPORT_STATE_KEY="export-state/watermark.json",
    )
    load_config(fake)  # todo lo obligatorio está en el job


def test_raiz_gcp_usa_nombres_que_existen():
    text = _read(GCP_ENV / "exporter.tf") + _read(GCP_ENV / "outputs_exporter.tf")
    platform = _read(GCP_ENV / "platform.tf")
    for module in set(re.findall(r"\bmodule\.([a-z0-9_]+)", text)) - {"exporter_job"}:
        assert re.search(rf'^module\s+"{module}"', platform, flags=re.M), f"module.{module} no está en platform.tf"
    for local in set(re.findall(r"\blocal\.([a-z0-9_]+)", text)):
        assert re.search(rf"^\s*{local}\s*=", platform + text, flags=re.M), f"local.{local}"
    missing = set(re.findall(r"\bvar\.([a-z0-9_]+)", text)) - declared_variables(GCP_ENV)
    assert not missing, f"variables sin declarar en infra/gcp/environments/dev: {sorted(missing)}"


def test_outputs_del_lote_dependen_de_recursos():
    """Con -target, Terraform solo guarda los outputs que dependen de recursos (Lote 2.4)."""
    for path in (AWS_ENV / "outputs_exporter.tf", GCP_ENV / "outputs_exporter.tf"):
        text = _strip_comments(_read(path))
        for name in re.findall(r'^output\s+"([^"]+)"', text, flags=re.M):
            body = block(text, rf'output\s+"{name}"')
            assert "module." in body or "depends_on" in body, f"{path.name}: {name}"


def test_ejemplos_de_auto_tfvars_coinciden_con_las_variables():
    for root, example in ((AWS_ENV, "gcp_exporter.auto.tfvars.example"), (GCP_ENV, "aws_exporter.auto.tfvars.example")):
        keys = set(re.findall(r"^([a-z0-9_]+)\s*=", _read(root / example), flags=re.M))
        assert keys and keys <= declared_variables(root), (example, sorted(keys - declared_variables(root)))


def test_gitignore_excluye_los_auto_tfvars():
    text = _read(INFRA / ".gitignore")
    assert "*.auto.tfvars" in text


def test_terraform_fmt_de_los_archivos_del_lote(tmp_path):
    exe = shutil.which("terraform") or shutil.which("tofu")
    if exe is None:
        pytest.skip("terraform no está en el PATH")
    for path in LOT_FILES + [GCP_ENV / "platform.tf"]:
        dest = tmp_path / path.relative_to(INFRA).as_posix().replace("/", "__")
        dest.write_bytes(path.read_bytes())
    proc = subprocess.run([exe, "fmt", "-check", "-diff", str(tmp_path)], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    assert proc.returncode == 0, proc.stdout + proc.stderr
