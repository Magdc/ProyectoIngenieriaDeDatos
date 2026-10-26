"""Terraform de AWS del lote (bucket, KMS y rol) y scripts de operación, sin nube.

``terraform validate`` y el ``plan`` los corre ``validate_3_1.py terraform`` (parte C);
aquí se fijan las reglas de §5.7, §5.8 y §5.12 y que los archivos nuevos encajen con los
del Lote 1.0 (variables declaradas, sin nombres repetidos).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from historical.work import PROJECT_DIR

AWS = PROJECT_DIR / "infra" / "aws"
ENV = AWS / "environments" / "dev"
NEW_ENV_FILES = ("lake.tf", "variables_lake.tf", "outputs_lake.tf")
MODULES = ("kms_key", "s3_lake", "historical_loader_role")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments(text: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def test_archivos_del_lote():
    for name in NEW_ENV_FILES:
        assert (ENV / name).is_file(), name
    for mod in MODULES:
        assert (AWS / "modules" / mod / "main.tf").is_file(), mod


def test_nombres_y_rutas():
    lake = _read(ENV / "lake.tf")
    assert '"ta-${var.env}-${var.aws_account_id}-lake"' in lake
    assert '"ta-${var.env}-lake"' in lake
    assert '"ta-historical-loader"' in lake
    assert '"raw/origin=historical/"' in lake
    for src in re.findall(r'source\s*=\s*"([^"]+)"', lake):
        assert (ENV / src / "main.tf").is_file(), src


def test_sin_cuenta_ni_secretos_en_el_codigo():
    for path in [ENV / n for n in NEW_ENV_FILES] + [AWS / "modules" / m / "main.tf" for m in MODULES]:
        text = _read(path)
        assert "648617469176" not in text, path
        assert not re.search(r"AKIA[0-9A-Z]{12,}", text), path


def test_bucket_cifrado_privado_y_solo_tls():
    text = _strip_comments(_read(AWS / "modules" / "s3_lake" / "main.tf"))
    assert 'sse_algorithm     = "aws:kms"' in text
    assert "bucket_key_enabled = true" in text
    for flag in ("block_public_acls", "block_public_policy", "ignore_public_acls",
                 "restrict_public_buckets"):
        assert re.search(rf"{flag}\s*=\s*true", text), flag
    assert '"aws:SecureTransport"' in text and 'effect  = "Deny"' in text
    assert 'prefix = "athena-results/"' in text
    assert "force_destroy = false" in text
    assert 'object_ownership = "BucketOwnerEnforced"' in text


def test_kms_con_rotacion():
    text = _read(AWS / "modules" / "kms_key" / "main.tf")
    assert "enable_key_rotation     = true" in text


def test_rol_minimo_privilegio():
    text = _strip_comments(_read(AWS / "modules" / "historical_loader_role" / "main.tf"))
    actions = set()
    for block in re.findall(r"actions\s*=\s*\[([^\]]*)\]", text):
        actions |= set(re.findall(r'"([^"]+)"', block))
    assert actions == {"sts:AssumeRole", "s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey"}
    assert "${var.bucket_arn}/${var.prefix}*" in text
    assert '"s3:prefix"' in text
    assert "*" not in "".join(a for a in actions)


def _blocks(kind: str, text: str) -> list[str]:
    return re.findall(rf'^\s*{kind}\s+"([^"]+)"', _strip_comments(text), flags=re.M)


def test_encaja_con_la_raiz_del_entorno():
    files = sorted(ENV.glob("*.tf"))
    texts = {p.name: _read(p) for p in files}
    declared = [v for t in texts.values() for v in _blocks("variable", t)]
    for kind in ("variable", "output", "module"):
        names = [n for t in texts.values() for n in _blocks(kind, t)]
        dup = sorted({n for n in names if names.count(n) > 1})
        assert not dup, f"{kind} repetidos en environments/dev: {dup}"
    used = set()
    for name in NEW_ENV_FILES:
        used |= set(re.findall(r"\bvar\.([a-z0-9_]+)", _strip_comments(texts[name])))
    missing = sorted(used - set(declared))
    assert not missing, f"variables sin declarar en environments/dev: {missing}"
    if "main.tf" in texts or "variables.tf" in texts:
        assert {"aws_account_id", "env"} <= set(declared)


def test_outputs_del_lote():
    text = _read(ENV / "outputs_lake.tf")
    for name in ("lake_bucket", "lake_bucket_arn", "kms_key_arn", "kms_key_alias",
                 "historical_loader_role_arn"):
        assert re.search(rf'output\s+"{name}"', text), name


@pytest.mark.skipif(shutil.which("terraform") is None, reason="sin terraform en el PATH")
def test_terraform_fmt():
    paths = [str(ENV / n) for n in NEW_ENV_FILES] + [str(AWS / "modules" / m) for m in MODULES]
    proc = subprocess.run(["terraform", "fmt", "-check", "-recursive", *paths],
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


# ------------------------------------------------------------------ scripts

def test_historical_nlp_sh():
    path = PROJECT_DIR / "scripts" / "historical_nlp.sh"
    text = _read(path)
    assert "\r\n" not in text, "finales de línea CRLF"
    assert "ta-dataflow-nlp-local:local" in text
    assert "--user" in text
    assert "TA_NLP_BATCH_SECONDS=0" not in text, "no forzar lotes sin espera (deja los lotes en 1)"
    for forbidden in (".config/gcloud", ".aws", "GOOGLE_APPLICATION_CREDENTIALS", "AWS_SECRET"):
        assert forbidden not in text, f"el contenedor no debe recibir credenciales ({forbidden})"
    if shutil.which("bash"):
        assert subprocess.run(["bash", "-n", str(path)]).returncode == 0


def test_load_historical_plan_por_subproceso():
    import json
    import sys

    proc = subprocess.run([sys.executable, str(PROJECT_DIR / "scripts" / "load_historical.py"),
                           "plan", "--as-of", "2026-10-23", "--root", "/nonexistent-ta"],
                          capture_output=True, text=True, cwd=PROJECT_DIR.parent)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["n"] == 50_000
