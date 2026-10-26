"""Pruebas del validador del Lote 3.2 con un ejecutor falso (sin red, Docker ni nube)."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ops import validate32 as v

UID = "112233445566778899001"
ACCOUNT = "franklin@example.com"
BUCKET = "ta-dev-648617469176-lake"
KMS = "arn:aws:kms:us-east-1:648617469176:key/11111111-2222-3333-4444-555555555555"
ROLE_ARN = "arn:aws:iam::648617469176:role/ta-gcp-exporter-role"
SA = f"sa-exporter@{v.DEFAULT_PROJECT}.iam.gserviceaccount.com"
IMAGE = f"us-central1-docker.pkg.dev/{v.DEFAULT_PROJECT}/ta-images/gcs-to-s3:abc1234-0f0f0f0f"
NOW = datetime(2026, 10, 24, 20, 0, tzinfo=timezone.utc)


# --- ejecutor falso -------------------------------------------------------------------------


class FakeRunner:
    """Devuelve la primera respuesta cuyo patrón aparece en el comando (texto o función)."""

    def __init__(self, tools=("gcloud", "terraform", "aws", "bash", "docker")):
        self.handlers: list[tuple] = []
        self.calls: list[dict] = []
        self.tools = set(tools)

    def on(self, pattern, out="", code=0, err="", *, first=False):
        entry = (pattern, out if callable(out) else v.Result(code, out, err))
        if first:
            self.handlers.insert(0, entry)
        else:
            self.handlers.append(entry)
        return self

    def which(self, name):
        return f"/usr/bin/{name}" if name in self.tools else None

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        self.calls.append({"cmd": list(cmd), "env": dict(env or {}), "stream": stream, "cwd": cwd})
        joined = " ".join(str(c) for c in cmd)
        for pattern, result in self.handlers:
            if (callable(pattern) and pattern(cmd)) or (isinstance(pattern, str) and pattern in joined):
                return result(cmd) if callable(result) else result
        return v.Result(0, "", "")

    def joined(self):
        return [" ".join(str(c) for c in call["cmd"]) for call in self.calls]

    def count(self, text):
        return sum(1 for j in self.joined() if text in j)


def arg(cmd, name):
    for i, c in enumerate(cmd):
        if c == name and i + 1 < len(cmd):
            return cmd[i + 1]
        if c.startswith(name + "="):
            return c.split("=", 1)[1]
    return None


def writes_json(data, code=0, out=""):
    def handler(cmd):
        path = arg(cmd, "--json-out")
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(data(cmd) if callable(data) else data))
        return v.Result(code, out or json.dumps(data(cmd) if callable(data) else data))
    return handler


def plan_json(creates=(), updates=(), deletes=(), data_reads=("data.aws_kms_alias.exporter_lake",)):
    rcs = [{"address": a, "mode": "managed", "change": {"actions": ["create"]}} for a in creates]
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["update"]}} for a in updates]
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["delete"]}} for a in deletes]
    rcs += [{"address": a, "mode": "data", "change": {"actions": ["read"]}} for a in data_reads]
    return json.dumps({"resource_changes": rcs})


def trust_doc(uid=UID, audience="ta-gcp-exporter-role"):
    return {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Federated": "accounts.google.com"}, "Action": "sts:AssumeRoleWithWebIdentity",
        "Condition": {"StringEquals": {"accounts.google.com:aud": [uid, SA, audience],
                                       "accounts.google.com:oaud": audience, "accounts.google.com:sub": uid}}}]}


def role_policy(actions=("s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey")):
    return {"PolicyDocument": {"Statement": [
        {"Effect": "Allow", "Action": ["s3:PutObject"] if "s3:PutObject" in actions else [], "Resource": f"arn:aws:s3:::{BUCKET}/raw/origin=stream/*"},
        {"Effect": "Allow", "Action": [a for a in actions if a != "s3:PutObject"], "Resource": [f"arn:aws:s3:::{BUCKET}", KMS]}]}}


def simulate(cmd):
    action = arg(cmd, "--action-names")
    resource = arg(cmd, "--resource-arns")
    ctx_entries = " ".join(c for c in cmd if c.startswith("ContextKeyName"))
    allowed = (
        (action == "s3:PutObject" and "/raw/origin=stream/" in resource)
        or (action == "s3:ListBucket" and "raw/origin=stream/" in ctx_entries)
        or (action == "kms:GenerateDataKey" and "s3.us-east-1.amazonaws.com" in ctx_entries)
    )
    return v.Result(0, json.dumps({"EvaluationResults": [{"EvalDecision": "allowed" if allowed else "implicitDeny"}]}))


def bucket_policy(unconditional=False):
    bindings = [
        {"role": "roles/storage.objectViewer", "members": [f"serviceAccount:{SA}"],
         "condition": {"expression": f'resource.type == "storage.googleapis.com/Bucket" || resource.name.startsWith("projects/_/buckets/x/objects/enriched/")'}},
        {"role": "roles/storage.objectUser", "members": [f"serviceAccount:{SA}"],
         "condition": {"expression": 'resource.name.startsWith("projects/_/buckets/x/objects/export-state/")'}},
        {"role": "roles/storage.objectAdmin", "members": ["serviceAccount:sa-dataflow@p.iam.gserviceaccount.com"]},
    ]
    if unconditional:
        bindings.append({"role": "roles/storage.objectViewer", "members": [f"serviceAccount:{SA}"]})
    return json.dumps({"bindings": bindings})


AWS_OUTPUTS = {"lake_bucket": {"value": BUCKET}, "kms_key_arn": {"value": KMS},
               "exporter_role_arn": {"value": ROLE_ARN}, "exporter_oidc_audience": {"value": "ta-gcp-exporter-role"}}


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    aws, gcp, ev = tmp_path / "aws", tmp_path / "gcp", tmp_path / "evidencia"
    for d in (aws, gcp, ev):
        d.mkdir()
    (aws / "backend.hcl").write_text('bucket = "x"\n')
    (aws / "terraform.tfvars").write_text('aws_account_id = "648617469176"\n')
    (gcp / "backend.hcl").write_text('bucket = "y"\n')
    (gcp / "terraform.tfvars").write_text('project_id = "p"\n')
    monkeypatch.setattr(v, "AWS_ENV", aws)
    monkeypatch.setattr(v, "GCP_ENV", gcp)
    monkeypatch.setattr(v, "AWS_AUTO", aws / "gcp_exporter.auto.tfvars")
    monkeypatch.setattr(v, "GCP_AUTO", gcp / "aws_exporter.auto.tfvars")
    monkeypatch.setattr(v, "EVIDENCE_DIR", ev)
    monkeypatch.setattr(v, "PROJECT", tmp_path)
    return tmp_path


def run_part(runner, *argv, sandbox_dir, confirm=None, sleeps=None):
    lines: list[str] = []
    sleeps = sleeps if sleeps is not None else []
    code = v.main(list(argv), runner=runner, printer=lines.append, confirm=confirm, sleep=sleeps.append,
                  now=lambda: NOW, evidence_path=sandbox_dir / "evidencia" / "validacion_3_2.json")
    evidence = json.loads((sandbox_dir / "evidencia" / "validacion_3_2.json").read_text())
    return code, lines, evidence


def steps(evidence, part):
    return {s["id"]: s for s in evidence["parts"][part]["steps"]}


# --- utilidades puras -------------------------------------------------------------------------


def test_parse_json_tolera_texto_antes():
    assert v.parse_json('WARNING: algo\n{"a": 1}') == {"a": 1}
    assert v.parse_json("[1, 2]") == [1, 2]
    assert v.parse_json("") is None and v.parse_json("sin json") is None


@pytest.mark.parametrize("text,expected", [
    ("114 passed in 3.1s", {"passed": 114, "skipped": 0, "failed": 0}),
    ("1 failed, 112 passed, 2 skipped in 4s", {"passed": 112, "skipped": 2, "failed": 1}),
    ("3 passed, 1 error in 1s", {"passed": 3, "error": 1}),
])
def test_pytest_counts(text, expected):
    counts = v.pytest_counts(text)
    for key, value in expected.items():
        assert counts[key] == value


def test_resumen_del_plan_ignora_lecturas_y_no_op():
    data = json.loads(plan_json(creates=["a"], updates=["b"], deletes=["c"]))
    data["resource_changes"].append({"address": "d", "mode": "managed", "change": {"actions": ["no-op"]}})
    data["resource_changes"].append({"address": "e", "mode": "managed", "change": {"actions": ["delete", "create"]}})
    summary = v.summarize_plan(data)
    assert summary == {"create": ["a"], "update": ["b"], "delete": ["c"], "replace": ["e"]}
    assert v.plan_line(summary) == "1 a crear, 1 a cambiar, 1 a destruir, 1 a reemplazar"


def test_tfvars_alineados_y_de_ida_y_vuelta(tmp_path):
    text = v.render_tfvars("cabecera\nsegunda", {"a": "x", "larguisimo": ["user:a@b"], "c": ""})
    assert text.splitlines()[:2] == ["# cabecera", "# segunda"]
    assert 'a          = "x"' in text and 'larguisimo = ["user:a@b"]' in text
    path = tmp_path / "x.auto.tfvars"
    path.write_text(text)
    assert v.parse_tfvars(path) == {"a": "x", "larguisimo": ["user:a@b"], "c": ""}
    assert v.tfvars_value(path, "a") == "x" and v.tfvars_value(path, "zzz") is None


def test_confianza_del_rol():
    assert v.trust_problems(trust_doc(), UID, SA, "ta-gcp-exporter-role") == []
    assert any("sub" in p for p in v.trust_problems(trust_doc(uid="999"), UID, SA, "ta-gcp-exporter-role"))
    assert any("oaud" in p for p in v.trust_problems(trust_doc(audience="otra"), UID, SA, "ta-gcp-exporter-role"))
    broad = trust_doc()
    broad["Statement"][0]["Condition"]["StringEquals"]["accounts.google.com:sub"] = [UID, "777"]
    assert any("otras cuentas" in p for p in v.trust_problems(broad, UID, SA, "ta-gcp-exporter-role"))


def test_acciones_de_la_politica():
    actions, resources = v.policy_actions(role_policy()["PolicyDocument"])
    assert actions == {"s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey"}
    assert f"arn:aws:s3:::{BUCKET}/raw/origin=stream/*" in resources


def test_costo_con_minimo_de_un_minuto():
    assert v.billable_seconds(12.3) == 60 and v.billable_seconds(75.01) == 75.1
    assert v.cost_per_run(10) == pytest.approx(60 * (0.000018 + 0.5 * 0.000002))


def test_helpers_de_ejecuciones():
    done = {"metadata": {"name": "projects/p/locations/r/jobs/ta-gcs-to-s3/executions/ta-gcs-to-s3-abc12",
                         "creationTimestamp": "2026-10-24T20:00:00Z"},
            "status": {"startTime": "2026-10-24T20:00:05Z", "completionTime": "2026-10-24T20:01:10Z", "succeededCount": 1}}
    assert v.execution_name(done) == "ta-gcs-to-s3-abc12"
    start, end = v.execution_times(done)
    assert (end - start).total_seconds() == 65 and v.execution_created(done).minute == 0
    assert v.execution_succeeded(done) is True and not v.execution_running(done)
    running = {"metadata": {"name": "x"}, "status": {"runningCount": 1}}
    assert v.execution_succeeded(running) is None and v.execution_running(running)
    failed = {"status": {"failedCount": 1, "completionTime": "2026-10-24T20:01:10Z"}}
    assert v.execution_succeeded(failed) is False
    by_condition = {"status": {"conditions": [{"type": "Completed", "state": "CONDITION_SUCCEEDED"}]}}
    assert v.execution_succeeded(by_condition) is True


def test_imagen_de_la_salida_del_build():
    out = 'imagen: x\nexport TA_EXPORTER_IMAGE=a\nexporter_image = "img:1"\nexporter_image = "img:2"\n'
    assert v.latest_image_from_build(out) == "img:2" and v.latest_image_from_build("nada") is None


def test_casos_simulados():
    cases = v.simulate_cases({"lake_bucket": BUCKET, "kms_key_arn": KMS})
    assert len(cases) == 8 and sum(1 for c in cases if c[-1]) == 3


# --- Runner real (procesos locales, sin red) ---------------------------------------------------------


def test_runner_sin_entrada_estandar_y_con_tiempo_maximo():
    runner = v.Runner(printer=lambda s: None)
    res = runner.run([sys.executable, "-c", "import sys; print(repr(sys.stdin.read()))"], timeout=30)
    assert res.ok and res.out.strip() == "''"
    slow = runner.run([sys.executable, "-c", "import time; time.sleep(5)"], timeout=0.5)
    assert slow.code == 124 and "tiempo agotado" in slow.err
    assert runner.run(["no-existe-este-comando-xyz"]).code == 127


def test_runner_muestra_la_salida_mientras_corre():
    shown: list[str] = []
    runner = v.Runner(printer=shown.append)
    res = runner.run([sys.executable, "-c", "print('uno'); print('dos')"], stream=True, timeout=30)
    assert res.ok and "uno" in res.out and shown == ["    │ uno", "    │ dos"]


# --- Ctx ----------------------------------------------------------------------------------------------


def test_ctx_comandos_con_proyecto_y_pythonpath_solo_para_el_exportador(sandbox):
    fake = FakeRunner()
    ctx = v.Ctx(runner=fake, options=v.build_parser().parse_args(["status"]), printer=lambda s: None)
    ctx.gcloud("run", "jobs", "list")
    ctx.exporter("status")
    gcloud_call, exporter_call = fake.calls
    assert gcloud_call["cmd"][-1] == f"--project={v.DEFAULT_PROJECT}" and "PYTHONPATH" not in gcloud_call["env"]
    assert exporter_call["env"]["PYTHONPATH"] == str(v.SRC) and exporter_call["env"]["CLOUDSDK_CORE_DISABLE_PROMPTS"] == "1"


def test_ctx_confirmacion(monkeypatch):
    ctx = v.Ctx(runner=FakeRunner(), options=v.build_parser().parse_args(["off", "--yes"]))
    assert ctx.ask("¿?")
    ctx = v.Ctx(runner=FakeRunner(), options=v.build_parser().parse_args(["off"]), confirm=lambda q: False)
    assert not ctx.ask("¿?")
    ctx = v.Ctx(runner=FakeRunner(), options=v.build_parser().parse_args(["off"]))
    monkeypatch.setattr("builtins.input", lambda q: (_ for _ in ()).throw(EOFError()))
    assert not ctx.ask("¿?")


# --- Parte A ------------------------------------------------------------------------------------------


def preflight_runner(dataflow="", services=""):
    fake = FakeRunner()
    fake.on("terraform version -json", json.dumps({"terraform_version": "1.9.8"}))
    fake.on("export_gcs_to_s3.py doctor", "X1  OK    Python: 3.11.17")
    fake.on("docker ps", "trendanalyzer-local-dataflow-local-1\notro")
    fake.on("status.py", "nada cobra")
    fake.on("dataflow jobs list", dataflow)
    fake.on("run services list", services)
    fake.on("scheduler jobs describe", json.dumps({"state": "PAUSED"}))
    return fake


def _repo_like(sandbox, old_binding=False):
    for rel in v.LOT_FILES + v.PREVIOUS_FILES:
        path = sandbox / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("# x\n")
    (v.GCP_ENV / "platform.tf").write_text(
        'resource "google_storage_bucket_iam_member" "exporter_lake_read" {}\n' if old_binding else "# sin el permiso amplio\n")
    (v.GCP_ENV / "exporter.tf").write_text('resource "google_storage_bucket_iam_member" "exporter_lake_enriched" {}\n')
    (sandbox / "infra").mkdir(exist_ok=True)
    (sandbox / "infra" / ".gitignore").write_text("*.auto.tfvars\n")


def test_preflight(sandbox, monkeypatch):
    monkeypatch.setattr(v, "LOT_FILES", [f.replace("infra/gcp/environments/dev/", "gcp/") for f in v.LOT_FILES])
    monkeypatch.setattr(v, "PREVIOUS_FILES", ["infra/validate.sh"])
    _repo_like(sandbox)
    code, lines, ev = run_part(preflight_runner(), "preflight", sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert s["A2"]["status"] == "OK" and s["A4"]["status"] == "OK" and s["A6"]["status"] == "OK"
    assert s["A7"]["status"] == "OK" and s["A10"]["status"] == "OK"
    assert s["A8"]["status"] == "AVISO" and "down-all" in s["A8"]["detail"]
    assert "terraform 1.9.8" in s["A5"]["detail"]
    assert code == (1 if s["A1"]["status"] == "FALLA" else 0)


def test_preflight_detecta_el_permiso_amplio_y_lo_que_cobra(sandbox):
    _repo_like(sandbox, old_binding=True)
    code, _, ev = run_part(preflight_runner(dataflow="ta-stream-dev", services="ta-stream-simulator"), "preflight", sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert code == 1 and s["A4"]["status"] == "FALLA" and "exporter_lake_read" in s["A4"]["detail"]
    assert s["A10"]["status"] == "AVISO" and "ta-stream-dev" in s["A10"]["detail"] and "off.py" in s["A10"]["detail"]


def test_preflight_en_windows_lo_dice(sandbox, monkeypatch):
    _repo_like(sandbox)
    monkeypatch.setattr(v, "on_windows", lambda: True)
    code, _, ev = run_part(preflight_runner(), "preflight", sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert code == 1 and s["A0"]["status"] == "FALLA" and "WSL" in s["A0"]["detail"]


def test_otras_partes_no_corren_en_windows(sandbox, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    fake = FakeRunner()
    lines: list[str] = []
    code = v.main(["terraform", "--apply", "--yes"], runner=fake, printer=lines.append,
                  evidence_path=sandbox / "evidencia" / "validacion_3_2.json")
    assert code == 2 and not fake.calls and "WSL" in lines[-1]


def test_preflight_sin_gcloud_no_da_ok_en_la_nube(sandbox):
    _repo_like(sandbox)
    fake = preflight_runner()
    fake.on("dataflow jobs list", "", code=127, err="no se encontró gcloud", first=True)
    fake.on("run services list", "", code=127, err="no se encontró gcloud", first=True)
    fake.on("scheduler jobs describe", "", code=127, err="no se encontró gcloud", first=True)
    _, _, ev = run_part(fake, "preflight", sandbox_dir=sandbox)
    a10 = steps(ev, "A")["A10"]
    assert a10["status"] == "AVISO" and "no se pudo comprobar" in a10["detail"]


# --- Parte B ------------------------------------------------------------------------------------------


def test_tests_con_los_conteos_esperados(sandbox):
    fake = FakeRunner()
    (sandbox / "src/historical/tests").mkdir(parents=True)
    (sandbox / "src/historical/tests/test_hist_infra.py").write_text("")
    fake.on("src/exporter", f"{v.B1_EXPECTED} passed in 3s")
    fake.on("test_ops_validate32.py", f"{v.B2_EXPECTED} passed in 1s")
    fake.on("contracts/tests", "18 passed in 1s")
    fake.on("test_hist_infra.py", "9 passed in 1s")
    code, _, ev = run_part(fake, "tests", sandbox_dir=sandbox)
    assert code == 0 and all(s["status"] == "OK" for s in steps(ev, "B").values())


def test_tests_con_omitidas_es_falla(sandbox):
    fake = FakeRunner()
    fake.on("src/exporter", f"{v.B1_EXPECTED - 2} passed, 2 skipped in 3s")
    fake.on("test_ops_validate32.py", f"{v.B2_EXPECTED} passed in 1s")
    fake.on("contracts/tests", "18 passed in 1s")
    code, _, ev = run_part(fake, "tests", sandbox_dir=sandbox)
    s = steps(ev, "B")
    assert code == 1 and s["B1"]["status"] == "FALLA" and s["B4"]["status"] == "AVISO"


# --- Parte C ------------------------------------------------------------------------------------------


def terraform_runner(sandbox, *, aws_plan=None, gcp_plan=None, outputs=AWS_OUTPUTS, identity_account="648617469176",
                     bucket_iam=None, policy=None):
    fake = FakeRunner()
    aws_env, gcp_env = str(v.AWS_ENV), str(v.GCP_ENV)
    fake.on("sts get-caller-identity", json.dumps({"Account": identity_account, "Arn": f"arn:aws:iam::{identity_account}:root"}))
    fake.on("config get-value account", ACCOUNT + "\n")
    fake.on("service-accounts describe", json.dumps({"uniqueId": UID, "email": SA}))
    fake.on("infra/validate.sh", "validate ok")
    fake.on(f"-chdir={aws_env} show -json", aws_plan or plan_json(creates=sorted(v.AWS_PLAN_CREATES)))
    fake.on(f"-chdir={aws_env} apply", "Apply complete! Resources: 2 added, 0 changed, 0 destroyed.")
    fake.on(f"-chdir={aws_env} output -json", json.dumps(outputs))
    fake.on(f"-chdir={gcp_env} show -json", gcp_plan or plan_json(
        creates=["google_storage_bucket_iam_member.exporter_lake_enriched",
                 f'google_service_account_iam_member.exporter_oidc_token_creator["user:{ACCOUNT}"]'],
        deletes=["google_storage_bucket_iam_member.exporter_lake_read"], data_reads=()))
    fake.on(f"-chdir={gcp_env} apply", "Apply complete! Resources: 2 added, 0 changed, 1 destroyed.")
    fake.on("iam get-role-policy", json.dumps(policy or role_policy()))
    fake.on("iam get-role ", json.dumps({"Role": {"AssumeRolePolicyDocument": trust_doc()}}))
    fake.on("simulate-principal-policy", simulate)
    fake.on("buckets get-iam-policy", bucket_iam or bucket_policy())
    fake.on("service-accounts get-iam-policy", json.dumps({"bindings": [
        {"role": "roles/iam.serviceAccountOpenIdTokenCreator", "members": [f"user:{ACCOUNT}"]}]}))
    return fake


def test_terraform_sin_apply_planea_y_escribe_los_enlaces(sandbox):
    fake = terraform_runner(sandbox, outputs={"lake_bucket": {"value": BUCKET}, "kms_key_arn": {"value": KMS}})
    code, _, ev = run_part(fake, "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0, s
    assert s["C0"]["status"] == "AVISO" and "raíz" in s["C0"]["detail"]
    assert s["C2"]["status"] == "OK" and "2 a crear" in s["C2"]["detail"]
    assert s["C3"]["status"] == "AVISO" and fake.count(f"-chdir={v.AWS_ENV} apply") == 0
    assert s["C4"]["status"] == "AVISO" and s["C5"]["status"] == "OMITIDO"
    # sin apply, el IAM real no se compara (sería una falla falsa)
    assert s["C8"]["status"] == "OMITIDO" and fake.count("buckets get-iam-policy") == 0
    aws_auto = v.AWS_AUTO.read_text()
    assert f'gcp_exporter_sa_unique_id = "{UID}"' in aws_auto and SA in aws_auto
    gcp_auto = v.parse_tfvars(v.GCP_AUTO)
    assert gcp_auto["exporter_oidc_token_creators"] == [f"user:{ACCOUNT}"] and gcp_auto["exporter_image"] == ""
    plan_cmd = next(c["cmd"] for c in fake.calls if f"-chdir={v.AWS_ENV}" in c["cmd"] and "plan" in c["cmd"])
    assert "-target=module.gcp_exporter_role" in plan_cmd and any(a.startswith("-out=") for a in plan_cmd)
    init_cmd = next(c["cmd"] for c in fake.calls if "init" in c["cmd"])
    assert "-backend-config=backend.hcl" in init_cmd
    assert fake.count("terraform fmt") == 2


def test_terraform_con_apply_completo(sandbox):
    fake = terraform_runner(sandbox)
    code, _, ev = run_part(fake, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    for sid in ("C2", "C3", "C4", "C4b", "C5", "C5b", "C6", "C7", "C8"):
        assert s[sid]["status"] == "OK", (sid, s[sid])
    assert "8 de 8" in s["C5b"]["detail"]
    assert v.parse_tfvars(v.GCP_AUTO)["exporter_role_arn"] == ROLE_ARN
    gcp_plan = next(c["cmd"] for c in fake.calls if f"-chdir={v.GCP_ENV}" in c["cmd"] and "plan" in c["cmd"])
    assert {f"-target={t}" for t in v.GCP_BASE_TARGETS} <= set(gcp_plan)


def test_terraform_otra_cuenta_se_detiene(sandbox):
    code, _, ev = run_part(terraform_runner(sandbox, identity_account="111111111111"), "terraform", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C0"]["status"] == "FALLA" and len(steps(ev, "C")) == 1


def test_terraform_sin_backend_hcl(sandbox):
    (v.GCP_ENV / "backend.hcl").unlink()
    code, _, ev = run_part(terraform_runner(sandbox), "terraform", sandbox_dir=sandbox)
    assert code == 1 and "gcp/backend.hcl" in steps(ev, "C")["C0b"]["detail"]


def test_terraform_plan_aws_inesperado_no_aplica(sandbox):
    fake = terraform_runner(sandbox, aws_plan=plan_json(creates=sorted(v.AWS_PLAN_CREATES), updates=["module.lake_bucket.aws_s3_bucket.this"]))
    code, _, ev = run_part(fake, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C2"]["status"] == "FALLA" and fake.count("apply") == 0


def test_terraform_plan_gcp_que_destruye_otra_cosa_no_aplica(sandbox):
    fake = terraform_runner(sandbox, gcp_plan=plan_json(deletes=["module.lake_bucket.google_storage_bucket.this"], data_reads=()))
    code, _, ev = run_part(fake, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 1 and s["C6"]["status"] == "FALLA" and fake.count(f"-chdir={v.GCP_ENV} apply") == 0


def test_terraform_no_confirmado(sandbox):
    fake = terraform_runner(sandbox)
    code, _, ev = run_part(fake, "terraform", "--apply", sandbox_dir=sandbox, confirm=lambda q: False)
    s = steps(ev, "C")
    assert s["C3"]["status"] == "AVISO" and s["C7"]["status"] == "AVISO" and fake.count(" apply ") == 0


def test_terraform_ya_aplicado(sandbox):
    fake = terraform_runner(sandbox, aws_plan=plan_json(), gcp_plan=plan_json(data_reads=()))
    code, _, ev = run_part(fake, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and "ya aplicado" in s["C2"]["detail"] and s["C3"]["detail"] == "nada que aplicar"


def test_terraform_detecta_permiso_amplio_y_politica_mala(sandbox):
    fake = terraform_runner(sandbox, bucket_iam=bucket_policy(unconditional=True),
                            policy=role_policy(("s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey", "s3:GetObject")))
    code, _, ev = run_part(fake, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 1 and s["C5"]["status"] == "FALLA" and "s3:GetObject" in s["C5"]["detail"]
    assert s["C8"]["status"] == "FALLA" and "sin condición" in s["C8"]["detail"]


def test_auto_tfvars_conserva_la_imagen(sandbox):
    v.GCP_AUTO.write_text(v.render_tfvars("x", {"exporter_image": IMAGE, "exporter_oidc_token_creators": ["user:otro@x"]}))
    ctx = v.Ctx(runner=FakeRunner(), options=v.build_parser().parse_args(["terraform"]), printer=lambda s: None)
    v.write_gcp_auto(ctx, {"exporter_role_arn": ROLE_ARN}, None)
    values = v.parse_tfvars(v.GCP_AUTO)
    assert values["exporter_image"] == IMAGE and values["exporter_oidc_token_creators"] == ["user:otro@x"]
    v.write_gcp_auto(ctx, {}, ACCOUNT, image="nueva")
    values = v.parse_tfvars(v.GCP_AUTO)
    assert values["exporter_image"] == "nueva" and values["exporter_role_arn"] == ROLE_ARN


# --- Parte D ------------------------------------------------------------------------------------------


def federation_runner(sandbox, *, token_fail_times=0, token_ok=True, to_upload=150, uploaded=20, plan2_planned=()):
    fake = FakeRunner()
    fake.on("output -json", json.dumps(AWS_OUTPUTS))
    calls = {"token": 0, "plan": 0}

    def token(cmd):
        calls["token"] += 1
        if calls["token"] <= token_fail_times:
            return v.Result(2, "", "error: impersonación: 403 PERMISSION_DENIED iam.serviceAccounts.getOpenIdToken")
        data = {"claims": {"azp": UID, "sub": UID, "aud": "ta-gcp-exporter-role", "email": SA},
                "problems": [] if token_ok else ["sub = '1'"], "trust_problems": [], "ok": token_ok}
        return writes_json(data, code=0 if token_ok else 1)(cmd)

    def plan(cmd):
        calls["plan"] += 1
        first = calls["plan"] == 1
        data = {"listed": 600, "candidates": 600, "to_upload": to_upload if first else to_upload - uploaded,
                "skipped_same_size": 0 if first else uploaded, "duration_s": 4.2,
                "planned_keys": [] if first else list(plan2_planned)}
        return writes_json(data)(cmd)

    keys = [f"raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-{i}.parquet" for i in range(uploaded)]
    fake.on("export_gcs_to_s3.py token", token)
    fake.on("export_gcs_to_s3.py probe", writes_json({"ok": True, "passed": 7, "total": 7,
                                                       "aws": {"assumed_arn": "arn:aws:sts::1:assumed-role/ta-gcp-exporter-role/x"},
                                                       "results": [{"id": "A1", "ok": True}]}))
    fake.on("export_gcs_to_s3.py plan", plan)
    fake.on("export_gcs_to_s3.py run", writes_json({"uploaded": uploaded, "overwritten": 0, "failed_count": 0, "bytes_uploaded": 9000,
                                                     "duration_s": 6.0, "uploaded_keys": keys}))
    fake.on("export_gcs_to_s3.py verify", writes_json({"status": "OK", "checks": [{"id": "V1", "status": "OK", "detail": "20 de 20"},
                                                                                   {"id": "V4", "status": "OK", "detail": "20 de 20"}]}))
    return fake, calls


def test_federacion_completa_con_reintento_de_propagacion(sandbox):
    fake, calls = federation_runner(sandbox, token_fail_times=1)
    sleeps: list[float] = []
    code, _, ev = run_part(fake, "federation", sandbox_dir=sandbox, sleeps=sleeps)
    s = steps(ev, "D")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert calls["token"] == 2 and sleeps == [30]
    assert [k for k in s] == ["D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"]
    run_cmd = next(c["cmd"] for c in fake.calls if "run" in c["cmd"] and "export_gcs_to_s3.py" in " ".join(c["cmd"]))
    assert arg(run_cmd, "--state-key") == v.VALIDATION_STATE and arg(run_cmd, "--max-objects") == "20"
    plans = [c["cmd"] for c in fake.calls if "plan" in c["cmd"] and "export_gcs_to_s3.py" in " ".join(c["cmd"])]
    # D6 planea sin cursor: los de D4 deben salir omitidos aunque sean de hace días
    assert [arg(c, "--state-key") for c in plans] == [v.VALIDATION_STATE, v.VALIDATION_STATE_IDEM]
    assert fake.calls[-1]["cmd"][:3] == ["gcloud", "storage", "rm"]
    env = next(c["env"] for c in fake.calls if "export_gcs_to_s3.py" in " ".join(c["cmd"]))
    assert env["TA_EXPORTER_ROLE_ARN"] == ROLE_ARN and env["TA_LAKE_KMS_KEY_ARN"] == KMS and env["PYTHONPATH"].endswith("src")


def test_federacion_token_no_aceptado(sandbox):
    fake, _ = federation_runner(sandbox, token_ok=False)
    code, _, ev = run_part(fake, "federation", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 1 and s["D1"]["status"] == "FALLA" and "D2" not in s


def test_federacion_sin_rol(sandbox):
    fake = FakeRunner().on("output -json", json.dumps({"lake_bucket": {"value": BUCKET}}))
    code, _, ev = run_part(fake, "federation", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D0"]["status"] == "FALLA"


def test_federacion_sin_nada_nuevo(sandbox):
    fake, _ = federation_runner(sandbox, to_upload=0, uploaded=0)
    code, _, ev = run_part(fake, "federation", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 0 and s["D5"]["status"] == "OMITIDO" and s["D6"]["status"] == "OMITIDO"


def test_federacion_detecta_que_vuelve_a_subir(sandbox):
    keys = [f"raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-{i}.parquet" for i in range(3)]
    fake, _ = federation_runner(sandbox, uploaded=20, plan2_planned=keys[:1])
    code, _, ev = run_part(fake, "federation", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D6"]["status"] == "FALLA"


# --- Parte E ------------------------------------------------------------------------------------------


def execution(name, *, done=True, ok=True, creator=SA, seconds=42):
    data = {"metadata": {"name": name, "creationTimestamp": "2026-10-24T20:00:00Z",
                         "annotations": {"run.googleapis.com/creator": creator}},
            "status": {"startTime": "2026-10-24T20:00:00Z"}}
    if done:
        data["status"]["completionTime"] = (datetime(2026, 10, 24, 20, tzinfo=timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
        data["status"]["succeededCount" if ok else "failedCount"] = 1
    return data


def job_runner(sandbox, *, job_plan=None, probe_ok=True, second_uploaded=0, stream_on=False, scheduler_execs=True,
               build_out=None, errors=()):
    v.GCP_AUTO.write_text(v.render_tfvars("x", {"exporter_role_arn": ROLE_ARN, "exporter_s3_bucket": BUCKET,
                                                 "exporter_kms_key_arn": KMS, "exporter_image": ""}))
    fake = FakeRunner()
    gcp_env = str(v.GCP_ENV)
    fake.on("output -json", json.dumps(AWS_OUTPUTS))
    fake.on("build_exporter_image.sh", build_out or f'construida en 200 s\nexporter_image = "{IMAGE}"\n')
    fake.on(f"-chdir={gcp_env} show -json", job_plan or plan_json(creates=sorted(v.GCP_JOB_CREATES), data_reads=()))
    fake.on(f"-chdir={gcp_env} apply", "Apply complete! Resources: 3 added, 0 changed, 0 destroyed.")
    state = {"sched": "PAUSED", "n": 0, "execs": [execution("ta-gcs-to-s3-old01")], "resumed": False}

    def describe_sched(cmd):
        return v.Result(0, json.dumps({"state": state["sched"], "schedule": "*/15 * * * *", "timeZone": "Etc/UTC"}))

    def resume(cmd):
        state["sched"] = "ENABLED"
        return v.Result(0, "")

    def pause(cmd):
        state["sched"] = "PAUSED"
        return v.Result(0, "")

    def force_run(cmd):
        if scheduler_execs:
            state["execs"].insert(0, execution("ta-gcs-to-s3-sched1", seconds=50))
        return v.Result(0, "")

    def execute(cmd):
        state["n"] += 1
        name = f"ta-gcs-to-s3-run{state['n']:02d}"
        return v.Result(0, json.dumps(execution(name, seconds=30 + state["n"])))

    def logs(cmd):
        flt = cmd[3]
        if "severity>=ERROR" in flt:
            return v.Result(0, json.dumps([{"jsonPayload": {"event": e}} for e in errors]))
        n = int(re.search(r"run(\d\d)", flt).group(1)) if re.search(r"run(\d\d)", flt) else 0
        if 'event="probe_result"' in flt:
            return v.Result(0, json.dumps([{"jsonPayload": {"ok": probe_ok, "passed": 13 if probe_ok else 12, "total": 13,
                                                            "results": [] if probe_ok else [{"id": "G3", "what": "leer raw/", "got": "permitido", "expect": "denegado", "ok": False}]}}]))
        uploaded = {2: 480, 3: second_uploaded, 4: 1}.get(n, 0)
        return v.Result(0, json.dumps([{"jsonPayload": {"ok": True, "uploaded": uploaded, "skipped_same_size": 20, "failed": 0,
                                                        "overwritten": 0, "bytes_uploaded": 1000, "watermark_after": "2026-10-24T19:59:00.000Z"}}]))

    fake.on("scheduler jobs describe", describe_sched)
    fake.on("scheduler jobs resume", resume)
    fake.on("scheduler jobs pause", pause)
    fake.on("scheduler jobs run", force_run)
    fake.on("run jobs describe", json.dumps({"spec": {"template": {"image": IMAGE, "serviceAccountName": SA}}}))
    fake.on("run jobs execute", execute)
    fake.on("run jobs executions list", lambda cmd: v.Result(0, json.dumps(state["execs"])))
    fake.on("logging read", logs)
    fake.on("dataflow jobs list", "ta-stream-dev" if stream_on else "")
    fake.on("export_gcs_to_s3.py verify", writes_json({"status": "OK", "gcs_enriched": 500, "s3_stream": 500,
                                                       "checks": [{"id": "V1", "status": "OK", "detail": "500 de 500"}]}))
    fake.on("s3api head-object", json.dumps({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KMS}))
    return fake, state


def test_job_completo(sandbox):
    (v.EVIDENCE_DIR / "validacion_3_2.json").write_text(json.dumps({"parts": {"D": {"steps": [{"id": "D4", "data": {
        "keys": ["raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-a.parquet"]}}]}}}))
    fake, state = job_runner(sandbox)
    code, _, ev = run_part(fake, "job", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    for sid in ("E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E9b", "E9c", "E10", "E10b", "E11", "E12"):
        assert s[sid]["status"] == "OK", (sid, s[sid])
    assert v.parse_tfvars(v.GCP_AUTO)["exporter_image"] == IMAGE
    assert state["sched"] == "PAUSED"
    probe_cmd = next(c["cmd"] for c in fake.calls if "execute" in c["cmd"])
    assert "--args=probe" in probe_cmd and "--wait" in probe_cmd
    cp = next(c["cmd"] for c in fake.calls if c["cmd"][:3] == ["gcloud", "storage", "cp"])
    assert cp[3].endswith("enriched/source=reddit/dt=2026-10-22/hr=10/part-a.parquet") and cp[4].endswith("-p999.parquet")
    assert fake.count("s3 rm s3://") == 1 and fake.count("storage rm gs://") == 1
    assert ev["canaries"][0]["cleaned"] is True
    assert s["E12"]["data"]["usd_per_day"] > 0 and "USD/día" in s["E12"]["detail"]


def test_job_construye_la_imagen(sandbox):
    fake, _ = job_runner(sandbox)
    code, _, ev = run_part(fake, "job", "--yes", "--no-canary", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert s["E1"]["data"]["image"] == IMAGE and s["E9"]["status"] == "OMITIDO"
    assert next(c for c in fake.calls if "build_exporter_image.sh" in " ".join(c["cmd"]))["stream"] is True


def test_job_sin_imagen_del_build(sandbox):
    fake, _ = job_runner(sandbox, build_out="ERROR: build failed")
    code, _, ev = run_part(fake, "job", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "E")["E1"]["status"] == "FALLA" and fake.count("plan") == 0


def test_job_plan_inesperado_no_aplica(sandbox):
    fake, _ = job_runner(sandbox, job_plan=plan_json(creates=sorted(v.GCP_JOB_CREATES), deletes=["module.simulator[0].x"], data_reads=()))
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "E")["E2"]["status"] == "FALLA" and fake.count(" apply ") == 0


def test_job_sonda_fallida_se_detiene(sandbox):
    fake, _ = job_runner(sandbox, probe_ok=False)
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 1 and s["E5"]["status"] == "FALLA" and "G3" in s["E5"]["detail"] and "E6" not in s


def test_job_segunda_ejecucion_que_sube_sin_stream_es_falla(sandbox):
    fake, _ = job_runner(sandbox, second_uploaded=5)
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "E")["E8"]["status"] == "FALLA"


def test_job_segunda_ejecucion_tras_un_corte_por_max_objects(sandbox, monkeypatch):
    fake, _ = job_runner(sandbox, second_uploaded=5)
    original = fake.handlers[[i for i, h in enumerate(fake.handlers) if h[0] == "logging read"][0]][1]

    def logs(cmd):
        res = original(cmd)
        if "run02" in cmd[3]:
            data = json.loads(res.out)
            data[0]["jsonPayload"]["pending"] = 5
            return v.Result(0, json.dumps(data))
        return res

    fake.handlers = [(p, logs if p == "logging read" else r) for p, r in fake.handlers]
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert s["E8"]["status"] == "OK" and "pendientes" in s["E8"]["detail"]


def test_job_segunda_ejecucion_con_stream_encendido(sandbox):
    fake, _ = job_runner(sandbox, second_uploaded=5, stream_on=True)
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert s["E8"]["status"] == "OK" and "stream" in s["E8"]["detail"]


def test_job_scheduler_siempre_vuelve_a_la_pausa(sandbox):
    fake, state = job_runner(sandbox, scheduler_execs=False)
    sleeps: list[float] = []
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox, sleeps=sleeps)
    s = steps(ev, "E")
    assert s["E10"]["status"] == "FALLA" and s["E10b"]["status"] == "OK" and state["sched"] == "PAUSED"
    assert len([x for x in sleeps if x == 20]) == 6 * 60 // 20 + 1


def test_job_scheduler_no_confirmado_queda_en_pausa(sandbox):
    fake, state = job_runner(sandbox)
    answers = iter([True, False])
    code, _, ev = run_part(fake, "job", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox, confirm=lambda q: next(answers))
    s = steps(ev, "E")
    assert s["E10"]["status"] == "AVISO" and fake.count("scheduler jobs resume") == 0 and state["sched"] == "PAUSED"


def test_job_errores_en_los_logs(sandbox):
    fake, _ = job_runner(sandbox, errors=("export_object_failed",))
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", "--no-canary", sandbox_dir=sandbox)
    assert steps(ev, "E")["E11"]["status"] == "AVISO"


def test_job_sin_valores_de_aws(sandbox):
    fake = FakeRunner().on("output -json", json.dumps({}))
    code, _, ev = run_part(fake, "job", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "E")["E0"]["status"] == "FALLA"


def test_execute_job_sin_json_usa_el_texto_y_describe(sandbox):
    fake = FakeRunner()
    fake.on("run jobs execute", "", err="Execution [ta-gcs-to-s3-xyz12] has successfully completed.")
    fake.on("executions describe", json.dumps(execution("ta-gcs-to-s3-xyz12", seconds=70)))
    ctx = v.Ctx(runner=fake, options=v.build_parser().parse_args(["job"]), printer=lambda s: None)
    part = v.Part(ctx, "E", "x")
    ex = v.execute_job(ctx, part, "E6", "x")
    assert ex["execution"] == "ta-gcs-to-s3-xyz12" and ex["duration_s"] == 70


def test_logs_que_no_llegan(sandbox):
    sleeps: list[float] = []
    ctx = v.Ctx(runner=FakeRunner().on("logging read", "[]"), options=v.build_parser().parse_args(["job"]),
                printer=lambda s: None, sleep=sleeps.append)
    assert v.job_log_event(ctx, "ta-gcs-to-s3-x", "export_done", wait_s=45) is None
    assert sleeps == [15, 15, 15]


# --- Parte F ------------------------------------------------------------------------------------------


def lake_report(stream_objects=520, problems=()):
    def origin(n, name):
        return {"objects": n, "bytes": n * 9000, "by_source": {s: {"objects": n // 3, "bytes": 1} for s in ("reddit", "facebook", "instagram")},
                "sample": {"key": f"raw/origin={name}/x", "rows": 3, "columns": 24, "problems": list(problems)}}
    return {"origins": {"historical": origin(v.HISTORICAL_OBJECTS, "historical"), "stream": origin(stream_objects, "stream")},
            "columns_source": "common.enriched_row.POSTS_ENRICHED_COLUMNS"}


def test_lake_ambos_origenes(sandbox):
    (v.EVIDENCE_DIR / "exporter_verify_3_2.json").write_text(json.dumps({"gcs_enriched": 520}))
    (v.EVIDENCE_DIR / "validacion_3_2.json").write_text(json.dumps({"parts": {"E": {"steps": [{"id": "E12", "data": {"usd_per_day": 0.11}}]}}}))
    fake = FakeRunner().on("output -json", json.dumps(AWS_OUTPUTS)).on("lake-report", writes_json(lake_report()))
    code, _, ev = run_part(fake, "lake", sandbox_dir=sandbox)
    s = steps(ev, "F")
    assert code == 0 and [s[k]["status"] for k in ("F1", "F2", "F3", "F4", "F5")] == ["OK"] * 5
    assert "12102" in s["F1"]["detail"] and "520" in s["F2"]["detail"]
    cmd = next(c["cmd"] for c in fake.calls if "lake-report" in c["cmd"])
    assert arg(cmd, "--aws-auth") == "profile"


def test_lake_sin_stream_es_falla(sandbox):
    fake = FakeRunner().on("output -json", json.dumps(AWS_OUTPUTS)).on("lake-report", writes_json(lake_report(stream_objects=0), code=1))
    code, _, ev = run_part(fake, "lake", sandbox_dir=sandbox)
    s = steps(ev, "F")
    assert code == 1 and s["F2"]["status"] == "FALLA" and s["F4"]["status"] == "FALLA"


def test_lake_con_identidad_en_claro(sandbox):
    fake = FakeRunner().on("output -json", json.dumps(AWS_OUTPUTS)).on("lake-report", writes_json(lake_report(problems=["columna user_id en claro"]), code=1))
    code, _, ev = run_part(fake, "lake", sandbox_dir=sandbox)
    assert code == 1 and "user_id" in steps(ev, "F")["F3"]["detail"]


# --- status y off -------------------------------------------------------------------------------------


def test_status_avisa_lo_encendido(sandbox):
    fake = FakeRunner()
    fake.on("scheduler jobs describe", json.dumps({"state": "ENABLED", "scheduleTime": "2026-10-24T20:15:00Z"}))
    fake.on("executions list", json.dumps([execution("ta-gcs-to-s3-r1", done=False), execution("ta-gcs-to-s3-r0")]))
    fake.on("export_gcs_to_s3.py status", json.dumps({"exists": True, "cursor_time_created": "2026-10-24T19:59:00.000Z",
                                                      "updated_at": "x", "last_run": {"uploaded": 3}}))
    code, _, ev = run_part(fake, "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "AVISO" and s["S2"]["status"] == "AVISO" and "ta-gcs-to-s3-r1" in s["S2"]["detail"]
    assert "2026-10-24T19:59" in s["S3"]["detail"]


def test_off_pausa_cancela_y_limpia(sandbox):
    (v.EVIDENCE_DIR / "validacion_3_2.json").write_text(json.dumps({"parts": {}, "canaries": [
        {"gcs": "enriched/source=reddit/dt=2026-10-24/hr=20/part-x-p999.parquet",
         "s3": "raw/origin=stream/source=reddit/dt=2026-10-24/hr=20/part-x-p999.parquet", "cleaned": False}]}))
    fake = FakeRunner()
    state = {"sched": "ENABLED", "cancelled": False}
    fake.on("scheduler jobs describe", lambda cmd: v.Result(0, json.dumps({"state": state["sched"]})))
    fake.on("scheduler jobs pause", lambda cmd: (state.update(sched="PAUSED"), v.Result(0, ""))[1])
    fake.on("executions cancel", lambda cmd: (state.update(cancelled=True), v.Result(0, ""))[1])
    fake.on("executions list", lambda cmd: v.Result(0, json.dumps([execution("ta-gcs-to-s3-r1", done=not state["cancelled"] and False)]
                                                                 if not state["cancelled"] else [execution("ta-gcs-to-s3-r1")])))
    fake.on("output -json", json.dumps(AWS_OUTPUTS))
    fake.on("status.py", "nada cobra")
    sleeps: list[float] = []
    code, _, ev = run_part(fake, "off", sandbox_dir=sandbox, sleeps=sleeps)
    s = steps(ev, "off")
    assert code == 0, s
    assert state["sched"] == "PAUSED" and state["cancelled"] and sleeps == [15]
    cancel = next(c["cmd"] for c in fake.calls if "cancel" in c["cmd"])
    assert "--quiet" in cancel and "ta-gcs-to-s3-r1" in cancel
    assert s["O3"]["detail"] == "1 de 1 borrados" and ev["canaries"][0]["cleaned"] is True
    assert "status.py no conoce el exportador" in s["O5"]["detail"]


def test_off_ya_en_ahorro_no_toca_nada(sandbox):
    fake = FakeRunner()
    fake.on("scheduler jobs describe", json.dumps({"state": "PAUSED"}))
    fake.on("executions list", json.dumps([execution("ta-gcs-to-s3-r0")]))
    code, _, ev = run_part(fake, "off", sandbox_dir=sandbox)
    assert code == 0 and fake.count("scheduler jobs pause") == 0 and fake.count("executions cancel") == 0


def test_off_sin_job_creado(sandbox):
    fake = FakeRunner()
    fake.on("scheduler jobs describe", "", code=1, err="ERROR: NOT_FOUND: Job not found")
    fake.on("executions list", "", code=1, err="ERROR: NOT_FOUND")
    code, _, ev = run_part(fake, "off", sandbox_dir=sandbox)
    s = steps(ev, "off")
    assert code == 0 and "NO_EXISTE" in s["O1"]["detail"] and s["O2"]["status"] == "OK"


# --- evidencia y CLI ----------------------------------------------------------------------------------


def test_la_evidencia_acumula_partes(sandbox):
    fake = FakeRunner().on("scheduler jobs describe", json.dumps({"state": "PAUSED"})).on("executions list", "[]")
    run_part(fake, "status", sandbox_dir=sandbox)
    _, _, ev = run_part(fake, "off", sandbox_dir=sandbox)
    assert set(ev["parts"]) == {"status", "off"} and ev["parts"]["off"]["status"] == "OK"
    assert ev["lote"] == "3.2" and ev["hu"] == ["HU-006", "HU-025"]


def test_cli_partes_y_opciones():
    parser = v.build_parser()
    for part in ("preflight", "tests", "terraform", "federation", "job", "lake", "status", "off"):
        assert parser.parse_args([part]).part == part
    args = parser.parse_args(["job", "--skip-build", "--wait-tick", "--no-canary", "--yes"])
    assert args.skip_build and args.wait_tick and args.no_canary and args.yes
    with pytest.raises(SystemExit):
        parser.parse_args(["destruir"])


def test_el_script_de_entrada_existe():
    script = Path(v.__file__).resolve().parents[2] / "scripts" / "validate_3_2.py"
    assert script.exists() and "ops.validate32" in script.read_text()


# --- v2.30: errores de Terraform legibles, estado sin guardar y diferencias del plan -------------


SCHED = "module.exporter_job[0].google_cloud_scheduler_job.this"


def plan_with_update(addr=SCHED, before=None, after=None, unknown=None, sensitive=None):
    data = json.loads(plan_json(data_reads=()))
    data["resource_changes"].append({"address": addr, "mode": "managed", "change": {
        "actions": ["update"], "before": before or {}, "after": after or {}, "after_unknown": unknown or {},
        "after_sensitive": sensitive or {}}})
    return data


def test_diferencias_del_plan_con_rutas_valores_desconocidos_y_sensibles():
    data = plan_with_update(
        before={"http_target": [{"headers": {"Content-Type": "application/json"}, "uri": "u"}], "description": "a", "token": "s1"},
        after={"http_target": [{"headers": {"Content-Type": "application/json", "User-Agent": "x"}, "uri": "u"}],
               "description": "b", "token": "s2", "state": None},
        unknown={"state": True}, sensitive={"token": True})
    lines = v.plan_diffs(data)[SCHED]
    assert "description: 'a' → 'b'" in lines
    assert "http_target.0.headers.User-Agent: null → 'x'" in lines
    assert "token: (valor sensible) → (valor sensible)" in lines and not any("s2" in ln for ln in lines)
    assert any(ln.startswith("state:") and "al aplicar" in ln for ln in lines)
    assert v.plan_diffs(json.loads(plan_json(creates=["a"], deletes=["b"]))) == {}


def test_error_de_terraform_sin_colores_ni_marcos():
    text = ("module.x: Creating...\n\x1b[31m╷\x1b[0m\n\x1b[31m│\x1b[0m \x1b[0m\x1b[1m\x1b[31mError: \x1b[0mFailed to save state\n"
            "\x1b[31m│\x1b[0m \x1b[0mError saving state: googleapi: Error 503: backendError\n"
            "\x1b[31m│\x1b[0m \x1b[0mTo retry writing this state, use the following command:\n"
            "\x1b[31m│\x1b[0m \x1b[0m    terraform state push errored.tfstate\n\x1b[31m╵\x1b[0m\n")
    detail = v.tf_error(v.Result(1, text))
    assert detail.splitlines()[0] == "Error: Failed to save state" and "Error 503" in detail and "\x1b" not in detail
    assert v.tf_error(v.Result(1, "uno\ndos")) == "uno\ndos"


def test_job_apply_que_no_guarda_el_estado(sandbox):
    fake, _ = job_runner(sandbox)

    def apply(cmd):
        (v.GCP_ENV / "errored.tfstate").write_text("{}")
        return v.Result(1, "\x1b[31m│\x1b[0m Error: Failed to save state\n│ Error 503\n│ terraform state push errored.tfstate\n")

    fake.on(f"-chdir={v.GCP_ENV} apply", apply, first=True)
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", sandbox_dir=sandbox)
    e3 = steps(ev, "E")["E3"]
    assert code == 1 and e3["status"] == "FALLA"
    assert "state push errored.tfstate" in e3["detail"] and "Error: Failed to save state" in e3["detail"] and "\x1b" not in e3["detail"]
    assert fake.count("run jobs execute") == 0


def test_job_con_estado_sin_guardar_no_planea(sandbox):
    fake, _ = job_runner(sandbox)
    (v.GCP_ENV / "errored.tfstate").write_text("{}")
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", sandbox_dir=sandbox)
    e2 = steps(ev, "E")["E2"]
    assert code == 1 and e2["status"] == "FALLA" and "state push" in e2["detail"]
    assert fake.count(" plan ") == 0 and fake.count(" apply ") == 0


def test_job_plan_que_cambia_el_scheduler_muestra_que_cambia(sandbox):
    plan = plan_with_update(before={"attempt_deadline": "180s"}, after={"attempt_deadline": "180.000s"})
    fake, _ = job_runner(sandbox, job_plan=json.dumps(plan))
    code, _, ev = run_part(fake, "job", "--yes", f"--image={IMAGE}", sandbox_dir=sandbox)
    e2 = steps(ev, "E")["E2"]
    assert code == 1 and e2["status"] == "FALLA" and "attempt_deadline: '180s' → '180.000s'" in e2["detail"]
    assert e2["data"]["diffs"][SCHED] == ["attempt_deadline: '180s' → '180.000s'"] and fake.count(" apply ") == 0


def test_off_avisa_si_status_muestra_algo_que_cobra(sandbox):
    """Visto en el apagado del 3.2: O5 dio OK con el simulador encendido (status.py: «ENCENDIDO (cobra)»)."""
    fake, state = job_runner(sandbox)
    fake.on("status.py", "local: nada encendido\nGCP simulador: ENCENDIDO (cobra) · https://x.run.app\nGCP Dataflow: ningún job activo",
            first=True)
    code, _, ev = run_part(fake, "off", "--yes", sandbox_dir=sandbox)
    o5 = steps(ev, "off")["O5"]
    assert code == 0 and o5["status"] == "AVISO" and "off.py" in o5["detail"] and "simulador: ENCENDIDO" in o5["detail"]
    fake2, _ = job_runner(sandbox)
    fake2.on("status.py", "local: nada encendido\nGCP simulador: apagado\nGCP Dataflow: ningún job activo", first=True)
    _, _, ev2 = run_part(fake2, "off", "--yes", sandbox_dir=sandbox)
    assert steps(ev2, "off")["O5"]["status"] == "OK"
