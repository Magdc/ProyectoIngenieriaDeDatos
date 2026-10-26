"""Validador del Lote 3.1 con un ejecutor falso: sin red, Docker, Terraform ni nube."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops import validate31 as v
from ops.validate31 import AVISO, FALLA, OK, Ctx, Result


class FakeRunner:
    """Responde según la primera regla cuyo texto aparece en el comando."""

    def __init__(self, rules, project: Path):
        self.rules = rules
        self.project = project
        self.calls: list[str] = []

    def __call__(self, cmd, cwd=None, env=None, live=False, timeout=None):
        line = " ".join(map(str, cmd))
        self.calls.append(line)
        for needle, action in self.rules:
            if needle in line:
                out = action(line, self.project) if callable(action) else action
                return out if isinstance(out, Result) else Result(0, str(out))
        return Result(0, "")


def _ctx(tmp_path, rules, argv=("preflight",), **kw) -> Ctx:
    args = v.build_parser().parse_args(list(argv))
    ctx = Ctx(args=args, run=FakeRunner(rules, tmp_path), which=lambda t: f"/usr/bin/{t}",
              ask=kw.pop("ask", lambda q: True), project=tmp_path,
              evidence=tmp_path / "evidencia" / "validacion_3_1.json",
              importer=kw.pop("importer", lambda name: None))
    return ctx


def _status(ctx, sid):
    return next(s for s in ctx.steps if s.id == sid)


def _write(project: Path, rel: str, data) -> None:
    path = project / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


# --------------------------------------------------------------------- parsers

@pytest.mark.parametrize("out, expected", [
    ("....\n120 passed in 9.1s", {"passed": 120, "failed": 0, "skipped": 0, "errors": 0}),
    ("x\n3 failed, 117 passed, 2 skipped in 10s", {"passed": 117, "failed": 3, "skipped": 2, "errors": 0}),
    ("= 1 error in 0.2s =", {"passed": 0, "failed": 0, "skipped": 0, "errors": 1}),
    ("SKIPPED [1] a.py: sin terraform\n10 passed, 1 skipped in 1s", {"passed": 10, "failed": 0, "skipped": 1, "errors": 0}),
    ("nada", {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}),
])
def test_parse_pytest(out, expected):
    assert v.parse_pytest(out) == expected


def test_parse_plan():
    assert v.parse_plan("Plan: 10 to add, 0 to change, 0 to destroy.") == {"add": 10, "change": 0, "destroy": 0}
    assert v.parse_plan("No changes. Your infrastructure matches") == {"add": 0, "change": 0, "destroy": 0}
    assert v.parse_plan("Error: boom") is None


def test_parse_json_tail():
    assert v.parse_json_tail('texto\n{"event": "x"}\n{"a": 1}') == {"a": 1}
    assert v.parse_json_tail('perfil: x\n{\n "a": {"b": 2}\n}') == {"a": {"b": 2}}
    assert v.parse_json_tail("sin json") is None


def test_read_tfvar(tmp_path):
    p = tmp_path / "terraform.tfvars"
    p.write_text('aws_account_id = "111122223333"\nenv = "dev"\n')
    assert v.read_tfvar(p, "aws_account_id") == "111122223333"
    assert v.read_tfvar(p, "otro") is None
    assert v.read_tfvar(tmp_path / "no", "x") is None


def test_evidencia_se_acumula_por_parte(tmp_path):
    ctx = _ctx(tmp_path, [])
    ctx.add(v.Step("A1", "x", OK, "bien"))
    v.save_evidence(ctx, "A")
    ctx2 = _ctx(tmp_path, [])
    ctx2.add(v.Step("B1", "y", FALLA, "mal"))
    v.save_evidence(ctx2, "B")
    data = json.loads(ctx.evidence.read_text())
    assert data["lote"] == "3.1" and data["parts"]["A"]["status"] == OK
    assert data["parts"]["B"]["status"] == FALLA


# --------------------------------------------------------------------- parte A

def _touch_files(project: Path):
    for rel in v.LOT_FILES + v.PREVIOUS_FILES:
        p = project / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("")


def test_preflight_ok_y_avisos(tmp_path):
    _touch_files(tmp_path)
    (tmp_path.parent / ".gitignore").write_text(".local-s3/\n")

    def importer(name):
        if name == "boto3":
            raise ImportError("no boto3")

    ctx = _ctx(tmp_path, [("doctor", "  D1   OK  x\n  D2   OK  y\nresultado: OK")], importer=importer)
    v.part_preflight(ctx)
    assert _status(ctx, "A1").status == AVISO and "boto3" in _status(ctx, "A1").detail
    assert _status(ctx, "A2").status == OK and _status(ctx, "A3").status == OK
    assert _status(ctx, "A4").status == OK and _status(ctx, "A6").status == OK
    assert _status(ctx, "A7").status == OK


def test_preflight_distingue_docker_apagado_de_imagen_ausente(tmp_path):
    ctx = _ctx(tmp_path, [("doctor", "ok"), ("docker info", Result(1, "Cannot connect"))])
    v.part_preflight(ctx)
    assert _status(ctx, "A6").status == AVISO and "no responde" in _status(ctx, "A6").detail
    ctx = _ctx(tmp_path, [("doctor", "ok"), ("docker image inspect", Result(1, "No such image"))])
    v.part_preflight(ctx)
    assert "no está" in _status(ctx, "A6").detail


STREAM_PS = ("trendanalyzer-local-dataflow-local-1\tta-dataflow-nlp-local:local\n"
             "trendanalyzer-local-simulator-1\tta-simulator:local\n"
             "otro\tpostgres:16\n")


def test_preflight_avisa_si_el_stream_local_compite_por_la_cpu(tmp_path):
    ctx = _ctx(tmp_path, [("doctor", "ok"), ("docker ps", STREAM_PS)])
    v.part_preflight(ctx)
    a8 = _status(ctx, "A8")
    assert a8.status == AVISO and "local_dataflow.sh down-all" in a8.detail
    assert a8.data["containers"] == ["trendanalyzer-local-dataflow-local-1",
                                     "trendanalyzer-local-simulator-1"]
    ctx = _ctx(tmp_path, [("doctor", "ok")])
    v.part_preflight(ctx)
    assert _status(ctx, "A8").status == OK


def test_nlp_no_arranca_con_el_stream_local_encendido(tmp_path, monkeypatch):
    rules = [("docker ps", STREAM_PS), ("historical_nlp.sh", "ok"), (" status ", _nlp_status())]
    ctx = _ctx(tmp_path, rules, ["load"])
    assert v._run_nlp(ctx, "E4", ".local-s3", "3101") is False
    assert _status(ctx, "E4a").status == FALLA and "--allow-busy" in _status(ctx, "E4a").detail
    assert not any("historical_nlp.sh" in c for c in ctx.run.calls)
    ctx = _ctx(tmp_path, rules, ["load", "--allow-busy"])
    assert v._run_nlp(ctx, "E4", ".local-s3", "3101") is True
    assert _status(ctx, "E4a").status == AVISO and _status(ctx, "E4").status == OK
    busy = [("docker ps", "ta-hist-nlp\tta-dataflow-nlp-local:local\n"), ("historical_nlp.sh", "ok")]
    ctx = _ctx(tmp_path, busy, ["load", "--allow-busy"])
    assert v._run_nlp(ctx, "E4", ".local-s3", "3101") is False
    assert "historical_nlp.sh stop" in _status(ctx, "E4a").detail


def test_preflight_falla_sin_archivos_ni_doctor(tmp_path):
    ctx = _ctx(tmp_path, [("doctor", Result(1, "  D2   FALLA  Simulador: no sé qué pasar a users"))])
    v.part_preflight(ctx)
    assert _status(ctx, "A2").status == FALLA
    assert _status(ctx, "A4").status == FALLA and "users" in _status(ctx, "A4").detail


# --------------------------------------------------------------------- parte B

def test_tests_omitidas_dan_aviso_y_fallas_falla(tmp_path):
    rules = [("src/historical", "SKIPPED [1] x: falta y\n120 passed, 1 skipped in 3s"),
             ("test_ops_validate31", "40 passed in 1s"),
             ("contracts/tests", "1 failed, 17 passed in 1s"),
             ("src/common", "64 passed in 2s")]
    ctx = _ctx(tmp_path, rules, ["tests"])
    v.part_tests(ctx)
    assert [s.status for s in ctx.steps] == [AVISO, OK, FALLA, OK]


# --------------------------------------------------------------------- parte C

OUTPUTS = {"lake_bucket": {"value": "ta-dev-111122223333-lake"},
           "kms_key_arn": {"value": "arn:aws:kms:us-east-1:111122223333:key/k-123"},
           "historical_loader_role_arn": {"value": "arn:aws:iam::111122223333:role/ta-historical-loader"},
           "kms_key_alias": {"value": "alias/ta-dev-lake"},
           "lake_bucket_arn": {"value": "arn:aws:s3:::ta-dev-111122223333-lake"}}


def _tf_rules(plan="Plan: 10 to add, 0 to change, 0 to destroy.", account="111122223333"):
    return [("sts get-caller-identity", json.dumps({"Account": account, "Arn": "arn:aws:iam::x:user/a"})),
            ("validate.sh", "ok"),
            (" init ", "Terraform has been successfully initialized!"),
            (" plan ", plan),
            (" apply ", "Apply complete! Resources: 10 added, 0 changed, 0 destroyed."),
            (" output -json", json.dumps(OUTPUTS)),
            ("get-bucket-encryption", json.dumps({"Rules": [{"ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": OUTPUTS["kms_key_arn"]["value"]}}]})),
            ("get-public-access-block", json.dumps({"PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True}}))]


def _tfvars(project: Path, account="111122223333", backend=True):
    p = project / v.AWS_ENV / "terraform.tfvars"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f'aws_account_id = "{account}"\n')
    if backend:
        (p.parent / "backend.hcl").write_text('region = "us-east-1"\n')


def test_terraform_sin_apply_se_detiene_con_aviso(tmp_path):
    _tfvars(tmp_path)
    ctx = _ctx(tmp_path, _tf_rules(), ["terraform"])
    v.part_terraform(ctx)
    assert _status(ctx, "C2").status == OK and _status(ctx, "C3").status == AVISO
    assert not any(s.id == "C4" for s in ctx.steps)
    assert not any(" apply " in c for c in ctx.run.calls)
    assert any("-target=module.historical_loader" in c for c in ctx.run.calls)


def test_terraform_apply_completo(tmp_path):
    _tfvars(tmp_path)
    ctx = _ctx(tmp_path, _tf_rules(), ["terraform", "--apply", "--yes"])
    v.part_terraform(ctx)
    assert [s.status for s in ctx.steps] == [OK] * 8, ctx.steps
    assert "ta-dev-111122223333-lake" in _status(ctx, "C4").detail


def test_terraform_init_con_backend_hcl_y_cuenta_raiz(tmp_path):
    _tfvars(tmp_path)
    (tmp_path / v.AWS_ENV / "backend.hcl").write_text('region = "us-east-1"\n')
    rules = _tf_rules()
    rules[0] = ("sts get-caller-identity", json.dumps({"Account": "111122223333",
                                                      "Arn": "arn:aws:iam::111122223333:root"}))
    ctx = _ctx(tmp_path, rules, ["terraform"])
    v.part_terraform(ctx)
    assert _status(ctx, "C0").status == AVISO and "raíz" in _status(ctx, "C0").detail
    init = next(c for c in ctx.run.calls if " init " in c)
    assert "-backend-config=backend.hcl" in init
    assert _status(ctx, "C2a").status == OK


def test_terraform_sin_backend_hcl_se_detiene_antes_de_init(tmp_path):
    _tfvars(tmp_path, backend=False)
    ctx = _ctx(tmp_path, _tf_rules(), ["terraform"])
    v.part_terraform(ctx)
    c0b = _status(ctx, "C0b")
    assert c0b.status == FALLA and "backend.hcl" in c0b.detail and "ta-dev-tflock" in c0b.detail
    assert not any(" init " in c or "validate.sh" in c for c in ctx.run.calls)


def test_terraform_init_fallido_muestra_el_error(tmp_path):
    _tfvars(tmp_path)
    rules = _tf_rules()
    rules[2] = (" init ", Result(1, "Error: Missing region value"))
    ctx = _ctx(tmp_path, rules, ["terraform"])
    v.part_terraform(ctx)
    assert _status(ctx, "C2a").status == FALLA and "Missing region" in _status(ctx, "C2a").detail


def test_terraform_ya_aplicado(tmp_path):
    _tfvars(tmp_path)
    ctx = _ctx(tmp_path, _tf_rules(plan="No changes. Your infrastructure matches the configuration."),
               ["terraform"])
    v.part_terraform(ctx)
    assert "ya aplicado" in _status(ctx, "C2").detail
    assert _status(ctx, "C5").status == OK


def test_terraform_plan_con_destrucciones_no_aplica(tmp_path):
    _tfvars(tmp_path)
    ctx = _ctx(tmp_path, _tf_rules(plan="Plan: 10 to add, 1 to change, 2 to destroy."),
               ["terraform", "--apply", "--yes"])
    v.part_terraform(ctx)
    assert _status(ctx, "C2").status == FALLA
    assert not any(" apply " in c for c in ctx.run.calls)


def test_terraform_otra_cuenta(tmp_path):
    _tfvars(tmp_path, account="999999999999")
    ctx = _ctx(tmp_path, _tf_rules(), ["terraform"])
    v.part_terraform(ctx)
    assert _status(ctx, "C0").status == FALLA and len(ctx.steps) == 1


def test_terraform_sin_confirmacion(tmp_path):
    _tfvars(tmp_path)
    ctx = _ctx(tmp_path, _tf_rules(), ["terraform", "--apply"], ask=lambda q: False)
    v.part_terraform(ctx)
    assert _status(ctx, "C3").status == AVISO and "confirmación" in _status(ctx, "C3").detail


# ---------------------------------------------------------------- partes D y E

def _verify_report(status=OK):
    return {"status": status, "summary": {"rows": 600, "files": 500},
            "checks": [{"id": f"H{i}", "status": OK, "detail": "ok"} for i in range(1, 13)]}


def _nlp_status(pending=0, missing=0):
    return json.dumps({"nlp": {"rows": 600, "pending_files": pending, "nlp_missing": missing,
                               "versions": {"robertuito-sentiment@a+robertuito-emotion@b": 600},
                               "rows_per_s_median": 22.5}})


def _write_verify(line, project):
    out = line.split("--json-out ")[1].split()[0]
    _write(project, out, _verify_report())
    return "resultado: OK"


def test_local_completo(tmp_path):
    rules = [("docker info", "29.7.2"), ("docker image inspect", "sha256:abc"),
             ("historical_nlp.sh doctor", "D6 OK"), ("historical_nlp.sh --root", "ok"),
             (" status ", _nlp_status()), (" verify ", _write_verify),
             ("--digest-only", "digest: x (igual al de generate.json)"),
             (" write ", "500 archivos (0 escritos, 500 sin cambios, 0 borrados), 600 filas"),
             (" generate ", "ok")]
    ctx = _ctx(tmp_path, rules, ["local"])
    v.part_local(ctx)
    assert [s.id for s in ctx.steps] == [f"D{i}" for i in range(1, 10)]
    assert all(s.status == OK for s in ctx.steps), ctx.steps
    gen = next(c for c in ctx.run.calls if " generate " in c and "--digest-only" not in c)
    assert "--key-source local_dev" in gen and ".local-s3/_smoke" in gen and "--seed 3199" in gen
    digest = next(c for c in ctx.run.calls if "--digest-only" in c)
    assert "--n 600" in digest and "--days 60" in digest


def test_load_pasa_opciones_y_espera_al_nlp(tmp_path, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    ctx = _ctx(tmp_path, [("historical_nlp.sh", Result(1, "x")), (" status ", _nlp_status(pending=1))],
               ["load", "--nlp-batch-seconds", "5", "--nlp-workers", "4"])
    v._run_nlp(ctx, "E4", ".local-s3", "3101")
    call = next(c for c in ctx.run.calls if "historical_nlp.sh" in c)
    assert "--batch-seconds 5" in call and "--workers 4" in call and "--runner" not in call


def test_nlp_runner_fnapi_se_rechaza(capsys):
    assert v.main(["load", "--nlp-runner", "fnapi"]) == 2
    assert "--nlp-workers" in capsys.readouterr().err


def test_local_sin_docker(tmp_path):
    ctx = _ctx(tmp_path, [("docker info", Result(1, "Cannot connect"))], ["local"])
    v.part_local(ctx)
    assert _status(ctx, "D1").status == FALLA and len(ctx.steps) == 1


def test_local_nlp_a_medias(tmp_path):
    rules = [("docker info", "29"), ("docker image inspect", "sha"), ("historical_nlp.sh doctor", "ok"),
             ("historical_nlp.sh --root", Result(1, "quedan 2")), (" status ", _nlp_status(pending=2)),
             (" generate ", "ok")]
    ctx = _ctx(tmp_path, rules, ["local"])
    v.part_local(ctx)
    assert _status(ctx, "D5").status == FALLA and "repita" in _status(ctx, "D5").detail


def test_load_completo(tmp_path, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "proj-x")

    def gen(line, project):
        if "--digest-only" in line:
            return "digest: abc (igual al de generate.json)"
        _write(project, "evidencia/historical_generate.json",
               {"rows": 50000, "digest": "abc" * 10, "key_check": {"status": "OK", "detail": "412 de 500"}})
        return "ok"

    rules = [("print-access-token", "token"), ("bq.run_query", json.dumps({"n_rows": 9000, "stream_users": 1500, "versions": ["v"]})),
             (" generate ", gen), ("historical_nlp.sh", "ok"), (" status ", _nlp_status()),
             (" write ", "12000 archivos (12000 escritos, 0 sin cambios, 0 borrados)"),
             (" verify ", _write_verify)]
    ctx = _ctx(tmp_path, rules, ["load", "--n", "50000"])
    v.part_load(ctx)
    assert [s.status for s in ctx.steps] == [OK] * 7, ctx.steps
    gen_call = next(c for c in ctx.run.calls if " generate " in c and "--digest-only" not in c)
    assert "--key-source secret" in gen_call and "--gcp-project proj-x" in gen_call
    assert any(" verify " in c and "--bq" in c for c in ctx.run.calls)
    assert any("--digest-only" in c and "--n 50000" in c for c in ctx.run.calls)


def test_load_sin_proyecto(tmp_path, monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    ctx = _ctx(tmp_path, [("output -raw project_id", Result(1, "")), ("print-access-token", "t")], ["load"])
    v.part_load(ctx)
    assert _status(ctx, "E1").status == FALLA and len(ctx.steps) == 1


# --------------------------------------------------------------------- parte F

def _simulate(too_wide=False):
    def sim(line, project):
        allowed = (("s3:PutObject" in line and "raw/origin=historical/_ta_probe" in line)
                   or ("s3:ListBucket" in line and "ContextKeyValues=raw/origin=historical/" in line)
                   or (too_wide and "raw/origin=stream/" in line))
        return json.dumps({"EvaluationResults": [
            {"EvalActionName": "x", "EvalDecision": "allowed" if allowed else "implicitDeny"}]})

    return sim


def _upload_rules(probe="DENEGADO", second_uploaded=0, caller="arn:aws:iam::111122223333:user/alejo",
                  too_wide=False):
    def upload(line, project):
        if "--json-out" in line:
            _write(project, "evidencia/historical_upload.json",
                   {"uploaded": 12000, "skipped_same_size": 0, "n_errors": 0, "n_stale": 0, "seconds": 80,
                    **({"permission_probe": {"put raw/origin=stream/": probe, "list trusted/": "DENEGADO"}}
                       if "--probe" in line else {})})
            return "ok"
        return "perfil: x\n" + json.dumps({"files": 12000, "to_upload": second_uploaded,
                                           "uploaded": second_uploaded,
                                           "skipped_same_size": 12000 - second_uploaded,
                                           "n_errors": 0, "n_stale": 0})

    return [(" output -json", json.dumps(OUTPUTS)), (" upload ", upload), (" verify ", _write_verify),
            ("sts get-caller-identity", json.dumps({"Account": "111122223333", "Arn": caller})),
            ("simulate-principal-policy", _simulate(too_wide))]


def test_upload_completo(tmp_path):
    ctx = _ctx(tmp_path, _upload_rules(), ["upload"])
    v.part_upload(ctx)
    assert [s.status for s in ctx.steps] == [OK] * 5, ctx.steps
    first = next(c for c in ctx.run.calls if " upload " in c)
    assert "--role-arn arn:aws:iam::111122223333:role/ta-historical-loader" in first
    assert "--kms-key-arn arn:aws:kms" in first
    assert any(" verify " in c and "--s3" in c for c in ctx.run.calls)


def test_upload_con_la_cuenta_raiz_usa_el_perfil_y_simula_el_rol(tmp_path):
    ctx = _ctx(tmp_path, _upload_rules(caller="arn:aws:iam::111122223333:root"), ["upload"])
    v.part_upload(ctx)
    assert [s.status for s in ctx.steps] == [OK, AVISO, OK, OK, OK], ctx.steps
    uploads = [c for c in ctx.run.calls if " upload " in c]
    assert all("--no-role" in c and "--role-arn" not in c and "--probe" not in c for c in uploads)
    assert "simulada" in _status(ctx, "F3").detail
    assert sum("simulate-principal-policy" in c for c in ctx.run.calls) == 6


def test_upload_politica_demasiado_amplia_en_la_simulacion(tmp_path):
    ctx = _ctx(tmp_path, _upload_rules(too_wide=True), ["upload"])
    v.part_upload(ctx)
    assert _status(ctx, "F3").status == FALLA and "raw/origin=stream/" in _status(ctx, "F3").detail


def test_upload_rol_demasiado_amplio(tmp_path):
    ctx = _ctx(tmp_path, _upload_rules(probe="PERMITIDO"), ["upload"])
    v.part_upload(ctx)
    assert _status(ctx, "F3").status == FALLA


def test_upload_no_idempotente(tmp_path):
    ctx = _ctx(tmp_path, _upload_rules(second_uploaded=5), ["upload"])
    v.part_upload(ctx)
    assert _status(ctx, "F5").status == FALLA


def test_upload_sin_outputs(tmp_path):
    ctx = _ctx(tmp_path, [(" output -json", Result(1, "No outputs"))], ["upload"])
    v.part_upload(ctx)
    assert _status(ctx, "F1").status == FALLA and len(ctx.steps) == 1


# ------------------------------------------------------------------------ main

def test_main_devuelve_1_con_falla_y_guarda_evidencia(tmp_path):
    ctx = _ctx(tmp_path, [(" output -json", Result(1, ""))], ["upload"])
    assert v.main(["upload"], ctx=ctx) == 1
    data = json.loads(ctx.evidence.read_text())
    assert data["parts"]["F"]["status"] == FALLA


def test_run_command_real_sin_stdin(tmp_path):
    import sys

    res = v.run_command([sys.executable, "-c", "import sys; print(sys.stdin.read() == '')"])
    assert res.rc == 0 and res.out.strip() == "True"
    live = v.run_command([sys.executable, "-c", "print('hola')"], live=True)
    assert live.out.strip() == "hola"
    assert v.run_command(["no-existe-este-comando"]).rc == 127
