"""on.py / off.py / status.py (operación). Sin red, sin Docker y sin Terraform: comandos falsos."""

from __future__ import annotations

import json

import pytest

from ops import services as sv


Result_OK_URL = sv.Result(0, "https://ta-stream-simulator-x.run.app\n")

SCHEDULE = {
    "Name": "ta-wf-raw-to-curated-cada-hora", "GroupName": "default", "ScheduleExpression": "rate(1 hour)",
    "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED", "Arn": "arn:aws:scheduler:us-east-1:1:schedule/x",
    "CreationDate": "2026-10-26T22:55:00Z",
    "Target": {"Arn": "arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun", "RoleArn": "arn:aws:iam::1:role/r",
               "Input": "{\"Name\": \"ta-wf-raw-to-curated\"}", "RetryPolicy": {"MaximumRetryAttempts": 0}},
}


def _schedule(state):
    return sv.Result(0, json.dumps({**SCHEDULE, "State": state}))


class FakeRun:
    """Responde según el comando; registra todo lo que se pidió."""

    def __init__(self, responses=None):
        self.calls = []
        self.responses = {
            "services describe": Result_OK_URL,
            "scheduler jobs describe": sv.Result(0, "PAUSED\n"),
            "scheduler get-schedule": _schedule("ENABLED"),
        }
        self.responses.update(responses or {})

    def __call__(self, args, *, capture=True, env=None, check=False):
        self.calls.append((list(args), env))
        key = " ".join(args)
        for pattern, res in self.responses.items():
            if pattern in key:
                res = res(args) if callable(res) else res
                if check and not res.ok:
                    raise sv.OpsError(key)
                return res
        return sv.Result(0, "", "")

    def ran(self, fragment):
        return [a for a, _e in self.calls if fragment in " ".join(a)]


def _plan(*changes):
    return sv.Result(0, json.dumps({"resource_changes": [
        {"address": a, "type": t, "change": {"actions": acts}} for a, t, acts in changes
    ]}))


SIM = ("module.cloud_run_simulator[0].google_cloud_run_v2_service.this", "google_cloud_run_v2_service")


@pytest.fixture
def tfvars(tmp_path, monkeypatch):
    f = tmp_path / "terraform.tfvars"
    f.write_text('project_id        = "p"\nsimulator_image   = "img"\nsimulator_running = false\n', encoding="utf-8")
    monkeypatch.setattr(sv, "TFVARS", f)
    return f


@pytest.fixture(autouse=True)
def _scripts_sin_crlf(monkeypatch):
    """Las pruebas no dependen de los scripts reales del repo."""
    monkeypatch.setattr(sv, "crlf_scripts", lambda scripts_dir=None: [] if scripts_dir is None else _REAL_CRLF(scripts_dir))


_REAL_CRLF = sv.crlf_scripts


@pytest.fixture(autouse=True)
def _terraform_listo(monkeypatch):
    monkeypatch.setattr(sv, "terraform_ready", lambda: True)


def _s(**kw):
    return sv.Settings(project="p", yes=True, wait=False, **kw)


# --------------------------------------------------------------------------- #
# terraform.tfvars
# --------------------------------------------------------------------------- #


def test_set_simulator_running_conserva_alineacion():
    text = 'project_id        = "p"\nsimulator_running = false # comentario\n'
    out = sv.set_simulator_running(text, True)
    assert out == 'project_id        = "p"\nsimulator_running = true # comentario\n'
    assert sv.read_simulator_running(out) is True
    assert sv.set_simulator_running(out, True) == out


def test_set_simulator_running_agrega_si_falta():
    out = sv.set_simulator_running('project_id = "p"\n', False)
    assert out.endswith("\nsimulator_running = false\n")
    assert sv.read_simulator_running('x = 1\n') is None


# --------------------------------------------------------------------------- #
# Plan vigilado
# --------------------------------------------------------------------------- #


def test_plan_solo_simulador_se_acepta():
    changes = sv.plan_changes(json.loads(_plan((SIM[0], SIM[1], ["create"]), ("x", "google_storage_bucket", ["no-op"])).out))
    assert changes == [(SIM[0], SIM[1], ["create"])]
    assert sv.unexpected_changes(changes, "create") == []


def test_plan_con_otros_cambios_se_rechaza():
    changes = [(SIM[0], SIM[1], ["create"]), ("module.pubsub.google_pubsub_topic.t", "google_pubsub_topic", ["delete"])]
    assert sv.unexpected_changes(changes, "create") == ["module.pubsub.google_pubsub_topic.t (delete)"]
    assert sv.unexpected_changes([(SIM[0], SIM[1], ["delete", "create"])], "create")  # reemplazo: no


def test_terraform_simulador_aplica_solo_si_el_plan_es_seguro(tfvars):
    run = FakeRun({" show -json ": _plan((SIM[0], SIM[1], ["create"]))})
    msgs = []
    out = sv.terraform_simulator(run, True, _s(), msgs.append)
    assert "create" in out
    assert sv.read_simulator_running(tfvars.read_text()) is True
    assert run.ran(" apply ")


def test_terraform_simulador_no_aplica_si_el_plan_toca_otra_cosa(tfvars):
    run = FakeRun({" show -json ": _plan((SIM[0], SIM[1], ["create"]), ("google_bigquery_table.t", "google_bigquery_table", ["delete"]))})
    with pytest.raises(sv.OpsError, match="no se aplica nada"):
        sv.terraform_simulator(run, True, _s(), print)
    assert not run.ran(" apply ")


def test_terraform_sin_cambios_no_aplica(tfvars):
    run = FakeRun({" show -json ": _plan()})
    assert sv.terraform_simulator(run, False, _s(), print) == "sin cambios"
    assert not run.ran(" apply ")


# --------------------------------------------------------------------------- #
# Dataflow y NLP
# --------------------------------------------------------------------------- #


def _jobs(*jobs):
    return sv.Result(0, json.dumps([{"id": i, "name": n, "state": st} for i, n, st in jobs]))


def test_cloud_on_no_relanza_un_job_activo(tfvars):
    run = FakeRun({" show -json ": _plan(), "jobs list": _jobs(("j1", "ta-stream-dev", "Running"))})
    sv.cloud_on(run, _s(), sv.Report())
    assert not run.ran("dataflow_job.sh start")


def test_cloud_on_lanza_con_nlp_si_hay_imagen(tfvars, monkeypatch):
    monkeypatch.setattr(sv, "nlp_code_present", lambda: True)
    monkeypatch.delenv("TA_NLP_IMAGE", raising=False)
    img = json.dumps([{"version": "sha256:x", "tags": ["abc1234", "latest"]}])
    run = FakeRun({" show -json ": _plan(), "jobs list": _jobs(), "images list": sv.Result(0, img)})
    sv.cloud_on(run, _s(), sv.Report())
    (args, env), = [c for c in run.calls if "dataflow_job.sh" in " ".join(c[0])]
    assert env["TA_NLP_IMAGE"] == "us-central1-docker.pkg.dev/p/ta-images/dataflow-nlp:abc1234"
    assert env["TA_NLP_ENABLED"] is None


def test_cloud_on_sin_imagen_apaga_el_nlp(tfvars, monkeypatch):
    monkeypatch.setattr(sv, "nlp_code_present", lambda: True)
    monkeypatch.delenv("TA_NLP_IMAGE", raising=False)
    run = FakeRun({" show -json ": _plan(), "jobs list": _jobs(), "images list": sv.Result(1, "", "not found")})
    sv.cloud_on(run, _s(), sv.Report())
    (_a, env), = [c for c in run.calls if "dataflow_job.sh" in " ".join(c[0])]
    assert env["TA_NLP_ENABLED"] == "false"
    with pytest.raises(sv.OpsError, match="build_nlp_image"):
        sv.cloud_on(run, _s(nlp="on"), sv.Report())


def test_cloud_on_sin_codigo_nlp_no_toca_variables(tfvars, monkeypatch):
    monkeypatch.setattr(sv, "nlp_code_present", lambda: False)
    run = FakeRun({" show -json ": _plan(), "jobs list": _jobs()})
    sv.cloud_on(run, _s(), sv.Report())
    (_a, env), = [c for c in run.calls if "dataflow_job.sh" in " ".join(c[0])]
    assert env["TA_NLP_ENABLED"] is None and env["TA_NLP_IMAGE"] is None


def test_cloud_off_drena_apaga_simulador_y_limpia(tfvars, monkeypatch):
    tfvars.write_text("simulator_running = true\n")
    state = {"jobs": [("j1", "ta-stream-dev", "Running")]}

    def jobs(_args):
        res = _jobs(*state["jobs"])
        if any("drain" in " ".join(a) for a, _e in run.calls):
            state["jobs"] = []
        return res

    monkeypatch.setattr(sv.time, "sleep", lambda s: None)
    run = FakeRun({
        "jobs list": jobs,
        " show -json ": _plan((SIM[0], SIM[1], ["delete"])),
        "subscriptions list": sv.Result(0, "projects/p/subscriptions/ta-verify-reddit-1\nprojects/p/subscriptions/ta-raw-reddit-df\n"),
    })
    sv.cloud_off(run, _s(), sv.Report())
    assert run.ran("jobs drain j1")
    assert run.ran(" apply ")
    assert sv.read_simulator_running(tfvars.read_text()) is False
    assert run.ran("subscriptions delete ta-verify-reddit-1")
    assert not run.ran("subscriptions delete ta-raw-reddit-df")


def test_cloud_off_puede_dejar_el_simulador(tfvars, monkeypatch):
    monkeypatch.setattr(sv.time, "sleep", lambda s: None)
    run = FakeRun({"jobs list": _jobs()})
    sv.cloud_off(run, _s(), sv.Report(), keep_simulator=True)
    assert not run.ran("terraform")


def test_main_off_por_defecto_solo_nube(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setattr(sv.time, "sleep", lambda s: None)
    run = FakeRun({"jobs list": _jobs(), " show -json ": _plan()})
    assert sv.main("off", ["--yes"], run=run) == 0
    assert not run.ran("down-all")


def test_main_error_claro_y_codigo_2(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    run = FakeRun({"docker info": sv.Result(1, "", "no docker")})
    assert sv.main("on", ["local", "--yes"], run=run) == 2


def test_status_no_modifica_nada(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    run = FakeRun({"jobs list": _jobs(("j1", "ta-stream-dev", "Running")), "services describe": sv.Result(0, "https://ta-stream-simulator-x.run.app\n")})
    assert sv.main("status", run=run) == 0
    mutating = [a for a, _e in run.calls if any(w in a for w in ("apply", "drain", "delete", "cancel", "start", "up", "down-all"))]
    assert mutating == []


def test_detecta_scripts_con_crlf(tmp_path, monkeypatch):
    (tmp_path / "local_dataflow.sh").write_bytes(b"#!/usr/bin/env bash\r\nset -euo pipefail\r\n")
    (tmp_path / "dataflow_job.sh").write_bytes(b"#!/usr/bin/env bash\nset -euo pipefail\n")
    assert sv.crlf_scripts(tmp_path) == ["project/scripts/local_dataflow.sh"]
    monkeypatch.setattr(sv, "crlf_scripts", lambda: ["project/scripts/local_dataflow.sh"])
    with pytest.raises(sv.OpsError, match="CRLF"):
        sv.check_line_endings()


def test_main_avisa_crlf_antes_de_actuar(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setattr(sv, "crlf_scripts", lambda: ["project/scripts/local_dataflow.sh"])
    run = FakeRun()
    assert sv.main("on", ["--yes"], run=run) == 2
    assert run.calls == [] or all("gcloud" in " ".join(a) for a, _e in run.calls)


def test_terraform_sin_init_da_el_comando(tfvars, monkeypatch):
    monkeypatch.setattr(sv, "terraform_ready", lambda: False)
    with pytest.raises(sv.OpsError, match="init -backend-config=backend.hcl"):
        sv.terraform_simulator(FakeRun(), True, _s(), print)


def test_plan_que_pide_init_da_el_comando(tfvars):
    run = FakeRun({" plan ": sv.Result(1, "", 'Error: Backend initialization required, please run "terraform init"')})
    with pytest.raises(sv.OpsError, match="init -backend-config=backend.hcl"):
        sv.terraform_simulator(run, True, _s(), print)


def test_status_simulador_por_describe():
    run = FakeRun({"services describe": sv.Result(0, "https://ta-stream-simulator-x.run.app\n")})
    assert sv.simulator_service(run, _s()) == "https://ta-stream-simulator-x.run.app"
    run = FakeRun({"services describe": sv.Result(1, "", "NOT_FOUND")})
    assert sv.simulator_service(run, _s()) is None


def test_cloud_on_sin_imagen_del_simulador_no_sigue(tfvars):
    tfvars.write_text('project_id = "p"\nsimulator_running = true\n')
    run = FakeRun()
    with pytest.raises(sv.OpsError, match="simulator_image"):
        sv.cloud_on(run, _s(), sv.Report())
    assert not run.ran("terraform")


def test_cloud_on_falla_si_el_servicio_no_aparece(tfvars, monkeypatch):
    monkeypatch.setattr(sv, "nlp_code_present", lambda: False)
    run = FakeRun({" show -json ": _plan(), "services describe": sv.Result(1, "", "Cannot find service")})
    with pytest.raises(sv.OpsError, match="no existe en Cloud Run"):
        sv.cloud_on(run, _s(), sv.Report())
    assert not run.ran("dataflow_job.sh start")


# --------------------------------------------------------------------------- #
# Flujo GCS → S3 → Glue (v2.51)
# --------------------------------------------------------------------------- #


class Toggle:
    """Estado de un Scheduler que cambia con ``resume``/``pause`` o ``update-schedule``."""

    def __init__(self, state):
        self.state = state
        self.inputs = []

    def gcloud(self, args):
        return sv.Result(0, self.state + "\n")

    def gcloud_set(self, args):
        self.state = "ENABLED" if "resume" in args else "PAUSED"
        return sv.Result(0, "")

    def aws(self, args):
        return _schedule(self.state)

    def aws_set(self, args):
        path = next(a for a in args if a.startswith("file://"))[len("file://"):]
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        self.inputs.append(data)
        self.state = data["State"]
        return sv.Result(0, "{}")


def test_flow_summary_dice_donde_se_corta():
    assert sv.flow_summary(True, True, "ENABLED", "ENABLED").startswith("Flujo de punta a punta: COMPLETO")
    off = sv.flow_summary(False, False, "PAUSED", "ENABLED")
    assert "CORTADO en simulador (no entra dato nuevo)" in off
    assert "también apagado: Dataflow, exportador" in off
    assert "CORTADO en exportador (llega a BigQuery, GCS y alertas, no a AWS)" in sv.flow_summary(True, True, "PAUSED", "ENABLED")
    assert "CORTADO en horario del lakehouse" in sv.flow_summary(True, True, "ENABLED", None)
    for text in (off, sv.flow_summary(True, True, "ENABLED", "ENABLED")):
        assert "ENCENDIDO" not in text and "(cobra)" not in text  # los validadores buscan esas marcas


def test_exporter_set_reanuda_y_comprueba():
    t = Toggle("PAUSED")
    run = FakeRun({"scheduler jobs describe": t.gcloud, "scheduler jobs resume": t.gcloud_set})
    assert sv.exporter_set(run, _s(), True) == ("PAUSED", "ENABLED")
    assert sv.exporter_set(run, _s(), True) == ("ENABLED", "ENABLED")
    assert len(run.ran("scheduler jobs resume")) == 1
    stuck = FakeRun({"scheduler jobs resume": sv.Result(1, "", "PERMISSION_DENIED")})
    with pytest.raises(sv.OpsError, match="PERMISSION_DENIED"):
        sv.exporter_set(stuck, _s(), True)


def test_exporter_inexistente_no_falla():
    run = FakeRun({"scheduler jobs describe": sv.Result(1, "", "NOT_FOUND: Job not found")})
    assert sv.exporter_set(run, _s(), False) == ("NO_EXISTE", "NO_EXISTE")
    assert not run.ran("scheduler jobs pause")


def test_lakehouse_set_reenvia_la_definicion_entera():
    t = Toggle("DISABLED")
    run = FakeRun({"scheduler get-schedule": t.aws, "scheduler update-schedule": t.aws_set})
    assert sv.lakehouse_set(run, True) == ("DISABLED", "ENABLED")
    sent, = t.inputs
    assert sent["State"] == "ENABLED"
    assert sent["Target"]["RetryPolicy"] == {"MaximumRetryAttempts": 0}  # sin ella vuelve a 185 reintentos
    assert "Arn" not in sent and "CreationDate" not in sent
    assert sv.lakehouse_set(run, True) == ("ENABLED", "ENABLED") and len(t.inputs) == 1


def test_lakehouse_sin_credenciales_da_el_comando():
    run = FakeRun({"scheduler get-schedule": sv.Result(255, "", "Unable to locate credentials")})
    with pytest.raises(sv.OpsError, match="aws scheduler get-schedule"):
        sv.lakehouse_set(run, True)


def test_flow_off_ultima_corrida_y_deja_el_lakehouse(monkeypatch):
    monkeypatch.setattr(sv.time, "sleep", lambda s: None)
    t = Toggle("ENABLED")
    pending = {"n": 1}

    def executions(_args):
        busy = pending["n"] > 0
        pending["n"] -= 1
        return sv.Result(0, json.dumps([{"metadata": {"name": "ta-gcs-to-s3-abc"}, "status": {}}] if busy else []))

    run = FakeRun({"scheduler jobs describe": t.gcloud, "scheduler jobs pause": t.gcloud_set,
                   "jobs executions list": executions, "jobs list": _jobs()})
    say = sv.Report()
    sv.flow_off(run, _s(), say)
    assert t.state == "PAUSED"
    execute = run.ran("run jobs execute ta-gcs-to-s3")
    assert execute and "--wait" in execute[0]
    assert len(run.ran("jobs executions list")) == 2  # esperó a la corrida programada
    assert not run.ran("update-schedule")
    assert any("se deja como está" in line for line in say.lines)


def test_flow_off_sin_ultima_corrida_si_ya_estaba_en_pausa_o_dataflow_drena():
    run = FakeRun({"jobs list": _jobs()})
    sv.flow_off(run, _s(), sv.Report())
    assert not run.ran("run jobs execute")
    t = Toggle("ENABLED")
    run = FakeRun({"scheduler jobs describe": t.gcloud, "scheduler jobs pause": t.gcloud_set,
                   "jobs list": _jobs(("j1", "ta-stream-dev", "Draining"))})
    sv.flow_off(run, _s(), sv.Report())
    assert t.state == "PAUSED" and not run.ran("run jobs execute")


def test_flow_off_lakehouse_lo_deshabilita():
    t = Toggle("ENABLED")
    run = FakeRun({"scheduler get-schedule": t.aws, "scheduler update-schedule": t.aws_set, "jobs list": _jobs()})
    sv.flow_off(run, _s(), sv.Report(), final_export=False, lakehouse=True)
    assert t.state == "DISABLED"


def test_main_on_reanuda_el_flujo_antes_del_stream(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    t = Toggle("PAUSED")
    run = FakeRun({"scheduler jobs describe": t.gcloud, "scheduler jobs resume": t.gcloud_set,
                   " show -json ": _plan(), "jobs list": _jobs(("j1", "ta-stream-dev", "Running"))})
    assert sv.main("on", ["cloud", "--yes"], run=run) == 0
    keys = [" ".join(a) for a, _e in run.calls]
    resume = next(i for i, k in enumerate(keys) if "scheduler jobs resume" in k)
    plan = next(i for i, k in enumerate(keys) if " plan " in k)
    assert resume < plan and t.state == "ENABLED"
    assert not run.ran("update-schedule")  # el lakehouse ya estaba ENABLED


def test_main_stream_only_no_toca_el_flujo(tfvars, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setattr(sv.time, "sleep", lambda s: None)
    run = FakeRun({" show -json ": _plan(), "jobs list": _jobs(("j1", "ta-stream-dev", "Running"))})
    assert sv.main("on", ["cloud", "--yes", "--stream-only"], run=run) == 0
    run.responses["jobs list"] = _jobs()
    assert sv.main("off", ["--yes", "--stream-only"], run=run) == 0
    assert not run.ran("scheduler jobs resume") and not run.ran("scheduler jobs pause")
    assert not run.ran("update-schedule") and not run.ran("run jobs execute")


def test_status_muestra_flujo_alertas_y_quicksight(tfvars):
    qs = json.dumps({"AccountInfo": {"AccountSubscriptionStatus": "ACCOUNT_CREATED", "Edition": "ENTERPRISE"}})
    run = FakeRun({"jobs list": _jobs(), "services describe": sv.Result(1, "", "NOT_FOUND"),
                   "sts get-caller-identity": sv.Result(0, "123456789012\n"),
                   "describe-account-subscription": sv.Result(0, qs)})
    lines = sv.status(run, _s())
    text = "\n".join(lines)
    assert "GCP exportador: en pausa" in text
    assert "AWS lakehouse: horario ta-wf-raw-to-curated-cada-hora ENABLED" in text
    assert "GCP alertas: sin notificador" in text
    assert "AWS QuickSight: ACCOUNT_CREATED" in text
    assert lines[-1].startswith("Flujo de punta a punta: CORTADO en simulador")
    hourly = [ln for ln in lines if not ln.startswith("AWS QuickSight")]  # QuickSight depende de la fecha
    assert [ln for ln in hourly if "ENCENDIDO" in ln or "(cobra)" in ln] == []  # nada cobra por hora


def test_quicksight_line_avisa_al_terminar_la_prueba():
    from datetime import datetime, timezone

    from ops import validate61

    qs = json.dumps({"AccountInfo": {"AccountSubscriptionStatus": "ACCOUNT_CREATED"}})
    run = FakeRun({"sts get-caller-identity": sv.Result(0, "123456789012\n"), "describe-account-subscription": sv.Result(0, qs)})
    during = sv.quicksight_line(run, now=validate61.SUBSCRIBED_AT)
    after = sv.quicksight_line(run, now=datetime(2027, 1, 1, tzinfo=timezone.utc))
    assert "prueba gratis hasta" in during and "(cobra)" not in during
    assert "(cobra)" in after and "unsubscribe --confirm-unsubscribe" in after
    no_creds = FakeRun({"sts get-caller-identity": sv.Result(255, "", "no creds")})
    assert sv.quicksight_line(no_creds) == "AWS QuickSight: sin dato (¿credenciales de AWS?)"
