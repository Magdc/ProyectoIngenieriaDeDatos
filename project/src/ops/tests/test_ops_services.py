"""on.py / off.py / status.py (operación). Sin red, sin Docker y sin Terraform: comandos falsos."""

from __future__ import annotations

import json

import pytest

from ops import services as sv


class FakeRun:
    """Responde según el comando; registra todo lo que se pidió."""

    def __init__(self, responses=None):
        self.calls = []
        self.responses = responses or {}

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
    run = FakeRun({"jobs list": _jobs(("j1", "ta-stream-dev", "Running")), "services list": sv.Result(0, "ta-stream-simulator\n")})
    assert sv.main("status", run=run) == 0
    mutating = [a for a, _e in run.calls if any(w in a for w in ("apply", "drain", "delete", "cancel", "start", "up", "down-all"))]
    assert mutating == []
