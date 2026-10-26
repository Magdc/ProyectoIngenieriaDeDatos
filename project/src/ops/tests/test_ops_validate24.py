"""Validador del Lote 2.4 (``scripts/validate_2_4.py``) sin red, Docker ni Terraform."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops import validate24 as v

T0 = datetime(2026, 10, 22, 19, 0, tzinfo=timezone.utc)


def _proc(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


class FakeRun:
    """Responde según el primer patrón que aparezca en el comando; registra las llamadas."""

    def __init__(self, rules):
        self.rules = rules
        self.calls: list[str] = []

    def __call__(self, cmd, cwd=None, timeout=None, env=None, stream=None):
        text = " ".join(str(c) for c in cmd)
        self.calls.append(text)
        self.streamed = getattr(self, "streamed", []) + ([text] if stream else [])
        for pattern, response in self.rules:
            if pattern in text:
                result = response(cmd) if callable(response) else response
                if isinstance(result, list):  # respuestas sucesivas
                    return result.pop(0) if len(result) > 1 else result[0]
                return result
        return _proc()

    def index(self, pattern):
        return next(i for i, c in enumerate(self.calls) if pattern in c)


class FakeClock:
    def __init__(self, start: datetime):
        self.t = start.timestamp()

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds

    def now(self):
        return datetime.fromtimestamp(self.t, tz=timezone.utc)


def _ctx(tmp_path, run, clock=None, confirm=True):
    clock = clock or FakeClock(T0)
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    return v.Ctx(run=run, clock=clock, sleep=clock.sleep, now=clock.now, confirm=lambda _p: confirm,
                 say=lambda _m: None, which=lambda name: f"/usr/bin/{name}", python="python",
                 project_dir=project, evidence_dir=project / "evidencia", env={"ENV": "dev"})


def _args(**over):
    base = dict(apply=False, yes=False, full_plan=False, wait_minutes=15, min_windows=3, down=False, minutes=12,
                catch_up_minutes=15, cancel_after_minutes=0,
                warmup_seconds=300, no_restart=False, shutdown=False, skip_build=False)
    base.update(over)
    return SimpleNamespace(**base)


def _write_report(verdict="OK", checks=None):
    def respond(cmd):
        out = Path(cmd[cmd.index("--json-out") + 1])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"verdict": verdict, "checks": checks or [{"id": "T1", "status": verdict}]}))
        return _proc("ok", 0 if verdict != "FALLA" else 1)
    return respond


# --------------------------------------------------------------------------- utilidades

@pytest.mark.parametrize("text,expected", [
    ("....\n98 passed, 2 skipped, 13 warnings in 5.2s", (98, 0, 2, 0)),
    ("== 3 failed, 10 passed in 1s ==", (10, 3, 0, 0)),
    ("1 passed, 1 error in 0.1s", (1, 0, 0, 1)),
])
def test_parse_pytest_summary(text, expected):
    summary = v.parse_pytest_summary(text)
    assert (summary["passed"], summary["failed"], summary["skipped"], summary["errors"]) == expected
    assert summary["found"]


def test_parse_pytest_sin_resumen():
    assert v.parse_pytest_summary("ImportError: x")["found"] is False


GOOD_PIPELINE = """
from streaming.enriched import attach_enriched
from streaming.trends import attach_trends
def build(p):
    unique = p
    enriched_rows = attach_enriched(unique)
    attach_trends(enriched_rows)
"""
GOOD_ENRICHED = """
def attach_enriched(unique):
    if unique is None:
        return None
    rows = unique
    return rows
"""


def test_hook_ok():
    assert v.hook_problems(GOOD_PIPELINE, GOOD_ENRICHED) == []
    nested = GOOD_PIPELINE.replace("enriched_rows = attach_enriched(unique)\n    attach_trends(enriched_rows)",
                                   "attach_trends(attach_enriched(unique))")
    assert v.hook_problems(nested, GOOD_ENRICHED) == []


@pytest.mark.parametrize("pipeline,enriched,fragment", [
    (GOOD_PIPELINE.replace("from streaming.trends import attach_trends\n", ""), GOOD_ENRICHED, "falta 'from"),
    (GOOD_PIPELINE.replace("    attach_trends(enriched_rows)\n", ""), GOOD_ENRICHED, "falta 'attach_trends"),
    (GOOD_PIPELINE.replace("attach_trends(enriched_rows)", "attach_trends(unique)"), GOOD_ENRICHED, "debe recibir"),
    (GOOD_PIPELINE, "def attach_enriched(unique):\n    x = 1\n    return\n", "no devuelve"),
    (None, None, "no está"),
])
def test_hook_problemas(pipeline, enriched, fragment):
    assert any(fragment in p for p in v.hook_problems(pipeline, enriched))


def _plan(creates=(), updates=(), deletes=()):
    changes = [{"address": a, "change": {"actions": ["create"]}} for a in creates]
    changes += [{"address": a, "change": {"actions": ["update"]}} for a in updates]
    changes += [{"address": a, "change": {"actions": ["delete"]}} for a in deletes]
    changes += [{"address": "x.noop", "change": {"actions": ["no-op"]}}]
    return {"resource_changes": changes}


@pytest.mark.parametrize("plan,status", [
    (_plan(v.EXPECTED_CREATES), v.OK),
    (_plan(), v.OK),
    (_plan(v.EXPECTED_CREATES, deletes=["google_cloud_run_v2_service.sim"]), v.FALLA),
    (_plan([*v.EXPECTED_CREATES, "google_x.extra"]), v.AVISO),
    (_plan(v.EXPECTED_CREATES, updates=["module.simulator.google_cloud_run_v2_service.this"]), v.AVISO),
    (_plan(list(v.EXPECTED_CREATES)[:3]), v.AVISO),
])
def test_check_plan(plan, status):
    assert v.check_plan(v.classify_plan(plan))[0] == status


def test_plan_solo_con_outputs_nuevos():
    plan = _plan()
    plan["output_changes"] = {"dataflow_template_spec": {"actions": ["create"]},
                              "trend_windows_table": {"actions": ["no-op"]}}
    classified = v.classify_plan(plan)
    assert classified["outputs"] == ["dataflow_template_spec"]
    status, detail = v.check_plan(classified)
    assert status == v.OK and "dataflow_template_spec" in detail


def test_reemplazo_es_falla():
    plan = {"resource_changes": [{"address": "a", "change": {"actions": ["delete", "create"]}}]}
    assert v.check_plan(v.classify_plan(plan))[0] == v.FALLA


def test_parse_json_lines():
    text = 'ruido\n{"event": "trends_config", "enabled": true}\n{"event": "otro"}\n{roto trends_config'
    assert v.parse_json_lines(text, "trends_config") == [{"event": "trends_config", "enabled": True}]


def test_evidencia_acumula_partes(tmp_path):
    path = tmp_path / "e.json"
    v.save_evidence("tests", [v.Step("B1", "x", v.OK)], path=path, now=T0)
    data = v.save_evidence("cloud", [v.Step("E1", "y", v.FALLA)], path=path, now=T0)
    assert set(data["parts"]) == {"tests", "cloud"}
    assert data["parts"]["cloud"]["verdict"] == v.FALLA and data["parts"]["tests"]["verdict"] == v.OK


# --------------------------------------------------------------------------- partes

def test_part_tests(tmp_path):
    run = FakeRun([
        ("test_trends_core.py", _proc(f"{v.EXPECTED_LOTE_TESTS} passed in 9s")),
        ("src/streaming -q", _proc("300 passed, 1 warning in 60s")),
        ("contracts/tests", _proc("18 passed in 1s")),
        ("src/ops/tests", _proc("SKIPPED [1] x\n40 passed, 1 skipped in 1s")),
    ])
    steps = v.part_tests(_ctx(tmp_path, run), _args())
    assert [s.status for s in steps] == [v.OK, v.OK, v.OK, v.OK]


def test_part_tests_omitidas_en_el_lote_son_aviso(tmp_path):
    run = FakeRun([("test_trends_core.py", _proc("SKIPPED [1] a.py:1: no está pipeline.py\n190 passed, 9 skipped"))])
    step = v.part_tests(_ctx(tmp_path, run), _args())[0]
    assert step.status == v.AVISO and "pipeline.py" in step.detail


def test_part_tests_falla(tmp_path):
    run = FakeRun([("test_trends_core.py", _proc("1 failed, 10 passed in 1s", 1)),
                   ("src/streaming -q", _proc("ERROR collecting", 2))])
    steps = v.part_tests(_ctx(tmp_path, run), _args())
    assert steps[0].status == v.FALLA and steps[1].status == v.FALLA


def _tf_rules(plan, outputs=None):
    outputs = outputs or {name: {"value": "v"} for name in v.EXPECTED_OUTPUTS}
    return [
        ("validate.sh", _proc("ok")),
        (" plan ", _proc("Plan: 10 to add")),
        (" show -json", _proc(json.dumps(plan))),
        (" apply ", _proc("Apply complete! Resources: 10 added, 0 changed, 0 destroyed.\n\nOutputs:\n\n"
                          'trend_windows_table = "p:ta_stream.trend_windows"')),
        (" output -json", _proc(json.dumps(outputs))),
    ]


def test_part_terraform_solo_plan_con_targets(tmp_path):
    run = FakeRun(_tf_rules(_plan(v.EXPECTED_CREATES)))
    steps = v.part_terraform(_ctx(tmp_path, run), _args())
    assert [s.status for s in steps] == [v.OK, v.OK, v.OMITIDO]
    plan_call = run.calls[run.index(" plan ")]
    assert "-target=module.trends_bq_tables" in plan_call
    assert not any(" apply " in c for c in run.calls)


def test_part_terraform_aplica(tmp_path):
    run = FakeRun(_tf_rules(_plan(v.EXPECTED_CREATES)))
    steps = v.part_terraform(_ctx(tmp_path, run), _args(apply=True, yes=True))
    assert [s.id for s in steps] == ["C1", "C2", "C3", "C4"]
    assert all(s.status == v.OK for s in steps)
    assert "tfplan_2_4" in run.calls[run.index(" apply ")]
    assert steps[2].detail == "Apply complete! Resources: 10 added, 0 changed, 0 destroyed."


def test_part_terraform_aplica_aunque_solo_cambien_outputs(tmp_path):
    plan = _plan()
    plan["output_changes"] = {name: {"actions": ["create"]} for name in v.DERIVABLE_OUTPUTS}
    run = FakeRun(_tf_rules(plan))
    steps = v.part_terraform(_ctx(tmp_path, run), _args(apply=True, yes=True))
    assert [s.status for s in steps] == [v.OK, v.OK, v.OK, v.OK]
    assert any(" apply " in c for c in run.calls)


def test_outputs_derivables_faltantes_son_aviso(tmp_path):
    outputs = {name: {"value": "v"} for name in v.EXPECTED_OUTPUTS if name not in v.DERIVABLE_OUTPUTS}
    run = FakeRun(_tf_rules(_plan(), outputs))
    steps = v.part_terraform(_ctx(tmp_path, run), _args(apply=True, yes=True))
    assert steps[-1].id == "C4" and steps[-1].status == v.AVISO
    outputs.pop("trend_windows_table")
    run = FakeRun(_tf_rules(_plan(), outputs))
    assert v.part_terraform(_ctx(tmp_path, run), _args(apply=True, yes=True))[-1].status == v.FALLA


def test_part_terraform_no_aplica_si_borra(tmp_path):
    run = FakeRun(_tf_rules(_plan(v.EXPECTED_CREATES, deletes=["google_cloud_run_v2_service.x"])))
    steps = v.part_terraform(_ctx(tmp_path, run), _args(apply=True, yes=True))
    assert steps[-1].status == v.FALLA and not any(" apply " in c for c in run.calls)


def test_part_terraform_cancelado(tmp_path):
    run = FakeRun(_tf_rules(_plan(v.EXPECTED_CREATES)))
    steps = v.part_terraform(_ctx(tmp_path, run, confirm=False), _args(apply=True))
    assert steps[-1].status == v.OMITIDO


def test_part_terraform_modulo_no_declarado(tmp_path):
    run = FakeRun([("validate.sh", _proc("", 1, 'Error: Reference to undeclared module "pubsub"'))])
    steps = v.part_terraform(_ctx(tmp_path, run), _args())
    assert steps[0].status == v.FALLA and "platform.tf" in steps[0].detail


def _jobs(*states):
    return [_proc(json.dumps([{"name": "ta-stream-dev", "id": f"job-{s}", "state": s,
                               "creationTime": "2026-10-22T19:00:00Z"}] if s else [])) for s in states]


def _cloud_rules(job_sequence):
    trends_config = json.dumps({"event": "trends_config", "enabled": True, "bq_table": "p:ta_stream.trend_windows",
                                "dlq_enabled": True, "dlq_table": "p:ta_stream.dlq_events",
                                "trend": {"late_delay_seconds": 60}})
    return [
        ("terraform -chdir=infra/gcp/environments/dev output -json",
         _proc(json.dumps({"project_id": {"value": "proj-x"}, "region": {"value": "us-central1"},
                           "labels": {"value": {"env": "dev"}}}))),
        ("print-access-token", _proc("token")),
        ("verify_trends.py", _write_report("OK")),
        ("run services describe", _proc("https://sim\n")),
        ("dataflow jobs list", job_sequence),
        ("dataflow jobs drain", _proc("Started draining job")),
        ("dataflow_job.sh start", _proc("Lanzando…\n" + trends_config + "\n")),
        ("verify_streaming.py", _proc("OK 13/13")),
        ("verify_enriched.py", _proc("OK")),
        ("logging read", _proc("[]")),
    ]


def test_part_cloud_completo(tmp_path):
    jobs = _jobs("Running", "Draining", "", "Pending", "Running")
    run = FakeRun(_cloud_rules(jobs))
    clock = FakeClock(T0)
    ctx = _ctx(tmp_path, run, clock)
    steps = v.part_cloud(ctx, _args())
    statuses = {s.id: s.status for s in steps}
    assert statuses == {"E1": v.OK, "E2": v.OK, "E3": v.OK, "E4a": v.OK, "E4": v.OK, "E5": v.OK, "E6": v.OK,
                        "E7": v.OK, "E8": v.OK, "E9": v.OK, "E10": v.OK}
    assert run.index("dataflow jobs drain job-Running") < run.index("dataflow_job.sh start")
    assert not any("dataflow_job.sh drain" in c for c in run.calls)
    assert any("dataflow_job.sh start" in c for c in run.streamed)
    report_call = run.calls[run.index("report --since")]
    assert "--since 2026-10-22T19:00:00.000Z" in report_call and "--write-dq" in report_call
    assert "--wait-caught-up-minutes 15" in report_call and "--watch-minutes 12" in report_call
    assert ctx.env["GCP_PROJECT"] == "proj-x"
    assert clock() - T0.timestamp() >= 300  # esperó la carga de los modelos


def test_drain_espera_a_un_job_que_ya_drena(tmp_path):
    run = FakeRun(_cloud_rules(_jobs("Draining", "Draining", "")))
    clock = FakeClock(T0)
    step = v.drain_and_wait(_ctx(tmp_path, run, clock), {"project": "p", "region": "r"}, "ta-stream-dev", "x", "E4a")
    assert step.status == v.OK
    assert not any("jobs drain" in c for c in run.calls)
    assert clock() - T0.timestamp() >= 30


def test_drain_tiempo_agotado(tmp_path):
    run = FakeRun(_cloud_rules(_jobs("Running", "Draining")))
    step = v.drain_and_wait(_ctx(tmp_path, run), {"project": "p", "region": "r"}, "ta-stream-dev", "x", "E4a",
                            timeout_s=120)
    assert step.status == v.FALLA and "gcloud dataflow jobs cancel job-Draining --region=r" in step.detail
    assert not any("jobs cancel" in c for c in run.calls)


def test_drain_largo_muestra_que_sigue_esperando(tmp_path):
    run = FakeRun(_cloud_rules(_jobs("Running", *["Draining"] * 20, "")))
    messages = []
    ctx = _ctx(tmp_path, run)
    ctx.say = messages.append
    step = v.drain_and_wait(ctx, {"project": "p", "region": "r"}, "ta-stream-dev", "x", "F3")
    assert step.status == v.OK and step.data["minutes"] >= 9
    assert sum("sigue" in m for m in messages) >= 3


def test_drain_atascado_se_cancela_si_se_pide(tmp_path):
    run = FakeRun(_cloud_rules(_jobs("Running", *["Draining"] * 25, "Cancelling", "")))
    step = v.drain_and_wait(_ctx(tmp_path, run), {"project": "p", "region": "r"}, "ta-stream-dev", "x", "F3",
                            cancel_after_s=600)
    assert step.status == v.AVISO and "cancelado" in step.detail
    assert sum("dataflow jobs cancel job-Draining" in c for c in run.calls) == 1


def test_shutdown_pide_el_drenado_con_gcloud(tmp_path):
    run = FakeRun(_cloud_rules(_jobs("", "Running", "Running")))
    steps = v.part_cloud(_ctx(tmp_path, run), _args(shutdown=True))
    assert steps[-1].id == "E11" and steps[-1].status == v.OK
    assert any("dataflow jobs drain job-Running" in c for c in run.calls)


def test_no_restart_usa_el_job_con_las_opciones_del_lote(tmp_path):
    rules = _cloud_rules(_jobs("Running"))
    rules.insert(0, ("jobs describe", _proc('{"sdkPipelineOptions": {"trend_windows_table": "auto", '
                                            '"dlq_df_subscription": "auto"}}')))
    run = FakeRun(rules)
    clock = FakeClock(T0 + timedelta(hours=1))  # el job se creó hace una hora: sin espera
    steps = v.part_cloud(_ctx(tmp_path, run, clock), _args(no_restart=True))
    statuses = {s.id: s.status for s in steps}
    assert statuses["E4"] == v.OK and statuses["E5"] == v.OMITIDO and statuses["E7"] == v.OK
    assert not any("jobs drain" in c or "dataflow_job.sh start" in c for c in run.calls)
    assert "--since 2026-10-22T19:00:00Z" in run.calls[run.index("report --since")]
    assert clock() == (T0 + timedelta(hours=1)).timestamp()


def test_no_restart_rechaza_un_job_sin_el_codigo_del_lote(tmp_path):
    rules = _cloud_rules(_jobs("Running"))
    rules.insert(0, ("jobs describe", _proc('{"sdkPipelineOptions": {"raw_window_seconds": 300}}')))
    steps = v.part_cloud(_ctx(tmp_path, FakeRun(rules)), _args(no_restart=True))
    assert steps[-1].id == "E4" and steps[-1].status == v.FALLA


def test_job_id_desde_la_salida():
    assert v.job_id_from_output("Submitted job: 2026-10-23_07_46_09-10292586617550137319\nEnviado.") == \
        "2026-10-23_07_46_09-10292586617550137319"
    assert v.job_id_from_output("job:\n  id: 2026-10-23_13_55_17-2551988360961133192\n") == \
        "2026-10-23_13_55_17-2551988360961133192"
    assert v.job_id_from_output("sin id") is None


def _template_launch_rules(states, errors=()):
    job_id = "2026-10-23_13_55_17-2551988360961133192"
    entries = [{"logName": "projects/p/logs/dataflow.googleapis.com%2Flauncher",
                "jsonPayload": {"message": e}} for e in errors]
    return job_id, [
        ("dataflow_template.py --json-out", _proc(f"job:\n  id: {job_id}\n  name: ta-stream-dev\n")),
        ("value(currentState)", [_proc(s + "\n") for s in states]),
        ("logging read", _proc(json.dumps(list(reversed(entries))))),
    ]


def test_start_job_falla_enseguida_si_el_lanzador_falla(tmp_path):
    job_id, rules = _template_launch_rules(
        ["JOB_STATE_QUEUED", "JOB_STATE_FAILED"],
        errors=["Error occurred in the launcher container: Template launch failed.",
                "AttributeError: 'NoneType' object has no attribute 'id'"])
    clock = FakeClock(T0)
    step, job = v._start_job(_ctx(tmp_path, FakeRun(rules), clock), {"project": "p", "region": "r"},
                             "ta-stream-dev", ["python", "scripts/dataflow_template.py", "--json-out", "x", "run"],
                             "F4", "Job lanzado desde la plantilla Flex", 1500)
    assert job is None and step.status == v.FALLA
    assert "JOB_STATE_FAILED" in step.detail and "AttributeError" in step.detail
    assert step.data["job_id"] == job_id
    assert clock() - T0.timestamp() < 120  # no esperó los 25 min


def test_errores_del_lanzador_incluyen_el_traceback_de_info(tmp_path):
    _job_id, rules = _template_launch_rules(
        ["JOB_STATE_FAILED"],
        errors=["{\"event\": \"flex_template_launch\", \"cwd\": \"/template/project/src\"}",
                "Traceback (most recent call last):", "FileNotFoundError: ../contracts",
                "Error: Template launch failed: exit status 1"])
    run = FakeRun(rules)
    step, _job = v._start_job(_ctx(tmp_path, run), {"project": "p", "region": "r"}, "ta-stream-dev",
                              ["python", "scripts/dataflow_template.py", "--json-out", "x", "run"], "F4", "t", 1500)
    assert "FileNotFoundError" in step.detail and "Traceback" in step.detail
    assert "flex_template_launch" not in step.detail.split(":\n", 1)[1]
    assert 'logName:"launcher"' in next(c for c in run.calls if "logging read" in c)


def test_start_job_espera_a_running_por_id(tmp_path):
    _job_id, rules = _template_launch_rules(["JOB_STATE_QUEUED", "JOB_STATE_PENDING", "JOB_STATE_RUNNING"])
    step, job = v._start_job(_ctx(tmp_path, FakeRun(rules)), {"project": "p", "region": "r"}, "ta-stream-dev",
                             ["python", "scripts/dataflow_template.py", "--json-out", "x", "run"], "F4", "t", 1500)
    assert step.status == v.OK and job["state"] == "JOB_STATE_RUNNING"


def test_part_cloud_sin_simulador(tmp_path):
    rules = _cloud_rules(_jobs(""))
    rules.insert(0, ("run services describe", _proc("", 1, "not found")))
    steps = v.part_cloud(_ctx(tmp_path, FakeRun(rules)), _args())
    assert steps[-1].id == "E3" and steps[-1].status == v.FALLA


def test_part_cloud_trends_config_apagado_es_falla(tmp_path):
    rules = _cloud_rules(_jobs("", "Running"))
    off = json.dumps({"event": "trends_config", "enabled": False, "reason": "sin_salida", "dlq_enabled": False})
    rules.insert(0, ("dataflow_job.sh start", _proc(off)))
    steps = v.part_cloud(_ctx(tmp_path, FakeRun(rules)), _args())
    assert {s.id: s.status for s in steps}["E5"] == v.FALLA


def test_part_cloud_logs_con_errores(tmp_path):
    rules = _cloud_rules(_jobs("", "Running"))
    entries = [{"jsonPayload": {"message": '{"event": "trend_bq_insert_failed"}'}},
               {"jsonPayload": {"message": "GETTING_PUBSUB_SUBSCRIPTION_FAILED"}}]
    rules.insert(0, ("logging read", _proc(json.dumps(entries))))
    steps = v.part_cloud(_ctx(tmp_path, FakeRun(rules)), _args())
    logs = {s.id: s for s in steps}["E10"]
    assert logs.status == v.FALLA and logs.data["trend_bq_insert_failed"] == 1


def test_part_local(tmp_path):
    clock = FakeClock(T0)
    ctx = _ctx(tmp_path, None, clock)
    lake = ctx.project_dir / ".local-lake" / "trend_windows" / "dt=2026-10-22" / "hr=19"
    config = json.dumps({"event": "trends_config", "enabled": True, "local_root": "/lake/trend_windows",
                         "trend": {"late_delay_seconds": 0}})

    def logs(_cmd):
        lake.mkdir(parents=True, exist_ok=True)
        rows = []
        for minute in range(1, 6):
            start = T0 + timedelta(minutes=minute)
            rows.append({"window_start": v.iso(start), "window_end": v.iso(start + timedelta(minutes=5)),
                         "brand": "postobon", "source": "reddit"})
        (lake / "part.jsonl").write_text("\n".join(json.dumps(r) for r in rows))
        return _proc(config + "\n")

    run = FakeRun([
        ("docker info", _proc("29.7.2")),
        ("local_dev.sh up", _proc("simulador sano")),
        ("docker image inspect", _proc("sha256:x")),
        ("docker ps", _proc("trendanalyzer-local-dataflow-local-1\n")),
        ("docker logs", logs),
        ("verify_trends.py", _write_report("OK")),
    ])
    ctx.run = run
    steps = v.part_local(ctx, _args(down=True))
    assert {s.id: s.status for s in steps} == {"D1": v.OK, "D2": v.OK, "D3": v.OK, "D4": v.OK, "D5": v.OK,
                                               "D6": v.OK, "D7": v.OK, "D8": v.OK}
    assert run.index("local_dataflow.sh down") < run.index("local_dev.sh down") < run.index("local_dev.sh up")
    assert run.index("local_dev.sh up") < run.index("local_nlp.sh up")
    assert "--local" in run.calls[run.index("verify_trends.py")]


def test_part_local_sin_docker(tmp_path):
    steps = v.part_local(_ctx(tmp_path, FakeRun([("docker info", _proc("", 1, "no daemon"))])), _args())
    assert steps == [steps[0]] and steps[0].status == v.FALLA


def test_part_template(tmp_path):
    def params(cmd):
        out = Path(cmd[cmd.index("--json-out") + 1])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"parameters": {"output_root": "gs://b", "trend_windows_table": "t"},
                                   "image": "img:tag"}))
        return _proc("ok")

    rules = _cloud_rules(_jobs("Running", "", "Running"))
    rules = [("dataflow_template.py", params),
             ("jobs describe", _proc('{"x": "dataflow-nlp trend_windows dlq_events"}')), *rules]
    steps = v.part_template(_ctx(tmp_path, FakeRun(rules)), _args())
    assert all(s.status == v.OK for s in steps), [(s.id, s.status, s.detail) for s in steps]
    assert [s.id for s in steps] == ["F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"]


def test_default_run_muestra_la_salida_en_vivo(tmp_path):
    lines = []
    proc = v.default_run([sys.executable, "-c", "print('uno'); import sys; print('dos', file=sys.stderr)"],
                         cwd=tmp_path, timeout=30, stream=lines.append)
    assert proc.returncode == 0 and lines == ["uno", "dos"] and proc.stdout == "uno\ndos\n"


def test_default_run_tiempo_agotado(tmp_path):
    for stream in (None, lambda _l: None):
        proc = v.default_run([sys.executable, "-c", "import time; time.sleep(30)"], cwd=tmp_path, timeout=0.5,
                             stream=stream)
        assert proc.returncode == 124 and "tiempo agotado" in proc.stderr


def test_default_run_sin_entrada_estandar_no_se_queda_esperando(tmp_path):
    """Un script que pide confirmación falla enseguida en vez de esperar en silencio (E4a)."""
    for stream in (None, lambda _l: None):
        proc = v.default_run([sys.executable, "-c", "input('¿Drenar? [s/N] ')"], cwd=tmp_path, timeout=20,
                             stream=stream)
        assert proc.returncode != 0 and proc.returncode != 124


def test_main_preflight_sin_archivos(tmp_path, capsys):
    ctx = _ctx(tmp_path, FakeRun([]))
    assert v.main(["preflight"], ctx=ctx) == 1
    evidence = json.loads((ctx.evidence_dir / "validacion_2_4.json").read_text())
    assert evidence["parts"]["preflight"]["verdict"] == v.FALLA
