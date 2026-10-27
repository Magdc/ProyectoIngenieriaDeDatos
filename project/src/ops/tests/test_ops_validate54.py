"""Pruebas del validador del Lote 5.4 con gcloud, aws, Dataflow y la prueba de humo falsos (sin red)."""

from __future__ import annotations

import importlib.util
import json

import pytest

from ops import validate54 as v

PLAN = {"event": "smoke_plan", "source": "reddit", "seq": 1, "event_id": "1f6ae60d-3afe-5534-a2ef-691ddaa5224c",
        "generator": "comment-gen@1.0", "messages": 1, "attributes": ["event_id", "origin", "schema_version", "source"],
        "contract": "ok", "warmup_event_id": "65acf1eb-5f70-5919-a427-b3d99d9fbd30", "warmup_generator": "ta-verify@2.1",
        "warmup_contract": "ok"}


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err)


def report(ok=True):
    stages = [{"stage": s, "from": "x", "objective_s": o, "latency_s": o / 2, "since_publish_s": o, "ok": ok or s != "curated",
               "detail": "d"} for s, o in v.OBJECTIVES.items()]
    return {"run_id": "smoke-x", "ok": ok, "event_id": PLAN["event_id"], "total_s": 900.0, "stages": stages,
            "warmup": {"ok": True, "latency_s": 140.0, "generator": "ta-verify@2.1"},
            "identity": {"checked": list(v.OBJECTIVES), "problems": {}, "same_user_hash": True, "stages_with_user_hash": 5}}


class Fake:
    def __init__(self, *, dataflow=False, simulator=False, exporter="PAUSED", schedule="ENABLED", plan=None, smoke=None):
        self.dataflow, self.simulator, self.exporter, self.schedule = dataflow, simulator, exporter, schedule
        self.plan = PLAN if plan is None else plan
        self.smoke = smoke
        self.calls = []

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        text = " ".join(cmd)
        if cmd[1:3] == ["-m", "pytest"]:
            target = next((c for c in cmd if c.startswith(("src/", "contracts/"))), "")
            counts = {"src/smoke": v.B1_EXPECTED, "src/ops/tests/test_ops_validate54.py": v.B2_EXPECTED,
                      "contracts/tests": v.B3_EXPECTED, "src/ops/tests/test_ops_validate53.py": v.B4_EXPECTED}
            return R(0, f"{counts.get(target, 1)} passed in 1.0s")
        if "smoke_e2e.py" in text and " plan" in text:
            return R(0, "aviso\n" + json.dumps(self.plan))
        if "smoke_e2e.py" in text and " run" in text:
            if self.smoke is not None:
                v.REPORT_JSON.write_text(json.dumps(self.smoke), encoding="utf-8")
                v.REPORT_MD.write_text(f"# Prueba de humo\n`{self.smoke['event_id']}`\n", encoding="utf-8")
            return R(0 if (self.smoke or {}).get("ok") else 1, '{"event": "smoke_done"}')
        if "dataflow jobs list" in text:
            return R(0, json.dumps([{"name": "ta-stream-dev-1", "id": "2026-10-27_j", "state": "JOB_STATE_RUNNING"}]
                                   if self.dataflow else []))
        if "dataflow jobs drain" in text:
            self.dataflow = False
            return R(0, "")
        if "dataflow_job.sh start" in text:
            assert env and env.get("TA_NLP_IMAGE", "").endswith(":abc123")
            self.dataflow = True
            return R(0, "lanzado")
        if "run services describe" in text:
            return R(0, "https://sim" if self.simulator else "")
        if "artifacts docker images list" in text:
            return R(0, json.dumps([{"tags": ["abc123", "latest"]}]))
        if "scheduler jobs describe" in text:
            return R(0, self.exporter + "\n")
        if "scheduler jobs pause" in text:
            self.exporter = "PAUSED"
            return R(0, "")
        if "subscriptions seek" in text or "subscriptions list" in text or "builds list" in text:
            return R(0, "")
        if cmd[:3] == ["aws", "scheduler", "get-schedule"]:
            return R(0, json.dumps({"State": self.schedule}))
        return R(0, "")

    def count(self, text):
        return sum(1 for c in self.calls if text in " ".join(c))


KEYS = {"preflight": "A", "tests": "B", "local": "L", "prepare": "P", "smoke": "E", "off": "O", "status": "status"}


@pytest.fixture(autouse=True)
def files(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "REPORT_JSON", tmp_path / "smoke_e2e_5_4.json")
    monkeypatch.setattr(v, "REPORT_MD", tmp_path / "smoke_e2e_5_4.md")
    adc = tmp_path / "adc.json"
    adc.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(v, "ADC", adc)


def run(part, fake, tmp_path, *extra, confirm=None):
    lines = []
    code = v.main([part, *extra], runner=fake, printer=lines.append, evidence_path=tmp_path / "ev.json",
                  confirm=confirm, sleep=lambda s: None)
    data = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    assert data["lote"] == "5.4" and data["hu"] == ["HU-048"]
    return code, {s["id"]: s for s in data["parts"][KEYS[part]]["steps"]}


def test_preflight_ok(tmp_path):
    code, steps = run("preflight", Fake(), tmp_path)
    assert code == 0 and all(s["status"] == v.OK for s in steps.values()), steps
    assert steps["A5"]["data"]["nlp_image"].endswith("dataflow-nlp:abc123")


def test_preflight_avisa_simulador_y_estados(tmp_path):
    code, steps = run("preflight", Fake(simulator=True, exporter="ENABLED", schedule="DISABLED"), tmp_path)
    assert code == 0 and [steps[k]["status"] for k in ("A5", "A6", "A7")] == [v.AVISO] * 3


def test_tests(tmp_path):
    code, steps = run("tests", Fake(), tmp_path)
    assert code == 0 and [s["status"] for s in steps.values()] == [v.OK] * 4


def test_local_ok_y_contrato_roto(tmp_path):
    code, steps = run("local", Fake(), tmp_path)
    assert code == 0 and steps["L1"]["status"] == v.OK and steps["L2"]["status"] == v.OK
    code, steps = run("local", Fake(plan=dict(PLAN, contract="payload: falta kind")), tmp_path)
    assert code == 1 and steps["L1"]["status"] == v.FALLA


def test_local_calentamiento_que_trusted_no_descarta(tmp_path):
    code, steps = run("local", Fake(plan=dict(PLAN, warmup_generator="comment-gen@1.0")), tmp_path)
    assert code == 1 and steps["L1"]["status"] == v.FALLA


def test_prepare_seek_y_dataflow(tmp_path):
    fake = Fake()
    code, steps = run("prepare", fake, tmp_path, "--yes")
    assert code == 0 and [steps[k]["status"] for k in ("P1", "P2", "P3")] == [v.OK] * 3
    assert fake.count("subscriptions seek") == 3 and fake.count("dataflow_job.sh start") == 1
    assert all(f"ta-raw-{s}-df" in " ".join(c) for s, c in zip(("reddit", "facebook", "instagram"),
                                                               [c for c in fake.calls if "seek" in c]))


def test_prepare_sin_confirmar_no_toca_nada(tmp_path):
    fake = Fake()
    code, steps = run("prepare", fake, tmp_path, confirm=lambda q: False)
    assert code == 0 and steps["P1"]["status"] == v.AVISO and "P2" not in steps
    assert fake.count("seek") == 0 and fake.count("dataflow_job.sh") == 0


def test_prepare_con_dataflow_encendido_omite_el_seek(tmp_path):
    fake = Fake(dataflow=True)
    code, steps = run("prepare", fake, tmp_path, "--yes")
    assert code == 0 and steps["P1"]["status"] == v.OMITIDO and steps["P3"]["status"] == v.OK
    assert fake.count("seek") == 0 and fake.count("dataflow_job.sh") == 0


def test_smoke_ok(tmp_path):
    code, steps = run("smoke", Fake(smoke=report()), tmp_path, "--yes")
    assert code == 0 and all(steps[f"E{i}"]["status"] == v.OK for i in range(1, 9)), steps
    assert steps["E8"]["data"]["event_id"] == PLAN["event_id"]


def test_smoke_con_una_etapa_fuera_de_objetivo(tmp_path):
    code, steps = run("smoke", Fake(smoke=report(ok=False)), tmp_path, "--yes")
    assert code == 1 and steps["E6"]["status"] == v.FALLA and steps["E2"]["status"] == v.OK


def test_smoke_sin_informe_y_sin_confirmar(tmp_path):
    code, steps = run("smoke", Fake(smoke=None), tmp_path, "--yes")
    assert code == 1 and steps["E0"]["status"] == v.FALLA
    fake = Fake(smoke=report())
    code, steps = run("smoke", fake, tmp_path, confirm=lambda q: False)
    assert code == 0 and steps["E0"]["status"] == v.AVISO and fake.count("smoke_e2e.py") == 0


def test_off_drena_y_pausa_el_exportador(tmp_path):
    fake = Fake(dataflow=True, exporter="ENABLED")
    code, steps = run("off", fake, tmp_path, "--yes")
    assert code == 0 and all(s["status"] == v.OK for s in steps.values()), steps
    assert fake.count("dataflow jobs drain") == 1 and fake.count("scheduler jobs pause") == 1


def test_status(tmp_path):
    v.REPORT_JSON.write_text(json.dumps(report()), encoding="utf-8")
    code, steps = run("status", Fake(), tmp_path)
    assert code == 0 and [s["status"] for s in steps.values()] == [v.OK] * 3
    code, steps = run("status", Fake(dataflow=True), tmp_path)
    assert steps["S1"]["status"] == v.AVISO


def test_lineas_json_y_script():
    assert v.smoke_events('ruido\n{"event": "smoke_done", "ok": true}\n{roto\n') == [{"event": "smoke_done", "ok": True}]
    spec = importlib.util.spec_from_file_location("validate_5_4", v.PROJECT / "scripts" / "validate_5_4.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main is v.main
