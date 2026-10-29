"""Pruebas del validador del Lote 7.2 con los módulos reales de Terraform y la nube falsa (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ops import validate72 as v

NOW = datetime(2026, 10, 29, 7, 0, tzinfo=timezone.utc)


def test_the_real_gcp_module_meets_hu042():
    assert v.gcp_module_problems(v.module_text("infra/gcp/modules/observability/main.tf")) == []
    problems = v.gcp_module_problems('display_name = "ta-op-latencia"')
    assert any("tasa de mensajes" in p for p in problems) and any("DLQ" in p for p in problems)
    assert "falta la alerta ta-op-dlq-creciente" in problems and "alguna alerta sin canales de notificación" in problems


def test_the_real_aws_module_shows_glue_states():
    assert v.aws_module_problems(v.module_text("infra/aws/modules/lakehouse_observability/main.tf")) == []
    assert "falta tablero de CloudWatch" in v.aws_module_problems("Glue Job State Change")


def test_every_component_logs_json_to_stdout(tmp_path):
    assert v.json_logger_problems() == []
    for rel in v.JSON_LOGGERS:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("import logging\nlogging.info('x')\n")
    assert v.json_logger_problems(tmp_path) == list(v.JSON_LOGGERS)


def test_pure_helpers():
    assert v.log_kinds([{"jsonPayload": {"a": 1}}, {"textPayload": "x"}, {"jsonPayload": {}}]) == {"json": 2, "texto": 1}
    assert v.aggregation("p95")["cross_series_reducer"] == "REDUCE_PERCENTILE_95"
    assert v.aggregation("rate")["per_series_aligner"] == "ALIGN_RATE" and v.aggregation("max")["per_series_aligner"] == "ALIGN_MAX"
    assert v.summarize([]) == {"n": 0} and v.summarize([3.0, 7.256, 1.0]) == {"n": 3, "max": 7.26, "last": 3.0}


class Fake:
    def __init__(self, dataflow=True):
        self.dataflow, self.calls = dataflow, []

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        from ops.validate32 import Result

        self.calls.append(cmd)
        text = " ".join(cmd)
        if cmd[:2] == ["bash", "project/infra/validate.sh"]:
            return Result(0, "OK")
        if "dataflow jobs list" in text:
            return Result(0, json.dumps([{"name": "ta-stream-dev", "id": "j"}] if self.dataflow else []))
        if "run services describe" in text:
            return Result(1, "", "NOT_FOUND")
        return Result(1, "", f"inesperado: {text}")


def _run(part, tmp_path, monkeypatch, fake, monitoring=None):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    code = v.main([part, "--project", "proj-x", "--yes"], runner=fake, printer=lambda s: None, sleep=lambda s: None,
                  now=lambda: NOW, evidence_path=tmp_path / "ev.json", monitoring=monitoring)
    ev = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    return code, {s["id"]: s for p in ev["parts"].values() for s in p["steps"]}


def test_local_with_the_real_files(tmp_path, monkeypatch):
    code, steps = _run("local", tmp_path, monkeypatch, Fake())
    assert code == 0, {k: s["detail"] for k, s in steps.items() if s["status"] != "OK"}
    assert set(steps) == {"L1", "L2", "L3", "L4"}


def test_metrics_with_and_without_data(tmp_path, monkeypatch):
    seen = []

    def monitoring(project, metric, flt, kind, minutes):
        seen.append((metric, kind))
        return [] if "system_lag" in metric else [12.0, 30.5]

    code, steps = _run("metrics", tmp_path, monkeypatch, Fake(), monitoring)
    assert code == 1 and steps["M5"]["status"] == "FALLA" and steps["M3"]["status"] == "OK"
    assert ("pubsub.googleapis.com/subscription/ack_latencies", "p95") in seen and len(seen) == len(v.SERIES)
    code, steps = _run("metrics", tmp_path, monkeypatch, Fake(dataflow=False), monitoring)
    assert code == 0 and steps["M0"]["status"] == "OMITIDO"
