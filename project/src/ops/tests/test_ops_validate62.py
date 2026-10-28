"""Pruebas del validador del Lote 6.2 con gcloud, bq, terraform y el CLI de alertas falsos (sin red)."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from alerts import check
from common import alert_message as am
from ops import validate62 as v

NOW = datetime(2026, 10, 27, 23, 0, tzinfo=timezone.utc)
PROJ = "proj-x"
URL = "https://ta-alert-notifier-abc-uc.a.run.app"
EMAIL = "persona@correo.test"


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err)


def _alert(end, kind="volume_up", brand="postobon", source="instagram", origin="dataflow"):
    metric = kind.split("_")[0]
    z = 12.0 if kind.endswith("up") else -9.0
    end_ms = am.to_ms(end)
    return am.build_alert(metric=metric, brand=brand, source=source, window_start_ms=end_ms - 300_000, window_end_ms=end_ms,
                          value=190 if metric == "volume" else -0.6, baseline_mean=47.0 if metric == "volume" else 0.1,
                          baseline_std=7.0, zscore=z, z_threshold=3.0, baseline_windows=60, n_posts=190, avg_sentiment=-0.6,
                          n_negative=150, n_positive=10, detected_ms=end_ms + 50_000, cooldown_seconds=900, origin=origin)


def _pull_item(data: bytes, publish_time: str):
    return {"ackId": "a", "message": {"data": base64.b64encode(data).decode(), "attributes": {}, "messageId": "1",
                                      "publishTime": publish_time}}


def _incident(alert_id: str, started: datetime) -> bytes:
    return json.dumps({"version": "1.2", "incident": {
        "incident_id": "0.inc", "policy_name": v.POLICY, "state": "open", "started_at": int(started.timestamp()),
        "summary": "coincidencia de log", "documentation": {
            "subject": "TrendAnalyzer: volume_up de postobon en instagram",
            "content": f"- Marca: postobon\n- Fuente: instagram\n- Tipo: volume_up\n- Alerta: {alert_id}"}}}).encode()


class Fake:
    def __init__(self, *, budget=True, plan_creates=None, plan_deletes=(), image_exists=True, incident=True,
                 warm=True, messages=True):
        self.budget, self.plan_creates, self.plan_deletes = budget, plan_creates, list(plan_deletes)
        self.image_exists, self.incident, self.warm, self.messages = image_exists, incident, warm, messages
        self.calls: list[list[str]] = []
        self.applied = False
        self.created: list[str] = []
        self.deleted: list[str] = []
        self.published: dict = {}
        self.pulls: dict[str, int] = {}
        self.vol_alert = _alert("2026-10-27T23:01:00.000Z")

    def which(self, name):
        return f"/usr/bin/{name}"

    # --- comandos -------------------------------------------------------------------------------------
    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        self.calls.append(list(cmd))
        text = " ".join(cmd)
        if cmd[0] == "terraform":
            return self.terraform(cmd, text)
        if cmd[0] == "bq":
            return self.bq(cmd[-1])
        if cmd[:2] == ["bash", "infra/validate.sh"]:
            return R(0, "OK")
        if cmd[:2] == ["bash", "scripts/build_alerts_image.sh"]:
            img = f"us-central1-docker.pkg.dev/{PROJ}/ta-images/alert-notifier:abc1234-0f0f0f0f"
            return R(0, f"imagen: {img}\ncontexto: 8 archivos\n11 passed\nalerts_notifier_image = \"{img}\"\n")
        if len(cmd) > 1 and cmd[1] == "scripts/alerts_check.py":
            return self.cli(cmd)
        if len(cmd) > 2 and cmd[1:3] == ["-m", "pytest"]:
            target = cmd[3]
            n = {"src/alerts": v.B1_EXPECTED, "src/streaming/tests/test_alerts_pipeline.py": v.B2_EXPECTED,
                 "src/streaming/tests": v.B3_EXPECTED, "src/ops/tests/test_ops_validate62.py": v.B4_EXPECTED}[target]
            return R(0, f"{n} passed in 1.0s")
        if cmd[0] == "gcloud":
            return self.gcloud(cmd, text)
        if "import apache_beam" in text:
            return R(0)
        raise AssertionError(f"comando inesperado: {cmd}")

    def terraform(self, cmd, text):
        if "plan" in cmd:
            return R(0, "Plan: …")
        if "show" in cmd:
            creates = [] if self.applied else sorted(self.plan_creates if self.plan_creates is not None else v.expected_creates(1, True))
            changes = [{"address": a, "mode": "managed", "change": {"actions": ["create"]}} for a in creates]
            changes += [{"address": a, "mode": "managed", "change": {"actions": ["delete"]}} for a in self.plan_deletes]
            return R(0, json.dumps({"resource_changes": changes}))
        if "apply" in cmd:
            self.applied = True
            return R(0, f"Apply complete! Resources: {len(v.expected_creates(1, True))} added, 0 changed, 0 destroyed.")
        if "output" in cmd:
            return R(0, json.dumps({k: {"value": val} for k, val in {
                "alerts_notifier_url": URL, "alerts_incidents_topic": "ta-alerts-incidents", "alert_policy_id": "projects/p/alertPolicies/1",
                "alert_policy_name": v.POLICY, "alert_email_channels": 1, "alerts_notifier_image": "img:1"}.items()}))
        return R(0)

    def bq(self, sql):
        if "MIN(window_start)" in sql:
            first = "2026-10-27T21:40:00.000Z" if self.warm else "2026-10-27T22:50:00.000Z"
            return R(0, json.dumps([{"first": first, "n": "80"}]))
        rows = []
        for i, end in enumerate(("23:01", "23:02", "23:03", "23:04", "23:05", "23:06")):
            alert = i < 4
            rows.append({"window_start": f"2026-10-27T{end}:00.000Z", "window_end": f"2026-10-27T{end}:00.000Z", "n_posts": "190",
                         "zscore_volume": "12.5" if alert else "1.1", "zscore_sentiment": "-9.0" if i < 2 else "-1.0",
                         "is_alert": "true" if alert else "false"})
        return R(0, json.dumps(rows))

    def cli(self, cmd):
        op = cmd[2]
        if op == "plan":
            sample = check.synthetic_alert(now_ms=am.to_ms("2026-10-27T23:00:00.000Z"))
            rec = am.log_record(sample, received_ms=am.to_ms(sample["detected_at"]) + 1000)
            return R(0, json.dumps({"event": "alerts_plan", "sample": sample, "sample_problems": [], "bytes": 700,
                                    "attributes": am.attributes_for(sample), "text": am.alert_text(sample), "aviso": rec["aviso"],
                                    "burst": {"count": 150, "brand": "postobon", "source": "instagram", "generator": check.BURST_GENERATOR,
                                              "scenario": "alert-check-6.2/plan", "contract_problems": [], "problems": []}}))
        if op == "publish-test":
            self.published = {"event": "alert_test_published", "alert_id": "11111111-1111-5111-8111-111111111111",
                              "message_id": "9", "alert_type": "volume_up", "brand": "postobon", "source": "instagram",
                              "window_end": "2026-10-27T23:00:00.000Z", "published_at": "2026-10-27T23:00:05.000Z",
                              "text": "[prueba] Volumen al alza de postobon en instagram: …"}
            return R(0, json.dumps(self.published))
        if op == "burst":
            return R(0, json.dumps({"event": "burst_published", "run_id": "r", "count": 150, "brand": "postobon",
                                    "source": "instagram", "generator": check.BURST_GENERATOR, "scenario": "alert-check-6.2/r",
                                    "first_event_time": "2026-10-27T22:59:57.000Z", "last_event_time": "2026-10-27T22:59:59.000Z",
                                    "published_from": "2026-10-27T23:00:00.000Z", "published_to": "2026-10-27T23:00:03.000Z"}))
        raise AssertionError(cmd)

    def gcloud(self, cmd, text):
        if "dataflow jobs list" in text:
            return R(0, json.dumps([{"name": "ta-stream-dev", "id": "j1", "state": "Running"}]))
        if "run services describe ta-stream-simulator" in text:
            return R(0, "https://sim.run.app\n")
        if "pubsub topics describe" in text:
            return R(0, json.dumps({"name": "projects/p/topics/ta-alerts"}))
        if "monitoring channels list" in text:
            chans = [{"type": "email", "displayName": f"ta-budget-{EMAIL}", "labels": {"email_address": EMAIL}}] if self.budget else []
            return R(0, json.dumps(chans))
        if "services identity create" in text:
            return R(0, "", "Service identity created: service-123@gcp-sa-monitoring-notification.iam.gserviceaccount.com")
        if "artifacts docker images describe" in text:
            return R(0, "sha256:abc\n" if self.image_exists else "")
        if f"run services describe {v.SERVICE}" in text:
            return R(0, json.dumps({"status": {"conditions": [{"type": "Ready", "status": "True"}]}}))
        if "run services get-iam-policy" in text:
            return R(0, json.dumps({"bindings": [{"role": "roles/run.invoker", "members": [f"serviceAccount:sa-alerts@{PROJ}.iam.gserviceaccount.com"]}]}))
        if f"subscriptions describe {v.PUSH_SUBSCRIPTION}" in text:
            return R(0, json.dumps({"pushConfig": {"pushEndpoint": URL, "oidcToken": {"serviceAccountEmail": f"sa-alerts@{PROJ}.iam.gserviceaccount.com"}}}))
        if "topics get-iam-policy" in text:
            return R(0, json.dumps({"bindings": [{"role": "roles/pubsub.publisher", "members": [f"serviceAccount:sa-dataflow@{PROJ}.iam.gserviceaccount.com"]}]}))
        if "monitoring policies list" in text:
            return R(0, json.dumps([{"enabled": True, "notificationChannels": ["a", "b"],
                                     "alertStrategy": {"notificationRateLimit": {"period": "300s"}}}]))
        if "subscriptions create" in text:
            self.created.append(cmd[4])
            return R(0)
        if "subscriptions delete" in text:
            self.deleted.append(cmd[4])
            return R(0)
        if "subscriptions seek" in text:
            return R(0)
        if "subscriptions pull" in text:
            return self.pull(cmd[4])
        if "logging read" in text:
            return self.logs(cmd[3])
        raise AssertionError(f"gcloud inesperado: {cmd}")

    def pull(self, sub):
        n = self.pulls[sub] = self.pulls.get(sub, 0) + 1
        if n > 1:
            return R(0, "[]")
        if sub.startswith("ta-verify-alerts-"):
            if not self.messages:
                return R(0, "[]")
            items = [self.vol_alert, _alert("2026-10-27T23:01:00.000Z", "sentiment_down"),
                     _alert("2026-10-27T23:03:00.000Z", brand="pepsi", source="reddit")]
            return R(0, json.dumps([_pull_item(am.encode(a)[0], "2026-10-27T23:01:51.123456Z") for a in items]))
        if not self.incident:
            return R(0, "[]")
        alert_id = self.published.get("alert_id") or self.vol_alert["alert_id"]
        return R(0, json.dumps([_pull_item(_incident(alert_id, NOW), "2026-10-27T23:02:40.000Z")]))

    def logs(self, flt):
        if "11111111-" in flt:
            # Como en Cloud Logging: severity va en la entrada, no en jsonPayload.
            return R(0, json.dumps([{"timestamp": "2026-10-27T23:00:09.5Z", "severity": "WARNING", "jsonPayload": {
                "alert": "trend", "message": self.published["text"], "received_at": "2026-10-27T23:00:09.000Z",
                "aviso": {"alerta_id": self.published["alert_id"]}}}]))
        rec = am.log_record(self.vol_alert, received_ms=am.to_ms("2026-10-27T23:01:53.000Z"))
        rec.pop("severity")
        return R(0, json.dumps([{"timestamp": "2026-10-27T23:01:53.1Z", "severity": "WARNING", "jsonPayload": rec}]))


def _main(fake, *argv, tmp_path, evidence=None):
    path = tmp_path / "ev.json"
    if evidence is not None:
        path.write_text(json.dumps(evidence), encoding="utf-8")
    lines: list[str] = []
    code = v.main([*argv, "--project", PROJ, "--yes"], runner=fake, printer=lines.append, sleep=lambda s: None,
                  now=lambda: NOW, evidence_path=path)
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    return code, "\n".join(lines), data


def _steps(data, part):
    return {s["id"]: s for s in data["parts"][part]["steps"]}


@pytest.fixture(autouse=True)
def _linux(monkeypatch, tmp_path):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    monkeypatch.setattr(v, "TFVARS", tmp_path / "terraform.tfvars")
    monkeypatch.setattr(v, "ALERTS_AUTO", tmp_path / "alerts.auto.tfvars")
    monkeypatch.setattr(v, "EVIDENCE_DIR", tmp_path)
    (tmp_path / "terraform.tfvars").write_text('project_id = "proj-x"\nsimulator_running = false\n', encoding="utf-8")


def test_utilidades_de_tfvars_y_canales():
    text = 'project_id = "p"\n'
    out = v.set_alert_emails(text, [EMAIL])
    assert v.read_alert_emails(out) == [EMAIL] and out.startswith(text)
    assert v.read_alert_emails(v.set_alert_emails(out, ["b@c.test"])) == ["b@c.test"]
    chans = [{"type": "email", "displayName": "ta-budget-x", "labels": {"email_address": EMAIL}},
             {"type": "email", "displayName": "otro", "labels": {"email_address": "no@c.test"}}, {"type": "pubsub"}]
    assert v.budget_emails(chans) == [EMAIL] and v.budget_emails(None) == []


def test_recursos_esperados_del_plan():
    full = v.expected_creates(1, True)
    assert len(full) == 10 and 'module.alerts.google_monitoring_notification_channel.email["0"]' in full
    assert len(v.expected_creates(2, False)) == 8
    assert not any(EMAIL in a for a in full)


def test_lectura_de_pubsub_logs_y_ventanas():
    items = v.pulled(json.dumps([_pull_item(b'{"a":1}', "2026-10-27T23:01:51.123456Z")]))
    assert items[0]["data"] == b'{"a":1}' and v.iso_ms(items[0]["publish_time"]) == "2026-10-27T23:01:51.123Z"
    assert v.iso_ms("2026-10-27T23:01:51Z") == "2026-10-27T23:01:51.000Z" and v.iso_ms("x") is None
    assert v.payloads([{"timestamp": "2026-10-27T23:00:09.5Z", "severity": "WARNING", "jsonPayload": {"a": 1}},
                       {"textPayload": "x"}]) == [{"a": 1, "_timestamp": "2026-10-27T23:00:09.500Z", "_severity": "WARNING"}]
    rows = [{"is_alert": "true", "zscore_volume": "5", "zscore_sentiment": "-1"},
            {"is_alert": "true", "zscore_volume": "1", "zscore_sentiment": "-4"}, {"is_alert": "false", "zscore_volume": "9"}]
    assert len(v.metric_windows(rows, "volume")) == 1 and len(v.metric_windows(rows, "sentiment")) == 1
    assert v.warm_at("2026-10-27T21:40:00.000Z") == datetime(2026, 10, 27, 22, 46, tzinfo=timezone.utc)
    alert = _alert("2026-10-27T23:01:00.000Z")
    assert v.rendered_problems(f"postobon instagram volume_up {alert['alert_id']}", alert) == []
    assert v.rendered_problems("postobon ${log.extracted_label.marca}", alert)


def test_partes_preflight_tests_y_local(tmp_path):
    fake = Fake()
    code, out, data = _main(fake, "tests", tmp_path=tmp_path)
    assert code == 0 and data["parts"]["B"]["status"] == "OK"
    code, out, data = _main(fake, "local", tmp_path=tmp_path)
    steps = _steps(data, "L")
    assert code == 0 and all(steps[k]["status"] == "OK" for k in ("L1", "L2", "L3", "L4")), out
    code, out, data = _main(fake, "preflight", tmp_path=tmp_path)
    steps = _steps(data, "A")
    assert steps["A5"]["status"] == "OK" and steps["A6"]["status"] == "AVISO"


def test_imagen_existente_no_se_reconstruye(tmp_path):
    fake = Fake()
    code, out, data = _main(fake, "image", tmp_path=tmp_path)
    assert code == 0 and _steps(data, "D")["D1"]["status"] == "OK"
    assert not any(c[:3] == ["bash", "scripts/build_alerts_image.sh", "cloudbuild"] for c in fake.calls)
    assert 'alerts_notifier_image = "us-central1-docker.pkg.dev/proj-x/ta-images/alert-notifier:abc1234-0f0f0f0f"' in \
        v.ALERTS_AUTO.read_text(encoding="utf-8")
    fake2 = Fake(image_exists=False)
    code, out, data = _main(fake2, "image", tmp_path=tmp_path)
    assert code == 0 and any(c[:3] == ["bash", "scripts/build_alerts_image.sh", "cloudbuild"] for c in fake2.calls)


def test_terraform_completa_correos_aplica_y_revisa(tmp_path):
    v.ALERTS_AUTO.write_text('alerts_notifier_image = "img:1"\n', encoding="utf-8")
    fake = Fake()
    code, out, data = _main(fake, "terraform", "--apply", tmp_path=tmp_path)
    steps = _steps(data, "C")
    assert code == 0, out
    assert [steps[k]["status"] for k in ("C0", "C0c", "C1", "C2", "C3", "C4", "C5")] == ["OK"] * 7
    assert v.read_alert_emails(v.TFVARS.read_text(encoding="utf-8")) == [EMAIL]
    assert EMAIL not in json.dumps(data) and EMAIL not in out


def test_terraform_no_aplica_un_plan_que_destruye(tmp_path):
    fake = Fake(plan_deletes=["google_pubsub_topic.algo"])
    code, out, data = _main(fake, "terraform", "--apply", tmp_path=tmp_path)
    assert code == 1 and _steps(data, "C")["C2"]["status"] == "FALLA" and not fake.applied
    v.TFVARS.write_text('project_id = "proj-x"\n', encoding="utf-8")
    fake2 = Fake(budget=False)
    code, out, data = _main(fake2, "terraform", tmp_path=tmp_path)
    assert code == 1 and _steps(data, "C")["C0"]["status"] == "FALLA"


def test_alerta_sintetica_de_punta_a_punta(tmp_path):
    fake = Fake()
    code, out, data = _main(fake, "notify", tmp_path=tmp_path)
    steps = _steps(data, "E")
    assert code == 0, out
    assert [steps[k]["status"] for k in ("E1", "E2", "E3", "E4", "E5")] == ["OK"] * 5 and steps["E6"]["status"] == "AVISO"
    assert steps["E3"]["data"]["publish_to_log_s"] == 4.0
    assert fake.created == fake.deleted and fake.created[0].startswith("ta-verify-incidents-")


def test_alerta_sintetica_sin_notificacion(tmp_path):
    fake = Fake(incident=False)
    code, out, data = _main(fake, "notify", tmp_path=tmp_path)
    assert code == 1 and _steps(data, "E")["E4"]["status"] == "FALLA" and fake.created == fake.deleted


def _prepared():
    return {"lote": "6.2", "parts": {"P": {"steps": [{"id": "P2", "data": {"started_at": "2026-10-27T21:45:00+00:00"}}]}}}


def test_flujo_real_con_ola_deteccion_y_cooldown(tmp_path):
    fake = Fake()
    code, out, data = _main(fake, "flow", "--watch-minutes", "1", tmp_path=tmp_path, evidence=_prepared())
    steps = _steps(data, "F")
    assert code == 0, out
    assert [steps[k]["status"] for k in ("F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9")] == ["OK"] * 10, out
    f7 = steps["F7"]["data"]
    assert f7["notifier_s"] == 53.0 and f7["notification_s"] == 100.0 and f7["detected_s"] == 50.0
    assert steps["F8"]["data"]["per_type"] == {"volume_up": 1, "sentiment_down": 1}
    assert steps["F5"]["data"]["others"] == 1
    assert sorted(fake.created) == sorted(fake.deleted) and len(fake.created) == 2


def test_flujo_sin_baseline_o_sin_mensajes(tmp_path):
    code, out, data = _main(Fake(warm=False), "flow", "--max-wait-minutes", "0", tmp_path=tmp_path, evidence=_prepared())
    assert code == 0 and _steps(data, "F")["F1"]["status"] == "AVISO"
    fake = Fake(messages=False)
    code, out, data = _main(fake, "flow", "--watch-minutes", "1", tmp_path=tmp_path, evidence=_prepared())
    assert code == 1 and _steps(data, "F")["F5"]["status"] == "FALLA" and sorted(fake.created) == sorted(fake.deleted)
    code, out, data = _main(Fake(), "flow", tmp_path=tmp_path, evidence={"lote": "6.2", "parts": {}})
    assert code == 1 and _steps(data, "F")["F0"]["status"] == "FALLA"


def test_correo_y_apagado(tmp_path, monkeypatch):
    code, out, data = _main(Fake(), "email", "--note", f"llegó a {EMAIL}", tmp_path=tmp_path)
    assert code == 1 and EMAIL not in json.dumps(data)
    code, out, data = _main(Fake(), "email", "--note", "asunto TrendAnalyzer: volume_up, 23:02", tmp_path=tmp_path)
    assert code == 0 and _steps(data, "M")["M1"]["status"] == "OK"
    state = {"on": True}
    monkeypatch.setattr(v.sv, "cloud_off", lambda run, s, say: state.update(on=False))
    monkeypatch.setattr(v.sv, "active_jobs", lambda run, s: [{"name": "ta-stream-dev"}] if state["on"] else [])
    monkeypatch.setattr(v.sv, "simulator_service", lambda run, s: "https://sim" if state["on"] else None)
    monkeypatch.setattr(v.sv, "verify_subscriptions", lambda run, s: [])
    code, out, data = _main(Fake(), "off", tmp_path=tmp_path, evidence=_prepared())
    assert code == 0 and _steps(data, "O")["O1"]["status"] == "OK" and "USD" in _steps(data, "O")["O1"]["detail"]
