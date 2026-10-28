"""Comprobaciones y CLI de las alertas (Lote 6.2): alerta sintética, ola de prueba, notificaciones y cooldown.

Usan el simulador y el contrato del repositorio, así que no van en la imagen del notificador.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from alerts import check, cli
from common import alert_message as am

NOW = datetime(2026, 10, 27, 21, 30, 15, tzinfo=timezone.utc)
NOW_MS = int(NOW.timestamp() * 1000)


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


class FakePublisher:
    def __init__(self):
        self.sent = []

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data, **attrs):
        self.sent.append((topic, data, attrs))
        return _Future(str(len(self.sent)))


def _events(out: str) -> list[dict]:
    return [json.loads(ln) for ln in out.splitlines() if ln.startswith("{")]


def test_alerta_sintetica():
    alert = check.synthetic_alert(now_ms=NOW_MS)
    assert am.alert_problems(alert) == []
    assert alert["origin"] == "check" and alert["alert_type"] == "volume_up"
    assert alert["window_end"] == "2026-10-27T21:30:00.000Z" and alert["window_start"] == "2026-10-27T21:25:00.000Z"
    assert am.alert_text(alert).startswith("[prueba] Volumen al alza de postobon en instagram")


def test_ola_de_prueba_cumple_el_contrato_y_trusted_la_descarta(envelope_validator):
    from lakehouse.glue_jobs.raw_to_trusted import SENTINEL_PREFIX

    envs = check.burst_envelopes("r1", "postobon", "instagram", 40, NOW)
    assert len(envs) == 40 and check.burst_problems(envs, "postobon", "instagram") == []
    assert all(envelope_validator(e) == [] for e in envs)
    assert all(e["generator"].startswith(SENTINEL_PREFIX) for e in envs)
    assert {e["sim"]["scenario"] for e in envs} == {"alert-check-6.2/r1"}
    assert {e["sim"]["label_sentiment"] for e in envs} == {"negative"}
    # Ids distintos de una corrida a otra (seq = segundos UNIX × 1000 + i).
    later = check.burst_envelopes("r2", "postobon", "instagram", 3, NOW.replace(second=16))
    assert not {e["event_id"] for e in later} & {e["event_id"] for e in envs}
    assert check.burst_problems(envs[:2] + envs[:1], "pepsi", "instagram")


def test_notificaciones_del_canal_de_pubsub():
    alert_id = "6b1d3f1e-0000-5000-8000-000000000001"
    raw = json.dumps({"version": "1.2", "incident": {
        "incident_id": "0.abc", "policy_name": "ta-alerta-tendencia", "state": "open", "started_at": NOW_MS // 1000,
        "summary": "Log match", "documentation": {"content": f"- Alerta: {alert_id}", "subject": "TrendAnalyzer: volume_up"}}})
    inc = check.parse_incident(raw.encode())
    assert inc["policy"] == "ta-alerta-tendencia" and inc["started_at"] == "2026-10-27T21:30:15.000Z"
    assert check.incident_for([{"content": "otra"}, inc], alert_id) is inc
    assert check.incident_for([inc], "otro-id") is None
    assert check.parse_incident(b"{}") is None and check.parse_incident(b"\xff") is None
    assert check.seconds_between("2026-10-27T21:30:00.000Z", "2026-10-27T21:31:30.500Z") == 90.5


def test_cooldown_sobre_los_mensajes_vistos():
    base = am.to_ms("2026-10-27T21:00:00.000Z")

    def a(minutes, kind="volume_up", brand="postobon", origin="dataflow"):
        z = 5.0 if kind.endswith("up") else -5.0
        return am.build_alert(metric=kind.split("_")[0], brand=brand, source="instagram",
                              window_start_ms=base + minutes * 60_000 - 300_000, window_end_ms=base + minutes * 60_000,
                              value=50 if kind.startswith("volume") else -0.5, baseline_mean=20.0, baseline_std=3.0,
                              zscore=z, z_threshold=3.0, baseline_windows=60, n_posts=50, avg_sentiment=-0.5,
                              n_negative=30, n_positive=5, detected_ms=base + minutes * 60_000 + 50_000,
                              cooldown_seconds=900, origin=origin)

    ok = [a(0), a(15), a(1, "sentiment_down"), a(2, brand="pepsi"), a(0), a(3, origin="check")]
    assert check.cooldown_violations(ok) == []
    bad = check.cooldown_violations([a(0), a(14)])
    assert len(bad) == 1 and "840 s" in bad[0]


def test_cli_plan(capsys):
    assert cli.main(["plan", "--count", "20"]) == 0
    plan = _events(capsys.readouterr().out)[-1]
    assert plan["event"] == "alerts_plan" and plan["sample_problems"] == []
    assert plan["burst"]["count"] == 20 and plan["burst"]["contract_problems"] == [] and plan["burst"]["problems"] == []
    assert plan["burst"]["generator"] == "ta-verify@6.2" and plan["cooldown_seconds"] == 900
    assert plan["log_severity"] == "WARNING" and plan["log_alert"] == "trend"
    assert set(plan["aviso"]) == {"marca", "fuente", "tipo", "ventana", "valor", "media", "cambio", "z", "origen", "alerta_id"}


def test_cli_publica_la_alerta_de_prueba_y_la_ola(capsys, monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert cli.main(["burst"]) == 2  # sin proyecto no publica nada
    pub = FakePublisher()
    args = cli.build_parser().parse_args(["publish-test", "--project", "proj-x"])
    assert cli.cmd_publish_test(args, publisher_factory=lambda: pub) == 0
    topic, data, attrs = pub.sent[0]
    assert topic == "projects/proj-x/topics/ta-alerts" and am.decode(data)["origin"] == "check"
    assert attrs["alert_type"] == "volume_up"
    args = cli.build_parser().parse_args(["burst", "--project", "proj-x", "--count", "12", "--run-id", "abc"])
    assert cli.cmd_burst(args, publisher_factory=lambda: pub) == 0
    burst = pub.sent[1:]
    assert len(burst) == 12 and {t for t, _, _ in burst} == {"projects/proj-x/topics/ta-raw-instagram"}
    assert all(set(a) == {"event_id", "source", "origin", "schema_version"} for _, _, a in burst)
    out = _events(capsys.readouterr().out)
    assert [e["event"] for e in out] == ["alert_test_published", "burst_published"]
    assert out[1]["count"] == 12 and out[1]["scenario"] == "alert-check-6.2/abc"
