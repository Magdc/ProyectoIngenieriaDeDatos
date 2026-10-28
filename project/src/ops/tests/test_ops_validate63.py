"""Pruebas del validador del Lote 6.3 con gcloud, bq, git y ``demo_scenario.py`` falsos (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from common import alert_message as am
from ops import validate62 as v62
from ops import validate63 as v

NOW = datetime(2026, 10, 28, 15, 0, tzinfo=timezone.utc)
PROJ = "proj-x"
SIM_URL = "https://ta-stream-simulator-abc-uc.a.run.app"
REPO_IMG = f"us-central1-docker.pkg.dev/{PROJ}/ta-images/stream-simulator"
NAME = "demo/postobon-negative/20261028T150205Z"


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err, 12.0)


def _iso(dt: datetime) -> str:
    return am.format_ts(int(dt.timestamp() * 1000))


def _env(i: int, at: datetime, marked: bool, brand: str = "postobon") -> dict:
    return {"event_id": f"e{i}", "ingest_time": _iso(at), "generator": "comment-gen@1.0",
            "brands": [brand if marked else ("pepsi" if i % 2 else "postobon")],
            "sim": {"label_sentiment": "negative" if marked else None, "scenario": NAME if marked else None, "chaos": []}}


def wave(start: datetime, before_s=120, during_s=300, after_s=180, m=3, wrong=False, late=False) -> list[dict]:
    """Eventos como los del simulador: 1 cada 2 s, ×m durante el escenario con 1 − 1/m marcados."""
    out, i, t = [], 0, start - timedelta(seconds=before_s)
    while t < start + timedelta(seconds=during_s + after_s):
        during = start <= t < start + timedelta(seconds=during_s)
        marked = during and i % m != 0
        out.append(_env(i, t, marked, "pepsi" if wrong and marked else "postobon"))
        i += 1
        t += timedelta(seconds=2 / m if during else 2)
    if late:
        out.append(_env(i, start + timedelta(seconds=during_s + 30), True))
    return out


SCENARIO = {"name": NAME, "brand": "postobon", "sentiment": "negative", "multiplier": 3.0, "duration_s": 300,
            "source": "instagram", "share": 0.6667, "started_at": "2026-10-28T15:02:05.000Z", "ends_at": "2026-10-28T15:07:05.000Z"}
START = am.parse_ts(SCENARIO["started_at"])


def _stats(envs):
    return v.wave_stats(envs, SCENARIO, START - timedelta(seconds=115), START + timedelta(seconds=480))


# --- utilidades puras ------------------------------------------------------------------------------------


def test_tfvars_image_and_tag():
    text = 'project_id = "p"\nsimulator_image   = "repo/stream-simulator:20261020-213231"\nsimulator_running = false\n'
    assert v.read_image(text) == "repo/stream-simulator:20261020-213231"
    new = v.set_image(text, "repo/stream-simulator:abc-6-3")
    assert 'simulator_image   = "repo/stream-simulator:abc-6-3"' in new and new.count("simulator_image") == 1
    assert v.set_image('project_id = "p"', "x:y").endswith('simulator_image = "x:y"\n')


def test_image_tag_is_a_content_fingerprint(tmp_path):
    for rel in v.IMAGE_INPUTS:
        p = tmp_path / rel / "a.py" if "." not in rel.rsplit("/", 1)[-1] else tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x = 1\n")
    tag = v.image_tag(tmp_path)
    assert tag.startswith("h-") and len(tag) == 14
    cache = tmp_path / "src" / "simulator" / "__pycache__" / "a.cpython-311.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"\0")
    (tmp_path / "src" / "common" / "a.py").write_bytes(b"x = 1\r\n")
    assert v.image_tag(tmp_path) == tag  # cachés fuera y CRLF = LF
    (tmp_path / "src" / "simulator" / "a.py").write_bytes(b"x = 2\n")
    assert v.image_tag(tmp_path) != tag
    assert len(v.image_files(tmp_path)) == len(v.IMAGE_INPUTS)


def test_raw_envelopes_skip_malformed_and_duplicates():
    good = _env(1, NOW, False)
    msgs = [{"data": json.dumps(good).encode()}, {"data": json.dumps(good).encode()}, {"data": b'{"event_id": "x", "te'},
            {"data": json.dumps({"event_id": "y"}).encode()}]
    assert v.raw_envelopes(msgs) == [good]


def test_wave_stats_on_a_good_wave():
    stats = _stats(wave(START))
    p = stats["phases"]
    assert v.wave_problems(stats, 3) == []
    assert stats["rate_ratio"] == pytest.approx(3.0, abs=0.15) and p["during"]["marked_share"] == pytest.approx(0.667, abs=0.01)
    assert p["before"]["marked"] == 0 and p["after"]["marked"] == 0
    assert p["during"]["normal_per_min"] == pytest.approx(p["before"]["normal_per_min"], rel=0.1)


def test_wave_problems_catch_wrong_brand_slow_rate_and_late_marks():
    assert any("otra marca" in x for x in v.wave_problems(_stats(wave(START, wrong=True)), 3))
    assert any(x.startswith("cadencia") for x in v.wave_problems(_stats(wave(START, m=3)), 6))
    assert any("fuera del escenario" in x for x in v.wave_problems(_stats(wave(START, late=True)), 3))
    assert "ningún evento marcado durante el escenario" in v.wave_problems(_stats(wave(START, m=1)), 1)


def test_public_members():
    assert v.public_members({"bindings": [{"role": "roles/run.invoker", "members": ["allUsers", "user:a@b.c"]}]}) == ["allUsers"]
    assert v.public_members({"bindings": []}) == [] and v.public_members(None) == []


# --- partes con comandos falsos ----------------------------------------------------------------------------


class Fake:
    def __init__(self, *, image_exists=True, deployed=None, public=False, anon=403, check_ok=True):
        self.image_exists, self.public, self.anon, self.check_ok = image_exists, public, anon, check_ok
        self.deployed = deployed or f"{REPO_IMG}:h-0123456789ab"
        self.calls: list[list[str]] = []
        self.started = False

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        self.calls.append(list(cmd))
        if cmd[:2] == ["bash", "scripts/build_simulator_image.sh"]:
            return R(0, f'Imagen publicada:\n  simulator_image = "{REPO_IMG}:{cmd[3]}"\n')
        if len(cmd) > 1 and cmd[1] == "scripts/demo_scenario.py":
            return self.demo(cmd[2:])
        if cmd[0] == "gcloud":
            return self.gcloud(cmd)
        return R(1, "", f"comando inesperado: {cmd}")

    def gcloud(self, cmd):
        text = " ".join(cmd)
        if "artifacts docker images describe" in text:
            return R(0, "sha256:0123456789abcdef0123\n") if self.image_exists else R(1, "", "NOT_FOUND")
        if "run services describe" in text and "containers[0].image" in text:
            return R(0, self.deployed + "\n")
        if "run services describe" in text:
            return R(0, SIM_URL + "\n")
        if "get-iam-policy" in text:
            members = ["allUsers"] if self.public else ["serviceAccount:sa@x"]
            return R(0, json.dumps({"bindings": [{"role": "roles/run.invoker", "members": members}]}))
        if "dataflow jobs list" in text:
            return R(0, json.dumps([{"name": "ta-stream-dev", "id": "j1", "state": "Running"}]))
        return R(1, "", f"gcloud inesperado: {text}")

    def demo(self, args):
        cmd = args[0]
        if cmd == "check":
            out = {"event": "scenario_check", "problems": [] if self.check_ok else ["cadencia durante/antes = 1.0 (esperado ≈ 3)"],
                   "api": [{"step": "GET /health", "status": 200, "want": 200, "ok": True}], "api_problems": [],
                   "simulator_logs": {"scenario_start": 1},
                   "timeline": {"events": 750, "rate_ratio": 2.88, "expected_share": 0.6667, "contract_problems": [],
                                "scenario": {"name": "demo/x"}, "status_after": {"active": None, "last": {"reason": "expired"}},
                                "phases": {k: {"per_min": 30.0, "marked": 0, "marked_share": 0.0, "normal_per_min": 30.0}
                                           for k in ("before", "during", "after")}}}
            return R(0 if self.check_ok else 1, json.dumps(out) + "\n")
        if cmd == "status" and "--no-token" in args:
            return R(1, json.dumps({"event": "scenario_rejected", "status": self.anon, "error": "<html>403 Forbidden"}) + "\n")
        if cmd == "status":
            last = {**SCENARIO, "reason": "expired", "ended_at": SCENARIO["ends_at"], "events": {"instagram": 300}}
            return R(0, json.dumps({"event": "scenario_status", "status": 200, "active": None,
                                    "last": last if self.started else None}) + "\n")
        if cmd == "start" and "cocacola" in args:
            return R(1, json.dumps({"event": "scenario_rejected", "status": 400, "problems": ["brand", "multiplier"]}) + "\n")
        if cmd == "start":
            self.started = True
            return R(0, json.dumps({"event": "scenario_started", "status": 201, "started": SCENARIO}) + "\n")
        return R(2, "")


def _main(fake, argv, tmp_path, monkeypatch, sleep=None):
    tfvars = tmp_path / "terraform.tfvars"
    if not tfvars.exists():
        tfvars.write_text(f'simulator_image = "{REPO_IMG}:h-0123456789ab"\nsimulator_running = true\n', encoding="utf-8")
    monkeypatch.setattr(v, "TFVARS", tfvars)
    monkeypatch.setattr(v, "on_windows", lambda: False)
    monkeypatch.setattr(v, "image_tag", lambda root=None: "h-0123456789ab")
    lines: list[str] = []
    code = v.main(argv + ["--project", PROJ, "--yes"], runner=fake, printer=lines.append, sleep=sleep or (lambda s: None),
                  now=lambda: NOW, evidence_path=tmp_path / "ev.json")
    ev = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    return code, "\n".join(lines), ev


def _steps(ev, part):
    return {s["id"]: s["status"] for s in ev["parts"][part]["steps"]}


def test_local_ok_and_fail(tmp_path, monkeypatch):
    code, _out, ev = _main(Fake(), ["local"], tmp_path, monkeypatch)
    assert code == 0 and _steps(ev, "L") == {"L1": "OK", "L2": "OK"}
    code, out, ev = _main(Fake(check_ok=False), ["local"], tmp_path, monkeypatch)
    assert code == 1 and _steps(ev, "L")["L1"] == "FALLA" and "cadencia" in out


def test_image_reuses_or_builds_and_writes_tfvars(tmp_path, monkeypatch):
    (tmp_path / "terraform.tfvars").write_text(f'simulator_image = "{REPO_IMG}:20261020-213231"\n', encoding="utf-8")
    fake = Fake(image_exists=False)
    code, _out, ev = _main(fake, ["image"], tmp_path, monkeypatch)
    assert code == 0 and _steps(ev, "D") == {"D1": "OK", "D2": "OK", "D3": "OK"}
    assert ["bash", "scripts/build_simulator_image.sh", "cloudbuild", "h-0123456789ab"] in fake.calls
    assert v.read_image((tmp_path / "terraform.tfvars").read_text(encoding="utf-8")) == f"{REPO_IMG}:h-0123456789ab"
    assert ev["parts"]["D"]["steps"][2]["data"]["previous"].endswith(":20261020-213231")
    fake = Fake(image_exists=True)
    code, _out, ev = _main(fake, ["image"], tmp_path, monkeypatch)
    assert code == 0 and not any(c[:2] == ["bash", "scripts/build_simulator_image.sh"] for c in fake.calls)


def test_auth_ok_and_public_or_stale_image_fail(tmp_path, monkeypatch):
    code, _out, ev = _main(Fake(), ["auth"], tmp_path, monkeypatch)
    assert code == 0 and set(_steps(ev, "S").values()) == {"OK"} and len(_steps(ev, "S")) == 5
    code, _out, ev = _main(Fake(public=True, anon=200, deployed=f"{REPO_IMG}:viejo"), ["auth"], tmp_path, monkeypatch)
    steps = _steps(ev, "S")
    assert code == 1 and steps["S1"] == steps["S2"] == steps["S3"] == "FALLA" and steps["S4"] == "OK"


def test_flow_happy_path(tmp_path, monkeypatch):
    ev_path = tmp_path / "ev.json"
    ev_path.write_text(json.dumps({"lote": "6.3", "parts": {"P": {"steps": [
        {"id": "P2", "data": {"started_at": (NOW - timedelta(hours=2)).isoformat()}}]}}}), encoding="utf-8")
    envs = wave(START)
    alert = am.build_alert(metric="volume", brand="postobon", source="instagram", window_start_ms=am.to_ms("2026-10-28T15:00:00.000Z"),
                           window_end_ms=am.to_ms("2026-10-28T15:05:00.000Z"), value=300, baseline_mean=70.0, baseline_std=8.0,
                           zscore=28.0, z_threshold=3.0, baseline_windows=60, n_posts=300, avg_sentiment=-0.5, n_negative=220,
                           n_positive=20, detected_ms=am.to_ms("2026-10-28T15:06:04.000Z"), cooldown_seconds=900)
    incident = json.dumps({"incident": {"incident_id": "0.inc", "policy_name": v62.POLICY, "state": "open",
                                        "started_at": int(datetime(2026, 10, 28, 15, 7, 30, tzinfo=timezone.utc).timestamp()),
                                        "summary": "log", "documentation": {"subject": "s", "content": f"Alerta: {alert['alert_id']}"}}})
    given: dict[str, bool] = {}

    def pull(ctx, sub, limit=100):
        kind = sub.split("-")[2]
        if given.get(kind):
            return []
        given[kind] = True
        if kind == "raw":
            return [{"data": json.dumps(e).encode(), "publish_time": e["ingest_time"]} for e in envs]
        if kind == "alerts":
            return [{"data": am.encode(alert)[0], "publish_time": "2026-10-28T15:06:05.000000Z"}]
        return [{"data": incident.encode(), "publish_time": "2026-10-28T15:08:30.000000Z"}]

    monkeypatch.setattr(v62, "create_sub", lambda ctx, name, topic: True)
    monkeypatch.setattr(v62, "delete_sub", lambda ctx, name: None)
    monkeypatch.setattr(v62, "pull", pull)
    monkeypatch.setattr(v62, "first_window", lambda ctx, b, s, since: ("2026-10-28T13:05:00.000Z", 80))
    monkeypatch.setattr(v62, "key_windows", lambda ctx, b, s, since: [
        {"window_end": "2026-10-28T15:05:00.000Z", "is_alert": "true", "zscore_volume": "28.0", "zscore_sentiment": "-6.1"},
        {"window_end": "2026-10-28T15:06:00.000Z", "is_alert": "true", "zscore_volume": "31.0", "zscore_sentiment": "-6.5"}])
    monkeypatch.setattr(v62, "notifier_logs", lambda ctx, extra, freshness="30m", limit=50: [{"received_at": "2026-10-28T15:06:06.000Z"}])
    monkeypatch.setattr(v, "simulator_logs", lambda ctx, name: [{"event": "scenario_start"}, {"event": "scenario_end"}])
    monkeypatch.setattr(v, "BEFORE_S", 120)
    monkeypatch.setattr(v62, "wait_for", lambda ctx, fn, timeout_s, poll_s: fn())
    fake = Fake()
    code, out, ev = _main(fake, ["flow"], tmp_path, monkeypatch)
    steps = _steps(ev, "F")
    assert list(steps) == ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12"]
    assert code == 0, out
    assert steps["F7"] == "OK" and steps["F11"] == "OK" and steps["F10"] == "OK"
    f11 = ev["parts"]["F"]["steps"][11]["data"]
    assert f11["rate_ratio"] == pytest.approx(3.0, abs=0.2)
