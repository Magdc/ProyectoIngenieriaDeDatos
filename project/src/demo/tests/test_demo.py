"""Cliente, CLI y comprobación local del escenario de demo (Lote 6.3, §3.29.1). Sin nube."""

import json

import pytest

from demo import cli, client, local

URL = "https://ta-stream-simulator-abc-uc.a.run.app"


class FakeGcloud:
    def __init__(self, url=URL, token="tok-123"):
        self.url, self.token, self.calls = url, token, []

    def __call__(self, args):
        self.calls.append(args)
        return self.url if args[0] == "run" else self.token


class FakeCall:
    def __init__(self, reply=client.Reply(201, {"started": {"name": "demo/x"}})):
        self.reply, self.calls = reply, []

    def __call__(self, url, method, path=client.PATH, body=None, token=None, timeout=30):
        self.calls.append({"url": url, "method": method, "body": body, "token": token})
        return self.reply


def _out(capsys):
    return [json.loads(ln) for ln in capsys.readouterr().out.splitlines()]


def test_start_in_cloud_uses_the_service_url_and_a_token(capsys):
    run, call = FakeGcloud(), FakeCall()
    assert cli.main(["start", "--project", "p1"], call=call, run=run) == 0
    assert run.calls[0][:4] == ["run", "services", "describe", "ta-stream-simulator"] and "--project=p1" in run.calls[0]
    assert run.calls[1] == ["auth", "print-identity-token"]
    assert call.calls == [{"url": URL, "method": "POST", "token": "tok-123",
                           "body": {"brand": "postobon", "sentiment": "negative", "multiplier": 3.0, "duration_s": 300,
                                    "source": "instagram"}}]
    assert _out(capsys)[0]["event"] == "scenario_started"


def test_local_url_goes_without_token_and_all_sources(capsys):
    run, call = FakeGcloud(), FakeCall(client.Reply(200, {"active": None, "last": None}))
    assert cli.main(["status", "--url", "http://localhost:8080"], call=call, run=run) == 0
    assert run.calls == [] and call.calls[0]["token"] is None and call.calls[0]["method"] == "GET"
    call = FakeCall()
    cli.main(["start", "--url", "http://localhost:8080", "--source", "all", "--name", "ensayo"], call=call, run=run)
    assert "source" not in call.calls[0]["body"] and call.calls[0]["body"]["name"] == "ensayo"
    assert [e["event"] for e in _out(capsys)] == ["scenario_status", "scenario_started"]


def test_no_token_for_the_auth_check(capsys):
    run, call = FakeGcloud(), FakeCall(client.Reply(403, {"error": "<html>Forbidden"}))
    assert cli.main(["status", "--no-token"], call=call, run=run) == 1
    assert len(run.calls) == 1 and call.calls[0]["url"] == URL and call.calls[0]["token"] is None
    assert _out(capsys)[0]["status"] == 403


def test_simulator_service_is_not_public():
    from pathlib import Path

    infra = Path(__file__).resolve().parents[3] / "infra" / "gcp"
    files = [infra / "modules" / "cloud_run_simulator" / "main.tf", infra / "environments" / "dev" / "platform.tf"]
    text = "\n".join(ln.split("#", 1)[0] for p in files for ln in p.read_text(encoding="utf-8").splitlines())
    assert "allUsers" not in text and "allAuthenticatedUsers" not in text
    assert "google_cloud_run_v2_service_iam" not in text  # nadie más invoca el simulador


def test_rejections_and_errors(capsys):
    call = FakeCall(client.Reply(409, {"error": "ya hay un escenario activo"}))
    assert cli.main(["start", "--url", "http://x"], call=call) == 1
    assert cli.main(["stop"], call=FakeCall(), run=FakeGcloud(url="")) == 2
    out = _out(capsys)
    assert out[0]["event"] == "scenario_rejected" and out[0]["status"] == 409
    assert out[1]["event"] == "scenario_error" and "no está desplegado" in out[1]["error"]


def test_scenario_body_and_identity_token():
    assert client.scenario_body("pepsi", "positive", 2, 60) == {"brand": "pepsi", "sentiment": "positive", "multiplier": 2,
                                                                 "duration_s": 60}
    with pytest.raises(client.ClientError):
        client.identity_token(lambda _a: "")


def test_call_against_the_real_server_and_html_errors():
    steps = local.api_check()
    assert [s["status"] for s in steps] == [200, 200, 400, 201, 409, 200, 200, 200, 200, 404]
    assert all(s["ok"] for s in steps), [s for s in steps if not s["ok"]]
    with pytest.raises(client.ClientError):
        client.call("http://127.0.0.1:9", "GET", timeout=2)


def test_timeline_meets_the_wave_rules():
    t = local.timeline(multiplier=3, duration_s=300, before_s=300, after_s=300)
    assert local.timeline_problems(t, 3) == []
    assert t["phases"]["before"]["marked"] == 0 and t["phases"]["after"]["marked"] == 0
    assert t["status_after"]["last"]["reason"] == "expired"
    bad = json.loads(json.dumps(t))
    bad["rate_ratio"] = 1.0
    bad["phases"]["after"]["marked"] = 4
    bad["generators"] = ["comment-gen@1.0", "ta-verify@6.2"]
    problems = local.timeline_problems(bad, 3)
    assert len(problems) == 3 and problems[0].startswith("cadencia")


def test_check_command(capsys):
    assert cli.main(["check", "--multiplier", "2", "--duration", "120"]) == 0
    out = _out(capsys)
    assert len(out) == 1 and out[0]["event"] == "scenario_check" and out[0]["problems"] == [] and out[0]["api_problems"] == []
    assert out[0]["simulator_logs"] == {"scenario_end": 1, "scenario_rejected": 1, "scenario_start": 1}
