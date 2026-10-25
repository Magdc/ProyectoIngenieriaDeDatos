"""Flujo completo del CLI con backend y reloj falsos (sin Pub/Sub ni GCS)."""

from __future__ import annotations

import json

import pytest

from ingest_check import cli
from ingest_check.cursor import NullCursorReader
from ingest_check.stats import CursorState
from ingest_check.testing import T0, FakeBackend, FakeClock, stream

SOURCES = ["reddit", "facebook", "instagram"]


class DictCursorReader:
    """Cursor en memoria; ``values`` se puede cambiar durante la prueba."""

    def __init__(self, values: dict[str, CursorState | None]) -> None:
        self.values = values

    def read(self, source):
        return self.values.get(source)

    def describe(self):
        return "memoria"


def _runtime(clock: FakeClock, **kw) -> cli.Runtime:
    logs: list[str] = []
    rt = cli.Runtime(clock=clock, sleep=clock.sleep, log=logs.append, **kw)
    rt.logs = logs  # type: ignore[attr-defined]
    return rt


def _run(argv, backend, cursors, rt):
    return cli.main(
        argv,
        rt=rt,
        backend_factory=lambda cfg, args, log: backend,
        cursor_factory=lambda args: cursors,
    )


BASE = ["--project", "p-test", "--validate", "no"]


def test_measure_passes_with_three_healthy_sources(tmp_path, capsys):
    clock = FakeClock()
    msgs = []
    for i, src in enumerate(SOURCES):
        msgs += stream(src, 1000 * (i + 1), 70, T0 + i * 0.3)
    backend = FakeBackend(clock, msgs)
    cursors = DictCursorReader({"reddit": CursorState(7, 1050), "facebook": CursorState(7, 2050),
                                "instagram": CursorState(7, 3050)})
    out = tmp_path / "ev" / "r.json"
    code = _run(["measure", *BASE, "--duration", "120", "--json-out", str(out)], backend, cursors, _runtime(clock))
    text = capsys.readouterr().out
    assert code == 0, text
    assert backend.setup_called and backend.torn_down
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["verdict"] == "ok"
    assert report["per_source"]["reddit"]["messages"] == 60  # T0..T0+118 dentro de la ventana
    assert report["per_source"]["reddit"]["seq_max"] == 1059
    assert report["per_source"]["facebook"]["cursor"]["seq"] == 2050
    assert report["total_rate_msg_s"] == pytest.approx(1.5, abs=0.02)
    assert "Resultado: OK" in text


def test_measure_fails_when_a_topic_is_silent(capsys):
    clock = FakeClock()
    backend = FakeBackend(clock, stream("reddit", 0, 70, T0) + stream("facebook", 0, 70, T0))
    code = _run(["measure", *BASE, "--cursor", "none", "--duration", "120"], backend, NullCursorReader(),
                _runtime(clock))
    text = capsys.readouterr().out
    assert code == 1
    assert "instagram" in text and "FALLA" in text


def test_measure_teardown_even_on_error(capsys):
    clock = FakeClock()

    class Boom(FakeBackend):
        def start(self, on_message):
            raise RuntimeError("sin permisos")

    backend = Boom(clock, [])
    rt = _runtime(clock)
    code = _run(["measure", *BASE, "--duration", "10"], backend, NullCursorReader(), rt)
    assert code == 2 and backend.torn_down
    assert any("sin permisos" in m for m in rt.logs)


def test_config_errors_exit_2(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    clock = FakeClock()
    rt = _runtime(clock)
    assert _run(["measure", "--validate", "no"], FakeBackend(clock), NullCursorReader(), rt) == 2
    assert any("GCP_PROJECT" in m for m in rt.logs)
    assert _run(["measure", *BASE, "--subscription", "reddit=ta-raw-reddit-df"],
                FakeBackend(clock), NullCursorReader(), rt) == 2
    assert _run(["measure", *BASE, "--subscription", "tiktok=x"], FakeBackend(clock), NullCursorReader(), rt) == 2
    assert _run(["measure", *BASE, "--topic-template", "raw"], FakeBackend(clock), NullCursorReader(), rt) == 2


def test_parse_subscriptions():
    assert cli.parse_subscriptions(["reddit=verif-r"], SOURCES) == {"reddit": "verif-r"}
    with pytest.raises(ValueError):
        cli.parse_subscriptions(["reddit"], SOURCES)


# ---------------------------------------------------------------------------
# restart-check


def _restart_scenario(resume_from: dict[str, int], restart_seconds: float = 40.0):
    """Antes: 90 s emitiendo. Reinicio de ``restart_seconds``. Después: 120 s desde ``resume_from``."""
    msgs = []
    before_end = T0 + 90
    resume_at = before_end + restart_seconds + 5
    for i, src in enumerate(SOURCES):
        start = 500 * (i + 1)
        msgs += stream(src, start, 45, T0)          # seq start .. start+44 en [T0, T0+88]
        msgs += stream(src, resume_from[src], 62, resume_at)
    return msgs


def test_restart_check_passes_when_seq_continues(capsys):
    clock = FakeClock()
    # Reanuda desde el último cursor guardado (repite algunos ids, ≤ 50).
    resume = {"reddit": 540, "facebook": 1040, "instagram": 1540}
    backend = FakeBackend(clock, _restart_scenario(resume))
    cursors = DictCursorReader({s: CursorState(7, v) for s, v in resume.items()})

    def prompt(_msg):
        clock.sleep(40)
        cursors.values = {s: CursorState(7, v + 5) for s, v in resume.items()}  # guardado al SIGTERM
        return ""

    rt = _runtime(clock, prompt=prompt, stdin_is_tty=lambda: True)
    code = _run(["restart-check", *BASE, "--before", "90", "--after", "120"], backend, cursors, rt)
    text = capsys.readouterr().out
    assert code == 0, text
    assert "reanudó en 540" in text
    assert "repetidos" in text


def test_restart_check_fails_when_seq_resets_to_zero(capsys):
    clock = FakeClock()
    backend = FakeBackend(clock, _restart_scenario({"reddit": 0, "facebook": 1040, "instagram": 1540}))
    rt = _runtime(clock, prompt=lambda _m: clock.sleep(40) or "", stdin_is_tty=lambda: True)
    code = _run(["restart-check", *BASE, "--cursor", "none"], backend, NullCursorReader(), rt)
    text = capsys.readouterr().out
    assert code == 1
    assert "seq volvió atrás" in text


def test_restart_check_fails_when_service_does_not_come_back(capsys):
    clock = FakeClock()
    msgs = []
    for src in SOURCES:
        msgs += stream(src, 300, 45, T0)
    backend = FakeBackend(clock, msgs)
    rt = _runtime(clock)
    code = _run(["restart-check", *BASE, "--cursor", "none", "--restart", "wait", "--restart-wait", "30"],
                backend, NullCursorReader(), rt)
    assert code == 1
    assert "no volvió a emitir" in capsys.readouterr().out


def test_restart_manual_without_tty_is_an_error():
    clock = FakeClock()
    rt = _runtime(clock, stdin_is_tty=lambda: False)
    backend = FakeBackend(clock, [])
    code = _run(["restart-check", *BASE, "--cursor", "none", "--before", "1", "--after", "1"],
                backend, NullCursorReader(), rt)
    assert code == 2 and backend.torn_down
    assert any("--restart wait" in m for m in rt.logs)


def test_restart_gcloud_builds_the_command(capsys):
    clock = FakeClock()
    calls: list[list[str]] = []

    def run(cmd):
        calls.append(cmd)
        clock.sleep(40)
        return 0

    resume = {"reddit": 540, "facebook": 1040, "instagram": 1540}
    backend = FakeBackend(clock, _restart_scenario(resume))
    rt = _runtime(clock, run=run)
    code = _run(["restart-check", *BASE, "--cursor", "none", "--restart", "gcloud", "--region", "us-central1"],
                backend, NullCursorReader(), rt)
    assert code == 0, capsys.readouterr().out
    cmd = calls[0]
    assert cmd[:5] == ["gcloud", "run", "services", "update", "ta-stream-simulator"]
    assert "--update-env-vars" in cmd and cmd[cmd.index("--region") + 1] == "us-central1"
    assert cmd[cmd.index("--project") + 1] == "p-test"


def test_restart_gcloud_failure_exits_2():
    clock = FakeClock()
    rt = _runtime(clock, run=lambda cmd: 1)
    code = _run(["restart-check", *BASE, "--cursor", "none", "--restart", "gcloud", "--before", "1"],
                FakeBackend(clock, []), NullCursorReader(), rt)
    assert code == 2
