"""Escenario de demo: validación del cuerpo, ciclo de vida, qué eventos fuerza y cadencia (Lote 6.3, §3.29)."""

import asyncio
import json

import pytest

from simulator.publisher import MemoryPublisher, SourceWorker
from simulator.scenario import ScenarioConflict, ScenarioControl, ScenarioError, parse_request
from simulator.state import MemoryStateStore
from simulator.tests._sim_helpers import NOW, factory

BODY = {"brand": "postobon", "sentiment": "negative", "multiplier": 5, "duration_s": 600, "source": "instagram"}
T = NOW.timestamp()


class Clock:
    def __init__(self, t: float = T) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _control(clock=None, logs=None):
    return ScenarioControl(factory().brands, 7, clock=clock or Clock(),
                           log=(lambda ev, **f: logs.append((ev, f))) if logs is not None else (lambda *a, **k: None))


def test_parse_valid_with_alias_and_default_name():
    a = parse_request({**BODY, "brand": "Postobón"}, factory().brands, T)
    assert a.brand == "postobon" and a.sentiment == "negative" and a.multiplier == 5.0 and a.duration_s == 600
    assert a.name == "demo/postobon-negative/20261020T170000Z"
    assert a.share == pytest.approx(0.8) and a.ends_at == T + 600
    d = a.to_dict()
    assert d["started_at"] == "2026-10-20T17:00:00.123Z" and d["ends_at"] == "2026-10-20T17:10:00.123Z"
    assert parse_request({**BODY, "name": "ensayo-1"}, factory().brands, T).name == "ensayo-1"


def test_parse_reports_every_problem():
    with pytest.raises(ScenarioError) as e:
        parse_request({"brand": "cocacola", "sentiment": "furioso", "multiplier": True, "duration_s": 30.5,
                       "source": "tiktok", "name": "-x", "extra": 1}, factory().brands, T)
    text = " | ".join(e.value.problems)
    for key in ("campo desconocido: extra", "brand desconocida", "sentiment inválido", "multiplier", "duration_s",
                "source inválida", "name debe"):
        assert key in text
    assert len(e.value.problems) == 7


@pytest.mark.parametrize("body", [None, [], "texto", {"brand": "pepsi"}])
def test_parse_rejects_non_objects_and_missing_fields(body):
    with pytest.raises(ScenarioError):
        parse_request(body, factory().brands, T)


@pytest.mark.parametrize("field,value,ok", [("multiplier", 2, True), ("multiplier", 10, True), ("multiplier", 1.5, False),
                                            ("multiplier", 11, False), ("duration_s", 60, True), ("duration_s", 3600, True),
                                            ("duration_s", 59, False), ("duration_s", 3601, False), ("duration_s", 120.0, True)])
def test_parse_limits(field, value, ok):
    body = {**BODY, field: value}
    if ok:
        assert parse_request(body, factory().brands, T)
    else:
        with pytest.raises(ScenarioError):
            parse_request(body, factory().brands, T)


def test_lifecycle_conflict_expire_and_stop():
    clock, logs = Clock(), []
    c = _control(clock, logs)
    assert c.status() == {"active": None, "last": None} and c.stop() is None
    c.start(BODY)
    with pytest.raises(ScenarioConflict):
        c.start(BODY)
    clock.t += 599
    assert c.status()["active"]["remaining_s"] == 1.0
    clock.t += 1
    assert c.current() is None
    assert c.last["reason"] == "expired" and c.last["ended_at"] == "2026-10-20T17:10:00.123Z"
    c.start({**BODY, "duration_s": 60})
    clock.t += 10
    stopped = c.stop()
    assert stopped["reason"] == "stopped" and stopped["ended_at"] == "2026-10-20T17:10:10.123Z"
    assert [ev for ev, _ in logs] == ["scenario_start", "scenario_end", "scenario_start", "scenario_end"]


def test_pick_is_deterministic_and_only_the_added_share():
    c = _control()
    c.start(BODY)
    picks = [c.pick("instagram", s) for s in range(3000)]
    share = sum(p is not None for p in picks) / len(picks)
    assert share == pytest.approx(0.8, abs=0.03)
    assert c.status()["active"]["events"] == {"instagram": sum(p is not None for p in picks)}
    again = _control()
    again.start(BODY)
    assert [again.pick("instagram", s) is None for s in range(3000)] == [p is None for p in picks]
    assert all(c.pick("reddit", s) is None for s in range(200))  # otra fuente: sin escenario
    p = next(p for p in picks if p)
    assert (p.brand, p.sentiment, p.source) == ("postobon", "negative", "instagram")
    assert c.speed("instagram") == 5.0 and c.speed("reddit") == 1.0


def test_without_source_applies_to_all():
    c = _control()
    c.start({k: v for k, v in BODY.items() if k != "source"})
    assert {s: c.speed(s) for s in ("reddit", "facebook", "instagram")} == {"reddit": 5.0, "facebook": 5.0, "instagram": 5.0}
    assert any(c.pick("reddit", s) for s in range(20))


async def _no_sleep(_s):
    await asyncio.sleep(0)


def test_worker_marks_forced_events_and_speeds_up():
    clock = Clock()
    c = _control(clock)
    pub = MemoryPublisher()
    w = SourceWorker("instagram", factory(), pub, MemoryStateStore(), clock=lambda: NOW, sleep=_no_sleep, control=c,
                     max_events=200)
    normal = [w._interval(s) for s in range(50)]
    c.start(BODY)
    assert [w._interval(s) for s in range(50)] == pytest.approx([x / 5 for x in normal])
    asyncio.run(w.run())
    envs = [json.loads(d) for _t, d, _a in pub.sent]
    marked = [e for e in envs if e["sim"]["scenario"]]
    assert 140 <= len(marked) <= 180
    assert {e["brands"][0] for e in marked} == {"postobon"} and {e["sim"]["label_sentiment"] for e in marked} == {"negative"}
    assert {e["sim"]["scenario"] for e in marked} == {"demo/postobon-negative/20261020T170000Z"}
    # mismo seq sin escenario ⇒ mismo event_id (el escenario no cambia la identidad)
    plain = factory().make("instagram", marked[0]["seq"], NOW).envelope
    assert plain["event_id"] == marked[0]["event_id"] and plain["sim"]["scenario"] is None
