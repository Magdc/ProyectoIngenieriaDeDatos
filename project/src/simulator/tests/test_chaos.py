"""Caos controlado (§3.9, HU-015)."""

import json
from collections import Counter
from datetime import datetime

from simulator.factory import EventFactory
from simulator.tests._sim_helpers import NOW, config_with, factory

SOURCES = ("reddit", "facebook", "instagram")


def _chaos_factory(rates=None, enabled=True):
    rates = rates or {"duplicate": 0.05, "late": 0.05, "null_fields": 0.05, "malformed": 0.05}
    return EventFactory(config_with(env={"SIM_CHAOS_ENABLED": str(enabled).lower()}, chaos={"rates": rates}))


def test_chaos_off_produces_no_defects():
    f = factory()
    assert f.config.chaos.enabled is False
    for s in SOURCES:
        for q in range(3000):
            em = f.make(s, q, NOW)
            assert em.chaos == () and len(em.messages) == 1
            assert json.loads(em.messages[0].data)["sim"]["chaos"] == []


def test_chaos_does_not_change_text_or_ids():
    clean, noisy = factory(), _chaos_factory()
    for s in SOURCES:
        for q in range(600):
            a, b = clean.make(s, q, NOW).envelope, noisy.make(s, q, NOW).envelope
            assert a["event_id"] == b["event_id"] and a["text"] == b["text"] and a["payload"] == b["payload"]


def test_defects_are_marked_and_shaped():
    f = _chaos_factory()
    seen = Counter()
    for s in SOURCES:
        for q in range(2000):
            em = f.make(s, q, NOW)
            seen.update(em.chaos)
            ids = {m.attributes["event_id"] for m in em.messages}
            assert ids == {em.envelope["event_id"]}
            if "malformed" in em.chaos:
                assert em.chaos == ("malformed",) and len(em.messages) == 1
                raw = em.messages[0].data
                try:
                    body = json.loads(raw)
                except ValueError:
                    continue  # JSON roto
                missing = {"event_time", "text", "source", "author", "payload"} - set(body)
                assert len(missing) == 1 and body["sim"]["chaos"] == ["malformed"]
                continue
            first = json.loads(em.messages[0].data)
            assert "duplicate" not in first["sim"]["chaos"]
            if "duplicate" in em.chaos:
                assert len(em.messages) == 2
                dup = json.loads(em.messages[1].data)
                assert dup["event_id"] == first["event_id"] and "duplicate" in dup["sim"]["chaos"]
            else:
                assert len(em.messages) == 1
            if "late" in em.chaos:
                et = datetime.strptime(first["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                it = datetime.strptime(first["ingest_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                assert 2 * 60 <= (it - et).total_seconds() <= 30 * 60 + 5
                assert "late" in first["sim"]["chaos"]
            if "null_fields" in em.chaos:
                assert first["lang"] is None or first["author"]["followers"] is None
                assert "null_fields" in first["sim"]["chaos"]
            assert len(first["sim"]["chaos"]) == len(set(first["sim"]["chaos"]))
    total = 6000
    for kind in ("duplicate", "late", "null_fields", "malformed"):
        assert 0.025 < seen[kind] / total < 0.08, (kind, seen[kind])


def test_default_rates_are_small():
    f = _chaos_factory(rates={"duplicate": 0.005, "late": 0.01, "null_fields": 0.01, "malformed": 0.002})
    seen = Counter()
    n = 0
    for s in SOURCES:
        for q in range(4000):
            seen.update(f.make(s, q, NOW).chaos)
            n += 1
    assert seen["late"] / n < 0.02 and seen["duplicate"] / n < 0.012 and seen["malformed"] / n < 0.006
    assert seen["late"] > 0 and seen["null_fields"] > 0
