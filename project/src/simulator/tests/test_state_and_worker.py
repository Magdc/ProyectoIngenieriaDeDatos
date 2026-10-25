"""Cursor persistente, reanudación y bucle asyncio (§3.5, §3.6, HU-013)."""

import asyncio
import json

import pytest

from simulator.publisher import MemoryPublisher, SourceWorker
from simulator.state import Cursor, LocalStateStore, MemoryStateStore, object_name, resume_seq, store_from_env
from simulator.tests._sim_helpers import NOW, factory


async def _no_sleep(_s):
    await asyncio.sleep(0)


def _run(worker):
    asyncio.run(worker.run())
    return worker


def _worker(store, pub, n, source="facebook", seed=7):
    return SourceWorker(source, factory(seed), pub, store, clock=lambda: NOW, sleep=_no_sleep, max_events=n)


def test_cursor_roundtrip_and_path():
    c = Cursor(seed=7, seq=1207)
    d = json.loads(c.to_json())
    assert d["seed"] == 7 and d["seq"] == 1207 and d["updated_at"].endswith("Z")
    assert Cursor.from_json(c.to_json()).seq == 1207
    assert object_name("reddit") == "sim-state/reddit.json"


def test_resume_rules():
    store = MemoryStateStore()
    assert resume_seq(store, "reddit", 7) == 0
    store.save("reddit", Cursor(7, 300))
    assert resume_seq(store, "reddit", 7) == 300
    assert resume_seq(store, "reddit", 8) == 0  # otra semilla: empieza de cero
    store.objects[object_name("reddit")] = "{no es json"
    assert resume_seq(store, "reddit", 7) == 0


def test_worker_checkpoints_every_50_and_on_stop():
    store, pub = MemoryStateStore(), MemoryPublisher()
    w = _run(_worker(store, pub, 120))
    assert w.stats.seq == 120 and len(pub.sent) == 120
    assert store.saves == 3  # seq 50, 100 y al detenerse
    assert store.load("facebook").seq == 120
    assert {t for t, _, _ in pub.sent} == {"ta-raw-facebook"}
    assert [json.loads(d)["seq"] for _, d, _ in pub.sent] == list(range(120))


def test_restart_continues_from_cursor_with_same_ids():
    store, pub = MemoryStateStore(), MemoryPublisher()
    _run(_worker(store, pub, 70))
    pub2 = MemoryPublisher()
    w2 = _run(_worker(store, pub2, 30))
    assert json.loads(pub2.sent[0][1])["seq"] == 70 and w2.stats.seq == 100

    ref = MemoryPublisher()
    _run(_worker(MemoryStateStore(), ref, 100))
    ids = lambda p: [a["event_id"] for _, _, a in p.sent]
    assert ids(pub) + ids(pub2) == ids(ref)  # reiniciar no cambia la secuencia


def test_crash_repeats_at_most_50_with_same_ids():
    store, pub = MemoryStateStore(), MemoryPublisher()
    _run(_worker(store, pub, 149))
    # Simula una caída: el último cursor durable es el de seq 100 (no el de la parada).
    store.save("facebook", Cursor(7, 100))
    pub2 = MemoryPublisher()
    _run(_worker(store, pub2, 49))
    first_ids = [a["event_id"] for _, _, a in pub.sent]
    again = [a["event_id"] for _, _, a in pub2.sent]
    assert again == first_ids[100:149] and len(again) <= 50


def test_seed_change_restarts_at_zero():
    store = MemoryStateStore()
    store.save("reddit", Cursor(seed=1, seq=500))
    pub = MemoryPublisher()
    _run(_worker(store, pub, 3, source="reddit"))
    assert json.loads(pub.sent[0][1])["seq"] == 0


def test_publish_error_retries_same_seq():
    class Flaky(MemoryPublisher):
        fails = 2

        async def publish(self, topic, data, attributes):
            if self.fails:
                self.fails -= 1
                raise RuntimeError("pubsub no disponible")
            return await super().publish(topic, data, attributes)

    pub = Flaky()
    w = _run(_worker(MemoryStateStore(), pub, 5, source="instagram"))
    assert w.stats.errors == 2
    assert [json.loads(d)["seq"] for _, d, _ in pub.sent] == [0, 1, 2, 3, 4]


def test_interval_between_1_and_3_seconds():
    waits = []

    async def rec(s):
        waits.append(s)

    w = SourceWorker("reddit", factory(), MemoryPublisher(), MemoryStateStore(), clock=lambda: NOW, sleep=rec, max_events=200)
    _run(w)
    assert len(waits) == 200 and all(1.0 <= x <= 3.0 for x in waits)
    assert 1.8 < sum(waits) / len(waits) < 2.2  # ~0.5 msg/s


def test_local_store_atomic(tmp_path):
    s = LocalStateStore(tmp_path)
    assert s.load("reddit") is None
    s.save("reddit", Cursor(7, 9))
    assert (tmp_path / "sim-state" / "reddit.json").exists() and s.load("reddit").seq == 9


def test_store_from_env():
    assert isinstance(store_from_env({"SIM_STATE_BACKEND": "memory"}), MemoryStateStore)
    assert isinstance(store_from_env({"SIM_STATE_BACKEND": "local", "SIM_STATE_DIR": "x"}), LocalStateStore)
    with pytest.raises(ValueError):
        store_from_env({"SIM_STATE_BACKEND": "firestore"})
