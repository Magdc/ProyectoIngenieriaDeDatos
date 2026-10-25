"""Servidor HTTP y orquestación de los tres workers (§3.1)."""

import asyncio

import pytest

from simulator.app import Simulator, parse_sources, route
from simulator.publisher import MemoryPublisher
from simulator.state import MemoryStateStore
from simulator.tests._sim_helpers import factory


def test_parse_sources():
    assert parse_sources(None) == ("reddit", "facebook", "instagram")
    assert parse_sources("reddit, instagram") == ("reddit", "instagram")
    with pytest.raises(ValueError):
        parse_sources("mastodon")


def test_routes_and_three_workers():
    pub = MemoryPublisher()
    sim = Simulator(factory().config, pub, MemoryStateStore(), max_events=3)
    for w in sim.workers.values():
        async def nowait(_s):
            await asyncio.sleep(0)
        w._sleep = nowait

    async def go():
        await sim.start()
        await sim.wait()

    asyncio.run(go())
    status, body = route(sim, "GET", "/healthz")
    assert status == 200 and body["status"] == "ok"
    assert {s: v["seq"] for s, v in body["sources"].items()} == {"reddit": 3, "facebook": 3, "instagram": 3}
    assert {t for t, _, _ in pub.sent} == {"ta-raw-reddit", "ta-raw-facebook", "ta-raw-instagram"}
    assert route(sim, "POST", "/control/scenario")[0] == 501
    assert route(sim, "GET", "/nada")[0] == 404
    assert route(sim, "POST", "/healthz")[0] == 405
