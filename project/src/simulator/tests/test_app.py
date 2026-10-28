"""Servidor HTTP y orquestación de los tres workers (§3.1); ``/control/scenario`` (Lote 6.3, §3.29)."""

import asyncio
import json

import pytest

from simulator.app import Simulator, parse_sources, route, start_http
from simulator.publisher import MemoryPublisher
from simulator.state import MemoryStateStore
from simulator.tests._sim_helpers import factory

BODY = {"brand": "postobon", "sentiment": "negative", "multiplier": 3, "duration_s": 300, "source": "instagram"}


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
    assert status == 200 and body["status"] == "ok" and body["scenario"] is None
    assert route(sim, "GET", "/health") == (status, body)
    assert {s: v["seq"] for s, v in body["sources"].items()} == {"reddit": 3, "facebook": 3, "instagram": 3}
    assert {t for t, _, _ in pub.sent} == {"ta-raw-reddit", "ta-raw-facebook", "ta-raw-instagram"}
    assert route(sim, "GET", "/nada")[0] == 404
    assert route(sim, "POST", "/healthz")[0] == 405


def test_scenario_routes():
    sim = Simulator(factory().config, MemoryPublisher(), MemoryStateStore())
    assert route(sim, "GET", "/control/scenario") == (200, {"active": None, "last": None})
    assert route(sim, "POST", "/control/scenario", b"{no es json")[0] == 400
    assert route(sim, "POST", "/control/scenario")[0] == 400  # sin cuerpo
    status, body = route(sim, "POST", "/control/scenario", json.dumps({**BODY, "brand": "cocacola"}).encode())
    assert status == 400 and body["problems"][0].startswith("brand desconocida")
    status, body = route(sim, "POST", "/control/scenario", json.dumps(BODY).encode())
    assert status == 201 and body["started"]["share"] == pytest.approx(0.6667)
    assert route(sim, "POST", "/control/scenario", json.dumps(BODY).encode())[0] == 409
    assert route(sim, "GET", "/healthz")[1]["scenario"] == body["started"]["name"]
    assert route(sim, "PUT", "/control/scenario")[0] == 405
    status, body = route(sim, "DELETE", "/control/scenario")
    assert status == 200 and body["stopped"]["reason"] == "stopped"
    assert route(sim, "DELETE", "/control/scenario") == (200, {"stopped": None})


def test_http_reads_the_body_and_limits_it():
    sim = Simulator(factory().config, MemoryPublisher(), MemoryStateStore())
    body = json.dumps(BODY).encode()
    requests = [
        b"POST /control/scenario HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: "
        + str(len(body)).encode() + b"\r\n\r\n" + body,
        b"POST /control/scenario HTTP/1.1\r\nContent-Length: 5000\r\n\r\n",
        b"GET /health HTTP/1.1\r\nContent-Length: abc\r\n\r\n",
    ]

    async def go():
        server = await start_http(sim, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        out = []
        for data in requests:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(data)
            await writer.drain()
            raw = await reader.read()
            writer.close()
            head, _, payload = raw.partition(b"\r\n\r\n")
            out.append((int(head.split()[1]), json.loads(payload)))
        server.close()
        await server.wait_closed()
        return out

    (s1, b1), (s2, _), (s3, _) = asyncio.run(go())
    assert s1 == 201 and b1["started"]["brand"] == "postobon"
    assert s2 == 413 and s3 == 400
