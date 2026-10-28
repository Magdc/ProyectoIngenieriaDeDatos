"""Comprobación local del escenario de demo, sin nube (Lote 6.3, §3.29.1).

- :func:`timeline`: corre el bucle real de una fuente (``SourceWorker``) con reloj virtual, un escenario
  en medio y ``MemoryPublisher``, y cuenta por tramo (antes, durante, después): eventos por minuto,
  cuántos llevan el escenario, su marca, sentimiento y generador, y si cumplen el envelope v1.0.
- :func:`api_check`: levanta el servidor HTTP real del simulador en ``127.0.0.1`` (puerto libre) y
  recorre ``/health`` y ``/control/scenario`` con el cliente: 200, 201, 400, 409, ``DELETE`` y 404.
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from demo import client
from simulator.publisher import MemoryPublisher, SourceWorker
from simulator.scenario import ScenarioControl
from simulator.sim_config import load_config
from simulator.state import MemoryStateStore

T0 = datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc)


class _VirtualClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def dt(self) -> datetime:
        return self.now

    def ts(self) -> float:
        return self.now.timestamp()

    async def sleep(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        await asyncio.sleep(0)


def timeline(brand: str = "postobon", sentiment: str = "negative", multiplier: float = 3, duration_s: int = 300,
             source: str = "instagram", before_s: int = 300, after_s: int = 300, seed: int = 7,
             start_seq: int = 0) -> dict[str, Any]:
    """Emite ``before_s`` sin escenario, ``duration_s`` con él y ``after_s`` sin él, en tiempo virtual."""
    from simulator.factory import EventFactory
    from streaming.schemas import ENVELOPE_SCHEMA, build_validator, describe_error, load_bundle

    config = load_config(env={"SIM_SEED": str(seed), "SIM_CHAOS_ENABLED": "false"})
    factory = EventFactory(config)
    clock = _VirtualClock(T0)
    control = ScenarioControl(factory.brands, factory.seed, clock=clock.ts)
    pub = MemoryPublisher()
    store = MemoryStateStore()
    if start_seq:
        from simulator.state import Cursor

        store.save(source, Cursor(seed=factory.seed, seq=start_seq))
    worker = SourceWorker(source, factory, pub, store, clock=clock.dt, sleep=clock.sleep, control=control)
    start_at = T0 + timedelta(seconds=before_s)
    stop_at = start_at + timedelta(seconds=duration_s + after_s)
    started: dict[str, Any] = {}

    async def drive() -> None:
        task = asyncio.create_task(worker.run())
        while clock.now < stop_at:
            if not started and clock.now >= start_at:
                started.update(control.start({"brand": brand, "sentiment": sentiment, "multiplier": multiplier,
                                              "duration_s": duration_s, "source": source}).to_dict())
            await asyncio.sleep(0)
        worker.stop()
        await task

    asyncio.run(drive())
    envs = [json.loads(data) for _t, data, _a in pub.sent]
    start_at = datetime.fromisoformat(started["started_at"].replace("Z", "+00:00"))
    ends_at = start_at + timedelta(seconds=duration_s)
    phases: dict[str, dict[str, Any]] = {}
    for key, lo, hi in (("before", T0, start_at), ("during", start_at, ends_at), ("after", ends_at, stop_at)):
        evs = [e for e in envs if lo <= datetime.fromisoformat(e["ingest_time"].replace("Z", "+00:00")) < hi]
        marked = [e for e in evs if e["sim"]["scenario"]]
        normal = [e for e in evs if not e["sim"]["scenario"]]
        minutes = (hi - lo).total_seconds() / 60
        phases[key] = {
            "events": len(evs),
            "per_min": round(len(evs) / minutes, 2),
            "marked": len(marked),
            "marked_share": round(len(marked) / len(evs), 4) if evs else 0.0,
            "normal_per_min": round(len(normal) / minutes, 2),
            "normal_brand_per_min": {b: round(sum(1 for e in normal if e["brands"][:1] == [b]) / minutes, 2)
                                     for b in sorted(factory.brands.keys)},
            "wrong_marked": [e["event_id"] for e in marked
                             if e["brands"][:1] != [brand] or e["sim"]["label_sentiment"] != sentiment
                             or e["sim"]["scenario"] != started.get("name") or e["source"] != source][:5],
        }
    validator = build_validator(load_bundle(), ENVELOPE_SCHEMA)
    contract = [p for p in (describe_error(validator, e) for e in envs) if p]
    before, during = phases["before"]["per_min"], phases["during"]["per_min"]
    return {
        "scenario": started,
        "phases": phases,
        "rate_ratio": round(during / before, 2) if before else None,
        "expected_share": round(1 - 1 / multiplier, 4),
        "events": len(envs),
        "generators": sorted({e["generator"] for e in envs}),
        "contract_problems": contract[:5],
        "status_after": control.status(),
    }


def timeline_problems(t: dict[str, Any], multiplier: float) -> list[str]:
    """Lo que la ola debe cumplir: cadencia ×m, fracción marcada 1 − 1/m, el resto igual y nada fuera de tiempo."""
    p = t["phases"]
    out = []
    ratio = t["rate_ratio"] or 0
    if not 0.75 * multiplier <= ratio <= 1.25 * multiplier:
        out.append(f"cadencia durante/antes = {ratio} (esperado ≈ {multiplier})")
    if abs(p["during"]["marked_share"] - t["expected_share"]) > 0.08:
        out.append(f"fracción marcada {p['during']['marked_share']} (esperado ≈ {t['expected_share']})")
    if p["before"]["marked"] or p["after"]["marked"]:
        out.append(f"eventos marcados fuera del escenario: antes {p['before']['marked']}, después {p['after']['marked']}")
    base = p["before"]["normal_per_min"]
    if base and not 0.6 <= p["during"]["normal_per_min"] / base <= 1.4:
        out.append(f"el tráfico normal cambió: {base}/min antes y {p['during']['normal_per_min']}/min durante")
    if p["during"]["wrong_marked"]:
        out.append(f"eventos marcados con otra marca, sentimiento o nombre: {p['during']['wrong_marked']}")
    if t["contract_problems"]:
        out.append(f"envelope v1.0: {t['contract_problems'][:2]}")
    if t["generators"] != ["comment-gen@1.0"]:
        out.append(f"generadores {t['generators']}")
    last = t["status_after"].get("last") or {}
    if t["status_after"].get("active") is not None or last.get("reason") != "expired":
        out.append(f"el escenario no terminó solo: {t['status_after']}")
    return out


def api_check(body: Optional[dict] = None) -> list[dict[str, Any]]:
    """Recorre la API con el servidor real del simulador en un hilo (``MemoryPublisher``, sin nube)."""
    from simulator.app import Simulator, start_http

    body = body or dict(client.DEFAULTS)
    config = load_config(env={"SIM_SEED": "7", "SIM_CHAOS_ENABLED": "false"})
    sim = Simulator(config, MemoryPublisher(), MemoryStateStore())
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    box: dict[str, Any] = {}

    async def boot() -> None:
        await sim.start()
        box["server"] = await start_http(sim, "127.0.0.1", 0)
        box["port"] = box["server"].sockets[0].getsockname()[1]
        ready.set()

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(boot())
        loop.run_forever()

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    if not ready.wait(30):
        raise client.ClientError("el servidor local no arrancó en 30 s")
    url = f"http://127.0.0.1:{box['port']}"
    steps: list[dict[str, Any]] = []

    def step(name: str, reply: client.Reply, want: int, extra: bool = True) -> client.Reply:
        steps.append({"step": name, "status": reply.status, "want": want, "ok": reply.status == want and extra,
                      "body": reply.body})
        return reply

    try:
        step("GET /health", client.call(url, "GET", "/health"), 200)
        r = client.call(url, "GET")
        step("GET /control/scenario (sin escenario)", r, 200, r.body.get("active") is None)
        r = client.call(url, "POST", body={**body, "brand": "cocacola", "multiplier": 50})
        step("POST inválido (marca y multiplicador)", r, 400, len(r.body.get("problems") or []) == 2)
        r = client.call(url, "POST", body=body)
        step("POST válido", r, 201, (r.body.get("started") or {}).get("brand") == body["brand"])
        step("POST con otro activo", client.call(url, "POST", body=body), 409)
        r = client.call(url, "GET")
        step("GET con escenario activo", r, 200, (r.body.get("active") or {}).get("name", "").startswith("demo/"))
        r = client.call(url, "GET", "/health")
        step("GET /health muestra el escenario", r, 200, bool(r.body.get("scenario")))
        r = client.call(url, "DELETE")
        step("DELETE lo termina", r, 200, (r.body.get("stopped") or {}).get("reason") == "stopped")
        r = client.call(url, "GET")
        step("GET después del DELETE", r, 200, r.body.get("active") is None and (r.body.get("last") or {}).get("reason") == "stopped")
        step("ruta desconocida", client.call(url, "GET", "/nada"), 404)
    finally:
        async def shutdown() -> None:
            box["server"].close()
            await box["server"].wait_closed()
            await sim.stop()

        asyncio.run_coroutine_threadsafe(shutdown(), loop).result(30)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
    return steps
