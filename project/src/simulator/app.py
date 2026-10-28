"""Punto de entrada del simulador ``ta-stream-simulator`` (§3.1, §3.5).

Un proceso, tres workers asyncio (uno por fuente) y un servidor HTTP mínimo:

- ``GET /healthz`` → 200 con el ``seq`` y los contadores de cada fuente (requisito de
  Cloud Run). Responde 503 si algún worker terminó por error. ``GET /health`` es lo mismo y sí
  llega desde fuera (el *frontend* de Cloud Run se queda con ``/healthz``, §3.1).
- ``/control/scenario`` (Lote 6.3, §3.29): ``POST`` inicia un escenario de demo (201; 400 si el
  cuerpo es inválido, 409 si ya hay uno), ``GET`` dice cuál está activo y cómo terminó el último,
  ``DELETE`` lo termina antes de tiempo. En Cloud Run lo protege el ID token (no es público).

Variables de entorno (además de las de §5.1):

| Variable | Por defecto | Uso |
|---|---|---|
| ``PORT`` | ``8080`` | Puerto HTTP (lo fija Cloud Run). |
| ``SIM_SOURCES`` | ``reddit,facebook,instagram`` | Fuentes a emitir. |
| ``SIM_PUBLISHER`` | ``pubsub`` | ``pubsub`` · ``stdout`` · ``memory``. |
| ``SIM_STATE_BACKEND`` | ``gcs`` | ``gcs`` · ``local`` · ``memory``. |
| ``SIM_STATE_BUCKET`` | ``{GCP_PROJECT}-ta-datasets`` | Bucket del cursor. |
| ``SIM_STATE_DIR`` | ``.sim-state`` | Carpeta del cursor con ``local``. |
| ``SIM_TOPIC_TEMPLATE`` | ``ta-raw-{source}`` | Nombre del topic. |
| ``SIM_CONFIG`` | ``config/sim.yaml`` | Otra configuración. |
| ``SIM_MAX_EVENTS`` | (sin límite) | Corta cada worker tras N eventos (pruebas locales). |

Uso local sin nube (desde ``project/``)::

    SIM_PUBLISHER=stdout SIM_STATE_BACKEND=local SIM_MAX_EVENTS=5 python -m simulator.app

(con ``src`` en ``PYTHONPATH``).
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from typing import Optional

from simulator.factory import EventFactory
from simulator.publisher import DEFAULT_TOPIC_TEMPLATE, Publisher, SourceWorker, log, publisher_from_env
from simulator.scenario import MAX_BODY_BYTES, ScenarioConflict, ScenarioControl, ScenarioError
from simulator.sim_config import SOURCES, SimConfig, load_config
from simulator.state import StateStore, store_from_env


def parse_sources(raw: Optional[str]) -> tuple[str, ...]:
    if not raw:
        return SOURCES
    out = tuple(s.strip().lower() for s in raw.split(",") if s.strip())
    bad = [s for s in out if s not in SOURCES]
    if bad or not out:
        raise ValueError(f"SIM_SOURCES inválido: {raw!r} (válidas: {', '.join(SOURCES)})")
    return out


class Simulator:
    def __init__(
        self,
        config: SimConfig,
        publisher: Publisher,
        store: StateStore,
        *,
        sources: tuple[str, ...] = SOURCES,
        topic_template: str = DEFAULT_TOPIC_TEMPLATE,
        max_events: Optional[int] = None,
    ) -> None:
        self.config = config
        self.factory = EventFactory(config)
        self.publisher = publisher
        self.control = ScenarioControl(self.factory.brands, self.factory.seed, log=log)
        self.workers = {
            s: SourceWorker(s, self.factory, publisher, store, topic_template=topic_template, max_events=max_events,
                            control=self.control)
            for s in sources
        }
        self.tasks: dict[str, asyncio.Task] = {}
        self.failed: dict[str, str] = {}

    def health(self) -> tuple[int, dict]:
        body = {
            "status": "ok" if not self.failed else "degraded",
            "seed": self.factory.seed,
            "chaos_enabled": self.config.chaos.enabled,
            "scenario": (self.control.status()["active"] or {}).get("name"),
            "sources": {
                s: {
                    "seq": w.stats.seq,
                    "events": w.stats.events,
                    "messages": w.stats.messages,
                    "errors": w.stats.errors,
                    "chaos": w.stats.chaos,
                    "last_publish_at": w.stats.last_publish_at,
                }
                for s, w in self.workers.items()
            },
        }
        if self.failed:
            body["failed"] = self.failed
        return (200 if not self.failed else 503), body

    async def _run_worker(self, source: str, worker: SourceWorker) -> None:
        try:
            await worker.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failed[source] = repr(exc)
            log("worker_failed", severity="ERROR", source=source, error=repr(exc))

    async def start(self) -> None:
        for s, w in self.workers.items():
            self.tasks[s] = asyncio.create_task(self._run_worker(s, w), name=f"worker-{s}")

    async def wait(self) -> None:
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

    async def stop(self) -> None:
        for w in self.workers.values():
            w.stop()
        await self.wait()
        await self.publisher.close()


# --- HTTP mínimo (sin dependencias) ---------------------------------------------------


_REASONS = {200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
            409: "Conflict", 413: "Payload Too Large", 503: "Service Unavailable"}


def _response(status: int, body: dict) -> bytes:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    head = (
        f"HTTP/1.1 {status} {_REASONS.get(status, 'OK')}\r\n"
        "Content-Type: application/json; charset=utf-8\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    return head + payload


def scenario_route(sim: Simulator, method: str, body: bytes) -> tuple[int, dict]:
    """``/control/scenario`` (§3.29.1)."""
    if method == "GET":
        return 200, sim.control.status()
    if method == "DELETE":
        return 200, {"stopped": sim.control.stop()}
    if method != "POST":
        return 405, {"error": "solo GET, POST o DELETE"}
    try:
        data = json.loads(body.decode("utf-8")) if body.strip() else None
    except (ValueError, UnicodeDecodeError):
        return 400, {"error": "el cuerpo no es JSON válido"}
    try:
        active = sim.control.start(data)
    except ScenarioError as exc:
        log("scenario_rejected", severity="WARNING", problems=exc.problems)
        return 400, {"error": "escenario inválido", "problems": exc.problems}
    except ScenarioConflict:
        return 409, {"error": "ya hay un escenario activo (DELETE /control/scenario para terminarlo)",
                     "active": sim.control.status()["active"]}
    return 201, {"started": active.to_dict()}


def route(sim: Simulator, method: str, path: str, body: bytes = b"") -> tuple[int, dict]:
    path = path.split("?", 1)[0]
    if path in ("/healthz", "/health"):
        if method != "GET":
            return 405, {"error": "solo GET"}
        return sim.health()
    if path == "/control/scenario":
        return scenario_route(sim, method, body)
    return 404, {"error": "no encontrado"}


async def _handle(sim: Simulator, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=10)
        parts = line.decode("latin-1").split()
        length = 0
        while True:  # cabeceras: solo interesa Content-Length
            h = await asyncio.wait_for(reader.readline(), timeout=10)
            if h in (b"\r\n", b"\n", b""):
                break
            key, _, value = h.decode("latin-1").partition(":")
            if key.strip().lower() == "content-length":
                length = int(value.strip()) if value.strip().isdigit() else -1
        if len(parts) < 2 or length < 0:
            status, body = 400, {"error": "petición inválida"}
        elif length > MAX_BODY_BYTES:
            status, body = 413, {"error": f"el cuerpo pasa de {MAX_BODY_BYTES} bytes"}
        else:
            data = await asyncio.wait_for(reader.readexactly(length), timeout=10) if length else b""
            status, body = route(sim, parts[0].upper(), parts[1], data)
        writer.write(_response(status, body))
        await writer.drain()
    except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def start_http(sim: Simulator, host: str, port: int) -> asyncio.AbstractServer:
    """Servidor HTTP del simulador (``port = 0`` elige uno libre; lo usan las pruebas y ``demo``)."""
    return await asyncio.start_server(lambda r, w: _handle(sim, r, w), host=host, port=port)


async def serve(env: Optional[dict] = None) -> None:
    env = dict(os.environ if env is None else env)
    config = load_config(env=env)
    max_events = int(env["SIM_MAX_EVENTS"]) if env.get("SIM_MAX_EVENTS") else None
    sim = Simulator(
        config,
        publisher_from_env(env),
        store_from_env(env),
        sources=parse_sources(env.get("SIM_SOURCES")),
        topic_template=env.get("SIM_TOPIC_TEMPLATE") or DEFAULT_TOPIC_TEMPLATE,
        max_events=max_events,
    )
    port = int(env.get("PORT") or 8080)
    server = await start_http(sim, "0.0.0.0", port)
    log("simulator_start", port=port, seed=config.seed, chaos_enabled=config.chaos.enabled, sources=list(sim.workers))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

    await sim.start()
    done = asyncio.create_task(sim.wait())
    stopper = asyncio.create_task(stop.wait())
    await asyncio.wait({done, stopper}, return_when=asyncio.FIRST_COMPLETED)
    log("simulator_stopping")
    await sim.stop()  # guarda los cursores
    stopper.cancel()
    server.close()
    await server.wait_closed()
    log("simulator_stopped")


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
