"""Publicación a Pub/Sub y bucle de emisión asyncio por fuente (§3.5, §5.3).

- ``PubSubPublisher``: ``google-cloud-pubsub``. Respeta ``PUBSUB_EMULATOR_HOST``
  (emulador, Lote 1.5) sin cambios de código.
- ``StdoutPublisher``: una línea JSON por mensaje, para correr en local sin nube.
- ``MemoryPublisher``: para pruebas.

``SourceWorker`` es el bucle de §3.5: lee el cursor, emite ``seq`` a ``seq``, guarda el
cursor cada ``checkpoint_every`` mensajes y al detenerse, y duerme 1–3 s entre mensajes (dividido
por ``multiplier`` mientras hay un escenario de demo para su fuente, Lote 6.3).
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional, Protocol

from simulator.factory import EventFactory, Scenario
from simulator.state import Cursor, StateStore, resume_seq

DEFAULT_TOPIC_TEMPLATE = "ta-raw-{source}"


def log(event: str, **fields) -> None:
    """Log JSON a stdout (§5.1)."""
    rec = {"severity": fields.pop("severity", "INFO"), "event": event, "component": "stream-simulator", **fields}
    sys.stdout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()


class Control(Protocol):
    """Lo que el bucle usa del escenario de demo (``simulator.scenario.ScenarioControl``)."""

    def pick(self, source: str, seq: int) -> Optional[Scenario]: ...
    def speed(self, source: str) -> float: ...


class Publisher(Protocol):
    async def publish(self, topic: str, data: bytes, attributes: dict) -> str: ...
    async def close(self) -> None: ...


class MemoryPublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, dict]] = []

    async def publish(self, topic: str, data: bytes, attributes: dict) -> str:
        self.sent.append((topic, data, dict(attributes)))
        return str(len(self.sent))

    async def close(self) -> None:
        return None


class StdoutPublisher:
    """Imprime ``{"topic", "attributes", "data"}``; ``data`` es el JSON si es válido, si no base64."""

    async def publish(self, topic: str, data: bytes, attributes: dict) -> str:
        try:
            body = json.loads(data)
            key = "data"
        except (ValueError, UnicodeDecodeError):
            body, key = base64.b64encode(data).decode(), "data_b64"
        sys.stdout.write(json.dumps({"topic": topic, "attributes": attributes, key: body}, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        return "stdout"

    async def close(self) -> None:
        return None


class PubSubPublisher:
    def __init__(self, project: str, client=None) -> None:
        if not project:
            raise ValueError("falta GCP_PROJECT para publicar en Pub/Sub")
        if client is None:
            from google.cloud import pubsub_v1  # import diferido

            client = pubsub_v1.PublisherClient()
        self.project = project
        self.client = client

    def topic_path(self, topic: str) -> str:
        return topic if topic.startswith("projects/") else f"projects/{self.project}/topics/{topic}"

    async def publish(self, topic: str, data: bytes, attributes: dict) -> str:
        fut = self.client.publish(self.topic_path(topic), data, **{k: str(v) for k, v in attributes.items()})
        return await asyncio.wrap_future(fut)

    async def close(self) -> None:
        stop = getattr(self.client, "stop", None)
        if callable(stop):
            await asyncio.to_thread(stop)


def publisher_from_env(env: dict | None = None) -> Publisher:
    """``SIM_PUBLISHER`` = ``pubsub`` (por defecto) | ``stdout`` | ``memory``."""
    env = dict(os.environ if env is None else env)
    kind = (env.get("SIM_PUBLISHER") or "pubsub").lower()
    if kind == "pubsub":
        return PubSubPublisher(env.get("GCP_PROJECT", ""))
    if kind == "stdout":
        return StdoutPublisher()
    if kind == "memory":
        return MemoryPublisher()
    raise ValueError(f"SIM_PUBLISHER desconocido: {kind!r}")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WorkerStats:
    seq: int = 0
    events: int = 0
    messages: int = 0
    errors: int = 0
    chaos: dict = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)
    last_publish_at: Optional[float] = None


class SourceWorker:
    """Emite los eventos de una fuente, en orden, desde el cursor guardado."""

    def __init__(
        self,
        source: str,
        factory: EventFactory,
        publisher: Publisher,
        store: StateStore,
        *,
        topic_template: str = DEFAULT_TOPIC_TEMPLATE,
        clock: Callable[[], datetime] = _utcnow,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        control: Optional[Control] = None,
        max_events: Optional[int] = None,
    ) -> None:
        self.source = source
        self.factory = factory
        self.publisher = publisher
        self.store = store
        self.topic = topic_template.format(source=source)
        self.clock = clock
        self._sleep = sleep
        self.control = control
        self.max_events = max_events
        self.config = factory.config
        self.stats = WorkerStats()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def checkpoint(self) -> None:
        self.store.save(self.source, Cursor(seed=self.factory.seed, seq=self.stats.seq))

    def _interval(self, seq: int) -> float:
        lo, hi = self.config.interval_s
        pause = random.Random(f"{self.factory.seed}|{self.source}|{seq}|sleep").uniform(lo, hi)
        return pause / self.control.speed(self.source) if self.control is not None else pause

    async def _wait(self, seconds: float) -> None:
        if self._sleep is not None:
            await self._sleep(seconds)
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def emit_one(self) -> None:
        seq = self.stats.seq
        scenario = self.control.pick(self.source, seq) if self.control is not None else None
        emission = self.factory.make(self.source, seq, self.clock(), scenario=scenario)
        for msg in emission.messages:
            await self.publisher.publish(self.topic, msg.data, msg.attributes)
            self.stats.messages += 1
        for c in emission.chaos:
            self.stats.chaos[c] = self.stats.chaos.get(c, 0) + 1
        self.stats.events += 1
        self.stats.seq = seq + 1
        self.stats.last_publish_at = time.time()
        if self.stats.seq % self.config.checkpoint_every == 0:
            self.checkpoint()

    async def run(self) -> None:
        self.stats.seq = resume_seq(self.store, self.source, self.factory.seed, log=log)
        log("worker_start", source=self.source, seq=self.stats.seq, topic=self.topic, seed=self.factory.seed)
        backoff = 1.0
        try:
            while not self._stop.is_set():
                if self.max_events is not None and self.stats.events >= self.max_events:
                    break
                try:
                    await self.emit_one()
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # error de publicación: reintenta el mismo seq
                    self.stats.errors += 1
                    log("publish_error", severity="ERROR", source=self.source, seq=self.stats.seq, error=repr(exc))
                    await self._wait(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue
                await self._wait(self._interval(self.stats.seq))
        finally:
            try:
                self.checkpoint()
                log("worker_stop", source=self.source, seq=self.stats.seq, events=self.stats.events)
            except Exception as exc:  # no tapar la causa original
                log("checkpoint_error", severity="ERROR", source=self.source, error=repr(exc))
