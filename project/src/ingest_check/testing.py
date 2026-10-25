"""Dobles de prueba del verificador: reloj falso, backend falso y envelopes sintéticos.

Sirven para las pruebas de ``ingest_check`` y para el consumidor local del
Lote 1.5. No se usan en producción.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Callable

T0 = 1_792_500_000.0  # 2026-10-20 aprox., UTC


def event_id(source: str, seq: int, seed: int = 7, origin: str = "stream") -> str:
    """Misma fórmula de §3.6."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def make_envelope(source: str, seq: int, seed: int = 7, chaos: list[str] | None = None,
                  kind: str = "comment") -> dict:
    """Envelope con la forma de §5.2 (valores sintéticos mínimos)."""
    return {
        "schema_version": "1.0",
        "event_id": event_id(source, seq, seed),
        "origin": "stream",
        "source": source,
        "generator": "comment-gen@1.0",
        "seq": seq,
        "event_time": "2026-10-20T23:00:00.000Z",
        "ingest_time": "2026-10-20T23:00:01.000Z",
        "author": {"user_id": "u_000001", "handle": "u/prueba", "followers": 10},
        "text": "Me encanta Postobón",
        "lang": "es",
        "brands": ["postobon"],
        "hashtags": [],
        "payload": {"kind": kind},
        "sim": {"label_sentiment": "positive", "scenario": None, "chaos": list(chaos or [])},
    }


def attributes_for(env: dict) -> dict[str, str]:
    return {
        "event_id": env["event_id"],
        "source": env["source"],
        "origin": env["origin"],
        "schema_version": env["schema_version"],
    }


@dataclass
class FakeMessage:
    published_at: float
    source: str
    data: bytes
    attributes: dict[str, str]
    message_id: str


def message(source: str, seq: int, published_at: float, message_id: str | None = None,
            chaos: list[str] | None = None, seed: int = 7) -> FakeMessage:
    env = make_envelope(source, seq, seed=seed, chaos=chaos)
    return FakeMessage(
        published_at=published_at,
        source=source,
        data=json.dumps(env).encode("utf-8"),
        attributes=attributes_for(env),
        message_id=message_id or f"m-{source}-{seq}-{published_at:.3f}",
    )


def stream(source: str, start_seq: int, count: int, t_start: float, interval: float = 2.0,
           seed: int = 7) -> list[FakeMessage]:
    """``count`` mensajes consecutivos cada ``interval`` s (0.5 msg/s por defecto)."""
    return [message(source, start_seq + i, t_start + i * interval, seed=seed) for i in range(count)]


class FakeClock:
    def __init__(self, start: float = T0) -> None:
        self.now = start
        self.listeners: list[Callable[[float], None]] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        for fn in self.listeners:
            fn(self.now)


@dataclass
class FakeBackend:
    """Entrega los mensajes programados cuyo ``published_at`` ya pasó en el reloj falso."""

    clock: FakeClock
    messages: list[FakeMessage] = field(default_factory=list)
    latency: float = 0.1
    setup_called: bool = False
    torn_down: bool = False
    _cb: Callable | None = None

    def __post_init__(self) -> None:
        self.messages = sorted(self.messages, key=lambda m: m.published_at)
        self.clock.listeners.append(self._deliver)

    def setup(self) -> dict[str, str]:
        self.setup_called = True
        return {}

    def start(self, on_message) -> None:
        self._cb = on_message
        self._deliver(self.clock.now)

    def _deliver(self, now: float) -> None:
        if self._cb is None:
            return
        while self.messages and self.messages[0].published_at + self.latency <= now:
            m = self.messages.pop(0)
            self._cb(m.source, m.data, m.attributes, m.message_id, m.published_at + self.latency, m.published_at)

    def stop(self) -> None:
        self._cb = None

    def teardown(self) -> None:
        self.torn_down = True
