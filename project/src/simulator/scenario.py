"""Escenarios de demo: ``/control/scenario`` (Lote 6.3, HU-040, §3.7 y §3.29).

Un escenario sube la cadencia de una fuente (o de las tres) por ``multiplier`` durante
``duration_s`` segundos y fuerza marca y sentimiento **en lo que se suma**: cada evento lleva el
escenario con probabilidad ``1 - 1/multiplier`` y el resto sigue la mezcla normal. Así la ola se
suma al tráfico de siempre (las otras marcas no caen y no dan alertas falsas de ``volume_down``) y
cada evento forzado queda marcado en ``sim.scenario`` (ADR-33).

- Qué evento lleva el escenario lo decide ``Random("{seed}|{source}|{seq}|scenario")``: no toca
  el ``Random`` del evento ni el del caos.
- Un solo escenario a la vez; termina solo al cumplir ``duration_s`` o con ``DELETE``.
- El estado vive en memoria: si Cloud Run reinicia el contenedor, el escenario termina.

Cuerpo de ``POST`` (JSON): ``brand`` (clave, nombre o alias de ``brands.yaml``), ``sentiment``
(``positive`` | ``negative`` | ``neutral``), ``multiplier`` (2–10), ``duration_s`` (60–3600),
``source`` (opcional; sin ella, las tres) y ``name`` (opcional; por defecto
``demo/{marca}-{sentimiento}/{AAAAMMDDTHHMMSSZ}``).
"""

from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from simulator.factory import Scenario
from simulator.generator.brands import Brands
from simulator.sim_config import SOURCES

SENTIMENTS = ("positive", "negative", "neutral")
MULTIPLIER_MIN, MULTIPLIER_MAX = 2.0, 10.0
DURATION_MIN_S, DURATION_MAX_S = 60, 3600
FIELDS = ("brand", "sentiment", "multiplier", "duration_s", "source", "name")
REQUIRED = ("brand", "sentiment", "multiplier", "duration_s")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,79}$")
#: Tamaño máximo del cuerpo de la petición.
MAX_BODY_BYTES = 4096


class ScenarioError(ValueError):
    """Petición inválida; ``problems`` dice qué campo falla."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class ScenarioConflict(RuntimeError):
    """Ya hay un escenario activo."""


def _iso(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class ActiveScenario:
    name: str
    brand: str
    sentiment: str
    multiplier: float
    duration_s: int
    source: Optional[str]
    started_at: float
    ends_at: float

    @property
    def share(self) -> float:
        """Fracción de eventos que lleva el escenario (la parte que se suma)."""
        return 1.0 - 1.0 / self.multiplier

    def applies_to(self, source: str) -> bool:
        return self.source is None or self.source == source

    def scenario(self) -> Scenario:
        return Scenario(name=self.name, brand=self.brand, sentiment=self.sentiment, source=self.source)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "brand": self.brand,
            "sentiment": self.sentiment,
            "multiplier": self.multiplier,
            "duration_s": self.duration_s,
            "source": self.source,
            "share": round(self.share, 4),
            "started_at": _iso(self.started_at),
            "ends_at": _iso(self.ends_at),
        }


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def parse_request(body: Any, brands: Brands, now: float) -> ActiveScenario:
    """Valida el cuerpo de ``POST /control/scenario``; ``ScenarioError`` con todos los problemas."""
    if not isinstance(body, dict):
        raise ScenarioError(["el cuerpo debe ser un objeto JSON"])
    problems = [f"campo desconocido: {k}" for k in sorted(set(body) - set(FIELDS))]
    problems += [f"falta {k}" for k in REQUIRED if body.get(k) is None]
    brand = None
    if body.get("brand") is not None:
        try:
            brand = brands.canonical(str(body["brand"]))
        except ValueError:
            problems.append(f"brand desconocida: {body['brand']!r} (válidas: {', '.join(brands.keys)})")
    sentiment = body.get("sentiment")
    if sentiment is not None and sentiment not in SENTIMENTS:
        problems.append(f"sentiment inválido: {sentiment!r} (válidos: {', '.join(SENTIMENTS)})")
    multiplier = _number(body.get("multiplier"))
    if body.get("multiplier") is not None and (multiplier is None or not MULTIPLIER_MIN <= multiplier <= MULTIPLIER_MAX):
        problems.append(f"multiplier debe ser un número entre {MULTIPLIER_MIN:g} y {MULTIPLIER_MAX:g}")
    duration = _number(body.get("duration_s"))
    if body.get("duration_s") is not None and (
        duration is None or duration != int(duration) or not DURATION_MIN_S <= duration <= DURATION_MAX_S
    ):
        problems.append(f"duration_s debe ser un entero entre {DURATION_MIN_S} y {DURATION_MAX_S}")
    source = body.get("source")
    if source is not None and source not in SOURCES:
        problems.append(f"source inválida: {source!r} (válidas: {', '.join(SOURCES)})")
    name = body.get("name")
    if name is not None and not (isinstance(name, str) and NAME_RE.match(name)):
        problems.append("name debe tener 1–80 caracteres de [A-Za-z0-9._/-] y empezar por letra o dígito")
    if problems:
        raise ScenarioError(problems)
    if name is None:
        name = f"demo/{brand}-{sentiment}/{datetime.fromtimestamp(now, timezone.utc):%Y%m%dT%H%M%SZ}"
    return ActiveScenario(
        name=name,
        brand=brand,
        sentiment=sentiment,
        multiplier=multiplier,
        duration_s=int(duration),
        source=source,
        started_at=now,
        ends_at=now + int(duration),
    )


class ScenarioControl:
    """Estado del escenario de demo, compartido por los workers del simulador."""

    def __init__(
        self,
        brands: Brands,
        seed: int,
        *,
        clock: Callable[[], float] = time.time,
        log: Callable[..., None] = lambda *_a, **_k: None,
    ) -> None:
        self.brands = brands
        self.seed = seed
        self.clock = clock
        self.log = log
        self._active: Optional[ActiveScenario] = None
        self._events: dict[str, int] = {}
        self.last: Optional[dict[str, Any]] = None

    def current(self) -> Optional[ActiveScenario]:
        """El escenario activo; si ya venció, lo cierra (``reason = expired``)."""
        if self._active is not None and self.clock() >= self._active.ends_at:
            self._finish("expired")
        return self._active

    def start(self, body: Any) -> ActiveScenario:
        if self.current() is not None:
            raise ScenarioConflict(self._active.name)
        active = parse_request(body, self.brands, self.clock())
        self._active, self._events = active, {}
        self.log("scenario_start", **active.to_dict())
        return active

    def stop(self) -> Optional[dict[str, Any]]:
        """Termina el escenario activo (``reason = stopped``); ``None`` si no había."""
        if self.current() is None:
            return None
        return self._finish("stopped")

    def _finish(self, reason: str) -> dict[str, Any]:
        active = self._active
        ended = min(self.clock(), active.ends_at)
        self.last = {**active.to_dict(), "ended_at": _iso(ended), "reason": reason, "events": dict(self._events)}
        self._active = None
        self.log("scenario_end", **self.last)
        return self.last

    def status(self) -> dict[str, Any]:
        active = self.current()
        body: dict[str, Any] = {"active": None, "last": self.last}
        if active is not None:
            left = max(0.0, active.ends_at - self.clock())
            body["active"] = {**active.to_dict(), "remaining_s": round(left, 1), "events": dict(self._events)}
        return body

    # -- lo que usan los workers ---------------------------------------------------------

    def speed(self, source: str) -> float:
        """Cuántas veces más rápido emite ``source`` ahora (1 sin escenario)."""
        active = self.current()
        return active.multiplier if active is not None and active.applies_to(source) else 1.0

    def pick(self, source: str, seq: int) -> Optional[Scenario]:
        """El escenario para el evento ``seq`` de ``source``, o ``None`` si sigue la mezcla normal."""
        active = self.current()
        if active is None or not active.applies_to(source):
            return None
        if random.Random(f"{self.seed}|{source}|{seq}|scenario").random() >= active.share:
            return None
        self._events[source] = self._events.get(source, 0) + 1
        return active.scenario()
