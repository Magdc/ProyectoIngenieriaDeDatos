"""Caos controlado (§3.9, HU-015).

Los defectos se deciden con un ``Random`` propio de cada evento
(``"{seed}|{source}|{seq}|chaos"``), separado del que produce el texto: encender o
apagar el caos no cambia el texto, la marca ni el ``event_id`` de ningún evento.

Defectos (valores de ``sim.chaos``, §5.2):

- ``duplicate``: se re-publica el mismo ``event_id`` (la copia lleva la marca).
- ``late``: ``event_time`` se adelanta entre 2 y 30 min (configurable).
- ``null_fields``: ``lang`` y/o ``author.followers`` en ``null``.
- ``malformed``: JSON roto o falta un campo requerido. Debe acabar en la DLQ.
"""

from __future__ import annotations

import copy
import json
import random
from dataclasses import dataclass, field
from datetime import timedelta

from simulator.sim_config import ChaosConfig

REQUIRED_TO_DROP = ("event_time", "text", "source", "author", "payload")


def chaos_rng(seed: int, source: str, seq: int) -> random.Random:
    return random.Random(f"{seed}|{source}|{seq}|chaos")


@dataclass(frozen=True)
class ChaosPlan:
    """Qué defectos le tocan a un evento. Todo en ``False`` = evento limpio."""

    duplicate: bool = False
    late: bool = False
    late_minutes: float = 0.0
    null_lang: bool = False
    null_followers: bool = False
    malformed: bool = False
    malformed_mode: str = ""  # "broken_json" | "missing:<campo>"
    labels: tuple[str, ...] = field(default=())

    @property
    def any(self) -> bool:
        return bool(self.labels)


CLEAN = ChaosPlan()


def plan(config: ChaosConfig, seed: int, source: str, seq: int) -> ChaosPlan:
    """Decide los defectos del evento ``seq``. Con el caos apagado devuelve ``CLEAN``."""
    if not config.enabled:
        return CLEAN
    rng = chaos_rng(seed, source, seq)
    r = config.rates
    # Extracciones en orden fijo: una por defecto, más sus parámetros.
    d_dup, d_late, d_null, d_mal = rng.random(), rng.random(), rng.random(), rng.random()
    late_min = rng.uniform(*config.late_minutes)
    null_pick = rng.randrange(3)  # 0 lang, 1 followers, 2 ambos
    mal_pick = rng.randrange(len(REQUIRED_TO_DROP) + 1)

    malformed = d_mal < r.get("malformed", 0.0)
    if malformed:
        # Un malformado no se combina con otros defectos: solo debe probar la DLQ.
        mode = "broken_json" if mal_pick == len(REQUIRED_TO_DROP) else f"missing:{REQUIRED_TO_DROP[mal_pick]}"
        return ChaosPlan(malformed=True, malformed_mode=mode, labels=("malformed",))

    duplicate = d_dup < r.get("duplicate", 0.0)
    late = d_late < r.get("late", 0.0)
    nulls = d_null < r.get("null_fields", 0.0)
    labels = []
    if duplicate:
        labels.append("duplicate")
    if late:
        labels.append("late")
    if nulls:
        labels.append("null_fields")
    return ChaosPlan(
        duplicate=duplicate,
        late=late,
        late_minutes=late_min if late else 0.0,
        null_lang=nulls and null_pick in (0, 2),
        null_followers=nulls and null_pick in (1, 2),
        labels=tuple(labels),
    )


def apply_to_envelope(env: dict, p: ChaosPlan, *, event_dt, format_ts) -> dict:
    """Aplica ``late`` y ``null_fields`` y registra los defectos en ``sim.chaos``.

    ``duplicate`` no cambia el original: la copia la arma :func:`duplicate_of`.
    ``malformed`` se aplica al serializar (:func:`malformed_bytes`).
    """
    if not p.any:
        return env
    out = copy.deepcopy(env)
    if p.late:
        out["event_time"] = format_ts(event_dt - timedelta(minutes=p.late_minutes))
    if p.null_lang:
        out["lang"] = None
    if p.null_followers:
        out["author"]["followers"] = None
    # El original lleva los defectos que lo afectan; "duplicate" solo va en la copia.
    out["sim"]["chaos"] = [c for c in p.labels if c != "duplicate"]
    return out


def duplicate_of(env: dict) -> dict:
    """Copia con el mismo ``event_id`` marcada como ``duplicate``."""
    dup = copy.deepcopy(env)
    chaos = list(dup["sim"].get("chaos") or [])
    if "duplicate" not in chaos:
        chaos.append("duplicate")
    dup["sim"]["chaos"] = chaos
    return dup


def malformed_bytes(env: dict, mode: str) -> bytes:
    """Cuerpo defectuoso: JSON truncado o envelope sin un campo requerido."""
    bad = copy.deepcopy(env)
    bad["sim"]["chaos"] = ["malformed"]
    if mode.startswith("missing:"):
        bad.pop(mode.split(":", 1)[1], None)
        return json.dumps(bad, ensure_ascii=False).encode("utf-8")
    raw = json.dumps(bad, ensure_ascii=False).encode("utf-8")
    return raw[: max(1, len(raw) // 2)]  # JSON roto
