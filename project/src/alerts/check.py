"""Comprobaciones de las alertas (Lote 6.2, HU-039): alerta sintética, ola de prueba y lectura de resultados.

Funciones puras que usan ``alerts.cli`` y ``ops.validate62`` (no van en la imagen del notificador).

* **Alerta sintética** (``origin = check``): una alerta de volumen bien formada que se publica directo en
  ``ta-alerts`` para probar notificador → log → política → correo sin Dataflow.
* **Ola de prueba**: ``count`` comentarios de una marca en una red, en pocos segundos, armados con el
  ``EventFactory`` del simulador (semilla reservada ``BURST_SEED``, caos apagado, escenario forzado con
  la marca y el sentimiento) y ``generator = ta-verify@6.2``: Dataflow los procesa como cualquier
  comentario (cuentan en ``trend_windows`` y disparan la alerta real) y Trusted los descarta, como a todo
  ``ta-verify@*`` (§3.21), así que no llegan a Curated ni al tablero. ``sim.scenario`` los marca.
* Lectura de las notificaciones del canal de Pub/Sub de Cloud Monitoring (``ta-alerts-incidents``) y
  comprobación del *cooldown* sobre los mensajes vistos en ``ta-alerts``.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from common.alert_message import build_alert, format_ts, to_ms

BURST_SEED = 990062
BURST_GENERATOR = "ta-verify@6.2"
BURST_SCENARIO = "alert-check-6.2"
DEFAULT_BRAND = "postobon"
DEFAULT_SOURCE = "instagram"
DEFAULT_COUNT = 150
COOLDOWN_SECONDS = 900
WINDOW_MS = 300_000
PERIOD_MS = 60_000


# --------------------------------------------------------------------------- alerta sintética

def synthetic_alert(brand: str = DEFAULT_BRAND, source: str = DEFAULT_SOURCE, now_ms: int | None = None) -> dict[str, Any]:
    """Alerta ``volume_up`` de prueba sobre la última ventana completa (``origin = check``)."""
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    end = now_ms - now_ms % PERIOD_MS
    return build_alert(metric="volume", brand=brand, source=source, window_start_ms=end - WINDOW_MS, window_end_ms=end,
                       value=120, baseline_mean=40.0, baseline_std=6.0, zscore=13.333333, z_threshold=3.0,
                       baseline_windows=60, n_posts=120, avg_sentiment=-0.41, n_negative=70, n_positive=10,
                       detected_ms=now_ms, cooldown_seconds=COOLDOWN_SECONDS, origin="check")


# --------------------------------------------------------------------------- ola de prueba

def burst_seq_base(now: datetime) -> int:
    """``seq`` inicial de la ola: los segundos UNIX × 1000 (cada corrida usa ids distintos)."""
    return int(now.timestamp()) * 1000


def burst_envelopes(run_id: str, brand: str, source: str, count: int, now: datetime,
                    sentiment: str | None = "negative") -> list[dict[str, Any]]:
    """Envelopes v1.0 de la ola (``count`` comentarios de ``brand`` en ``source``)."""
    from simulator.factory import EventFactory, Scenario
    from simulator.sim_config import load_config

    if count <= 0:
        raise ValueError("count debe ser > 0")
    config = load_config(env={"SIM_SEED": str(BURST_SEED), "SIM_CHAOS_ENABLED": "false"})
    factory = EventFactory(config)
    scenario = Scenario(name=f"{BURST_SCENARIO}/{run_id}", brand=brand, sentiment=sentiment, source=source)
    base = burst_seq_base(now)
    out = []
    for i in range(count):
        env = json.loads(json.dumps(factory.make(source, base + i, now, scenario=scenario).envelope))
        env["generator"] = BURST_GENERATOR
        if not env["brands"] or env["brands"][0] != brand:
            env["brands"] = [brand] + [b for b in env["brands"] if b != brand]
        out.append(env)
    return out


def burst_problems(envelopes: list[dict[str, Any]], brand: str, source: str) -> list[str]:
    """Que la ola sea lo que se pidió: marca principal, red, generador, escenario e ids únicos."""
    out = []
    if len({e["event_id"] for e in envelopes}) != len(envelopes):
        out.append("event_id repetidos")
    for e in envelopes:
        if e["brands"][:1] != [brand] or e["source"] != source:
            out.append(f"{e['event_id']}: marca {e['brands'][:1]} / red {e['source']}")
        if e["generator"] != BURST_GENERATOR or not str(e["sim"]["scenario"] or "").startswith(BURST_SCENARIO):
            out.append(f"{e['event_id']}: generador {e['generator']} / escenario {e['sim']['scenario']}")
    return out[:10]


# --------------------------------------------------------------------------- resultados

def parse_incident(data: bytes | str) -> dict[str, Any] | None:
    """Notificación del canal de Pub/Sub de Cloud Monitoring → ``{incident_id, policy, state, started_at, …}``."""
    try:
        doc = json.loads(data if isinstance(data, str) else data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    inc = doc.get("incident") if isinstance(doc, dict) else None
    if not isinstance(inc, dict):
        return None
    started = inc.get("started_at")
    doc_content = inc.get("documentation") or {}
    content = doc_content.get("content") if isinstance(doc_content, dict) else str(doc_content)
    return {
        "incident_id": inc.get("incident_id"),
        "policy": inc.get("policy_name"),
        "state": inc.get("state"),
        "started_at": format_ts(int(started) * 1000) if isinstance(started, (int, float)) and started else None,
        "summary": inc.get("summary"),
        "subject": doc_content.get("subject") if isinstance(doc_content, dict) else None,
        "content": content,
        "version": doc.get("version"),
    }


def incident_for(incidents: Iterable[Mapping[str, Any]], alert_id: str) -> Mapping[str, Any] | None:
    """La notificación cuyo texto lleva el ``alert_id`` (la documentación de la política lo incluye)."""
    for inc in incidents:
        text = " ".join(str(inc.get(k) or "") for k in ("content", "subject", "summary"))
        if alert_id and alert_id in text:
            return inc
    return None


def cooldown_violations(alerts: Iterable[Mapping[str, Any]], cooldown_s: int = COOLDOWN_SECONDS) -> list[str]:
    """Pares de mensajes de la misma marca × fuente × tipo con ventanas a menos del *cooldown* (ids únicos)."""
    by_key: dict[tuple[str, str, str], dict[str, int]] = {}
    for a in alerts:
        if a.get("origin") == "check":
            continue
        by_key.setdefault((a["brand"], a["source"], a["alert_type"]), {})[a["alert_id"]] = to_ms(a["window_end"])
    out = []
    for key, ends in sorted(by_key.items()):
        times = sorted(ends.values())
        for prev, cur in zip(times, times[1:]):
            if cur - prev < cooldown_s * 1000:
                out.append(f"{'|'.join(key)}: ventanas que terminan a {(cur - prev) / 1000:.0f} s (< {cooldown_s} s)")
    return out


def seconds_between(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    return round((to_ms(end) - to_ms(start)) / 1000, 1)


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


__all__ = [
    "BURST_SEED", "BURST_GENERATOR", "BURST_SCENARIO", "synthetic_alert", "burst_envelopes", "burst_problems",
    "parse_incident", "incident_for", "cooldown_violations", "seconds_between",
]
