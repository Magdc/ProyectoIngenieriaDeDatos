"""Utilidades compartidas por las pruebas del simulador (Lote 1.2)."""

from __future__ import annotations

import functools
import re
from datetime import datetime, timezone

from simulator.factory import EventFactory
from simulator.sim_config import config_from_dict, load_config

NOW = datetime(2026, 10, 20, 17, 0, 0, 123000, tzinfo=timezone.utc)
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
HASHTAG_RE = re.compile(r"^[a-z0-9_]{1,100}$")


@functools.lru_cache(maxsize=None)
def factory(seed: int = 7) -> EventFactory:
    """Fábrica con la configuración real de ``sim.yaml`` y caos apagado."""
    return EventFactory(load_config(env={"SIM_SEED": str(seed), "SIM_CHAOS_ENABLED": "false"}))


def config_with(env: dict | None = None, **overrides):
    """``sim.yaml`` real con secciones reemplazadas (p. ej. ``chaos={...}``)."""
    import yaml
    from simulator.sim_config import DEFAULT_CONFIG_PATH

    data = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(data.get(k), dict):
            data[k] = {**data[k], **v}
        else:
            data[k] = v
    return config_from_dict(data, env or {})
