"""sim.yaml y variables de entorno (Lote 1.2)."""

import pytest

from simulator.sim_config import load_config
from simulator.tests._sim_helpers import config_with


def test_defaults_match_architecture():
    cfg = load_config(env={})
    assert cfg.seed == 7
    assert cfg.interval_s == (1.0, 3.0)
    assert cfg.checkpoint_every == 50
    assert cfg.event_lag_s == (0.0, 5.0)
    assert cfg.post_probability == {"reddit": 0.30, "facebook": 0.15, "instagram": 0.0}
    assert cfg.hashtag_probability == {"instagram": 0.60, "facebook": 0.15, "reddit": 0.0}
    assert cfg.pool_size == 2000
    assert cfg.chaos.enabled is False
    assert cfg.chaos.rates == {"duplicate": 0.005, "late": 0.01, "null_fields": 0.01, "malformed": 0.002}
    assert cfg.chaos.late_minutes == (2.0, 30.0)


def test_env_overrides_seed_and_chaos():
    cfg = load_config(env={"SIM_SEED": "42", "SIM_CHAOS_ENABLED": "true"})
    assert cfg.seed == 42 and cfg.chaos.enabled is True
    assert load_config(env={"SIM_CHAOS_ENABLED": "false"}).chaos.enabled is False


def test_invalid_values_are_rejected():
    with pytest.raises(ValueError):
        load_config(env={"SIM_CHAOS_ENABLED": "quizas"})
    with pytest.raises(ValueError):
        config_with(kinds={"instagram": {"post": 0.1}})
    with pytest.raises(ValueError):
        config_with(chaos={"rates": {"duplicate": 2}})
    with pytest.raises(ValueError):
        config_with(chaos={"rates": {"explota": 0.1}})
