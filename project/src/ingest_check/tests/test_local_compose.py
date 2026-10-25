"""Coherencia de ``docker-compose.yml`` con §5.1, §5.3 y HU-047 (sin Docker)."""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

PROJECT = Path(__file__).resolve().parents[3]
COMPOSE = PROJECT / "docker-compose.yml"


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _env(svc: dict) -> dict:
    env = svc.get("environment") or {}
    assert isinstance(env, dict), "environment debe ser un mapa"
    return {k: str(v) for k, v in env.items()}


def test_services(compose):
    assert set(compose["services"]) >= {"pubsub-emulator", "pubsub-init", "simulator", "validator"}
    assert compose["services"]["validator"].get("profiles") == ["tools"]


def test_simulator_uses_emulator_and_local_state(compose):
    env = _env(compose["services"]["simulator"])
    assert env["PUBSUB_EMULATOR_HOST"] == "pubsub-emulator:8085"
    assert env["SIM_PUBLISHER"] == "pubsub"
    assert env["SIM_STATE_BACKEND"] == "local" and env["SIM_STATE_DIR"] == "/state"
    assert env["SIM_TOPIC_TEMPLATE"] == "ta-raw-{source}"
    assert env["SIM_SOURCES"] == "reddit,facebook,instagram"
    assert env["GCP_PROJECT"] == "ta-local"
    assert "SIM_SEED" in env and "SIM_CHAOS_ENABLED" in env
    build = compose["services"]["simulator"]["build"]
    assert build["dockerfile"] == "src/simulator/Dockerfile" and build["target"] == "runtime"


def test_no_credentials_anywhere():
    text = COMPOSE.read_text(encoding="utf-8")
    for needle in ("GOOGLE_APPLICATION_CREDENTIALS", "AWS_", "application_default_credentials",
                   ".config/gcloud", ".aws", "project-0568d9ba"):
        assert needle not in text, needle


def test_start_order(compose):
    s = compose["services"]
    assert s["pubsub-init"]["depends_on"]["pubsub-emulator"]["condition"] == "service_healthy"
    assert s["simulator"]["depends_on"]["pubsub-init"]["condition"] == "service_completed_successfully"
    assert s["validator"]["depends_on"]["simulator"]["condition"] == "service_healthy"


def test_cursor_volume_shared(compose):
    s = compose["services"]
    assert "sim-state:/state" in s["simulator"]["volumes"]
    assert "sim-state:/state:ro" in s["validator"]["volumes"]
    assert "sim-state" in compose["volumes"]


def test_mounted_paths_exist(compose):
    for name in ("pubsub-init", "validator"):
        for vol in compose["services"][name]["volumes"]:
            host = vol.split(":")[0]
            if host.startswith("./") and host not in ("./evidencia",):
                assert (PROJECT / host).exists(), f"{name}: no existe {host}"
    assert (PROJECT / "local" / "validator.Dockerfile").is_file()
    assert (PROJECT / "scripts" / "local_pubsub_init.py").is_file()
