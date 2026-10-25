"""docker-compose.dataflow.yml: servicio `dataflow-local` (DirectRunner) y verificador."""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

from .conftest import PROJECT_DIR  # noqa: E402

OVERRIDE = PROJECT_DIR / "docker-compose.dataflow.yml"
BASE = PROJECT_DIR / "docker-compose.yml"


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(OVERRIDE.read_text(encoding="utf-8"))


def _env(service) -> dict:
    env = service.get("environment", {})
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    return env


def test_servicios_y_perfiles(compose):
    services = compose["services"]
    assert services["dataflow-local"]["profiles"] == ["dataflow"]
    assert services["streaming-check"]["profiles"] == ["tools"]
    for name in ("dataflow-local", "streaming-check"):
        assert services[name]["depends_on"]["pubsub-init"]["condition"] == "service_completed_successfully"


def test_pipeline_usa_directrunner_emulador_y_lake_local(compose):
    svc = compose["services"]["dataflow-local"]
    command = " ".join(svc["command"])
    assert "streaming.pipeline" in command
    assert "--runner=DirectRunner" in command
    assert "--output_root=/lake" in command
    env = _env(svc)
    assert env["PUBSUB_EMULATOR_HOST"] == "pubsub-emulator:8085"
    assert env["GCP_PROJECT"] == "ta-local"  # ficticio: el proyecto real no aparece aquí
    assert env["ENV"] == "local"
    assert any(v.endswith(":/lake") for v in svc["volumes"])


@pytest.mark.parametrize("name", ["dataflow-local", "streaming-check"])
def test_sin_credenciales_de_nube(compose, name):
    svc = compose["services"][name]
    env = _env(svc)
    forbidden = {"GOOGLE_APPLICATION_CREDENTIALS", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                 "AWS_PROFILE", "CLOUDSDK_CONFIG"}
    assert not forbidden & set(env)
    mounts = " ".join(svc.get("volumes", []))
    for token in (".config/gcloud", ".aws", "application_default_credentials"):
        assert token not in mounts


@pytest.mark.parametrize("name", ["dataflow-local", "streaming-check"])
def test_rutas_montadas_existen(compose, name):
    for volume in compose["services"][name]["volumes"]:
        host = volume.split(":", 1)[0]
        if host.startswith("${") or host in ("./evidencia",):
            continue  # el lake y la evidencia se crean al arrancar
        assert (PROJECT_DIR / host).exists(), host


def test_imagen_y_contexto(compose):
    build = compose["services"]["dataflow-local"]["build"]
    assert (PROJECT_DIR / build["context"] / build["dockerfile"]).is_file()
    streaming_ctx = PROJECT_DIR / build["additional_contexts"]["streaming"]
    assert (streaming_ctx / "requirements.txt").is_file()
    dockerfile = (PROJECT_DIR / build["context"] / build["dockerfile"]).read_text()
    assert "COPY --from=streaming requirements.txt" in dockerfile
    assert compose["services"]["streaming-check"]["image"] == compose["services"]["dataflow-local"]["image"]


def test_compatible_con_el_compose_del_lote_1_5(compose):
    if not BASE.is_file():
        pytest.skip("falta docker-compose.yml (Lote 1.5)")
    base = yaml.safe_load(BASE.read_text(encoding="utf-8"))
    services = base.get("services", {})
    for name in ("pubsub-emulator", "pubsub-init", "simulator"):
        assert name in services, f"el Compose base no tiene {name}"
    assert not {"dataflow-local", "streaming-check"} & set(services)


def test_requirements_fijan_versiones_exactas():
    reqs = (PROJECT_DIR / "src" / "streaming" / "requirements.txt").read_text().splitlines()
    pins = [r for r in reqs if r.strip() and not r.startswith("#")]
    assert pins and all("==" in r for r in pins)
    assert any(r.startswith("apache-beam[gcp]==") for r in pins)


def test_setup_py_empaqueta_streaming_y_common():
    text = (PROJECT_DIR / "src" / "setup.py").read_text()
    assert '"streaming"' in text and '"common"' in text
    assert "*.tests" in text
