"""Fixtures y marcadores de las pruebas del exportador.

Marcadores:
- ``repo``: necesitan el repositorio (``project/infra``, ``src/common``, ``contracts/``);
  la imagen del job no los trae y los deselecciona.
- ``pyarrow``: necesitan pyarrow (solo ``lake_check``, parte F).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from exporter.config import load_config

PROJECT = Path(__file__).resolve().parents[3]
KMS = "arn:aws:kms:us-east-1:648617469176:key/11111111-2222-3333-4444-555555555555"
ROLE = "arn:aws:iam::648617469176:role/ta-gcp-exporter-role"


def pytest_configure(config):
    config.addinivalue_line("markers", "repo: necesita el repositorio completo (no corre en la imagen)")
    config.addinivalue_line("markers", "pyarrow: necesita pyarrow (no corre en la imagen)")


@pytest.fixture
def env() -> dict[str, str]:
    return {
        "GCP_PROJECT": "project-0568d9ba-848f-413c-88b",
        "TA_LAKE_BUCKET": "ta-dev-648617469176-lake",
        "TA_LAKE_KMS_KEY_ARN": KMS,
        "TA_EXPORTER_ROLE_ARN": ROLE,
        "TA_EXPORT_WORKERS": "4",
    }


@pytest.fixture
def cfg(env):
    return load_config(env)


@pytest.fixture
def project_dir() -> Path:
    return PROJECT
