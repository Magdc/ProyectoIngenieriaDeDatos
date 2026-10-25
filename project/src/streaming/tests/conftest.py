"""Fixtures del Lote 2.1.

`mini_bundle` es un schema mínimo propio (no depende de `contracts/`) para
probar la lógica; `contracts_bundle` carga los schemas reales y omite la prueba
si faltan `contracts/`, `jsonschema` o `referencing`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[3]
CONTRACTS_DIR = PROJECT_DIR / "contracts"

_TS = {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"}

MINI_ENVELOPE = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.test/contracts/envelope.v1.schema.json",
    "type": "object",
    "required": ["schema_version", "event_id", "origin", "source", "event_time", "text", "payload"],
    "properties": {
        "schema_version": {"const": "1.0"},
        "event_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "origin": {"enum": ["stream", "historical"]},
        "source": {"enum": ["reddit", "facebook", "instagram"]},
        "event_time": _TS,
        "text": {"type": "string", "minLength": 1},
        # $ref relativo: se resuelve contra el $id (registro por $id).
        "payload": {"$ref": "payload.mini.schema.json"},
    },
}
MINI_PAYLOAD = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.test/contracts/payload.mini.schema.json",
    "type": "object",
    "required": ["kind"],
    "properties": {"kind": {"enum": ["post", "comment"]}},
}
MINI_ATTRIBUTES = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["event_id", "source", "origin", "schema_version"],
    "properties": {"source": {"enum": ["reddit", "facebook", "instagram"]}},
    "additionalProperties": {"type": "string"},
}


@pytest.fixture
def mini_bundle():
    pytest.importorskip("jsonschema")
    pytest.importorskip("referencing")
    return {
        "envelope.v1.schema.json": MINI_ENVELOPE,
        "payload.mini.schema.json": MINI_PAYLOAD,
        "pubsub.attributes.schema.json": MINI_ATTRIBUTES,
    }


@pytest.fixture
def mini_validators(mini_bundle):
    from streaming.validate import Validators

    return Validators(mini_bundle)


@pytest.fixture(scope="session")
def contracts_bundle():
    pytest.importorskip("jsonschema")
    pytest.importorskip("referencing")
    if not (CONTRACTS_DIR / "envelope.v1.schema.json").is_file():
        pytest.skip("faltan los schemas de contracts/")
    from streaming.schemas import load_bundle

    return load_bundle(CONTRACTS_DIR)


@pytest.fixture
def sample_envelope():
    """Envelope que cumple MINI_ENVELOPE (y, por construcción, el centinela real)."""
    from streaming.inject import sentinel_envelope

    return sentinel_envelope("pytest", 123, now=1_800_000_000.0)
