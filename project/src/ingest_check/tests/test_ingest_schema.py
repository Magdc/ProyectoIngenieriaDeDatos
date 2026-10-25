"""Carga del validador de schemas (con schemas sintéticos y, si existen, con ``contracts/``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("referencing")

from ingest_check.schema import SchemaUnavailable, default_contracts_dir, load_envelope_validator  # noqa: E402


def _write(path: Path, doc: dict) -> None:
    path.write_text(json.dumps(doc), encoding="utf-8")


@pytest.mark.parametrize("with_ids", [False, True])
def test_resolves_references_between_files(tmp_path, with_ids):
    payload = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
               "required": ["kind"], "properties": {"kind": {"enum": ["post", "comment"]}}}
    envelope = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
                "required": ["seq", "payload"],
                "properties": {"seq": {"type": "integer"}, "payload": {"$ref": "payload.test.schema.json"}}}
    if with_ids:
        payload["$id"] = "https://example.test/contracts/payload.test.schema.json"
        envelope["$id"] = "https://example.test/contracts/envelope.v1.schema.json"
    _write(tmp_path / "payload.test.schema.json", payload)
    _write(tmp_path / "envelope.v1.schema.json", envelope)

    validate = load_envelope_validator(tmp_path)
    assert validate({"seq": 1, "payload": {"kind": "post"}}) == []
    errors = validate({"seq": "x", "payload": {"kind": "otro"}})
    assert len(errors) == 2 and any(e.startswith("payload/kind") for e in errors)


def test_missing_contracts_dir(tmp_path):
    with pytest.raises(SchemaUnavailable):
        load_envelope_validator(tmp_path)


def _real_envelope_examples() -> list[Path]:
    root = default_contracts_dir() / "examples" / "valid"
    if not root.is_dir():
        return []
    out = []
    for p in sorted(root.glob("*.json")):
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        # Solo envelopes de salida del simulador (con identidad en claro), no enriquecidos ni atributos.
        if isinstance(doc, dict) and "schema_version" in doc and "user_id" in (doc.get("author") or {}):
            out.append(p)
    return out


@pytest.mark.skipif(not (default_contracts_dir() / "envelope.v1.schema.json").is_file(),
                    reason="contracts/ no está presente")
def test_real_contracts_accept_valid_envelope_examples():
    examples = _real_envelope_examples()
    if not examples:
        pytest.skip("no hay ejemplos de envelope en contracts/examples/valid/")
    validate = load_envelope_validator()
    for p in examples:
        assert validate(json.loads(p.read_text(encoding="utf-8"))) == [], p.name
