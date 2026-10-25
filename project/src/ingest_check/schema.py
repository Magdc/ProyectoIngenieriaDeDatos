"""Validación opcional contra ``contracts/envelope.v1.schema.json`` (§5.13).

Registra todos los ``*.schema.json`` de ``contracts/`` por su ``$id`` y por su
nombre de archivo, de modo que las referencias a los payloads se resuelvan
tanto si usan ``$id`` absolutos como rutas relativas.
"""

from __future__ import annotations

import json
from pathlib import Path

from ingest_check.stats import Validator

ENVELOPE_SCHEMA = "envelope.v1.schema.json"


class SchemaUnavailable(RuntimeError):
    """Faltan ``jsonschema``/``referencing`` o los schemas."""


def default_contracts_dir() -> Path:
    # src/ingest_check/schema.py → parents[2] = project/
    return Path(__file__).resolve().parents[2] / "contracts"


def load_envelope_validator(contracts_dir: str | Path | None = None, max_errors: int = 3) -> Validator:
    try:
        from jsonschema import Draft202012Validator
        from referencing import Registry, Resource
        from referencing.jsonschema import DRAFT202012
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise SchemaUnavailable("faltan jsonschema o referencing") from exc

    root = Path(contracts_dir) if contracts_dir else default_contracts_dir()
    main = root / ENVELOPE_SCHEMA
    if not main.is_file():
        raise SchemaUnavailable(f"no existe {main}")

    resources: list[tuple[str, object]] = []
    envelope: dict | None = None
    for path in sorted(root.glob("*.schema.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        res = Resource.from_contents(doc, default_specification=DRAFT202012)
        resources.append((path.name, res))
        resources.append((path.resolve().as_uri(), res))
        if isinstance(doc, dict) and isinstance(doc.get("$id"), str):
            resources.append((doc["$id"], res))
        if path.name == ENVELOPE_SCHEMA:
            envelope = doc
    assert envelope is not None
    registry = Registry().with_resources(resources)
    if "$id" not in envelope:
        # Sin $id, las referencias relativas se resuelven contra la ruta del archivo.
        envelope = {**envelope, "$id": main.resolve().as_uri()}
    validator = Draft202012Validator(envelope, registry=registry)

    def validate(doc: dict) -> list[str]:
        errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
        out = []
        for e in errors[:max_errors]:
            where = "/".join(str(p) for p in e.absolute_path) or "(raíz)"
            out.append(f"{where}: {e.message}")
        return out

    return validate
