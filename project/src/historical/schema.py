"""Validación contra ``contracts/`` (§5.13): envelope (§5.2) y envelope enriquecido.

Registra todos los ``contracts/*.schema.json`` por ``$id`` y por nombre de archivo, igual
que ``ingest_check.schema`` y ``streaming.schemas`` (Lotes 1.4 y 2.1), sin *format
checker*, como ``contracts/tests/``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Mapping

from historical.work import PROJECT_DIR

DEFAULT_CONTRACTS = PROJECT_DIR / "contracts"
ENVELOPE = "envelope.v1.schema.json"
ENRICHED = "envelope.enriched.v1.schema.json"


class SchemaUnavailable(RuntimeError):
    """Faltan ``jsonschema``/``referencing`` o los schemas."""


def load_validators(contracts_dir: str | Path | None = None) -> dict[str, Callable[[Mapping[str, Any]], list[str]]]:
    """``{"envelope": f, "enriched": f}``; cada ``f(doc)`` devuelve la lista de errores."""
    base = Path(contracts_dir) if contracts_dir else DEFAULT_CONTRACTS
    try:
        from jsonschema import Draft202012Validator
        from referencing import Registry, Resource
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise SchemaUnavailable(f"faltan jsonschema/referencing: {exc}") from exc
    files = sorted(base.glob("*.schema.json"))
    if not files:
        raise SchemaUnavailable(f"no hay schemas en {base}")
    schemas: dict[str, dict] = {}
    registry = Registry()
    for path in files:
        doc = json.loads(path.read_text(encoding="utf-8"))
        schemas[path.name] = doc
        resource = Resource.from_contents(doc)
        registry = registry.with_resource(path.name, resource)
        registry = registry.with_resource(f"./{path.name}", resource)
        if isinstance(doc, dict) and doc.get("$id"):
            registry = registry.with_resource(doc["$id"], resource)
    out: dict[str, Callable[[Mapping[str, Any]], list[str]]] = {}
    for key, name in (("envelope", ENVELOPE), ("enriched", ENRICHED)):
        if name not in schemas:
            raise SchemaUnavailable(f"falta {name} en {base}")
        validator = Draft202012Validator(schemas[name], registry=registry)

        def check(doc: Mapping[str, Any], _v=validator) -> list[str]:
            errors = sorted(_v.iter_errors(doc), key=lambda e: list(e.absolute_path))
            return [f"{'/'.join(str(p) for p in e.absolute_path) or '<raíz>'}: {e.message}"[:500]
                    for e in errors[:5]]

        out[key] = check
    return out
