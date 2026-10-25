"""Carga de `contracts/*.schema.json` y construcción de validadores.

Los schemas se leen **al construir el pipeline** (en la máquina que lo lanza) y
viajan como diccionarios dentro de los DoFn; así los workers de Dataflow no
necesitan la carpeta `contracts/`. Cada schema se registra por su `$id` y por
su nombre de archivo, de modo que los `$ref` funcionan con cualquiera de los dos
estilos (igual que `ingest_check.schema`, Lote 1.4).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ENVELOPE_SCHEMA = "envelope.v1.schema.json"
ATTRIBUTES_SCHEMA = "pubsub.attributes.schema.json"

SchemaBundle = dict[str, dict[str, Any]]


def default_contracts_dir() -> Path:
    """`project/contracts`, relativo a este archivo (`project/src/streaming/`)."""
    return Path(__file__).resolve().parents[2] / "contracts"


def load_bundle(contracts_dir: str | Path | None = None) -> SchemaBundle:
    """Lee todos los `*.schema.json`. Falla si no está el schema del envelope."""
    base = Path(contracts_dir) if contracts_dir else default_contracts_dir()
    if not base.is_dir():
        raise FileNotFoundError(f"no existe la carpeta de contratos: {base}")
    bundle: SchemaBundle = {}
    for path in sorted(base.glob("*.schema.json")):
        with path.open(encoding="utf-8") as fh:
            bundle[path.name] = json.load(fh)
    if ENVELOPE_SCHEMA not in bundle:
        raise FileNotFoundError(f"falta {ENVELOPE_SCHEMA} en {base}")
    return bundle


def build_validator(bundle: SchemaBundle, name: str):
    """Validador Draft 2020-12 para `name` con todos los schemas registrados.

    No usa *format checker*: las reglas de §5.2 las fijan patrones del propio
    schema, igual que en `contracts/tests/`.
    """
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    resources = []
    for file_name, schema in bundle.items():
        resource = Resource.from_contents(schema, default_specification=DRAFT202012)
        resources.append((file_name, resource))
        schema_id = schema.get("$id") if isinstance(schema, dict) else None
        if schema_id:
            resources.append((schema_id, resource))
    registry = Registry().with_resources(resources)
    schema = bundle[name]
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, registry=registry)


def describe_error(validator, instance: Any, max_len: int = 500) -> str | None:
    """Primer error relevante como `ruta: mensaje`, o `None` si es válido."""
    from jsonschema.exceptions import best_match

    error = best_match(validator.iter_errors(instance))
    if error is None:
        return None
    path = "/".join(str(p) for p in error.absolute_path) or "(raíz)"
    message = error.message
    if len(message) > max_len:
        message = message[: max_len - 1] + "…"
    return f"{path}: {message}"
