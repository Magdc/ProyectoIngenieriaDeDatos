"""Salida del simulador contra ``contracts/`` (§5.12, §5.13; HU-001, HU-010 a HU-012).

≥ 1.000 eventos por fuente con el caos apagado deben cumplir ``envelope.v1.schema.json``
(con su payload) y los atributos ``pubsub.attributes.schema.json``. Se omite si faltan
``jsonschema``, ``referencing`` o la carpeta ``contracts/``.
"""

import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
referencing = pytest.importorskip("referencing")

from simulator.tests._sim_helpers import NOW, factory  # noqa: E402

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
N = 1000


def _validators():
    if not (CONTRACTS / "envelope.v1.schema.json").exists():
        pytest.skip(f"no se encontró {CONTRACTS}")
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    resources = []
    schemas = {}
    for p in sorted(CONTRACTS.glob("*.schema.json")):
        schema = json.loads(p.read_text(encoding="utf-8"))
        schemas[p.name] = schema
        res = Resource.from_contents(schema, default_specification=DRAFT202012)
        resources.append((p.name, res))
        resources.append((f"./{p.name}", res))
        if schema.get("$id"):
            resources.append((schema["$id"], res))
    registry = Registry().with_resources(resources)

    def make(name):
        cls = jsonschema.validators.validator_for(schemas[name], default=jsonschema.Draft202012Validator)
        return cls(schemas[name], registry=registry, format_checker=cls.FORMAT_CHECKER)

    attrs = make("pubsub.attributes.schema.json") if "pubsub.attributes.schema.json" in schemas else None
    return make("envelope.v1.schema.json"), attrs


@pytest.mark.parametrize("source", ["reddit", "facebook", "instagram"])
def test_events_match_contract(source):
    env_v, attr_v = _validators()
    f = factory()
    for q in range(N):
        em = f.make(source, q, NOW)
        for msg in em.messages:
            body = json.loads(msg.data)
            errors = sorted(env_v.iter_errors(body), key=lambda e: list(e.path))
            assert not errors, f"{source} seq={q}: {errors[0].message} en {list(errors[0].path)}"
            if attr_v is not None:
                a_err = list(attr_v.iter_errors(msg.attributes))
                assert not a_err, f"{source} seq={q} atributos: {a_err[0].message}"
