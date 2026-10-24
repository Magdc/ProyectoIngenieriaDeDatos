"""Fixtures compartidas. ``envelope_validator`` valida contra ``project/contracts/`` y se omite
(skip) si faltan ``jsonschema``/``referencing`` o los schemas."""
import json
from pathlib import Path
from urllib.parse import urljoin

import pytest

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"


@pytest.fixture(scope="session")
def envelope_validator():
    jsonschema = pytest.importorskip("jsonschema")
    referencing = pytest.importorskip("referencing")
    from referencing.jsonschema import DRAFT202012

    path = CONTRACTS / "envelope.v1.schema.json"
    if not path.exists():
        pytest.skip("contracts/envelope.v1.schema.json no está en el repositorio")
    resources = []
    for p in sorted(CONTRACTS.glob("*.schema.json")):
        doc = json.loads(p.read_text(encoding="utf-8"))
        res = referencing.Resource.from_contents(doc, default_specification=DRAFT202012)
        resources.append((p.name, res))
        if "$id" in doc:
            resources += [(doc["$id"], res), (urljoin(doc["$id"], p.name), res)]
    registry = referencing.Registry().with_resources(resources)
    schema = json.loads(path.read_text(encoding="utf-8"))
    try:
        validator = jsonschema.Draft202012Validator(schema, registry=registry)
    except TypeError:
        pytest.skip("jsonschema < 4.18 (sin soporte de registry); instale la versión de contracts/requirements.txt")

    def check(doc):
        """Devuelve la lista de errores (vacía = válido)."""
        try:
            errores = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
        except referencing.exceptions.Unresolvable as exc:
            pytest.skip(f"no se pudo resolver un $ref del schema: {exc}")
        return [f"{'/'.join(map(str, e.path)) or '<raíz>'}: {e.message}" for e in errores]

    return check


@pytest.fixture(scope="session")
def payloads():
    """Payloads de ejemplo por fuente, copiados de ARCHITECTURE.md §5.2."""
    return {
        "reddit": {"reddit_id": "t1_k3x9a1", "kind": "comment", "subreddit": "r/colombia", "title": None,
                   "parent_id": "t3_j8f2ds", "score": 27, "upvote_ratio": None, "num_comments": None,
                   "permalink": "/r/colombia/comments/j8f2ds/_/k3x9a1/", "flair": None},
        "facebook": {"comment_id": "1029384756_5647382910", "kind": "comment",
                     "page_id": "sim-page-postobon", "page_name": "Postobón (sim)",
                     "post_id": "sim-post-000812", "parent_id": "sim-post-000812",
                     "reactions_total": 14,
                     "reactions": {"like": 9, "love": 3, "haha": 1, "wow": 0, "sad": 0, "angry": 1},
                     "shares_count": 2, "comments_count": 3,
                     "permalink": "https://sim.facebook.local/postobon/posts/000812"},
        "instagram": {"comment_id": "17912345678901234", "kind": "comment",
                      "media_id": "17855544433322211", "media_type": "IMAGE",
                      "account_username": "postobon.sim", "parent_id": None,
                      "like_count": 31, "comments_count": 4,
                      "permalink": "https://sim.instagram.local/p/000812/"},
    }
