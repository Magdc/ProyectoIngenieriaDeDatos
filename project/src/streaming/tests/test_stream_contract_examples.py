"""El validador del pipeline acepta y rechaza lo mismo que `contracts/` (§5.13).

Toma de `contracts/examples/{valid,invalid}/` los ejemplos con forma de
envelope **sin anonimizar** (tienen `author.user_id` y no traen campos de NLP):
los válidos deben pasar y los inválidos deben ir a la DLQ como `schema`.
"""

from __future__ import annotations

import json

import pytest

from streaming.inject import attributes_for
from streaming.records import DlqRecord, ValidEvent

from .conftest import CONTRACTS_DIR

NLP_FIELDS = {"sentiment_label", "emotion_label", "nlp_model_version", "process_time"}


def _raw_envelopes(kind: str):
    folder = CONTRACTS_DIR / "examples" / kind
    if not folder.is_dir():
        return []
    found = []
    for path in sorted(folder.glob("*.json")):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            continue
        author = obj.get("author") if isinstance(obj, dict) else None
        if (isinstance(author, dict) and "user_id" in author and "schema_version" in obj
                and not NLP_FIELDS & set(obj)):
            found.append((path.name, obj))
    return found


def _attrs(obj):
    try:
        return attributes_for(obj)
    except KeyError:
        return {}


def test_ejemplos_validos_pasan(contracts_bundle):
    from streaming.validate import Validators

    examples = _raw_envelopes("valid")
    if not examples:
        pytest.skip("no hay ejemplos de envelope sin anonimizar en contracts/examples/valid/")
    validators = Validators(contracts_bundle)
    for name, obj in examples:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        result = validators.check(obj["source"], data, _attrs(obj))
        assert isinstance(result, ValidEvent), f"{name}: {getattr(result, 'error_detail', '')}"


def test_ejemplos_invalidos_van_a_la_dlq(contracts_bundle):
    from streaming.validate import Validators

    examples = _raw_envelopes("invalid")
    if not examples:
        pytest.skip("no hay ejemplos de envelope sin anonimizar en contracts/examples/invalid/")
    validators = Validators(contracts_bundle)
    for name, obj in examples:
        source = obj.get("source") if obj.get("source") in ("reddit", "facebook", "instagram") else "reddit"
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        result = validators.check(source, data, _attrs(obj))
        assert isinstance(result, DlqRecord), f"{name} pasó la validación y no debía"
        assert result.error_type == "schema", f"{name}: {result.error_type} {result.error_detail}"
