"""NLP (Lote 2.3) contra ``contracts/envelope.enriched.v1.schema.json``. Se omite sin ``contracts/``."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from common import label_map
from streaming import nlp
from streaming.nlp_testing import FAKE_REVISIONS

SCHEMA = Path(__file__).resolve().parents[3] / "contracts" / "envelope.enriched.v1.schema.json"
pytestmark = pytest.mark.skipif(not SCHEMA.is_file(), reason="falta contracts/envelope.enriched.v1.schema.json")


def _prop(name):
    doc = json.loads(SCHEMA.read_text(encoding="utf-8"))
    prop = (doc.get("properties") or {}).get(name)
    if prop is None:
        pytest.skip(f"el schema no define {name} en properties")
    return prop


def _values(prop):
    if "enum" in prop:
        return set(prop["enum"])
    for key in ("anyOf", "oneOf"):
        for sub in prop.get(key, []):
            if "enum" in sub:
                return set(sub["enum"])
    return None


def test_version_cumple_el_patron_del_contrato():
    prop = _prop("nlp_model_version")
    version = nlp.nlp_model_version(FAKE_REVISIONS["sentiment"], FAKE_REVISIONS["emotion"])
    pattern = prop.get("pattern")
    if not pattern:
        pytest.skip("nlp_model_version sin pattern en el schema")
    assert re.search(pattern, version), f"{version!r} no cumple {pattern!r}"


def test_etiquetas_mapeadas_son_las_del_contrato():
    sent = _values(_prop("sentiment_label"))
    emo = _values(_prop("emotion_label"))
    mapped_s = {label_map.map_sentiment(x) for x in nlp.EXPECTED_LABELS["sentiment"]}
    mapped_e = {label_map.map_emotion(x) for x in nlp.EXPECTED_LABELS["emotion"]}
    if sent is not None:
        assert mapped_s == sent
    if emo is not None:
        assert mapped_e == emo
