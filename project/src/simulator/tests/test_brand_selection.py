"""Pesos de marca que varían con seq y hashtags de marca (§3.8, HU-014)."""

import random
from collections import Counter

import pytest

from common.hashtags import extract_hashtags
from simulator.enrich.brands import BrandSelector, add_hashtags, brand_keys, brands_in_text, first_sentence, load_brands
from simulator.tests._sim_helpers import config_with, factory


def test_weights_normalized_deterministic_and_varying():
    sel = factory().selector
    w0 = sel.weights(0)
    assert set(w0) == set(brand_keys(load_brands()))
    assert abs(sum(w0.values()) - 1) < 1e-9
    assert sel.weights(1234) == sel.weights(1234)
    # Las tendencias existen: el peso de cada marca se mueve a lo largo del día.
    for b in w0:
        series = [sel.weights(s)[b] for s in range(0, 20000, 100)]
        assert max(series) - min(series) > 0.10, b


def test_choice_follows_weights():
    sel = factory().selector
    seq = 900
    rng = random.Random(1)
    counts = Counter(sel.choose(rng, seq) for _ in range(20000))
    for b, w in sel.weights(seq).items():
        assert abs(counts[b] / 20000 - w) < 0.02


def test_unknown_brand_in_sim_yaml_fails():
    cfg = config_with(brand_weights={"brands": {"cocacola": {"base": 1.0}}})
    with pytest.raises(ValueError):
        BrandSelector(load_brands(), cfg)


def test_brands_list_starts_with_chosen_and_has_no_duplicates():
    b = load_brands()
    out = brands_in_text("pepsi", "Pepsi vs Postobón, gana Postobon.", b)
    assert out[0] == "pepsi" and "postobon" in out and len(out) == len(set(out))
    assert brands_in_text("sprite", "sin marca", b) == ["sprite"]


def test_add_hashtags_appends_missing_canonical_tags():
    text = "Me gusta Postobón. #Colombia"
    rng = random.Random(3)
    out = add_hashtags(rng, text, brand_display="Postobón", probability=1.0, count=(2, 2), sector=["Colombia", "Gaseosa"])
    tags = extract_hashtags(out)
    assert out.startswith(text)
    assert tags[0] == "colombia" and len(tags) == len(set(tags)) == 3
    assert "postobon" in tags and "gaseosa" in tags


def test_add_hashtags_respects_probability_zero():
    rng = random.Random(3)
    assert add_hashtags(rng, "hola", brand_display="Pepsi", probability=0.0, count=(1, 2), sector=["Colombia"]) == "hola"


def test_first_sentence():
    assert first_sentence("Hola parce. ¿Qué tal?") == "Hola parce."
    assert first_sentence("sin punto final") == "sin punto final"
