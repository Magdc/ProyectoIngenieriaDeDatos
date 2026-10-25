"""Eventos de las tres fuentes: determinismo, mezcla de tipos y payload (HU-010 a HU-014)."""

import json
import uuid
from collections import Counter
from datetime import timedelta

import pytest

from common.hashtags import extract_hashtags
from simulator.enrich.brands import brand_keys, load_brands
from simulator.factory import EventFactory, Scenario
from simulator.generator.comment_generator import GENERATOR_ID
from simulator.tests._sim_helpers import HASHTAG_RE, NOW, TS_RE, factory

SOURCES = ("reddit", "facebook", "instagram")
N = 1500


@pytest.fixture(scope="module")
def events():
    f = factory()
    return {s: [f.make(s, q, NOW).envelope for q in range(N)] for s in SOURCES}


@pytest.mark.parametrize("source", SOURCES)
def test_same_seed_and_seq_give_same_event(source):
    a = factory().make(source, 1207, NOW).envelope
    b = EventFactory(factory().config).make(source, 1207, NOW + timedelta(hours=3)).envelope
    assert a["event_id"] == b["event_id"]
    assert a["text"] == b["text"]
    for k in ("author", "brands", "hashtags", "payload", "sim", "seq"):
        assert a[k] == b[k], k
    # Solo los tiempos dependen del reloj.
    assert a["ingest_time"] != b["ingest_time"]


@pytest.mark.parametrize("source", SOURCES)
def test_event_id_formula_and_seed_dependence(source):
    e = factory(7).make(source, 31, NOW).envelope
    assert e["event_id"] == str(uuid.uuid5(uuid.NAMESPACE_URL, f"stream|{source}|7|31"))
    other = factory(8).make(source, 31, NOW).envelope
    assert other["event_id"] != e["event_id"]


def test_ids_unique_per_source(events):
    for s, evs in events.items():
        assert len({e["event_id"] for e in evs}) == N
        assert [e["seq"] for e in evs] == list(range(N))


def test_common_envelope_fields(events):
    keys = load_brands()
    canon = set(brand_keys(keys))
    for s, evs in events.items():
        for e in evs:
            assert list(e) == [
                "schema_version", "event_id", "origin", "source", "generator", "seq", "event_time",
                "ingest_time", "author", "text", "lang", "brands", "hashtags", "payload", "sim",
            ]
            assert e["schema_version"] == "1.0" and e["origin"] == "stream" and e["source"] == s
            assert e["generator"] == GENERATOR_ID == "comment-gen@1.0"
            assert TS_RE.match(e["event_time"]) and TS_RE.match(e["ingest_time"])
            assert e["event_time"] <= e["ingest_time"]
            assert e["text"].strip() and e["lang"] == "es"
            assert e["brands"] and e["brands"][0] in canon and set(e["brands"]) <= canon
            assert len(e["brands"]) == len(set(e["brands"]))
            assert e["hashtags"] == extract_hashtags(e["text"])
            assert all(HASHTAG_RE.match(h) for h in e["hashtags"])
            assert set(e["author"]) == {"user_id", "handle", "followers"}
            assert e["sim"]["label_sentiment"] in ("positive", "negative", "neutral", None)
            assert e["sim"]["scenario"] is None and e["sim"]["chaos"] == []


def test_event_time_lag_is_0_to_5_seconds(events):
    from datetime import datetime

    for e in events["facebook"][:300]:
        et = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        it = datetime.strptime(e["ingest_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        assert timedelta(0) <= it - et <= timedelta(seconds=5.001)


def test_kind_mix(events):
    for s, expected in (("reddit", 0.30), ("facebook", 0.15), ("instagram", 0.0)):
        share = Counter(e["payload"]["kind"] for e in events[s])["post"] / N
        assert abs(share - expected) < 0.04, (s, share)


def test_reddit_payload(events):
    by_id = {e["payload"]["reddit_id"]: e for e in events["reddit"]}
    for e in events["reddit"]:
        p = e["payload"]
        assert list(p) == ["reddit_id", "kind", "subreddit", "title", "parent_id", "score",
                           "upvote_ratio", "num_comments", "permalink", "flair"]
        assert p["subreddit"].startswith("r/") and p["permalink"].startswith(f"/{p['subreddit']}/comments/")
        assert isinstance(p["score"], int)
        if p["kind"] == "post":
            assert p["reddit_id"].startswith("t3_") and p["parent_id"] is None
            assert p["title"] and e["text"].startswith(p["title"])
            assert isinstance(p["upvote_ratio"], float) and 0 < p["upvote_ratio"] <= 1
            assert isinstance(p["num_comments"], int) and p["num_comments"] >= 0
        else:
            assert p["reddit_id"].startswith("t1_") and p["parent_id"].startswith("t3_")
            assert p["title"] is None and p["upvote_ratio"] is None and p["num_comments"] is None
            parent = by_id.get(p["parent_id"])
            if parent is not None:  # padre real: anterior, post y mismo subreddit
                assert parent["seq"] < e["seq"] and parent["payload"]["kind"] == "post"
                assert parent["payload"]["subreddit"] == p["subreddit"]
    linked = sum(1 for e in events["reddit"] if e["payload"]["parent_id"] in by_id)
    assert linked / Counter(e["payload"]["kind"] for e in events["reddit"])["comment"] > 0.95


def test_facebook_payload(events):
    brands = load_brands()
    posts = {e["payload"]["post_id"]: e for e in events["facebook"] if e["payload"]["kind"] == "post"}
    for e in events["facebook"]:
        p = e["payload"]
        assert list(p) == ["comment_id", "kind", "page_id", "page_name", "post_id", "parent_id", "reactions_total",
                           "reactions", "shares_count", "comments_count", "permalink"]
        brand = e["brands"][0]
        assert p["page_id"] == f"sim-page-{brand}" and p["page_name"].endswith("(sim)")
        assert set(p["reactions"]) == {"like", "love", "haha", "wow", "sad", "angry"}
        assert sum(p["reactions"].values()) == p["reactions_total"]
        assert min(p["shares_count"], p["comments_count"], p["reactions_total"]) >= 0
        assert p["permalink"].startswith(f"https://sim.facebook.local/{brand}/posts/")
        if p["kind"] == "post":
            assert p["parent_id"] is None
        else:
            assert p["parent_id"] == p["post_id"] and p["parent_id"].startswith("sim-post-")
            parent = posts.get(p["parent_id"])
            if parent is not None:  # comenta un post anterior de la misma página
                assert parent["seq"] < e["seq"] and parent["brands"][0] == brand


def test_instagram_payload(events):
    for e in events["instagram"]:
        p = e["payload"]
        assert list(p) == ["comment_id", "kind", "media_id", "media_type", "account_username", "parent_id",
                           "like_count", "comments_count", "permalink"]
        assert p["kind"] == "comment" and p["parent_id"] is None
        assert p["account_username"] == f"{e['brands'][0]}.sim"
        assert p["media_type"] in ("IMAGE", "VIDEO", "CAROUSEL_ALBUM")
        assert p["comment_id"].isdigit() and p["media_id"].isdigit()
        assert p["permalink"].startswith("https://sim.instagram.local/")
        assert e["author"]["handle"].startswith("@")
    # Una misma media tiene siempre el mismo tipo.
    seen = {}
    for e in events["instagram"]:
        p = e["payload"]
        assert seen.setdefault(p["media_id"], p["media_type"]) == p["media_type"]


def test_hashtag_rate_by_network(events):
    # El generador ya pone hashtags en algunos cierres; el simulador añade según la red.
    rate = {s: sum(1 for e in evs if e["hashtags"]) / N for s, evs in events.items()}
    assert rate["instagram"] > rate["facebook"] > rate["reddit"] - 0.02
    assert rate["instagram"] - rate["reddit"] > 0.3


def test_brand_mix_follows_trend(events):
    counts = Counter(e["brands"][0] for e in events["facebook"])
    assert set(counts) == set(brand_keys(load_brands()))


def test_scenario_forces_brand_and_sentiment_and_is_marked():
    f = factory()
    sc = Scenario(name="ola-negativa", brand="postobon", sentiment="negative", source="instagram")
    e = f.make("instagram", 10, NOW, scenario=sc).envelope
    assert e["brands"][0] == "postobon" and e["sim"]["scenario"] == "ola-negativa"
    assert e["sim"]["label_sentiment"] == "negative"
    assert e["event_id"] == f.make("instagram", 10, NOW).envelope["event_id"]
    assert f.make("reddit", 10, NOW, scenario=sc).envelope["sim"]["scenario"] is None


def test_messages_carry_pubsub_attributes():
    em = factory().make("reddit", 3, NOW)
    assert len(em.messages) == 1
    msg = em.messages[0]
    assert json.loads(msg.data) == em.envelope
    assert msg.attributes == {
        "event_id": em.envelope["event_id"], "source": "reddit", "origin": "stream", "schema_version": "1.0",
    }


def test_rejects_bad_input():
    f = factory()
    with pytest.raises(ValueError):
        f.make("mastodon", 0, NOW)
    with pytest.raises(ValueError):
        f.make("reddit", -1, NOW)
    with pytest.raises(ValueError):
        f.make("reddit", 0, NOW.replace(tzinfo=None))


def test_independent_of_pythonhashseed():
    """El pool de Faker y los eventos no dependen de la aleatorización de ``hash()``."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    src = Path(__file__).resolve().parents[2]
    code = (
        "import json,datetime as d;from simulator.factory import EventFactory;"
        "from simulator.sim_config import load_config;"
        "f=EventFactory(load_config(env={}));n=d.datetime(2026,1,1,tzinfo=d.timezone.utc);"
        "print(json.dumps([f.make(s,q,n).envelope for s in ('reddit','facebook','instagram') for q in (0,1,99)]))"
    )
    outs = set()
    for hs in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": hs, "PYTHONPATH": str(src) + os.pathsep + os.environ.get("PYTHONPATH", "")}
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=src, timeout=120)
        assert r.returncode == 0, r.stderr
        outs.add(r.stdout)
    assert len(outs) == 1
