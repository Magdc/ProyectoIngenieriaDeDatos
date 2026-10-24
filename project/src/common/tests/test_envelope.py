import random
import re
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.envelope import (
    build_envelope, event_id, event_rng, format_ts, normalize_engagement, parse_ts,
    pubsub_attributes,
)

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
T0 = datetime(2026, 10, 17, 14, 3, 11, 482000, tzinfo=timezone.utc)


def _env(**kw):
    base = dict(
        origin="stream", source="reddit", seed=7, seq=1207, event_time=T0,
        ingest_time=T0 + timedelta(milliseconds=1538), user_id="u_000481", handle="u/mariag_89",
        followers=412, text="Soy fan d Postobón y no pienso cambiar. 🙌 #Postobón #Colombia",
        payload={"reddit_id": "t1_k3x9a1", "kind": "comment"}, brands=["postobon"],
        generator="comment-gen@1.0", label_sentiment="positive",
    )
    base.update(kw)
    return build_envelope(**base)


def test_event_id_sigue_la_formula_de_3_6():
    esperado = str(uuid.uuid5(uuid.NAMESPACE_URL, "stream|reddit|7|1207"))
    assert event_id("stream", "reddit", 7, 1207) == esperado
    assert esperado == esperado.lower()


def test_event_id_cambia_con_origen_fuente_semilla_y_seq():
    base = event_id("stream", "reddit", 7, 1)
    assert len({base, event_id("historical", "reddit", 7, 1), event_id("stream", "facebook", 7, 1),
                event_id("stream", "reddit", 8, 1), event_id("stream", "reddit", 7, 2)}) == 5


def test_event_rng_reproducible_y_dependiente_de_la_clave():
    a = [event_rng(7, "reddit", 5).random() for _ in range(3)]
    assert a == [event_rng(7, "reddit", 5).random() for _ in range(3)]
    assert event_rng(7, "reddit", 5).random() != event_rng(7, "reddit", 6).random()


def test_event_rng_identico_entre_procesos_con_distinto_hashseed():
    codigo = ("from common.envelope import event_rng;"
              "r=event_rng(7,'reddit',5);print([r.randrange(10**6) for _ in range(5)])")
    src = str(Path(__file__).resolve().parents[2])
    salidas = set()
    for hs in ("1", "2", "random"):
        env = {"PYTHONHASHSEED": hs, "PYTHONPATH": src, "PATH": ""}
        salidas.add(subprocess.run([sys.executable, "-c", codigo], env=env, capture_output=True,
                                   text=True, check=True).stdout)
    assert len(salidas) == 1


def test_format_ts_utc_milisegundos_y_z():
    assert format_ts(T0) == "2026-10-17T14:03:11.482Z"
    bogota = datetime(2026, 10, 17, 9, 3, 11, 482000, tzinfo=timezone(timedelta(hours=-5)))
    assert format_ts(bogota) == "2026-10-17T14:03:11.482Z"
    assert TS_RE.match(format_ts(datetime(2026, 1, 1, tzinfo=timezone.utc)))   # ms aunque sean 000
    assert parse_ts("2026-10-17T14:03:11.482Z") == T0
    with pytest.raises(ValueError):
        format_ts(datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        parse_ts("2026-10-17T14:03:11.482+00:00")


def test_build_envelope_forma_de_5_2():
    e = _env()
    assert list(e) == ["schema_version", "event_id", "origin", "source", "generator", "seq",
                       "event_time", "ingest_time", "author", "text", "lang", "brands", "hashtags",
                       "payload", "sim"]
    assert e["schema_version"] == "1.0"
    assert e["event_id"] == event_id("stream", "reddit", 7, 1207)
    assert TS_RE.match(e["event_time"]) and TS_RE.match(e["ingest_time"])
    assert e["author"] == {"user_id": "u_000481", "handle": "u/mariag_89", "followers": 412}
    assert e["hashtags"] == ["postobon", "colombia"]          # canónicos y en orden
    assert "#Postobón" in e["text"]                            # el texto conserva la forma original
    assert e["sim"] == {"label_sentiment": "positive", "scenario": None, "chaos": []}


def test_build_envelope_acepta_followers_nulo_y_hashtags_explicitos():
    e = _env(followers=None, hashtags=["a", "b"], lang=None, chaos=["null_fields"])
    assert e["author"]["followers"] is None and e["hashtags"] == ["a", "b"] and e["lang"] is None


def test_build_envelope_rechaza_incumplimientos():
    casos = [
        dict(origin="batch"), dict(source="mastodon"), dict(text="  "), dict(generator="comment-gen"),
        dict(lang="spa"), dict(followers=-1), dict(label_sentiment="positivo"),
        dict(brands=["Postobón"]), dict(brands=["pepsi", "pepsi"]), dict(chaos=["bug"]),
        dict(chaos=["late", "late"]), dict(hashtags=["Postobón"]), dict(hashtags=["a", "a"]),
    ]
    for kw in casos:
        with pytest.raises(ValueError):
            _env(**kw)


def test_pubsub_attributes():
    e = _env()
    assert pubsub_attributes(e) == {"event_id": e["event_id"], "source": "reddit",
                                    "origin": "stream", "schema_version": "1.0"}


def test_normalize_engagement_segun_5_2():
    assert normalize_engagement("reddit", {"score": 27, "num_comments": None}) == \
        {"likes": 27, "shares": None, "comments": None}
    assert normalize_engagement("reddit", {"score": -3, "num_comments": 4}) == \
        {"likes": -3, "shares": None, "comments": 4}
    assert normalize_engagement("facebook", {"reactions_total": 14, "shares_count": 2,
                                             "comments_count": 3}) == \
        {"likes": 14, "shares": 2, "comments": 3}
    assert normalize_engagement("instagram", {"like_count": 31, "comments_count": 4}) == \
        {"likes": 31, "shares": None, "comments": 4}
    with pytest.raises(ValueError):
        normalize_engagement("mastodon", {})
