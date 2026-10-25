"""Mensajes de prueba para verificar el pipeline (centinela, duplicado y defectos).

Cada corrida tiene un `run_id`; todos los mensajes lo llevan (en el texto o en
el cuerpo truncado) para encontrarlos en `raw/` y `dlq/` entre el tráfico
normal del simulador. El centinela usa una semilla reservada (`SENTINEL_SEED`)
y `generator = "ta-verify@2.1"`, así que no choca con ningún `event_id` del
simulador ni del histórico (§3.6).
"""

from __future__ import annotations

import json
import random
import string
import time
import uuid
from dataclasses import dataclass, field

from streaming.records import (
    ERROR_ATTRIBUTES,
    ERROR_ENCODING,
    ERROR_JSON,
    ERROR_ROUTING,
    ERROR_SCHEMA,
    format_ts,
)

SENTINEL_SEED = 990001
SENTINEL_GENERATOR = "ta-verify@2.1"
_B36 = string.digits + string.ascii_lowercase


def event_id(origin: str, source: str, seed: int, seq: int) -> str:
    """Misma fórmula que `common.envelope.event_id()` (§3.6)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def attributes_for(envelope: dict) -> dict:
    """Atributos de §5.3."""
    return {k: str(envelope[k]) for k in ("event_id", "source", "origin", "schema_version")}


def _b36(rng: random.Random, n: int = 7) -> str:
    return "".join(rng.choice(_B36) for _ in range(n))


def sentinel_envelope(run_id: str, seq: int, now: float | None = None) -> dict:
    """Envelope v1.0 válido de Reddit (comentario) que identifica la corrida."""
    now = time.time() if now is None else now
    rng = random.Random(f"{SENTINEL_SEED}|reddit|{seq}")
    post, comment = _b36(rng), _b36(rng)
    return {
        "schema_version": "1.0",
        "event_id": event_id("stream", "reddit", SENTINEL_SEED, seq),
        "origin": "stream",
        "source": "reddit",
        "generator": SENTINEL_GENERATOR,
        "seq": seq,
        "event_time": format_ts(now - 1),
        "ingest_time": format_ts(now),
        "author": {"user_id": "u_999999", "handle": "u/ta_verify", "followers": 0},
        "text": f"Centinela de verificación {run_id}: Postobón bien fría.",
        "lang": "es",
        "brands": ["postobon"],
        "hashtags": [],
        "payload": {
            "reddit_id": f"t1_{comment}",
            "kind": "comment",
            "subreddit": "r/colombia",
            "title": None,
            "parent_id": f"t3_{post}",
            "score": 1,
            "upvote_ratio": None,
            "num_comments": None,
            "permalink": f"/r/colombia/comments/{post}/_/{comment}/",
            "flair": None,
        },
        "sim": {"label_sentiment": None, "scenario": None, "chaos": []},
    }


def dumps(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@dataclass
class Injection:
    """Un mensaje a publicar y lo que se espera de él."""

    name: str
    topic_source: str             # fuente del topic donde se publica
    data: bytes
    attributes: dict
    expect: str                   # "raw" o un tipo de error de la DLQ
    event_id: str | None = None


@dataclass
class Plan:
    run_id: str
    sentinel_id: str
    sentinel: dict
    messages: list[Injection] = field(default_factory=list)

    def expected_errors(self) -> dict[str, str]:
        """`nombre → tipo de error` de los mensajes que deben ir a la DLQ."""
        return {m.name: m.expect for m in self.messages if m.expect != "raw"}

    def manifest(self) -> dict:
        return {
            "run_id": self.run_id,
            "sentinel_event_id": self.sentinel_id,
            "messages": [{"name": m.name, "topic_source": m.topic_source, "expect": m.expect,
                          "event_id": m.event_id} for m in self.messages],
        }


def build_plan(run_id: str | None = None, now: float | None = None) -> Plan:
    """Centinela (×2, para probar la deduplicación) y un mensaje por tipo de error."""
    now = time.time() if now is None else now
    run_id = run_id or time.strftime("%Y%m%d%H%M%S", time.gmtime(now)) + f"-{uuid.uuid4().hex[:4]}"
    base_seq = int(now * 1000) % 10**12  # único por corrida y estable dentro de ella

    sentinel = sentinel_envelope(run_id, base_seq, now)
    sid = sentinel["event_id"]
    attrs = attributes_for(sentinel)
    plan = Plan(run_id=run_id, sentinel_id=sid, sentinel=sentinel)
    add = plan.messages.append

    add(Injection("sentinel", "reddit", dumps(sentinel), attrs, "raw", sid))
    add(Injection("sentinel_duplicate", "reddit", dumps(sentinel), attrs, "raw", sid))

    # json: cuerpo truncado (como el caos `malformed`, §3.9).
    broken = sentinel_envelope(run_id, base_seq + 1, now)
    text = dumps(broken).decode("utf-8")  # se corta por caracteres, no por bytes (sigue siendo UTF-8)
    add(Injection("truncated_json", "reddit", text[: len(text) // 2].encode("utf-8"),
                  attributes_for(broken), ERROR_JSON, broken["event_id"]))

    # schema: falta un campo obligatorio.
    missing = sentinel_envelope(run_id, base_seq + 2, now)
    missing.pop("event_time")
    add(Injection("missing_field", "reddit", dumps(missing), attributes_for(missing),
                  ERROR_SCHEMA, missing["event_id"]))

    # attributes: el atributo `source` no coincide con el cuerpo.
    mismatch = sentinel_envelope(run_id, base_seq + 3, now)
    bad_attrs = {**attributes_for(mismatch), "source": "facebook"}
    add(Injection("attribute_mismatch", "reddit", dumps(mismatch), bad_attrs,
                  ERROR_ATTRIBUTES, mismatch["event_id"]))

    # routing: envelope válido de Reddit publicado en el topic de Facebook.
    routed = sentinel_envelope(run_id, base_seq + 4, now)
    add(Injection("wrong_topic", "facebook", dumps(routed), attributes_for(routed),
                  ERROR_ROUTING, routed["event_id"]))

    # encoding: bytes que no son UTF-8.
    enc_id = event_id("stream", "reddit", SENTINEL_SEED, base_seq + 5)
    add(Injection("not_utf8", "reddit",
                  b"\xff\xfe ta-verify " + f"{run_id} {enc_id}".encode("ascii"),
                  {"event_id": enc_id, "source": "reddit", "origin": "stream",
                   "schema_version": "1.0"}, ERROR_ENCODING, enc_id))
    return plan


def publish(plan: Plan, project: str, topic_template: str = "ta-raw-{source}",
            publisher=None) -> list[str]:
    """Publica el plan; devuelve los `message_id`. Respeta `PUBSUB_EMULATOR_HOST`."""
    if publisher is None:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
    ids = []
    for msg in plan.messages:
        topic = publisher.topic_path(project, topic_template.format(source=msg.topic_source))
        ids.append(publisher.publish(topic, msg.data, **msg.attributes).result(timeout=60))
    return ids
