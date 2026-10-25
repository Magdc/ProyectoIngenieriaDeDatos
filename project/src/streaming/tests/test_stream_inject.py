import uuid

import pytest

from streaming.inject import (
    SENTINEL_GENERATOR,
    SENTINEL_SEED,
    build_plan,
    event_id,
    publish,
    sentinel_envelope,
)
from streaming.records import DlqRecord, ERROR_TYPES, ValidEvent


def test_event_id_sigue_la_formula_de_3_6():
    expected = uuid.uuid5(uuid.NAMESPACE_URL, "stream|reddit|7|1207")
    assert event_id("stream", "reddit", 7, 1207) == str(expected)


def test_centinela_es_reproducible_e_identificable():
    a = sentinel_envelope("r1", 55, now=1_800_000_000.0)
    b = sentinel_envelope("r1", 55, now=1_800_000_000.0)
    assert a == b
    assert a["generator"] == SENTINEL_GENERATOR
    assert a["event_id"] == event_id("stream", "reddit", SENTINEL_SEED, 55)
    assert "r1" in a["text"]
    assert a["payload"]["reddit_id"].startswith("t1_")
    assert a["payload"]["parent_id"].startswith("t3_")


def test_plan_cubre_cada_tipo_de_error_y_el_duplicado():
    plan = build_plan("run-1", now=1_800_000_000.0)
    names = [m.name for m in plan.messages]
    assert names.count("sentinel") == 1 and "sentinel_duplicate" in names
    assert sorted(plan.expected_errors().values()) == sorted(ERROR_TYPES)
    ids = [m.event_id for m in plan.messages if m.expect != "raw"]
    assert len(set(ids)) == len(ids) and plan.sentinel_id not in ids
    assert plan.manifest()["sentinel_event_id"] == plan.sentinel_id


def _results(validators, plan):
    out = {}
    for msg in plan.messages:
        out[msg.name] = validators.check(msg.topic_source, msg.data, msg.attributes,
                                         now=1_800_000_000.0)
    return out


def _assert_plan_outcomes(validators):
    plan = build_plan("run-2", now=1_800_000_000.0)
    results = _results(validators, plan)
    for msg in plan.messages:
        result = results[msg.name]
        if msg.expect == "raw":
            assert isinstance(result, ValidEvent), (msg.name, result)
            assert result.event_id == plan.sentinel_id
        else:
            assert isinstance(result, DlqRecord), msg.name
            assert result.error_type == msg.expect, (msg.name, result.error_detail)
            # El verificador encuentra cada registro por su event_id dentro de raw_message.
            assert msg.event_id in result.raw_message, msg.name


def test_resultados_esperados_con_schema_minimo(mini_validators):
    _assert_plan_outcomes(mini_validators)


def test_resultados_esperados_con_los_contratos_reales(contracts_bundle):
    """El centinela cumple envelope.v1.schema.json y cada defecto cae en su error_type."""
    from streaming.validate import Validators

    _assert_plan_outcomes(Validators(contracts_bundle))


def test_publish_usa_el_topic_de_cada_mensaje():
    class FakeFuture:
        def __init__(self, n):
            self.n = n

        def result(self, timeout=None):
            return str(self.n)

    class FakePublisher:
        def __init__(self):
            self.calls = []

        def topic_path(self, project, topic):
            return f"projects/{project}/topics/{topic}"

        def publish(self, topic, data, **attrs):
            self.calls.append((topic, data, attrs))
            return FakeFuture(len(self.calls))

    plan = build_plan("run-3")
    fake = FakePublisher()
    ids = publish(plan, "ta-local", publisher=fake)
    assert ids == [str(i) for i in range(1, len(plan.messages) + 1)]
    topics = [c[0] for c in fake.calls]
    assert topics.count("projects/ta-local/topics/ta-raw-facebook") == 1
    assert topics.count("projects/ta-local/topics/ta-raw-reddit") == len(plan.messages) - 1
    assert all(isinstance(v, str) for _, _, attrs in fake.calls for v in attrs.values())


@pytest.mark.parametrize("seq", [0, 1, 999_999_999_999])
def test_centinela_valido_para_cualquier_seq(mini_validators, seq):
    env = sentinel_envelope("r", seq, now=1_800_000_000.0)
    from streaming.inject import attributes_for, dumps

    assert isinstance(mini_validators.check("reddit", dumps(env), attributes_for(env)), ValidEvent)
