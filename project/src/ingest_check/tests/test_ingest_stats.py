"""Métricas por fuente y criterios de HU-017 (sin Pub/Sub)."""

from __future__ import annotations

import json

import pytest

from ingest_check.stats import (
    FAIL,
    OK,
    WARN,
    CursorState,
    IngestStats,
    Thresholds,
    cursor_checks,
    measure_checks,
    parse_message,
    summarize,
    verdict,
)
from ingest_check.testing import T0, attributes_for, make_envelope, message, stream


def _collect(msgs, validator=None) -> IngestStats:
    st = IngestStats(validator=validator)
    for m in msgs:
        st.add(m.source, m.data, m.attributes, m.message_id, m.published_at + 0.1, m.published_at)
    return st


def _by_status(checks, name):
    return next(c for c in checks if c.name == name)


def test_parse_valid_message():
    m = message("reddit", 12, T0)
    o = parse_message("reddit", m.data, m.attributes, m.message_id, T0 + 1, T0)
    assert o.seq == 12 and o.body_source == "reddit" and o.body_ok
    assert o.event_id == m.attributes["event_id"]
    assert o.published_at == T0 and o.received_at == T0 + 1
    assert o.kind == "comment" and o.chaos == ()
    assert o.schema_errors is None


def test_parse_malformed_uses_attribute_event_id():
    env = make_envelope("facebook", 3)
    o = parse_message("facebook", b'{"schema_version": "1.0", "event_', attributes_for(env), "x", T0)
    assert o.malformed and o.seq is None
    assert o.effective_event_id == env["event_id"]
    assert o.published_at == T0  # sin publish_time se usa received_at


def test_parse_incomplete_and_non_object():
    o = parse_message("reddit", json.dumps({"event_id": "a", "source": "reddit"}).encode(), {}, "x", T0)
    assert o.missing == ("seq",) and not o.malformed
    o2 = parse_message("reddit", b"[1, 2]", {}, "y", T0)
    assert o2.malformed
    o3 = parse_message("reddit", b"\xff\xfe", {}, "z", T0)
    assert o3.malformed


def test_bool_seq_is_not_an_int():
    env = make_envelope("reddit", 1)
    env["seq"] = True
    o = parse_message("reddit", json.dumps(env).encode(), {}, "x", T0)
    assert o.seq is None


def test_validator_errors_are_captured_and_never_raise():
    m = message("reddit", 1, T0)

    def boom(_doc):
        raise RuntimeError("x")

    o = parse_message("reddit", m.data, m.attributes, "x", T0, validator=boom)
    assert o.schema_errors and "validador" in o.schema_errors[0]
    o2 = parse_message("reddit", m.data, m.attributes, "x", T0, validator=lambda d: [])
    assert o2.schema_errors == ()


def test_summary_rate_unique_and_seq():
    st = _collect(stream("instagram", 100, 60, T0))  # 60 msgs en 120 s
    s = summarize(st.observations(), "instagram", T0, T0 + 120)
    assert s.messages == 60 and s.unique_event_ids == 60
    assert s.rate == pytest.approx(0.5)
    assert (s.seq_min, s.seq_max, s.seq_gaps) == (100, 159, 0)
    checks = measure_checks(s, Thresholds())
    assert verdict(checks) == OK, [c for c in checks if c.status != OK]


def test_window_is_half_open_on_publish_time():
    st = _collect(stream("reddit", 0, 10, T0))  # T0 .. T0+18
    s = summarize(st.observations(), "reddit", T0 + 2, T0 + 10)
    assert s.seq_min == 1 and s.seq_max == 4 and s.messages == 4


def test_other_sources_are_ignored():
    st = _collect(stream("reddit", 0, 5, T0) + stream("facebook", 0, 7, T0))
    assert summarize(st.observations(), "reddit", T0, T0 + 100).messages == 5
    assert summarize(st.observations(), "facebook", T0, T0 + 100).messages == 7


def test_redelivery_is_not_a_duplicate():
    msgs = stream("reddit", 0, 20, T0)
    msgs.append(msgs[3])  # mismo message_id: reentrega de Pub/Sub
    s = summarize(_collect(msgs).observations(), "reddit", T0, T0 + 100)
    assert s.redeliveries == 1 and s.messages == 20 and s.duplicate_copies == 0


def test_chaos_duplicates_are_expected_but_plain_duplicates_are_not():
    msgs = stream("facebook", 0, 200, T0, interval=0.5)
    msgs.append(message("facebook", 5, T0 + 3, message_id="dup-chaos", chaos=["duplicate"]))
    msgs.append(message("facebook", 6, T0 + 4, message_id="dup-plain"))
    s = summarize(_collect(msgs).observations(), "facebook", T0, T0 + 100)
    assert s.duplicate_copies == 2 and s.chaos_duplicates == 1 and s.unexpected_duplicates == 1
    c = _by_status(measure_checks(s, Thresholds(rate_min=0, rate_max=10)), "event_id únicos")
    assert c.status == WARN  # ratio > 99 % pero hay un duplicado no marcado


def test_low_unique_ratio_fails():
    msgs = stream("reddit", 0, 20, T0)
    msgs += [message("reddit", i, T0 + 50 + i, message_id=f"again-{i}") for i in range(5)]
    s = summarize(_collect(msgs).observations(), "reddit", T0, T0 + 100)
    c = _by_status(measure_checks(s, Thresholds(rate_min=0, rate_max=10)), "event_id únicos")
    assert c.status == FAIL


def test_rate_out_of_range_fails():
    st = _collect(stream("reddit", 0, 60, T0, interval=1.0))  # 1 msg/s
    s = summarize(st.observations(), "reddit", T0, T0 + 60)
    assert _by_status(measure_checks(s, Thresholds()), "tasa").status == FAIL


def test_too_few_messages_short_circuits():
    s = summarize(_collect(stream("reddit", 0, 3, T0)).observations(), "reddit", T0, T0 + 120)
    checks = measure_checks(s, Thresholds())
    assert len(checks) == 1 and checks[0].status == FAIL


def test_malformed_fails_without_chaos_and_warns_with_chaos():
    msgs = stream("reddit", 0, 30, T0)
    env = make_envelope("reddit", 30)
    from ingest_check.testing import FakeMessage

    msgs.append(FakeMessage(T0 + 59, "reddit", b'{"broken', attributes_for(env), "bad"))
    s = summarize(_collect(msgs).observations(), "reddit", T0, T0 + 62)
    assert s.malformed == 1
    assert _by_status(measure_checks(s, Thresholds()), "cuerpo").status == FAIL
    assert _by_status(measure_checks(s, Thresholds(), expect_chaos=True), "cuerpo").status == WARN


def test_attribute_and_source_mismatch_fail():
    msgs = stream("instagram", 0, 30, T0)
    wrong = message("reddit", 999, T0 + 1, message_id="wrong-topic")  # cuerpo de reddit en instagram
    wrong.source = "instagram"
    msgs.append(wrong)
    bad_attr = message("instagram", 500, T0 + 2, message_id="bad-attr")
    bad_attr.attributes["event_id"] = "otro"
    msgs.append(bad_attr)
    s = summarize(_collect(msgs).observations(), "instagram", T0, T0 + 60)
    assert s.source_mismatch == 1 and s.attr_mismatch == 1
    assert _by_status(measure_checks(s, Thresholds()), "atributos").status == FAIL


def test_seq_gaps_are_a_warning():
    msgs = [m for i, m in enumerate(stream("reddit", 0, 40, T0)) if i not in (10, 11)]
    s = summarize(_collect(msgs).observations(), "reddit", T0, T0 + 80)
    assert s.seq_gaps == 2
    assert _by_status(measure_checks(s, Thresholds()), "seq").status == WARN


def test_schema_invalid_counts():
    st = _collect(stream("reddit", 0, 30, T0), validator=lambda d: ["x: malo"] if d["seq"] % 10 == 0 else [])
    s = summarize(st.observations(), "reddit", T0, T0 + 60)
    assert s.schema_checked and s.schema_invalid == 3
    assert _by_status(measure_checks(s, Thresholds()), "schema").status == FAIL


def test_cursor_checks():
    th = Thresholds()
    assert cursor_checks("reddit", None, 10, th)[0].status == WARN
    ok = cursor_checks("reddit", CursorState(7, 150), 180, th, expected_seed=7)
    assert verdict(ok) == OK
    far = cursor_checks("reddit", CursorState(7, 10), 500, th)
    assert far[0].status == WARN
    seed = cursor_checks("reddit", CursorState(8, 150), 180, th, expected_seed=7)
    assert verdict(seed) == FAIL


def test_to_dict_is_json_serializable():
    s = summarize(_collect(stream("reddit", 0, 15, T0)).observations(), "reddit", T0, T0 + 30)
    json.dumps(s.to_dict())
