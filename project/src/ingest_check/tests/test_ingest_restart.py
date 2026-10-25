"""Reglas de ``restart_checks`` y lectura del cursor."""

from __future__ import annotations

import json

import pytest

from ingest_check.cursor import LocalCursorReader, NullCursorReader, parse_cursor, read_all
from ingest_check.stats import FAIL, OK, WARN, CursorState, IngestStats, Thresholds, restart_checks, summarize, verdict
from ingest_check.testing import T0, stream

TH = Thresholds()


def _obs(msgs):
    st = IngestStats()
    for m in msgs:
        st.add(m.source, m.data, m.attributes, m.message_id, m.published_at + 0.1, m.published_at)
    return st.observations()


def _check(before_seq0, before_n, after_seq0, after_n, **kw):
    msgs = stream("reddit", before_seq0, before_n, T0) + stream("reddit", after_seq0, after_n, T0 + 200)
    obs = _obs(msgs)
    t1, t2, t3 = T0 + 150, T0 + 190, T0 + 400
    return restart_checks(
        "reddit",
        summarize(obs, "reddit", T0, t1),
        summarize(obs, "reddit", t1, t3),
        summarize(obs, "reddit", t2, t3),
        TH, obs, before_window=(T0, t1), resumed_window=(t1, t3), **kw,
    )


def _named(checks, name):
    return next(c for c in checks if c.name == name)


def test_continues_from_cursor_with_small_repeat():
    checks = _check(100, 60, 140, 60)  # antes 100..159, después 140..199 (20 repetidos)
    assert _named(checks, "reinicio").status == OK
    assert _named(checks, "repetidos").status == OK
    assert "20 event_id" in _named(checks, "repetidos").detail
    assert verdict(checks) == OK


def test_reset_to_zero_fails():
    checks = _check(100, 60, 0, 60)
    assert _named(checks, "reinicio").status == FAIL


def test_repeat_beyond_checkpoint_fails():
    checks = _check(100, 60, 105, 60)  # vuelve 54 atrás: más que la tolerancia de 50
    assert _named(checks, "reinicio").status == FAIL


def test_small_before_is_inconclusive():
    checks = _check(0, 20, 5, 60)
    assert _named(checks, "reinicio").status == WARN


def test_no_progress_after_restart_fails():
    checks = _check(100, 60, 120, 20)  # después 120..139, nunca supera 159
    assert _named(checks, "progreso").status == FAIL


def test_cursor_before_after():
    ok = _check(100, 60, 150, 60, cursor_before=CursorState(7, 150), cursor_after=CursorState(7, 200))
    assert _named(ok, "cursor").status == OK
    back = _check(100, 60, 150, 60, cursor_before=CursorState(7, 150), cursor_after=CursorState(7, 0))
    assert _named(back, "cursor").status == FAIL
    seed = _check(100, 60, 150, 60, cursor_before=CursorState(7, 150), cursor_after=CursorState(9, 200))
    assert _named(seed, "cursor").status == FAIL


def test_parse_cursor():
    c = parse_cursor('{"seed": 7, "seq": 1207, "updated_at": "2026-10-17T14:03:13.020Z"}')
    assert c == CursorState(7, 1207, "2026-10-17T14:03:13.020Z")
    for bad in ("no json", "[]", '{"seed": 7}', '{"seed": "7", "seq": 1}', '{"seed": 7, "seq": true}'):
        with pytest.raises(ValueError):
            parse_cursor(bad)


@pytest.mark.parametrize("nested", [False, True])
def test_local_reader(tmp_path, nested):
    folder = tmp_path / "sim-state" if nested else tmp_path
    folder.mkdir(exist_ok=True)
    (folder / "reddit.json").write_text(json.dumps({"seed": 7, "seq": 42}), encoding="utf-8")
    (folder / "facebook.json").write_text("roto", encoding="utf-8")
    reader = LocalCursorReader(tmp_path)
    got = read_all(reader, ["reddit", "facebook", "instagram"])
    assert got == {"reddit": CursorState(7, 42), "facebook": None, "instagram": None}
    assert str(tmp_path) in reader.describe()


def test_null_reader():
    assert read_all(NullCursorReader(), ["reddit"]) == {"reddit": None}


def test_gcs_reader_uses_sim_state_prefix(monkeypatch):
    storage = pytest.importorskip("google.cloud.storage")
    from ingest_check.cursor import GcsCursorReader

    blobs = {"sim-state/reddit.json": b'{"seed": 7, "seq": 99}'}
    seen = {}

    class Blob:
        def __init__(self, name):
            self.name = name

        def exists(self):
            return self.name in blobs

        def download_as_bytes(self):
            return blobs[self.name]

    class Bucket:
        def blob(self, name):
            return Blob(name)

    class Client:
        def __init__(self, project=None):
            seen["project"] = project

        def bucket(self, name):
            seen["bucket"] = name
            return Bucket()

    monkeypatch.setattr(storage, "Client", Client)
    reader = GcsCursorReader("p-ta-datasets", project="p")
    assert seen == {"project": "p", "bucket": "p-ta-datasets"}
    assert reader.read("reddit") == CursorState(7, 99)
    assert reader.read("facebook") is None
    assert reader.describe() == "gs://p-ta-datasets/sim-state/"
