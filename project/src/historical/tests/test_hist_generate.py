"""Etapa 1 (generate) con dobles: aceptación, digest, anonimización y abortos."""

from __future__ import annotations

import datetime as dt
import gzip
import json

import pytest

from historical.generate import GenerateError, accepted_events, generate
from historical.testing import FakeEnvelopes, make_fake_enricher
from historical.work import WorkDir, read_jsonl_gz

PT = dt.datetime(2026, 10, 23, 18, tzinfo=dt.timezone.utc)


def _gen(tmp_path, cfg, as_of, **kw):
    work = WorkDir(tmp_path, cfg.seed)
    kw.setdefault("enrich", make_fake_enricher("k", PT))
    kw.setdefault("envelopes", FakeEnvelopes(cfg.seed))
    return work, generate(cfg, as_of, work, process_time=PT, progress_every=0, **kw)


def test_genera_n_exacto_y_seq_creciente(tmp_path, small_cfg, as_of):
    work, res = _gen(tmp_path, small_cfg, as_of)
    assert res["rows"] == 900
    assert {s: v["accepted"] for s, v in res["sources"].items()} == {
        "reddit": 300, "facebook": 300, "instagram": 300}
    for seqs in res["seqs"].values():
        assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    files = work.stage1_files()
    assert [p.name for p in files][:2] == ["facebook-0000.jsonl.gz", "facebook-0001.jsonl.gz"]
    rows = [r for p in files for r in read_jsonl_gz(p)]
    assert len(rows) == 900
    times = [r["event_time"] for r in rows if r["source"] == "reddit"]
    assert times == sorted(times)


def test_digest_reproducible_y_sensible_a_la_semilla(tmp_path, small_cfg, as_of):
    _, a = _gen(tmp_path / "a", small_cfg, as_of)
    _, b = _gen(tmp_path / "b", small_cfg, as_of)
    assert a["digest"] == b["digest"]
    _, c = _gen(tmp_path / "c", small_cfg.replace(seed=99), as_of)
    assert c["digest"] != a["digest"]


def test_digest_only_no_escribe_ni_anonimiza(tmp_path, small_cfg, as_of):
    work = WorkDir(tmp_path, small_cfg.seed)
    res = generate(small_cfg, as_of, work, envelopes=FakeEnvelopes(small_cfg.seed), enrich=None,
                   digest_only=True, progress_every=0)
    _, full = _gen(tmp_path / "full", small_cfg, as_of)
    assert res["digest"] == full["digest"]
    assert not work.dir.exists()


def test_disco_sin_identidad_en_claro(tmp_path, small_cfg, as_of):
    work, res = _gen(tmp_path, small_cfg, as_of)
    for path in work.stage1_files():
        text = gzip.decompress(path.read_bytes()).decode()
        assert '"user_id"' not in text and '"handle"' not in text
    assert res["users"]["user_ids_with_several_hashes"] == 0
    assert res["users"]["hashes_with_several_user_ids"] == 0


def test_aborta_si_enrich_deja_identidad(tmp_path, small_cfg, as_of):
    def leaky(env):
        return dict(env)

    with pytest.raises(GenerateError, match="identidad en claro"):
        _gen(tmp_path, small_cfg, as_of, enrich=leaky)


def test_aborta_si_la_llave_no_coincide_y_no_publica_etapa(tmp_path, small_cfg, as_of):
    calls = []

    def key_check(hashes):
        calls.append(len(hashes))
        return {"status": "FALLA", "detail": "0 coincidencias"}

    with pytest.raises(GenerateError, match="llave HMAC"):
        _gen(tmp_path, small_cfg, as_of, key_check=key_check)
    work = WorkDir(tmp_path, small_cfg.seed)
    assert calls and calls[0] <= 500
    assert not work.stage1.exists() and not work.generate_json.exists()


def test_aborta_con_envelopes_invalidos(tmp_path, small_cfg, as_of):
    with pytest.raises(GenerateError, match="envelope.v1"):
        _gen(tmp_path, small_cfg, as_of, validate_envelope=lambda env: ["text: vacío"])


def test_regenerar_borra_etapas_siguientes(tmp_path, small_cfg, as_of):
    work, _ = _gen(tmp_path, small_cfg, as_of)
    work.stage2.mkdir(parents=True)
    (work.stage2 / "reddit-0000.jsonl.gz").write_bytes(b"viejo")
    work.output_json.write_text("{}")
    _gen(tmp_path, small_cfg, as_of)
    assert not list(work.stage2.iterdir())
    assert not work.output_json.exists()


def test_tope_de_candidatos_fuerza_la_aceptacion(small_cfg, as_of):
    cfg = small_cfg.replace(max_candidates_per_slot=1, n=30)

    class NoBrand(FakeEnvelopes):
        def envelope(self, source, seq, now):
            env = super().envelope(source, seq, now)
            env["brands"] = ["postobon"]
            env["sim"]["label_sentiment"] = "positive"
            return env

    stats: dict = {}
    out = list(accepted_events(cfg, as_of, NoBrand(cfg.seed), "reddit", 10, stats))
    assert len(out) == 10
    assert stats["candidates"] == 10
    assert stats["forced"] + 0 <= 10


def test_generate_json_guarda_plan_llave_y_seq(tmp_path, small_cfg, as_of):
    work, res = _gen(tmp_path, small_cfg, as_of, key_info={"source": "x", "fingerprint": "ab"})
    saved = json.loads(work.generate_json.read_text())
    assert saved["digest"] == res["digest"]
    assert saved["plan"]["as_of"] == "2026-10-23T00:00:00.000Z"
    assert saved["key"] == {"source": "x", "fingerprint": "ab"}
    assert set(saved["seqs"]) == {"reddit", "facebook", "instagram"}
    assert saved["process_time"] == "2026-10-23T18:00:00.000Z"
