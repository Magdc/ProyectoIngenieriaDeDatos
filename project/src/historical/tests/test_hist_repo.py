"""Contra el código real de los lotes 1.1–2.3 y ``contracts/``.

Se omiten solo si falta una pieza (fuera del repo). En el repositorio, con el entorno de
``docs/validation/validation-3-1.md`` (Faker y PyYAML del simulador, pyarrow, jsonschema
y Beam), deben correr todas: es la prueba de que los adaptadores encajan con las firmas
reales antes de gastar tiempo en una corrida completa.
"""

from __future__ import annotations

import datetime as dt
import importlib
import inspect
import io
import re

import pytest

from historical.plan import UTC
from historical.verify import event_id_formula

NOW = dt.datetime(2026, 6, 1, 15, 30, 0, 250000, tzinfo=UTC)


def _need(*modules: str) -> None:
    for name in modules:
        try:
            importlib.import_module(name)
        except ImportError as exc:
            pytest.skip(f"falta {name} ({exc}); fuera del repo")


@pytest.fixture(scope="module")
def envs():
    _need("simulator.factory", "simulator.sim_config", "common.envelope")
    from historical.adapters import SimulatorEnvelopes

    return SimulatorEnvelopes.build(3101)


@pytest.fixture(scope="module")
def validators():
    _need("jsonschema", "referencing")
    from historical.schema import SchemaUnavailable, load_validators

    try:
        return load_validators()
    except SchemaUnavailable as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope="module")
def dev_key():
    _need("common.hmac_key")
    from historical.adapters import load_key

    return load_key("local_dev")


def test_simulador_real_produce_envelopes_historicos_validos(envs, validators):
    for source in ("reddit", "facebook", "instagram"):
        for seq in range(0, 60, 3):
            env = envs.envelope(source, seq, NOW)
            assert env["origin"] == "historical" and env["source"] == source and env["seq"] == seq
            assert env["event_id"] == event_id_formula("historical", source, 3101, seq)
            assert validators["envelope"](env) == [], (source, seq)
            assert env["sim"]["chaos"] == []


def test_simulador_real_es_determinista(envs):
    a = envs.envelope("facebook", 17, NOW)
    b = envs.envelope("facebook", 17, NOW)
    assert a == b


def test_mismo_pool_de_usuarios_que_el_stream(envs):
    from historical.adapters import SimulatorEnvelopes

    stream = SimulatorEnvelopes.build(7)
    for source in ("reddit", "facebook", "instagram"):
        hist = {envs.envelope(source, s, NOW)["author"]["user_id"] for s in range(400)}
        live = {stream.envelope(source, s, NOW)["author"]["user_id"] for s in range(400)}
        assert all(re.match(r"^u_\d{6}$", u) for u in hist)
        assert hist & live, f"{source}: el histórico no comparte usuarios con el stream"


def test_anonimizacion_real_y_contrato_enriquecido(envs, validators, dev_key):
    from historical.adapters import make_enricher
    from historical.work import HEX64_CHECK, find_identity_keys

    enrich = make_enricher(dev_key.value, dt.datetime.now(UTC))
    by_user: dict[str, set[str]] = {}
    for source in ("reddit", "facebook", "instagram"):
        for seq in range(30):
            env = envs.envelope(source, seq, NOW)
            out = enrich(env)
            assert not find_identity_keys(out)
            uh = out["author"]["user_hash"]
            assert HEX64_CHECK.match(uh)
            by_user.setdefault(env["author"]["user_id"], set()).add(uh)
            out.update(sentiment_label="neutral", sentiment_score=0.5, emotion_label="neutral",
                       emotion_score=0.5,
                       nlp_model_version="robertuito-sentiment@aaaaaaaaaaaa+robertuito-emotion@bbbbbbbbbbbb")
            assert validators["enriched"](out) == [], (source, seq)
    assert all(len(h) == 1 for h in by_user.values())


def test_fila_y_parquet_reales(envs, dev_key):
    _need("pyarrow", "common.parquet_rows", "common.enriched_row")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from historical.adapters import make_enricher, posts_enriched_columns, rows_to_parquet, to_row

    enrich = make_enricher(dev_key.value, dt.datetime.now(UTC))
    rows = []
    for source in ("reddit", "facebook", "instagram"):
        e = enrich(envs.envelope(source, 5, NOW))
        e.update(sentiment_label="positive", sentiment_score=0.9, emotion_label="joy",
                 emotion_score=0.8, nlp_model_version="robertuito-sentiment@x+robertuito-emotion@y")
        rows.append(to_row(e))
    table = pq.read_table(io.BytesIO(rows_to_parquet(rows)))
    assert table.schema.names == posts_enriched_columns()
    assert len(table.schema.names) == 24
    t = table.schema.field("event_time").type
    assert pa.types.is_timestamp(t) and t.unit == "ms"
    out = table.to_pylist()
    assert {r["origin"] for r in out} == {"historical"}
    assert all(r["sentiment_label"] == "positive" for r in out)
    assert out[0]["event_time"].strftime("%Y-%m-%dT%H") == "2026-06-01T15"


def test_pipeline_real_pequeno(tmp_path, envs, validators, dev_key, cfg_factory, as_of):
    _need("pyarrow", "common.parquet_rows")
    from historical import adapters
    from historical.generate import generate
    from historical.nlp_batch import run_nlp_stage
    from historical.testing import FakeNlpEngine
    from historical.verify import FALLA, run_checks
    from historical.work import WorkDir
    from historical.writer import write_output

    cfg = cfg_factory(n=300, days=60, chunk_size=100)
    work = WorkDir(tmp_path, cfg.seed)
    pt = dt.datetime.now(UTC)
    gen = generate(cfg, as_of, work, envelopes=envs,
                   enrich=adapters.make_enricher(dev_key.value, pt),
                   validate_envelope=validators["envelope"], process_time=pt, progress_every=0)
    assert gen["rows"] == 300 and gen["invalid"] == 0
    run_nlp_stage(work, FakeNlpEngine(), to_row=adapters.to_row)
    write_output(work, to_row=adapters.to_row, rows_to_parquet=adapters.rows_to_parquet,
                 validate_enriched=validators["enriched"])
    report = run_checks(work, columns=adapters.posts_enriched_columns(), allow_fake_nlp=True)
    failed = [c for c in report["checks"] if c["status"] == FALLA]
    assert not failed, failed


def test_marcas_del_plan_iguales_a_brands_yaml():
    _need("simulator.generator.brands")
    from simulator.generator.brands import load_brands

    from historical.plan import load_config

    brands = load_brands()
    keys = getattr(brands, "keys")
    keys = set(keys() if callable(keys) else keys)
    assert set(load_config().brands) == keys


def test_streaming_nlp_expone_nlp_from_options():
    _need("apache_beam", "streaming.nlp")
    from streaming import nlp

    assert callable(getattr(nlp, "nlp_from_options", None))
    assert len(inspect.signature(nlp.nlp_from_options).parameters) >= 1
