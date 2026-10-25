"""Rama enriched completa con TestStream en el DirectRunner (Lote 2.2). Sin red."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from common.enriched_row import COLUMN_NAMES
from common.hmac_key import KeySource
from common.parquet_rows import parquet_bytes_to_rows
from streaming import enriched
from streaming.enriched import EnrichedConfig, EnrichFn, attach_enriched, envelope_of, partition_path
from streaming.enriched_testing import TEST_KEY, FakeValidEvent, make_envelope

FIXED_NOW = datetime(2026, 10, 17, 14, 3, 20, 0, tzinfo=timezone.utc)


def fixed_clock():
    return FIXED_NOW


def _options(*argv):
    # Igual que el Lote 2.1: el DirectRunner clásico (Prism no admite todo y descarga un binario).
    opts = PipelineOptions(["--runner=BundleBasedDirectRunner", *argv])
    opts.view_as(StandardOptions).streaming = True
    return opts


def _config(root, bq=None):
    return EnrichedConfig(
        enabled=True,
        root=str(root),
        prefix="enriched",
        window_seconds=60,
        bq_table=bq,
        key_source=KeySource("env", env_var="TA_TEST_HMAC"),
        runner="DirectRunner",
    )


def _envelopes(n=12):
    return [make_envelope(s, i) for i, s in enumerate(["reddit", "facebook", "instagram"] * (n // 3))]


def _read_lake(root):
    files, rows = [], []
    for dirpath, _d, names in os.walk(os.path.join(root, "enriched")):
        for name in names:
            path = os.path.join(dirpath, name)
            files.append(os.path.relpath(path, root))
            with open(path, "rb") as fh:
                rows.extend(parquet_bytes_to_rows(fh.read()))
    return sorted(files), rows


def test_envelope_of_acepta_las_formas_del_lote_2_1():
    env = make_envelope("reddit", 1)
    raw = json.dumps(env).encode()
    assert envelope_of(env) == env
    assert envelope_of(raw) == env
    assert envelope_of(raw.decode()) == env
    assert envelope_of(("k", FakeValidEvent(env))) == env
    assert envelope_of(FakeValidEvent(env)) == env

    class WithDict:
        def __init__(self):
            self.envelope = env

    assert envelope_of(WithDict()) == env
    with pytest.raises(TypeError, match="no reconocido"):
        envelope_of(object())


def test_rama_completa_escribe_parquet_anonimizado(tmp_path, monkeypatch):
    monkeypatch.setenv("TA_TEST_HMAC", TEST_KEY)
    envs = _envelopes(12)
    elements = [FakeValidEvent(e) for e in envs[:6]] + envs[6:]  # mezcla de formas
    stream = TestStream().advance_watermark_to(0)
    for i, e in enumerate(elements):
        stream = stream.add_elements([TimestampedValue(e, 10 + i)])
    stream = stream.advance_watermark_to_infinity()

    with beam.Pipeline(options=_options()) as p:
        unique = p | stream
        attach_enriched(unique, config=_config(tmp_path))

    files, rows = _read_lake(tmp_path)
    assert len(rows) == 12
    assert {r["event_id"] for r in rows} == {e["event_id"] for e in envs}
    for r in rows:
        assert tuple(r) == COLUMN_NAMES
        assert "user_id" not in r and "handle" not in r
    sources = {f.split(os.sep)[1] for f in files}
    assert sources == {"source=reddit", "source=facebook", "source=instagram"}
    for f in files:
        name = os.path.basename(f)
        assert name.startswith("part-") and name.endswith("Z-p000.parquet")
        assert not name.startswith(".")


def test_elemento_invalido_se_cuenta_y_no_detiene_el_pipeline(tmp_path, monkeypatch):
    monkeypatch.setenv("TA_TEST_HMAC", TEST_KEY)
    bad = make_envelope("reddit", 99)
    del bad["author"]["user_id"]
    stream = TestStream().add_elements([make_envelope("reddit", 1), bad, object()]).advance_watermark_to_infinity()
    with beam.Pipeline(options=_options()) as p:
        attach_enriched(p | stream, config=_config(tmp_path))
    _files, rows = _read_lake(tmp_path)
    assert len(rows) == 1


def test_enrichfn_lee_la_llave_en_setup(monkeypatch):
    monkeypatch.setenv("TA_TEST_HMAC", TEST_KEY)
    fn = EnrichFn(KeySource("env", env_var="TA_TEST_HMAC"), clock=fixed_clock)
    assert fn._key is None  # no viaja serializada
    fn.setup()
    (row,) = list(fn.process(make_envelope("facebook", 2)))
    assert row["process_time"] == "2026-10-17T14:03:20.000Z"


def test_partition_path():
    p = partition_path("gs://b-ta-lake", "enriched", "reddit", datetime(2026, 10, 17, 14, 5, tzinfo=timezone.utc), 1)
    assert p == "gs://b-ta-lake/enriched/source=reddit/dt=2026-10-17/hr=14/part-20261017T140500Z-p001.parquet"


def test_write_bytes_local_es_atomico_e_idempotente(tmp_path):
    path = str(tmp_path / "a" / "b" / "part-x.parquet")
    enriched.write_bytes(path, b"uno")
    enriched.write_bytes(path, b"uno")
    assert open(path, "rb").read() == b"uno"
    assert os.listdir(tmp_path / "a" / "b") == ["part-x.parquet"]


_BQ_ROWS: list = []


def _record_bq_row(row):
    _BQ_ROWS.append(row)  # función de módulo: el runner la serializa por referencia


class _FakeWriteToBigQuery(beam.PTransform):
    instances: list = []
    rows = _BQ_ROWS

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        _FakeWriteToBigQuery.instances.append(self)

    def expand(self, pcoll):
        return pcoll | beam.Map(_record_bq_row)


def test_sink_de_bigquery_streaming_inserts(tmp_path, monkeypatch):
    monkeypatch.setenv("TA_TEST_HMAC", TEST_KEY)
    _FakeWriteToBigQuery.instances.clear()
    _FakeWriteToBigQuery.rows.clear()
    fake = _FakeWriteToBigQuery
    fake.Method = beam.io.WriteToBigQuery.Method
    monkeypatch.setattr(beam.io, "WriteToBigQuery", fake)
    stream = TestStream().add_elements(_envelopes(6)).advance_watermark_to_infinity()
    with beam.Pipeline(options=_options()) as p:
        attach_enriched(p | stream, config=_config(tmp_path, bq="p1:ta_stream.posts_enriched"))
    (inst,) = fake.instances
    assert inst.kwargs["table"] == "p1:ta_stream.posts_enriched"
    assert inst.kwargs["method"] == beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
    assert inst.kwargs["create_disposition"] == beam.io.BigQueryDisposition.CREATE_NEVER
    assert len(fake.rows) == 6
    for r in fake.rows:
        assert isinstance(r["payload"], str) and json.loads(r["payload"])
        assert r["event_time"].endswith("Z")
        assert "handle" not in r


def test_apagada_no_agrega_nada(tmp_path):
    cfg = EnrichedConfig(False, str(tmp_path), "enriched", 60, None, KeySource("local_dev"), "DirectRunner")
    with beam.Pipeline(options=_options()) as p:
        out = attach_enriched(p | beam.Create([make_envelope()]), config=cfg)
    assert out is None
    assert not (tmp_path / "enriched").exists()


def test_attach_lee_las_opciones_del_pipeline(tmp_path):
    opts = PipelineOptions(["--runner=BundleBasedDirectRunner", f"--enriched_root={tmp_path}"])
    with beam.Pipeline(options=opts) as p:
        attach_enriched(p | beam.Create([make_envelope("instagram", 1)]))
    _files, rows = _read_lake(tmp_path)
    assert len(rows) == 1
    assert len(rows[0]["user_hash"]) == 64
