from __future__ import annotations

import gzip
from types import SimpleNamespace

import pytest

pytest.importorskip("apache_beam")

from streaming.records import DlqRecord  # noqa: E402
from streaming.transforms import _WriteGroupFn, to_dlq_pubsub_message, write_atomically  # noqa: E402


def _write(root, partition, lines, start=1_800_000_000, pane_index=0):
    fn = _WriteGroupFn(str(root))
    win = SimpleNamespace(start=start)
    pane = SimpleNamespace(index=pane_index)
    return list(fn.process((partition, lines), win=win, pane=pane))


def test_nombre_y_contenido_deterministas(tmp_path):
    out = _write(tmp_path, "raw/source=reddit/dt=2027-01-15/hr=08", ["b", "a"])
    path, n = out[0]
    assert n == 2
    assert path.endswith("raw/source=reddit/dt=2027-01-15/hr=08/part-20270115T080000Z-p000.jsonl.gz")
    first = open(path, "rb").read()
    assert gzip.decompress(first) == b"a\nb\n"  # ordenadas, una por línea

    # Reintento de la misma ventana: mismo nombre y mismos bytes (idempotente).
    _write(tmp_path, "raw/source=reddit/dt=2027-01-15/hr=08", ["a", "b"])
    assert open(path, "rb").read() == first
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 1


def test_paneles_tardios_no_pisan_al_primero(tmp_path):
    _write(tmp_path, "dlq/dt=2027-01-15", ["x"], pane_index=0)
    _write(tmp_path, "dlq/dt=2027-01-15", ["y"], pane_index=1)
    names = sorted(p.name for p in tmp_path.rglob("*.jsonl.gz"))
    assert names == ["part-20270115T080000Z-p000.jsonl.gz", "part-20270115T080000Z-p001.jsonl.gz"]


def test_write_atomically_sobrescribe_sin_dejar_temporales(tmp_path):
    target = tmp_path / "a" / "b" / "f.jsonl.gz"
    write_atomically(str(target), b"uno")
    write_atomically(str(target), b"dos")
    assert target.read_bytes() == b"dos"
    assert [p.name for p in target.parent.iterdir()] == ["f.jsonl.gz"]


def test_mensaje_dlq_lleva_el_cuerpo_original():
    rec = DlqRecord(received_at="2027-01-15T08:00:00.000Z", source="reddit", error_type="schema",
                    error_detail="(raíz): falta text", raw_message="{}", process_ts=0.0,
                    data=b"{}", attributes={"event_id": "e1", "source": "reddit"})
    msg = to_dlq_pubsub_message(rec)
    assert msg.data == b"{}"
    assert msg.attributes["event_id"] == "e1"
    assert msg.attributes["dlq_error_type"] == "schema"
    assert msg.attributes["dlq_error_detail"] == "(raíz): falta text"


# --------------------------------------------------------------------------- opciones

def test_make_options_direct_runner(mini_bundle, tmp_path, monkeypatch):
    import streaming.pipeline as pl
    from apache_beam.options.pipeline_options import StandardOptions

    monkeypatch.setattr(pl, "load_bundle", lambda _d: mini_bundle)
    opts, cfg = pl.make_options(["--project", "ta-local", "--output_root", str(tmp_path)])
    assert opts.view_as(StandardOptions).runner == "BundleBasedDirectRunner"
    assert opts.view_as(StandardOptions).streaming is True
    assert cfg.direct and cfg.id_label is None


def test_make_options_dataflow(mini_bundle, monkeypatch):
    import streaming.pipeline as pl
    from apache_beam.options.pipeline_options import GoogleCloudOptions

    monkeypatch.setattr(pl, "load_bundle", lambda _d: mini_bundle)
    monkeypatch.setenv("GCP_PROJECT", "p1")
    monkeypatch.setenv("GCP_REGION", "us-central1")
    opts, cfg = pl.make_options(["--runner", "DataflowRunner", "--raw_window_seconds", "120"])
    gcp = opts.view_as(GoogleCloudOptions)
    assert gcp.project == "p1" and gcp.region == "us-central1"
    assert gcp.temp_location == "gs://p1-ta-lake/_dataflow/temp"
    assert gcp.staging_location == "gs://p1-ta-lake/_dataflow/staging"
    assert cfg.id_label == "event_id" and cfg.raw_window_seconds == 120


def test_run_con_configuracion_invalida_devuelve_2(monkeypatch, capsys, tmp_path):
    import logging

    import streaming.pipeline as pl

    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    try:
        assert pl.run(["--contracts_dir", str(tmp_path)]) == 2
        assert "config_error" in capsys.readouterr().out
    finally:
        root.handlers[:], level = saved
        root.setLevel(level)


# --------------------------------------------------------------------------- deduplicación

def test_dedup_no_usa_timers():
    """Un timer de tiempo de proceso retiene la marca de agua en Dataflow (raw/ se atrasaba)."""
    from apache_beam.transforms import userstate

    from streaming.transforms import _DedupFn

    _, timers = userstate.get_dofn_specs(_DedupFn())
    assert not timers


def _dedup(events, window_seconds=3600):
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.testing.test_pipeline import TestPipeline
    from apache_beam.testing.util import assert_that, equal_to

    from streaming.transforms import DedupByEventId

    opts = PipelineOptions(runner="BundleBasedDirectRunner")
    opts.view_as(StandardOptions).streaming = True
    return beam, opts, TestPipeline, assert_that, equal_to, DedupByEventId


def test_dedup_misma_ventana_descarta_y_otra_ventana_conserva():
    from streaming.records import ValidEvent

    beam, opts, TestPipeline, assert_that, equal_to, DedupByEventId = _dedup(None)
    base = 1_800_000_000.0  # 08:00:00 UTC: inicio de una ventana de 1 h
    events = [
        ValidEvent("reddit", "a", "{a1}", base + 10),
        ValidEvent("reddit", "a", "{a2}", base + 1500),   # misma hora: se descarta
        ValidEvent("reddit", "b", "{b}", base + 20),
        ValidEvent("reddit", "a", "{a3}", base + 3700),   # hora siguiente: pasa (Trusted la quita)
    ]
    with TestPipeline(options=opts) as p:
        out = (p | beam.Create(events) | DedupByEventId(3600)
               | beam.Map(lambda e: (e.event_id, e.body, float(e.process_ts))))
        assert_that(out, equal_to([("a", "{a1}", base + 10), ("b", "{b}", base + 20),
                                   ("a", "{a3}", base + 3700)]))


def test_dedup_marca_la_salida_con_la_hora_de_proceso():
    import apache_beam as beam

    from streaming.records import ValidEvent

    _, opts, TestPipeline, assert_that, equal_to, DedupByEventId = _dedup(None)

    class Ts(beam.DoFn):
        def process(self, e, ts=beam.DoFn.TimestampParam):
            yield float(ts) == e.process_ts

    with TestPipeline(options=opts) as p:
        out = (p | beam.Create([ValidEvent("reddit", "x", "{}", 1_800_000_123.5)])
               | DedupByEventId(3600) | beam.ParDo(Ts()))
        assert_that(out, equal_to([True]))
