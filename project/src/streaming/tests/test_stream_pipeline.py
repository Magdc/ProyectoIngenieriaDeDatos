"""Pipeline completo con TestStream en el DirectRunner (sin Pub/Sub ni nube)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

beam = pytest.importorskip("apache_beam")

from apache_beam.io.gcp.pubsub import PubsubMessage  # noqa: E402
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions  # noqa: E402
from apache_beam.testing.test_pipeline import TestPipeline  # noqa: E402
from apache_beam.testing.test_stream import TestStream  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402
from apache_beam.transforms.window import TimestampedValue  # noqa: E402

from streaming.config import resolve  # noqa: E402
from streaming.inject import attributes_for, build_plan, dumps, sentinel_envelope  # noqa: E402
from streaming.pipeline import build_from_messages  # noqa: E402


def _options():
    opts = PipelineOptions(runner="BundleBasedDirectRunner")
    opts.view_as(StandardOptions).streaming = True
    return opts


def _cfg(bundle, root, window=60):
    return resolve(project="ta-local", runner="DirectRunner", sources="reddit,facebook,instagram",
                   subscription_template="ta-raw-{source}-df", dlq_topic="", output_root=str(root),
                   raw_window_seconds=window, dedup_window_seconds=900, pubsub_id_label="event_id",
                   dlq_table="", bundle=bundle)


def _msg(source, envelope=None, data=None, attrs=None, ts=0):
    if envelope is not None:
        data = dumps(envelope)
        attrs = attributes_for(envelope) if attrs is None else attrs
    return TimestampedValue((source, PubsubMessage(data, attrs or {})), ts)


def _read_lines(root: Path, prefix: str):
    lines = []
    for path in sorted((root / prefix).rglob("*.jsonl.gz")):
        lines += [ln for ln in gzip.decompress(path.read_bytes()).decode().splitlines() if ln]
    return lines


def test_valida_deduplica_y_escribe_raw_y_dlq(mini_bundle, tmp_path):
    a = sentinel_envelope("p", 1)
    b = sentinel_envelope("p", 2)
    stream = (
        TestStream()
        .add_elements([_msg("reddit", a, ts=1), _msg("reddit", b, ts=2), _msg("reddit", a, ts=3),
                       _msg("reddit", data=b'{"roto', attrs={"event_id": "x"}, ts=4)])
        .advance_watermark_to(120)
        # Copia tardía de `a` (otra ventana): la deduplicación es por event_id, no por ventana.
        .add_elements([_msg("reddit", a, ts=130),
                       _msg("facebook", b, ts=131)])  # envelope de reddit en la suscripción de facebook
        .advance_watermark_to_infinity()
    )
    cfg = _cfg(mini_bundle, tmp_path)
    with TestPipeline(options=_options()) as p:
        out = build_from_messages(p | stream, cfg)
        assert_that(out["unique"] | beam.Map(lambda e: e.event_id),
                    equal_to([a["event_id"], b["event_id"]]), label="unique")
        assert_that(out["dlq"] | beam.Map(lambda r: (r.source, r.error_type)),
                    equal_to([("reddit", "json"), ("facebook", "routing")]), label="dlq")

    raw = _read_lines(tmp_path, "raw")
    assert sorted(json.loads(x)["event_id"] for x in raw) == sorted([a["event_id"], b["event_id"]])
    assert set(raw) == {dumps(a).decode(), dumps(b).decode()}  # el cuerpo, tal como llegó
    raw_dirs = {p.parent.relative_to(tmp_path).as_posix() for p in tmp_path.glob("raw/**/*.jsonl.gz")}
    assert all(d.startswith("raw/source=reddit/dt=") and "/hr=" in d for d in raw_dirs)

    dlq = [json.loads(x) for x in _read_lines(tmp_path, "dlq")]
    assert sorted(r["error_type"] for r in dlq) == ["json", "routing"]
    assert all(set(r) == {"received_at", "source", "error_type", "error_detail", "raw_message"}
               for r in dlq)
    assert not list(tmp_path.rglob("*.tmp-*")), "quedaron temporales sin renombrar"


def test_plan_de_verificacion_completo(mini_bundle, tmp_path):
    """Lo mismo que hace `verify_streaming.py check`, sin Pub/Sub."""
    from streaming.lake_report import LocalLake, scan
    from streaming.cli import evaluate

    plan = build_plan("pipe")
    stream = TestStream().add_elements(
        [_msg(m.topic_source, data=m.data, attrs=m.attributes, ts=i)
         for i, m in enumerate(plan.messages)]).advance_watermark_to_infinity()
    with TestPipeline(options=_options()) as p:
        build_from_messages(p | stream, _cfg(mini_bundle, tmp_path))
    result = scan(LocalLake(str(tmp_path)), ["reddit", "facebook", "instagram"], hours=1)
    checks = evaluate(plan, result, None)
    assert all(c["status"] == "ok" for c in checks), checks


def test_construccion_con_dlq_topic_y_tabla(mini_bundle, tmp_path):
    """Con topic y tabla configurados el grafo incluye ambos destinos (no se ejecuta)."""
    import dataclasses

    cfg = dataclasses.replace(_cfg(mini_bundle, tmp_path), dlq_topic="projects/ta-local/topics/ta-dlq",
                              dlq_table="ta-local:ta_stream.dlq_events")
    p = beam.Pipeline(options=_options())
    out = build_from_messages(p | beam.Create([]), cfg)
    assert "dlq_messages" in out
    labels = {t.full_label for t in p.transforms_stack[0].parts}
    assert {"PublishDlq", "WriteDlqTable"} <= labels


def test_build_pipeline_lee_una_suscripcion_por_fuente(mini_bundle, tmp_path):
    from streaming.pipeline import build_pipeline

    p = beam.Pipeline(options=_options())
    build_pipeline(p, _cfg(mini_bundle, tmp_path))
    labels = {t.full_label for t in p.transforms_stack[0].parts}
    assert {"Read[reddit]", "Read[facebook]", "Read[instagram]", "Merge"} <= labels
