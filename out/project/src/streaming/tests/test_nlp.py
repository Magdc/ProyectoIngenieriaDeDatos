"""NLP del streaming (Lote 2.3, HU-022). Sin torch ni modelos: clasificadores falsos."""

from __future__ import annotations

import json
import os
import re

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, WorkerOptions
from apache_beam.testing.test_stream import TestStream

from common import label_map
from common.enriched_row import envelope_to_row
from common.hmac_key import KeySource
from common.parquet_rows import parquet_bytes_to_rows
from streaming import nlp
from streaming.enriched import EnrichedConfig, attach_enriched
from streaming.enriched_testing import TEST_KEY, make_envelope
from streaming.nlp_testing import FAKE_REVISIONS, failing_loader, fake_loader, identity

PT = "2026-10-17T14:03:13.900Z"
VERSION = nlp.nlp_model_version(FAKE_REVISIONS["sentiment"], FAKE_REVISIONS["emotion"])

_OUT: list = []


def _collect(row):
    _OUT.append(row)


def _opts(*argv):
    o = PipelineOptions(["--runner=BundleBasedDirectRunner", *argv])
    o.view_as(StandardOptions).streaming = True
    return o


def _handlers(loader=fake_loader):
    return nlp.RobertuitoNlp(
        nlp.RobertuitoModelHandler("sentiment", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=4),
        nlp.RobertuitoModelHandler("emotion", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=4),
    )


def _rows(n=6):
    return [envelope_to_row(make_envelope(s, i), TEST_KEY, PT) for i, s in enumerate(["reddit", "facebook", "instagram"] * (n // 3))]


# --------------------------------------------------------------------------- #
# Etiquetas y versión
# --------------------------------------------------------------------------- #


def test_label_map_cubre_las_etiquetas_crudas_de_los_modelos():
    assert {label_map.map_sentiment(x) for x in nlp.EXPECTED_LABELS["sentiment"]} == {"negative", "neutral", "positive"}
    assert label_map.map_sentiment("POS") == "positive" and label_map.map_sentiment("NEG") == "negative"
    emotions = {label_map.map_emotion(x) for x in nlp.EXPECTED_LABELS["emotion"]}
    assert emotions == {"joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral"}
    assert label_map.map_emotion("others") == "neutral"


def test_version_tiene_el_formato_del_contrato():
    assert VERSION == "robertuito-sentiment@0123456789ab+robertuito-emotion@fedcba987654"
    assert re.match(r"^robertuito-sentiment@[0-9a-f]+\+robertuito-emotion@[0-9a-f]+$", VERSION)


def test_manifiesto(tmp_path):
    (tmp_path / "models.json").write_text(json.dumps({t: {"repo": nlp.REPOS[t], "revision": "a" * 40, "path": t} for t in nlp.TASKS}))
    assert nlp.read_manifest(str(tmp_path))["emotion"]["revision"] == "a" * 40
    (tmp_path / "models.json").write_text(json.dumps({"sentiment": {"revision": "x"}}))
    with pytest.raises(ValueError):
        nlp.read_manifest(str(tmp_path))


# --------------------------------------------------------------------------- #
# ModelHandler
# --------------------------------------------------------------------------- #


def test_handler_preprocesa_y_devuelve_model_id():
    seen = []

    def pre(t):
        seen.append(t)
        return t.upper()

    h = nlp.RobertuitoModelHandler("sentiment", loader=fake_loader, preprocessor=pre, batch_size=8)
    model = h.load_model()
    rows = _rows(3)
    out = list(h.run_inference(rows, model))
    assert [r.example for r in out] == rows
    assert all(r.model_id == "robertuito-sentiment@0123456789ab" for r in out)
    assert seen == [r["text"] for r in rows]
    assert h.batch_elements_kwargs() == {"min_batch_size": 1, "max_batch_size": 8}
    assert h.share_model_across_processes() is True
    with pytest.raises(ValueError):
        nlp.RobertuitoModelHandler("ironia")


def test_handler_detecta_salida_incompleta():
    class Short:
        def get_revision(self):
            return "a" * 40

        def classify(self, texts):
            return []

    h = nlp.RobertuitoModelHandler("emotion", loader=lambda d, t: Short(), preprocessor=identity)
    with pytest.raises(ValueError):
        list(h.run_inference(_rows(3), Short()))


# --------------------------------------------------------------------------- #
# Transform con RunInference
# --------------------------------------------------------------------------- #


def test_transform_rellena_sentimiento_emocion_y_version():
    _OUT.clear()
    with beam.Pipeline(options=_opts()) as p:
        p | beam.Create(_rows(6)) | _handlers() | beam.Map(_collect)
    assert len(_OUT) == 6
    for r in _OUT:
        assert r["sentiment_label"] == "positive"  # los textos de prueba dicen "soy fan"
        assert r["sentiment_score"] == pytest.approx(0.93)
        assert r["emotion_label"] in ("neutral", "joy")
        assert 0 <= r["emotion_score"] <= 1
        assert r["nlp_model_version"] == VERSION
        assert "user_id" not in r and "handle" not in r


def test_modelo_compartido_entre_procesos():
    """Con share_model_across_processes el modelo llega como proxy: solo métodos."""
    _OUT.clear()
    shared = nlp.RobertuitoNlp(
        nlp.RobertuitoModelHandler("sentiment", loader=fake_loader, preprocessor=identity, share_across_processes=True),
        nlp.RobertuitoModelHandler("emotion", loader=fake_loader, preprocessor=identity, share_across_processes=True),
    )
    with beam.Pipeline(options=_opts()) as p:
        p | beam.Create(_rows(3)) | shared | beam.Map(_collect)
    assert len(_OUT) == 3 and all(r["nlp_model_version"] == VERSION for r in _OUT)


def test_falla_de_inferencia_no_pierde_filas():
    _OUT.clear()
    with beam.Pipeline(options=_opts()) as p:
        p | beam.Create(_rows(6)) | _handlers(failing_loader) | beam.Map(_collect)
    assert len(_OUT) == 6
    for r in _OUT:
        assert r["sentiment_label"] is None and r["nlp_model_version"] is None


def test_rama_enriched_con_nlp_escribe_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("TA_TEST_HMAC", TEST_KEY)
    cfg = EnrichedConfig(True, str(tmp_path), "enriched", 60, None, KeySource("env", env_var="TA_TEST_HMAC"), "DirectRunner")
    stream = TestStream().add_elements([make_envelope(s, i) for i, s in enumerate(["reddit", "facebook", "instagram"] * 2)]).advance_watermark_to_infinity()
    with beam.Pipeline(options=_opts()) as p:
        attach_enriched(p | stream, config=cfg, nlp=_handlers())
    rows = []
    for d, _x, files in os.walk(tmp_path / "enriched"):
        for f in files:
            rows.extend(parquet_bytes_to_rows(open(os.path.join(d, f), "rb").read()))
    assert len(rows) == 6
    assert all(r["nlp_model_version"] == VERSION and r["sentiment_label"] == "positive" for r in rows)


# --------------------------------------------------------------------------- #
# Configuración e integración
# --------------------------------------------------------------------------- #


def test_auto_local_sin_modelos_queda_apagado(tmp_path):
    cfg = nlp.resolve_nlp_config(PipelineOptions([]), environ={"TA_NLP_MODELS_DIR": str(tmp_path)})
    assert not cfg.enabled and "sin modelos" in cfg.reason
    with pytest.raises(nlp.NlpConfigError):
        nlp.resolve_nlp_config(PipelineOptions(["--nlp_enabled=true"]), environ={"TA_NLP_MODELS_DIR": str(tmp_path)})


def test_auto_local_con_modelos_se_activa(tmp_path):
    (tmp_path / "models.json").write_text("{}")
    cfg = nlp.resolve_nlp_config(PipelineOptions([]), environ={"TA_NLP_MODELS_DIR": str(tmp_path)})
    assert cfg.enabled and cfg.container_image is None


def test_dataflow_fija_la_imagen_en_las_opciones():
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--region=us-central1"])
    cfg = nlp.resolve_nlp_config(opts, environ={})
    assert cfg.enabled and cfg.container_image == "us-central1-docker.pkg.dev/p1/ta-images/dataflow-nlp:latest"
    nlp.apply_container_image(opts, cfg)
    assert opts.view_as(WorkerOptions).sdk_container_image == cfg.container_image
    assert opts.view_as(SetupOptions).sdk_location == "container"
    assert opts.get_all_options()["sdk_container_image"] == cfg.container_image  # la ve el runner


def test_imagen_desde_el_entorno_y_explicita():
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1"])
    assert nlp.resolve_nlp_config(opts, environ={"TA_NLP_IMAGE": "img:abc"}).container_image == "img:abc"
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--nlp_container_image=img:x"])
    assert nlp.resolve_nlp_config(opts, environ={"TA_NLP_IMAGE": "img:abc"}).container_image == "img:x"


def test_imagen_en_conflicto_y_opciones_invalidas():
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--sdk_container_image=otra", "--nlp_container_image=img:x"])
    cfg = nlp.resolve_nlp_config(opts, environ={})
    with pytest.raises(nlp.NlpConfigError):
        nlp.apply_container_image(opts, cfg)
    with pytest.raises(nlp.NlpConfigError):
        nlp.resolve_nlp_config(PipelineOptions(["--nlp_enabled=quizas"]), environ={})
    with pytest.raises(nlp.NlpConfigError):
        nlp.resolve_nlp_config(PipelineOptions(["--runner=DataflowRunner"]), environ={})


def test_apagado_por_variable_de_entorno():
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1"])
    assert not nlp.resolve_nlp_config(opts, environ={"TA_NLP_ENABLED": "false"}).enabled
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--nlp_enabled=true"])
    assert nlp.resolve_nlp_config(opts, environ={"TA_NLP_ENABLED": "false"}).enabled


def test_apagado_explicito():
    opts = PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--nlp_enabled=false"])
    assert nlp.nlp_from_options(opts) is None
    assert not opts.view_as(WorkerOptions).sdk_container_image


def test_preprocess_tweet_de_pysentimiento():
    pytest.importorskip("emoji")
    if __import__("importlib").util.find_spec("pysentimiento") is None:
        pytest.skip("pip install --no-deps pysentimiento==0.7.3 emoji==2.14.0")
    out = nlp.preprocess("@mariag_89 me encanta Postobón 😍 https://x.co jajajaja")
    assert "@usuario" in out or "@USER" in out or "usuario" in out.lower()
    assert "http" not in out
    assert "😍" not in out


def test_hilos_de_inferencia():
    assert nlp.nlp_threads({"TA_NLP_THREADS": "3"}) == 3
    assert nlp.nlp_threads({}) >= 1


def test_lotes_por_tiempo_en_el_handler_y_la_config():
    h = nlp.RobertuitoModelHandler("sentiment", loader=fake_loader, batch_size=16, batch_seconds=5)
    assert h.batch_elements_kwargs() == {"min_batch_size": 16, "max_batch_size": 16, "max_batch_duration_secs": 5.0}
    assert "max_batch_duration_secs" not in nlp.RobertuitoModelHandler("emotion", loader=fake_loader, batch_seconds=0).batch_elements_kwargs()
    cfg = nlp.resolve_nlp_config(PipelineOptions(["--runner=DataflowRunner", "--project=p1"]), environ={})
    assert cfg.batch_seconds == 5.0
    cfg = nlp.resolve_nlp_config(PipelineOptions(["--runner=DataflowRunner", "--project=p1"]), environ={"TA_NLP_BATCH_SECONDS": "2"})
    assert cfg.batch_seconds == 2.0
    with pytest.raises(nlp.NlpConfigError):
        nlp.resolve_nlp_config(PipelineOptions(["--runner=DataflowRunner", "--project=p1", "--nlp_batch_seconds=90"]), environ={})


_SIZES: list = []


class _CountingLoader:
    """Clasificador falso que registra el tamaño de cada lote."""

    def __call__(self, directory, task):
        clf = fake_loader(directory, task)
        inner = clf.fn

        def fn(texts):
            _SIZES.append(len(texts))
            return inner(texts)

        clf.fn = fn
        return clf


def test_stream_con_espera_agrupa_en_lotes():
    """Con TestStream y espera por tiempo, los comentarios se infieren en lotes, no de a uno."""
    from apache_beam.transforms.window import FixedWindows, TimestampedValue

    _OUT.clear()
    _SIZES.clear()
    rows = _rows(12)
    stream = TestStream()
    for i, r in enumerate(rows):
        stream = stream.add_elements([TimestampedValue(r, 10 + i)])
    stream = stream.advance_processing_time(10).advance_watermark_to_infinity()
    loader = _CountingLoader()
    t = nlp.RobertuitoNlp(
        nlp.RobertuitoModelHandler("sentiment", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=16, batch_seconds=2),
        nlp.RobertuitoModelHandler("emotion", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=16, batch_seconds=2),
    )
    with beam.Pipeline(options=_opts()) as p:
        p | stream | beam.WindowInto(FixedWindows(3600)) | t | beam.Map(_collect)
    assert len(_OUT) == 12 and all(r["nlp_model_version"] == VERSION for r in _OUT)
    assert max(_SIZES) > 1, f"lotes de tamaño {_SIZES}"


def test_stream_lento_vacia_el_lote_por_tiempo():
    """Si llegan pocos comentarios, el lote sale al vencer la espera (no espera a llenarse)."""
    from apache_beam.transforms.window import FixedWindows, TimestampedValue

    _OUT.clear()
    _SIZES.clear()
    rows = _rows(6)
    stream = TestStream()
    for start in (0, 3):
        stream = stream.add_elements([TimestampedValue(r, 10 + start + i) for i, r in enumerate(rows[start:start + 3])])
        stream = stream.advance_processing_time(5)
    stream = stream.advance_watermark_to_infinity()
    loader = _CountingLoader()
    t = nlp.RobertuitoNlp(
        nlp.RobertuitoModelHandler("sentiment", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=16, batch_seconds=2),
        nlp.RobertuitoModelHandler("emotion", loader=loader, preprocessor=identity, share_across_processes=False, batch_size=16, batch_seconds=2),
    )
    with beam.Pipeline(options=_opts()) as p:
        p | stream | beam.WindowInto(FixedWindows(3600)) | t | beam.Map(_collect)
    assert len(_OUT) == 6
    assert all(n < 16 for n in _SIZES) and max(_SIZES) > 1, _SIZES
