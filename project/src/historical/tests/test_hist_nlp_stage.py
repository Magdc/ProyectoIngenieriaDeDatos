"""Etapa 2 (nlp): reanudación, reintento y el motor Beam con un ``streaming.nlp`` falso."""

from __future__ import annotations

import json

import pytest

from historical.adapters import NLP_FIELDS
from historical.nlp_batch import BeamNlpEngine, NlpStageError, run_nlp_stage, summarize_stage2
from historical.testing import FAKE_VERSION, FakeNlpEngine, fake_to_row
from historical.work import WorkDir, read_jsonl_gz


def test_sin_stage1_falla(tmp_path):
    with pytest.raises(NlpStageError, match="generate"):
        run_nlp_stage(WorkDir(tmp_path, 1), FakeNlpEngine(), to_row=fake_to_row)


def test_completa_reanuda_y_no_repite(tmp_path, small_cfg, pipeline):
    work, _, nlp, _ = pipeline(tmp_path, small_cfg, write=False)
    assert nlp["rows"] == 900 and nlp["nlp_missing"] == 0 and nlp["pending_files"] == 0
    engine = FakeNlpEngine()
    again = run_nlp_stage(work, engine, to_row=fake_to_row)
    assert engine.calls == 0 and again["skipped_files"] == len(work.stage1_files())
    rows = [r for p in work.stage2_files() for r in read_jsonl_gz(p)]
    assert all(r["nlp_model_version"] == FAKE_VERSION for r in rows)
    assert all(r[f] is not None for r in rows for f in NLP_FIELDS)


def test_corte_a_mitad_sigue_donde_iba(tmp_path, small_cfg, pipeline):
    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    victim = work.stage2_files()[1]
    (work.stage2 / f"{victim.name}.json").unlink()
    engine = FakeNlpEngine()
    res = run_nlp_stage(work, engine, to_row=fake_to_row)
    assert engine.calls == 1 and res["processed_files"] == 1


def test_reintenta_las_filas_sin_nlp(tmp_path, small_cfg, pipeline):
    work, _, nlp, _ = pipeline(tmp_path, small_cfg, nlp_fail_first=5, write=False)
    assert nlp["nlp_missing"] == 0
    meta = json.loads((work.stage2 / f"{work.stage1_files()[0].name}.json").read_text())
    assert meta["retried"] == 5


def test_sin_reintento_queda_pendiente_y_se_repite(tmp_path, small_cfg, pipeline):
    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    for side in work.stage2.glob("*.json"):
        side.unlink()
    res = run_nlp_stage(work, FakeNlpEngine(fail_first=3), to_row=fake_to_row, retry_failed=False)
    assert res["nlp_missing"] == 3
    res2 = run_nlp_stage(work, FakeNlpEngine(), to_row=fake_to_row)
    assert res2["processed_files"] == 1 and res2["nlp_missing"] == 0


def test_stage1_cambiado_invalida_stage2(tmp_path, small_cfg, pipeline):
    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    first = work.stage1_files()[0]
    first.write_bytes(first.read_bytes())  # mismo contenido: sigue válido
    assert summarize_stage2(work)["pending_files"] == 0
    rows = read_jsonl_gz(first)
    from historical.work import jsonl_gz_bytes

    first.write_bytes(jsonl_gz_bytes(rows[:-1]))
    assert summarize_stage2(work)["pending_files"] == 1


# ------------------------------------------------------------------ motor Beam

def _fake_nlp_module(mkmod, calls, returns="transform"):
    beam = pytest.importorskip("apache_beam")

    class FakeRobertuito(beam.PTransform):
        def expand(self, pcoll):
            def annotate(batch):
                for row in batch:
                    out = dict(row)
                    out.update(sentiment_label="neutral", sentiment_score=0.5,
                               emotion_label="neutral", emotion_score=0.5,
                               nlp_model_version=FAKE_VERSION)
                    yield out

            return (pcoll | beam.BatchElements(min_batch_size=4, max_batch_size=4)
                    | beam.FlatMap(annotate))

    def nlp_from_options(options):
        calls.append(options.get_all_options(drop_default=True))
        t = FakeRobertuito()
        return {"transform": t, "tuple": ("cfg", t), "none": None}[returns]

    return mkmod("streaming.nlp", nlp_from_options=nlp_from_options)


def test_motor_beam_con_el_transform_del_pipeline(install_modules, mkmod):
    calls: list = []
    install_modules({"streaming.nlp": _fake_nlp_module(mkmod, calls)})
    rows = [{"event_id": f"e{i}", "text": "hola", "label_sentiment": None} for i in range(10)]
    out = BeamNlpEngine(batch_size=4).annotate(rows)
    assert set(out) == {f"e{i}" for i in range(10)}
    assert out["e3"]["nlp_model_version"] == FAKE_VERSION
    assert calls and calls[0].get("nlp_batch_size") in (None, 4)


def test_motor_beam_acepta_tupla(install_modules, mkmod):
    install_modules({"streaming.nlp": _fake_nlp_module(mkmod, [], returns="tuple")})
    out = BeamNlpEngine().annotate([{"event_id": "x", "text": "a"}])
    assert out["x"]["sentiment_label"] == "neutral"


def test_motor_beam_sin_modelos_explica(install_modules, mkmod):
    from historical.adapters import AdapterError

    install_modules({"streaming.nlp": _fake_nlp_module(mkmod, [], returns="none")})
    with pytest.raises(AdapterError, match="models.json"):
        BeamNlpEngine().annotate([{"event_id": "x", "text": "a"}])


def test_motor_beam_rechaza_fnapi(install_modules, mkmod):
    calls: list = []
    install_modules({"streaming.nlp": _fake_nlp_module(mkmod, calls)})
    rows = [{"event_id": f"e{i}", "text": "hola"} for i in range(12)]
    engine = BeamNlpEngine()
    assert len(engine.annotate(rows)) == 12 and engine.describe()["runner"] == "bundle"
    with pytest.raises(ValueError, match="FnApiRunner"):
        BeamNlpEngine(runner="fnapi")


def test_motor_beam_no_fuerza_espera_cero(install_modules, mkmod, monkeypatch):
    calls: list = []
    monkeypatch.delenv("TA_NLP_BATCH_SECONDS", raising=False)
    install_modules({"streaming.nlp": _fake_nlp_module(mkmod, calls)})
    BeamNlpEngine().annotate([{"event_id": "x", "text": "a"}])
    import os

    assert "TA_NLP_BATCH_SECONDS" not in os.environ
    with pytest.raises(ValueError):
        BeamNlpEngine(runner="prism")


def test_resumen_de_lotes():
    from historical.nlp_batch import batch_summary

    assert batch_summary({16: 10, 4: 1}) == {"batches": 11, "mean_size": 14.91, "top": {16: 10, 4: 1}}
    assert batch_summary({})["mean_size"] is None


def test_motor_beam_lote_incompleto_con_temporizador(install_modules, mkmod, monkeypatch):
    """El último lote incompleto (sale con el temporizador) no se pierde ni rompe el pipeline,
    tampoco con dos agrupaciones con estado encadenadas (sentimiento → emoción): es justo el
    caso en que el FnApiRunner falla ("time-pending bundles") o pierde filas."""
    beam = pytest.importorskip("apache_beam")
    from apache_beam.ml.inference.base import ModelHandler, PredictionResult, RunInference

    class _M:
        pass

    class H(ModelHandler):
        def load_model(self):
            return _M()

        def run_inference(self, batch, model, inference_args=None):
            return [PredictionResult(r, len(batch)) for r in batch]

        def batch_elements_kwargs(self):
            return {"min_batch_size": 16, "max_batch_size": 16, "max_batch_duration_secs": 5}

    class T(beam.PTransform):
        def expand(self, pcoll):
            def merge(pr):
                out = dict(pr.example)
                out.update(sentiment_label="neutral", sentiment_score=0.5, emotion_label="joy",
                           emotion_score=0.5, nlp_model_version=FAKE_VERSION)
                return out

            def example(pr):
                return pr.example

            return (pcoll | "Sentimiento" >> RunInference(H()) | "Fila" >> beam.Map(example)
                    | "Emocion" >> RunInference(H()) | beam.Map(merge))

    install_modules({"streaming.nlp": mkmod("streaming.nlp", nlp_from_options=lambda o: T())})
    monkeypatch.delenv("TA_NLP_BATCH_SECONDS", raising=False)
    for n in (4, 37):
        rows = [{"event_id": f"e{i}", "text": "x"} for i in range(n)]
        engine = BeamNlpEngine()
        out = engine.annotate(rows)
        assert len(out) == n
        if engine.last_batches:
            assert sum(k * v for k, v in engine.last_batches.items()) == 2 * n


def test_varios_procesos_reparten_los_archivos(tmp_path, small_cfg, pipeline):
    from historical.nlp_batch import run_nlp_stage

    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    for p in list(work.stage2.iterdir()):
        p.unlink()
    res = run_nlp_stage(work, None, workers=2, engine_spec={"kind": "fake"}, to_row_name="fake")
    assert res["pending_files"] == 0 and res["nlp_missing"] == 0 and res["workers"] == 2
    used = {json.loads(p.read_text())["worker"] for p in work.stage2.glob("*.jsonl.gz.json")}
    assert used == {0, 1}
    again = run_nlp_stage(work, None, workers=2, engine_spec={"kind": "fake"}, to_row_name="fake")
    assert again["skipped_files"] == len(work.stage1_files())


def test_hilos_por_proceso(monkeypatch):
    from historical.nlp_batch import worker_threads

    monkeypatch.delenv("TA_NLP_THREADS", raising=False)
    monkeypatch.setattr("os.cpu_count", lambda: 16)
    assert worker_threads(4) == 4 and worker_threads(1) == 16 and worker_threads(32) == 1
    monkeypatch.setenv("TA_NLP_THREADS", "3")
    assert worker_threads(4) == 3


def test_bench_no_se_queda_esperando_a_un_proceso_muerto():
    import queue as queue_mod

    from historical.nlp_batch import _gather

    class Proc:
        def __init__(self, code):
            self.exitcode, self.killed = code, False

        def is_alive(self):
            return self.exitcode is None

        def terminate(self):
            self.killed = True

    class Q:
        def __init__(self, items):
            self.items = list(items)

        def get(self, timeout=None):
            if self.items:
                return self.items.pop(0)
            raise queue_mod.Empty

    alive, dead = Proc(None), Proc(-9)
    results, error = _gather([alive, dead], Q([{"n": 1}]), timeout=60)
    assert results == [{"n": 1}] and "-9" in error and "memoria" in error and alive.killed
    results, error = _gather([Proc(0), Proc(0)], Q([{"n": 1}, {"n": 2}]), timeout=60)
    assert len(results) == 2 and error is None
    results, error = _gather([Proc(None)], Q([]), timeout=0)
    assert results == [] and "sin respuesta" in error


def test_bench_usa_filas_reales_de_stage1(tmp_path, small_cfg, pipeline):
    """Con etapa 1, el benchmark mide con los textos reales (los sintéticos son más cortos y
    sobreestiman la velocidad): unas pocas filas de cada archivo, de las tres fuentes."""
    from historical.doctor import real_rows
    from historical.testing import fake_to_row

    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    rows = real_rows(work.root, work.seed, 50, fake_to_row)
    assert len(rows) == 50 and all(r["text"] for r in rows)
    assert {r["source"] for r in rows} == {"reddit", "facebook", "instagram"}
    assert real_rows(tmp_path / "vacio", 1, 10, fake_to_row) is None
