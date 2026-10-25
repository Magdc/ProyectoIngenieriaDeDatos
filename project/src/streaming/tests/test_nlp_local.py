"""NLP en local (DirectRunner): sin MultiProcessShared y con el modelo cargado una vez por proceso.

Motivo (validación local del Lote 2.3): con ``share_model_across_processes() = True`` el
servidor de ``MultiProcessShared`` vive en un hilo del mismo proceso. El BundleBasedDirectRunner
rehace el DoFn y llama ``setup()`` en cada bundle, así que se crean y se liberan proxies sin
parar; el ``_decref`` de un proxy viejo termina corriendo en el hilo que acepta conexiones del
servidor y lo bloquea. El único hilo del DirectRunner queda esperando para siempre: ni
``enriched/`` ni ``raw/`` vuelven a escribirse (D4 y D6 en FALLA). Sin compartir entre procesos,
``shared.Shared`` solo mantiene vivo el último objeto adquirido y el modelo se recargaría a menudo:
de ahí la caché por proceso de ``load_model()``.
"""

from __future__ import annotations

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from streaming import nlp

CALLS: list[str] = []


def counting_loader(directory: str, task: str) -> nlp.Classifier:
    CALLS.append(task)
    return nlp.Classifier(lambda texts: [{"label": "POS", "score": 0.9} for _ in texts], "a" * 40, task)


def _models(tmp_path, monkeypatch):
    (tmp_path / nlp.MANIFEST).write_text('{"sentiment": {"revision": "a", "path": "s"}, "emotion": {"revision": "b", "path": "e"}}')
    monkeypatch.setenv("TA_NLP_MODELS_DIR", str(tmp_path))
    monkeypatch.delenv("TA_NLP_ENABLED", raising=False)


def test_local_no_comparte_el_modelo_entre_procesos(tmp_path, monkeypatch):
    _models(tmp_path, monkeypatch)
    o = PipelineOptions(["--runner=BundleBasedDirectRunner"])
    o.view_as(StandardOptions).streaming = True
    stage = nlp.nlp_from_options(o)
    assert stage is not None
    assert stage._sent.share_model_across_processes() is False
    assert stage._emo.share_model_across_processes() is False


def test_dataflow_si_comparte_el_modelo_entre_procesos(monkeypatch):
    monkeypatch.delenv("TA_NLP_ENABLED", raising=False)
    monkeypatch.delenv("TA_NLP_IMAGE", raising=False)
    o = PipelineOptions(["--runner=DataflowRunner", "--project=p-test", "--region=us-central1"])
    stage = nlp.nlp_from_options(o)
    assert stage._sent.share_model_across_processes() is True
    assert stage._emo.share_model_across_processes() is True


def test_load_model_carga_una_vez_por_proceso(tmp_path):
    CALLS.clear()
    nlp._LOADED.clear()
    a = nlp.RobertuitoModelHandler("sentiment", directory=str(tmp_path), loader=counting_loader, share_across_processes=False)
    b = nlp.RobertuitoModelHandler("sentiment", directory=str(tmp_path), loader=counting_loader, share_across_processes=False)
    e = nlp.RobertuitoModelHandler("emotion", directory=str(tmp_path), loader=counting_loader, share_across_processes=False)
    assert a.load_model() is b.load_model()  # un DoFn nuevo por bundle no recarga el modelo
    e.load_model()
    assert CALLS == ["sentiment", "emotion"]


def test_loader_lambda_no_se_cachea(tmp_path):
    seen = []

    def local_loader(directory, task):
        seen.append(task)
        return counting_loader(directory, task)

    h = nlp.RobertuitoModelHandler("sentiment", directory=str(tmp_path), loader=local_loader)
    h.load_model()
    h.load_model()
    assert seen == ["sentiment", "sentiment"]
