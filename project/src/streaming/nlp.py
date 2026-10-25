"""NLP del streaming (Lote 2.3, HU-022): sentimiento y emoción en español con RoBERTuito.

Se engancha sola en la rama enriched del Lote 2.2: ``attach_enriched(unique)`` llama a
``nlp_from_options()`` y, si el NLP está activo, mete esta etapa entre el enriquecimiento y
las salidas (``enriched/`` y ``posts_enriched``). ``pipeline.py`` no cambia.

    filas ─► RunInference(sentimiento) ─► RunInference(emoción) ─► filas con NLP

* Modelos: ``pysentimiento/robertuito-sentiment-analysis`` (``NEG``/``NEU``/``POS``) y
  ``pysentimiento/robertuito-emotion-analysis`` (``others``, ``joy``, ``sadness``, ``anger``,
  ``surprise``, ``disgust``, ``fear``), horneados en la imagen ``dataflow-nlp`` con su commit.
* Preprocesado: ``preprocess_tweet(text, lang="es")`` de pysentimiento (lo mismo que su
  ``create_analyzer`` para español). Se carga desde el archivo, sin importar el paquete
  completo (que arrastraría ``datasets`` y ``spacy``).
* Etiquetas: ``common.label_map.map_sentiment`` y ``map_emotion`` (Lote 1.1, §3.3).
* ``nlp_model_version`` = ``robertuito-sentiment@<commit12>+robertuito-emotion@<commit12>``.
* Si una inferencia falla, las filas siguen con el NLP en nulo y suben ``nlp/nlp_failed_rows``:
  no se pierden datos ni se detiene el pipeline.

Opciones (todas con valor por defecto):

=========================  ====================================================================
``--nlp_enabled``          ``auto`` (Dataflow: sí; local: sí si la imagen trae los modelos),
                           ``true`` o ``false``; con ``auto`` manda ``TA_NLP_ENABLED`` si existe
``--nlp_container_image``  imagen de los workers; por defecto ``TA_NLP_IMAGE`` o
                           ``{región}-docker.pkg.dev/{proyecto}/ta-images/dataflow-nlp:latest``
``--nlp_batch_size``       tamaño máximo de lote por inferencia (16)
``--nlp_batch_seconds``    espera máxima para juntar un lote, en segundos (5; 0 = sin espera).
                           En streaming los *bundles* traen 1 o 2 elementos: sin esta espera
                           cada comentario se infiere solo y el CPU no alcanza (lote 1 ≈ 3/s
                           frente a ≈ 20/s con lote 16, medido con ``scripts/nlp_bench.py``)
=========================  ====================================================================
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.ml.inference.base import ModelHandler, PredictionResult, RunInference
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, StandardOptions, WorkerOptions

from common import label_map

LOG = logging.getLogger("streaming.nlp")
METRICS_NS = "nlp"

TASKS = ("sentiment", "emotion")
REPOS = {
    "sentiment": "pysentimiento/robertuito-sentiment-analysis",
    "emotion": "pysentimiento/robertuito-emotion-analysis",
}
#: Etiquetas crudas que deben traer los modelos (``config.id2label``). La imagen lo verifica.
EXPECTED_LABELS = {
    "sentiment": frozenset({"NEG", "NEU", "POS"}),
    "emotion": frozenset({"others", "joy", "sadness", "anger", "surprise", "disgust", "fear"}),
}
MAX_LENGTH = 128  # longitud máxima de RoBERTuito (la misma que usa pysentimiento)
DEFAULT_MODELS_DIR = "/opt/ta-nlp/models"
MANIFEST = "models.json"
IMAGE_NAME = "dataflow-nlp"
DEFAULT_REPOSITORY = "ta-images"
VERSION_LEN = 12

# Modelos ya cargados en este proceso (solo con loaders de módulo, p. ej. load_classifier).
# El BundleBasedDirectRunner vuelve a llamar setup() en cada bundle y shared.Shared solo
# mantiene vivo el último objeto adquirido: sin esta caché el modelo se recargaría a menudo.
_LOADED: dict[tuple, Any] = {}
_LOADED_LOCK = threading.Lock()


def _log(event: str, **fields: Any) -> None:
    LOG.info(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))


# --------------------------------------------------------------------------- #
# Manifiesto de modelos y versión
# --------------------------------------------------------------------------- #


def models_dir(environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    return env.get("TA_NLP_MODELS_DIR") or DEFAULT_MODELS_DIR


def read_manifest(directory: str) -> dict[str, Any]:
    """``models.json`` que escribe ``nlp_image/fetch_models.py``."""
    with open(os.path.join(directory, MANIFEST), encoding="utf-8") as fh:
        manifest = json.load(fh)
    for task in TASKS:
        entry = manifest.get(task) or {}
        if not entry.get("revision") or not entry.get("path"):
            raise ValueError(f"models.json sin {task}.revision o {task}.path")
    return manifest


def model_id(task: str, revision: str) -> str:
    return f"robertuito-{task}@{revision[:VERSION_LEN]}"


def nlp_model_version(sentiment_revision: str, emotion_revision: str) -> str:
    """``robertuito-sentiment@<hash>+robertuito-emotion@<hash>`` (§5.2)."""
    return f"{model_id('sentiment', sentiment_revision)}+{model_id('emotion', emotion_revision)}"


# --------------------------------------------------------------------------- #
# Preprocesado
# --------------------------------------------------------------------------- #

_PREPROCESS: Callable[..., str] | None = None


def _load_preprocess_tweet() -> Callable[..., str]:
    """``pysentimiento.preprocessing.preprocess_tweet`` sin ejecutar ``pysentimiento/__init__``."""
    spec = importlib.util.find_spec("pysentimiento")
    if spec is None or not spec.submodule_search_locations:
        raise ImportError("falta pysentimiento (pip install --no-deps pysentimiento==0.7.3 emoji)")
    path = os.path.join(list(spec.submodule_search_locations)[0], "preprocessing.py")
    mod_spec = importlib.util.spec_from_file_location("ta_pysentimiento_preprocessing", path)
    module = importlib.util.module_from_spec(mod_spec)
    assert mod_spec.loader is not None
    mod_spec.loader.exec_module(module)
    return module.preprocess_tweet


def preprocess(text: str) -> str:
    """Preprocesado de pysentimiento para español (usuarios, URLs, hashtags, emojis, risas)."""
    global _PREPROCESS
    if _PREPROCESS is None:
        _PREPROCESS = _load_preprocess_tweet()
    return _PREPROCESS(text or "", lang="es")


# --------------------------------------------------------------------------- #
# Clasificadores y ModelHandler
# --------------------------------------------------------------------------- #


class Classifier:
    """Envoltura de un ``transformers.pipeline`` de clasificación con su versión."""

    def __init__(self, fn: Callable[[list[str]], list[dict]], revision: str, task: str):
        self.fn = fn
        self.revision = revision
        self.task = task

    def __call__(self, texts: list[str]) -> list[dict]:
        return self.fn(texts)

    def classify(self, texts: list[str]) -> list[dict]:
        return self.fn(texts)

    def get_revision(self) -> str:
        # Método (no atributo): con share_model_across_processes el modelo llega como proxy,
        # que solo expone métodos.
        return self.revision


def nlp_threads(environ: Mapping[str, str] | None = None) -> int:
    """Hilos de torch para la inferencia: ``TA_NLP_THREADS`` o todos los núcleos.

    Con ``share_model_across_processes`` hay un solo modelo por VM atendiendo a todos los
    procesos del SDK, así que conviene que use todos los núcleos (la imagen fija
    ``OMP_NUM_THREADS=1`` para el resto de librerías).
    """
    env = os.environ if environ is None else environ
    value = env.get("TA_NLP_THREADS")
    if value:
        return max(1, int(value))
    return max(1, os.cpu_count() or 1)


def configure_threads() -> int:
    import torch

    n = nlp_threads()
    torch.set_num_threads(n)
    return n


def load_classifier(directory: str, task: str) -> Classifier:
    """Carga un modelo horneado en la imagen y comprueba sus etiquetas."""
    from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

    configure_threads()
    manifest = read_manifest(directory)
    entry = manifest[task]
    path = os.path.join(directory, entry["path"])
    tokenizer = AutoTokenizer.from_pretrained(path, local_files_only=True)
    tokenizer.model_max_length = MAX_LENGTH
    model = AutoModelForSequenceClassification.from_pretrained(path, local_files_only=True)
    labels = frozenset(model.config.id2label.values())
    if labels != EXPECTED_LABELS[task]:
        raise ValueError(f"etiquetas inesperadas en {task}: {sorted(labels)}")
    clf = pipeline("text-classification", model=model, tokenizer=tokenizer, device=-1)

    def run(texts: list[str]) -> list[dict]:
        out = clf(texts, truncation=True, max_length=MAX_LENGTH, batch_size=len(texts) or 1)
        return [o[0] if isinstance(o, list) else o for o in out]

    _log("nlp_model_loaded", task=task, repo=entry.get("repo"), revision=entry["revision"], threads=nlp_threads())
    return Classifier(run, entry["revision"], task)


class RobertuitoModelHandler(ModelHandler):
    """ModelHandler de RunInference: filas de ``posts_enriched`` → etiqueta y score."""

    def __init__(
        self,
        task: str,
        *,
        directory: str | None = None,
        loader: Callable[[str, str], Any] = load_classifier,
        preprocessor: Callable[[str], str] = preprocess,
        batch_size: int = 16,
        batch_seconds: float = 0.0,
        share_across_processes: bool = True,
    ):
        if task not in TASKS:
            raise ValueError(f"tarea desconocida: {task}")
        self._task = task
        self._directory = directory
        self._loader = loader
        self._pre = preprocessor
        self._batch_size = max(1, int(batch_size))
        self._batch_seconds = max(0.0, float(batch_seconds))
        self._share = share_across_processes

    def load_model(self):
        directory = self._directory or models_dir()
        name = getattr(self._loader, "__qualname__", "")
        if "<" in name or not name:  # lambda o closure (pruebas): sin caché
            return self._loader(directory, self._task)
        key = (getattr(self._loader, "__module__", ""), name, directory, self._task)
        with _LOADED_LOCK:
            if key not in _LOADED:
                _LOADED[key] = self._loader(directory, self._task)
            return _LOADED[key]

    def run_inference(self, batch: Sequence[dict], model: Any, inference_args: dict | None = None) -> Iterable[PredictionResult]:
        texts = [self._pre(str(r.get("text") or "")) for r in batch]
        outputs = list(model.classify(texts))
        if len(outputs) != len(batch):
            raise ValueError(f"el modelo devolvió {len(outputs)} resultados para {len(batch)} textos")
        mid = model_id(self._task, model.get_revision())
        return [PredictionResult(example=row, inference=out, model_id=mid) for row, out in zip(batch, outputs)]

    def batch_elements_kwargs(self) -> Mapping[str, Any]:
        kwargs: dict[str, Any] = {"min_batch_size": 1, "max_batch_size": self._batch_size}
        if self._batch_seconds > 0 and self._batch_size > 1:
            # Agrupación con estado: emite al llenar el lote o a los N s, lo que pase primero.
            # min = max porque el estimador adaptativo de BatchElements arranca en 1 y crece
            # despacio (con lote 1 el CPU no alcanza). El timer de tiempo de proceso retiene la
            # marca de agua como mucho N s, no los 15 min del Lote 2.1.
            kwargs["min_batch_size"] = self._batch_size
            kwargs["max_batch_duration_secs"] = self._batch_seconds
        return kwargs

    def share_model_across_processes(self) -> bool:
        # Dataflow: un solo modelo por VM aunque haya varios procesos del SDK (memoria de
        # e2-standard-2). En local (DirectRunner) va en False: ahí el servidor de
        # MultiProcessShared vive en un hilo del mismo proceso y, como el DirectRunner rehace
        # el DoFn en cada bundle, el _decref de los proxies viejos acaba bloqueando al hilo
        # que acepta conexiones: el único hilo del DirectRunner queda esperando para siempre.
        return self._share

    def get_metrics_namespace(self) -> str:
        return f"{METRICS_NS}_{self._task}"


# --------------------------------------------------------------------------- #
# Fusión de resultados en la fila
# --------------------------------------------------------------------------- #


def _score(value: Any) -> float:
    s = float(value)
    return min(1.0, max(0.0, s))


class _MergeSentiment(beam.DoFn):
    def __init__(self):
        super().__init__()
        self._ok = Metrics.counter(METRICS_NS, "sentiment_rows")

    def process(self, result: PredictionResult):
        row = dict(result.example)
        inf = result.inference or {}
        row["sentiment_label"] = label_map.map_sentiment(str(inf["label"]))
        row["sentiment_score"] = _score(inf["score"])
        row["nlp_model_version"] = result.model_id  # se completa con la emoción
        self._ok.inc()
        yield row


class _MergeEmotion(beam.DoFn):
    def __init__(self):
        super().__init__()
        self._ok = Metrics.counter(METRICS_NS, "emotion_rows")

    def process(self, result: PredictionResult):
        row = dict(result.example)
        inf = result.inference or {}
        row["emotion_label"] = label_map.map_emotion(str(inf["label"]))
        row["emotion_score"] = _score(inf["score"])
        sent = row.get("nlp_model_version")
        row["nlp_model_version"] = f"{sent}+{result.model_id}" if sent else None
        if row.get("sentiment_label") is None:
            # sin sentimiento no hay versión completa: el NLP de la fila queda incompleto
            row["nlp_model_version"] = None
        self._ok.inc()
        yield row


def _clear_nlp(row: Mapping[str, Any]) -> dict:
    out = dict(row)
    for k in ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version"):
        out[k] = None
    return out


class _PassFailed(beam.DoFn):
    """Filas de un lote cuya inferencia falló: siguen sin NLP (no se pierden)."""

    def __init__(self, task: str):
        super().__init__()
        self._task = task
        self._failed = Metrics.counter(METRICS_NS, "nlp_failed_rows")

    def process(self, failed):
        batch, exc = failed if isinstance(failed, tuple) and len(failed) == 2 else ([failed], None)
        detail = str(exc[1])[:500] if isinstance(exc, tuple) and len(exc) > 1 else str(exc)[:500]
        rows = batch if isinstance(batch, list) else [batch]
        _log("nlp_inference_failed", task=self._task, rows=len(rows), error=detail)
        for row in rows:
            self._failed.inc()
            yield _clear_nlp(row)


class RobertuitoNlp(beam.PTransform):
    """Filas de ``posts_enriched`` → mismas filas con sentimiento, emoción y versión."""

    def __init__(self, sentiment_handler: ModelHandler, emotion_handler: ModelHandler):
        super().__init__()
        self._sent = sentiment_handler
        self._emo = emotion_handler

    def expand(self, rows):
        s_main, s_other = rows | "Sentiment" >> RunInference(self._sent).with_exception_handling()
        s_rows = s_main | "MergeSentiment" >> beam.ParDo(_MergeSentiment())
        s_failed = s_other.failed_inferences | "SentimentFailed" >> beam.ParDo(_PassFailed("sentiment"))
        after_sent = (s_rows, s_failed) | "AfterSentiment" >> beam.Flatten()

        e_main, e_other = after_sent | "Emotion" >> RunInference(self._emo).with_exception_handling()
        e_rows = e_main | "MergeEmotion" >> beam.ParDo(_MergeEmotion())
        e_failed = e_other.failed_inferences | "EmotionFailed" >> beam.ParDo(_PassFailed("emotion"))
        return (e_rows, e_failed) | "AfterEmotion" >> beam.Flatten()


# --------------------------------------------------------------------------- #
# Opciones, configuración e integración
# --------------------------------------------------------------------------- #


class NlpOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):  # noqa: D401 - API de Beam
        parser.add_argument("--nlp_enabled", default="auto", help="auto | true | false")
        parser.add_argument("--nlp_container_image", default=None, help="Imagen de los workers con los modelos")
        parser.add_argument("--nlp_batch_size", type=int, default=16, help="Tamaño máximo de lote por inferencia")
        parser.add_argument("--nlp_batch_seconds", type=float, default=None,
                            help="Espera máxima para juntar un lote (s); por defecto TA_NLP_BATCH_SECONDS o 5")


class NlpConfigError(ValueError):
    """Combinación de opciones inválida."""


@dataclass(frozen=True)
class NlpConfig:
    enabled: bool
    reason: str
    container_image: str | None
    batch_size: int
    is_dataflow: bool
    batch_seconds: float = 5.0

    def summary(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "reason": self.reason,
            "container_image": self.container_image,
            "batch_size": self.batch_size,
            "batch_seconds": self.batch_seconds,
        }


def default_image(project: str | None, region: str | None, environ: Mapping[str, str]) -> str | None:
    if environ.get("TA_NLP_IMAGE"):
        return environ["TA_NLP_IMAGE"]
    if not project:
        return None
    region = region or environ.get("GCP_REGION") or "us-central1"
    return f"{region}-docker.pkg.dev/{project}/{DEFAULT_REPOSITORY}/{IMAGE_NAME}:latest"


def resolve_nlp_config(options: PipelineOptions | None, environ: Mapping[str, str] | None = None) -> NlpConfig:
    env = os.environ if environ is None else environ
    options = options or PipelineOptions([])
    no = options.view_as(NlpOptions)
    gco = options.view_as(GoogleCloudOptions)
    runner = str(options.view_as(StandardOptions).runner or "DirectRunner")
    is_dataflow = "dataflow" in runner.lower()
    project = gco.project or env.get("GCP_PROJECT")
    # --nlp_enabled gana; si no se pasó (auto), TA_NLP_ENABLED permite apagarlo sin tocar dataflow_job.sh.
    flag = str(no.nlp_enabled or "auto").strip().lower()
    if flag == "auto" and env.get("TA_NLP_ENABLED"):
        flag = env["TA_NLP_ENABLED"].strip().lower()
    if flag not in ("auto", "true", "false"):
        raise NlpConfigError("--nlp_enabled debe ser auto, true o false")
    if no.nlp_batch_size < 1:
        raise NlpConfigError("--nlp_batch_size debe ser ≥ 1")
    raw_secs = no.nlp_batch_seconds if no.nlp_batch_seconds is not None else env.get("TA_NLP_BATCH_SECONDS", "5")
    batch_seconds = float(raw_secs)
    if not 0 <= batch_seconds <= 30:
        raise NlpConfigError("--nlp_batch_seconds debe estar entre 0 y 30 (la latencia objetivo es < 60 s)")

    image = None
    if flag == "false":
        enabled, reason = False, "--nlp_enabled=false"
    elif is_dataflow:
        image = no.nlp_container_image or options.view_as(WorkerOptions).sdk_container_image or default_image(project, gco.region, env)
        if not image:
            raise NlpConfigError("el NLP en Dataflow necesita --project o --nlp_container_image")
        enabled, reason = True, "dataflow"
    else:
        present = os.path.isfile(os.path.join(models_dir(env), MANIFEST))
        if flag == "true" and not present:
            raise NlpConfigError(f"--nlp_enabled=true pero no hay modelos en {models_dir(env)} (¿imagen dataflow-nlp?)")
        enabled = present
        reason = "modelos locales" if present else f"sin modelos en {models_dir(env)}"
    return NlpConfig(enabled, reason, image, int(no.nlp_batch_size), is_dataflow, batch_seconds)


def apply_container_image(options: PipelineOptions, cfg: NlpConfig) -> None:
    """Fija la imagen de los workers en las opciones del propio pipeline (antes de ``run()``)."""
    if not (cfg.enabled and cfg.is_dataflow and cfg.container_image):
        return
    wo = options.view_as(WorkerOptions)
    if wo.sdk_container_image and wo.sdk_container_image != cfg.container_image:
        raise NlpConfigError(f"sdk_container_image ({wo.sdk_container_image}) distinto de --nlp_container_image ({cfg.container_image})")
    wo.sdk_container_image = cfg.container_image
    so = options.view_as(SetupOptions)
    if not so.sdk_location or so.sdk_location == "default":
        so.sdk_location = "container"  # Beam ya viene en la imagen: no se vuelve a instalar
    _log("nlp_container_image", image=cfg.container_image)


def nlp_from_options(options: PipelineOptions | None) -> RobertuitoNlp | None:
    """Etapa NLP según las opciones, o ``None`` si está apagada."""
    cfg = resolve_nlp_config(options)
    _log("nlp_config", **cfg.summary())
    if not cfg.enabled:
        return None
    if options is not None:
        apply_container_image(options, cfg)
    return RobertuitoNlp(
        RobertuitoModelHandler("sentiment", batch_size=cfg.batch_size, batch_seconds=cfg.batch_seconds,
                               share_across_processes=cfg.is_dataflow),
        RobertuitoModelHandler("emotion", batch_size=cfg.batch_size, batch_seconds=cfg.batch_seconds,
                               share_across_processes=cfg.is_dataflow),
    )
