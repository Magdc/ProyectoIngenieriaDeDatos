# Imagen de los workers de Dataflow con NLP (Lote 2.3, HU-022).
# Contexto: project/src/streaming/nlp_image (autocontenido; no usa el .dockerignore de project/).
#
#   runtime : SDK de Beam 2.77 (Python 3.11) + torch CPU + transformers + pysentimiento (solo el
#             preprocesado) + los dos modelos de RoBERTuito horneados. La usa Dataflow.
#   local   : la misma, sin el ENTRYPOINT del SDK, para el servicio dataflow-local (DirectRunner).
#
# El código del proyecto (streaming, common) NO va en la imagen: Dataflow lo instala con
# --setup_file y en local se monta en /work.

ARG BEAM_IMAGE=apache/beam_python3.11_sdk:2.77.0

FROM ${BEAM_IMAGE} AS models
ARG SENTIMENT_REVISION=main
ARG EMOTION_REVISION=main
RUN pip install --no-cache-dir huggingface_hub==0.26.5
COPY fetch_models.py /tmp/fetch_models.py
RUN python /tmp/fetch_models.py /opt/ta-nlp/models \
      --sentiment-revision "${SENTIMENT_REVISION}" --emotion-revision "${EMOTION_REVISION}"

FROM ${BEAM_IMAGE} AS runtime
ENV TA_NLP_MODELS_DIR=/opt/ta-nlp/models \
    HF_HOME=/tmp/hf \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    TOKENIZERS_PARALLELISM=false \
    OMP_NUM_THREADS=1
COPY requirements-nlp.txt /tmp/requirements-nlp.txt
# torch para CPU desde el índice de PyTorch; luego transformers y el preprocesado de pysentimiento
# (sin sus dependencias: datasets, spacy y accelerate no hacen falta). Se comprueba que la
# versión de Beam no cambió.
RUN BEAM_BEFORE="$(python -c 'import apache_beam; print(apache_beam.__version__)')" \
 && pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r /tmp/requirements-nlp.txt \
 && pip install --no-cache-dir --no-deps pysentimiento==0.7.3 \
 && BEAM_AFTER="$(python -c 'import apache_beam; print(apache_beam.__version__)')" \
 && test "$BEAM_BEFORE" = "$BEAM_AFTER" \
 && (pip check || true) \
 && python -c "import apache_beam, torch, transformers; print('beam', apache_beam.__version__, 'torch', torch.__version__, 'transformers', transformers.__version__)"
COPY --from=models /opt/ta-nlp/models /opt/ta-nlp/models
COPY image_smoke.py /opt/ta-nlp/image_smoke.py
RUN python /opt/ta-nlp/image_smoke.py

FROM runtime AS local
ENTRYPOINT []
ENV PYTHONPATH=/work/src
CMD ["python", "-c", "print('ta-dataflow-nlp-local: el comando lo define docker-compose')"]
