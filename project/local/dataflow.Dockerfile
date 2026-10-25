# Imagen `ta-dataflow-local` (Lote 2.1): Apache Beam con DirectRunner para el
# servicio `dataflow-local` y el verificador `streaming-check`
# (docker-compose.dataflow.yml). Solo trae dependencias: el código (src/,
# scripts/, contracts/) se monta en /work en solo lectura, así que un cambio de
# código no exige reconstruirla.
#
# Las versiones salen de src/streaming/requirements.txt, que llega como
# contexto adicional `streaming` (no depende de project/.dockerignore).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp \
    PYTHONPATH=/work/src

COPY --from=streaming requirements.txt /tmp/streaming-requirements.txt
RUN pip install -r /tmp/streaming-requirements.txt \
 && python -c "import apache_beam, jsonschema, referencing, google.cloud.pubsub_v1; print('beam', apache_beam.__version__)"

WORKDIR /work
CMD ["python", "-m", "streaming.pipeline", "--help"]
