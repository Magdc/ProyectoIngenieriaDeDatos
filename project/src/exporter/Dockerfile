# Imagen del exportador gcs-to-s3 (Lote 3.2, ARCHITECTURE.md §3.18).
# Contexto: el directorio temporal que arma scripts/build_exporter_image.sh
# (exporter/ + este Dockerfile + requirements.txt + cloudbuild.yaml). No usa las
# listas de permitidos de project/ (.dockerignore, .gcloudignore).
#
# Etapas: deps → runtime (lo que se publica) → test (pruebas sin red; Cloud Build la
# construye primero y falla si alguna prueba falla o se omite).
ARG PYTHON_IMAGE=python:3.11-slim

FROM ${PYTHON_IMAGE} AS deps
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

FROM deps AS runtime
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin exporter
COPY exporter/ exporter/
RUN rm -rf exporter/tests exporter/Dockerfile exporter/cloudbuild.yaml && python -c "import exporter.cli, boto3, google.cloud.storage"
USER 10001
ENTRYPOINT ["python", "-m", "exporter.cli"]
CMD ["run"]

FROM deps AS test
RUN pip install pytest==8.3.5
COPY exporter/ exporter/
RUN python -m pytest exporter/tests -q -rs -p no:cacheprovider -m "not repo and not pyarrow" > /tmp/pytest.txt 2>&1; \
    status=$?; cat /tmp/pytest.txt; \
    if grep -q "SKIPPED" /tmp/pytest.txt; then echo "hay pruebas omitidas en la imagen" >&2; exit 1; fi; \
    exit $status
