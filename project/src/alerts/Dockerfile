# Imagen del notificador de alertas ta-alert-notifier (Lote 6.2, ARCHITECTURE.md §3.28).
# Contexto: el directorio temporal que arma scripts/build_alerts_image.sh (alerts/__init__.py,
# alerts/notifier.py, alerts/tests/test_notifier.py, common/__init__.py, common/alert_message.py,
# este Dockerfile y cloudbuild.yaml). Solo biblioteca estándar: no hay requirements.txt.
#
# Etapas: base → runtime (lo que se publica) → test (pruebas sin red; Cloud Build la construye
# primero y falla si alguna prueba falla o se omite).
ARG PYTHON_IMAGE=python:3.11-slim

FROM ${PYTHON_IMAGE} AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    PORT=8080
WORKDIR /app
COPY common/ common/
COPY alerts/ alerts/

FROM base AS runtime
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin notifier \
    && rm -rf alerts/tests && python -c "import alerts.notifier"
USER 10001
EXPOSE 8080
ENTRYPOINT ["python", "-m", "alerts.notifier"]

FROM base AS test
RUN pip install pytest==8.3.5
RUN python -m pytest alerts/tests -q -rs -p no:cacheprovider > /tmp/pytest.txt 2>&1; \
    status=$?; cat /tmp/pytest.txt; \
    if grep -q "SKIPPED" /tmp/pytest.txt; then echo "hay pruebas omitidas en la imagen" >&2; exit 1; fi; \
    exit $status
