# syntax=docker/dockerfile:1
# Imagen de ta-stream-simulator (Lote 1.3, HU-016). Contexto de construcción: project/
#
#   docker build -f src/simulator/Dockerfile -t ta-stream-simulator .            # imagen final
#   docker build -f src/simulator/Dockerfile --target test .                     # corre las pruebas en Python 3.11
#
# Solo entran al contexto los archivos que permite project/.dockerignore.

ARG PYTHON_VERSION=3.11

# --- dependencias -------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS deps
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY src/simulator/requirements.txt src/simulator/constraints.txt ./
RUN python -m pip install --prefix=/install -r requirements.txt -c constraints.txt

# --- pruebas (Python 3.11, contra contracts/) -----------------------------------------
FROM python:${PYTHON_VERSION}-slim AS test
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY --from=deps /install /usr/local
RUN python -m pip install "pytest>=8" "jsonschema>=4.21" "referencing>=0.35"
COPY pytest.ini ./
COPY contracts/ contracts/
COPY src/conftest.py src/conftest.py
COPY src/common/ src/common/
COPY src/simulator/ src/simulator/
COPY scripts/generate_comments.py scripts/generate_comments.py
# Falla la construcción si una prueba falla o si alguna se omite (las de contrato deben correr).
RUN python -m pytest src/common src/simulator -q -rs -p no:cacheprovider > /tmp/pytest.log 2>&1; \
    rc=$?; cat /tmp/pytest.log; \
    if [ "$rc" -ne 0 ]; then exit "$rc"; fi; \
    if grep -qiE "[0-9]+ skipped" /tmp/pytest.log; then echo "ERROR: hay pruebas omitidas" >&2; exit 1; fi

# --- imagen final ---------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PORT=8080
WORKDIR /app
COPY --from=deps /install /usr/local
COPY src/common/ src/common/
COPY src/simulator/ src/simulator/
RUN rm -rf src/common/tests src/simulator/tests src/simulator/Dockerfile \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sim \
 && chown -R sim /app
USER 10001
EXPOSE 8080
# Cloud Run manda SIGTERM al apagar; app.py guarda los cursores antes de salir.
CMD ["python", "-m", "simulator.app"]
