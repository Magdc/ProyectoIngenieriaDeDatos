# Lanzador de la plantilla Flex del pipeline de streaming (Lote 2.4, HU-005).
#
# No se construye con el contexto de project/ (su .dockerignore y .gcloudignore son
# listas de permitidos del simulador): scripts/dataflow_template.py arma un contexto
# temporal con este Dockerfile, src/streaming/requirements.txt (como requirements.txt),
# project/src/ (sin pruebas) y project/contracts/.
#
# Los workers NO usan esta imagen: usan dataflow-nlp (Lote 2.3) vía sdk_container_image,
# y reciben el código del proyecto con --setup_file (FLEX_TEMPLATE_PYTHON_SETUP_FILE).
# Por eso Beam debe ser la misma versión en las dos imágenes (2.77.0).

ARG LAUNCHER_BASE=gcr.io/dataflow-templates-base/python311-template-launcher-base:latest
FROM ${LAUNCHER_BASE}

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt /template/requirements.txt
RUN pip install -r /template/requirements.txt

COPY project/ /template/project/
# Igual que dataflow_job.sh: desde src/ (--contracts_dir=../contracts, --setup_file=./setup.py).
WORKDIR /template/project/src

ENV PYTHONPATH=/template/project/src \
    FLEX_TEMPLATE_PYTHON_PY_FILE=/template/project/src/streaming/flex_main.py \
    FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/project/src/setup.py

# Prueba de humo: misma versión de Beam que los workers y el pipeline importable.
RUN python -c "import apache_beam as b; assert b.__version__ == '2.77.0', b.__version__" \
 && test -f ./setup.py && test -d ../contracts \
 && python -c "import streaming.pipeline, streaming.trends, streaming.flex_main as f; assert f.patch_dataflow_result(); \
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult as R; assert R(None, None).job_id() is None; \
print('lanzador listo')"
