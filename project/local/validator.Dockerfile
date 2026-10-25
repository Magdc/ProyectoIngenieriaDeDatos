# Herramientas del entorno local (Lote 1.5, HU-047): pubsub-init y validator.
# Solo dependencias: el código (src/, scripts/, contracts/) se monta en /work como volumen
# de solo lectura, así que no hace falta reconstruir la imagen al cambiarlo.
# Versiones: las de src/simulator/constraints.txt (Lote 1.3) y jsonschema del Lote 1.1.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --no-cache-dir \
        google-cloud-pubsub==2.43.0 \
        google-cloud-storage==3.17.1 \
        jsonschema==4.26.0 \
        "referencing>=0.36,<1"

WORKDIR /work
