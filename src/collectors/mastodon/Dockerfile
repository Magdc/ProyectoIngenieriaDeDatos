# -----------------------------------------------------------------------
# Dockerfile — Mastodon Collector
# Cloud Run Gen2 · puerto 8080 · Python 3.12-slim
# -----------------------------------------------------------------------
# Este contenedor es la imagen referenciada por la variable
# collector_container_image en infra/gcp/environments/dev/variables.tf.
# Al hacer push a Artifact Registry, Terraform puede referenciar:
#   REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:TAG
# -----------------------------------------------------------------------

FROM python:3.12-slim

# Evitar archivos .pyc y hacer los logs inmediatos (sin buffer)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# Instalar dependencias primero (capa cacheada)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY main.py .

# Exponer el puerto que espera Cloud Run
EXPOSE 8080

# Gunicorn como servidor WSGI de producción
# - 1 worker (Cloud Run escala horizontalmente, no por workers)
# - timeout de 300 s para coincidir con el timeout configurado en cloud-run.tf
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "300", "main:app"]
