# -----------------------------------------------------------------------
# Dockerfile — Profiler (Cloud Run Job)
# Cloud Run Jobs Gen2 · Python 3.12-slim
# -----------------------------------------------------------------------
# A diferencia de los collectors (que son servicios HTTP con /ingest),
# el profiler es un Job: corre hasta completar y termina.
# Cloud Run Jobs no necesita EXPOSE ni un servidor HTTP.
# -----------------------------------------------------------------------

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias de produccion
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir google-cloud-storage>=2.16,<3

# Copiar el modulo de calidad (importado por profiler.py)
COPY ../quality/expectations.py ./expectations.py
COPY ../quality/validator.py    ./validator.py

# Copiar el codigo del profiler
COPY profiler.py .

# El entrypoint lee RAW_BUCKET_NAME del entorno (inyectado por Terraform)
# y escribe el reporte en GCS bajo profiling/YYYY/MM/DD/
CMD ["python", "profiler.py", \
     "--gcs-bucket", "$(RAW_BUCKET_NAME)", \
     "--output",     "/tmp/profiling_report.md"]
