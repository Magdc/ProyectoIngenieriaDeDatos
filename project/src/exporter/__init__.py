"""Exportador GCS → S3 (Lote 3.2, HU-006 y HU-025).

Copia los Parquet de ``gs://{GCP_PROJECT}-ta-lake/enriched/`` a
``s3://{lake_bucket}/raw/origin=stream/`` con el mismo nombre (ARCHITECTURE.md §5.6).
La identidad es la de ``sa-exporter``: su ID token de Google asume el rol de AWS
``ta-gcp-exporter-role`` con ``AssumeRoleWithWebIdentity``, sin llaves estáticas.

El paquete no importa código de otros lotes: solo copia bytes. ``lake_check`` (parte F
de la validación) es la única pieza que usa ``common/`` y pyarrow, y los carga al usarse.
"""

EXPORTER_ID = "gcs-to-s3@1.0"

__all__ = ["EXPORTER_ID"]
