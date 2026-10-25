"""Pipeline de streaming de TrendAnalyzer (Dataflow / Apache Beam).

Lote 2.1 (HU-018, HU-019): lectura de ``ta-raw-{source}-df``, validación contra
``contracts/``, deduplicación por ``event_id``, DLQ (``ta-dlq`` + ``dlq/``) y
escritura de ``raw/`` en el lake de GCS. Ver ARCHITECTURE.md §3.13.
"""

PIPELINE_VERSION = "2.1.0"
