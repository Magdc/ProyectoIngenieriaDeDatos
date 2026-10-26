"""Carga histórica (Lote 3.1, HU-024).

Genera el dataset histórico con el **mismo** simulador y generador del stream (otra
semilla), lo anonimiza y le corre el NLP con el mismo código del pipeline, y lo deja
como Parquet en ``raw/origin=historical/`` del lake de S3 (ARCHITECTURE.md §3.17).

Etapas (cada una reanudable, ver ``HISTORICAL.md``):

1. ``generate``  (host)        envelopes del simulador → aceptación por tendencias →
                                ``enrich_envelope()`` con la llave HMAC → ``stage1/``.
2. ``nlp``       (contenedor)  ``streaming.nlp`` en lote → ``stage2/``.
3. ``write``     (host)        Parquet por ``source/dt/hr`` (hora del evento) en el
                                espejo local del bucket.
4. ``verify``    (host)        reglas del lote; con ``--bq`` cruza ``user_hash`` y la
                                versión del NLP con el stream.
5. ``upload``    (host)        subida idempotente a ``s3://…/raw/origin=historical/``.

Los módulos de este paquete importan sus dependencias pesadas (simulador, pyarrow,
Beam, boto3, BigQuery) solo al usarlas: el mismo paquete corre en el host y dentro de
la imagen de NLP.
"""

TOOL_ID = "historical-loader@1.0"
