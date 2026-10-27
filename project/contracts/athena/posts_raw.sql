-- ta_raw.posts_raw · ARCHITECTURE.md §5.7
-- Capa Raw: Parquet+Snappy tal como llega a S3 (ya anonimizado), stream (exportador) e historico (loader).
-- Placeholders de despliegue: {{lake_bucket}}. Los ${...} de abajo son variables de Athena (partition projection).
--
-- Layout fisico de S3 (los dos productores deben respetarlo):
--   s3://{{lake_bucket}}/raw/origin={stream|historical}/source={s}/dt={YYYY-MM-DD}/hr={HH}/*.parquet
-- Las columnas `origin` y `source` tambien viajan DENTRO del Parquet, por eso las claves de particion
-- se llaman part_* (Athena/Hive no admite una clave de particion con el nombre de una columna de datos).
-- El esquema de columnas es el de BigQuery posts_enriched (envelope enriquecido aplanado);
-- `payload` viaja como JSON en STRING. Debe coincidir columna a columna con bigquery/posts_enriched.sql.
--
-- Catálogo (Lote 5.3, §3.25): las líneas de comentario pegadas al CREATE son la descripción de la tabla en
-- Glue y el comentario de cada columna es su Comment, que empieza por su clase de privacidad entre corchetes.

-- Capa Raw (bronce): una fila por comentario simulado de redes sociales, tal como llega a S3 desde GCP (stream,
-- exportador gcs-to-s3) y desde la carga histórica (loader). Ya viene anonimizada: el autor es user_hash, nunca
-- user_id ni handle. Puede tener event_id repetidos, filas sin NLP y los centinelas ta-verify@* de los
-- verificadores: Trusted (ta_trusted.posts) los resuelve. Particiones: origen, red social, día y hora (en el
-- stream, de proceso; en el histórico, de event_time). Datos sintéticos.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_raw.posts_raw (
  event_id string,             -- [sin_dato_personal] Identificador del evento (UUID del simulador); clave de deduplicación
  origin string,               -- [sin_dato_personal] Origen del dato: stream (simulador en vivo) | historical (carga histórica)
  source string,               -- [sin_dato_personal] Red social simulada: reddit | facebook | instagram
  generator string,            -- [sin_dato_personal] Nombre y versión del generador de comentarios (ta-verify@* = centinela de validación)
  event_time timestamp,        -- [sin_dato_personal] Instante en que se publicó el comentario (UTC)
  ingest_time timestamp,       -- [sin_dato_personal] Instante en que el simulador o el loader lo emitió (UTC)
  process_time timestamp,      -- [sin_dato_personal] Instante en que Dataflow (o el loader) lo enriqueció (UTC)
  user_hash string,            -- [anonimizado] Autor seudonimizado: HMAC-SHA256 en hex (64) de user_id con la llave ta-hmac-key; mismo usuario, mismo hash
  followers bigint,            -- [cuasi_identificador] Seguidores del autor al publicar (puede ser nulo)
  text string,                 -- [texto_libre] Texto del comentario tal como se publicó (sintético: plantillas, emojis y hashtags)
  lang string,                 -- [sin_dato_personal] Idioma del texto (es)
  brands array<string>,        -- [sin_dato_personal] Marcas mencionadas, claves de brands.yaml (postobon, pepsi, sprite); puede ser vacío
  brand_primary string,        -- [sin_dato_personal] Marca principal: la primera de brands (nula si no hay)
  hashtags array<string>,      -- [sin_dato_personal] Hashtags del texto en forma canónica (minúsculas, sin #, [a-z0-9_]), en orden de aparición
  likes bigint,                -- [sin_dato_personal] Me gusta, normalizados desde payload según la red (§5.2)
  shares bigint,               -- [sin_dato_personal] Veces compartido, normalizado desde payload (solo Facebook lo tiene)
  comments bigint,             -- [sin_dato_personal] Respuestas o comentarios recibidos, normalizado desde payload
  sentiment_label string,      -- [sin_dato_personal] Sentimiento predicho por RoBERTuito: negative | neutral | positive (nulo si no hubo NLP)
  sentiment_score double,      -- [sin_dato_personal] Confianza del modelo en sentiment_label, en [0, 1]
  emotion_label string,        -- [sin_dato_personal] Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP)
  emotion_score double,        -- [sin_dato_personal] Confianza del modelo en emotion_label, en [0, 1]
  label_sentiment string,      -- [sin_dato_personal] Sentimiento de verdad del simulador (ground truth; nulo en categorías ambiguas). Solo para evaluar el NLP
  nlp_model_version string,    -- [sin_dato_personal] Modelos de NLP y commit con que se predijo (nulo sin NLP)
  payload string               -- [cuasi_identificador] JSON con los campos propios de la red (ids del post, enlace, página, reacciones); sin identidad del autor
)
PARTITIONED BY (
  part_origin string,          -- [sin_dato_personal] Partición de S3 origin=: stream | historical
  part_source string,          -- [sin_dato_personal] Partición de S3 source=: reddit | facebook | instagram
  part_dt string,              -- [sin_dato_personal] Partición de S3 dt=: día UTC (AAAA-MM-DD); en el stream, el de proceso; en el histórico, el de event_time
  part_hr string               -- [sin_dato_personal] Partición de S3 hr=: hora UTC (00-23), con el mismo criterio que part_dt
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/raw/'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'projection.enabled' = 'true',
  'projection.part_origin.type' = 'enum',
  'projection.part_origin.values' = 'stream,historical',
  'projection.part_source.type' = 'enum',
  'projection.part_source.values' = 'reddit,facebook,instagram',
  'projection.part_dt.type' = 'date',
  'projection.part_dt.format' = 'yyyy-MM-dd',
  'projection.part_dt.range' = '2026-01-01,NOW',
  'projection.part_dt.interval' = '1',
  'projection.part_dt.interval.unit' = 'DAYS',
  'projection.part_hr.type' = 'integer',
  'projection.part_hr.range' = '0,23',
  'projection.part_hr.digits' = '2',
  'storage.location.template' = 's3://{{lake_bucket}}/raw/origin=${part_origin}/source=${part_source}/dt=${part_dt}/hr=${part_hr}/'
);
