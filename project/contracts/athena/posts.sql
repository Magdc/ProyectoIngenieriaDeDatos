-- ta_trusted.posts · ARCHITECTURE.md §5.7
-- Capa Trusted: union stream + historico, deduplicado por event_id, tipos correctos, texto limpio.
-- Placeholders de despliegue: {{lake_bucket}}.
--
-- Layout: s3://{{lake_bucket}}/trusted/posts/source={s}/dt={YYYY-MM-DD}/*.parquet
-- Glue escribe con partitionBy(source, dt), asi que esas dos columnas NO estan dentro del Parquet.
-- dt = DATE(event_time) en UTC (decision de este contrato; en Raw, dt es la fecha de proceso en el stream y la de event_time en el historico).
-- Idempotencia: Glue sobrescribe la particion dt completa (§5.12).
-- Respecto a posts_raw: sin `source` como columna (es particion) y con text_clean.
--
-- Catálogo (Lote 5.3, §3.25): las líneas de comentario pegadas al CREATE son la descripción de la tabla en
-- Glue y el comentario de cada columna es su Comment, que empieza por su clase de privacidad entre corchetes.

-- Capa Trusted (plata): una fila por comentario (event_id único), unión de stream e histórico. La reconstruye
-- entera cada hora ta-job-raw-to-trusted desde Raw: deduplica por event_id (se queda con el menor process_time),
-- descarta los centinelas ta-verify@* y las filas sin event_id o event_time, corrige tipos y añade text_clean.
-- Las filas sin NLP se conservan con el NLP en nulo. Particiones: red social y día UTC de event_time.
-- Fuente de Curated. Datos sintéticos, ya anonimizados.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_trusted.posts (
  event_id string,             -- [sin_dato_personal] Identificador del evento (UUID del simulador); único en esta tabla
  origin string,               -- [sin_dato_personal] Origen del dato: stream (simulador en vivo) | historical (carga histórica)
  generator string,            -- [sin_dato_personal] Nombre y versión del generador de comentarios
  event_time timestamp,        -- [sin_dato_personal] Instante en que se publicó el comentario (UTC); nunca nulo
  ingest_time timestamp,       -- [sin_dato_personal] Instante en que el simulador o el loader lo emitió (UTC)
  process_time timestamp,      -- [sin_dato_personal] Instante en que Dataflow (o el loader) lo enriqueció (UTC)
  user_hash string,            -- [anonimizado] Autor seudonimizado: HMAC-SHA256 en hex (64) de user_id con la llave ta-hmac-key; mismo usuario, mismo hash
  followers bigint,            -- [cuasi_identificador] Seguidores del autor al publicar (puede ser nulo)
  text string,                 -- [texto_libre] Texto original del comentario, sin cambios (sintético)
  text_clean string,           -- [texto_libre] Texto normalizado con la regla text-clean@1: sin emojis, URLs, menciones ni hashtags, en minúsculas
  lang string,                 -- [sin_dato_personal] Idioma del texto (es)
  brands array<string>,        -- [sin_dato_personal] Marcas mencionadas, claves de brands.yaml (postobon, pepsi, sprite); puede ser vacío
  brand_primary string,        -- [sin_dato_personal] Marca principal: la primera de brands (nula si no hay)
  hashtags array<string>,      -- [sin_dato_personal] Hashtags del texto en forma canónica (minúsculas, sin #, [a-z0-9_]), en orden de aparición
  likes bigint,                -- [sin_dato_personal] Me gusta, normalizados desde payload según la red (§5.2)
  shares bigint,               -- [sin_dato_personal] Veces compartido, normalizado desde payload (solo Facebook lo tiene)
  comments bigint,             -- [sin_dato_personal] Respuestas o comentarios recibidos, normalizado desde payload
  sentiment_label string,      -- [sin_dato_personal] Sentimiento predicho por RoBERTuito: negative | neutral | positive (nulo si la fila no tiene NLP)
  sentiment_score double,      -- [sin_dato_personal] Confianza del modelo en sentiment_label, en [0, 1]
  emotion_label string,        -- [sin_dato_personal] Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP)
  emotion_score double,        -- [sin_dato_personal] Confianza del modelo en emotion_label, en [0, 1]
  label_sentiment string,      -- [sin_dato_personal] Sentimiento de verdad del simulador (ground truth; nulo en categorías ambiguas). Solo para evaluar el NLP
  nlp_model_version string,    -- [sin_dato_personal] Modelos de NLP y commit con que se predijo (nulo sin NLP)
  payload string               -- [cuasi_identificador] JSON con los campos propios de la red (ids del post, enlace, página, reacciones); sin identidad del autor
)
PARTITIONED BY (
  source string,               -- [sin_dato_personal] Red social simulada (partición source=): reddit | facebook | instagram
  dt string                    -- [sin_dato_personal] Día UTC de event_time (partición dt=, AAAA-MM-DD)
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/trusted/posts/'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'reddit,facebook,instagram',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2026-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://{{lake_bucket}}/trusted/posts/source=${source}/dt=${dt}/'
);
