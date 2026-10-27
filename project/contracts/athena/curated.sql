-- ta_curated · ARCHITECTURE.md §5.7
-- Modelo analitico: esquema estrella + agregados + aristas. Parquet+Snappy en
-- s3://{{lake_bucket}}/curated/{tabla}/ . Placeholders de despliegue: {{lake_bucket}}.
-- Claves foraneas naturales (sin surrogate keys): fact_posts.source -> dim_source.source,
-- fact_posts.brand_primary -> dim_brand.brand, fact_posts.user_hash -> dim_user.user_hash,
-- fact_posts.date_key -> dim_date.date_key.
-- Las tablas no particionadas se regeneran completas en cada corrida; solo fact_posts se particiona por dt.
--
-- Catálogo (Lote 5.3, §3.25): las líneas de comentario pegadas a cada CREATE son la descripción de la tabla en
-- Glue y el comentario de cada columna es su Comment, que empieza por su clase de privacidad entre corchetes.
-- Todas las tablas las escribe ta-job-trusted-to-curated (cada hora, desde ta_trusted.posts).

-- Hechos: un registro por comentario de Trusted (grano = event_id), con sus llaves a las dimensiones, el engagement
-- y el NLP (nulo en las filas sin NLP). Partición dt = día UTC de event_time.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.fact_posts (
  event_id string,             -- [sin_dato_personal] Identificador del comentario (UUID del simulador); grano de la tabla
  date_key int,                -- [sin_dato_personal] Día UTC de event_time como entero yyyymmdd; llave a dim_date.date_key
  origin string,               -- [sin_dato_personal] Origen del dato: stream | historical
  source string,               -- [sin_dato_personal] Red social simulada; llave a dim_source.source
  brand_primary string,        -- [sin_dato_personal] Marca principal (la primera de brands); llave a dim_brand.brand
  brands array<string>,        -- [sin_dato_personal] Todas las marcas mencionadas (claves de brands.yaml)
  user_hash string,            -- [anonimizado] Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave a dim_user.user_hash
  event_time timestamp,        -- [sin_dato_personal] Instante en que se publicó el comentario (UTC)
  likes bigint,                -- [sin_dato_personal] Me gusta, normalizados según la red
  shares bigint,               -- [sin_dato_personal] Veces compartido (solo Facebook)
  comments bigint,             -- [sin_dato_personal] Respuestas o comentarios recibidos
  sentiment_label string,      -- [sin_dato_personal] Sentimiento predicho: negative | neutral | positive (nulo sin NLP)
  sentiment_score double,      -- [sin_dato_personal] Confianza del modelo en sentiment_label, en [0, 1]
  emotion_label string,        -- [sin_dato_personal] Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP)
  emotion_score double,        -- [sin_dato_personal] Confianza del modelo en emotion_label, en [0, 1]
  label_sentiment string       -- [sin_dato_personal] Sentimiento de verdad del simulador (ground truth); solo para medir la precisión del NLP
)
PARTITIONED BY (
  dt string                    -- [sin_dato_personal] Día UTC de event_time (partición dt=, AAAA-MM-DD)
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/fact_posts/'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2026-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://{{lake_bucket}}/curated/fact_posts/dt=${dt}/'
);

-- Dimensión calendario: un día por fila, continuo desde el primer hasta el último dt de fact_posts.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_date (
  date_key int,                -- [sin_dato_personal] Día como entero yyyymmdd (llave)
  full_date date,              -- [sin_dato_personal] Fecha del día (UTC)
  year int,                    -- [sin_dato_personal] Año
  month int,                   -- [sin_dato_personal] Mes (1-12)
  day int,                     -- [sin_dato_personal] Día del mes (1-31)
  iso_week int,                -- [sin_dato_personal] Semana ISO 8601 (1-53)
  day_of_week int,             -- [sin_dato_personal] Día de la semana: 1 = lunes ... 7 = domingo
  is_weekend boolean           -- [sin_dato_personal] Verdadero en sábado y domingo
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_date/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Dimensión red social: las tres fuentes simuladas con su nombre visible y su mezcla de publicaciones y comentarios.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_source (
  source string,               -- [sin_dato_personal] Red social simulada (llave): reddit | facebook | instagram
  display_name string,         -- [sin_dato_personal] Nombre para mostrar en los tableros
  kind_mix string              -- [sin_dato_personal] Mezcla de kind de la fuente (§3.3), p. ej. post 30% · comment 70%
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_source/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Dimensión marca: las marcas de brands.yaml (propia y competidores), más cualquier marca de los datos que no esté
-- (con display y role nulos).
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_brand (
  brand string,                -- [sin_dato_personal] Clave canónica de brands.yaml (postobon, pepsi, sprite); llave
  display string,              -- [sin_dato_personal] Nombre para mostrar de la marca
  role string                  -- [sin_dato_personal] Papel de la marca para el análisis: propia | competidor
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_brand/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Dimensión autor seudonimizado: una fila por user_hash con su actividad. Solo user_hash: nunca handle ni user_id.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_user (
  user_hash string,            -- [anonimizado] Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave
  first_seen timestamp,        -- [cuasi_identificador] Primer comentario del autor (mínimo event_time, UTC)
  last_seen timestamp,         -- [cuasi_identificador] Último comentario del autor (máximo event_time, UTC)
  n_posts bigint,              -- [cuasi_identificador] Comentarios del autor en fact_posts
  max_followers bigint         -- [cuasi_identificador] Máximo de seguidores observado para el autor
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_user/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Conteos de emoción por hora UTC, marca principal, red social y emoción (formato largo). Solo cuenta las filas con
-- emoción predicha.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.agg_emotion_by_brand_hour (
  hour_start timestamp,        -- [sin_dato_personal] Inicio de la hora (UTC, truncado a la hora)
  brand string,                -- [sin_dato_personal] Marca principal (brand_primary)
  source string,               -- [sin_dato_personal] Red social simulada
  emotion_label string,        -- [sin_dato_personal] Emoción predicha
  n_posts bigint               -- [sin_dato_personal] Comentarios de esa hora, marca, red y emoción
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/agg_emotion_by_brand_hour/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Volumen y sentimiento diario por marca principal. n_posts cuenta todas las filas; los conteos por sentimiento y
-- avg_sentiment, solo las que tienen sentimiento. El día es UTC; los tableros convierten a America/Bogota (§5.12).
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.agg_trend_daily (
  full_date date,              -- [sin_dato_personal] Día UTC
  brand string,                -- [sin_dato_personal] Marca principal (brand_primary)
  n_posts bigint,              -- [sin_dato_personal] Comentarios del día y la marca (también los que no tienen NLP)
  n_negative bigint,           -- [sin_dato_personal] Comentarios con sentimiento negative
  n_neutral bigint,            -- [sin_dato_personal] Comentarios con sentimiento neutral
  n_positive bigint,           -- [sin_dato_personal] Comentarios con sentimiento positive
  avg_sentiment double         -- [sin_dato_personal] Media de +score (positive), -score (negative) y 0 (neutral), en [-1, 1]; como trend_windows
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/agg_trend_daily/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Arista usuario -> marca para análisis de relaciones: una fila por par (user_hash, marca); weight = número de
-- menciones (filas del usuario cuyo brands contiene la marca).
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.edges_user_brand (
  user_hash string,            -- [anonimizado] Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave a dim_user
  brand string,                -- [sin_dato_personal] Marca mencionada; llave a dim_brand
  weight bigint                -- [sin_dato_personal] Comentarios del autor que mencionan la marca (cada marca una vez por comentario)
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/edges_user_brand/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Coocurrencia de hashtags: arista no dirigida, siempre hashtag_a < hashtag_b; weight = comentarios con los dos.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.edges_hashtag_cooccurrence (
  hashtag_a string,            -- [sin_dato_personal] Primer hashtag del par (forma canónica; el menor por punto de código)
  hashtag_b string,            -- [sin_dato_personal] Segundo hashtag del par (siempre mayor que hashtag_a)
  weight bigint                -- [sin_dato_personal] Comentarios que contienen los dos hashtags
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/edges_hashtag_cooccurrence/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
