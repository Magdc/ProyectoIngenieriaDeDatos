-- ta_curated · ARCHITECTURE.md §5.7
-- Modelo analitico: esquema estrella + agregados + aristas. Parquet+Snappy en
-- s3://{{lake_bucket}}/curated/{tabla}/ . Placeholders de despliegue: {{lake_bucket}}.
-- Claves foraneas naturales (sin surrogate keys): fact_posts.source -> dim_source.source,
-- fact_posts.brand_primary -> dim_brand.brand, fact_posts.user_hash -> dim_user.user_hash,
-- fact_posts.date_key -> dim_date.date_key.
-- Las tablas no particionadas se regeneran completas en cada corrida; solo fact_posts se particiona por dt.

-- Un registro por post. Grano = event_id.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.fact_posts (
  event_id string,
  date_key int,
  origin string,
  source string,
  brand_primary string,
  brands array<string>,
  user_hash string,
  event_time timestamp,
  likes bigint,
  shares bigint,
  comments bigint,
  sentiment_label string,
  sentiment_score double,
  emotion_label string,
  emotion_score double,
  label_sentiment string
)
PARTITIONED BY (dt string)
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

CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_date (
  date_key int,            -- yyyymmdd
  full_date date,
  year int,
  month int,
  day int,
  iso_week int,
  day_of_week int,         -- 1 = lunes ... 7 = domingo
  is_weekend boolean
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_date/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_source (
  source string,           -- reddit | facebook | instagram
  display_name string,
  kind_mix string          -- descripcion de la mezcla de kind (§3.3)
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_source/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_brand (
  brand string,            -- clave canonica de brands.yaml (postobon, pepsi, sprite)
  display string,
  role string              -- propia | competidor
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_brand/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Solo user_hash: nunca handle ni user_id.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.dim_user (
  user_hash string,
  first_seen timestamp,
  last_seen timestamp,
  n_posts bigint,
  max_followers bigint
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/dim_user/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Conteos de emocion por marca / fuente / hora (formato largo).
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.agg_emotion_by_brand_hour (
  hour_start timestamp,    -- UTC, truncado a la hora
  brand string,
  source string,
  emotion_label string,
  n_posts bigint
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/agg_emotion_by_brand_hour/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Volumen y sentimiento diario por marca. avg_sentiment: misma definicion que BigQuery trend_windows
-- (con signo en [-1, 1]). El dia es UTC; los dashboards convierten a America/Bogota (§5.12).
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.agg_trend_daily (
  full_date date,
  brand string,
  n_posts bigint,
  n_negative bigint,
  n_neutral bigint,
  n_positive bigint,
  avg_sentiment double
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/agg_trend_daily/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Arista usuario -> marca; weight = numero de menciones.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.edges_user_brand (
  user_hash string,
  brand string,
  weight bigint
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/edges_user_brand/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Co-ocurrencia de hashtags; arista no dirigida, siempre hashtag_a < hashtag_b.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_curated.edges_hashtag_cooccurrence (
  hashtag_a string,
  hashtag_b string,
  weight bigint
)
STORED AS PARQUET
LOCATION 's3://{{lake_bucket}}/curated/edges_hashtag_cooccurrence/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
