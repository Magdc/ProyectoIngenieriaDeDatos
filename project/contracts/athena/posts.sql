-- ta_trusted.posts · ARCHITECTURE.md §5.7
-- Capa Trusted: union stream + historico, deduplicado por event_id, tipos correctos, texto limpio.
-- Placeholders de despliegue: {{lake_bucket}}.
--
-- Layout: s3://{{lake_bucket}}/trusted/posts/source={s}/dt={YYYY-MM-DD}/*.parquet
-- Glue escribe con partitionBy(source, dt), asi que esas dos columnas NO estan dentro del Parquet.
-- dt = DATE(event_time) en UTC (decision de este contrato; la de Raw es la fecha de proceso).
-- Idempotencia: Glue sobrescribe la particion dt completa (§5.12).
-- Respecto a posts_raw: sin `source` como columna (es particion) y con text_clean.
CREATE EXTERNAL TABLE IF NOT EXISTS ta_trusted.posts (
  event_id string,
  origin string,
  generator string,
  event_time timestamp,
  ingest_time timestamp,
  process_time timestamp,
  user_hash string,
  followers bigint,
  text string,
  text_clean string,
  lang string,
  brands array<string>,
  brand_primary string,
  hashtags array<string>,
  likes bigint,
  shares bigint,
  comments bigint,
  sentiment_label string,
  sentiment_score double,
  emotion_label string,
  emotion_score double,
  label_sentiment string,
  nlp_model_version string,
  payload string
)
PARTITIONED BY (
  source string,
  dt string
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
