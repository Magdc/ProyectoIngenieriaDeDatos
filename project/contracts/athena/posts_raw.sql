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
CREATE EXTERNAL TABLE IF NOT EXISTS ta_raw.posts_raw (
  event_id string,
  origin string,
  source string,
  generator string,
  event_time timestamp,
  ingest_time timestamp,
  process_time timestamp,
  user_hash string,
  followers bigint,
  text string,
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
  part_origin string,
  part_source string,
  part_dt string,
  part_hr string
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
