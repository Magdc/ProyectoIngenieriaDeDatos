-- posts_enriched · ARCHITECTURE.md §5.5
-- Un registro por evento enriquecido y anonimizado (envelope enriquecido aplanado).
-- Reemplazar {{GCP_PROJECT}} al aplicar. Dataset: ta_stream (ubicación = GCP_REGION, us-east1).
-- Mapeo desde el envelope enriquecido:
--   user_hash <- author.user_hash        followers <- author.followers
--   label_sentiment <- sim.label_sentiment (ground truth, SOLO evaluación)
--   brand_primary <- brands[0] (NULL si brands = [])
-- Retención: expiración de partición a 90 días (§5.12).
CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.posts_enriched` (
  event_id STRING NOT NULL,
  origin STRING NOT NULL,
  source STRING NOT NULL,
  generator STRING NOT NULL,
  event_time TIMESTAMP NOT NULL,
  ingest_time TIMESTAMP NOT NULL,
  process_time TIMESTAMP NOT NULL,
  user_hash STRING NOT NULL,
  followers INT64,
  text STRING NOT NULL,
  lang STRING,
  brands ARRAY<STRING>,
  brand_primary STRING,
  hashtags ARRAY<STRING>,
  likes INT64,
  shares INT64,
  comments INT64,
  sentiment_label STRING NOT NULL,
  sentiment_score FLOAT64,
  emotion_label STRING NOT NULL,
  emotion_score FLOAT64,
  label_sentiment STRING,
  nlp_model_version STRING NOT NULL,
  payload JSON
)
PARTITION BY DATE(event_time)
CLUSTER BY source, brand_primary
OPTIONS (
  partition_expiration_days = 90,
  description = "Eventos enriquecidos y anonimizados (stream). event_id es unico por contrato; BigQuery no lo impone, lo garantiza el dedup de Dataflow.",
  labels = [("project", "trend-analyzer"), ("env", "dev"), ("owner", "alejandro")]
);
