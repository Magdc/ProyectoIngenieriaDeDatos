-- trend_windows · ARCHITECTURE.md §5.5
-- Ventanas deslizantes de 5 min (paso 1 min) por brand x source.
-- avg_sentiment: media del sentimiento con signo en [-1, 1]
--   (positive = +sentiment_score, negative = -sentiment_score, neutral = 0).
-- zscore_*: contra baseline movil; NULL mientras no haya baseline (is_alert = FALSE hasta 60 ventanas, §5.12).
CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.trend_windows` (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  brand STRING NOT NULL,
  source STRING NOT NULL,
  n_posts INT64 NOT NULL,
  n_negative INT64 NOT NULL,
  n_positive INT64 NOT NULL,
  avg_sentiment FLOAT64,
  emotion_counts JSON,
  zscore_volume FLOAT64,
  zscore_sentiment FLOAT64,
  is_alert BOOL NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY brand, source
OPTIONS (
  description = "Ventanas de tendencia de 5 min (paso 1 min) por marca y fuente.",
  labels = [("project", "trend-analyzer"), ("env", "dev"), ("owner", "alejandro")]
);
