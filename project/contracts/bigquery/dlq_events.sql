-- dlq_events · ARCHITECTURE.md §5.5
-- Mensajes que no pasan validacion (tambien se publican en el topic ta-dlq y en gs://...-ta-lake/dlq/).
-- source puede ser NULL: un mensaje malformado puede no dejar determinarla.
CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.dlq_events` (
  received_at TIMESTAMP NOT NULL,
  source STRING,
  error_type STRING NOT NULL,
  error_detail STRING,
  raw_message STRING
)
PARTITION BY DATE(received_at)
OPTIONS (
  description = "Eventos rechazados por validacion, con el mensaje original.",
  labels = [("project", "trend-analyzer"), ("env", "dev"), ("owner", "alejandro")]
);
