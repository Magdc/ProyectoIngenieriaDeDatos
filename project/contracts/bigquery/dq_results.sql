-- dq_results · ARCHITECTURE.md §5.5 y §5.10
-- Una fila por verificacion de calidad ejecutada. La definicion de columnas la fija este contrato
-- (ARCHITECTURE.md solo nombra la tabla). Tambien se escribe un reporte en s3://...-lake/quality/{run_date}/.
-- kpi: completitud | unicidad | duplicados | anonimizacion | latencia | precision_nlp | otro
CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.dq_results` (
  run_id STRING NOT NULL,
  run_ts TIMESTAMP NOT NULL,
  layer STRING NOT NULL,
  table_name STRING NOT NULL,
  suite STRING,
  check_name STRING NOT NULL,
  kpi STRING,
  observed_value FLOAT64,
  threshold FLOAT64,
  passed BOOL NOT NULL,
  details JSON
)
PARTITION BY DATE(run_ts)
CLUSTER BY layer, kpi
OPTIONS (
  description = "Resultados de calidad de datos (Great Expectations / dbt) por ejecucion.",
  labels = [("project", "trend-analyzer"), ("env", "dev"), ("owner", "alejandro")]
);
