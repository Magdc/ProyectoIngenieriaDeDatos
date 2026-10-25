# Outputs del Lote 2.2 (§5.9).

output "bq_dataset" {
  description = "Dataset de BigQuery del streaming."
  value       = module.bigquery_stream.dataset_id
}

output "posts_enriched_table" {
  description = "proyecto:dataset.tabla de posts_enriched (formato de --posts_enriched_table)."
  value       = module.bigquery_stream.posts_enriched_table_spec
}

output "hmac_secret_id" {
  description = "Secreto de la llave HMAC (sin valor; se carga con scripts/hmac_key.sh)."
  value       = module.hmac_secret.secret_id
}
