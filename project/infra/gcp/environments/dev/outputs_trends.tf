# Outputs del Lote 2.4 (§5.9). Los lee scripts/dataflow_template.py para la plantilla.

output "trend_windows_table" {
  description = "trend_windows (proyecto:ta_stream.trend_windows)."
  value       = module.trends_bq_tables.trend_windows_table
}

output "trend_windows_latest_view" {
  description = "Vista con la versión vigente de cada ventana (proyecto.ta_stream.trend_windows_latest)."
  value       = module.trends_bq_tables.trend_windows_latest_view
}

output "dlq_events_table" {
  description = "dlq_events (proyecto:ta_stream.dlq_events)."
  value       = module.trends_bq_tables.dlq_events_table
}

output "dq_results_table" {
  description = "dq_results (proyecto:ta_stream.dq_results)."
  value       = module.trends_bq_tables.dq_results_table
}

output "dlq_df_subscription" {
  description = "Suscripción ta-dlq-df (projects/…/subscriptions/ta-dlq-df)."
  value       = google_pubsub_subscription.trends_dlq_df.id
}

# Estos dos outputs solo usan variables. Sin depends_on, un apply con -target (el que hace
# validate_2_4.py terraform) no los guarda en el estado: Terraform solo evalúa los outputs
# que dependen de algún recurso del objetivo.

output "dataflow_template_spec" {
  description = "Ruta de la plantilla Flex del pipeline de streaming."
  value       = "gs://${var.project_id}-ta-lake/_dataflow/templates/ta-stream.json"
  depends_on  = [google_pubsub_subscription.trends_dlq_df]
}

output "dataflow_launcher_image" {
  description = "Imagen del lanzador de la plantilla Flex (sin etiqueta)."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repository}/dataflow-launcher"
  depends_on  = [google_pubsub_subscription.trends_dlq_df]
}
