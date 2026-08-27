output "environment" {
  description = "Ambiente configurado para este despliegue."
  value       = var.environment
}
output "region" {
  description = "Region principal configurada para los recursos de GCP."
  value       = var.region
}

output "resource_prefix" {
  description = "Prefijo comun usado para nombrar recursos."
  value       = local.resource_prefix
}

output "raw_bucket_name" {
  description = "Nombre esperado del bucket Raw de Cloud Storage."
  value       = local.raw_bucket_name
}

output "pubsub_topic_raw_events" {
  description = "Nombre esperado del topic principal de Pub/Sub para eventos raw."
  value       = local.pubsub_topic_raw_events
}

output "bigquery_dataset_streaming" {
  description = "ID esperado del dataset de BigQuery para datos recientes procesados."
  value       = local.bigquery_dataset_streaming
}

output "cloud_run_collector_urls" {
  description = "URLs de los collectors desplegados en Cloud Run."
  value       = { for name, service in google_cloud_run_v2_service.collectors : name => service.uri }
}

output "scheduler_jobs" {
  description = "Jobs programados para ingesta near-real-time."
  value = {
    reddit = google_cloud_scheduler_job.reddit_ingestion.name
    news   = google_cloud_scheduler_job.news_ingestion.name
  }
}

output "secret_ids" {
  description = "Secretos declarados para credenciales externas. Los valores reales se cargan fuera de Terraform."
  value       = { for name, secret in google_secret_manager_secret.collector_secrets : name => secret.secret_id }
}
