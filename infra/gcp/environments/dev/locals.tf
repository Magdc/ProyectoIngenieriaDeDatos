locals {
  resource_prefix = "${var.name_prefix}-${var.environment}"
  common_labels = {
    application = "trend-analyzer"
    environment = var.environment
    managed_by  = "terraform"
    component   = "streaming"
  }
  pubsub_topic_raw_events        = "${local.resource_prefix}-raw-events"
  pubsub_topic_dead_letter       = "${local.resource_prefix}-dead-letter"
  pubsub_subscription_dataflow   = "${local.resource_prefix}-dataflow-sub"
  pubsub_subscription_deadletter = "${local.resource_prefix}-dead-letter-sub"

  cloud_run_mastodon_collector = "${local.resource_prefix}-mastodon-collector"
  cloud_run_news_collector     = "${local.resource_prefix}-news-collector"

  scheduler_news_job = "${local.resource_prefix}-news-ingestion-job"

  bigquery_dataset_streaming = replace("${local.resource_prefix}_streaming", "-", "_")

  raw_bucket_name = lower("${local.resource_prefix}-${var.project_id}-raw")

  collector_secret_ids = {
    mastodon_access_token = "${local.resource_prefix}-mastodon-access-token"
    news_api_key          = "${local.resource_prefix}-news-api-key"
  }

  dataflow_job_name          = "${local.resource_prefix}-ingestion-job"
  dataflow_template_gcs_path = "gs://${google_storage_bucket.dataflow_temp.name}/templates/ingestion_pipeline.json"
}
