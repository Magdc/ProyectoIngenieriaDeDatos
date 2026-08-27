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
  cloud_run_reddit_collector   = "${local.resource_prefix}-reddit-collector"
  cloud_run_news_collector     = "${local.resource_prefix}-news-collector"

  scheduler_reddit_job = "${local.resource_prefix}-reddit-ingestion-job"
  scheduler_news_job   = "${local.resource_prefix}-news-ingestion-job"

  bigquery_dataset_streaming = replace("${local.resource_prefix}_streaming", "-", "_")

  raw_bucket_name = lower("${local.resource_prefix}-${var.project_id}-raw")

  collector_secret_ids = {
    mastodon_access_token = "${local.resource_prefix}-mastodon-access-token"
    reddit_client_id      = "${local.resource_prefix}-reddit-client-id"
    reddit_client_secret  = "${local.resource_prefix}-reddit-client-secret"
    reddit_user_agent     = "${local.resource_prefix}-reddit-user-agent"
    news_api_key          = "${local.resource_prefix}-news-api-key"
  }
}
