
resource "google_service_account" "mastodon_collector" {
  account_id   = "${local.resource_prefix}-mastodon-sa"
  display_name = "Trend Analyzer Mastodon Collector"
  description  = "Service account used by the Mastodon Cloud Run collector."
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_service_account" "reddit_collector" {
  account_id   = "${local.resource_prefix}-reddit-sa"
  display_name = "Trend Analyzer Reddit Collector"
  description  = "Service account used by the Reddit Cloud Run collector."
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_service_account" "news_collector" {
  account_id   = "${local.resource_prefix}-news-sa"
  display_name = "Trend Analyzer News Collector"
  description  = "Service account used by the News Cloud Run collector."
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_service_account" "scheduler_invoker" {
  account_id   = "${local.resource_prefix}-scheduler-sa"
  display_name = "Trend Analyzer Cloud Scheduler Invoker"
  description  = "Service account used by Cloud Scheduler to invoke Cloud Run collectors."
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

resource "google_service_account" "dataflow_worker" {
  account_id   = "${local.resource_prefix}-dataflow-sa"
  display_name = "Trend Analyzer Dataflow Worker"
  description  = "Service account reserved for the future Dataflow streaming pipeline."
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}