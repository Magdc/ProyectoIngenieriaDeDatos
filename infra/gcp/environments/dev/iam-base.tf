locals {
  workload_service_accounts = {
    mastodon_collector = google_service_account.mastodon_collector.email
    reddit_collector   = google_service_account.reddit_collector.email
    news_collector     = google_service_account.news_collector.email
    dataflow_worker    = google_service_account.dataflow_worker.email
  }
}

resource "google_project_iam_member" "workload_log_writer" {
  for_each = local.workload_service_accounts

  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "workload_metric_writer" {
  for_each = local.workload_service_accounts

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}
