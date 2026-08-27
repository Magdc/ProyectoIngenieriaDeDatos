resource "google_cloud_scheduler_job" "reddit_ingestion" {
  name        = local.scheduler_reddit_job
  project     = var.project_id
  region      = var.region
  description = "Scheduled ingestion trigger for the Reddit collector."
  schedule    = var.reddit_ingestion_schedule
  time_zone   = var.scheduler_time_zone

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.collectors["reddit"].uri}/ingest"

    headers = {
      Content-Type = "application/json"
    }

    body = base64encode(jsonencode({
      source = "reddit"
    }))

    oidc_token {
      service_account_email = google_service_account.scheduler_invoker.email
      audience              = google_cloud_run_v2_service.collectors["reddit"].uri
    }
  }

  attempt_deadline = "300s"

  retry_config {
    retry_count = 3
  }

  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
}

resource "google_cloud_scheduler_job" "news_ingestion" {
  name        = local.scheduler_news_job
  project     = var.project_id
  region      = var.region
  description = "Scheduled ingestion trigger for the RSS and News API collector."
  schedule    = var.news_ingestion_schedule
  time_zone   = var.scheduler_time_zone

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.collectors["news"].uri}/ingest"

    headers = {
      Content-Type = "application/json"
    }

    body = base64encode(jsonencode({
      source = "news"
    }))

    oidc_token {
      service_account_email = google_service_account.scheduler_invoker.email
      audience              = google_cloud_run_v2_service.collectors["news"].uri
    }
  }

  attempt_deadline = "300s"

  retry_config {
    retry_count = 3
  }

  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
}
