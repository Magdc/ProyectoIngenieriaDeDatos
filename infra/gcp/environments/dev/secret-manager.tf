resource "google_secret_manager_secret" "collector_secrets" {
  for_each = local.collector_secret_ids

  project   = var.project_id
  secret_id = each.value
  labels    = local.common_labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_iam_member" "mastodon_collector_access" {
  secret_id = google_secret_manager_secret.collector_secrets["mastodon_access_token"].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.mastodon_collector.email}"
}

resource "google_secret_manager_secret_iam_member" "news_collector_access" {
  secret_id = google_secret_manager_secret.collector_secrets["news_api_key"].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.news_collector.email}"
}