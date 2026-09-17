locals {
  cloud_run_collectors = {
    mastodon = {
      name                  = local.cloud_run_mastodon_collector
      source                = "mastodon"
      service_account_email = google_service_account.mastodon_collector.email
      secret_env_vars = {
        MASTODON_ACCESS_TOKEN_SECRET = local.collector_secret_ids.mastodon_access_token
      }
    }
    news = {
      name                  = local.cloud_run_news_collector
      source                = "news"
      service_account_email = google_service_account.news_collector.email
      secret_env_vars = {
        NEWS_API_KEY_SECRET = local.collector_secret_ids.news_api_key
      }
    }
  }
}

resource "google_cloud_run_v2_service" "collectors" {
  for_each = local.cloud_run_collectors

  name                = each.value.name
  project             = var.project_id
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  labels              = local.common_labels
  deletion_protection = false

  template {
    service_account                  = each.value.service_account_email
    timeout                          = "300s"
    max_instance_request_concurrency = 80
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN2"

    scaling {
      min_instance_count = 0
      max_instance_count = var.collector_max_instances
    }

    containers {
      image = var.collector_container_image

      ports {
        name           = "http1"
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = var.collector_cpu
          memory = var.collector_memory
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "SOURCE_NAME"
        value = each.value.source
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "PUBSUB_TOPIC"
        value = google_pubsub_topic.raw_events.name
      }

      env {
        name  = "RAW_BUCKET_NAME"
        value = google_storage_bucket.raw_events.name
      }

      dynamic "env" {
        for_each = each.value.secret_env_vars
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_project_service.required_apis,
    google_pubsub_topic.raw_events,
    google_storage_bucket.raw_events,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  for_each = {
    news = google_cloud_run_v2_service.collectors["news"].name
  }

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}