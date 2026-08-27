# Ingesta de eventos crudos en streaming
resource "google_pubsub_topic" "raw_events" {
  name                       = local.pubsub_topic_raw_events
  project                    = var.project_id
  labels                     = local.common_labels
  message_retention_duration = "86400s" # Retención de 24 horas

  depends_on = [google_project_service.required_apis]
}

# Dead Letter para eventos con fallos repetitivos
resource "google_pubsub_topic" "dead_letter" {
  name       = local.pubsub_topic_dead_letter
  project    = var.project_id
  labels     = local.common_labels

  depends_on = [google_project_service.required_apis]
}

# Suscripción de streaming para el procesador Dataflow
resource "google_pubsub_subscription" "dataflow" {
  name                       = local.pubsub_subscription_dataflow
  project                    = var.project_id
  topic                      = google_pubsub_topic.raw_events.id
  labels                     = local.common_labels

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = false

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  expiration_policy {
    ttl = ""
  }
}

# Suscripción para inspección/auditoría de errores en la DLQ
resource "google_pubsub_subscription" "dead_letter" {
  name                       = local.pubsub_subscription_deadletter
  project                    = var.project_id
  topic                      = google_pubsub_topic.dead_letter.id
  labels                     = local.common_labels

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = true

  expiration_policy {
    ttl = ""
  }
}

# Mapeo de Service Accounts de colectores para asignación masiva de permisos
locals {
  collector_service_accounts = {
    mastodon = google_service_account.mastodon_collector.email
    news     = google_service_account.news_collector.email
  }
}

# Permisos para que los colectores publiquen en raw_events
resource "google_pubsub_topic_iam_member" "collector_publishers" {
  for_each = local.collector_service_accounts

  project = var.project_id
  topic   = google_pubsub_topic.raw_events.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${each.value}"
}

# Permiso para que Dataflow consuma la suscripción principal
resource "google_pubsub_subscription_iam_member" "dataflow_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.dataflow.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Permisos IAM para el agente de servicio interno de Pub/Sub (Dead Letter Queue)
resource "google_pubsub_topic_iam_member" "pubsub_service_agent_dead_letter_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "pubsub_service_agent_dataflow_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.dataflow.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}