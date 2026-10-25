# Topics y suscripciones de Pub/Sub (Lote 1.3, ARCHITECTURE.md §5.3).
# Un topic por fuente (ta-raw-{source}) con su suscripción pull ta-raw-{source}-df,
# más los topics ta-dlq y ta-alerts. Retención de 7 días en topics y suscripciones.

variable "project_id" {
  type = string
}

variable "sources" {
  type    = list(string)
  default = ["reddit", "facebook", "instagram"]
}

variable "extra_topics" {
  description = "Topics sin suscripción propia en este lote (DLQ y alertas)."
  type        = list(string)
  default     = ["ta-dlq", "ta-alerts"]
}

variable "retention" {
  description = "Retención de mensajes (topics y suscripciones)."
  type        = string
  default     = "604800s" # 7 días
}

variable "ack_deadline_seconds" {
  type    = number
  default = 60
}

variable "labels" {
  type    = map(string)
  default = {}
}

resource "google_pubsub_topic" "raw" {
  for_each                   = toset(var.sources)
  project                    = var.project_id
  name                       = "ta-raw-${each.key}"
  message_retention_duration = var.retention
  labels                     = merge(var.labels, { component = "ingest", source = each.key })
}

resource "google_pubsub_topic" "extra" {
  for_each                   = toset(var.extra_topics)
  project                    = var.project_id
  name                       = each.key
  message_retention_duration = var.retention
  labels                     = merge(var.labels, { component = "ingest" })
}

resource "google_pubsub_subscription" "dataflow" {
  for_each                   = google_pubsub_topic.raw
  project                    = var.project_id
  name                       = "${each.value.name}-df"
  topic                      = each.value.id
  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.retention
  retain_acked_messages      = false
  labels                     = merge(var.labels, { component = "ingest", source = each.key })

  expiration_policy {
    ttl = "" # nunca expira por inactividad
  }
}

output "topic_names" {
  description = "Mapa source -> nombre del topic."
  value       = { for k, t in google_pubsub_topic.raw : k => t.name }
}

output "topic_ids" {
  value = merge(
    { for k, t in google_pubsub_topic.raw : k => t.id },
    { for k, t in google_pubsub_topic.extra : k => t.id },
  )
}

output "extra_topic_names" {
  value = { for k, t in google_pubsub_topic.extra : k => t.name }
}

output "subscription_names" {
  description = "Mapa source -> nombre de la suscripción de Dataflow."
  value       = { for k, s in google_pubsub_subscription.dataflow : k => s.name }
}

output "subscription_ids" {
  value = { for k, s in google_pubsub_subscription.dataflow : k => s.id }
}
