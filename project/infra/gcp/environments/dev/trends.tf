# Lote 2.4 (HU-005, HU-023) · ARCHITECTURE.md §3.16, §5.3, §5.5 y §5.8.
#
# Se suma a platform.tf (Lote 1.3), streaming.tf (Lote 2.2) y nlp.tf (Lote 2.3) sin
# modificarlos:
#   * tablas trend_windows, dlq_events y dq_results + vista trend_windows_latest;
#   * suscripción ta-dlq-df (la lee Dataflow para llenar dlq_events);
#   * sa-dataflow: roles/pubsub.viewer en las suscripciones que lee (quita el aviso
#     GETTING_PUBSUB_SUBSCRIPTION_FAILED de los workers) y subscriber en ta-dlq-df.
#
# Los depends_on apuntan a los módulos de platform.tf (pubsub) y streaming.tf
# (bigquery_stream). Si validate.sh dice "Reference to undeclared module", cambie el
# nombre por el que aparece en esos archivos (ver docs/validation/validation-2-4.md, C).

locals {
  trends_labels          = merge(local.platform_labels, { component = "trends" })
  trends_dataflow_member = "serviceAccount:sa-dataflow@${var.project_id}.iam.gserviceaccount.com"
  trends_read_subscriptions = {
    reddit    = "ta-raw-reddit-df"
    facebook  = "ta-raw-facebook-df"
    instagram = "ta-raw-instagram-df"
    dlq       = "ta-dlq-df"
  }
}

module "trends_bq_tables" {
  source = "../../modules/bigquery_stream_tables"

  project_id                    = var.project_id
  dataset_id                    = var.trends_dataset_id
  labels                        = local.trends_labels
  deletion_protection           = var.trends_tables_deletion_protection
  trend_windows_expiration_days = var.trend_windows_expiration_days
  dlq_events_expiration_days    = var.dlq_events_expiration_days
  dq_results_expiration_days    = var.dq_results_expiration_days

  depends_on = [module.bigquery_stream]
}

resource "google_pubsub_subscription" "trends_dlq_df" {
  project = var.project_id
  name    = "ta-dlq-df"
  topic   = "projects/${var.project_id}/topics/ta-dlq"

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = merge(local.platform_labels, { component = "streaming" })

  expiration_policy {
    ttl = ""
  }

  depends_on = [module.pubsub]
}

resource "google_pubsub_subscription_iam_member" "trends_dataflow_viewer" {
  for_each = local.trends_read_subscriptions

  project      = var.project_id
  subscription = each.value
  role         = "roles/pubsub.viewer"
  member       = local.trends_dataflow_member

  depends_on = [module.pubsub, google_pubsub_subscription.trends_dlq_df]
}

resource "google_pubsub_subscription_iam_member" "trends_dataflow_dlq_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.trends_dlq_df.name
  role         = "roles/pubsub.subscriber"
  member       = local.trends_dataflow_member
}
