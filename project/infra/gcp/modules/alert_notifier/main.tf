# Notificador de alertas de tendencia (Lote 6.2, HU-039; ARCHITECTURE.md §3.28, §5.3, §5.8 y §5.12).
#
#   ta-alerts ─(push con ID token de sa-alerts)─► Cloud Run ta-alert-notifier ─► log WARNING alert=trend
#     ─► política basada en logs ta-alerta-tendencia ─► correo (alert_emails) y topic ta-alerts-incidents
#
# - El servicio escala a cero (0 a 1 instancias, CPU solo durante las peticiones) y no es público:
#   solo sa-alerts tiene run.invoker. Sin imagen (image = "") no se crean el servicio, su invoker ni
#   la suscripción push; la política, los canales y ta-alerts-incidents sí.
# - Los correos son un canal por dirección, con clave por posición ("0", "1"…): las direcciones no
#   aparecen en las direcciones de recurso ni en la evidencia (alert_emails es sensible).
# - El canal de Pub/Sub ta-alerts-incidents recibe una copia de cada notificación; el validador la
#   lee para medir cuándo salió el correo. Cloud Monitoring publica con su agente de servicio.

variable "project_id" {
  type = string
}

variable "project_number" {
  type = string
}

variable "region" {
  type = string
}

variable "service_name" {
  type    = string
  default = "ta-alert-notifier"
}

variable "image" {
  description = "Imagen alert-notifier:{etiqueta}. Vacía = sin servicio ni suscripción push."
  type        = string
  default     = ""
}

variable "service_account_email" {
  description = "Identidad del servicio y de la suscripción push (sa-alerts)."
  type        = string
}

variable "alerts_topic" {
  type    = string
  default = "ta-alerts"
}

variable "push_subscription" {
  type    = string
  default = "ta-alerts-notifier"
}

variable "incidents_topic" {
  type    = string
  default = "ta-alerts-incidents"
}

variable "policy_name" {
  type    = string
  default = "ta-alerta-tendencia"
}

variable "alert_emails" {
  type      = list(string)
  default   = []
  sensitive = true
}

variable "rate_limit_seconds" {
  description = "Como máximo una notificación de la política por periodo (las basadas en logs lo exigen)."
  type        = number
  default     = 300
}

variable "auto_close_seconds" {
  description = "Cierre automático de los incidentes (mínimo 1800 s)."
  type        = number
  default     = 1800
}

variable "labels" {
  type    = map(string)
  default = {}
}

locals {
  create_service = var.image != ""
  email_keys     = [for i in range(nonsensitive(length(var.alert_emails))) : tostring(i)]
  monitoring_sa  = "serviceAccount:service-${var.project_number}@gcp-sa-monitoring-notification.iam.gserviceaccount.com"
  log_filter     = <<-EOT
    resource.type="cloud_run_revision"
    resource.labels.service_name="${var.service_name}"
    severity="WARNING"
    jsonPayload.alert="trend"
  EOT
  # Etiqueta del incidente → campo de jsonPayload.aviso (textos que arma common.alert_message.notification_fields).
  extracted = ["marca", "fuente", "tipo", "ventana", "valor", "media", "cambio", "z", "origen", "alerta_id"]
}

# --- Cloud Run ----------------------------------------------------------------------------

resource "google_cloud_run_v2_service" "this" {
  count               = local.create_service ? 1 : 0
  project             = var.project_id
  location            = var.region
  name                = var.service_name
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false
  labels              = var.labels

  template {
    service_account                  = var.service_account_email
    max_instance_request_concurrency = 20
    timeout                          = "30s"
    labels                           = var.labels

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    containers {
      image = var.image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        cpu_idle = true # CPU solo mientras atiende una petición
      }

      startup_probe {
        http_get {
          path = "/healthz"
          port = 8080
        }
        period_seconds    = 3
        timeout_seconds   = 2
        failure_threshold = 10
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  # La API devuelve un bloque scaling a nivel de servicio (ceros) que no se usa: sin esto, cada plan
  # propone quitarlo (visto en la validación, C4). El escalado real es el de template.scaling.
  lifecycle {
    ignore_changes = [scaling]
  }
}

resource "google_cloud_run_v2_service_iam_member" "invoker" {
  count    = local.create_service ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.this[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account_email}"
}

# --- Pub/Sub ---------------------------------------------------------------------------------

resource "google_pubsub_subscription" "push" {
  count   = local.create_service ? 1 : 0
  project = var.project_id
  name    = var.push_subscription
  topic   = "projects/${var.project_id}/topics/${var.alerts_topic}"
  labels  = var.labels

  ack_deadline_seconds       = 30
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  push_config {
    push_endpoint = google_cloud_run_v2_service.this[0].uri
    oidc_token {
      service_account_email = var.service_account_email
      audience              = google_cloud_run_v2_service.this[0].uri
    }
  }

  depends_on = [google_cloud_run_v2_service_iam_member.invoker]
}

resource "google_pubsub_topic" "incidents" {
  project                    = var.project_id
  name                       = var.incidents_topic
  labels                     = var.labels
  message_retention_duration = "604800s"
}

# --- Cloud Monitoring ---------------------------------------------------------------------------

resource "google_monitoring_notification_channel" "email" {
  for_each     = toset(local.email_keys)
  project      = var.project_id
  display_name = "${var.policy_name}-correo-${each.key}"
  type         = "email"
  user_labels  = var.labels
  labels = {
    email_address = var.alert_emails[tonumber(each.key)]
  }
}

resource "google_monitoring_notification_channel" "incidents" {
  project      = var.project_id
  display_name = "${var.policy_name}-pubsub"
  type         = "pubsub"
  user_labels  = var.labels
  labels = {
    topic = google_pubsub_topic.incidents.id
  }
}

# El agente de notificaciones de Cloud Monitoring no se crea solo al crear el canal por API: lo crea
# validate_6_2.py terraform (C0c) con `gcloud beta services identity create --service=monitoring.googleapis.com`
# antes del apply (con el proveedor google, sin google-beta, no hay recurso para eso).
resource "google_pubsub_topic_iam_member" "incidents_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.incidents.name
  role    = "roles/pubsub.publisher"
  member  = local.monitoring_sa

  depends_on = [google_monitoring_notification_channel.incidents]
}

resource "google_monitoring_alert_policy" "trend" {
  project      = var.project_id
  display_name = var.policy_name
  combiner     = "OR"
  severity     = "WARNING"
  user_labels  = var.labels
  notification_channels = concat(
    [for k in local.email_keys : google_monitoring_notification_channel.email[k].id],
    [google_monitoring_notification_channel.incidents.id],
  )

  conditions {
    display_name = "Log del notificador con alert=trend"
    condition_matched_log {
      filter           = local.log_filter
      label_extractors = { for k in local.extracted : k => "EXTRACT(jsonPayload.aviso.${k})" }
    }
  }

  alert_strategy {
    auto_close = "${var.auto_close_seconds}s"
    notification_rate_limit {
      period = "${var.rate_limit_seconds}s"
    }
  }

  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: $${log.extracted_label.tipo} de $${log.extracted_label.marca} en $${log.extracted_label.fuente}"
    content   = <<-EOT
      **Alerta de tendencia de TrendAnalyzer** (origen: $${log.extracted_label.origen})

      - Marca: $${log.extracted_label.marca}
      - Fuente: $${log.extracted_label.fuente}
      - Tipo: $${log.extracted_label.tipo}
      - Ventana: $${log.extracted_label.ventana}
      - Valor: $${log.extracted_label.valor} (media del baseline: $${log.extracted_label.media})
      - Cambio: $${log.extracted_label.cambio} · z = $${log.extracted_label.z}
      - Alerta: $${log.extracted_label.alerta_id}

      Detalle completo en Cloud Logging (servicio ${var.service_name}, jsonPayload) y en el topic ${var.alerts_topic}.
      Cooldown: 15 min por marca × fuente × tipo. Como máximo un correo cada ${var.rate_limit_seconds / 60} min.
      Guía: project/src/alerts/ALERTS.md (proyecto ${var.project_id}).
    EOT
  }

  depends_on = [google_pubsub_topic_iam_member.incidents_publisher]
}

# --- outputs -------------------------------------------------------------------------------------

output "service_uri" {
  value = local.create_service ? google_cloud_run_v2_service.this[0].uri : null
}

output "service_name" {
  value = var.service_name
}

output "push_subscription" {
  value = local.create_service ? google_pubsub_subscription.push[0].name : null
}

output "incidents_topic" {
  value = google_pubsub_topic.incidents.name
}

output "policy_name" {
  value = google_monitoring_alert_policy.trend.display_name
}

output "policy_id" {
  value = google_monitoring_alert_policy.trend.name
}

output "email_channels" {
  value = length(google_monitoring_notification_channel.email)
}
