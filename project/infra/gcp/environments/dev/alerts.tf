# Lote 6.2 (HU-039) · ARCHITECTURE.md §3.28, §5.3, §5.8 y §5.12.
#
# Se suma a platform.tf (Lote 1.3) y trends.tf (Lote 2.4) sin modificarlos:
#   * sa-alerts: identidad del notificador y de la suscripción push (run.invoker solo sobre su servicio);
#   * sa-dataflow: pubsub.publisher en ta-alerts (la rama de alertas del job publica ahí);
#   * módulo alert_notifier: Cloud Run ta-alert-notifier, suscripción push ta-alerts-notifier,
#     topic ta-alerts-incidents, canales de notificación y la política ta-alerta-tendencia.
#
# La imagen llega por alerts.auto.tfvars (lo escribe validate_6_2.py; no se versiona) y los correos por
# alert_emails en terraform.tfvars (no se versiona). Sin imagen no se crean el servicio ni la push.

data "google_project" "alerts" {
  project_id = var.project_id
}

module "sa_alerts" {
  source       = "../../modules/service_account"
  project_id   = var.project_id
  account_id   = "sa-alerts"
  display_name = "TrendAnalyzer · alertas"
  description  = "Notificador de alertas de tendencia (Lote 6.2): recibe ta-alerts por push y escribe el log del correo."
}

resource "google_pubsub_topic_iam_member" "dataflow_alerts_publisher" {
  project = var.project_id
  topic   = module.pubsub.extra_topic_names["ta-alerts"]
  role    = "roles/pubsub.publisher"
  member  = module.sa_dataflow.member
}

module "alerts" {
  source = "../../modules/alert_notifier"

  project_id            = var.project_id
  project_number        = data.google_project.alerts.number
  region                = var.region
  image                 = var.alerts_notifier_image
  service_account_email = module.sa_alerts.email
  alerts_topic          = module.pubsub.extra_topic_names["ta-alerts"]
  alert_emails          = var.alert_emails
  rate_limit_seconds    = var.alert_rate_limit_seconds
  auto_close_seconds    = var.alert_auto_close_seconds
  labels                = merge(local.platform_labels, { component = "alerts" })
}
