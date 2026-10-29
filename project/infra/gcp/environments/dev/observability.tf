# Lote 7.2 (HU-042, §3.32, ADR-36): tablero «TrendAnalyzer · operación» y alertas de operación (Dataflow fallido,
# latencia, DLQ creciente, exportador fallido) con los canales de las alertas del 6.2.

module "observability" {
  source                    = "../../modules/observability"
  project_id                = var.project_id
  notification_channels     = module.alerts.notification_channel_ids
  latency_threshold_seconds = var.ops_latency_threshold_seconds
  dlq_threshold             = var.ops_dlq_threshold
  labels                    = merge(local.platform_labels, { component = "observability" })
}

variable "ops_latency_threshold_seconds" {
  description = "Edad máxima de la marca de agua de Dataflow antes de avisar (§5.12)."
  type        = number
  default     = 300
}

variable "ops_dlq_threshold" {
  description = "Mensajes en ta-dlq en 10 min antes de avisar."
  type        = number
  default     = 30
}

output "ops_dashboard_id" {
  description = "Tablero de Cloud Monitoring del Lote 7.2."
  value       = module.observability.dashboard_id
}

output "ops_alert_policies" {
  description = "Alertas de operación del Lote 7.2."
  value       = module.observability.policy_names
}
