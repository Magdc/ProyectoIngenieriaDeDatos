# Lote 7.3 (HU-043, §3.31, ADR-35): alertas de gasto por olvido (Dataflow y simulador encendidos de más),
# con los canales de las alertas del 6.2. Los presupuestos siguen en el bootstrap (Lote 1.0, §5.14).

module "cost_guard" {
  source                = "../../modules/cost_guard"
  project_id            = var.project_id
  notification_channels = module.alerts.notification_channel_ids
  dataflow_max_hours    = var.cost_dataflow_max_hours
  simulator_max_hours   = var.cost_simulator_max_hours
  labels                = merge(local.platform_labels, { component = "costs" })
}

variable "cost_dataflow_max_hours" {
  description = "Horas de vida de un job ta-stream-* antes de avisar (una validación con baseline dura ~2 h)."
  type        = number
  default     = 4
}

variable "cost_simulator_max_hours" {
  description = "Horas seguidas con el simulador encendido antes de avisar (máximo efectivo 23,5 h)."
  type        = number
  default     = 8
}

output "cost_guard_policies" {
  description = "Nombres de las políticas de gasto (Lote 7.3)."
  value       = module.cost_guard.policy_names
}
