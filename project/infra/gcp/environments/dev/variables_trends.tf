# Variables del Lote 2.4 (trends.tf). Todas tienen valor por defecto.

variable "trends_dataset_id" {
  description = "Dataset de BigQuery donde van trend_windows, dlq_events y dq_results (§5.5)."
  type        = string
  default     = "ta_stream"
}

variable "trends_tables_deletion_protection" {
  description = "Protección contra borrado de trend_windows, dlq_events y dq_results."
  type        = bool
  default     = false
}

variable "trend_windows_expiration_days" {
  description = "Expiración de las particiones de trend_windows, en días (§5.12)."
  type        = number
  default     = 90
}

variable "dlq_events_expiration_days" {
  description = "Expiración de las particiones de dlq_events, en días (§5.12)."
  type        = number
  default     = 30
}

variable "dq_results_expiration_days" {
  description = "Expiración de las particiones de dq_results, en días; null = no expiran (§5.12)."
  type        = number
  default     = null
}
