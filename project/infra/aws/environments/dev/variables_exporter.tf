# Variables del Lote 3.2 (federación del exportador). Todas tienen valor por defecto: con
# gcp_exporter_sa_unique_id vacío no se crea el rol y los planes de otros lotes no cambian.

variable "gcp_exporter_sa_unique_id" {
  description = "uniqueId numérico de sa-exporter (gcloud iam service-accounts describe sa-exporter@… --format='value(uniqueId)'). Vacío = no se crea ta-gcp-exporter-role."
  type        = string
  default     = ""

  validation {
    condition     = var.gcp_exporter_sa_unique_id == "" || can(regex("^[0-9]{10,30}$", var.gcp_exporter_sa_unique_id))
    error_message = "gcp_exporter_sa_unique_id debe estar vacío o ser el uniqueId numérico de sa-exporter."
  }
}

variable "gcp_exporter_sa_email" {
  description = "Correo de sa-exporter (sa-exporter@{proyecto}.iam.gserviceaccount.com)."
  type        = string
  default     = ""
}

variable "gcp_exporter_audience" {
  description = "Audiencia del ID token que pide el exportador (TA_EXPORTER_AUDIENCE)."
  type        = string
  default     = "ta-gcp-exporter-role"
}
