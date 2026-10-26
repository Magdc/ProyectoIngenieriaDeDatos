# Variables del Lote 3.1 (lake.tf). Todas con valor por defecto: terraform.tfvars no cambia.

variable "lake_kms_deletion_window_days" {
  description = "Días de espera antes de borrar la llave del lake si se destruye (7 a 30)."
  type        = number
  default     = 30
}

variable "athena_results_expiration_days" {
  description = "Días de vida de athena-results/ en el lake (§5.12)."
  type        = number
  default     = 7
}

variable "historical_loader_principals" {
  description = "ARN que pueden asumir ta-historical-loader; vacío = toda la cuenta."
  type        = list(string)
  default     = []
}
