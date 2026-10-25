# Variables del Lote 2.2. Todas tienen valor por defecto: terraform.tfvars no cambia.

variable "bq_dataset_id" {
  description = "Dataset de BigQuery del streaming (§5.5)."
  type        = string
  default     = "ta_stream"
}

variable "posts_enriched_partition_expiration_days" {
  description = "Expiración de las particiones de posts_enriched (§5.12)."
  type        = number
  default     = 90
}

variable "bq_deletion_protection" {
  description = "Protección contra borrado de posts_enriched."
  type        = bool
  default     = true
}

variable "hmac_secret_id" {
  description = "Secreto con la llave HMAC (TA_HMAC_SECRET_NAME, §5.1)."
  type        = string
  default     = "ta-hmac-key"
}
