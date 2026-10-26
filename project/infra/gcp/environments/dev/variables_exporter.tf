# Variables del Lote 3.2 (exportador gcs-to-s3). Todas tienen valor por defecto; los valores
# de AWS y la imagen los escribe validate_3_2.py en aws_exporter.auto.tfvars (no se versiona).

variable "exporter_image" {
  description = "Imagen del exportador (…/ta-images/gcs-to-s3:{etiqueta}). Vacía = no se crean el job ni el Scheduler."
  type        = string
  default     = ""
}

variable "exporter_role_arn" {
  description = "ARN de ta-gcp-exporter-role (output exporter_role_arn de infra/aws)."
  type        = string
  default     = ""
}

variable "exporter_s3_bucket" {
  description = "Bucket del lake en S3 (output lake_bucket de infra/aws)."
  type        = string
  default     = ""
}

variable "exporter_kms_key_arn" {
  description = "ARN de la llave del lake (output kms_key_arn de infra/aws)."
  type        = string
  default     = ""
}

variable "exporter_audience" {
  description = "Audiencia del ID token (output exporter_oidc_audience de infra/aws)."
  type        = string
  default     = "ta-gcp-exporter-role"
}

variable "exporter_aws_region" {
  description = "Región de AWS del lake."
  type        = string
  default     = "us-east-1"
}

variable "exporter_oidc_token_creators" {
  description = "Miembros (user:correo) que pueden emitir ID tokens de sa-exporter para validar desde su máquina."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for m in var.exporter_oidc_token_creators : can(regex("^(user|group|serviceAccount):", m))])
    error_message = "Cada miembro debe empezar por user:, group: o serviceAccount:."
  }
}

variable "exporter_schedule" {
  description = "Cron del Scheduler (UTC). El Scheduler se crea en pausa."
  type        = string
  default     = "*/15 * * * *"
}

variable "exporter_cpu" {
  type    = string
  default = "1"
}

variable "exporter_memory" {
  type    = string
  default = "512Mi"
}

variable "exporter_task_timeout_seconds" {
  description = "Tiempo máximo de una corrida (menor que los 15 min del Scheduler)."
  type        = number
  default     = 840
}

variable "exporter_workers" {
  description = "Subidas en paralelo por corrida (TA_EXPORT_WORKERS, 1 a 10)."
  type        = number
  default     = 8

  validation {
    condition     = var.exporter_workers >= 1 && var.exporter_workers <= 10
    error_message = "exporter_workers debe estar entre 1 y 10 (tamaño de los pools HTTP del exportador)."
  }
}

variable "exporter_max_objects" {
  description = "Máximo de objetos por corrida (TA_EXPORT_MAX_OBJECTS); el resto sale en la siguiente."
  type        = number
  default     = 5000
}
