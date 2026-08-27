variable "project_id" {
  description = "ID del proyecto de GCP donde se desplegara la infraestructura"
  type        = string
}
variable "region" {
  description = "Region principal para los recursos regionales de GCP"
  type        = string
  default     = "us-central1"
}
variable "environment" {
  description = "Ambiente de despligue."
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "El ambiente debe ser dev, qa o prod."
  }
}
variable "name_prefix" {
  description = "Prefijo base para nombrar los recursos del proyecto"
  type        = string
  default     = "trend"
}

variable "collector_container_image" {
  description = "Imagen de contenedor temporal para los collectors de Cloud Run."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

variable "collector_cpu" {
  description = "CPU asignada a cada collector de Cloud Run."
  type        = string
  default     = "1"
}

variable "collector_memory" {
  description = "Memoria asignada a cada collector de Cloud Run."
  type        = string
  default     = "512Mi"
}

variable "collector_max_instances" {
  description = "Maximo de instancias por collector de Cloud Run."
  type        = number
  default     = 3
}

variable "scheduler_time_zone" {
  description = "Zona horaria usada por Cloud Scheduler."
  type        = string
  default     = "America/Bogota"
}

variable "reddit_ingestion_schedule" {
  description = "Frecuencia cron para consultar Reddit."
  type        = string
  default     = "*/15 * * * *"
}

variable "news_ingestion_schedule" {
  description = "Frecuencia cron para consultar fuentes RSS o News API."
  type        = string
  default     = "*/30 * * * *"
}
