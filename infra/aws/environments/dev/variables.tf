variable "aws_region" {
  description = "Region principal para la infraestructura batch en AWS."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de despliegue."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "El ambiente debe ser dev, qa o prod."
  }
}

variable "name_prefix" {
  description = "Prefijo base para nombrar los recursos de AWS."
  type        = string
  default     = "trend"
}

variable "force_destroy_buckets" {
  description = "Permite destruir buckets aunque contengan objetos. Mantener false para proteger datos."
  type        = bool
  default     = false
}
