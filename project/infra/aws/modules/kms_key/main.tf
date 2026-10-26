# Llave KMS del lake (Lote 3.1, adelantada del Lote 4.1; ADR-21).
# Política por defecto de la cuenta: los permisos se dan con IAM (rol del loader, y en la
# Fase 4 Glue, Athena y QuickSight). Rotación anual automática.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "alias" {
  description = "Alias sin el prefijo alias/ (p. ej. ta-dev-lake)."
  type        = string
}

variable "description" {
  description = "Descripción de la llave."
  type        = string
}

variable "deletion_window_in_days" {
  description = "Días de espera antes de borrar la llave si se destruye (7 a 30)."
  type        = number
  default     = 30
}

resource "aws_kms_key" "this" {
  description             = var.description
  enable_key_rotation     = true
  deletion_window_in_days = var.deletion_window_in_days
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.alias}"
  target_key_id = aws_kms_key.this.key_id
}

output "key_arn" {
  value = aws_kms_key.this.arn
}

output "key_id" {
  value = aws_kms_key.this.key_id
}

output "alias_name" {
  value = aws_kms_alias.this.name
}
