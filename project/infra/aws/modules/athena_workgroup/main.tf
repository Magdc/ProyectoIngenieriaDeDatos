# Workgroup de Athena ta-analytics (Lote 4.1, HU-007 y HU-030; ARCHITECTURE.md §3.19 y §5.7).
#
# Resultados en s3://{lake}/athena-results/ cifrados con SSE-KMS y la llave del lake (el bucket
# los borra a los 7 días, §5.12), límite de bytes escaneados por consulta y configuración forzada
# (un cliente no puede cambiar la ubicación, el cifrado ni el límite). Consultas guardadas de
# ejemplo (named queries) desde project/src/lakehouse/queries/.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "name" {
  description = "Nombre del workgroup (§5.7)."
  type        = string
  default     = "ta-analytics"
}

variable "description" {
  description = "Descripción del workgroup."
  type        = string
  default     = "TrendAnalyzer: consultas sobre el lake (ta_raw, ta_trusted, ta_curated)"
}

variable "bucket_name" {
  description = "Bucket del lake."
  type        = string
}

variable "results_prefix" {
  description = "Prefijo de los resultados (termina en /)."
  type        = string
  default     = "athena-results/"

  validation {
    condition     = endswith(var.results_prefix, "/") && !startswith(var.results_prefix, "/")
    error_message = "results_prefix debe terminar en / y no empezar con /."
  }
}

variable "kms_key_arn" {
  description = "Llave del lake para cifrar los resultados (SSE_KMS)."
  type        = string
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Límite de bytes escaneados por consulta (mínimo de Athena: 10 MB)."
  type        = number

  validation {
    condition     = var.bytes_scanned_cutoff_per_query >= 10485760
    error_message = "Athena exige un límite de al menos 10485760 bytes (10 MB)."
  }
}

variable "engine_version" {
  description = "Motor de Athena."
  type        = string
  default     = "Athena engine version 3"
}

variable "named_queries" {
  description = "Consultas guardadas: nombre → {database, description, query}."
  type = map(object({
    database    = string
    description = string
    query       = string
  }))
  default = {}
}

resource "aws_athena_workgroup" "this" {
  name          = var.name
  description   = var.description
  state         = "ENABLED"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    requester_pays_enabled             = false
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff_per_query

    engine_version {
      selected_engine_version = var.engine_version
    }

    result_configuration {
      output_location = "s3://${var.bucket_name}/${var.results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = var.kms_key_arn
      }
    }
  }
}

resource "aws_athena_named_query" "this" {
  for_each = var.named_queries

  name        = each.key
  workgroup   = aws_athena_workgroup.this.id
  database    = each.value.database
  description = each.value.description
  query       = each.value.query
}

output "name" {
  description = "Nombre del workgroup."
  value       = aws_athena_workgroup.this.name
}

output "arn" {
  description = "ARN del workgroup (lo usan las políticas de IAM)."
  value       = aws_athena_workgroup.this.arn
}

output "output_location" {
  description = "Dónde quedan los resultados."
  value       = aws_athena_workgroup.this.configuration[0].result_configuration[0].output_location
}

output "bytes_scanned_cutoff_per_query" {
  description = "Límite de bytes por consulta."
  value       = aws_athena_workgroup.this.configuration[0].bytes_scanned_cutoff_per_query
}

output "named_query_ids" {
  description = "Nombre → id de las consultas guardadas."
  value       = { for k, q in aws_athena_named_query.this : k => q.id }
}
