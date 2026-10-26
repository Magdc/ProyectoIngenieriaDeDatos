# Rol de AWS que asume sa-exporter (GCP) con su ID token de Google (Lote 3.2, HU-006).
#
# Federación: AWS trae a Google como proveedor OIDC integrado ("accounts.google.com"), así que
# no se crea un aws_iam_openid_connect_provider. La confianza fija al emisor y a la cuenta:
#   accounts.google.com:aud  = claim azp del token (uniqueId o correo de la SA; si el token no
#                              trae azp, AWS usa su aud, por eso también se acepta la audiencia)
#   accounts.google.com:oaud = claim aud (la audiencia que pide el exportador)
#   accounts.google.com:sub  = uniqueId numérico de sa-exporter (cambia si se recrea la SA)
#
# Permisos (ARCHITECTURE.md §5.8): solo escribir y listar bajo raw/origin=stream/ y cifrar con
# la llave del lake a través de S3. Sin lectura ni borrado. IAM no tiene una acción
# s3:HeadObject (un HEAD exige s3:GetObject): la idempotencia se hace listando la partición
# (s3:ListBucket con s3:prefix), como ta-historical-loader.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "role_name" {
  description = "Nombre del rol (§5.8)."
  type        = string
  default     = "ta-gcp-exporter-role"
}

variable "bucket_name" {
  description = "Bucket del lake (ta-{env}-{cuenta}-lake)."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN de la llave del lake (alias/ta-{env}-lake)."
  type        = string
}

variable "aws_region" {
  description = "Región del bucket y de la llave (kms:ViaService = s3.{región}.amazonaws.com)."
  type        = string
}

variable "object_prefix" {
  description = "Único prefijo donde puede escribir el exportador."
  type        = string
  default     = "raw/origin=stream/"

  validation {
    condition     = endswith(var.object_prefix, "/") && !startswith(var.object_prefix, "/")
    error_message = "object_prefix debe terminar en / y no empezar con /."
  }
}

variable "gcp_sa_unique_id" {
  description = "uniqueId numérico de sa-exporter (claim sub)."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{10,30}$", var.gcp_sa_unique_id))
    error_message = "gcp_sa_unique_id debe ser el uniqueId numérico de sa-exporter."
  }
}

variable "gcp_sa_email" {
  description = "Correo de sa-exporter (algunos tokens lo traen en azp)."
  type        = string

  validation {
    condition     = endswith(var.gcp_sa_email, ".iam.gserviceaccount.com")
    error_message = "gcp_sa_email debe ser el correo de una service account."
  }
}

variable "audience" {
  description = "Audiencia del ID token (claim aud)."
  type        = string
}

variable "max_session_seconds" {
  description = "Duración máxima de la sesión asumida."
  type        = number
  default     = 3600
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "SaExporterWebIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["accounts.google.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "accounts.google.com:aud"
      values   = [var.gcp_sa_unique_id, var.gcp_sa_email, var.audience]
    }

    condition {
      test     = "StringEquals"
      variable = "accounts.google.com:oaud"
      values   = [var.audience]
    }

    condition {
      test     = "StringEquals"
      variable = "accounts.google.com:sub"
      values   = [var.gcp_sa_unique_id]
    }
  }
}

data "aws_iam_policy_document" "s3" {
  statement {
    sid       = "PutStreamObjects"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["arn:aws:s3:::${var.bucket_name}/${var.object_prefix}*"]
  }

  statement {
    sid       = "ListStreamPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.object_prefix}*"]
    }
  }

  statement {
    sid       = "EncryptWithLakeKey"
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey"]
    resources = [var.kms_key_arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.aws_region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  description          = "Lote 3.2: sa-exporter (GCP) escribe enriched/ en ${var.object_prefix} con su ID token."
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = var.max_session_seconds
}

resource "aws_iam_role_policy" "s3" {
  name   = "${var.role_name}-s3"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.s3.json
}

output "role_arn" {
  description = "ARN del rol (TA_EXPORTER_ROLE_ARN del job)."
  value       = aws_iam_role.this.arn
}

output "role_name" {
  description = "Nombre del rol."
  value       = aws_iam_role.this.name
}

output "audience" {
  description = "Audiencia que debe pedir el exportador."
  value       = var.audience
  depends_on  = [aws_iam_role_policy.s3]
}
