# Rol ta-historical-loader (§5.8; Lote 3.1, HU-024).
# Lo asumen los desarrolladores de la cuenta con su perfil de AWS CLI para subir el
# histórico: solo escribe y lista bajo raw/origin=historical/ y solo usa la llave del
# lake para cifrar. No puede leer, borrar ni escribir en otros prefijos.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "role_name" {
  description = "Nombre del rol."
  type        = string
  default     = "ta-historical-loader"
}

variable "aws_account_id" {
  description = "Cuenta que puede asumir el rol (sus principales con sts:AssumeRole)."
  type        = string
}

variable "trusted_principal_arns" {
  description = "Principales que pueden asumir el rol; vacío = toda la cuenta (root)."
  type        = list(string)
  default     = []
}

variable "bucket_arn" {
  description = "ARN del bucket del lake."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN de la llave KMS del lake."
  type        = string
}

variable "prefix" {
  description = "Prefijo de escritura (§5.7)."
  type        = string
  default     = "raw/origin=historical/"
}

locals {
  principals = length(var.trusted_principal_arns) > 0 ? var.trusted_principal_arns : ["arn:aws:iam::${var.aws_account_id}:root"]
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AssumeFromAccount"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = local.principals
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  description          = "Carga historica (Lote 3.1): escribe solo en ${var.prefix} del lake"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid       = "PutHistorical"
    actions   = ["s3:PutObject"]
    resources = ["${var.bucket_arn}/${var.prefix}*"]
  }

  statement {
    sid       = "ListHistorical"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.prefix}*"]
    }
  }

  statement {
    sid       = "EncryptWithLakeKey"
    actions   = ["kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.role_name}-s3"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

output "role_arn" {
  value = aws_iam_role.this.arn
}

output "role_name" {
  value = aws_iam_role.this.name
}
