# Rol ta-glue-role de los jobs de Glue (Lote 4.1, HU-007; ARCHITECTURE.md §5.8).
#
# Mínimo privilegio para Raw → Trusted → Curated:
#   - lee raw/ (sin escribir ni borrar: Raw es de ta-historical-loader y ta-gcp-exporter-role);
#   - lee, escribe y borra en trusted/, curated/ y quality/ (sobrescribir particiones dt, §5.12);
#   - lee los scripts de _glue/scripts/ y usa _glue/temp/ como directorio temporal;
#   - cifra y descifra con la llave del lake solo a través de S3;
#   - lee el catálogo de las tres bases (sin crear ni cambiar tablas: las crea Terraform);
#   - lee y escribe las propiedades de corrida de su workflow;
#   - escribe sus logs en /aws-glue/* y métricas en el espacio de nombres Glue.

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
  default     = "ta-glue-role"
}

variable "bucket_name" {
  description = "Bucket del lake."
  type        = string
}

variable "kms_key_arn" {
  description = "Llave del lake."
  type        = string
}

variable "aws_region" {
  description = "Región (kms:ViaService y ARN de Glue y CloudWatch Logs)."
  type        = string
}

variable "aws_account_id" {
  description = "Cuenta (ARN de Glue y CloudWatch Logs)."
  type        = string
}

variable "read_prefixes" {
  description = "Prefijos que puede leer y listar (terminan en /)."
  type        = list(string)
  default     = ["raw/", "trusted/", "curated/", "quality/", "_glue/"]
}

variable "write_prefixes" {
  description = "Prefijos donde puede escribir y borrar (terminan en /)."
  type        = list(string)
  default     = ["trusted/", "curated/", "quality/", "_glue/temp/"]

  validation {
    condition     = alltrue([for p in var.write_prefixes : endswith(p, "/") && !startswith(p, "raw/")])
    error_message = "write_prefixes deben terminar en / y no pueden incluir raw/ (Raw es de los productores)."
  }
}

variable "database_names" {
  description = "Bases de Glue que puede leer."
  type        = list(string)
}

variable "workflow_name" {
  description = "Workflow cuyas propiedades de corrida puede leer y escribir."
  type        = string
}

locals {
  bucket_arn   = "arn:aws:s3:::${var.bucket_name}"
  glue_arn     = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}"
  log_arn_base = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/*"
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "GlueService"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  description          = "Lote 4.1: jobs de Glue del lake (lee raw/, escribe trusted/, curated/ y quality/)"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid       = "BucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [local.bucket_arn]
  }

  statement {
    sid       = "ListLakePrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [for p in var.read_prefixes : "${p}*"]
    }
  }

  statement {
    sid       = "ReadLakePrefixes"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [for p in var.read_prefixes : "${local.bucket_arn}/${p}*"]
  }

  statement {
    sid    = "WriteLakePrefixes"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [for p in var.write_prefixes : "${local.bucket_arn}/${p}*"]
  }

  statement {
    sid       = "LakeKeyViaS3"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.aws_region}.amazonaws.com"]
    }
  }

  statement {
    sid    = "ReadCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = concat(
      ["${local.glue_arn}:catalog"],
      [for db in var.database_names : "${local.glue_arn}:database/${db}"],
      [for db in var.database_names : "${local.glue_arn}:table/${db}/*"],
    )
  }

  statement {
    sid       = "WorkflowRunProperties"
    effect    = "Allow"
    actions   = ["glue:GetWorkflowRunProperties", "glue:PutWorkflowRunProperties"]
    resources = ["${local.glue_arn}:workflow/${var.workflow_name}"]
  }

  statement {
    sid       = "GlueLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn_base, "${local.log_arn_base}:*"]
  }

  statement {
    sid       = "GlueMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.role_name}-lake"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

output "role_arn" {
  description = "ARN de ta-glue-role."
  value       = aws_iam_role.this.arn
  depends_on  = [aws_iam_role_policy.this]
}

output "role_name" {
  description = "Nombre del rol."
  value       = aws_iam_role.this.name
}

output "policy_name" {
  description = "Política en línea del rol."
  value       = aws_iam_role_policy.this.name
}
