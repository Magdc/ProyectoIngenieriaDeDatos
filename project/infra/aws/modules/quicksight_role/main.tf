# Rol ta-quicksight-role (Lote 4.1, HU-007; ARCHITECTURE.md §5.8). Lo usará QuickSight (Lote 6.1)
# como rol de servicio: consulta Athena solo en el workgroup ta-analytics y solo lee curated/.
#
#   - Athena: lanzar, seguir y leer consultas en ta-analytics (sin cambiar ni borrar el workgroup);
#     listar workgroups y catálogos (lo pide la consola de QuickSight; no da acceso a datos);
#   - Glue: leer el catálogo de ta_curated (Athena lo consulta con la identidad de quien pregunta);
#   - S3: leer curated/ y escribir/leer los resultados en athena-results/;
#   - KMS: la llave del lake solo a través de S3 (datos y resultados cifrados con ella).

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
  default     = "ta-quicksight-role"
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
  description = "Región."
  type        = string
}

variable "aws_account_id" {
  description = "Cuenta."
  type        = string
}

variable "workgroup_name" {
  description = "Único workgroup de Athena que puede usar."
  type        = string
}

variable "results_prefix" {
  description = "Prefijo de los resultados de Athena (termina en /)."
  type        = string
  default     = "athena-results/"
}

variable "database_names" {
  description = "Bases de Glue que puede leer."
  type        = list(string)
}

variable "read_prefixes" {
  description = "Prefijos de datos que puede leer (terminan en /)."
  type        = list(string)
  default     = ["curated/"]
}

locals {
  bucket_arn = "arn:aws:s3:::${var.bucket_name}"
  glue_arn   = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}"
  athena_arn = "arn:aws:athena:${var.aws_region}:${var.aws_account_id}"
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "QuickSightService"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["quicksight.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  description          = "Lote 4.1: QuickSight consulta ta_curated por Athena (workgroup ${var.workgroup_name})"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid    = "QueryWorkgroup"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:BatchGetQueryExecution",
      "athena:ListQueryExecutions",
      "athena:GetWorkGroup",
    ]
    resources = ["${local.athena_arn}:workgroup/${var.workgroup_name}"]
  }

  statement {
    sid    = "ReadDataCatalog"
    effect = "Allow"
    actions = [
      "athena:GetDataCatalog",
      "athena:ListDatabases",
      "athena:GetDatabase",
      "athena:ListTableMetadata",
      "athena:GetTableMetadata",
    ]
    resources = ["${local.athena_arn}:datacatalog/AwsDataCatalog"]
  }

  statement {
    sid       = "ListAthenaResources"
    effect    = "Allow"
    actions   = ["athena:ListWorkGroups", "athena:ListDataCatalogs"]
    resources = ["*"]
  }

  statement {
    sid    = "ReadCuratedCatalog"
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
    sid       = "BucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [local.bucket_arn]
  }

  statement {
    sid       = "ListCuratedAndResults"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [for p in concat(var.read_prefixes, [var.results_prefix]) : "${p}*"]
    }
  }

  statement {
    sid       = "ReadCurated"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [for p in var.read_prefixes : "${local.bucket_arn}/${p}*"]
  }

  statement {
    sid    = "AthenaResults"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.bucket_arn}/${var.results_prefix}*"]
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
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.role_name}-athena"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

output "role_arn" {
  description = "ARN de ta-quicksight-role."
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
