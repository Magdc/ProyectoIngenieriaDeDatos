# Rol de AWS para el `terraform plan` del CD (Lote 7.1, HU-041; ARCHITECTURE.md §3.30, ADR-34).
#
# GitHub Actions lo asume con su token OIDC (sin llaves), solo desde `repository` en la rama main. Permisos:
# leer el estado de Terraform (sin el candado: el plan corre con -lock=false -refresh=false) y ViewOnlyAccess
# para los data sources (sin leer datos de S3; la cuenta es compartida con Campus Locator).
# El proveedor OIDC de GitHub es uno por cuenta: si ya existe, create_oidc_provider = false y se usa su ARN.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

variable "name" {
  type    = string
  default = "ta-github-cd"
}

variable "repository" {
  type = string
}

variable "branch" {
  type    = string
  default = "main"
}

variable "tfstate_bucket" {
  type = string
}

variable "create_oidc_provider" {
  type    = bool
  default = true
}

variable "oidc_provider_arn" {
  description = "ARN del proveedor OIDC de GitHub si ya existe en la cuenta (con create_oidc_provider = false)."
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_oidc_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  tags           = var.tags
}

locals {
  provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : var.oidc_provider_arn
}

data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.repository}:ref:refs/heads/${var.branch}"]
    }
  }
}

resource "aws_iam_role" "cd" {
  name                 = var.name
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
  tags                 = var.tags
}

resource "aws_iam_role_policy_attachment" "view_only" {
  role       = aws_iam_role.cd.name
  policy_arn = "arn:aws:iam::aws:policy/job-function/ViewOnlyAccess"
}

data "aws_iam_policy_document" "state" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.tfstate_bucket}"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.tfstate_bucket}/*"]
  }
}

resource "aws_iam_role_policy" "state" {
  name   = "tfstate-read"
  role   = aws_iam_role.cd.id
  policy = data.aws_iam_policy_document.state.json
}

output "role_arn" {
  value = aws_iam_role.cd.arn
}
