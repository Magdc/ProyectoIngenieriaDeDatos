# Bootstrap de AWS (HU-046, Lote 1.0)
#
# Se aplica UNA vez, con estado local: crea el bucket S3 y la tabla DynamoDB
# del estado remoto con bloqueo (ADR-14). Ver infra/BOOTSTRAP.md.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Autenticación: perfil local de AWS CLI (`aws configure` o AWS_PROFILE), §5.1.
provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      project = "trend-analyzer"
      env     = var.env
      owner   = var.owner
    }
  }
}

module "tfstate" {
  source = "../modules/tfstate_backend"

  bucket_name     = "ta-${var.env}-${var.aws_account_id}-tfstate"
  lock_table_name = "ta-${var.env}-tflock"
}

module "budget" {
  source = "../modules/budget"

  name         = "ta-${var.env}-monthly"
  limit_usd    = var.budget_usd
  alert_emails = var.alert_emails
}
