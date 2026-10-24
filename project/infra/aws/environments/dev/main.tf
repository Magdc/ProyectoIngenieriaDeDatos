# Entorno dev de AWS. Vacío a propósito en el Lote 1.0: los módulos de S3, Glue,
# Athena e IAM llegan en las Fases 3 y 4.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Configuración parcial: el resto se pasa con `-backend-config=backend.hcl`.
  backend "s3" {
    key     = "env/dev/terraform.tfstate"
    encrypt = true
  }
}

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
