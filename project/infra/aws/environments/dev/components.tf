# Lote 7.3 (HU-043, §3.31, ADR-35): un proveedor por componente para que `default_tags` ponga la etiqueta
# `component` en todo lo que crea cada módulo (los módulos no reciben etiquetas). El proveedor sin alias de main.tf
# queda para lo que no es de ningún componente. Para ver el costo por componente en Cost Explorer hay que activar
# `component` como etiqueta de asignación de costos (Billing → Cost allocation tags): es una opción de la cuenta,
# que es compartida, y la decide el usuario.

locals {
  base_tags = {
    project = "trend-analyzer"
    env     = var.env
    owner   = var.owner
  }
}

provider "aws" {
  alias               = "lake"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "lake" })
  }
}

provider "aws" {
  alias               = "historical"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "historical" })
  }
}

provider "aws" {
  alias               = "exporter"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "exporter" })
  }
}

provider "aws" {
  alias               = "lakehouse"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "lakehouse" })
  }
}

provider "aws" {
  alias               = "serving"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "serving" })
  }
}

provider "aws" {
  alias               = "cicd"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "cicd" })
  }
}

provider "aws" {
  alias               = "observability"
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = merge(local.base_tags, { component = "observability" })
  }
}
