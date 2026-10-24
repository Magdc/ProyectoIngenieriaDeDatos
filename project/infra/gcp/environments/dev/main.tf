# Entorno dev de GCP. Vacío a propósito en el Lote 1.0: el Lote 1.3 añade aquí
# Cloud Run, Pub/Sub, buckets y service accounts (módulos en ../../modules/).

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Configuración parcial: el bucket se pasa con `-backend-config=backend.hcl`.
  backend "gcs" {
    prefix = "env/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  labels = {
    project = "trend-analyzer"
    env     = var.env
    owner   = var.owner
  }
}
