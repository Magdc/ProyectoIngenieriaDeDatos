# Bootstrap de GCP (HU-046, Lote 1.0)
#
# Se aplica UNA vez, con estado local, porque crea el bucket que guardará el
# estado remoto del resto de la infraestructura (ADR-14). Ver infra/BOOTSTRAP.md.

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# Autenticación: Application Default Credentials (§5.1). Sin llaves JSON.
provider "google" {
  project = var.project_id
  region  = var.region

  # Billing Budgets y Monitoring se cobran/cuotean contra el proyecto indicado.
  user_project_override = true
  billing_project       = var.project_id
}

locals {
  labels = {
    project = "trend-analyzer"
    env     = var.env
    owner   = var.owner
  }

  services = [
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudbilling.googleapis.com",
    "billingbudgets.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
  ]
}

module "services" {
  source     = "../modules/project_services"
  project_id = var.project_id
  services   = local.services
}

module "tfstate" {
  source     = "../modules/tfstate_bucket"
  project_id = var.project_id
  name       = "${var.project_id}-ta-tfstate"
  location   = var.region
  labels     = local.labels

  depends_on = [module.services]
}

module "budget" {
  source = "../modules/budget"

  project_id         = var.project_id
  project_number     = var.project_number
  billing_account_id = var.billing_account_id
  amount             = var.budget_amount
  currency_code      = var.budget_currency
  alert_emails       = var.alert_emails

  depends_on = [module.services]
}
