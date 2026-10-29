# Federación GitHub Actions → GCP para el CD (Lote 7.1, HU-041; ARCHITECTURE.md §3.30, ADR-34).
#
# Sin llaves: el workflow .github/workflows/cd.yml pide un token OIDC a GitHub y lo cambia por uno de
# sa-github-cd. Solo lo acepta para `repository` y la rama main (attribute_condition). La cuenta puede:
# - publicar imágenes en ta-images (Cloud Build con los scripts build_*_image.sh);
# - leer el estado de Terraform para un `plan -refresh=false -lock=false` (no ve secretos: sin refresh no
#   se leen las versiones del secreto HMAC).
# Se crea solo con github_cd_enabled = true (decisión del usuario; por defecto no existe nada).

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

variable "project_id" {
  type = string
}

variable "repository" {
  description = "owner/nombre del repositorio de GitHub que puede usar la federación."
  type        = string
}

variable "branch" {
  description = "Única rama aceptada (refs/heads/<branch>)."
  type        = string
  default     = "main"
}

variable "region" {
  type = string
}

variable "artifact_repository" {
  description = "Repositorio de Artifact Registry de las imágenes (ta-images)."
  type        = string
}

variable "tfstate_bucket" {
  description = "Bucket del estado de Terraform (solo lectura)."
  type        = string
}

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.project_id
  workload_identity_pool_id = "ta-github"
  display_name              = "TrendAnalyzer · GitHub Actions"
  description               = "CD del Lote 7.1: solo ${var.repository} en ${var.branch}"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  attribute_condition = "assertion.repository == \"${var.repository}\" && assertion.ref == \"refs/heads/${var.branch}\""
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "cd" {
  project      = var.project_id
  account_id   = "sa-github-cd"
  display_name = "TrendAnalyzer · GitHub Actions (CD)"
}

resource "google_service_account_iam_member" "federation" {
  service_account_id = google_service_account.cd.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.repository}"
}

resource "google_artifact_registry_repository_iam_member" "writer" {
  project    = var.project_id
  location   = var.region
  repository = var.artifact_repository
  role       = "roles/artifactregistry.writer"
  member     = google_service_account.cd.member
}

resource "google_project_iam_member" "project" {
  for_each = toset([
    "roles/cloudbuild.builds.editor", # gcloud builds submit
    "roles/serviceusage.serviceUsageConsumer",
    "roles/logging.viewer", # ver el log de la construcción
  ])
  project = var.project_id
  role    = each.value
  member  = google_service_account.cd.member
}

# La fuente de `gcloud builds submit` se sube a {proyecto}_cloudbuild (lo creó Cloud Build en el Lote 1.3):
# permiso solo en ese bucket, no en el lake.
resource "google_storage_bucket_iam_member" "cloudbuild_source" {
  bucket = "${var.project_id}_cloudbuild"
  role   = "roles/storage.objectAdmin"
  member = google_service_account.cd.member
}

resource "google_storage_bucket_iam_member" "tfstate" {
  bucket = var.tfstate_bucket
  role   = "roles/storage.objectViewer"
  member = google_service_account.cd.member
}

output "workload_identity_provider" {
  value = google_iam_workload_identity_pool_provider.github.name
}

output "service_account" {
  value = google_service_account.cd.email
}
