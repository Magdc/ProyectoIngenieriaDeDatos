# Repositorio Docker de Artifact Registry (Lote 1.3). Conserva las 10 versiones más
# recientes de cada imagen y borra las imágenes sin etiqueta de más de 7 días.

variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "repository_id" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "keep_recent" {
  type    = number
  default = 10
}

resource "google_artifact_registry_repository" "this" {
  project                = var.project_id
  location               = var.location
  repository_id          = var.repository_id
  format                 = "DOCKER"
  description            = "Imágenes de TrendAnalyzer"
  labels                 = var.labels
  cleanup_policy_dry_run = false

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = var.keep_recent
    }
  }

  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"
    condition {
      tag_state  = "UNTAGGED"
      older_than = "604800s"
    }
  }
}

output "repository_id" {
  value = google_artifact_registry_repository.this.repository_id
}

output "url" {
  description = "Prefijo de las imágenes: {region}-docker.pkg.dev/{proyecto}/{repo}"
  value       = "${var.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
}
