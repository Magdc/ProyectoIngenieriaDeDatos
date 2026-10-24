variable "project_id" {
  type = string
}

variable "name" {
  description = "Nombre del bucket de estado (globalmente único)."
  type        = string
}

variable "location" {
  description = "Ubicación del bucket (región de GCP)."
  type        = string
}

variable "noncurrent_versions_to_keep" {
  description = "Versiones antiguas del estado que se conservan."
  type        = number
  default     = 10
}

variable "labels" {
  type    = map(string)
  default = {}
}

resource "google_storage_bucket" "this" {
  project  = var.project_id
  name     = var.name
  location = var.location
  labels   = var.labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = var.noncurrent_versions_to_keep
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

output "name" {
  value = google_storage_bucket.this.name
}

output "url" {
  value = google_storage_bucket.this.url
}
