# Bucket de GCS con acceso uniforme, sin acceso público y reglas de ciclo de vida
# opcionales por prefijo (Lote 1.3, §5.4 y §5.12).

variable "project_id" {
  type = string
}

variable "name" {
  type = string
}

variable "location" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "versioning" {
  type    = bool
  default = false
}

variable "noncurrent_days" {
  description = "Con versionado: días que se conserva una versión no vigente (0 = sin regla)."
  type        = number
  default     = 0
}

variable "prefix_expiration" {
  description = "Mapa prefijo -> días hasta borrar los objetos (p. ej. { \"raw/\" = 30 })."
  type        = map(number)
  default     = {}
}

variable "force_destroy" {
  type    = bool
  default = false
}

resource "google_storage_bucket" "this" {
  project                     = var.project_id
  name                        = var.name
  location                    = var.location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy
  labels                      = var.labels

  versioning {
    enabled = var.versioning
  }

  dynamic "lifecycle_rule" {
    for_each = var.prefix_expiration
    content {
      action {
        type = "Delete"
      }
      condition {
        age            = lifecycle_rule.value
        matches_prefix = [lifecycle_rule.key]
      }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.versioning && var.noncurrent_days > 0 ? [1] : []
    content {
      action {
        type = "Delete"
      }
      condition {
        days_since_noncurrent_time = var.noncurrent_days
        with_state                 = "ARCHIVED"
      }
    }
  }
}

output "name" {
  value = google_storage_bucket.this.name
}

output "url" {
  value = google_storage_bucket.this.url
}
