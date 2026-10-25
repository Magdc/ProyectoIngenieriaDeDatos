# Secreto ta-hmac-key (§5.1, ADR-06). Lote 2.2.
# Terraform crea el contenedor y los permisos; el VALOR se sube con scripts/hmac_key.sh,
# para que la llave no quede en el estado de Terraform ni en el repositorio.

variable "project_id" {
  type = string
}

variable "secret_id" {
  type    = string
  default = "ta-hmac-key"
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "accessor_members" {
  description = "Miembros con roles/secretmanager.secretAccessor (solo quien anonimiza: sa-dataflow; el loader usa ADC del desarrollador)."
  type        = set(string)
  default     = []
}

resource "google_secret_manager_secret" "this" {
  project   = var.project_id
  secret_id = var.secret_id
  labels    = var.labels

  replication {
    auto {}
  }

  # Perder el secreto cambia todos los user_hash: no se borra con terraform destroy.
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_secret_manager_secret_iam_member" "accessors" {
  for_each  = var.accessor_members
  project   = var.project_id
  secret_id = google_secret_manager_secret.this.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value
}

output "secret_id" {
  value = google_secret_manager_secret.this.secret_id
}

output "secret_name" {
  description = "projects/{número}/secrets/{id}"
  value       = google_secret_manager_secret.this.name
}
