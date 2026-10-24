variable "project_id" {
  description = "ID del proyecto GCP (GCP_PROJECT)."
  type        = string
}

variable "services" {
  description = "APIs a habilitar."
  type        = list(string)
}

resource "google_project_service" "this" {
  for_each = toset(var.services)

  project = var.project_id
  service = each.value

  # No deshabilitar APIs al destruir: evita romper recursos que aún dependan de ellas.
  disable_on_destroy         = false
  disable_dependent_services = false
}

output "enabled_services" {
  description = "APIs habilitadas por este módulo."
  value       = sort([for s in google_project_service.this : s.service])
}
