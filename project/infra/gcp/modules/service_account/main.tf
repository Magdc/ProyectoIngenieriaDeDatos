# Una service account con roles a nivel de proyecto (los permisos sobre recursos
# concretos se dan en el entorno, junto al recurso; §5.8).

variable "project_id" {
  type = string
}

variable "account_id" {
  type = string
}

variable "display_name" {
  type = string
}

variable "description" {
  type    = string
  default = ""
}

variable "project_roles" {
  type    = list(string)
  default = []
}

resource "google_service_account" "this" {
  project      = var.project_id
  account_id   = var.account_id
  display_name = var.display_name
  description  = var.description
}

resource "google_project_iam_member" "roles" {
  for_each = toset(var.project_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.this.email}"
}

output "email" {
  value = google_service_account.this.email
}

output "member" {
  value = "serviceAccount:${google_service_account.this.email}"
}

output "name" {
  value = google_service_account.this.name
}
