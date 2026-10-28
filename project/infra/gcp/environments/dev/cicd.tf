# Lote 7.1 (HU-041, §3.30, ADR-34): federación de GitHub Actions para el CD (.github/workflows/cd.yml).
# Apagada por defecto: github_cd_enabled = true la crea (decisión del usuario: confiar en el repositorio público).

module "github_cd" {
  count               = var.github_cd_enabled ? 1 : 0
  source              = "../../modules/github_cd"
  project_id          = var.project_id
  repository          = var.github_repository
  region              = var.region
  artifact_repository = "ta-images"
  tfstate_bucket      = "${var.project_id}-ta-tfstate"
}

variable "github_cd_enabled" {
  description = "Crea la federación GitHub → GCP del CD (pool ta-github, sa-github-cd). Por defecto, no."
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "owner/nombre del repositorio que puede publicar imágenes y leer el estado (solo la rama main)."
  type        = string
  default     = "AlejandroSPosada/Trend_Analyzer"
}

output "github_cd_workload_identity_provider" {
  description = "Valor de la variable del repositorio TA_GCP_WIF_PROVIDER (null si el CD está apagado)."
  value       = try(module.github_cd[0].workload_identity_provider, null)
}

output "github_cd_service_account" {
  description = "Valor de la variable del repositorio TA_GCP_CD_SA (null si el CD está apagado)."
  value       = try(module.github_cd[0].service_account, null)
}
