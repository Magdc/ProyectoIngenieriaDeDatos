# Lote 7.1 (HU-041, §3.30, ADR-34): rol de GitHub Actions para el `terraform plan` del CD (.github/workflows/cd.yml).
# Apagado por defecto: github_cd_enabled = true lo crea (decisión del usuario).

module "github_cd" {
  count                = var.github_cd_enabled ? 1 : 0
  source               = "../../modules/github_cd_role"
  repository           = var.github_repository
  tfstate_bucket       = "ta-${var.env}-${var.aws_account_id}-tfstate"
  create_oidc_provider = var.github_oidc_provider_arn == ""
  oidc_provider_arn    = var.github_oidc_provider_arn
  tags                 = { component = "cicd" }
}

variable "github_cd_enabled" {
  description = "Crea el rol ta-github-cd (y el proveedor OIDC de GitHub si no se da uno). Por defecto, no."
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "owner/nombre del repositorio que puede asumir el rol (solo la rama main)."
  type        = string
  default     = "AlejandroSPosada/Trend_Analyzer"
}

variable "github_oidc_provider_arn" {
  description = "ARN del proveedor OIDC de GitHub si la cuenta ya tiene uno (es uno por cuenta). Vacío = crearlo."
  type        = string
  default     = ""
}

output "github_cd_role_arn" {
  description = "Valor de la variable del repositorio TA_AWS_CD_ROLE (null si el CD está apagado)."
  value       = try(module.github_cd[0].role_arn, null)
}
