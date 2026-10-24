variable "project_id" {
  description = "ID del proyecto GCP (GCP_PROJECT)."
  type        = string
}

variable "project_number" {
  description = "Número del proyecto GCP."
  type        = string
}

variable "region" {
  description = "Región de GCP (ADR-12)."
  type        = string
  default     = "us-central1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "owner" {
  type    = string
  default = "alejandro"
}

variable "billing_account_id" {
  description = "Cuenta de facturación vinculada al proyecto (XXXXXX-XXXXXX-XXXXXX). Ver `gcloud billing accounts list`."
  type        = string
}

variable "budget_amount" {
  description = "Presupuesto mensual en la moneda de la cuenta de facturación."
  type        = number
}

variable "budget_currency" {
  description = "Moneda de la cuenta de facturación. Debe coincidir con la de la cuenta (la consola mostró COP)."
  type        = string
  default     = "COP"
}

variable "alert_emails" {
  description = "Destinatarios de alertas (por ahora solo el correo de Alejandro). No se versiona."
  type        = list(string)
}
