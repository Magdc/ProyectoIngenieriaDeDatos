variable "aws_account_id" {
  description = "ID de la cuenta AWS. El provider se niega a operar en otra cuenta."
  type        = string
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "owner" {
  type    = string
  default = "alejandro"
}

variable "budget_usd" {
  description = "Presupuesto mensual en USD."
  type        = number
}

variable "alert_emails" {
  description = "Destinatarios de alertas de presupuesto. No se versiona."
  type        = list(string)
}
