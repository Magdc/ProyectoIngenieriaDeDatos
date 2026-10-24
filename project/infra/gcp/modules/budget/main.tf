variable "project_id" {
  type = string
}

variable "project_number" {
  description = "Número del proyecto (el filtro del presupuesto lo exige)."
  type        = string
}

variable "billing_account_id" {
  description = "ID de la cuenta de facturación, formato XXXXXX-XXXXXX-XXXXXX."
  type        = string
}

variable "amount" {
  description = "Monto mensual del presupuesto, en la moneda de la cuenta de facturación."
  type        = number
}

variable "currency_code" {
  description = "Moneda de la cuenta de facturación (debe coincidir; p. ej. COP o USD)."
  type        = string
}

variable "alert_emails" {
  description = "Destinatarios de las alertas de presupuesto."
  type        = list(string)
}

variable "thresholds" {
  description = "Umbrales (fracción del monto) que disparan aviso sobre gasto real."
  type        = list(number)
  default     = [0.5, 0.9, 1.0]
}

resource "google_monitoring_notification_channel" "email" {
  for_each = toset(var.alert_emails)

  project      = var.project_id
  display_name = "ta-budget-${each.value}"
  type         = "email"

  labels = {
    email_address = each.value
  }
}

resource "google_billing_budget" "this" {
  billing_account = var.billing_account_id
  display_name    = "ta-budget-${var.project_id}"

  budget_filter {
    projects = ["projects/${var.project_number}"]
  }

  amount {
    specified_amount {
      currency_code = var.currency_code
      units         = tostring(floor(var.amount))
    }
  }

  dynamic "threshold_rules" {
    for_each = var.thresholds
    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  all_updates_rule {
    monitoring_notification_channels = [for c in google_monitoring_notification_channel.email : c.id]
    disable_default_iam_recipients   = false
  }
}

output "budget_name" {
  value = google_billing_budget.this.name
}
