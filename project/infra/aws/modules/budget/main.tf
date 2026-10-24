variable "name" {
  type = string
}

variable "limit_usd" {
  description = "Presupuesto mensual en USD (AWS Budgets de la cuenta)."
  type        = number
}

variable "alert_emails" {
  type = list(string)
}

variable "actual_thresholds" {
  description = "Porcentajes del límite que avisan sobre gasto real."
  type        = list(number)
  default     = [50, 80, 100]
}

resource "aws_budgets_budget" "monthly" {
  name         = var.name
  budget_type  = "COST"
  limit_amount = tostring(var.limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = var.actual_thresholds
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.alert_emails
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.alert_emails
  }
}

output "budget_name" {
  value = aws_budgets_budget.monthly.name
}
