# Lote 7.2 (HU-042, §3.32, ADR-36): estado de los jobs de Glue, alarma de fallos con aviso por correo (SNS) y el
# tablero de CloudWatch ta-lakehouse. Los correos van en terraform.tfvars (alarm_emails), que no se versiona.

module "lakehouse_observability" {
  source             = "../../modules/lakehouse_observability"
  providers          = { aws = aws.observability }
  region             = var.aws_region
  failures_log_group = "/aws/events/ta-glue-job-fallos"
  athena_workgroup   = "ta-analytics"
  alarm_emails       = var.alarm_emails
  retention_days     = var.lakehouse_log_retention_days
}

variable "alarm_emails" {
  description = "Destinatarios de la alarma de Glue (cada uno confirma la suscripción de SNS). Vacío = sin correo."
  type        = list(string)
  default     = []
  sensitive   = true
}

output "ops_sns_topic_arn" {
  value = module.lakehouse_observability.sns_topic_arn
}

output "ops_glue_alarm" {
  value = module.lakehouse_observability.alarm_name
}

output "ops_dashboard" {
  value = module.lakehouse_observability.dashboard_name
}

output "ops_email_subscriptions" {
  value = module.lakehouse_observability.email_subscriptions
}
