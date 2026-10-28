# Outputs del Lote 6.2 (alertas). Los lee validate_6_2.py. Los que dependen de la imagen son null sin ella.

output "alerts_service_account" {
  value = module.sa_alerts.email
}

output "alerts_notifier_url" {
  value = module.alerts.service_uri
}

output "alerts_notifier_image" {
  value = var.alerts_notifier_image != "" ? var.alerts_notifier_image : null
  # Con -target, Terraform solo guarda los outputs que dependen de un recurso del objetivo (§3.16).
  depends_on = [module.sa_alerts]
}

output "alerts_push_subscription" {
  value = module.alerts.push_subscription
}

output "alerts_incidents_topic" {
  value = module.alerts.incidents_topic
}

output "alert_policy_name" {
  value = module.alerts.policy_name
}

output "alert_policy_id" {
  value = module.alerts.policy_id
}

output "alert_email_channels" {
  value = module.alerts.email_channels
}
