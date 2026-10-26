# Outputs del Lote 3.2 (§5.9). Dependen de recursos del módulo (no solo de variables) para
# que se guarden también con plan/apply con -target (observado en el Lote 2.4).

output "exporter_role_arn" {
  description = "ARN de ta-gcp-exporter-role (TA_EXPORTER_ROLE_ARN del job ta-gcs-to-s3)."
  value       = try(module.gcp_exporter_role[0].role_arn, null)
}

output "exporter_role_name" {
  description = "Nombre del rol del exportador."
  value       = try(module.gcp_exporter_role[0].role_name, null)
}

output "exporter_oidc_audience" {
  description = "Audiencia del ID token de sa-exporter (TA_EXPORTER_AUDIENCE)."
  value       = try(module.gcp_exporter_role[0].audience, null)
}
