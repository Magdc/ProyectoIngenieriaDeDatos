# Outputs del Lote 3.2 (§5.9). Dependen de recursos del lote para que también se guarden con
# plan/apply con -target (observado en el Lote 2.4).

output "exporter_sa_unique_id" {
  description = "uniqueId de sa-exporter (claim sub de su ID token; lo usa la confianza del rol de AWS)."
  value       = data.google_service_account.exporter.unique_id
  depends_on  = [google_storage_bucket_iam_member.exporter_lake_enriched]
}

output "exporter_job_name" {
  description = "Cloud Run Job del exportador (null sin imagen)."
  value       = try(module.exporter_job[0].job_name, null)
}

output "exporter_scheduler_name" {
  description = "Cloud Scheduler del exportador (se crea en pausa)."
  value       = try(module.exporter_job[0].scheduler_name, null)
}

output "exporter_image" {
  description = "Imagen desplegada en el job."
  value       = try(module.exporter_job[0].image, null)
}
