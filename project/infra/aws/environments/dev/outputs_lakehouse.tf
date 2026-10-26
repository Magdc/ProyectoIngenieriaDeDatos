# Outputs del Lote 4.1 (§5.9). Todos dependen de recursos de los módulos (no solo de variables)
# para que se guarden también con plan/apply con -target (observado en el Lote 2.4).
# lake_bucket y kms_key_arn están en outputs_lake.tf (3.1); exporter_role_arn en
# outputs_exporter.tf (3.2).

output "glue_db_names" {
  description = "Capa → base de Glue (raw → ta_raw, trusted → ta_trusted, curated → ta_curated)."
  value       = module.glue_catalog.database_names
}

output "glue_table_names" {
  description = "Tablas del catálogo (base.tabla)."
  value       = module.glue_catalog.table_names
}

output "athena_workgroup" {
  description = "Workgroup de Athena (ta-analytics)."
  value       = module.athena.name
}

output "athena_results_location" {
  description = "Resultados de Athena (SSE-KMS con la llave del lake, 7 días)."
  value       = module.athena.output_location
}

output "athena_bytes_scanned_cutoff" {
  description = "Límite de bytes escaneados por consulta en ta-analytics."
  value       = module.athena.bytes_scanned_cutoff_per_query
}

output "glue_role_arn" {
  description = "ARN de ta-glue-role (rol de los jobs de Glue)."
  value       = module.glue_role.role_arn
}

output "quicksight_role_arn" {
  description = "ARN de ta-quicksight-role (Lote 6.1)."
  value       = module.quicksight_role.role_arn
}

output "glue_workflow_name" {
  description = "Workflow de Glue (ta-wf-raw-to-curated)."
  value       = module.glue_workflow.workflow_name
}

output "glue_job_names" {
  description = "Clave → nombre de los jobs del workflow."
  value       = module.glue_workflow.job_names
}

output "glue_script_locations" {
  description = "Clave → s3:// del script de cada job."
  value       = module.glue_workflow.script_locations
}

output "lakehouse_schedule_name" {
  description = "Horario de EventBridge Scheduler (creado deshabilitado)."
  value       = module.lakehouse_schedule.schedule_name
}

output "lakehouse_scheduler_role_arn" {
  description = "Rol que asume el Scheduler para arrancar el workflow."
  value       = module.lakehouse_schedule.role_arn
}

output "glue_failures_log_group" {
  description = "Grupo de CloudWatch Logs con los fallos de los jobs ta-job-*."
  value       = module.glue_failure_events.log_group_name
}
