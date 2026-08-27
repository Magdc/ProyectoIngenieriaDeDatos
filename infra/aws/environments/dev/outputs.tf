output "aws_region" {
  description = "Region configurada para AWS."
  value       = var.aws_region
}

output "raw_bucket_name" {
  description = "Bucket S3 para datasets historicos raw."
  value       = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  description = "Bucket S3 para datasets historicos curated."
  value       = aws_s3_bucket.curated.bucket
}

output "athena_results_bucket_name" {
  description = "Bucket S3 para resultados de consultas Athena."
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database_name" {
  description = "Base de datos Glue Data Catalog para historicos."
  value       = aws_glue_catalog_database.batch.name
}

output "glue_role_arn" {
  description = "Rol IAM reservado para jobs batch de Glue."
  value       = aws_iam_role.glue_etl.arn
}

output "athena_workgroup_name" {
  description = "Workgroup de Athena para consultas historicas."
  value       = aws_athena_workgroup.analytics.name
}
