# Outputs del Lote 3.1 (§5.9). Los consume scripts/load_historical.py upload (vía
# validate_3_1.py) y, desde el Lote 4.1, Glue/Athena.

output "lake_bucket" {
  description = "Bucket del lake (§5.7)."
  value       = module.lake_bucket.bucket
}

output "lake_bucket_arn" {
  value = module.lake_bucket.bucket_arn
}

output "kms_key_arn" {
  description = "Llave KMS del lake (SSE-KMS)."
  value       = module.lake_kms.key_arn
}

output "kms_key_alias" {
  value = module.lake_kms.alias_name
}

output "historical_loader_role_arn" {
  description = "Rol ta-historical-loader (escribe solo en raw/origin=historical/)."
  value       = module.historical_loader.role_arn
}
