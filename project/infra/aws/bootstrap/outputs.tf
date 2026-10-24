output "tfstate_bucket" {
  description = "Bucket de estado remoto. Úsalo en environments/dev/backend.hcl."
  value       = module.tfstate.bucket_name
}

output "tflock_table" {
  description = "Tabla de bloqueo. Úsala en environments/dev/backend.hcl."
  value       = module.tfstate.lock_table_name
}

output "budget_name" {
  value = module.budget.budget_name
}
