output "tfstate_bucket" {
  description = "Bucket de estado remoto. Úsalo en environments/dev/backend.hcl."
  value       = module.tfstate.name
}

output "enabled_services" {
  value = module.services.enabled_services
}

output "budget_name" {
  value = module.budget.budget_name
}
