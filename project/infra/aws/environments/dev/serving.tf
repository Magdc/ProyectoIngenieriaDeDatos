# Serving en QuickSight (Lote 6.1, HU-037 y HU-038; ARCHITECTURE.md §3.27, ADR-31).
#
# Se suma a lakehouse.tf sin modificarlo. Solo los roles IAM de acceso al tablero: la suscripción de QuickSight
# (cuenta, edición y correo de avisos) se crea una vez a mano y lo de adentro de QuickSight (fuente Athena con
# ta-quicksight-role, datasets SPICE, grupos, usuarios, análisis y tablero) lo despliega scripts/quicksight.py.

module "quicksight_access" {
  source = "../../modules/quicksight_access"

  aws_account_id = var.aws_account_id
}

output "quicksight_access_role_arns" {
  description = "Roles IAM de acceso al tablero (autor y lector, §5.8)."
  value       = module.quicksight_access.role_arns
}
