# Federación GCP → AWS del exportador gcs-to-s3 (Lote 3.2, HU-006; ARCHITECTURE.md §3.18).
#
# Se suma a main.tf / variables.tf (Lote 1.0) y a lake.tf (Lote 3.1) sin modificarlos: usa
# sus variables env, aws_account_id y aws_region y la llave y el bucket del lake (por nombre,
# con data sources). El rol solo se crea cuando gcp_exporter_sa_unique_id tiene valor; ese
# valor (y el correo) los escribe validate_3_2.py en gcp_exporter.auto.tfvars, que no se
# versiona (*.auto.tfvars en infra/.gitignore).

locals {
  exporter_lake_bucket = "ta-${var.env}-${var.aws_account_id}-lake"
  exporter_role_create = var.gcp_exporter_sa_unique_id != ""
}

# Solo se lee cuando se crea el rol (los planes de otros lotes no dependen de ella).
data "aws_kms_alias" "exporter_lake" {
  provider = aws.exporter
  count    = local.exporter_role_create ? 1 : 0
  name     = "alias/ta-${var.env}-lake"
}

module "gcp_exporter_role" {
  source    = "../../modules/gcp_exporter_role"
  providers = { aws = aws.exporter }
  count     = local.exporter_role_create ? 1 : 0

  role_name           = "ta-gcp-exporter-role"
  bucket_name         = local.exporter_lake_bucket
  kms_key_arn         = data.aws_kms_alias.exporter_lake[0].target_key_arn
  aws_region          = var.aws_region
  object_prefix       = "raw/origin=stream/"
  gcp_sa_unique_id    = var.gcp_exporter_sa_unique_id
  gcp_sa_email        = var.gcp_exporter_sa_email
  audience            = var.gcp_exporter_audience
  max_session_seconds = 3600
}
