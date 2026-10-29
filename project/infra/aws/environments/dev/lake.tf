# Lote 3.1 (HU-024, ADR-21): adelanta del Lote 4.1 (HU-007) lo mínimo para que el
# histórico tenga dónde escribir: el bucket del lake, su llave KMS y el rol
# ta-historical-loader. Se suma a los archivos del Lote 1.0 sin modificarlos: usa sus
# variables aws_account_id y env, y el proveedor aws ya configurado (default_tags y
# allowed_account_ids). El Lote 4.1 amplía estos recursos (Glue, Athena, EventBridge,
# políticas de Glue y QuickSight sobre la misma llave).

locals {
  lake_bucket_name = "ta-${var.env}-${var.aws_account_id}-lake"
}

module "lake_kms" {
  source    = "../../modules/kms_key"
  providers = { aws = aws.lake }

  alias                   = "ta-${var.env}-lake"
  description             = "TrendAnalyzer ${var.env}: cifrado SSE-KMS del lake (${local.lake_bucket_name})"
  deletion_window_in_days = var.lake_kms_deletion_window_days
}

module "lake_bucket" {
  source    = "../../modules/s3_lake"
  providers = { aws = aws.lake }

  bucket_name                    = local.lake_bucket_name
  kms_key_arn                    = module.lake_kms.key_arn
  athena_results_expiration_days = var.athena_results_expiration_days
}

module "historical_loader" {
  source    = "../../modules/historical_loader_role"
  providers = { aws = aws.historical }

  role_name              = "ta-historical-loader"
  aws_account_id         = var.aws_account_id
  trusted_principal_arns = var.historical_loader_principals
  bucket_arn             = module.lake_bucket.bucket_arn
  kms_key_arn            = module.lake_kms.key_arn
  prefix                 = "raw/origin=historical/"
}
