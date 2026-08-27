data "aws_caller_identity" "current" {}

locals {
  resource_prefix = "${var.name_prefix}-${var.environment}"

  common_tags = {
    Application = "trend-analyzer"
    Environment = var.environment
    ManagedBy   = "terraform"
    Component   = "batch"
  }

  account_suffix             = data.aws_caller_identity.current.account_id
  raw_bucket_name            = lower("${local.resource_prefix}-${local.account_suffix}-batch-raw")
  curated_bucket_name        = lower("${local.resource_prefix}-${local.account_suffix}-batch-curated")
  athena_results_bucket_name = lower("${local.resource_prefix}-${local.account_suffix}-athena-results")
  glue_database_name         = replace("${local.resource_prefix}_batch_catalog", "-", "_")
  glue_role_name             = "${local.resource_prefix}-glue-etl-role"
  athena_workgroup_name      = "${local.resource_prefix}-analytics"
}
