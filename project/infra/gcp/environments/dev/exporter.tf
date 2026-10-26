# Exportador gcs-to-s3 (Lote 3.2, HU-006 y HU-025; ARCHITECTURE.md §3.18, §5.6 y §5.8).
#
# Se suma a platform.tf (Lote 1.3) sin cambiar sus recursos salvo uno: el storage.objectViewer
# de sa-exporter en todo el lake (exporter_lake_read) se quitó de platform.tf y aquí se
# reemplaza por uno con condición. La escritura en export-state/ sigue siendo la de
# platform.tf (exporter_watermark, objectUser con condición).
#
# Los valores de AWS (rol, bucket, llave) llegan por aws_exporter.auto.tfvars, que escribe
# validate_3_2.py desde `terraform output` de infra/aws y no se versiona. Sin imagen no se
# crean el job ni el Scheduler.

data "google_service_account" "exporter" {
  account_id = "sa-exporter" # el mismo account_id de module.sa_exporter (platform.tf)
  project    = var.project_id
}

locals {
  exporter_lake_object_prefix = "projects/_/buckets/${module.lake_bucket.name}/objects/enriched/"
  exporter_create_job = alltrue([
    var.exporter_image != "",
    var.exporter_role_arn != "",
    var.exporter_s3_bucket != "",
    var.exporter_kms_key_arn != "",
  ])
}

# sa-exporter: listar el bucket (storage.objects.list se evalúa sobre el bucket; da los
# nombres, no el contenido) y leer solo los objetos de enriched/ (HU-025).
resource "google_storage_bucket_iam_member" "exporter_lake_enriched" {
  bucket = module.lake_bucket.name
  role   = "roles/storage.objectViewer"
  member = module.sa_exporter.member

  condition {
    title       = "solo-enriched"
    description = "Lote 3.2: listar el bucket y leer solo objetos de enriched/"
    expression  = "resource.type == \"storage.googleapis.com/Bucket\" || (resource.type == \"storage.googleapis.com/Object\" && resource.name.startsWith(\"${local.exporter_lake_object_prefix}\"))"
  }
}

# Quien valida desde su máquina (parte D) emite ID tokens de sa-exporter (solo esa SA).
resource "google_service_account_iam_member" "exporter_oidc_token_creator" {
  for_each           = toset(var.exporter_oidc_token_creators)
  service_account_id = "projects/${var.project_id}/serviceAccounts/${module.sa_exporter.email}"
  role               = "roles/iam.serviceAccountOpenIdTokenCreator"
  member             = each.value
}

module "exporter_job" {
  source = "../../modules/cloud_run_exporter"
  count  = local.exporter_create_job ? 1 : 0

  project_id            = var.project_id
  region                = var.region
  job_name              = "ta-gcs-to-s3"
  scheduler_name        = "ta-gcs-to-s3-cada-15-min"
  image                 = var.exporter_image
  service_account_email = module.sa_exporter.email
  labels                = merge(local.platform_labels, { component = "exporter" })
  schedule              = var.exporter_schedule
  cpu                   = var.exporter_cpu
  memory                = var.exporter_memory
  task_timeout_seconds  = var.exporter_task_timeout_seconds
  max_retries           = 1

  env_vars = {
    ENV                       = var.env
    GCP_PROJECT               = var.project_id
    GCP_REGION                = var.region
    TA_GCS_LAKE_BUCKET        = module.lake_bucket.name
    TA_EXPORT_STATE_KEY       = "export-state/watermark.json"
    TA_LAKE_BUCKET            = var.exporter_s3_bucket
    TA_LAKE_KMS_KEY_ARN       = var.exporter_kms_key_arn
    TA_EXPORTER_ROLE_ARN      = var.exporter_role_arn
    TA_EXPORTER_AUDIENCE      = var.exporter_audience
    TA_EXPORTER_SA            = module.sa_exporter.email
    AWS_REGION                = var.exporter_aws_region
    TA_EXPORT_WORKERS         = tostring(var.exporter_workers)
    TA_EXPORT_MAX_OBJECTS     = tostring(var.exporter_max_objects)
    TA_EXPORT_OVERLAP_SECONDS = "3600"
  }

  depends_on = [google_storage_bucket_iam_member.exporter_lake_enriched]
}
