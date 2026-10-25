# Lote 2.2 (HU-020, HU-021): BigQuery ta_stream.posts_enriched y secreto ta-hmac-key.
# Se suma a los archivos del Lote 1.0 y del Lote 1.3 sin modificarlos: usa sus variables
# (project_id, region) y local.platform_labels. La cuenta sa-dataflow ya existe (platform.tf);
# aquí se referencia por su correo para no depender del nombre interno del módulo.
#
# Adelanta del Lote 2.4 (HU-005) solo lo que la rama enriched necesita: el dataset, la tabla
# posts_enriched, el secreto y los dos permisos de sa-dataflow que faltaban (§5.8).

locals {
  ta_stream_labels  = merge(local.platform_labels, { component = "streaming" })
  sa_dataflow_email = "sa-dataflow@${var.project_id}.iam.gserviceaccount.com"
}

module "bigquery_stream" {
  source = "../../modules/bigquery_stream"

  project_id                               = var.project_id
  location                                 = var.region
  dataset_id                               = var.bq_dataset_id
  labels                                   = local.ta_stream_labels
  posts_enriched_partition_expiration_days = var.posts_enriched_partition_expiration_days
  deletion_protection                      = var.bq_deletion_protection
  writer_members                           = ["serviceAccount:${local.sa_dataflow_email}"]
}

module "hmac_secret" {
  source = "../../modules/hmac_secret"

  project_id       = var.project_id
  secret_id        = var.hmac_secret_id
  labels           = local.ta_stream_labels
  accessor_members = ["serviceAccount:${local.sa_dataflow_email}"]
}
