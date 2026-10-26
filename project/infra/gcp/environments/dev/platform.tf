# Infraestructura GCP base del entorno dev (Lote 1.3, HU-004 y HU-016).
#
# Se suma a main.tf / variables.tf / outputs.tf del Lote 1.0 sin modificarlos: usa sus
# variables project_id, region, env y owner, y el proveedor google que ya configuran.
# Contratos: ARCHITECTURE.md §5.3 (Pub/Sub), §5.4 (GCS), §5.8 (IAM), §5.9 (outputs), §3.1 (Cloud Run).

locals {
  platform_labels = {
    project = "trend-analyzer"
    env     = var.env
    owner   = var.owner
  }
  sources              = ["reddit", "facebook", "instagram"]
  datasets_bucket_name = "${var.project_id}-ta-datasets"
  lake_bucket_name     = "${var.project_id}-ta-lake"
  deploy_simulator     = var.simulator_image != "" && var.simulator_running
}

# --- Pub/Sub (§5.3) -------------------------------------------------------------------

module "pubsub" {
  source     = "../../modules/pubsub"
  project_id = var.project_id
  sources    = local.sources
  labels     = local.platform_labels
}

# --- Buckets (§5.4, retención §5.12) --------------------------------------------------

module "datasets_bucket" {
  source          = "../../modules/gcs_bucket"
  project_id      = var.project_id
  name            = local.datasets_bucket_name
  location        = var.region
  labels          = merge(local.platform_labels, { component = "simulator" })
  versioning      = true # protege el cursor (sim-state/) ante una escritura mala
  noncurrent_days = 7
}

module "lake_bucket" {
  source     = "../../modules/gcs_bucket"
  project_id = var.project_id
  name       = local.lake_bucket_name
  location   = var.region
  labels     = merge(local.platform_labels, { component = "lake" })
  prefix_expiration = {
    "raw/" = 30
    "dlq/" = 30
  }
}

# --- Artifact Registry ------------------------------------------------------------------

module "artifact_registry" {
  source        = "../../modules/artifact_registry"
  project_id    = var.project_id
  location      = var.region
  repository_id = var.artifact_repository
  labels        = local.platform_labels
}

# --- Service accounts (§5.8) --------------------------------------------------------------

module "sa_simulator" {
  source       = "../../modules/service_account"
  project_id   = var.project_id
  account_id   = "sa-simulator"
  display_name = "TrendAnalyzer · stream-simulator"
  description  = "Publica en ta-raw-* y guarda el cursor en ${local.datasets_bucket_name}."
}

module "sa_dataflow" {
  source        = "../../modules/service_account"
  project_id    = var.project_id
  account_id    = "sa-dataflow"
  display_name  = "TrendAnalyzer · Dataflow"
  description   = "Pipeline de streaming (Fase 2)."
  project_roles = ["roles/dataflow.worker"]
}

module "sa_exporter" {
  source       = "../../modules/service_account"
  project_id   = var.project_id
  account_id   = "sa-exporter"
  display_name = "TrendAnalyzer · gcs-to-s3"
  description  = "Exporta enriched/ a S3 (Lote 3.2)."
}

# sa-simulator: publicar en los tres topics de entrada y leer/escribir el bucket de datasets.
resource "google_pubsub_topic_iam_member" "simulator_publisher" {
  for_each = module.pubsub.topic_names
  project  = var.project_id
  topic    = each.value
  role     = "roles/pubsub.publisher"
  member   = module.sa_simulator.member
}

resource "google_storage_bucket_iam_member" "simulator_datasets" {
  bucket = module.datasets_bucket.name
  role   = "roles/storage.objectUser"
  member = module.sa_simulator.member
}

# sa-dataflow: consumir las suscripciones, publicar inválidos en ta-dlq y escribir el lake.
# (BigQuery y el secreto HMAC se conceden en el Lote 2.4, cuando existan.)
resource "google_pubsub_subscription_iam_member" "dataflow_subscriber" {
  for_each     = module.pubsub.subscription_names
  project      = var.project_id
  subscription = each.value
  role         = "roles/pubsub.subscriber"
  member       = module.sa_dataflow.member
}

resource "google_pubsub_topic_iam_member" "dataflow_dlq_publisher" {
  project = var.project_id
  topic   = module.pubsub.extra_topic_names["ta-dlq"]
  role    = "roles/pubsub.publisher"
  member  = module.sa_dataflow.member
}

resource "google_storage_bucket_iam_member" "dataflow_lake" {
  bucket = module.lake_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.sa_dataflow.member
}

# sa-exporter: escribir solo bajo export-state/ (watermark, §5.6). Desde el Lote 3.2 (v2.30)
# la lectura del lake está limitada a enriched/: el objectViewer sin condición que había aquí
# (exporter_lake_read) se reemplazó por exporter_lake_enriched, con condición, en exporter.tf.
resource "google_storage_bucket_iam_member" "exporter_watermark" {
  bucket = module.lake_bucket.name
  role   = "roles/storage.objectUser"
  member = module.sa_exporter.member

  condition {
    title       = "solo-export-state"
    description = "Escritura limitada al prefijo export-state/"
    expression  = "resource.name.startsWith(\"projects/_/buckets/${module.lake_bucket.name}/objects/export-state/\")"
  }
}

# --- Cloud Run: ta-stream-simulator (§3.1) ----------------------------------------------
# Se crea cuando hay imagen (simulator_image) y simulator_running = true.
# Apagarlo (simulator_running = false) borra el servicio; el cursor sigue en GCS y al
# volver a encenderlo continúa desde el último seq guardado.

module "simulator" {
  count                 = local.deploy_simulator ? 1 : 0
  source                = "../../modules/cloud_run_simulator"
  project_id            = var.project_id
  region                = var.region
  image                 = var.simulator_image
  service_account_email = module.sa_simulator.email
  cpu                   = var.simulator_cpu
  memory                = var.simulator_memory
  labels                = merge(local.platform_labels, { component = "simulator" })
  env_vars = {
    ENV               = var.env
    GCP_PROJECT       = var.project_id
    GCP_REGION        = var.region
    SIM_SEED          = tostring(var.sim_seed)
    SIM_CHAOS_ENABLED = tostring(var.sim_chaos_enabled)
    SIM_PUBLISHER     = "pubsub"
    SIM_STATE_BACKEND = "gcs"
    SIM_STATE_BUCKET  = module.datasets_bucket.name
  }

  depends_on = [
    google_pubsub_topic_iam_member.simulator_publisher,
    google_storage_bucket_iam_member.simulator_datasets,
  ]
}
