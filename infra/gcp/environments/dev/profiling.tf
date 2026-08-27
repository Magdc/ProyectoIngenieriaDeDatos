# -----------------------------------------------------------------------
# infra/gcp/environments/dev/profiling.tf
#
# Cloud Run Job — Profiler (US-14)
# Ejecuta el script de profiling sobre el Raw layer en GCS y escribe
# el reporte bajo gs://RAW_BUCKET_NAME/profiling/YYYY/MM/DD/
#
# Activacion manual:
#   gcloud run jobs execute trend-dev-profiler-job --region us-central1
#
# Activacion periodica (opcional — anadir aqui si se requiere cron):
#   Se puede conectar a Cloud Scheduler apuntando al Job con un trigger HTTP
#   o usando el flag --execute-now en el CI/CD pipeline.
# -----------------------------------------------------------------------

resource "google_cloud_run_v2_job" "profiler" {
  name     = "${local.resource_prefix}-profiler-job"
  project  = var.project_id
  location = var.region
  labels   = local.common_labels

  deletion_protection = false

  template {
    template {
      service_account = google_service_account.profiler.email

      timeout     = "1800s" # 30 min maximo — suficiente para 180 dias de datos
      max_retries = 1

      containers {
        # Imagen temporal — reemplazar con la imagen real del profiler
        # REGION-docker.pkg.dev/PROJECT_ID/REPO/profiler-job:TAG
        image = var.collector_container_image

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi" # pandas puede consumir bastante con datasets grandes
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }

        env {
          name  = "RAW_BUCKET_NAME"
          value = google_storage_bucket.raw_events.name
        }

        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }
      }
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_storage_bucket.raw_events,
  ]
}

# ---------------------------------------------------------------------------
# Service Account del profiler
# ---------------------------------------------------------------------------
resource "google_service_account" "profiler" {
  account_id   = "${local.resource_prefix}-profiler-sa"
  display_name = "TrendAnalyzer Profiler Job SA"
  project      = var.project_id
}

# Lectura del bucket Raw (para leer los eventos crudos)
resource "google_storage_bucket_iam_member" "profiler_raw_reader" {
  bucket = google_storage_bucket.raw_events.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.profiler.email}"
}

# Escritura en el mismo bucket para guardar el reporte bajo profiling/
resource "google_storage_bucket_iam_member" "profiler_report_writer" {
  bucket = google_storage_bucket.raw_events.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.profiler.email}"
}

# Permiso para ejecutar el Job (necesario para activacion desde CI/CD o Scheduler)
resource "google_cloud_run_v2_job_iam_member" "profiler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.profiler.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}
