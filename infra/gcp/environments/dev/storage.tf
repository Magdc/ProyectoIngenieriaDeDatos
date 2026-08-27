# Bucket para almacenar los datos crudos (Raw Data / Parquet / Respaldos)
resource "google_storage_bucket" "raw_events" {
  name          = local.raw_bucket_name
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  labels        = local.common_labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }

    condition {
      age        = 30
      with_state = "LIVE"
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age        = 180
      with_state = "LIVE"
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age        = 30
      with_state = "ARCHIVED"
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Bucket temporal requerido por Dataflow para staging y temporales de ejecución
resource "google_storage_bucket" "dataflow_temp" {
  name                        = lower("${local.resource_prefix}-${var.project_id}-dataflow-temp")
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  labels                      = local.common_labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true # Permite borrar fácilmente archivos temporales en dev

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7 # Limpia archivos temporales automáticamente cada 7 días
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Asignación de permisos para que la Service Account de Dataflow administre el bucket Raw
resource "google_storage_bucket_iam_member" "dataflow_raw_object_admin" {
  bucket = google_storage_bucket.raw_events.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Asignación de permisos para que la Service Account de Dataflow administre el bucket Temp
resource "google_storage_bucket_iam_member" "dataflow_temp_object_admin" {
  bucket = google_storage_bucket.dataflow_temp.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}