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

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket_iam_member" "dataflow_raw_object_admin" {
  bucket = google_storage_bucket.raw_events.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}