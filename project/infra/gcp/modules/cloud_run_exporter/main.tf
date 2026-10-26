# Cloud Run Job ta-gcs-to-s3 y su Cloud Scheduler (Lote 3.2, HU-025; ARCHITECTURE.md §5.6).
#
# El job corre con sa-exporter. El Scheduler lo dispara cada 15 min con la identidad de
# sa-exporter (roles/run.invoker solo sobre este job) y se crea **en pausa**: el estado por
# defecto es el de ahorro. Se pausa y reanuda con gcloud (validate_3_2.py schedule/off);
# Terraform ignora "paused" para que eso no aparezca como deriva en los planes de on.py/off.py.

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "job_name" {
  type    = string
  default = "ta-gcs-to-s3"
}

variable "scheduler_name" {
  type    = string
  default = "ta-gcs-to-s3-cada-15-min"
}

variable "image" {
  description = "Imagen del exportador (…/ta-images/gcs-to-s3:{etiqueta})."
  type        = string
}

variable "service_account_email" {
  description = "sa-exporter: identidad del job y del Scheduler."
  type        = string
}

variable "env_vars" {
  description = "Variables de entorno del contenedor (§5.1 y §5.6)."
  type        = map(string)
}

variable "labels" {
  type = map(string)
}

variable "schedule" {
  type    = string
  default = "*/15 * * * *"
}

variable "time_zone" {
  type    = string
  default = "Etc/UTC"
}

variable "cpu" {
  type    = string
  default = "1"
}

variable "memory" {
  type    = string
  default = "512Mi"
}

variable "task_timeout_seconds" {
  description = "Tiempo máximo de una corrida (menor que el intervalo del Scheduler)."
  type        = number
  default     = 840
}

variable "max_retries" {
  type    = number
  default = 1
}

resource "google_cloud_run_v2_job" "this" {
  name                = var.job_name
  project             = var.project_id
  location            = var.region
  deletion_protection = false
  labels              = var.labels

  template {
    labels      = var.labels
    task_count  = 1
    parallelism = 1

    template {
      service_account = var.service_account_email
      timeout         = "${var.task_timeout_seconds}s"
      max_retries     = var.max_retries

      containers {
        image = var.image
        args  = ["run"]

        resources {
          limits = {
            cpu    = var.cpu
            memory = var.memory
          }
        }

        dynamic "env" {
          for_each = var.env_vars
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.this.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account_email}"
}

resource "google_cloud_scheduler_job" "this" {
  name             = var.scheduler_name
  project          = var.project_id
  region           = var.region
  description      = "Lote 3.2: dispara ${var.job_name} (GCS enriched/ → S3 raw/origin=stream/)."
  schedule         = var.schedule
  time_zone        = var.time_zone
  attempt_deadline = "180s"
  paused           = true

  # Sin retry_config: Cloud Scheduler no reintenta por defecto (los reintentos son los del job).
  # Con `retry_config { retry_count = 0 }` la API no devuelve el bloque y cada plan proponía
  # añadirlo (visto en E2, v2.30).

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.this.name}:run"
    body        = base64encode("{}")
    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = var.service_account_email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  lifecycle {
    ignore_changes = [paused]
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_invoker]
}

output "job_name" {
  value = google_cloud_run_v2_job.this.name
}

output "scheduler_name" {
  value = google_cloud_scheduler_job.this.name
}

output "image" {
  value = google_cloud_run_v2_job.this.template[0].template[0].containers[0].image
}
