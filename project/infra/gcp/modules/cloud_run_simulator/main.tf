# Servicio Cloud Run ta-stream-simulator (Lote 1.3, ARCHITECTURE.md §3.1, HU-016).
#
# - min = max = 1 instancia: con más habría emisiones duplicadas y el cursor se pisaría.
# - CPU siempre asignada (cpu_idle = false, equivale a --no-cpu-throttling): el bucle de
#   emisión corre en segundo plano, sin peticiones HTTP.
# - Sin acceso público: no se concede roles/run.invoker a allUsers. /control/scenario
#   (Lote 6.3) exige ID token: lo comprueba el frontend de Cloud Run antes de llegar al contenedor.
# - Sondas de arranque y de vida sobre GET /healthz (503 si un worker cayó → reinicio).

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "name" {
  type    = string
  default = "ta-stream-simulator"
}

variable "image" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "env_vars" {
  type    = map(string)
  default = {}
}

variable "cpu" {
  type    = string
  default = "1"
}

variable "memory" {
  type    = string
  default = "512Mi"
}

variable "labels" {
  type    = map(string)
  default = {}
}

resource "google_cloud_run_v2_service" "this" {
  project             = var.project_id
  location            = var.region
  name                = var.name
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false # permite apagarlo con simulator_running = false
  labels              = var.labels

  template {
    service_account                  = var.service_account_email
    max_instance_request_concurrency = 10
    timeout                          = "60s"
    labels                           = var.labels

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = var.image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
        cpu_idle          = false # CPU siempre asignada
        startup_cpu_boost = true
      }

      dynamic "env" {
        for_each = var.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }

      startup_probe {
        http_get {
          path = "/healthz"
          port = 8080
        }
        initial_delay_seconds = 2
        period_seconds        = 5
        timeout_seconds       = 3
        failure_threshold     = 12
      }

      liveness_probe {
        http_get {
          path = "/healthz"
          port = 8080
        }
        period_seconds    = 30
        timeout_seconds   = 5
        failure_threshold = 3
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

output "uri" {
  value = google_cloud_run_v2_service.this.uri
}

output "name" {
  value = google_cloud_run_v2_service.this.name
}
