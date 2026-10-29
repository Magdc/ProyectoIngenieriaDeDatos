# Alertas de gasto por olvido (Lote 7.3, HU-043; ARCHITECTURE.md §3.31, ADR-35).
#
# Lo que más cobra en GCP es lo que queda encendido: Dataflow (≈ 0,25 USD/h) y el simulador (≈ 0,10 USD/h, §5.12).
# Los presupuestos del Lote 1.0 avisan por el gasto del mes, con días de atraso; estas dos políticas avisan el mismo
# día si algo sigue encendido más de lo que dura una validación o una demo:
# - un job de Dataflow `ta-stream-*` con más de `dataflow_max_hours` de vida (métrica job/elapsed_time);
# - el servicio ta-stream-simulator con instancias durante `simulator_max_hours` seguidas (container/instance_count).
# Notifican por los canales del Lote 6.2 (correo y copia en ta-alerts-incidents).

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

variable "project_id" {
  type = string
}

variable "notification_channels" {
  type = list(string)
}

variable "dataflow_max_hours" {
  type    = number
  default = 4
}

variable "simulator_max_hours" {
  type    = number
  default = 8
}

variable "labels" {
  type    = map(string)
  default = {}
}

locals {
  # Cloud Monitoring acepta como mucho 23 h 30 min de duración en una condición de umbral.
  simulator_seconds = min(var.simulator_max_hours * 3600, 84600)
}

resource "google_monitoring_alert_policy" "dataflow_on" {
  project      = var.project_id
  display_name = "ta-gasto-dataflow-encendido"
  combiner     = "OR"
  severity     = "WARNING"
  user_labels  = var.labels

  notification_channels = var.notification_channels

  conditions {
    display_name = "Dataflow ta-stream-* encendido más de ${var.dataflow_max_hours} h"
    condition_threshold {
      filter          = "resource.type = \"dataflow_job\" AND metric.type = \"dataflow.googleapis.com/job/elapsed_time\" AND resource.label.job_name = starts_with(\"ta-stream-\")"
      comparison      = "COMPARISON_GT"
      threshold_value = var.dataflow_max_hours * 3600
      duration        = "0s"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MAX"
      }
      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: Dataflow lleva más de ${var.dataflow_max_hours} h encendido"
    content   = "El job $${resource.label.job_name} sigue corriendo (≈ 0,25 USD/h). Si no hay una demo o una validación en curso: `python project/scripts/off.py` (drena Dataflow y apaga el simulador). Guía: docs/ACTIVE_SERVICES.md."
  }
}

resource "google_monitoring_alert_policy" "simulator_on" {
  project      = var.project_id
  display_name = "ta-gasto-simulador-encendido"
  combiner     = "OR"
  severity     = "WARNING"
  user_labels  = var.labels

  notification_channels = var.notification_channels

  conditions {
    display_name = "ta-stream-simulator con instancias más de ${var.simulator_max_hours} h"
    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.label.service_name = \"ta-stream-simulator\" AND metric.type = \"run.googleapis.com/container/instance_count\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "${local.simulator_seconds}s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: el simulador lleva más de ${var.simulator_max_hours} h encendido"
    content   = "ta-stream-simulator sigue con instancias (≈ 0,10 USD/h, y con Dataflow encendido llegan alertas naturales). Si no hace falta: `python project/scripts/off.py`. Guía: docs/ACTIVE_SERVICES.md."
  }
}

output "policy_names" {
  value = [google_monitoring_alert_policy.dataflow_on.display_name, google_monitoring_alert_policy.simulator_on.display_name]
}

output "policy_ids" {
  value = [google_monitoring_alert_policy.dataflow_on.name, google_monitoring_alert_policy.simulator_on.name]
}
