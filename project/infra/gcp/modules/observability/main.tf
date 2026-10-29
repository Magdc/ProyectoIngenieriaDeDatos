# Observabilidad de GCP (Lote 7.2, HU-042; ARCHITECTURE.md §3.32, ADR-36).
#
# Un tablero de Cloud Monitoring «TrendAnalyzer · operación» y cuatro alertas de operación, todo con métricas del
# sistema (Pub/Sub, Dataflow, Cloud Run): no hay que tocar el código para medir.
#   tasa de mensajes  → pubsub topic/send_message_operation_count de ta-raw-* (y ta-dlq, ta-alerts)
#   latencia p95      → subscription/ack_latencies de ta-raw-*-df (distribución: p95 real) y la edad de la marca
#                       de agua de Dataflow (job/data_watermark_age), que acota la latencia de punta a punta
#   tamaño de la DLQ  → mensajes publicados en ta-dlq y atraso de ta-dlq-df
# Alertas: Dataflow fallido, latencia fuera del objetivo (marca de agua > 5 min), DLQ creciente y exportador fallido.
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

variable "job_prefix" {
  description = "Prefijo de los jobs de Dataflow vigilados."
  type        = string
  default     = "ta-stream-"
}

variable "latency_threshold_seconds" {
  description = "Edad máxima de la marca de agua de Dataflow (§5.12: detección < 5 min)."
  type        = number
  default     = 300
}

variable "dlq_threshold" {
  description = "Mensajes en ta-dlq en 10 min para avisar (normal: ~2 con el caos de §3.9)."
  type        = number
  default     = 30
}

variable "labels" {
  type    = map(string)
  default = {}
}

locals {
  dataflow  = "resource.type = \"dataflow_job\" AND resource.label.job_name = starts_with(\"${var.job_prefix}\")"
  raw_topic = "resource.type = \"pubsub_topic\" AND resource.label.topic_id = starts_with(\"ta-raw-\")"
  raw_subs  = "resource.type = \"pubsub_subscription\" AND resource.label.subscription_id = monitoring.regex.full_match(\"ta-raw-.*-df\")"
  dlq_topic = "resource.type = \"pubsub_topic\" AND resource.label.topic_id = \"ta-dlq\""

  # Un gráfico de líneas de una sola consulta.
  chart = {
    for k, w in {
      rate = {
        title  = "Mensajes publicados por minuto (ta-raw-*)"
        filter = "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\" AND ${local.raw_topic}"
        agg    = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE", crossSeriesReducer = "REDUCE_SUM", groupByFields = ["resource.label.topic_id"] }
      }
      backlog = {
        title  = "Atraso de Dataflow: mensajes sin leer en ta-raw-*-df"
        filter = "metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND ${local.raw_subs}"
        agg    = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MAX", crossSeriesReducer = "REDUCE_SUM", groupByFields = ["resource.label.subscription_id"] }
      }
      ack_p95 = {
        title  = "Latencia p95 de lectura de Dataflow (ack, ms)"
        filter = "metric.type = \"pubsub.googleapis.com/subscription/ack_latencies\" AND ${local.raw_subs}"
        agg    = { alignmentPeriod = "300s", perSeriesAligner = "ALIGN_DELTA", crossSeriesReducer = "REDUCE_PERCENTILE_95", groupByFields = ["resource.label.subscription_id"] }
      }
      watermark = {
        title  = "Edad de la marca de agua de Dataflow (s; objetivo < 300)"
        filter = "metric.type = \"dataflow.googleapis.com/job/data_watermark_age\" AND ${local.dataflow}"
        agg    = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MAX" }
      }
      lag = {
        title  = "Retraso del sistema de Dataflow (s)"
        filter = "metric.type = \"dataflow.googleapis.com/job/system_lag\" AND ${local.dataflow}"
        agg    = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MAX" }
      }
      dlq = {
        title  = "DLQ: mensajes publicados en ta-dlq (cada 10 min)"
        filter = "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\" AND ${local.dlq_topic}"
        agg    = { alignmentPeriod = "600s", perSeriesAligner = "ALIGN_DELTA", crossSeriesReducer = "REDUCE_SUM" }
      }
      alerts = {
        title  = "Alertas de tendencia publicadas (ta-alerts)"
        filter = "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\" AND resource.type = \"pubsub_topic\" AND resource.label.topic_id = \"ta-alerts\""
        agg    = { alignmentPeriod = "300s", perSeriesAligner = "ALIGN_DELTA", crossSeriesReducer = "REDUCE_SUM" }
      }
      exporter = {
        title  = "Exportador GCS → S3: ejecuciones por resultado"
        filter = "metric.type = \"run.googleapis.com/job/completed_execution_count\" AND resource.type = \"cloud_run_job\" AND resource.label.job_name = \"ta-gcs-to-s3\""
        agg    = { alignmentPeriod = "3600s", perSeriesAligner = "ALIGN_DELTA", crossSeriesReducer = "REDUCE_SUM", groupByFields = ["metric.label.result"] }
      }
      } : k => {
      title = w.title
      xyChart = {
        dataSets = [{
          plotType        = "LINE"
          timeSeriesQuery = { timeSeriesFilter = { filter = w.filter, aggregation = w.agg } }
        }]
      }
    }
  }
  order = ["rate", "backlog", "ack_p95", "watermark", "lag", "dlq", "alerts", "exporter"]
}

resource "google_monitoring_dashboard" "ops" {
  project = var.project_id
  dashboard_json = jsonencode({
    displayName = "TrendAnalyzer · operación"
    labels      = { trendanalyzer = "" }
    mosaicLayout = {
      columns = 12
      tiles = [for i, k in local.order : {
        xPos   = (i % 2) * 6
        yPos   = floor(i / 2) * 4
        width  = 6
        height = 4
        widget = local.chart[k]
      }]
    }
  })

  lifecycle {
    # La API devuelve el JSON normalizado (orden de claves, valores por defecto).
    ignore_changes = [dashboard_json]
  }
}

resource "google_monitoring_alert_policy" "dataflow_failed" {
  project               = var.project_id
  display_name          = "ta-op-dataflow-fallido"
  combiner              = "OR"
  severity              = "ERROR"
  user_labels           = var.labels
  notification_channels = var.notification_channels
  conditions {
    display_name = "Un job ${var.job_prefix}* quedó en Failed"
    condition_threshold {
      filter          = "metric.type = \"dataflow.googleapis.com/job/is_failed\" AND ${local.dataflow}"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: Dataflow $${resource.label.job_name} falló"
    content   = "El job $${resource.label.job_name} terminó en Failed: no llegan datos a BigQuery ni a GCS. Ver sus logs en la consola de Dataflow y relanzar con `python project/scripts/on.py` (runbook: docs/RUNBOOK.md)."
  }
}

resource "google_monitoring_alert_policy" "latency" {
  project               = var.project_id
  display_name          = "ta-op-latencia"
  combiner              = "OR"
  severity              = "WARNING"
  user_labels           = var.labels
  notification_channels = var.notification_channels
  conditions {
    display_name = "Marca de agua de Dataflow con más de ${var.latency_threshold_seconds} s durante 5 min"
    condition_threshold {
      filter          = "metric.type = \"dataflow.googleapis.com/job/data_watermark_age\" AND ${local.dataflow}"
      comparison      = "COMPARISON_GT"
      threshold_value = var.latency_threshold_seconds
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: latencia de Dataflow fuera del objetivo"
    content   = "La marca de agua de $${resource.label.job_name} lleva más de ${var.latency_threshold_seconds} s de atraso: las ventanas de tendencia (y las alertas) llegan tarde. Suele ser el NLP saturado (un solo worker) o un atraso en ta-raw-*-df. Runbook: docs/RUNBOOK.md."
  }
}

resource "google_monitoring_alert_policy" "dlq" {
  project               = var.project_id
  display_name          = "ta-op-dlq-creciente"
  combiner              = "OR"
  severity              = "WARNING"
  user_labels           = var.labels
  notification_channels = var.notification_channels
  conditions {
    display_name = "Más de ${var.dlq_threshold} mensajes en ta-dlq en 10 min"
    condition_threshold {
      filter          = "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\" AND ${local.dlq_topic}"
      comparison      = "COMPARISON_GT"
      threshold_value = var.dlq_threshold
      duration        = "0s"
      aggregations {
        alignment_period     = "600s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: la DLQ está creciendo"
    content   = "Entraron más de ${var.dlq_threshold} mensajes a ta-dlq en 10 min (lo normal son ~2 por el caos de §3.9). Revisar `dlq_events` en BigQuery (columna `reason`) y el runbook: docs/RUNBOOK.md («Atender la DLQ»)."
  }
}

resource "google_monitoring_alert_policy" "exporter_failed" {
  project               = var.project_id
  display_name          = "ta-op-exportador-fallido"
  combiner              = "OR"
  severity              = "ERROR"
  user_labels           = var.labels
  notification_channels = var.notification_channels
  conditions {
    display_name = "Una ejecución de ta-gcs-to-s3 terminó en failed"
    condition_threshold {
      filter          = "metric.type = \"run.googleapis.com/job/completed_execution_count\" AND resource.type = \"cloud_run_job\" AND resource.label.job_name = \"ta-gcs-to-s3\" AND metric.label.result = \"failed\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  documentation {
    mime_type = "text/markdown"
    subject   = "TrendAnalyzer: falló el exportador GCS → S3"
    content   = "Una ejecución de ta-gcs-to-s3 falló: lo de `enriched/` no llega a S3 hasta la siguiente (cada 15 min). Ver `gcloud run jobs executions list --job ta-gcs-to-s3` y el log `export_done`/`export_failed` (§3.18.1)."
  }
}

output "dashboard_id" {
  value = google_monitoring_dashboard.ops.id
}

output "policy_names" {
  value = [for p in [google_monitoring_alert_policy.dataflow_failed, google_monitoring_alert_policy.latency,
  google_monitoring_alert_policy.dlq, google_monitoring_alert_policy.exporter_failed] : p.display_name]
}
