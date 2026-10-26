# Tablas de ta_stream que suma el Lote 2.4 (ARCHITECTURE.md §5.5): trend_windows,
# dlq_events, dq_results y la vista trend_windows_latest. El dataset y posts_enriched
# los crea el Lote 2.2 (módulo bigquery_stream); este módulo solo añade tablas.
#
# Los schemas JSON se generan desde src/common/stream_tables.py:
#   python -m common.stream_tables --write infra/gcp/modules/bigquery_stream_tables/schemas
# y una prueba exige que coincidan (src/streaming/tests/test_trends_terraform.py).

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

variable "project_id" {
  description = "Proyecto de GCP."
  type        = string
}

variable "dataset_id" {
  description = "Dataset donde van las tablas (ta_stream, §5.5)."
  type        = string
}

variable "labels" {
  description = "Etiquetas comunes (project, env, owner, component)."
  type        = map(string)
  default     = {}
}

variable "deletion_protection" {
  description = "Protección contra borrado de las tablas (no de la vista)."
  type        = bool
  default     = false
}

variable "trend_windows_expiration_days" {
  description = "Expiración de las particiones de trend_windows, en días."
  type        = number
  default     = 90
}

variable "dlq_events_expiration_days" {
  description = "Expiración de las particiones de dlq_events, en días (igual que dlq/ en GCS)."
  type        = number
  default     = 30
}

variable "dq_results_expiration_days" {
  description = "Expiración de las particiones de dq_results, en días; null = no expiran."
  type        = number
  default     = null
}

locals {
  ms_per_day = 86400000
}

resource "google_bigquery_table" "trend_windows" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = "trend_windows"
  description         = "Ventanas deslizantes de 5 min (paso 1 min) por marca y fuente. Una fila por panel; la versión vigente es la de mayor n_posts (vista trend_windows_latest)."
  deletion_protection = var.deletion_protection
  labels              = var.labels
  schema              = file("${path.module}/schemas/trend_windows.json")
  clustering          = ["brand", "source"]

  time_partitioning {
    type          = "DAY"
    field         = "window_start"
    expiration_ms = var.trend_windows_expiration_days * local.ms_per_day
  }
}

resource "google_bigquery_table" "dlq_events" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = "dlq_events"
  description         = "Mensajes que no pasaron la validación de Dataflow, leídos de ta-dlq (suscripción ta-dlq-df)."
  deletion_protection = var.deletion_protection
  labels              = var.labels
  schema              = file("${path.module}/schemas/dlq_events.json")
  clustering          = ["source", "error_type"]

  time_partitioning {
    type          = "DAY"
    field         = "received_at"
    expiration_ms = var.dlq_events_expiration_days * local.ms_per_day
  }
}

resource "google_bigquery_table" "dq_results" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = "dq_results"
  description         = "Una fila por verificación de calidad ejecutada (§5.10)."
  deletion_protection = var.deletion_protection
  labels              = var.labels
  schema              = file("${path.module}/schemas/dq_results.json")
  clustering          = ["layer", "kpi"]

  time_partitioning {
    type          = "DAY"
    field         = "run_ts"
    expiration_ms = var.dq_results_expiration_days == null ? null : var.dq_results_expiration_days * local.ms_per_day
  }
}

resource "google_bigquery_table" "trend_windows_latest" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = "trend_windows_latest"
  description         = "Versión vigente de cada ventana × marca × fuente: la fila de mayor n_posts de trend_windows."
  deletion_protection = false
  labels              = var.labels

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT * EXCEPT (version_rank)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY window_start, brand, source
            ORDER BY n_posts DESC, n_negative DESC, n_positive DESC
          ) AS version_rank
        FROM `${var.project_id}.${var.dataset_id}.trend_windows`
      )
      WHERE version_rank = 1
    SQL
  }

  depends_on = [google_bigquery_table.trend_windows]
}

output "trend_windows_table" {
  description = "trend_windows en el formato de Beam (proyecto:dataset.tabla)."
  value       = "${var.project_id}:${var.dataset_id}.${google_bigquery_table.trend_windows.table_id}"
}

output "dlq_events_table" {
  description = "dlq_events en el formato de Beam."
  value       = "${var.project_id}:${var.dataset_id}.${google_bigquery_table.dlq_events.table_id}"
}

output "dq_results_table" {
  description = "dq_results en el formato de Beam."
  value       = "${var.project_id}:${var.dataset_id}.${google_bigquery_table.dq_results.table_id}"
}

output "trend_windows_latest_view" {
  description = "Vista trend_windows_latest en formato SQL (proyecto.dataset.vista)."
  value       = "${var.project_id}.${var.dataset_id}.${google_bigquery_table.trend_windows_latest.table_id}"
}
