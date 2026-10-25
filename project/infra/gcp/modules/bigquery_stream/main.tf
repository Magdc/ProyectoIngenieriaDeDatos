# Dataset ta_stream y tabla posts_enriched (§5.5). Lote 2.2.
# El Lote 2.4 suma trend_windows, dlq_events y dq_results a este mismo dataset.

variable "project_id" {
  type = string
}

variable "location" {
  description = "Ubicación del dataset (= región de GCP, §5.5)."
  type        = string
}

variable "dataset_id" {
  type    = string
  default = "ta_stream"
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "posts_enriched_partition_expiration_days" {
  description = "Expiración de cada partición diaria de posts_enriched (§5.12: 90 días)."
  type        = number
  default     = 90
}

variable "deletion_protection" {
  description = "Protege posts_enriched contra terraform destroy."
  type        = bool
  default     = true
}

variable "writer_members" {
  description = "Miembros con roles/bigquery.dataEditor en el dataset (p. ej. serviceAccount:sa-dataflow@…)."
  type        = set(string)
  default     = []
}

resource "google_bigquery_dataset" "this" {
  project                    = var.project_id
  dataset_id                 = var.dataset_id
  location                   = var.location
  description                = "TrendAnalyzer: analítica caliente del streaming (posts_enriched; Lote 2.4: trend_windows, dlq_events, dq_results)."
  labels                     = var.labels
  delete_contents_on_destroy = false
}

resource "google_bigquery_table" "posts_enriched" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.this.dataset_id
  table_id            = "posts_enriched"
  description         = "Envelope enriquecido y anonimizado (§5.2, §5.5). Una fila por event_id."
  labels              = var.labels
  deletion_protection = var.deletion_protection
  schema              = file("${path.module}/schemas/posts_enriched.json")

  time_partitioning {
    type          = "DAY"
    field         = "event_time"
    expiration_ms = var.posts_enriched_partition_expiration_days * 24 * 60 * 60 * 1000
  }

  clustering = ["source", "brand_primary"]
}

resource "google_bigquery_dataset_iam_member" "writers" {
  for_each   = var.writer_members
  project    = var.project_id
  dataset_id = google_bigquery_dataset.this.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = each.value
}

output "dataset_id" {
  value = google_bigquery_dataset.this.dataset_id
}

output "posts_enriched_table_id" {
  description = "proyecto.dataset.tabla"
  value       = "${var.project_id}.${google_bigquery_dataset.this.dataset_id}.${google_bigquery_table.posts_enriched.table_id}"
}

output "posts_enriched_table_spec" {
  description = "proyecto:dataset.tabla (formato de --posts_enriched_table)"
  value       = "${var.project_id}:${google_bigquery_dataset.this.dataset_id}.${google_bigquery_table.posts_enriched.table_id}"
}
