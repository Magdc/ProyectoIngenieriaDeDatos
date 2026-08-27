# Dataset principal de analítica para streaming
resource "google_bigquery_dataset" "streaming_dataset" {
  dataset_id                  = local.bigquery_dataset_streaming
  friendly_name               = "Trend Analyzer Streaming Analytics"
  description                 = "Capa analítica estructurada para almacenar eventos procesados desde Dataflow"
  location                    = var.region
  default_table_expiration_ms = null

  labels = local.common_labels

  depends_on = [google_project_service.required_apis]
}

# Tabla Principal Analítica (Partitioned & Clustered)
resource "google_bigquery_table" "events_processed" {
  dataset_id          = google_bigquery_dataset.streaming_dataset.dataset_id
  table_id            = "events_processed"
  project             = var.project_id
  deletion_protection = false

  # Particionamiento diario por tiempo de ingesta para optimizar costos de consulta
  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  # Clustering por fuente e idioma para acelerar el filtrado analítico
  clustering = ["source_name", "language"]

  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED", description = "ID único del evento" },
    { name = "source_name", type = "STRING", mode = "REQUIRED", description = "Origen del dato (mastodon, news)" },
    { name = "title", type = "STRING", mode = "NULLABLE", description = "Titular o cabecera" },
    { name = "clean_text", type = "STRING", mode = "NULLABLE", description = "Texto normalizado y limpio para NLP/analítica" },
    { name = "url", type = "STRING", mode = "NULLABLE", description = "Enlace original de la publicación" },
    { name = "language", type = "STRING", mode = "NULLABLE", description = "Código de idioma ISO-639-1" },
    { name = "tags", type = "STRING", mode = "REPEATED", description = "Categorías o etiquetas extraídas" },
    { name = "author_id", type = "STRING", mode = "NULLABLE", description = "ID del autor/usuario" },
    { name = "author_username", type = "STRING", mode = "NULLABLE", description = "Nombre de usuario del autor" },
    { 
      name = "metrics", 
      type = "RECORD", 
      mode = "NULLABLE", 
      description = "Métricas de interacción, normalizadas entre fuentes",
      fields = [
        { name = "primary_count", type = "INTEGER", mode = "NULLABLE", description = "Métrica principal de engagement (ej. favourites en Mastodon)" },
        { name = "secondary_count", type = "INTEGER", mode = "NULLABLE", description = "Métrica secundaria de engagement (ej. reblogs en Mastodon, comentarios en News)" },
        { name = "replies_count", type = "INTEGER", mode = "NULLABLE", description = "Respuestas o comentarios directos" },
        { name = "author_followers_count", type = "INTEGER", mode = "NULLABLE" }
      ]
    },
    { name = "published_at", type = "TIMESTAMP", mode = "NULLABLE", description = "Fecha original de publicación" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Fecha de procesamiento en la canalización" }
  ])
}

# Tabla Dead Letter Queue para almacenamiento de registros malformados o fallidos
resource "google_bigquery_table" "events_deadletter" {
  dataset_id          = google_bigquery_dataset.streaming_dataset.dataset_id
  table_id            = "events_deadletter"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "failed_at"
  }

  schema = jsonencode([
    { name = "raw_payload", type = "STRING", mode = "REQUIRED", description = "Payload JSON crudo que causó la falla" },
    { name = "error_message", type = "STRING", mode = "REQUIRED", description = "Detalle del error durante la validación" },
    { name = "failed_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Timestamp de captura del fallo" }
  ])
}

# Vista Analítica: Métricas resumidas por Fuente y Día (Ideal para perfilado y dashboards)
resource "google_bigquery_table" "view_daily_source_summary" {
  dataset_id = google_bigquery_dataset.streaming_dataset.dataset_id
  table_id   = "v_daily_source_summary"
  project    = var.project_id

  view {
    query = <<SQL
      SELECT 
        DATE(ingested_at) AS event_date,
        source_name,
        COUNT(1) AS total_events,
        COUNT(DISTINCT author_id) AS total_authors,
        SUM(metrics.primary_count) AS total_primary_engagement,
        SUM(metrics.secondary_count) AS total_secondary_engagement
      FROM `${var.project_id}.${local.bigquery_dataset_streaming}.events_processed`
      GROUP BY 1, 2
SQL
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.events_processed]
}