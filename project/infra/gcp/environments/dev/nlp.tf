# Lote 2.3 (HU-022): sa-dataflow descarga la imagen dataflow-nlp de Artifact Registry.
# Usa var.region y var.artifact_repository (Lote 1.3) y local.sa_dataflow_email (Lote 2.2).

resource "google_artifact_registry_repository_iam_member" "dataflow_image_reader" {
  project    = var.project_id
  location   = var.region
  repository = var.artifact_repository
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${local.sa_dataflow_email}"
}
