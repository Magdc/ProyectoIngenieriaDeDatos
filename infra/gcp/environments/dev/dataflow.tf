resource "null_resource" "build_dataflow_template" {
  triggers = {
    main_py_hash          = filesha256("${path.module}/../../../../src/main.py")
    requirements_hash     = filesha256("${path.module}/../../../../src/requirements.txt")
    subscription_name     = google_pubsub_subscription.dataflow.name
    raw_bucket_name       = google_storage_bucket.raw_events.name
    dataflow_temp_bucket  = google_storage_bucket.dataflow_temp.name
  }

  provisioner "local-exec" {
    working_dir = "${path.module}/../../../../src"
    command     = <<-EOT
      set -euo pipefail
      ${var.dataflow_python_bin} -m venv .dataflow_build_venv
      . .dataflow_build_venv/bin/activate
      pip install --quiet --upgrade pip
      pip install --quiet -r requirements.txt
      python main.py \
        --runner=DataflowRunner \
        --project=${var.project_id} \
        --region=${var.region} \
        --staging_location=gs://${google_storage_bucket.dataflow_temp.name}/staging \
        --temp_location=gs://${google_storage_bucket.dataflow_temp.name}/temp \
        --template_location=${local.dataflow_template_gcs_path} \
        --service_account_email=${google_service_account.dataflow_worker.email} \
        --subscription=projects/${var.project_id}/subscriptions/${google_pubsub_subscription.dataflow.name} \
        --output_table=${var.project_id}:${local.bigquery_dataset_streaming}.events_processed \
        --deadletter_table=${var.project_id}:${local.bigquery_dataset_streaming}.events_deadletter \
        --raw_gcs_bucket=gs://${google_storage_bucket.raw_events.name} \
        --requirements_file=requirements.txt
      deactivate
    EOT
  }

  depends_on = [
    google_project_service.required_apis,
    google_pubsub_subscription.dataflow,
    google_storage_bucket.raw_events,
    google_storage_bucket.dataflow_temp,
    google_bigquery_table.events_processed,
    google_bigquery_table.events_deadletter,
    google_project_iam_member.dataflow_worker,
    google_project_iam_member.dataflow_bq_writer,
    google_project_iam_member.dataflow_bq_job_user,
    google_storage_bucket_iam_member.dataflow_raw_object_admin,
    google_storage_bucket_iam_member.dataflow_temp_object_admin,
    google_pubsub_subscription_iam_member.dataflow_subscriber,
  ]
}

resource "google_dataflow_job" "ingestion_pipeline" {
  name    = local.dataflow_job_name
  project = var.project_id
  region  = var.region

  template_gcs_path = local.dataflow_template_gcs_path
  temp_gcs_location = "gs://${google_storage_bucket.dataflow_temp.name}/temp"

  service_account_email = google_service_account.dataflow_worker.email
  machine_type          = var.dataflow_machine_type
  max_workers           = var.dataflow_max_workers

  on_delete = "drain"

  labels = local.common_labels

  depends_on = [null_resource.build_dataflow_template]
}