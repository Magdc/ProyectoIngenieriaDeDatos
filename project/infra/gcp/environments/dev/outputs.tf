# Los outputs de §5.9 (topic_names, lake_bucket, ...) se añaden en el Lote 1.3.
output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "labels" {
  value = local.labels
}
