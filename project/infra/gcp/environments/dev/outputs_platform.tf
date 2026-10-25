# Outputs del Lote 1.3 (ARCHITECTURE.md §5.9). bq_dataset llega con el Lote 2.4.

output "topic_names" {
  description = "Mapa source -> topic ta-raw-{source}."
  value       = module.pubsub.topic_names
}

output "subscription_names" {
  description = "Mapa source -> suscripción ta-raw-{source}-df."
  value       = module.pubsub.subscription_names
}

output "dlq_topic" {
  value = module.pubsub.extra_topic_names["ta-dlq"]
}

output "alerts_topic" {
  value = module.pubsub.extra_topic_names["ta-alerts"]
}

output "datasets_bucket" {
  value = module.datasets_bucket.name
}

output "lake_bucket" {
  value = module.lake_bucket.name
}

output "artifact_registry_url" {
  description = "Prefijo de las imágenes Docker."
  value       = module.artifact_registry.url
}

output "simulator_url" {
  description = "URL del servicio (null si no está desplegado). No es pública: requiere ID token."
  value       = local.deploy_simulator ? module.simulator[0].uri : null
}

output "service_accounts" {
  description = "Mapa nombre -> correo de las service accounts."
  value = {
    "sa-simulator" = module.sa_simulator.email
    "sa-dataflow"  = module.sa_dataflow.email
    "sa-exporter"  = module.sa_exporter.email
  }
}
