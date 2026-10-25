#!/usr/bin/env bash
# Estado real de la infraestructura en la nube, para pedir o validar un lote (ARCHITECTURE §8).
# Solo lee: no crea, enciende ni borra nada. Pegue la salida en la petición del lote.
#
#   bash scripts/preflight.sh            # desde project/ (o desde cualquier carpeta)
#
# Para el entorno local (emulador), use: bash scripts/local_dev.sh preflight
set -u

cd "$(dirname "$0")/.."            # project/
P="${GCP_PROJECT:-project-0568d9ba-848f-413c-88b}"
R="${GCP_REGION:-us-central1}"
TF=infra/gcp/environments/dev

section() { printf '\n== %s\n' "$*"; }

section "Fecha (UTC) · revise que coincida con la hora real (WSL se desfasa al suspender)"
date -u +%Y-%m-%dT%H:%M:%SZ

section "gcloud"
echo "proyecto activo: $(gcloud config get-value project 2>/dev/null)"
if gcloud auth application-default print-access-token >/dev/null 2>&1; then echo "ADC: ok"
else echo "ADC: FALTA → gcloud auth application-default login"; fi

section "terraform.tfvars (simulador)"
grep -E '^(simulator_running|simulator_image|sim_seed|sim_chaos_enabled)' "$TF/terraform.tfvars" 2>/dev/null \
  || echo "sin $TF/terraform.tfvars"

section "Cloud Run"
gcloud run services list --project "$P" --region "$R" \
  --format='table(metadata.name,status.conditions[0].status,status.url)' 2>&1

section "Pub/Sub · topics"
gcloud pubsub topics list --project "$P" --format='value(name.basename())' 2>&1

section "Pub/Sub · suscripciones (ta-verify-* sobrantes = una verificación interrumpida)"
gcloud pubsub subscriptions list --project "$P" --format='value(name.basename())' 2>&1

section "Cursor del simulador (gs://$P-ta-datasets/sim-state/)"
for s in reddit facebook instagram; do
  printf '%s: ' "$s"
  gcloud storage cat "gs://$P-ta-datasets/sim-state/$s.json" 2>/dev/null || printf 'sin cursor'
  echo
done

section "Dataflow · jobs activos"
gcloud dataflow jobs list --project "$P" --region "$R" --status=active \
  --format='table(id,name,state)' 2>&1

section "BigQuery · datasets"
bq ls --project_id "$P" 2>&1 | head -20

section "Terraform outputs (sin simulator_url = simulador apagado)"
terraform -chdir="$TF" output 2>&1 | grep -E 'simulator_url|bq_dataset|lake_bucket' \
  || echo "(no hay simulator_url: el servicio está apagado o el entorno no está inicializado)"

section "AWS"
aws sts get-caller-identity --query Account --output text 2>&1
