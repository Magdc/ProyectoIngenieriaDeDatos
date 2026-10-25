#!/usr/bin/env bash
# Lote 2.1 · Job de Dataflow del pipeline de streaming en GCP.
#
#   bash scripts/dataflow_job.sh preflight   # estado real, solo lectura
#   bash scripts/dataflow_job.sh start [--machine-type e2-standard-2] [--window 300]
#                                      [--network N] [--subnetwork URL] [--dlq-table P:ta_stream.dlq_events]
#   bash scripts/dataflow_job.sh status      # jobs activos y contadores del pipeline
#   bash scripts/dataflow_job.sh drain       # apagado ordenado: termina las ventanas abiertas
#   bash scripts/dataflow_job.sh cancel      # apagado inmediato (puede perder la ventana abierta)
#
# Lee GCP_PROJECT (obligatorio), GCP_REGION (us-central1) y ENV (dev) — §5.1.
# Nombres de §5.3, §5.4 y §5.15: suscripciones ta-raw-{source}-df, topic ta-dlq,
# bucket {GCP_PROJECT}-ta-lake, cuenta sa-dataflow. Job: ta-stream-{ENV}.
# Cuesta dinero mientras está activo: al terminar, `drain` (ARCHITECTURE.md §3.13).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

: "${GCP_PROJECT:?defina GCP_PROJECT (p. ej. export GCP_PROJECT=project-0568d9ba-848f-413c-88b)}"
GCP_REGION="${GCP_REGION:-us-central1}"
ENV="${ENV:-dev}"
JOB_NAME="ta-stream-${ENV}"
LAKE="gs://${GCP_PROJECT}-ta-lake"
SA="sa-dataflow@${GCP_PROJECT}.iam.gserviceaccount.com"
SOURCES=(reddit facebook instagram)
PYTHON="${PYTHON:-$(command -v python3.11 || command -v python3)}"

say()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
g()    { gcloud --project "$GCP_PROJECT" "$@"; }

active_job_ids() {
  g dataflow jobs list --region "$GCP_REGION" --status active \
    --filter "name=${JOB_NAME}" --format 'value(id)' 2>/dev/null || true
}

preflight() {
  local bad=0
  say "Proyecto ${GCP_PROJECT} · región ${GCP_REGION} · job ${JOB_NAME}"
  command -v gcloud >/dev/null || fail "gcloud no está instalado"
  local acct; acct="$(gcloud config get-value account 2>/dev/null || true)"
  [[ -n "$acct" ]] && say "✓ gcloud con la cuenta $acct" || { say "✗ gcloud sin cuenta (gcloud auth login)"; bad=1; }
  if "$PYTHON" -c 'import sys, setuptools, apache_beam as b; assert sys.version_info[:2] == (3, 11), sys.version; print(b.__version__)' >/tmp/ta_beam_v 2>&1; then
    say "✓ $PYTHON (3.11) con apache-beam $(cat /tmp/ta_beam_v)"
  else
    say "✗ hace falta Python 3.11 con src/streaming/requirements.txt ($(tail -1 /tmp/ta_beam_v))"; bad=1
  fi
  for s in "${SOURCES[@]}"; do
    g pubsub subscriptions describe "ta-raw-${s}-df" --format 'value(name)' >/dev/null 2>&1 \
      && say "✓ suscripción ta-raw-${s}-df" || { say "✗ falta ta-raw-${s}-df (Lote 1.3)"; bad=1; }
  done
  g pubsub topics describe ta-dlq --format 'value(name)' >/dev/null 2>&1 \
    && say "✓ topic ta-dlq" || { say "✗ falta ta-dlq"; bad=1; }
  gcloud storage buckets describe "$LAKE" --format 'value(name)' >/dev/null 2>&1 \
    && say "✓ bucket $LAKE" || { say "✗ falta $LAKE"; bad=1; }
  g iam service-accounts describe "$SA" --format 'value(email)' >/dev/null 2>&1 \
    && say "✓ cuenta $SA" || { say "✗ falta $SA"; bad=1; }
  if g compute networks describe default --format 'value(name)' >/dev/null 2>&1; then
    say "✓ red default (los workers la usan si no se pasa --network)"
  else
    say "! no hay red default: pase --network/--subnetwork a start"
  fi
  local ids; ids="$(active_job_ids)"
  if [[ -n "$ids" ]]; then say "! job activo: $ids (cuesta dinero; start se negará a lanzar otro)"
  else say "· sin jobs activos de ${JOB_NAME} (estado de ahorro)"; fi
  if g run services describe ta-stream-simulator --region "$GCP_REGION" --format 'value(status.url)' >/dev/null 2>&1; then
    say "· simulador ENCENDIDO (publica ~1,5 msg/s)"
  else
    say "· simulador apagado (simulator_running = false): sin él solo llegan los mensajes de verify_streaming.py check"
  fi
  [[ $bad == 0 ]] && say "Preflight OK" || { say "Preflight con fallas"; exit 1; }
}

start() {
  local machine="e2-standard-2" window=300 network="" subnetwork="" dlq_table=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --machine-type) machine="$2"; shift 2 ;;
      --window)       window="$2"; shift 2 ;;
      --network)      network="$2"; shift 2 ;;
      --subnetwork)   subnetwork="$2"; shift 2 ;;
      --dlq-table)    dlq_table="$2"; shift 2 ;;
      *) fail "opción desconocida: $1" ;;
    esac
  done
  [[ -z "$(active_job_ids)" ]] || fail "ya hay un job ${JOB_NAME} activo (status / drain)"
  local extra=()
  [[ -n "$network" ]] && extra+=(--network "$network")
  [[ -n "$subnetwork" ]] && extra+=(--subnetwork "$subnetwork")
  [[ -n "$dlq_table" ]] && extra+=(--dlq_table "$dlq_table")
  say "Lanzando ${JOB_NAME} (${machine}, 1 worker, Streaming Engine, ventana ${window} s)…"
  (cd src && "$PYTHON" -m streaming.pipeline \
      --runner DataflowRunner \
      --project "$GCP_PROJECT" --region "$GCP_REGION" \
      --job_name "$JOB_NAME" \
      --service_account_email "$SA" \
      --temp_location "${LAKE}/_dataflow/temp" \
      --staging_location "${LAKE}/_dataflow/staging" \
      --setup_file ./setup.py \
      --contracts_dir ../contracts \
      --enable_streaming_engine \
      --num_workers 1 --max_num_workers 1 --autoscaling_algorithm NONE \
      --worker_machine_type "$machine" \
      --labels project=trend-analyzer --labels "env=${ENV}" --labels owner=alejandro \
      --labels component=streaming \
      --output_root "$LAKE" --raw_window_seconds "$window" \
      "${extra[@]}")
  rm -rf src/ta_streaming.egg-info  # lo deja el empaquetado de --setup_file
  say "Enviado. El job tarda 3–5 min en llegar a Running: bash scripts/dataflow_job.sh status"
}

status() {
  g dataflow jobs list --region "$GCP_REGION" --status active \
    --format 'table(id, name, type, state, creationTime.date(tz=America/Bogota))'
  local id
  for id in $(active_job_ids); do
    say "Contadores del pipeline (${id}):"
    g dataflow metrics list "$id" --region "$GCP_REGION" --source user \
      --format 'table(name.name, scalar)' 2>/dev/null | grep -E 'NAME|valid_|dlq_|duplicates|unique|files|lines' || true
  done
}

stop() {
  local verb="$1" ids
  ids="$(active_job_ids)"
  [[ -n "$ids" ]] || { say "No hay jobs activos de ${JOB_NAME}. Nada que apagar."; return 0; }
  for id in $ids; do g dataflow jobs "$verb" "$id" --region "$GCP_REGION"; done
  say "Esperando a que termine (drain puede tardar unos minutos: cierra las ventanas abiertas)…"
  for _ in $(seq 1 60); do
    [[ -z "$(active_job_ids)" ]] && { say "✓ sin jobs activos de ${JOB_NAME}"; return 0; }
    sleep 15
  done
  fail "el job sigue activo tras 15 min; revise la consola de Dataflow"
}

case "${1:-}" in
  preflight) preflight ;;
  start)     shift; start "$@" ;;
  status)    status ;;
  drain)     stop drain ;;
  cancel)    stop cancel ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
