#!/usr/bin/env bash
# Lote 2.1 · Pipeline de streaming en local (DirectRunner + emulador de Pub/Sub).
#
# Se apoya en el entorno del Lote 1.5 (docker-compose.yml) y le suma
# docker-compose.dataflow.yml. Sin credenciales de GCP ni de AWS.
#
#   bash scripts/local_dataflow.sh preflight   # comprueba requisitos (no cambia nada)
#   bash scripts/local_dataflow.sh up          # emulador + simulador + dataflow-local
#   bash scripts/local_dataflow.sh status      # contenedores y resumen del lake local
#   bash scripts/local_dataflow.sh check [s]   # centinela + defectos → raw/, dlq/, ta-dlq (timeout s, 240)
#   bash scripts/local_dataflow.sh report      # resumen de raw/ y dlq/ (solo lectura)
#   bash scripts/local_dataflow.sh logs        # logs del pipeline
#   bash scripts/local_dataflow.sh down        # para solo dataflow-local
#   bash scripts/local_dataflow.sh down-all    # para todo el entorno local (conserva el lake)
#   bash scripts/local_dataflow.sh reset-lake  # borra el lake local
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

export LOCAL_UID="${LOCAL_UID:-$(id -u)}"
export LOCAL_GID="${LOCAL_GID:-$(id -g)}"
export TA_LOCAL_LAKE="${TA_LOCAL_LAKE:-./.local-lake}"
DC=(docker compose -f docker-compose.yml -f docker-compose.dataflow.yml)

say()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

preflight() {
  local ok=1
  command -v docker >/dev/null || { say "✗ docker no está instalado"; ok=0; }
  docker info >/dev/null 2>&1 || { say "✗ el daemon de Docker no responde (¿Docker Desktop abierto?)"; ok=0; }
  docker compose version >/dev/null 2>&1 || { say "✗ falta docker compose v2"; ok=0; }
  for f in docker-compose.yml docker-compose.dataflow.yml local/dataflow.Dockerfile \
           src/streaming/requirements.txt src/streaming/pipeline.py contracts/envelope.v1.schema.json; do
    [[ -f "$f" ]] || { say "✗ falta $f"; ok=0; }
  done
  if [[ $ok == 1 ]]; then
    local services
    services="$("${DC[@]}" --profile dataflow --profile tools config --services 2>/dev/null || true)"
    for s in pubsub-emulator pubsub-init simulator dataflow-local streaming-check; do
      grep -qx "$s" <<<"$services" || { say "✗ el Compose combinado no tiene el servicio $s"; ok=0; }
    done
  fi
  mkdir -p "$TA_LOCAL_LAKE" evidencia 2>/dev/null && [[ -w "$TA_LOCAL_LAKE" ]] \
    || { say "✗ no se puede escribir en $TA_LOCAL_LAKE"; ok=0; }
  if [[ $ok == 1 ]]; then
    say "✓ preflight OK · lake local: $TA_LOCAL_LAKE · uid:gid $LOCAL_UID:$LOCAL_GID"
    say "  Estado actual del entorno:"
    "${DC[@]}" --profile dataflow ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' 2>/dev/null || true
  else
    exit 1
  fi
}

up() {
  mkdir -p "$TA_LOCAL_LAKE" evidencia
  say "Construyendo y levantando emulador, simulador y dataflow-local…"
  "${DC[@]}" --profile dataflow up -d --build
  say "Esperando a que arranque el pipeline (hasta 180 s)…"
  for _ in $(seq 1 90); do
    if "${DC[@]}" logs --no-color dataflow-local 2>/dev/null | grep -q '"event": "pipeline_start"'; then
      say "✓ pipeline en marcha. Los archivos de raw/ aparecen al cerrar cada ventana" \
          "(${TA_LOCAL_RAW_WINDOW:-60} s) en $TA_LOCAL_LAKE."
      return 0
    fi
    if "${DC[@]}" ps --status exited --services 2>/dev/null | grep -qx dataflow-local; then
      "${DC[@]}" logs --no-color --tail 50 dataflow-local
      fail "dataflow-local terminó al arrancar (logs arriba)"
    fi
    sleep 2
  done
  "${DC[@]}" logs --no-color --tail 50 dataflow-local
  fail "el pipeline no registró pipeline_start en 180 s"
}

status() {
  "${DC[@]}" --profile dataflow ps
  report || true
}

report() {
  "${DC[@]}" run --rm streaming-check report --root /lake \
    --json-out /evidencia/local_streaming_report.json
}

check() {
  local timeout="${1:-240}"
  "${DC[@]}" run --rm streaming-check check --root /lake --timeout "$timeout" --poll 10 \
    --json-out /evidencia/local_streaming_check.json
}

case "${1:-}" in
  preflight)  preflight ;;
  up)         up ;;
  status)     status ;;
  report)     report ;;
  check)      check "${2:-240}" ;;
  logs)       "${DC[@]}" logs -f --tail 100 dataflow-local ;;
  down)       "${DC[@]}" --profile dataflow rm -sf dataflow-local ;;
  down-all)   "${DC[@]}" --profile dataflow --profile tools down ;;
  reset-lake) say "Borrando $TA_LOCAL_LAKE"; rm -rf "${TA_LOCAL_LAKE:?}"; mkdir -p "$TA_LOCAL_LAKE" ;;
  *) sed -n '2,17p' "$0"; exit 2 ;;
esac
