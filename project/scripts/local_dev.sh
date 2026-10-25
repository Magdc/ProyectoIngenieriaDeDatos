#!/usr/bin/env bash
# Entorno local de TrendAnalyzer (Lote 1.5, HU-047). Guía: LOCAL_DEV.md.
#
#   bash scripts/local_dev.sh preflight        comprueba Docker, puertos y archivos (no cambia nada)
#   bash scripts/local_dev.sh up               emulador + topics + simulador, y espera a que esté sano
#   bash scripts/local_dev.sh status           contenedores, /healthz y cursor
#   bash scripts/local_dev.sh verify [seg]     consumidor de validación (por defecto 60 s)
#   bash scripts/local_dev.sh restart-check    el seq continúa tras reiniciar el simulador
#   bash scripts/local_dev.sh logs             logs del simulador
#   bash scripts/local_dev.sh down             apaga todo y conserva el cursor
#   bash scripts/local_dev.sh reset            apaga todo y borra el cursor (seq vuelve a 0)
#
# No usa credenciales de GCP ni de AWS. Se puede llamar desde cualquier carpeta.
set -euo pipefail

cd "$(dirname "$0")/.."            # project/
SIM_PORT="${SIM_PORT:-8080}"
EMU_PORT="${PUBSUB_EMULATOR_PORT:-8085}"
DC=(docker compose)

ok()   { printf '  [OK]    %s\n' "$*"; }
warn() { printf '  [AVISO] %s\n' "$*"; }
bad()  { printf '  [FALLA] %s\n' "$*"; FAILED=1; }

port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
ours_running() { [ -n "$("${DC[@]}" ps -q "$1" 2>/dev/null)" ]; }

preflight() {
  FAILED=0
  echo "== Preflight del entorno local"
  if docker info >/dev/null 2>&1; then ok "Docker responde ($(docker version --format '{{.Server.Version}}' 2>/dev/null))"
  else bad "Docker no responde: abra Docker Desktop (con integración WSL) o inicie el servicio docker"; fi
  if docker compose version >/dev/null 2>&1; then ok "$(docker compose version)"
  else bad "falta 'docker compose' (plugin v2)"; fi
  for f in docker-compose.yml src/simulator/Dockerfile contracts/envelope.v1.schema.json \
           scripts/verify_ingest.py scripts/local_pubsub_init.py local/validator.Dockerfile; do
    [ -f "$f" ] && ok "existe $f" || bad "falta $f"
  done
  for triple in "pubsub-emulator:$EMU_PORT:PUBSUB_EMULATOR_PORT" "simulator:$SIM_PORT:SIM_PORT"; do
    IFS=: read -r svc port var <<<"$triple"
    if port_busy "$port"; then
      if ours_running "$svc"; then ok "puerto $port en uso por $svc (ya está arriba)"
      else bad "puerto $port ocupado por otro proceso: libérelo o use $var=<otro puerto>"; fi
    else ok "puerto $port libre"; fi
  done
  if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    warn "GOOGLE_APPLICATION_CREDENTIALS está definido en su shell; los contenedores no lo usan"
  fi
  [ "$FAILED" = 0 ] && echo "Preflight: OK" || { echo "Preflight: FALLA"; return 1; }
}

wait_healthy() {
  local svc="$1" timeout="${2:-240}" cid status t=0
  while [ "$t" -lt "$timeout" ]; do
    cid="$("${DC[@]}" ps -q "$svc" 2>/dev/null || true)"
    if [ -n "$cid" ]; then
      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo desconocido)"
      case "$status" in
        healthy) ok "$svc sano"; return 0 ;;
        exited|dead) bad "$svc terminó: revise 'bash scripts/local_dev.sh logs'"; return 1 ;;
      esac
    fi
    sleep 3; t=$((t + 3))
  done
  bad "$svc no quedó sano en ${timeout}s"; return 1
}

status() {
  "${DC[@]}" ps
  echo "== /healthz (desde el host)"
  python3 - "$SIM_PORT" <<'EOF' || true
import sys, urllib.request
try:
    print(urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=3).read().decode())
except Exception as exc:
    print(f"sin respuesta: {exc}")
EOF
  echo "== Cursor (volumen sim-state)"
  "${DC[@]}" run --rm --no-deps -T --entrypoint sh validator -c \
    'for f in $(find /state -name "*.json" | sort); do echo "$f: $(cat "$f")"; done' 2>/dev/null || warn "no se pudo leer el volumen"
}

up() {
  preflight
  echo "== Construyendo y levantando (la primera vez tarda: corre las pruebas de la imagen)"
  "${DC[@]}" up -d --build simulator
  FAILED=0
  wait_healthy simulator 300 || { "${DC[@]}" logs --tail 50 pubsub-init simulator; exit 1; }
  echo "Listo. Siguiente: bash scripts/local_dev.sh verify"
}

verify() {
  local dur="${1:-60}"
  mkdir -p evidencia
  "${DC[@]}" run --rm validator measure --duration "$dur" --validate yes --cursor local \
    --json-out evidencia/local_measure.json
}

restart_check() {
  local before=60 after=60 settle=10 code
  mkdir -p evidencia
  echo "== restart-check: ${before}s antes, reinicio del simulador, ${after}s después"
  "${DC[@]}" run --rm -T validator restart-check --before "$before" --after "$after" \
    --restart wait --restart-wait 40 --validate yes --cursor local \
    --json-out evidencia/local_restart.json &
  local pid=$!
  sleep $((before + settle))
  echo "== reiniciando el simulador (SIGTERM → guarda el cursor → arranca)"
  "${DC[@]}" restart simulator
  set +e; wait "$pid"; code=$?; set -e
  return "$code"
}

case "${1:-}" in
  preflight)      preflight ;;
  up)             up ;;
  status)         status ;;
  verify)         verify "${2:-60}" ;;
  restart-check)  restart_check ;;
  logs)           "${DC[@]}" logs -f --tail 100 simulator ;;
  down)           "${DC[@]}" --profile tools down ;;
  reset)          "${DC[@]}" --profile tools down -v ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
