#!/usr/bin/env bash
# Etapa 2 del histórico (Lote 3.1, HU-024): NLP en lote dentro de la imagen de NLP local
# del Lote 2.3 (ta-dataflow-nlp-local:local), con el código montado en /work y sin
# credenciales. Reanudable: si se corta, se vuelve a lanzar y sigue donde iba.
#
#   bash scripts/historical_nlp.sh                       # etapa nlp con los valores por defecto
#   bash scripts/historical_nlp.sh --root .local-s3/_smoke --seed 3199
#   bash scripts/historical_nlp.sh doctor                # NLP real sobre 3 filas sintéticas
#   bash scripts/historical_nlp.sh doctor --nlp-bench 160   # filas/s y tamaño de lote por configuración
#   bash scripts/historical_nlp.sh stop                  # detiene una corrida en curso
#
# Variables: TA_HIST_NLP_IMAGE (imagen), TA_HIST_NLP_CONTAINER (nombre), TA_NLP_THREADS.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${TA_HIST_NLP_IMAGE:-ta-dataflow-nlp-local:local}"
NAME="${TA_HIST_NLP_CONTAINER:-ta-hist-nlp}"

if [[ "${1:-}" == "stop" ]]; then
  docker rm -f "$NAME" >/dev/null 2>&1 && echo "detenido: $NAME" || echo "no había contenedor $NAME"
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker no responde: abra Docker Desktop (con la integración de WSL)." >&2
  exit 2
fi
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "No está la imagen $IMAGE. Constrúyala una vez con: bash scripts/local_nlp.sh build (10-15 min)." >&2
  exit 2
fi
if docker ps -a --format '{{.Names}}' | grep -x "$NAME" >/dev/null; then
  echo "Ya existe el contenedor $NAME (¿otra corrida en curso?). Si no, bórrelo: docker rm -f $NAME" >&2
  exit 2
fi

# Rutas del host dentro de project/ → /work/… (el contenedor solo ve project/).
translate() {
  local p="$1"
  if [[ "$p" == /* ]]; then
    if [[ "$p" == "$PROJECT_DIR"* ]]; then
      echo "/work${p#"$PROJECT_DIR"}"
    else
      echo "La ruta $p está fuera de project/ y el contenedor no la ve." >&2
      return 1
    fi
  else
    echo "$p"
  fi
}

if [[ "${1:-}" == "doctor" ]]; then
  shift
  cmd=(python scripts/load_historical.py doctor --nlp-only --nlp-run)
else
  cmd=(python scripts/load_historical.py nlp)
fi
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --root|--json-out)
      args+=("$1" "$(translate "$2")"); shift 2 ;;
    --root=*|--json-out=*)
      args+=("${1%%=*}=$(translate "${1#*=}")"); shift ;;
    *)
      args+=("$1"); shift ;;
  esac
done
if [[ -n "${TA_HIST_ROOT:-}" ]]; then
  root_in="$(translate "$TA_HIST_ROOT")"
else
  root_in=""
fi

env_args=(-e HOME=/tmp -e PYTHONPATH=/work/src -e PYTHONUNBUFFERED=1)
[[ -n "${TA_NLP_THREADS:-}" ]] && env_args+=(-e "TA_NLP_THREADS=$TA_NLP_THREADS")
[[ -n "${TA_HIST_NLP_WORKERS:-}" ]] && env_args+=(-e "TA_HIST_NLP_WORKERS=$TA_HIST_NLP_WORKERS")
[[ -n "$root_in" ]] && env_args+=(-e "TA_HIST_ROOT=$root_in")
[[ -n "${HIST_SEED:-}" ]] && env_args+=(-e "HIST_SEED=$HIST_SEED")

echo "imagen: $IMAGE · contenedor: $NAME · $(date -u +%Y-%m-%dT%H:%M:%SZ)"
cd "$PROJECT_DIR"
exec docker run --rm --init --name "$NAME" --user "$(id -u):$(id -g)" \
  "${env_args[@]}" -v "$PROJECT_DIR":/work -w /work \
  --entrypoint "" "$IMAGE" "${cmd[@]}" ${args[@]+"${args[@]}"}
