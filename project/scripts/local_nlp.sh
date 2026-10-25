#!/usr/bin/env bash
# Pipeline local con NLP (Lote 2.3): el servicio dataflow-local del Lote 2.1 con la imagen
# dataflow-nlp (target local). Suma docker-compose.nlp.yml a los dos Compose existentes sin
# modificarlos. Sin credenciales de GCP; la construcción necesita salida a Hugging Face.
#
#   bash scripts/local_nlp.sh build    # construye ta-dataflow-nlp-local:local (descarga los modelos, ~2,5 GB)
#   bash scripts/local_nlp.sh up       # emulador + simulador + dataflow-local con NLP
#   bash scripts/local_nlp.sh logs     # logs de dataflow-local
#   bash scripts/local_nlp.sh smoke    # prueba de humo de la imagen (modelos y etiquetas)
#   bash scripts/local_nlp.sh down     # para dataflow-local (deja el emulador y el simulador)
#   bash scripts/local_nlp.sh restore  # vuelve dataflow-local a la imagen del Lote 2.1

set -euo pipefail
cd "$(dirname "$0")/.."

FILES=(-f docker-compose.yml -f docker-compose.dataflow.yml -f docker-compose.nlp.yml)
BASE=(-f docker-compose.yml -f docker-compose.dataflow.yml)
dc() { docker compose "${FILES[@]}" --profile dataflow "$@"; }

case "${1:-}" in
  build) dc build dataflow-local ;;
  up) dc up -d --build dataflow-local; dc ps ;;
  logs) dc logs -f --tail=200 dataflow-local ;;
  smoke) docker run --rm --entrypoint python ta-dataflow-nlp-local:local /opt/ta-nlp/image_smoke.py ;;
  down) dc stop dataflow-local ;;
  restore) docker compose "${BASE[@]}" --profile dataflow up -d --force-recreate dataflow-local ;;
  *) echo "Uso: bash scripts/local_nlp.sh {build|up|logs|smoke|down|restore}" >&2; exit 2 ;;
esac
