#!/usr/bin/env bash
# Construye la imagen de ta-stream-simulator, corre las pruebas en Python 3.11 y la publica
# en Artifact Registry (Lote 1.3). Imprime la referencia de la imagen al final.
#
# Uso (desde cualquier carpeta):
#   GCP_PROJECT=<proyecto> bash project/scripts/build_simulator_image.sh [cloudbuild|docker] [tag]
#
#   cloudbuild (por defecto): construye en Cloud Build; no necesita Docker local.
#   docker: construye con el Docker local y hace push (requiere `gcloud auth configure-docker`).
#   tag: por defecto, el commit corto de git (o la fecha y hora si no hay git).
set -euo pipefail

MODE="${1:-cloudbuild}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${GCP_PROJECT:?Define GCP_PROJECT (p. ej. project-0568d9ba-848f-413c-88b)}"
REGION="${GCP_REGION:-us-central1}"
REPO="${TA_ARTIFACT_REPO:-ta-images}"
IMAGE_NAME="stream-simulator"
TAG="${2:-$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || date -u +%Y%m%d-%H%M%S)}"
if [ -n "$(git -C "$PROJECT_DIR" status --porcelain -- src 2>/dev/null || true)" ] && [ -z "${2:-}" ]; then
  TAG="${TAG}-dirty-$(date -u +%H%M%S)"
fi
IMAGE="${REGION}-docker.pkg.dev/${GCP_PROJECT}/${REPO}/${IMAGE_NAME}:${TAG}"

cd "$PROJECT_DIR"
case "$MODE" in
  cloudbuild)
    gcloud builds submit . \
      --project="$GCP_PROJECT" \
      --region="$REGION" \
      --config=src/simulator/cloudbuild.yaml \
      --substitutions="_REGION=${REGION},_REPO=${REPO},_IMAGE=${IMAGE_NAME},_TAG=${TAG}"
    ;;
  docker)
    docker build -f src/simulator/Dockerfile --target test .
    docker build -f src/simulator/Dockerfile --target runtime -t "$IMAGE" .
    docker push "$IMAGE"
    ;;
  *)
    echo "Modo desconocido: $MODE (usa cloudbuild o docker)" >&2
    exit 2
    ;;
esac

echo
echo "Imagen publicada:"
echo "  simulator_image = \"${IMAGE}\""
