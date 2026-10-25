#!/usr/bin/env bash
# Construye y publica la imagen de los workers con NLP (Lote 2.3, HU-022).
#
#   bash scripts/build_nlp_image.sh [cloudbuild|docker] [TAG]
#
#   cloudbuild (por defecto): compila en Cloud Build y publica en Artifact Registry
#                             {región}-docker.pkg.dev/{proyecto}/ta-images/dataflow-nlp:{TAG} y :latest
#   docker                  : compila con el Docker local (target runtime) y publica igual
#                             (necesita gcloud auth configure-docker {región}-docker.pkg.dev)
#
# TAG por defecto: commit corto de git. La construcción descarga los modelos de Hugging Face y
# corre la prueba de humo (image_smoke.py); si falla, no se publica nada.
# Revisiones de los modelos: SENTIMENT_REVISION y EMOTION_REVISION (por defecto main; el commit
# exacto queda en models.json y en nlp_model_version).

set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-cloudbuild}"
TAG="${2:-$(git rev-parse --short HEAD 2>/dev/null || date -u +%Y%m%d%H%M%S)}"
PROJECT="${GCP_PROJECT:?defina GCP_PROJECT}"
REGION="${GCP_REGION:-us-central1}"
REPO="${TA_ARTIFACT_REPOSITORY:-ta-images}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/dataflow-nlp"
CONTEXT="src/streaming/nlp_image"
SREV="${SENTIMENT_REVISION:-main}"
EREV="${EMOTION_REVISION:-main}"

echo "Imagen: ${IMAGE}:${TAG} (y :latest) · modo ${MODE}"
case "$MODE" in
  cloudbuild)
    gcloud builds submit "$CONTEXT" --project "$PROJECT" --region "$REGION" \
      --config "$CONTEXT/cloudbuild.yaml" \
      --substitutions "_IMAGE=${IMAGE},_TAG=${TAG},_SENTIMENT_REVISION=${SREV},_EMOTION_REVISION=${EREV}"
    ;;
  docker)
    docker build --target runtime \
      --build-arg "SENTIMENT_REVISION=${SREV}" --build-arg "EMOTION_REVISION=${EREV}" \
      -t "${IMAGE}:${TAG}" -t "${IMAGE}:latest" "$CONTEXT"
    docker push "${IMAGE}:${TAG}"
    docker push "${IMAGE}:latest"
    ;;
  *) echo "Uso: bash scripts/build_nlp_image.sh [cloudbuild|docker] [TAG]" >&2; exit 2 ;;
esac

echo
echo "Publicada. Para lanzar el job con esta versión exacta:"
echo "  export TA_NLP_IMAGE=${IMAGE}:${TAG}"
echo "  bash scripts/dataflow_job.sh start"
