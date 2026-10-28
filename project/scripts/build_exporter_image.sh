#!/usr/bin/env bash
# Construye, prueba y publica la imagen del exportador gcs-to-s3 (Lote 3.2, ARCHITECTURE.md §3.18).
#
#   bash scripts/build_exporter_image.sh [cloudbuild|docker|test] [--tag ETIQUETA] [--dry-run]
#
# test (Lote 7.1, CI): solo construye la etapa `test` con el Docker local (las pruebas corren dentro);
# no publica nada ni necesita GCP_PROJECT.
#
# Imagen: {GCP_REGION}-docker.pkg.dev/{GCP_PROJECT}/ta-images/gcs-to-s3:{etiqueta} y :latest.
# Etiqueta por defecto: {commit corto o fecha UTC}-{huella del contenido de src/exporter}, así
# que cambiar el código cambia la etiqueta aunque no haya commit (y Terraform ve el cambio).
# El contexto es un directorio temporal con exporter/, el Dockerfile, requirements.txt y
# cloudbuild.yaml: no usa las listas de permitidos de project/ (.dockerignore, .gcloudignore).
# Al final imprime la línea `exporter_image = "…"` que lee validate_3_2.py.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # project/
mode="cloudbuild"
tag=""
dry=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    cloudbuild|docker|test) mode="$1" ;;
    --tag) tag="${2:?falta la etiqueta}"; shift ;;
    --dry-run) dry=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "argumento desconocido: $1" >&2; exit 2 ;;
  esac
  shift
done

project="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
region="${GCP_REGION:-us-central1}"
repo="${TA_ARTIFACT_REPOSITORY:-ta-images}"
if [[ -z "$project" && "$mode" != "test" ]]; then
  echo "error: defina GCP_PROJECT (o gcloud config set project …)" >&2
  exit 2
fi

src="$here/src/exporter"
for f in Dockerfile requirements.txt cloudbuild.yaml cli.py; do
  [[ -f "$src/$f" ]] || { echo "error: falta src/exporter/$f" >&2; exit 2; }
done

if [[ -z "$tag" ]]; then
  base="$(git -C "$here" rev-parse --short HEAD 2>/dev/null || date -u +%Y%m%d-%H%M)"
  digest="$(cd "$src" && find . -type f -not -path '*/__pycache__/*' -not -name '*.pyc' -print0 \
    | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -c1-8)"
  tag="${base}-${digest}"
fi
image="${region}-docker.pkg.dev/${project}/${repo}/gcs-to-s3"

ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
mkdir -p "$ctx/exporter"
(cd "$src" && find . -type f -not -path '*/__pycache__/*' -not -name '*.pyc' -print0 \
  | xargs -0 -I{} cp --parents {} "$ctx/exporter/")
cp "$src/Dockerfile" "$src/requirements.txt" "$src/cloudbuild.yaml" "$ctx/"

echo "imagen:   ${image}:${tag} (y :latest)"
echo "modo:     ${mode}"
echo "contexto: $(cd "$ctx" && find . -type f | wc -l) archivos"
if [[ "$dry" == 1 ]]; then
  echo "dry-run: no se construye nada"
  echo "exporter_image = \"${image}:${tag}\""
  exit 0
fi

start=$(date +%s)
if [[ "$mode" == "test" ]]; then
  docker build --target test -t gcs-to-s3-test "$ctx"
  echo "pruebas de la imagen OK en $(( $(date +%s) - start )) s (no se publica)"
  exit 0
fi
if [[ "$mode" == "cloudbuild" ]]; then
  gcloud builds submit "$ctx" \
    --project "$project" \
    --region "$region" \
    --config "$ctx/cloudbuild.yaml" \
    --substitutions "_IMAGE=${image},_TAG=${tag}"
else
  docker build --target test -t gcs-to-s3-test "$ctx"
  docker build --target runtime -t "${image}:${tag}" -t "${image}:latest" "$ctx"
  gcloud auth configure-docker "${region}-docker.pkg.dev" --quiet
  docker push "${image}:${tag}"
  docker push "${image}:latest"
fi
echo "construida en $(( $(date +%s) - start )) s"
echo "export TA_EXPORTER_IMAGE=${image}:${tag}"
echo "exporter_image = \"${image}:${tag}\""
