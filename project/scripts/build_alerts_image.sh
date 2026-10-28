#!/usr/bin/env bash
# Construye, prueba y publica la imagen del notificador de alertas (Lote 6.2, ARCHITECTURE.md §3.28).
#
#   bash scripts/build_alerts_image.sh [cloudbuild|docker] [--tag ETIQUETA] [--dry-run]
#
# Imagen: {GCP_REGION}-docker.pkg.dev/{GCP_PROJECT}/ta-images/alert-notifier:{etiqueta} y :latest.
# Etiqueta por defecto: h-{huella sha256 de los archivos de la imagen, 12 hex}: cambiar el código
# cambia la etiqueta (y Terraform ve el cambio); el mismo código no se reconstruye.
# El contexto es un directorio temporal solo con lo que usa el servicio (lista FILES): el
# notificador, su prueba y common/alert_message.py; ni el CLI ni las comprobaciones del validador.
# Al final imprime la línea `alerts_notifier_image = "…"` que lee validate_6_2.py.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # project/
mode="cloudbuild"
tag=""
dry=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    cloudbuild|docker) mode="$1" ;;
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
if [[ -z "$project" ]]; then
  echo "error: defina GCP_PROJECT (o gcloud config set project …)" >&2
  exit 2
fi

src="$here/src"
FILES=(
  alerts/__init__.py
  alerts/notifier.py
  alerts/tests/__init__.py
  alerts/tests/test_notifier.py
  common/__init__.py
  common/alert_message.py
)
for f in "${FILES[@]}" alerts/Dockerfile alerts/cloudbuild.yaml; do
  [[ -f "$src/$f" ]] || { echo "error: falta src/$f" >&2; exit 2; }
done

if [[ -z "$tag" ]]; then
  # Solo la huella del contenido: mismo código ⇒ misma etiqueta (no se reconstruye ni cambia el servicio).
  tag="h-$(cd "$src" && sha256sum "${FILES[@]}" alerts/Dockerfile alerts/cloudbuild.yaml | sha256sum | cut -c1-12)"
fi
image="${region}-docker.pkg.dev/${project}/${repo}/alert-notifier"

ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
(cd "$src" && cp --parents "${FILES[@]}" "$ctx/")
cp "$src/alerts/Dockerfile" "$src/alerts/cloudbuild.yaml" "$ctx/"

echo "imagen:   ${image}:${tag} (y :latest)"
echo "modo:     ${mode}"
echo "contexto: $(cd "$ctx" && find . -type f | wc -l) archivos"
if [[ "$dry" == 1 ]]; then
  echo "dry-run: no se construye nada"
  echo "alerts_notifier_image = \"${image}:${tag}\""
  exit 0
fi

start=$(date +%s)
if [[ "$mode" == "cloudbuild" ]]; then
  gcloud builds submit "$ctx" \
    --project "$project" \
    --region "$region" \
    --config "$ctx/cloudbuild.yaml" \
    --substitutions "_IMAGE=${image},_TAG=${tag}"
else
  docker build --target test -t alert-notifier-test "$ctx"
  docker build --target runtime -t "${image}:${tag}" -t "${image}:latest" "$ctx"
  gcloud auth configure-docker "${region}-docker.pkg.dev" --quiet
  docker push "${image}:${tag}"
  docker push "${image}:latest"
fi
echo "construida en $(( $(date +%s) - start )) s"
echo "alerts_notifier_image = \"${image}:${tag}\""
