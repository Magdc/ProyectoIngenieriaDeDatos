#!/usr/bin/env bash
# Llave HMAC de anonimización en Secret Manager (Lote 2.2, §5.1, ADR-06).
#
# Terraform crea el secreto `ta-hmac-key` (sin versiones) y da acceso a sa-dataflow.
# El VALOR no pasa por Terraform ni por el repositorio: este script lo genera y lo
# sube por stdin, sin imprimirlo ni escribirlo en disco.
#
#   bash scripts/hmac_key.sh status   # solo lectura: secreto, versiones y quién puede leerlo
#   bash scripts/hmac_key.sh init     # crea la primera versión (se niega si ya hay una)
#
# Cambiar la llave cambia TODOS los user_hash (stream e histórico dejan de cruzar).
# No hay comando de rotación: si hiciera falta, se decide aparte y se reprocesa.

set -euo pipefail
set +x  # nunca trazar este script

PROJECT="${GCP_PROJECT:-}"
SECRET="${TA_HMAC_SECRET_NAME:-ta-hmac-key}"

die() { echo "ERROR: $*" >&2; exit 2; }
[[ -n "$PROJECT" ]] || die "defina GCP_PROJECT (p. ej. export GCP_PROJECT=project-0568d9ba-848f-413c-88b)"
command -v gcloud >/dev/null || die "falta gcloud"

secret_exists() {
  gcloud secrets describe "$SECRET" --project "$PROJECT" --format='value(name)' >/dev/null 2>&1
}

enabled_versions() {
  gcloud secrets versions list "$SECRET" --project "$PROJECT" \
    --filter='state=ENABLED' --format='value(name)' 2>/dev/null | wc -l | tr -d ' '
}

cmd_status() {
  if ! secret_exists; then
    echo "Secreto $SECRET: NO EXISTE en $PROJECT (aplique Terraform del Lote 2.2)."
    exit 1
  fi
  echo "Secreto $SECRET: existe en $PROJECT"
  local n; n="$(enabled_versions)"
  echo "Versiones habilitadas: $n"
  echo "Con acceso de lectura (roles/secretmanager.secretAccessor):"
  gcloud secrets get-iam-policy "$SECRET" --project "$PROJECT" \
    --flatten='bindings[].members' \
    --filter='bindings.role=roles/secretmanager.secretAccessor' \
    --format='value(bindings.members)' | sed 's/^/  /'
  if [[ "$n" == "0" ]]; then
    echo "Falta la llave: corra 'bash scripts/hmac_key.sh init'."
    exit 1
  fi
}

cmd_init() {
  secret_exists || die "el secreto $SECRET no existe: aplique primero Terraform (infra/gcp/environments/dev)"
  local n; n="$(enabled_versions)"
  if [[ "$n" != "0" ]]; then
    echo "El secreto ya tiene $n versión(es) habilitada(s): no se crea otra."
    echo "Una llave nueva cambiaría todos los user_hash. Si de verdad hace falta, hágalo a mano y reprocese."
    exit 1
  fi
  command -v python3 >/dev/null || die "falta python3 para generar la llave"
  # 32 bytes aleatorios en hex (64 caracteres), directo a stdin de gcloud.
  python3 -c 'import secrets, sys; sys.stdout.write(secrets.token_hex(32))' \
    | gcloud secrets versions add "$SECRET" --project "$PROJECT" --data-file=- >/dev/null
  echo "Versión creada en $SECRET (el valor no se mostró ni se guardó en disco)."
  cmd_status
}

case "${1:-}" in
  status) cmd_status ;;
  init) cmd_init ;;
  *) echo "Uso: bash scripts/hmac_key.sh {status|init}" >&2; exit 2 ;;
esac
