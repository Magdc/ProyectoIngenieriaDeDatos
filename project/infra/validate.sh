#!/usr/bin/env bash
# Validación local de Terraform sin credenciales ni backend (HU-046).
# Uso: bash project/infra/validate.sh
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v terraform >/dev/null 2>&1; then
  echo "terraform no está instalado (se requiere >= 1.6)" >&2
  exit 1
fi

echo "== terraform fmt -check =="
terraform fmt -check -recursive

DIRS=(
  gcp/bootstrap
  gcp/environments/dev
  aws/bootstrap
  aws/environments/dev
)

for d in "${DIRS[@]}"; do
  echo "== validate: $d =="
  terraform -chdir="$d" init -backend=false -input=false >/dev/null
  terraform -chdir="$d" validate
done

echo "OK"
