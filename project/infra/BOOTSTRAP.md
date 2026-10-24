# Bootstrap de Terraform (Lote 1.0 · HU-046)

Crea lo mínimo para que el resto de la infraestructura use **estado remoto** (ADR-14):

| Nube | Qué crea el bootstrap | Nombre |
|---|---|---|
| GCP | APIs del proyecto, bucket de estado con versionado, presupuesto con aviso por correo | `project-0568d9ba-848f-413c-88b-ta-tfstate` |
| AWS | Bucket S3 de estado (versionado, SSE, solo TLS), tabla DynamoDB de bloqueo, presupuesto con aviso por correo | `ta-dev-648617469176-tfstate`, `ta-dev-tflock` |

El bootstrap usa **estado local** (el bucket aún no existe). Después, `environments/dev/` de cada nube usa el bucket remoto.

## 0. Requisitos

- Terraform ≥ 1.6, `gcloud` y AWS CLI.
- Cuenta de Alejandro como Owner/Editor del proyecto GCP y vinculada a la cuenta de facturación (confirmar con `gcloud billing projects describe project-0568d9ba-848f-413c-88b`).
- Perfil de AWS CLI configurado (`aws configure`) con permisos sobre S3, DynamoDB y Budgets.

## 1. Validar sin credenciales

```bash
bash project/infra/validate.sh
```

## 2. GCP

```bash
gcloud auth login
gcloud config set project project-0568d9ba-848f-413c-88b
gcloud auth application-default login
gcloud auth application-default set-quota-project project-0568d9ba-848f-413c-88b
gcloud billing accounts list          # copia el ID de la cuenta de facturación

cd project/infra/gcp/bootstrap
cp terraform.tfvars.example terraform.tfvars   # edita billing_account_id, budget_amount, alert_emails
terraform init
terraform plan
terraform apply
```

Si la primera ejecución falla porque `serviceusage` o `cloudresourcemanager` aún no están activos, habilítalos a mano una vez y repite:

```bash
gcloud services enable serviceusage.googleapis.com cloudresourcemanager.googleapis.com
```

## 3. AWS

```bash
cd project/infra/aws/bootstrap
cp terraform.tfvars.example terraform.tfvars   # edita budget_usd y alert_emails
terraform init
terraform plan
terraform apply
```

El provider tiene `allowed_account_ids`: si el perfil apunta a otra cuenta, se detiene.

## 4. Conectar los entornos al estado remoto

```bash
cd project/infra/gcp/environments/dev
cp backend.hcl.example backend.hcl && cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform plan        # sin cambios: aún no hay recursos

cd ../../../aws/environments/dev
cp backend.hcl.example backend.hcl && cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform plan
```

## 5. Notas

- Los buckets y la tabla tienen `prevent_destroy`. El estado del bootstrap queda local (no se versiona); si se pierde, se recupera con `terraform import` de esos recursos.
- La moneda del presupuesto de GCP debe coincidir con la de la cuenta de facturación (por defecto `COP`; cámbiala en `terraform.tfvars` si no).
- El bloqueo de AWS usa DynamoDB para respetar Terraform ≥ 1.6 (§5.1). Con ≥ 1.10 se podría usar `use_lockfile = true` y eliminar la tabla.
- Aún no se crean service accounts ni accesos de Miguel y Camila: llegan con el Lote 1.3 o cuando los necesiten sus lotes.
