# Validación del Lote 3.2 · HU-006, HU-025

> Federación OIDC GCP → AWS (`sa-exporter` asume `ta-gcp-exporter-role` con su ID token de Google,
> sin llaves) y exportador `gcs-to-s3`: un Cloud Run Job que copia `gs://…-ta-lake/enriched/` a
> `s3://ta-dev-{cuenta}-lake/raw/origin=stream/`, disparado por Cloud Scheduler cada 15 min. El
> Scheduler se crea **en pausa** (estado de ahorro). Contrato en `docs/ARCHITECTURE.md` §3.18 y §5.6;
> guía del módulo en `project/src/exporter/EXPORTER.md`.
>
> **Automático:** `python project/scripts/validate_3_2.py <parte>` desde la raíz del repo, con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_3_2.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-006 · Proveedor OIDC de Google en AWS (integrado: `accounts.google.com`, no se crea recurso) | C5 (confianza con `Federated = accounts.google.com`), D2 |
| HU-006 · `sa-exporter` asume `ta-gcp-exporter-role` con `AssumeRoleWithWebIdentity` | D1–D2 (desde su máquina, token real de `sa-exporter`), E5 (desde Cloud Run) |
| HU-006 · El rol solo escribe y lista en `raw/origin=stream/` y cifra con la llave del lake | C5, C5b (8 casos simulados), D2 (sondas reales A2–A7) |
| HU-025 · Cloud Run Job `ta-gcs-to-s3` lanzado por Cloud Scheduler cada 15 min | E2–E4, E10 (disparo real con la identidad del Scheduler) |
| HU-025 · Lee solo `enriched/` y usa `export-state/watermark.json`; lo actualiza solo tras subir con éxito | C8 (IAM real), E5 (G1–G6 como `sa-exporter`), B1 (`test_exp_core.py`), E6–E8 |
| HU-025 · Escribe `raw/origin=stream/source=…/dt=…/hr=…/*.parquet` con SSE-KMS | D4–D5, E7 (V1–V4), E9b |
| HU-025 · Idempotente: omite lo que ya está con el mismo tamaño | D6, E8 |
| HU-025 · Datos de stream e histórico en `s3://…/raw/` (y "Listo cuando" de la Fase 3) | F1–F4 |
| Pendiente del 2.1/3.1: `sa-exporter` deja de leer todo el lake | A4, C6–C8 |

## Antes de empezar

1. Copie los archivos del lote en sus rutas (`project/…` y `docs/…`). Incluye una edición de
   `project/infra/gcp/environments/dev/platform.tf`: se quita `exporter_lake_read` (el `objectViewer`
   sin condición de `sa-exporter`); `exporter.tf` pone en su lugar uno con condición.
2. Terminal en WSL, **entrando de nuevo a la carpeta** (tras copiar desde Windows la terminal puede
   quedar en una carpeta "fantasma" y Python falla con `FileNotFoundError`):
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   ```
   El venv ya trae lo que usa el lote: `boto3`, `google-cloud-storage`, `google-auth` y `pyarrow`.
3. Credenciales (como en el 3.1): perfil de AWS CLI de Terraform (`aws sts get-caller-identity`) y,
   en GCP, `gcloud auth login` **y** `gcloud auth application-default login` con la misma cuenta
   (la parte D impersona a `sa-exporter` con ADC y, si falla, con `gcloud`).
   ```bash
   export GCP_PROJECT=project-0568d9ba-848f-413c-88b
   gcloud config get-value account
   gcloud auth application-default print-access-token > /dev/null && echo adc-ok
   ```
4. `backend.hcl` y `terraform.tfvars` en `project/infra/aws/environments/dev` y en
   `project/infra/gcp/environments/dev` (no se versionan; ya existen en su máquina). El validador
   escribe además dos archivos que **tampoco se versionan** (`*.auto.tfvars`, en `infra/.gitignore`):
   `infra/aws/environments/dev/gcp_exporter.auto.tfvars` (uniqueId y correo de `sa-exporter`) y
   `infra/gcp/environments/dev/aws_exporter.auto.tfvars` (rol, bucket y llave de AWS, su cuenta para
   el token y la imagen del job). Los `*.example` muestran el formato.
5. No corra nada más largo mientras copia archivos del lote, ni al revés.

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada | Nube, Docker | 0 |
| C | Credenciales de AWS y GCP | Simulador, Dataflow, Docker | 0 (IAM) |
| D | Lo de C aplicado | Simulador, Dataflow, Docker | ~0 (20 PUT y unos LIST en S3) |
| E | Lo de C aplicado; Cloud Build | **Simulador y Dataflow** (`enriched/` ya tiene datos de lotes anteriores), Docker | Cloud Build (centavos), 5–6 ejecuciones del job (≥ 1 min cada una, ~0,01 USD), unos miles de PUT (~0,01 USD) |
| F | Credenciales de AWS | GCP, Docker | ~0 (LIST y 2 GET) |

Compruebe el estado con `python project/scripts/status.py` y `python project/scripts/validate_3_2.py status`.
**Este lote no necesita el simulador ni Dataflow**: si `status.py` los muestra encendidos, apáguelos
(`python project/scripts/off.py`). Con el stream encendido E8 y E9 lo toleran, pero cobran sin aportar.
Si los contenedores `trendanalyzer-local-*` siguen encendidos (A8), no estorban a este lote pero comen
CPU: `bash project/scripts/local_dataflow.sh down-all`.

**Estado de ahorro del lote:** el Scheduler `ta-gcs-to-s3-cada-15-min` se crea **pausado**; el job no
cobra mientras no corre. E10 lo reanuda solo para un disparo y lo vuelve a pausar (también si algo
falla). Encenderlo de forma permanente lo deciden los compañeros (≈ 0,16 USD/día medido en E12).

---

## Parte A · Preflight del código

```bash
python project/scripts/validate_3_2.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows/PowerShell: repita desde WSL. Las demás partes ni arrancan fuera de WSL |
| A1 | Python 3.11; `boto3`, `botocore`, `google-cloud-storage`, `google-auth` importan; `pyarrow` (AVISO si falta: lo usa F) |
| A2 | Los 32 archivos del lote presentes |
| A3 | Los 10 archivos de lotes anteriores que usa (raíces de Terraform, `lake.tf`, `common/enriched_row.py`, `status.py`, `off.py`, `validate.sh`) |
| A4 | `platform.tf` ya **no** declara `exporter_lake_read` y `exporter.tf` declara `exporter_lake_enriched` |
| A5 | `gcloud`, `terraform` (con versión), `aws`, `bash`; `docker` es opcional |
| A6 | `export_gcs_to_s3.py doctor --offline`: X1–X5 sin FALLA (X3 en AVISO es normal: la configuración la pone el validador) |
| A7 | `*.auto.tfvars` en `infra/.gitignore` |
| A8 | Ningún contenedor `trendanalyzer-local-*` ni de NLP encendido (AVISO si lo hay, con el comando para apagarlo) |
| A9 | Salida de `status.py` |
| A10 | Nada que cobre por hora encendido (Dataflow, simulador); Scheduler del exportador `NO_EXISTE` o `PAUSED`. AVISO si no pudo preguntarle a `gcloud` |

Manual: `cd project && python scripts/export_gcs_to_s3.py doctor --offline`.

## Parte B · Pruebas

```bash
python project/scripts/validate_3_2.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/exporter -q -rs` | **120 passed, 0 skipped** (incluye `test_exp_infra.py` con `terraform fmt` y `test_exp_repo.py` con `common/` real) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate32.py -q` | **68 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/historical/tests/test_hist_infra.py -q` | sin fallas (la raíz AWS del 3.1 sigue encajando con los archivos nuevos) |

B1 tarda segundos. Si tiene omitidas, la línea `SKIPPED` dice qué falta (`terraform` en el PATH o
`src/common/enriched_row.py`). Si falla `test_columnas_de_posts_enriched_desde_el_codigo_real`, el
mensaje trae la forma real de `POSTS_ENRICHED_COLUMNS`: páseme ese bloque (el ajuste va en
`src/exporter/lake_check.py`).

## Parte C · Terraform (AWS y GCP)

```bash
python project/scripts/validate_3_2.py terraform            # validate.sh + init + planes (no aplica)
python project/scripts/validate_3_2.py terraform --apply    # aplica (pide confirmación por raíz)
```

Los planes van limitados con `-target` a los recursos del lote. El validador lee el plan en JSON
(`terraform show -json`) y **no aplica** si cambia cualquier otra cosa.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars`. AVISO si es la cuenta raíz: aquí **no afecta** (el rol lo asume `sa-exporter` con su token, no su perfil) |
| C0b | `backend.hcl` y `terraform.tfvars` en las dos raíces |
| C0c | uniqueId de `sa-exporter` (`gcloud iam service-accounts describe`) escrito en `gcp_exporter.auto.tfvars`; su cuenta de `gcloud` |
| C1 | `bash infra/validate.sh` en verde (fmt de todo `infra/`, incluidos los `*.auto.tfvars`, y `validate` de las cuatro raíces) |
| C2a | `terraform init` del entorno AWS (instala `gcp_exporter_role`) |
| C2 | **2 a crear, 0 a cambiar, 0 a destruir**: `aws_iam_role.this` y `aws_iam_role_policy.s3` (o "ya aplicado") |
| C3 | `Apply complete! Resources: 2 added, 0 changed, 0 destroyed.` |
| C4 | Outputs `exporter_role_arn` (`arn:aws:iam::648617469176:role/ta-gcp-exporter-role`), `exporter_oidc_audience` (`ta-gcp-exporter-role`), `lake_bucket`, `kms_key_arn` |
| C4b | `aws_exporter.auto.tfvars` con el rol, el bucket, la llave y `exporter_oidc_token_creators = ["user:<su cuenta>"]` |
| C5 | Confianza: `Federated = accounts.google.com`, `aud` incluye el uniqueId, `oaud` = audiencia, `sub` = solo el uniqueId; política: exactamente `s3:PutObject`, `s3:ListBucket` y `kms:GenerateDataKey`, `PutObject` solo en `raw/origin=stream/*` |
| C5b | `iam simulate-principal-policy`, **8 de 8**: permitidos `put` y `list` en `raw/origin=stream/` y `GenerateDataKey` vía S3; denegados `put` en `raw/origin=historical/`, `get` y `delete` en `raw/origin=stream/`, `list trusted/` y `kms:Decrypt` |
| C6a | `terraform init` del entorno GCP (instala `cloud_run_exporter`) |
| C6 | **2 a crear, 0 a cambiar, 1 a destruir**: crear `exporter_lake_enriched` y `exporter_oidc_token_creator["user:…"]`, destruir `exporter_lake_read` (o "ya aplicado") |
| C7 | `Apply complete! Resources: 2 added, 0 changed, 1 destroyed.` |
| C8 | Solo con el plan de GCP aplicado (si no, OMITIDO). IAM real: `sa-exporter` sin `objectViewer` sin condición; con `objectViewer` condicionado a `enriched/` y `objectUser` a `export-state/`; su cuenta con `serviceAccountOpenIdTokenCreator` sobre `sa-exporter` |

Manual (desde `project/`):

```bash
gcloud iam service-accounts describe sa-exporter@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com --format="value(uniqueId)"
cp infra/aws/environments/dev/gcp_exporter.auto.tfvars.example infra/aws/environments/dev/gcp_exporter.auto.tfvars   # y ponga el uniqueId
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan -target=module.gcp_exporter_role -out=../../../../evidencia/tfplan_3_2_aws
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_3_2_aws
terraform -chdir=infra/aws/environments/dev output
cp infra/gcp/environments/dev/aws_exporter.auto.tfvars.example infra/gcp/environments/dev/aws_exporter.auto.tfvars    # con los outputs y su cuenta
terraform -chdir=infra/gcp/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/gcp/environments/dev plan \
  -target=google_storage_bucket_iam_member.exporter_lake_enriched \
  -target=google_storage_bucket_iam_member.exporter_lake_read \
  -target=google_service_account_iam_member.exporter_oidc_token_creator -out=../../../../evidencia/tfplan_3_2_gcp
terraform -chdir=infra/gcp/environments/dev apply ../../../../evidencia/tfplan_3_2_gcp
gcloud storage buckets get-iam-policy gs://project-0568d9ba-848f-413c-88b-ta-lake --format=json
```

`evidencia/tfplan_3_2_*` pueden contener valores de `terraform.tfvars`: no los versione.

## Parte D · Federación real desde su máquina (sin Cloud Run)

```bash
python project/scripts/validate_3_2.py federation
```

Usa un watermark de prueba (`export-state/_validation/watermark-d.json`) para no tocar el del job, y
sube **20 objetos reales** (lo que E después omite).

| Paso | Qué hace | Esperado |
|---|---|---|
| D0 | Lee `terraform output` de `infra/aws` | Rol y bucket |
| D1 | `token --token-source impersonate --trust-from-aws` | Claims del ID token real de `sa-exporter` (`azp`, `sub` = uniqueId, `aud` = `ta-gcp-exporter-role`, `email`) y **la confianza del rol los acepta**. Si el permiso de token aún no se propaga (403), reintenta cada 30 s hasta 3 veces |
| D2 | `probe --no-gcs` | **A1** `AssumeRoleWithWebIdentity` real → `arn:aws:sts::648617469176:assumed-role/ta-gcp-exporter-role/ta-gcs-to-s3-…`; A2 listar `raw/origin=stream/` permitido; A3–A5 y A7 denegados (listar el histórico y `trusted/`, escribir en el histórico, borrar en el stream); **A6** leer un objeto del stream: denegado si ya hay objetos, o OK con `got = sin objetos` mientras `raw/origin=stream/` está vacío (S3 responde `NoSuchKey` a una clave que no existe porque el rol puede listar; la lectura la cubre C5b y se prueba en vivo en E5, cuando E ya subió objetos) |
| D3 | `plan` (no escribe) | Objetos en `enriched/` y cuántos faltan en S3, con el tiempo que tardó el listado (dato real para E6) |
| D4 | `run --max-objects 20` con el rol asumido | 20 subidos (o los que falten si son menos), 0 fallidos |
| D5 | `verify --keys-from` (con su perfil de AWS) | V1 OK; **V4 OK**: los 20 con `aws:kms`, la llave del lake, *bucket key* y el CRC32C igual al de GCS |
| D6 | `plan` sin cursor (con un watermark que no existe, así mira todo `enriched/`) | Los 20 de D4 salen omitidos por mismo tamaño; ninguno sigue por subir |
| D7 | Borra el watermark de prueba | `gs://…-ta-lake/export-state/_validation/watermark-d.json` borrado |

Manual (desde la raíz, con los valores de `terraform -chdir=project/infra/aws/environments/dev output`):

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b TA_LAKE_BUCKET=ta-dev-648617469176-lake
export TA_LAKE_KMS_KEY_ARN=<kms_key_arn> TA_EXPORTER_ROLE_ARN=<exporter_role_arn>
python project/scripts/export_gcs_to_s3.py token --token-source impersonate --trust-from-aws
python project/scripts/export_gcs_to_s3.py probe --no-gcs --token-source impersonate
python project/scripts/export_gcs_to_s3.py plan --token-source impersonate --state-key export-state/_validation/watermark-d.json
python project/scripts/export_gcs_to_s3.py run  --token-source impersonate --state-key export-state/_validation/watermark-d.json --max-objects 20 --json-out project/evidencia/exporter_run_d_3_2.json
python project/scripts/export_gcs_to_s3.py verify --keys-from project/evidencia/exporter_run_d_3_2.json --state-key export-state/_validation/watermark-d.json
gcloud storage rm gs://project-0568d9ba-848f-413c-88b-ta-lake/export-state/_validation/watermark-d.json
```

## Parte E · Cloud Run Job y Cloud Scheduler

```bash
python project/scripts/validate_3_2.py job               # --skip-build si la imagen ya está; --wait-tick para esperar el disparo programado
```

Pide confirmación dos veces: antes del `apply` (E3) y antes de reanudar el Scheduler (E10); `--yes`
las omite. Si corta la parte con Ctrl+C, corra `validate_3_2.py off`.

| Paso | Qué hace | Esperado |
|---|---|---|
| E0 | Valores de AWS en `aws_exporter.auto.tfvars` | Escritos por C |
| E1 | `build_exporter_image.sh cloudbuild` | Etapa de pruebas sin omitidas e imagen `…/ta-images/gcs-to-s3:{commit}-{huella}` publicada (estimado 3–8 min; anota el tiempo real) |
| E2 | Escribe `exporter_image` y planea `-target=module.exporter_job` | **3 a crear**: el job, `run.invoker` de `sa-exporter` sobre el job y el Scheduler (o solo cambiar la imagen del job) |
| E3 | `apply` (pide confirmación) | `Apply complete! Resources: 3 added…` |
| E4 | `gcloud scheduler jobs describe` y `gcloud run jobs describe` | Scheduler **PAUSED**, `*/15 * * * *` `Etc/UTC`; job con `sa-exporter` y la imagen de E1 |
| E5 | `gcloud run jobs execute ta-gcs-to-s3 --args=probe --wait` y su log `probe_result` | **13/13**: G1 listar `enriched/`, G2 leer `enriched/`, **G3 leer `raw/` denegado**, G4 escribir en `enriched/` denegado, G5–G6 escribir y borrar en `export-state/`; A1–A7 como en D2, ahora desde Cloud Run |
| E6 | Primera ejecución (`--wait`) y su log `export_done` | Sube el atraso de `enriched/` (lo que faltaba tras D4), 0 fallidos; watermark escrito; anota la duración |
| E7 | `verify --sample 50` (su perfil) | V1 OK (todo lo anterior al cursor está en S3 con el mismo tamaño), V3 OK, V4 OK (SSE-KMS y CRC32C en la muestra) |
| E8 | Segunda ejecución | **0 subidos** (sin el stream encendido), 0 sobrescritos |
| E9 | Canario: copia un Parquet de `enriched/` a la hora actual como `part-…-p999.parquet` y ejecuta | 1 subido; E9b: en S3 con `aws:kms` y la llave del lake; E9c: el canario se borra de GCS y de S3 |
| E10 | Reanuda el Scheduler, lo **dispara** (`gcloud scheduler jobs run`) y espera la ejecución | Ejecución nueva creada por `sa-exporter` (la identidad del Scheduler) que termina bien. Con `--wait-tick` espera el disparo programado (hasta 18 min) |
| E10b | Pausa el Scheduler (siempre, también si E10 falla) | Estado **PAUSED** |
| E11 | Logs del job con severidad ERROR desde el inicio de E | 0 (las denegaciones esperadas de la sonda van en INFO) |
| E12 | Costo con lo medido | Duración de cada ejecución, ≥ 60 s cobrados por ejecución → ≈ USD por ejecución y por día con 96 ejecuciones |

Manual (desde `project/`):

```bash
bash scripts/build_exporter_image.sh cloudbuild               # copie la línea exporter_image = "…" al aws_exporter.auto.tfvars
terraform -chdir=infra/gcp/environments/dev plan -target=module.exporter_job -out=../../../../evidencia/tfplan_3_2_job
terraform -chdir=infra/gcp/environments/dev apply ../../../../evidencia/tfplan_3_2_job
gcloud scheduler jobs describe ta-gcs-to-s3-cada-15-min --location us-central1 --format="value(state)"   # PAUSED
gcloud run jobs execute ta-gcs-to-s3 --region us-central1 --args=probe --wait
gcloud run jobs execute ta-gcs-to-s3 --region us-central1 --wait
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="ta-gcs-to-s3" AND jsonPayload.event="export_done"' --limit 3 --format=json
python scripts/export_gcs_to_s3.py verify --sample 50        # con TA_LAKE_BUCKET y TA_LAKE_KMS_KEY_ARN exportadas
gcloud scheduler jobs resume ta-gcs-to-s3-cada-15-min --location us-central1
gcloud scheduler jobs run    ta-gcs-to-s3-cada-15-min --location us-central1
gcloud run jobs executions list --job ta-gcs-to-s3 --region us-central1 --limit 3
gcloud scheduler jobs pause  ta-gcs-to-s3-cada-15-min --location us-central1
```

## Parte F · Ambos orígenes en S3

```bash
python project/scripts/validate_3_2.py lake
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | Cuenta `raw/origin=historical/` | **12.102** objetos (los del 3.1), por fuente |
| F2 | Cuenta `raw/origin=stream/` | Los de E (frente a lo que había en `enriched/` al verificar en E7) |
| F3 | Lee un Parquet de cada origen (pyarrow) | Las 24 columnas de `posts_enriched` en el orden de `common.enriched_row`, `origin` correcto, sin `user_id`/`handle` (columnas ni `payload`), `user_hash` hex de 64 |
| F4 | "Listo cuando" de la Fase 3 | Datos de ambos orígenes en `s3://…/raw/` con el mismo esquema |
| F5 | Costo (de E12) | Encendido ≈ USD/día; pausado, 0 de Cloud Run (el Scheduler pausado cuenta como job: 0,10 USD/mes si pasa de los 3 gratuitos de la cuenta de facturación) |

Manual: `python project/scripts/export_gcs_to_s3.py lake-report --aws-auth profile` y
`aws s3 ls s3://ta-dev-648617469176-lake/raw/origin=stream/ --recursive --summarize | tail -2`.

## Apagado

```bash
python project/scripts/validate_3_2.py off          # pausa el Scheduler, cancela ejecuciones, borra canarios y el watermark de prueba
python project/scripts/validate_3_2.py status       # esperado: Scheduler PAUSED, ninguna ejecución en curso
python project/scripts/status.py                    # lo demás que cobra (no conoce el exportador)
```

| Paso | Esperado |
|---|---|
| O1 | Scheduler `PAUSED` (lo pausa si estaba `ENABLED`) |
| O2 | Ninguna ejecución en curso (cancela las que haya) |
| O3 | Canarios de E9 borrados de GCS y S3 |
| O4 | `export-state/_validation/` borrado |
| O5 | `status.py`: simulador y Dataflow apagados si no los necesita otro lote. Si algo dice `ENCENDIDO`, O5 da **AVISO** con el comando para apagarlo (`off.py`; `--keep-simulator` deja el simulador) |

Lo que queda creado y no cobra por hora: el rol de AWS (gratis), el job (no cobra si no corre), el
Scheduler pausado (cuenta como job de Scheduler: 0,10 USD/mes por encima de los 3 gratuitos), la imagen
en Artifact Registry (centavos) y los objetos de `raw/origin=stream/` en S3 (centavos). Para encender
el exportador de forma permanente: `gcloud scheduler jobs resume ta-gcs-to-s3-cada-15-min --location us-central1`
(y `off` para volver).

## Cierre

**Resultado de la validación (v2.31): Lote 3.2 cerrado y Fase 3 cerrada.** Validó A (asignado a M), 24–25 oct 2026.

| Parte | Resultado |
|---|---|
| A · Preflight | AVISO solo por el simulador encendido (A10; este lote no lo usa). Lo demás OK; la primera corrida fue desde PowerShell (corrección de v2.30) |
| B · Pruebas | OK con el código final: B1 120 sin omitidas, B2 68, B3 18, B4 11 |
| C · Terraform | AVISO solo por la cuenta raíz (C0). AWS: rol y política aplicados; confianza y política OK, 8/8 simulados. GCP: 2 creados y `exporter_lake_read` destruido; C8 OK |
| D · Federación | OK: `azp = sub` = 101053368441381590999, `aud = ta-gcp-exporter-role`; rol asumido; 20 objetos subidos y verificados; 1.155 → 1.135 por subir. Antes falló A6 por la sonda (corrección de v2.30) |
| E · Job y Scheduler | OK (16/16) en la tercera corrida: sonda 13/13; 1.135 objetos (51 MB) en 107 s; 1.155 de 1.155 verificados; segunda ejecución 0 subidos; canario 1 y borrado; disparo real del Scheduler OK y de vuelta a `PAUSED`; 0 errores; ≈ 0,16 USD/día encendido. Las dos primeras corridas pararon en E3 (estado sin guardar) y E2 (`retry_config`), corregidos en v2.30 |
| F · Ambos orígenes | OK: 12.102 (histórico, 130,9 MB) + 1.155 (stream, 51,9 MB), mismas 24 columnas: "Listo cuando" de la Fase 3 |
| Apagado | `off` y `status` OK: Scheduler `PAUSED`, sin ejecuciones en curso. Una segunda corrida de `off` dio O5 en AVISO: el simulador seguía encendido porque no estaba en el estado de Terraform (`off.py` no lo pudo apagar; ver `ACTIVE_SERVICES.md` §5) |

Cifras en `CHANGELOG.md` (v2.31) y `ARCHITECTURE.md` §3.18.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` en `from pathlib import Path` antes de imprimir nada | La carpeta actual quedó inválida en `/mnt/c` tras copiar archivos desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A0/A1 `Python 3.14…`, faltan `boto3` y `google.cloud.storage`, `no se encontró el comando: gcloud` | Está en PowerShell (Windows), no en WSL. Abra WSL, `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal`, `source ~/.venvs/ta-streaming/bin/activate` y repita. |
| A4 `platform.tf todavía declara exporter_lake_read` | No se copió el `platform.tf` del lote. Sin ese cambio C6 no destruye el permiso amplio. |
| B1 `SKIPPED … terraform no está en el PATH` | Ejecute desde WSL con Terraform instalado (`terraform version`). |
| C1 `validate.sh` marca un `*.auto.tfvars` | El validador los formatea con `terraform fmt`; si los editó a mano: `terraform fmt infra/aws/environments/dev infra/gcp/environments/dev`. |
| C2/C6/E2 FALLA "no aplique" | El plan cambia algo fuera del lote. **No aplique**. El detalle lista cada atributo que cambia (`ruta: antes → después`); revise con `git status` qué `.tf` cambió y páseme el detalle. |
| C3/C7/E3 FALLA con `Failed to save state` / `errored.tfstate`; o C/E dicen "hay un estado sin guardar" | El `apply` creó recursos y no pudo guardar el estado. **No vuelva a planear ni aplicar.** Desde `project/`: `terraform -chdir=infra/gcp/environments/dev state push errored.tfstate` (o `infra/aws/…`; se niega si el estado remoto cambió), compruebe con `state list` y mueva `errored.tfstate` fuera del repo. Si el push falla por credenciales: `gcloud auth application-default login` y repita. Luego repita la parte (en E con `--skip-build`). |
| C2a/C6a `Backend configuration changed` | `terraform -chdir=… init -reconfigure -backend-config=backend.hcl` (como en el 3.1). |
| C8 sin `serviceAccountOpenIdTokenCreator` | ¿Se aplicó C7? ¿`aws_exporter.auto.tfvars` tiene `user:<la cuenta de gcloud>`? |
| D1 `403` / `PERMISSION_DENIED … getOpenIdToken` | El permiso de C7 tarda en propagarse (el validador reintenta 3 veces). Si sigue: `gcloud auth application-default login` con la misma cuenta de `gcloud config get-value account`. |
| D1 la confianza no acepta el token (`accounts.google.com:aud` o `sub`) | Pegue el detalle (trae `azp`, `sub` y lo que acepta el rol). Si `sa-exporter` se recreó, cambió su uniqueId: repita `terraform --apply`. |
| D2 A1 `InvalidIdentityToken` / `AccessDenied` al asumir | Igual que la fila anterior: compare los claims de D1 con `aws iam get-role --role-name ta-gcp-exporter-role`. |
| D3 AVISO "enriched/ está vacío" | No hay datos del stream: el exportador no tiene qué copiar. Basta con que el stream haya corrido alguna vez (Lote 2.2+); si no, encienda simulador y Dataflow un rato (`on.py`). |
| D4/E6 `AccessDenied` de KMS | ¿`TA_LAKE_KMS_KEY_ARN` es el ARN de la llave (no el alias)? C5b debe tener `GenerateDataKey vía S3` permitido. |
| E1 falla en Cloud Build por permisos | La cuenta de Compute que usa Cloud Build tiene `roles/cloudbuild.builds.builder` desde el Lote 1.3; si el error es del *pool* regional, use `bash project/scripts/build_exporter_image.sh docker` (Docker Desktop) y `validate_3_2.py job --skip-build`. |
| E5 G1 FALLA (no puede listar) | La condición `resource.type == "storage.googleapis.com/Bucket"` no cubre el listado en este bucket: páseme el detalle (corrección en `exporter.tf`). Mientras tanto el Scheduler sigue en pausa. |
| E5 G3 OK con "no se pudo probar" | `raw/` está vacío (expira a 30 días): la lectura de `raw/` quedó sin probar en vivo; C8 la cubre con la política real. |
| E6 tarda más de lo esperado | Es la primera copia de todo `enriched/` (miles de archivos pequeños); el job corta a los 5.000 objetos y sigue en la próxima. |
| E8 sube objetos | ¿Está encendido el stream (Dataflow)? Entonces es lo nuevo y E8 lo acepta; si no, páseme el `export_done`. |
| E10 no aparece ejecución | Revise `gcloud scheduler jobs describe … --format=json` (último `status`) y que `sa-exporter` tenga `run.invoker` en el job (E2). El validador ya volvió a pausar el Scheduler (E10b). |
| `on.py`/`off.py` se niegan porque el plan de GCP cambia el exportador | Hay cambios del lote sin aplicar (por ejemplo, una imagen nueva en `aws_exporter.auto.tfvars`): termine E (o vuelva la imagen anterior) y repita. |
| Quedó el Scheduler encendido | `python project/scripts/validate_3_2.py off`. |
