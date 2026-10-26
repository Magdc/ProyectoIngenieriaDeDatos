# Exportador `gcs-to-s3` (Lote 3.2)

Copia los Parquet de `gs://{GCP_PROJECT}-ta-lake/enriched/` a `s3://ta-dev-{cuenta}-lake/raw/origin=stream/`
con el mismo nombre de archivo, cada 15 minutos, desde un Cloud Run Job. La identidad es `sa-exporter`:
su ID token de Google asume en AWS el rol `ta-gcp-exporter-role` con `AssumeRoleWithWebIdentity`,
sin llaves estáticas (HU-006, HU-025). Contrato en `docs/ARCHITECTURE.md` §3.18 y §5.6; validación en
`docs/validation/validation-3-2.md`.

Rutas relativas a `project/`.

## 1. Qué hace una corrida

```
export-state/watermark.json ─► lista enriched/source={s}/ desde la partición de (cursor − solape − margen)
   ─► candidatos: timeCreated > cursor − solape (1 h)
   ─► lista S3 raw/origin=stream/ solo en las particiones de los candidatos
   ─► sube lo que falta o tiene otro tamaño (PUT con SSE-KMS, bucket key y el CRC32C de GCS)
   ─► si nada falló: escribe el watermark con precondición de generación
```

- **Solo `enriched/`**: lista `enriched/source={reddit,facebook,instagram}/` y solo exporta claves
  `enriched/source=…/dt=AAAA-MM-DD/hr=HH/part-*.parquet`; otras se cuentan como `unrecognized`.
- **Idempotente**: si el objeto ya está en S3 con el mismo tamaño, se omite (`skipped_same_size`). Con
  otro tamaño se sobrescribe y queda un log `export_object_overwritten`.
- **Íntegro**: el PUT lleva `ChecksumCRC32C` = el CRC32C que GCS guarda del objeto (mismo formato), así
  que S3 rechaza el objeto si los bytes no son idénticos; además la descarga de GCS verifica su checksum.
- **Cifrado**: `ServerSideEncryption=aws:kms`, la llave del lake y `BucketKeyEnabled`; si S3 responde
  otro cifrado o llave, el objeto cuenta como fallido.
- **Watermark solo tras subir con éxito**: con una sola falla no se escribe y la corrida sale con 1
  (Cloud Run reintenta una vez). Lo ya subido no se repite en la siguiente.
- **Un solo PUT por objeto** (los Parquet de `enriched/` pesan KB): sin *multipart*, que pediría además
  `kms:Decrypt`.

## 2. Configuración (variables de entorno)

| Variable | Por defecto | Uso |
|---|---|---|
| `GCP_PROJECT` | — | Proyecto; da el bucket y el correo de `sa-exporter` por defecto |
| `TA_GCS_LAKE_BUCKET` | `{GCP_PROJECT}-ta-lake` | Lake de GCS |
| `TA_EXPORT_STATE_KEY` | `export-state/watermark.json` | Watermark (debe quedar bajo `export-state/`) |
| `TA_LAKE_BUCKET` | — (obligatoria) | Bucket del lake en S3 (output `lake_bucket` de `infra/aws`) |
| `TA_LAKE_KMS_KEY_ARN` | — (obligatoria para subir) | ARN de la llave del lake (output `kms_key_arn`) |
| `TA_EXPORTER_ROLE_ARN` | — | ARN de `ta-gcp-exporter-role` (output `exporter_role_arn`) |
| `TA_EXPORTER_AUDIENCE` | `ta-gcp-exporter-role` | Audiencia del ID token (output `exporter_oidc_audience`) |
| `TA_EXPORTER_SA` | `sa-exporter@{GCP_PROJECT}.iam.gserviceaccount.com` | Cuenta a impersonar fuera de Cloud Run |
| `AWS_REGION` | `us-east-1` | Región del lake |
| `TA_EXPORT_SOURCES` | las tres | Fuentes a exportar |
| `TA_EXPORT_WORKERS` | `8` | Subidas en paralelo (1 a 10) |
| `TA_EXPORT_MAX_OBJECTS` | `5000` | Máximo por corrida; el resto sale en la siguiente |
| `TA_EXPORT_OVERLAP_SECONDS` | `3600` | Solape: lo creado en la última hora antes del cursor se vuelve a mirar |
| `TA_EXPORT_MARGIN_SECONDS` | `7200` | Margen del listado para los paneles tardíos (se escriben en la partición de su ventana) |
| `TA_EXPORTER_SESSION_SECONDS` | `3600` | Duración de la sesión asumida |

En Cloud Run las pone Terraform (`infra/gcp/environments/dev/exporter.tf`); en la máquina del equipo
las pone `validate_3_2.py` desde `terraform output` de `infra/aws`.

## 3. Comandos

Desde la raíz del repo, con `~/.venvs/ta-streaming` activado:

```bash
python project/scripts/export_gcs_to_s3.py doctor --offline      # dependencias y configuración
python project/scripts/export_gcs_to_s3.py token --token-source impersonate --trust-from-aws
python project/scripts/export_gcs_to_s3.py probe --no-gcs --token-source impersonate
python project/scripts/export_gcs_to_s3.py plan  --token-source impersonate --state-key export-state/_validation/x.json
python project/scripts/export_gcs_to_s3.py run   --token-source impersonate --max-objects 20 --state-key export-state/_validation/x.json
python project/scripts/export_gcs_to_s3.py verify --sample 50                 # con su perfil de AWS
python project/scripts/export_gcs_to_s3.py status                             # el watermark
python project/scripts/export_gcs_to_s3.py lake-report                        # ambos orígenes en S3
```

- `--token-source`: `auto` (metadatos en Cloud Run, impersonación fuera), `metadata`, `impersonate`
  o `gcloud` (`gcloud auth print-identity-token --impersonate-service-account=…`).
- `--aws-auth role|profile`: `run`, `plan`, `probe` usan el rol por defecto; `verify` y `lake-report`
  usan el perfil de AWS CLI (necesitan leer, y el rol no puede).
- `--json-out` guarda el resultado; ningún comando imprime el token ni las credenciales.
- Salida: 0 bien, 1 una comprobación falló, 2 error de uso, configuración o identidad.

## 4. Watermark (`export-state/watermark.json`)

```json
{
  "schema": "ta-export-watermark@1",
  "cursor_time_created": "2026-10-24T20:14:58.123Z",
  "updated_at": "2026-10-24T20:15:41.007Z",
  "run_id": "20261024T201530Z-1a2b3c4d",
  "exporter": "gcs-to-s3@1.0",
  "last_run": {"listed": 214, "candidates": 38, "uploaded": 3, "skipped_same_size": 35, "duration_s": 6.2, "…": "…"}
}
```

`cursor_time_created` es el mayor `timeCreated` de GCS ya exportado (UTC con milisegundos). Nunca
retrocede. Si una corrida corta por `TA_EXPORT_MAX_OBJECTS`, queda justo antes del primer pendiente.
Se escribe con `ifGenerationMatch`: si otra corrida escribió en medio, se queda con el cursor mayor
y reintenta (hasta 3 veces).

## 5. Identidad y permisos

| Dónde | Qué | Dónde se define |
|---|---|---|
| GCP | `sa-exporter`: `objectViewer` con condición (listar el bucket y leer solo `enriched/`) y `objectUser` con condición en `export-state/` | `exporter.tf` y `platform.tf` (`exporter_watermark`) |
| GCP | `roles/run.invoker` de `sa-exporter` sobre el job (lo usa el Scheduler) | módulo `cloud_run_exporter` |
| GCP | `roles/iam.serviceAccountOpenIdTokenCreator` sobre `sa-exporter` para quien valida desde su máquina | `exporter_oidc_token_creators` |
| AWS | `ta-gcp-exporter-role`: confianza en `accounts.google.com` con `aud` ∈ {uniqueId, correo, audiencia}, `oaud` = audiencia, `sub` = uniqueId | módulo `gcp_exporter_role` |
| AWS | `s3:PutObject` en `raw/origin=stream/*`, `s3:ListBucket` con `s3:prefix` = `raw/origin=stream/*`, `kms:GenerateDataKey` en la llave del lake vía S3 | ídem |

AWS compara `accounts.google.com:aud` con el claim **`azp`** del token (si existe) y
`accounts.google.com:oaud` con su `aud`; por eso la confianza acepta en `aud` el uniqueId y el correo
de la cuenta (según cómo se emitió el token) y fija la identidad con `sub`. `token` muestra los
claims reales y si el rol los aceptaría, antes de cualquier corrida.

## 6. Imagen

```bash
bash project/scripts/build_exporter_image.sh            # Cloud Build (por defecto)
bash project/scripts/build_exporter_image.sh docker     # Docker local + push
bash project/scripts/build_exporter_image.sh --dry-run  # solo calcula la etiqueta
```

`{región}-docker.pkg.dev/{proyecto}/ta-images/gcs-to-s3:{commit o fecha}-{huella del código}` y
`:latest`. La huella cambia con cualquier cambio en `src/exporter/`, así que Terraform ve la imagen
nueva aunque no haya *commit*. La etapa `test` corre las pruebas sin red y falla si alguna falla o
se omite; la imagen publicada (`runtime`) no trae pruebas y corre como uid 10001.

## 7. Operación

El Scheduler `ta-gcs-to-s3-cada-15-min` se crea **en pausa** y Terraform ignora ese campo:

```bash
gcloud scheduler jobs resume ta-gcs-to-s3-cada-15-min --location us-central1   # encender
gcloud scheduler jobs pause  ta-gcs-to-s3-cada-15-min --location us-central1   # apagar
python project/scripts/validate_3_2.py status                                  # estado
python project/scripts/validate_3_2.py off                                     # pausa, cancela y confirma
```

Una corrida a mano: `gcloud run jobs execute ta-gcs-to-s3 --region us-central1 --wait`. Logs:
`export_start`, `export_assumed_role`, `export_plan`, `export_object_failed`, `export_object_overwritten`,
`export_watermark_updated`, `export_done` y, con `--args=probe`, `probe_result`.

## 8. Si algo no encaja

- `token` dice que el rol no acepta el token: compare `azp`, `aud` y `sub` con la confianza
  (`aws iam get-role --role-name ta-gcp-exporter-role`). Si `sa-exporter` se recreó, su uniqueId cambió:
  repita `validate_3_2.py terraform --apply`.
- `403` al emitir el token desde su máquina: falta (o aún no se propaga) `serviceAccountOpenIdTokenCreator`.
- `AccessDenied` de KMS al subir: la llave que se pasa no es la del lake (`TA_LAKE_KMS_KEY_ARN` debe ser
  el ARN de la llave, no el alias) o la política del rol perdió `kms:GenerateDataKey` vía S3.
- `lake-report` sin `common.enriched_row`: compara con las columnas de §5.5 y lo dice (AVISO).
