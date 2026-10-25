# Infraestructura GCP base y despliegue del simulador (Lote 1.3)

Guía para aplicar el Lote 1.3 (HU-004, HU-016). Contrato en `docs/ARCHITECTURE.md` §5.15.
Todos los comandos se corren **desde la raíz del repo** (donde están `docs/` y `project/`), en WSL,
con `gcloud` y `terraform` ya autenticados como en el Lote 1.0 (`BOOTSTRAP.md`).

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b
export TF_DEV=project/infra/gcp/environments/dev
```

> **Costo.** Con el simulador desplegado hay una instancia de Cloud Run encendida las 24 horas
> (1 vCPU y 512 MiB, CPU siempre asignada). Es el costo fijo más alto hasta ahora. Cuando no se
> use, apáguenlo (paso 6): el cursor queda en GCS y al volver a encenderlo continúa donde iba.

## 1. Validar sin credenciales

```bash
bash project/infra/validate.sh
```

Debe pasar `fmt` y `validate` en las cuatro raíces, igual que en el Lote 1.0.

## 2. Crear la base (sin simulador)

```bash
terraform -chdir=$TF_DEV init -backend-config=backend.hcl
terraform -chdir=$TF_DEV plan -out=tfplan
terraform -chdir=$TF_DEV apply tfplan
```

El `plan` debe proponer **solo creaciones**, nada que cambie o destruya lo del Lote 1.0:

| Recurso | Nombres |
|---|---|
| Topics | `ta-raw-reddit`, `ta-raw-facebook`, `ta-raw-instagram`, `ta-dlq`, `ta-alerts` |
| Suscripciones | `ta-raw-{reddit,facebook,instagram}-df` (pull, ack 60 s, retención 7 días) |
| Buckets | `{GCP_PROJECT}-ta-datasets` (versionado) y `{GCP_PROJECT}-ta-lake` (`raw/` y `dlq/` a 30 días) |
| Artifact Registry | `ta-images` (Docker, `us-central1`) |
| Service accounts | `sa-simulator`, `sa-dataflow`, `sa-exporter` con los permisos de §5.8 |

El servicio Cloud Run **todavía no** se crea: falta la imagen (`simulator_image` vacío).

## 3. Construir y publicar la imagen

```bash
GCP_PROJECT=$GCP_PROJECT bash project/scripts/build_simulator_image.sh
```

Usa Cloud Build (no hace falta Docker local). Primero corre las pruebas de `common/` y `simulator/`
en **Python 3.11** dentro de la imagen, contra `contracts/`; si alguna falla o se omite, no publica.
Al final imprime una línea como:

```
  simulator_image = "us-central1-docker.pkg.dev/<proyecto>/ta-images/stream-simulator:<tag>"
```

Con Docker en WSL también sirve: `gcloud auth configure-docker us-central1-docker.pkg.dev` una vez y
`GCP_PROJECT=$GCP_PROJECT bash project/scripts/build_simulator_image.sh docker`.

## 4. Desplegar el simulador

Agreguen la línea del paso 3 a `$TF_DEV/terraform.tfvars` (no se versiona) y apliquen:

```bash
terraform -chdir=$TF_DEV plan -out=tfplan     # solo debe crear module.simulator[0]
terraform -chdir=$TF_DEV apply tfplan
terraform -chdir=$TF_DEV output
```

## 5. Verificar (HU-016)

```bash
# Servicio: min = max = 1 y CPU siempre asignada
gcloud run services describe ta-stream-simulator --region us-central1 \
  --format='yaml(spec.template.metadata.annotations)'

# /healthz responde 200 (el servicio no es público: va con ID token)
URL=$(terraform -chdir=$TF_DEV output -raw simulator_url)
curl -s -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$URL/healthz"; echo

# Los 3 topics reciben mensajes: se miran en las suscripciones de Dataflow sin confirmarlos
for s in reddit facebook instagram; do
  gcloud pubsub subscriptions pull ta-raw-$s-df --limit 2 --format='value(message.attributes.event_id,message.attributes.source)'
done

# Cursor en GCS (aparece tras los primeros 50 mensajes de cada fuente, ~2 min)
gcloud storage cat gs://$GCP_PROJECT-ta-datasets/sim-state/reddit.json; echo

# Logs del simulador
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="ta-stream-simulator"' \
  --limit 20 --format='value(jsonPayload.event,jsonPayload.source,jsonPayload.seq)'
```

En `annotations` deben aparecer `autoscaling.knative.dev/minScale: '1'`, `autoscaling.knative.dev/maxScale: '1'`
y `run.googleapis.com/cpu-throttling: 'false'`. El `pull` sin `--auto-ack` no consume los mensajes.

## 6. Apagar y volver a encender

```bash
terraform -chdir=$TF_DEV apply -var simulator_running=false   # borra el servicio, conserva todo lo demás
terraform -chdir=$TF_DEV apply                                # lo vuelve a crear con terraform.tfvars
```

Al volver a encenderlo, el log `worker_start` de cada fuente debe arrancar en el `seq` del cursor
(no en 0). Eso comprueba que el cursor sobrevive a un reinicio. Para dejarlo apagado de forma
estable, pongan `simulator_running = false` en `terraform.tfvars`.

## Problemas conocidos

- **Cloud Build sin permisos** (`PERMISSION_DENIED` al subir la imagen o escribir logs): la cuenta
  de servicio por defecto de Cloud Build necesita `roles/artifactregistry.writer` y `roles/logging.logWriter`.
  Alternativa: el modo `docker`.
- **`Error 409: already exists`** en el `apply`: el recurso se creó a mano antes. Se importa con
  `terraform -chdir=$TF_DEV import <dirección> <id>` en vez de borrarlo.
- **El servicio no arranca**: `gcloud run services logs read ta-stream-simulator --region us-central1`.
  Un `PermissionDenied` al publicar apunta a los permisos de `sa-simulator` sobre `ta-raw-*`.
