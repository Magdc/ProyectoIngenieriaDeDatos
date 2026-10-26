# Servicios activos: encender, comprobar y apagar

Guía para dejar encendido lo necesario mientras se validan lotes y apagar lo que cobra al terminar.

Todos los comandos se corren **desde la carpeta `project/`** del repo, uno por línea. Antes de empezar, en cada terminal:

```
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal/project
source ~/.venvs/ta-streaming/bin/activate
```

---

## Atajo: `on.py`, `off.py` y `status.py`

Tres comandos que hacen lo de las secciones 1 a 3 por ti. Se corren desde la raíz del repo, con `~/.venvs/ta-streaming` activo (el lanzamiento de Dataflow necesita Beam):

```
python project/scripts/status.py
python project/scripts/on.py
python project/scripts/off.py
```

| Comando | Qué hace |
|---|---|
| `status.py` | Solo lee: qué hay encendido en local y en GCP, y qué cobra. |
| `on.py` | Local (emulador, simulador y `dataflow-local`) **y** GCP (simulador en Cloud Run y job de Dataflow). Espera a que el job esté en *Running*. |
| `on.py local` / `on.py cloud` | Solo una de las dos partes. |
| `on.py --restart-job` | Si ya hay un job, lo drena y lanza uno nuevo. Úsalo tras copiar código nuevo del pipeline. |
| `on.py --nlp off` | Lanza el job sin NLP. Con `auto` (por defecto), usa la última imagen `dataflow-nlp` publicada si existe, y si no la hay, lanza sin NLP. |
| `off.py` | Solo **lo que cobra**: drena Dataflow, apaga el simulador de Cloud Run y borra las suscripciones temporales `ta-verify-*`. Lo local queda encendido. |
| `off.py --keep-simulator` | Apaga solo Dataflow; deja el simulador para seguir mañana. |
| `off.py all` | También apaga lo local. |
| `off.py --force` | Si el job sigue drenando a los 15 min, lo cancela. |

Antes de actuar, `on.py` y `off.py` muestran lo que van a hacer y piden confirmación. `--yes` la omite.

Cómo se protege la nube:
- Para el simulador, cambian `simulator_running` en `terraform.tfvars` sin tocar la alineación, y revisan el `plan` antes de aplicar.
- **Solo aplican si el plan cambia únicamente el servicio de Cloud Run.** Si propone cualquier otra cosa (por ejemplo, recursos de un lote nuevo que aún no aplicaste), se detienen sin aplicar nada y te dicen qué cambió.
- Los recursos de cada lote nuevo se aplican a mano, como en 2.3.
- Nunca borran topics, buckets, BigQuery, el secreto ni imágenes.

**El exportador del Lote 3.2 no está en estos tres comandos todavía** (queda para el Lote 7.3): su Scheduler se crea en pausa y se ve, enciende y apaga con `validate_3_2.py status` / `off` (sección 2.7).

**Tampoco el horario del lakehouse de AWS (Lote 4.1):** se crea deshabilitado y se ve, enciende y apaga con `validate_4_1.py status` / `on` / `off` (sección 2.8).

Un día típico:

```
python project/scripts/on.py
python project/scripts/off.py --keep-simulator
```

El primero va por la mañana; el segundo, al terminar el día. Al cerrar la semana de validaciones, `python project/scripts/off.py`.

---

## 0. Qué cuesta y qué no

| Dónde | Servicio | Costo encendido | ¿Se apaga? |
|---|---|---|---|
| Local | Emulador de Pub/Sub, simulador, `dataflow-local` | 0 (solo memoria del portátil) | No hace falta entre lotes |
| GCP | Topics, suscripciones, buckets, BigQuery, secreto HMAC, Artifact Registry, service accounts | Prácticamente 0 | **Nunca**: no se encienden ni se apagan |
| GCP | Simulador en Cloud Run (`ta-stream-simulator`) | ~2 USD/día | Al terminar la semana de validaciones |
| GCP | Job de Dataflow (`ta-stream-dev`) | ~0,25 USD/h (~6 USD/día) | **Al terminar cada día** |
| GCP | Cloud Build (imagen de NLP, Lote 2.3) | ~0,10 USD por construcción | Termina solo |
| Local | Contenedor `ta-hist-nlp` (NLP del histórico, Lote 3.1) | 0 | Termina solo (40–60 min con 50.000 filas); `bash scripts/historical_nlp.sh stop` lo detiene |
| AWS | Bucket del lake, llave KMS `alias/ta-dev-lake` y rol `ta-historical-loader` (Lote 3.1) | ~1 USD/mes por la llave; el bucket, centavos (~110 MB) | **Nunca**: no se encienden ni se apagan |
| GCP | Exportador `ta-gcs-to-s3` (Cloud Run Job) con su Cloud Scheduler cada 15 min (Lote 3.2) | **Pausado (por defecto): 0** de Cloud Run; el Scheduler pausado cuenta como job (0,10 USD/mes si pasa de los 3 gratuitos de la cuenta). **Encendido: ≈ 0,16 USD/día** (medido en v2.31: 96 ejecuciones de 36–118 s, mínimo 60 s cobrados) más centavos de S3 | Se pausa con `validate_3_2.py off` (2.7). Encenderlo de forma permanente lo decide el equipo |
| AWS | Rol `ta-gcp-exporter-role` y objetos de `raw/origin=stream/` (Lote 3.2) | Rol gratis; objetos, centavos | **Nunca** |
| GCP | Cloud Build (imagen `gcs-to-s3`, Lote 3.2) | Centavos por construcción | Termina solo |
| AWS | Catálogo de Glue (`ta_raw`, `ta_trusted`, `ta_curated`), workgroup `ta-analytics`, roles, regla de fallos y grupos de logs (Lote 4.1) | Prácticamente 0 (el catálogo entra en el millón gratuito de Glue; logs, centavos) | **Nunca** |
| AWS | Horario `ta-wf-raw-to-curated-cada-hora` (EventBridge Scheduler) y el workflow de Glue que arranca (Lote 4.1) | **Deshabilitado (por defecto): 0.** Encendido con los marcadores de posición del 4.1: ≈ 0,0009 USD por corrida, **≈ 0,022 USD/día** (2 jobs Python shell de 0,0625 DPU, mínimo 1 min cada uno); con los jobs reales de 4.2/4.3, más | Se apaga con `validate_4_1.py off` (2.8). Encenderlo lo decide el equipo |
| AWS | Consultas de Athena en `ta-analytics` | 5 USD/TB escaneado, mínimo 10 MB por consulta (≈ 0,00005 USD); límite de 1 GiB por consulta | Termina sola |
| AWS → local | Profiling de calidad (Lote 5.1): `profile_data.py sync` copia `raw/` del lake a `~/.cache/trendanalyzer/quality` (solo lectura) | Primera vez: ~13.300 GET y unos LIST (≈ 0,01 USD con las llamadas a KMS) y ~0,18 GB de salida de AWS (dentro de los 100 GB gratuitos al mes; sin comprobar en la factura). Después solo LIST. No deja nada encendido | No hace falta; `python project/scripts/validate_5_1.py clean` borra la caché local (~0,25 GB) |

Con el presupuesto mensual de 400.000 COP, dejar el simulador una semana y Dataflow solo en jornadas de trabajo cabe sin problema. Las alertas del presupuesto (Lote 1.0) avisan por correo si algo se queda encendido.

**Cómo validar más rápido:**
1. Haz primero la **parte local** de cada lote (partes A a D de cada `validation-*.md`), dejando el entorno local encendido todo el tiempo.
2. Al final, haz **una sola sesión en GCP** para varios lotes juntos: un `terraform apply`, una construcción de la imagen, un arranque del job y todas las verificaciones.

Relanzar Dataflow es lo que más tiempo cuesta (5 a 8 minutos), y hace falta cada vez que cambia el código. Agrupando, se hace una vez.

---

## 1. Local

### 1.1 Encender (una vez; queda encendido)

Requisito: Docker Desktop abierto, con WSL integrado.

```
bash scripts/local_dataflow.sh preflight
bash scripts/local_dataflow.sh up
```

Esto levanta el emulador de Pub/Sub, el simulador y `dataflow-local` (el pipeline con DirectRunner). Tarda 1 a 2 minutos la primera vez. Después, cada arranque es de segundos.

**Con NLP (Lote 2.3):** la primera vez hay que construir la imagen. Tarda 10 a 15 minutos y descarga unos 2,5 GB.

```
bash scripts/local_nlp.sh build
bash scripts/local_nlp.sh up
```

`local_nlp.sh up` reemplaza el `dataflow-local` normal por el que trae los modelos. Para volver al normal:

```
bash scripts/local_nlp.sh restore
```

### 1.2 Comprobar que está encendido

```
docker compose -f docker-compose.yml -f docker-compose.dataflow.yml --profile dataflow ps
```

Esperado: `pubsub-emulator`, `simulator` y `dataflow-local` en `running`; `pubsub-init` en `exited (0)`, porque solo crea los topics y termina.

Para ver que fluyen datos (salir con Ctrl+C):

```
bash scripts/local_dataflow.sh logs
```

Y que se escriben archivos:

```
find .local-lake/raw -name 'part-*' | tail -3
find .local-lake/enriched -name 'part-*.parquet' | tail -3
```

### 1.3 Tras copiar código nuevo de un lote (sin apagar nada)

El código está montado en el contenedor, así que no hay que reconstruir ni apagar. Basta con reiniciar solo el pipeline:

```
docker compose -f docker-compose.yml -f docker-compose.dataflow.yml --profile dataflow restart dataflow-local
```

Con NLP, en cambio:

```
bash scripts/local_nlp.sh up
```

Espera 2 a 3 minutos (ventanas de 60 s) antes de verificar.

### 1.4 Si el lake local crece demasiado

Vacía `.local-lake/` sin apagar nada:

```
bash scripts/local_dataflow.sh reset-lake
```

### 1.5 Apagar lo local (opcional; no cuesta dinero)

Hay dos niveles:

```
bash scripts/local_dataflow.sh down
```

Para solo el pipeline. El emulador y el simulador siguen.

```
bash scripts/local_dataflow.sh down-all
```

Apaga todo lo local. El cursor del simulador (volumen `sim-state`) y `.local-lake/` se conservan. Al volver a encender, el emulador arranca vacío (vive en memoria) y el simulador continúa desde su cursor.

---

## 2. GCP

### 2.1 Preparar la terminal (cada sesión)

```
export GCP_PROJECT=project-0568d9ba-848f-413c-88b
export GCP_REGION=us-central1
gcloud config set project project-0568d9ba-848f-413c-88b
gcloud auth application-default print-access-token > /dev/null && echo credenciales-ok
```

Si no imprime `credenciales-ok`:

```
gcloud auth login
gcloud auth application-default login
```

### 2.2 Ver qué está encendido (solo lectura; úsalo siempre antes y después)

```
bash scripts/preflight.sh
gcloud dataflow jobs list --region us-central1 --status=active --format="value(name,id,currentState)"
gcloud run services list --region us-central1 --format="value(name)"
grep -n "simulator_running" infra/gcp/environments/dev/terraform.tfvars
```

| Si ves… | Significa |
|---|---|
| Un job en la lista de Dataflow | Dataflow encendido (cobra) |
| `ta-stream-simulator` en Cloud Run | Simulador encendido (cobra) |
| `simulator_running = true` | Terraform lo mantiene encendido en el próximo `apply` |

### 2.3 Infraestructura del lote (una vez por lote nuevo; luego queda)

Solo cuando copiaste archivos de Terraform nuevos (`.tf`). No cobra de forma apreciable y **no se apaga**.

```
bash infra/validate.sh
terraform -chdir=infra/gcp/environments/dev plan -out=tfplan
terraform -chdir=infra/gcp/environments/dev apply tfplan
```

Revisa el `plan` antes de aplicar: debe **crear** lo del lote y no **destruir** nada.

**Una sola vez en la vida del proyecto** (Lote 2.2), la llave HMAC:

```
bash scripts/hmac_key.sh status
bash scripts/hmac_key.sh init
```

`init` se niega si ya existe una versión, así que es seguro repetirlo por error.

**Una vez por cambio en el NLP** (Lote 2.3), la imagen de los workers. Tarda 15 a 25 minutos en Cloud Build.

```
bash scripts/build_nlp_image.sh cloudbuild
```

Al final imprime la línea `export TA_NLP_IMAGE=…:<commit>`. Guárdala; la usas en 2.5.

### 2.4 Encender el simulador (puede quedar encendido varios días)

Cambia `simulator_running` a `true` en `terraform.tfvars` y aplica:

```
sed -i 's/^simulator_running\( *\)= *false/simulator_running\1= true/' infra/gcp/environments/dev/terraform.tfvars
grep -n "simulator_running" infra/gcp/environments/dev/terraform.tfvars
terraform -chdir=infra/gcp/environments/dev apply
```

El `apply` solo debe crear el servicio de Cloud Run (`1 to add`). Escribe `yes` si es así.

Comprobar, unos 2 minutos después:

```
gcloud run services describe ta-stream-simulator --region us-central1 --format="value(status.conditions[0].status)"
```

Esperado: `True`. El simulador publica unos 0,5 mensajes por segundo por fuente.

### 2.5 Encender Dataflow (una vez por sesión de trabajo)

Si quedó un job de antes, primero drénalo (ver 3.1).

Elige **una** de estas dos opciones según qué lotes vas a validar:

- **Con NLP (Lote 2.3, la imagen ya está publicada):**

  ```
  unset TA_NLP_ENABLED
  export TA_NLP_IMAGE=us-central1-docker.pkg.dev/project-0568d9ba-848f-413c-88b/ta-images/dataflow-nlp:<commit>
  ```

- **Sin NLP (validando solo el Lote 2.2, o la imagen aún no existe):**

  ```
  export TA_NLP_ENABLED=false
  ```

Luego arranca el job:

```
bash scripts/dataflow_job.sh preflight
bash scripts/dataflow_job.sh start
bash scripts/dataflow_job.sh status
```

Pasa a *Running* en 2 minutos (5 a 8 con la imagen de NLP). Los archivos de `enriched/` aparecen unos 10 minutos después (ventanas de 300 s).

**Cuándo relanzar:** cada vez que copias código nuevo del pipeline (`src/streaming/` o `src/common/`). Dataflow no toma el código nuevo en un job que ya está corriendo. Es drenar (3.1) y volver a hacer `start`.

### 2.6 Atajo: todo lo de la nube en un bloque

Con la infraestructura ya aplicada (2.3), encender todo para una sesión es:

```
bash scripts/preflight.sh
sed -i 's/^simulator_running\( *\)= *false/simulator_running\1= true/' infra/gcp/environments/dev/terraform.tfvars
terraform -chdir=infra/gcp/environments/dev apply -auto-approve
bash scripts/dataflow_job.sh start
bash scripts/dataflow_job.sh status
```

Antes de `start`, exporta `TA_NLP_IMAGE` o `TA_NLP_ENABLED=false` (2.5).

`-auto-approve` aplica sin preguntar. Úsalo solo si ya revisaste ese `plan` antes y no copiaste archivos `.tf` nuevos desde entonces.

### 2.7 Exportador GCS → S3 (Lote 3.2)

El job `ta-gcs-to-s3` copia `enriched/` a `s3://ta-dev-648617469176-lake/raw/origin=stream/`. Solo cobra mientras corre; lo dispara el Scheduler `ta-gcs-to-s3-cada-15-min`, que se crea **en pausa**. Terraform no toca ese estado, así que pausar o reanudar no cambia los planes de `on.py`/`off.py`. Desde la raíz del repo:

```
python project/scripts/validate_3_2.py status
```

Muestra el estado del Scheduler (`PAUSED` = ahorro), las ejecuciones en curso y el watermark.

Encender (exportar cada 15 min):

```
gcloud scheduler jobs resume ta-gcs-to-s3-cada-15-min --location us-central1
```

Una sola corrida a mano, sin encender el Scheduler:

```
gcloud run jobs execute ta-gcs-to-s3 --region us-central1 --wait
```

Apagar y confirmar (pausa el Scheduler, cancela ejecuciones en curso y lo comprueba):

```
python project/scripts/validate_3_2.py off
```

Sin el stream encendido no hay nada nuevo en `enriched/`: con el Scheduler encendido, cada ejecución lista, no sube nada y cobra igual (medido: 36–118 s por ejecución, ≈ 0,0016 USD cada una).

### 2.8 Lakehouse de AWS: horario del workflow de Glue (Lote 4.1)

El horario `ta-wf-raw-to-curated-cada-hora` (EventBridge Scheduler, `rate(1 hour)`) arranca el workflow `ta-wf-raw-to-curated` (Raw → Trusted → Curated). Se crea **deshabilitado** y Terraform no toca ese estado. Mientras los jobs sean los marcadores de posición del 4.1 no producen Trusted ni Curated: encenderlo solo sirve para probar. Desde la raíz del repo, en WSL con el perfil de AWS:

```
python project/scripts/validate_4_1.py status
```

Muestra el estado del horario (`DISABLED` = ahorro), las corridas y job runs en curso y los horarios de prueba que hayan quedado.

Encender (pide confirmación y muestra el costo por día):

```
python project/scripts/validate_4_1.py on
```

Una sola corrida a mano, sin encender el horario:

```
aws glue start-workflow-run --name ta-wf-raw-to-curated --region us-east-1
```

Apagar y confirmar (deshabilita el horario, detiene corridas y job runs, borra los horarios de prueba y las marcas `_ta_placeholder/`):

```
python project/scripts/validate_4_1.py off
```

El horario no se enciende con un comando de una línea de `aws scheduler`: `update-schedule` exige reenviar toda su definición (si se omite la política de reintentos, vuelve a 185 reintentos). Use `on`/`off` o el botón *Enable/Disable* de la consola de EventBridge Scheduler.

---

## 3. Apagar la nube (para no gastar)

### 3.1 Al terminar cada día: Dataflow

```
bash scripts/dataflow_job.sh drain
```

`drain` cierra las ventanas abiertas y no pierde datos; tarda unos minutos. Comprueba que terminó:

```
gcloud dataflow jobs list --region us-central1 --status=active --format="value(name,currentState)"
```

Esperado: vacío. Si queda en *Draining* más de 15 minutos:

```
bash scripts/dataflow_job.sh cancel
```

`cancel` puede perder la última ventana sin cerrar, unos minutos de `raw/` y `enriched/`. Los mensajes se conservan en Pub/Sub (7 días) y el siguiente job los procesa.

### 3.2 Al terminar la semana de validaciones: el simulador

```
sed -i 's/^simulator_running\( *\)= *true/simulator_running\1= false/' infra/gcp/environments/dev/terraform.tfvars
grep -n "simulator_running" infra/gcp/environments/dev/terraform.tfvars
terraform -chdir=infra/gcp/environments/dev apply
```

El `apply` solo debe destruir el servicio de Cloud Run (`1 to destroy`). Escribe `yes` si es así. El cursor se conserva en GCS: al encenderlo de nuevo, continúa donde iba.

### 3.3 Restos temporales de los verificadores

Normalmente se borran solos. Si cortaste un verificador con Ctrl+C, pueden quedar suscripciones temporales:

```
gcloud pubsub subscriptions list --format="value(name)" | grep ta-verify
```

Si sale alguna, bórrala con este comando (expiran solas a 1 día de todos modos):

```
gcloud pubsub subscriptions delete NOMBRE
```

Cambia `NOMBRE` por el que salió, sin la parte `projects/…/subscriptions/`.

Y que no quede una construcción corriendo:

```
gcloud builds list --region us-central1 --ongoing
```

### 3.4 Confirmar que nada cobra

```
gcloud dataflow jobs list --region us-central1 --status=active
gcloud run services list --region us-central1 --format="value(name)"
gcloud builds list --region us-central1 --ongoing
docker ps --filter name=ta-hist-nlp --format "{{.Names}}"
gcloud scheduler jobs describe ta-gcs-to-s3-cada-15-min --location us-central1 --format="value(state)"
aws scheduler get-schedule --name ta-wf-raw-to-curated-cada-hora --region us-east-1 --query State --output text
```

Esperado: las cuatro primeras salidas vacías (o sin `ta-stream-simulator` en la segunda), `PAUSED` en la quinta (o `NOT_FOUND` si el job del Lote 3.2 todavía no se creó) y `DISABLED` en la sexta (o `ResourceNotFoundException` si el Lote 4.1 todavía no se aplicó). Lo que queda creado (topics, buckets, BigQuery, secreto, imagen) cuesta centavos al mes y **no** se borra: perder el secreto cambiaría todos los `user_hash`.

---

## 4. Flujo recomendado para validar un lote

| Paso | Dónde | Qué encender | Qué hacer |
|---|---|---|---|
| 1 | Local | Nada nuevo: ya está encendido (1.1) | Copiar los archivos del lote |
| 2 | Local | — | Partes A y B (pruebas, sin Docker) |
| 3 | Local | Reiniciar solo el pipeline (1.3) | Parte D (local) del lote |
| 4 | — | — | Repetir 1 a 3 con el siguiente lote, sin apagar nada |
| 5 | GCP | Infraestructura de todos los lotes juntos (2.3) | Un `plan` y un `apply` |
| 6 | GCP | Simulador (2.4) y Dataflow (2.5) **una vez** | Parte E de cada lote, uno tras otro, sobre el mismo job |
| 7 | GCP | Apagar Dataflow (3.1) al final del día | — |
| 8 | GCP | Apagar el simulador (3.2) al cerrar la semana | Confirmar (3.4) |

Si en el paso 6 falla algo, se aísla el lote culpable sin rehacer la sesión. Por ejemplo, con el NLP: `export TA_NLP_ENABLED=false`, drenar y `start`.

---

## 5. Problemas frecuentes

| Síntoma | Causa | Qué hacer |
|---|---|---|
| El verificador ve 0 mensajes en GCP | El simulador está apagado | 2.2 y 2.4 |
| `enriched/` vacío en GCP tras 10 min | El job no está corriendo o corre código viejo | 2.2; si el código cambió, drenar y `start` (2.5) |
| El job se queda en *Pending* con NLP | La imagen no existe o falta el permiso de Artifact Registry | 2.3 (`apply` del Lote 2.3 y `build_nlp_image.sh`) |
| `terraform apply` propone destruir cosas que no son el simulador | Archivos `.tf` cambiados o borrados | **No aplicar**; revisar con `git status` |
| `validate.sh` marca `terraform.tfvars` | Espacios desalineados al editar | `terraform fmt infra/gcp/environments/dev/terraform.tfvars` |
| Local: `find` no encuentra archivos | Ruta con `project/` repetido (ya estás dentro de `project/`) | Usar `.local-lake/…` |
| Local: no llegan datos tras reiniciar el portátil | Docker Desktop cerrado o los contenedores se detuvieron | Abrir Docker Desktop y `bash scripts/local_dataflow.sh up` |
| El Scheduler del exportador quedó `ENABLED` sin querer | Se reanudó para una prueba | `python project/scripts/validate_3_2.py off` (2.7) |
| `on.py`/`off.py` se niegan por cambios del exportador en el plan de GCP | Hay archivos del Lote 3.2 o una imagen nueva sin aplicar | Terminar la parte C/E de `validation-3-2.md` y repetir |
| El horario del lakehouse quedó `ENABLED` sin querer | Se encendió para una prueba | `python project/scripts/validate_4_1.py off` (2.8) |
| Aparecen objetos en `trusted/_ta_placeholder/` o `curated/_ta_placeholder/` | El workflow corrió con los marcadores de posición del 4.1 (a mano o con el horario) | `python project/scripts/validate_4_1.py off` los borra; no son datos |
| `off.py` dice "simulador: sin cambios" pero `status.py` lo sigue mostrando `ENCENDIDO` | El servicio existe en GCP pero no en el estado de Terraform (`terraform -chdir=infra/gcp/environments/dev state list \| grep module.simulator` no muestra nada), así que con `simulator_running = false` no hay nada que borrar (visto en v2.31) | Con `simulator_running = false`: `gcloud run services delete ta-stream-simulator --region us-central1 --quiet`. Terraform queda alineado y `on.py` lo vuelve a crear; el cursor sigue en GCS |
