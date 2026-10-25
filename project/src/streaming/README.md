# Pipeline de streaming · Lote 2.1

Validación, deduplicación, DLQ y `raw/` (HU-018, HU-019). Contrato y decisiones en
`docs/ARCHITECTURE.md` §3.13; nombres en §5.3–§5.5.

```
ta-raw-{source}-df ─► Validate ─┬─► Dedup(event_id) ─► raw/source=…/dt=…/hr=…/part-*.jsonl.gz
                                └─► DLQ ─┬─► ta-dlq (Pub/Sub, cuerpo original + atributos dlq_*)
                                         ├─► dlq/dt=…/part-*.jsonl.gz
                                         └─► ta_stream.dlq_events (opcional; la tabla llega en el Lote 2.4)
```

Todos los comandos se corren desde `project/`.

## Pruebas (sin nube)

```bash
python -m pip install -r src/streaming/requirements.txt pytest
python -m pytest src/streaming -q
```

Las pruebas que usan `contracts/` (validación del centinela y de los ejemplos) se omiten si falta
la carpeta. Ninguna usa red ni credenciales.

## Local: DirectRunner contra el emulador

Se suma al entorno del Lote 1.5 (`docker-compose.yml`) con `docker-compose.dataflow.yml`.

### Preflight

| Debe estar | Cómo se comprueba | Salida esperada |
|---|---|---|
| Docker Desktop abierto (WSL) | `bash scripts/local_dataflow.sh preflight` | `✓ preflight OK` y la tabla de contenedores |
| Nada de GCP | — | El entorno local no usa credenciales; el simulador de Cloud Run puede seguir apagado |

### Arranque, verificación y apagado

```bash
bash scripts/local_dataflow.sh up          # emulador + simulador + dataflow-local (~3-5 min la primera vez: instala Beam)
bash scripts/local_dataflow.sh check       # centinela ×2 y un defecto por tipo; espera hasta 240 s
bash scripts/local_dataflow.sh report      # resumen de raw/ y dlq/ de las últimas 2 h
bash scripts/local_dataflow.sh down-all    # apaga todo el entorno local (el lake queda en .local-lake/)
```

`check` deja `evidencia/local_streaming_check.json`; `report`, `evidencia/local_streaming_report.json`.
Los archivos aparecen al cerrar cada ventana (60 s en local). `.local-lake/` lo crea el script con su
usuario; no se versiona.

Limitación del DirectRunner: si se para el contenedor, se pierde la ventana abierta (el DirectRunner
confirma los mensajes al leerlos). En Dataflow, `drain` sí la cierra.

## GCP: Dataflow

### Preflight (solo lectura)

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b GCP_REGION=us-central1
bash scripts/dataflow_job.sh preflight
```

Debe dar `Preflight OK`: Python 3.11 con `src/streaming/requirements.txt`, las tres suscripciones
`ta-raw-*-df`, `ta-dlq`, el bucket `…-ta-lake` y `sa-dataflow`. Informa si hay un job activo y si el
simulador está encendido. Para validar con tráfico real, encender antes el simulador
(`simulator_running = true` → `terraform apply`, `infra/DEPLOY_SIMULATOR.md`).

### Arranque y verificación

```bash
bash scripts/dataflow_job.sh start          # e2-standard-2, 1 worker, Streaming Engine, ventana 300 s
bash scripts/dataflow_job.sh status         # Running a los 3–5 min; contadores valid_*, dlq_*, duplicates_dropped
python scripts/verify_streaming.py check --allow-cloud --timeout 900 \
       --json-out evidencia/streaming_check.json
python scripts/verify_streaming.py report --hours 1 --json-out evidencia/streaming_report.json
```

`check` en la nube publica 7 mensajes de prueba en los topics reales (centinela con
`generator = ta-verify@2.1` y semilla 990001, que no choca con el simulador) y crea una suscripción
temporal `ta-verify-dlq-{run}` sobre `ta-dlq` que borra al terminar (expira sola a 1 día). Necesita
ADC con permiso para publicar, crear suscripciones y leer el bucket (Owner o Editor).

### Apagado

```bash
bash scripts/dataflow_job.sh drain          # cierra las ventanas abiertas y termina; espera hasta 15 min
bash scripts/dataflow_job.sh status         # no debe listar ningún job
```

Y, si se encendió para la prueba, volver a `simulator_running = false` → `terraform apply`.
Un job de streaming con 1 × e2-standard-2 y Streaming Engine cuesta del orden de 0,25 USD por hora
(≈ 6 USD al día); el presupuesto avisa pero no corta (§5.12).

### Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| El job falla al arrancar con `pubsub.subscriptions.get` denegado | A `sa-dataflow` le falta permiso de lectura de metadatos de la suscripción | `gcloud projects add-iam-policy-binding $GCP_PROJECT --member serviceAccount:sa-dataflow@$GCP_PROJECT.iam.gserviceaccount.com --role roles/pubsub.viewer` y repetir; luego declararlo en Terraform |
| `Workflow failed` por red | El proyecto no tiene red `default` | `start --network RED --subnetwork URL` |
| `check` termina por tiempo con `raw/` vacío | Job aún no está en Running, o la ventana (300 s) no ha cerrado | `status`; subir `--timeout` |
| Errores de versión de Python en el worker | El job se lanzó con un Python distinto de 3.11 | Lanzar desde el entorno 3.11 (`PYTHON=python3.11`) |
| Queda `src/ta_streaming.egg-info/` | Lo crea el empaquetado de `--setup_file` | `start` lo borra; agregar `*.egg-info/` al `.gitignore` |
