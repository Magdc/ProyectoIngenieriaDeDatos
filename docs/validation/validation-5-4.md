# Validación del Lote 5.4 · HU-048

> **Prueba de humo de extremo a extremo** (ADR-30). `project/scripts/smoke_e2e.py run` publica en Pub/Sub un
> **comentario centinela normal** (`generator = comment-gen@1.0`, el `EventFactory` del simulador con la semilla
> reservada 990048, decisión del usuario del 27-oct-2026) con un `event_id` conocido y lo sigue por BigQuery
> `posts_enriched`, GCS `enriched/`, S3 `raw/origin=stream/`, `ta_trusted.posts` y `ta_curated.fact_posts` (Athena),
> midiendo la latencia de cada etapa contra su objetivo (§5.12) y comprobando que no aparezcan `user_id` ni `handle`
> después de Dataflow. Antes publica un **calentamiento** (`ta-verify@2.1`, que Trusted descarta) para que Dataflow
> cargue los modelos de NLP. Contrato en `docs/ARCHITECTURE.md` §3.26 (interfaces en §3.26.1).
>
> **Automático:** `python project/scripts/validate_5_4.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_5_4.json`; la prueba
> de humo, su informe en `project/evidencia/smoke_e2e_5_4.{json,md}` (insumo de HU-044 y HU-045).
>
> **Cambia la nube** (con el OK del usuario del 27-oct-2026): `prepare` descarta con *seek* lo pendiente en
> `ta-raw-*-df` (el atraso del 24-oct, ~73.600 mensajes; irreversible) y enciende Dataflow con NLP (≈ 0,25 USD/h hasta
> `off`); `smoke` publica 2 mensajes, ejecuta el exportador una vez y arranca una corrida del workflow; `off` drena
> Dataflow. El simulador queda apagado, el exportador en pausa y el horario del lakehouse encendido.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-048 · `scripts/smoke_e2e.py` emite un evento centinela con `event_id` conocido | L1 (contrato), E2 (publicado y visto), E8 (`event_id` en la evidencia) |
| HU-048 · Aparece en BigQuery `posts_enriched` (< 1 min), en `enriched/` de GCS, en `raw/origin=stream/` de S3 y en `ta_curated.fact_posts` desde Athena | E2, E3, E4, E6 (y E5: `ta_trusted.posts`) |
| HU-048 · Reporta la latencia de cada etapa y falla si alguna excede su objetivo | E2–E6 (objetivos de §5.12, L2) |
| HU-048 · Sin `handle` ni `user_id` en ningún punto posterior a Dataflow | E7 (columnas y claves de `payload` en las 5 etapas; mismo `user_hash`) |
| HU-048 · Resultados como evidencia para HU-044 y HU-045 | E8 (`smoke_e2e_5_4.{json,md}`, sin datos personales) |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | S3, Glue y Athena |
| ADC de GCP | `~/.config/gcloud/application_default_credentials.json` | Pub/Sub, BigQuery y GCS desde Python |
| `gcloud` con sesión | Proyecto `project-0568d9ba-848f-413c-88b` (o `--project`) | Dataflow, *seek*, exportador, Scheduler |

### Preparación

1. Terminal en WSL:
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   export AWS_PROFILE=default
   aws sts get-caller-identity --query Account --output text
   ```
   Esperado: `648617469176`.
2. `git status --short` y `git log -1 --oneline`: el commit del lote en `main`, sin cambios.
3. Los eventos que publicaría la prueba, sin nube:
   ```bash
   python project/scripts/smoke_e2e.py plan
   ```
   Esperado: una línea `smoke_plan` con `"contract": "ok"`, `"generator": "comment-gen@1.0"`, `"warmup_generator": "ta-verify@2.1"` y `"warmup_contract": "ok"`.

### Ejecución

```bash
python project/scripts/validate_5_4.py preflight
python project/scripts/validate_5_4.py tests
python project/scripts/validate_5_4.py local
python project/scripts/validate_5_4.py prepare --yes   # seek + Dataflow con NLP (5–10 min hasta Running)
python project/scripts/validate_5_4.py smoke --yes     # ≈ 15–30 min
python project/scripts/validate_5_4.py off --yes       # drena Dataflow (hasta 15 min; --force lo cancela)
python project/scripts/validate_5_4.py status
```

Salida esperada (resumen): cada parte termina con `-- Parte X: OK` y código 0. Si `smoke` falla, **corra igual
`off --yes`**: Dataflow cobra hasta que se drena.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, B, L | Nada encendido (A lee el estado de GCP y AWS) | 0 |
| P | `gcloud`, Beam en `ta-streaming`, la imagen `dataflow-nlp` publicada | Dataflow ≈ 0,25 USD/h desde aquí hasta O |
| E | Dataflow en Running (P), ADC de GCP, perfil de AWS | 2 mensajes; consultas de BigQuery (< 1 centavo); 1 ejecución del exportador (≈ 0,0016 USD); 1 corrida del workflow (≈ 0,0196 USD); 2 consultas de Athena |
| O | `gcloud` | Deja de cobrar |

**Estado final:** Dataflow apagado, simulador apagado, exportador `PAUSED`, horario del lakehouse `ENABLED` (estado
normal). Quedan en el lake el centinela (1 fila en Trusted y Curated) y el calentamiento (en BigQuery, `enriched/` y
Raw; Trusted lo descarta).

---

## Parte A · Preflight

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 con Pub/Sub, BigQuery, GCS, boto3, pyarrow, jsonschema y Beam |
| A2 | Los 9 archivos del lote y los 7 que reutiliza |
| A3 | `gcloud`, `aws`, `bash` |
| A4 | Proyecto de GCP y ADC presentes |
| A5 | Dataflow apagado (o encendido de antes), **simulador apagado** y la imagen `dataflow-nlp` publicada |
| A6 | Exportador `PAUSED` (la prueba ejecuta el job a mano) |
| A7 | Horario del lakehouse `ENABLED` |

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `src/smoke`: **11** passed (pipeline falso: corrida completa, etapa fuera de objetivo, evento que no llega, identidad en claro, bytes distintos en S3, workflow ocupado, job fallido, sin Dataflow, utilidades, CLI) |
| B2 | `test_ops_validate54.py`: **14** passed |
| B3 | `contracts/tests`: 18 |
| B4 | `test_ops_validate53.py`: 20 (de ahí sale `leak_problems`) |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | El centinela (`comment-gen@1.0`, 1 mensaje con los atributos de §5.3) y el calentamiento (`ta-verify@2.1`, que Trusted descarta) cumplen `contracts/envelope.v1.schema.json` |
| L2 | Los objetivos del código = §5.12: `bigquery ≤ 60 s desde published; gcs_enriched ≤ 900 s desde published; s3_raw ≤ 300 s desde gcs_enriched; trusted ≤ 900 s desde s3_raw; curated ≤ 600 s desde trusted` |

## Parte P · Preparar la nube

| Paso | Esperado |
|---|---|
| P1 | Con Dataflow apagado: `gcloud pubsub subscriptions seek` de `ta-raw-{reddit,facebook,instagram}-df` a la hora actual (lo anterior queda descartado). Con Dataflow ya encendido: OMITIDO |
| P2 | `dataflow_job.sh start` con la última imagen `dataflow-nlp` (`services.nlp_env`) |
| P3 | El job `ta-stream-dev-…` en Running |

## Parte E · Prueba de humo

| Paso | Esperado |
|---|---|
| E1 | El calentamiento llega a BigQuery (su latencia incluye la carga de los modelos; no se juzga) |
| E2 | BigQuery `posts_enriched`: ≤ 60 s desde la publicación |
| E3 | GCS `enriched/`: el Parquet con el `event_id` ≤ 900 s desde la publicación (ventanas de 300 s; la hora es la de creación del objeto) |
| E4 | S3 `raw/origin=stream/`: el mismo archivo, **mismos bytes** que en GCS, ≤ 300 s después de aparecer en GCS (la prueba ejecuta `ta-gcs-to-s3` con `--wait`) |
| E5 | `ta_trusted.posts` en Athena: ≤ 900 s desde S3 (arranca una corrida del workflow; si hay otra en curso, espera y arranca la suya; la hora es el fin de `ta-job-raw-to-trusted`) |
| E6 | `ta_curated.fact_posts` en Athena: ≤ 600 s desde Trusted (fin de `ta-job-trusted-to-curated`) |
| E7 | Sin `user_id` ni `handle` (columnas ni claves de `payload`) en las 5 etapas y el mismo `user_hash` hex de 64 en todas |
| E8 | `smoke_e2e_5_4.json` y `.md` escritos, sin correos, llaves, URLs prefirmadas ni hashes de 64 hex |

## Parte O · Apagado

| Paso | Esperado |
|---|---|
| O1 | Dataflow drenado (con `--force`, cancelado si sigue drenando a los 15 min); horas encendido y costo estimado |
| O2 | Exportador `PAUSED` (si quedó `ENABLED`, lo pausa) |
| O3 | Simulador apagado |
| O4 | Horario del lakehouse `ENABLED` |
| O5 | Sin suscripciones temporales `ta-verify-*` |

## Status

| Paso | Esperado |
|---|---|
| S1 | Dataflow apagado |
| S2 | Exportador `PAUSED` y horario `ENABLED` |
| S3 | La última prueba de humo: OK, con su centinela y su total |

## Cierre

**Cerrado en v2.46** (27-oct-2026, WSL; `main` = `1f1fedc`; `ta-streaming`, ADC de GCP y perfil `default`; con el OK
del usuario para la nube).

| Parte | Resultado |
|---|---|
| A · Preflight | OK (7): dependencias; 9 + 7 archivos; ADC; Dataflow y simulador apagados, imagen `dataflow-nlp:20261022224102`; exportador `PAUSED`; horario `ENABLED` |
| B · Pruebas | OK (4): B1 11, B2 14, B3 18, B4 20 |
| L · Local | OK (2): centinela y calentamiento válidos; objetivos = §5.12 |
| P · Preparar | OK (3): *seek* de las 3 suscripciones a 16:22:57Z; Dataflow `2026-10-27_09_23_26-4213968076598262329` con NLP en Running |
| E · Prueba de humo | OK (8): `smoke-20261027T162356Z`, centinela `4e2712e1-0004-56bc-8606-209103050e96`. Calentamiento 229,1 s; **BigQuery 19,1 s** (≤ 60); **GCS 175,1 s** (≤ 900); **S3 78,6 s** desde GCS (≤ 300; mismos bytes); **Trusted 149,1 s** desde S3 (≤ 900); **Curated 62,4 s** desde Trusted (≤ 600); **total 465,2 s**; identidad limpia en las 5 etapas; informe sin datos personales |
| O · Apagado | OK (5): Dataflow drenado (≈ 0,26 h ≈ 0,07 USD); exportador `PAUSED`; simulador apagado; horario `ENABLED`; sin `ta-verify-*` |
| `status` | OK (3) |

Cifras también en `ARCHITECTURE.md` §3.26, `CHANGELOG.md` v2.46 y `project/evidencia/smoke_e2e_5_4.md`. Nada quedó
encendido.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| A5 «imagen NLP: FALTA» | `bash project/scripts/build_nlp_image.sh cloudbuild` (o lance sin NLP: el centinela llega igual, sin sentimiento) |
| P3 FALLA | `bash project/scripts/dataflow_job.sh status`; con la imagen de NLP tarda 5–8 min en Running |
| E1 FALLA (el calentamiento no llega en 20 min) | Dataflow no está leyendo: revise que P1 haya movido las suscripciones y los logs del job; luego `off` |
| E2 > 60 s con el calentamiento OK | Repita `smoke` (el job ya está caliente); si persiste, mire la etapa NLP del job (§3.15) |
| E3 FALLA | Los archivos de `enriched/` se escriben al cerrar cada ventana de 300 s (+ un panel tardío): espere y mire `gs://…-ta-lake/enriched/source=reddit/dt=…/hr=…/` |
| E4 FALLA «no apareció» | `gcloud logging read` del job `ta-gcs-to-s3` (`export_done`, §3.18.1): watermark, `candidates`, `failed` |
| E5/E6 FALLA | `python project/scripts/validate_4_4.py status` y el reporte de la corrida en `quality/_latest/` |
| O1 «sigue drenando» | `python project/scripts/validate_5_4.py off --yes --force` |
