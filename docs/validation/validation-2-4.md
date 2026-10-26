# Validación del Lote 2.4 · HU-005, HU-023

> Ventanas de tendencias (`trend_windows`), `dlq_events`, `dq_results`, permiso `pubsub.viewer`,
> latencia p95 y plantilla Flex de Dataflow. Contrato en `docs/ARCHITECTURE.md` §3.16; guía del
> módulo en `project/src/streaming/TRENDS.md`.
>
> **Automático:** `python project/scripts/validate_2_4.py <parte>` desde la raíz del repo, con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_2_4.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-005 · `ta_stream` con `posts_enriched`, `trend_windows`, `dlq_events` y `dq_results` | C2–C4, E2 |
| HU-005 · `posts_enriched` particionada y con *clustering*; secreto solo para `sa-dataflow` | Lote 2.2 (ya validado) |
| HU-005 · plantilla de Dataflow parametrizada con los outputs de Terraform | F1–F8 |
| HU-018 · inválidos también en `dlq_events` (pendiente del 2.1) | E6 |
| HU-023 · ventanas de 5 min, paso 1 min, por marca × fuente, con sus columnas | B1, D5, E7 (T1, T3, T4) |
| HU-023 · p95 `process_time − event_time` < 5 min | E7 (T7) |
| HU-023 · tardíos hasta 30 min; más tarde, `late_events` sin recalcular | B1 (`test_trends_pipeline.py`, `test_trends_config.py`) |
| HU-023 · arranque en frío: sin alertas antes de 60 ventanas | B1, E7 (T5) |
| Pendiente del 2.4: `roles/pubsub.viewer` para `sa-dataflow` | C2, E10 |

## Antes de empezar

1. Copie los archivos del lote en sus rutas (`project/…` y `docs/…`).
2. **Enganche** (`TRENDS.md` §2): en `project/src/streaming/pipeline.py`,
   ```python
   from streaming.trends import attach_trends            # Lote 2.4
   ...
   enriched_rows = attach_enriched(unique)
   attach_trends(enriched_rows)                           # Lote 2.4
   ```
   y, si `attach_enriched()` no devuelve nada, `return rows` al final de esa función en
   `enriched.py`. La parte A lo comprueba.
3. Terminal en WSL, en la raíz del repo, con `source ~/.venvs/ta-streaming/bin/activate`
   (Python 3.11.17, Beam 2.77.0). Si `from pathlib import Path` da `FileNotFoundError`, vuelva a
   entrar a la carpeta con `cd` (ver v2.25).

## Antes de E y F: sin atraso

Si el job estuvo apagado con el simulador encendido, las suscripciones `ta-raw-*-df` acumulan mensajes y el job nuevo arranca atrasado: T4, T7 y T8 miden ese atraso y fallan. En desarrollo, descártelo justo antes de lanzar (los mensajes son sintéticos):

```bash
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
for s in ta-raw-reddit-df ta-raw-facebook-df ta-raw-instagram-df ta-dlq-df; do
  gcloud pubsub subscriptions seek $s --time=$NOW --project=project-0568d9ba-848f-413c-88b
done
```

Con esto, la parte F pasó completa (v2.27).

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada | Nube, Docker | 0 |
| C | Credenciales de GCP (ADC) | Simulador, Dataflow | 0 |
| D | Docker Desktop; imagen `ta-dataflow-nlp-local:local` (si falta se construye) | Nube | 0 |
| E | Simulador encendido; tablas de la parte C | Docker | ~0,25 USD/h de Dataflow (~40 min) |
| F | Lo de E; Cloud Build | Docker | ~0,10 USD de Cloud Build + Dataflow |

Compruebe el estado con `python project/scripts/status.py`. No asuma nada encendido: E3 falla
si el simulador está apagado (enciéndalo como indica `docs/ACTIVE_SERVICES.md`).

---

## Parte A · Preflight del código

```bash
python project/scripts/validate_2_4.py preflight
```

| Paso | Esperado |
|---|---|
| A1 | Python 3.11, Beam 2.77.0, `google-cloud-bigquery` importable |
| A2 | Los 20 archivos del lote presentes |
| A3 | Los archivos de los lotes 2.1 a 2.3 presentes |
| A4 | `pipeline.py` llama a `attach_trends` con lo que devuelve `attach_enriched` |
| A5 | `gcloud`, `terraform`, `docker`, `bash` |
| A6 | Salida de `status.py` |

## Parte B · Pruebas

```bash
python project/scripts/validate_2_4.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/streaming/tests/test_trends_*.py src/streaming/tests/test_flex_template.py src/ops/tests/test_ops_validate24.py -q -rs` | **224 passed, 0 skipped** |
| B2 | `python -m pytest src/streaming -q` | **362 passed** (190 de los lotes 2.1–2.3 + 172 del 2.4) |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/ops/tests -q` | **89 passed** (37 del 2.3 + 52 del 2.4) |

Si B1 tiene omitidas, el detalle dice cuál falta (`contracts/`, `pipeline.py`, `platform.tf`,
`common.label_map`). Los avisos `NOT NULL en la DDL y NULLABLE en Terraform (compatible)` son
esperables: Terraform deja nulas las columnas que el código nunca deja nulas, como en el 2.2.

## Parte C · Terraform

```bash
python project/scripts/validate_2_4.py terraform            # validate.sh + plan
python project/scripts/validate_2_4.py terraform --apply    # aplica (pide confirmación)
```

El plan va limitado con `-target` a los recursos del lote, para que la deriva de otros lotes
(por ejemplo `simulator_running`) no se aplique por accidente (`--full-plan` lo quita).

| Paso | Esperado |
|---|---|
| C1 | `bash project/infra/validate.sh` en verde |
| C2 | **10 a crear, 0 a cambiar, 0 a destruir**: 3 tablas, la vista, `ta-dlq-df`, 4 × `pubsub.viewer` y `pubsub.subscriber` en `ta-dlq-df`. Si ya se aplicó con la versión anterior de `outputs_trends.tf`: 0 recursos y dos outputs a guardar (`dataflow_template_spec`, `dataflow_launcher_image`) |
| C3 | `Apply complete! Resources: 10 added, 0 changed, 0 destroyed.` (o `0 added` si solo guarda outputs) |
| C4 | Outputs `trend_windows_table`, `trend_windows_latest_view`, `dlq_events_table`, `dq_results_table`, `dlq_df_subscription`, `dataflow_template_spec`, `dataflow_launcher_image`. Si faltan solo los dos últimos es AVISO: `dataflow_template.py` los deriva de `lake_bucket` y `artifact_registry_url` |

Manual:

```bash
cd project
bash infra/validate.sh
terraform -chdir=infra/gcp/environments/dev plan \
  -target=module.trends_bq_tables -target=google_pubsub_subscription.trends_dlq_df \
  -target=google_pubsub_subscription_iam_member.trends_dataflow_viewer \
  -target=google_pubsub_subscription_iam_member.trends_dataflow_dlq_subscriber -out=evidencia/tfplan_2_4
terraform -chdir=infra/gcp/environments/dev apply evidencia/tfplan_2_4
```

`evidencia/tfplan_2_4` puede contener valores de `terraform.tfvars`: no lo versione.

Si `validate.sh` dice `Reference to undeclared module`, `trends.tf` apunta a `module.pubsub` o
`module.bigquery_stream` con otro nombre que el de `platform.tf`/`streaming.tf`: cambie el nombre
en los `depends_on` de `trends.tf` (`test_trends_terraform.py` le dice cuál).

## Parte D · Local (emulador + DirectRunner con NLP)

```bash
python project/scripts/validate_2_4.py local          # --down para apagar al final
```

| Paso | Qué hace | Esperado |
|---|---|---|
| D1 | `docker info` | Docker responde |
| D2 | `local_dataflow.sh down`, `local_dev.sh down` y `up` | Emulador vacío (sin *backlog*) y simulador sanos; el cursor se conserva |
| D3 | `local_nlp.sh up` (construye la imagen si falta) | En los logs de `dataflow-local`: `trends_config` con `enabled: true`, `local_root: …/trend_windows`, `late_delay_seconds: 0` |
| D4 | Espera ventanas en `.local-lake/trend_windows/` | ≥ 3 ventanas cerradas en ≤ 15 min (las primeras tardan ~6 min: modelos + primera ventana) |
| D5 | `verify_trends.py report --local --since <D3>` | T2–T7 OK: 3 fuentes, 0 filas con problemas, conciliación ≥ 99 %, 0 alertas en el arranque, p95 < 5 min |
| D6 | `verify_enriched.py report --local --expect-nlp` | OK (regresión 2.2/2.3) |
| D7 | `bash scripts/local_dataflow.sh check` | OK (regresión 2.1 con la rama de tendencias enganchada) |

Manual: `bash scripts/local_dataflow.sh down && bash scripts/local_dev.sh down && bash scripts/local_dev.sh up && bash scripts/local_nlp.sh up`, esperar ~10 min y
`python scripts/verify_trends.py --json-out evidencia/local_trends_report.json report --local --since <hora UTC del up>`.

En local es normal ver varias filas por ventana: el DirectRunner fija la marca de agua en la hora
actual, así que muchos posts llegan "tarde" a su primera ventana y generan un panel tardío.
`versions.keys_with_late_panes` lo cuenta.

## Parte E · Nube (Dataflow)

```bash
python project/scripts/validate_2_4.py cloud          # --minutes 12 por defecto; --shutdown drena al final
```

| Paso | Qué hace | Esperado |
|---|---|---|
| E1 | Proyecto, región y ADC | `project-0568d9ba-848f-413c-88b · us-central1 · job ta-stream-dev` |
| E2 | `verify_trends.py tables` | T1 OK: 3 tablas con las columnas de §5.5 y la vista |
| E3 | `gcloud run services describe ta-stream-simulator` | URL del simulador |
| E4a | `gcloud dataflow jobs drain <id>` del job anterior y espera, mostrando los cambios de estado | Sin job activo (3–10 min); si ya estaba drenando, solo espera |
| E4 | `dataflow_job.sh start` con el código del 2.4 | Job en *Running* |
| E5 | `trends_config` de la salida del lanzamiento | `bq_table = …:ta_stream.trend_windows`, `dlq_table = …:ta_stream.dlq_events`, `late_delay_seconds: 60` |
| E6 | Espera 5 min y `verify_trends.py dlq-check --allow-cloud` | D1–D3 OK: los 3 defectos (`json`, `schema`, `encoding`) en `dlq_events`, `source = reddit`, una copia cada uno |
| E7 | `verify_trends.py report --since <E4a> --wait-caught-up-minutes 15 --watch-minutes 12 --write-dq` | Espera a que el job procese el *backlog* del cambio (la ventana más reciente cerró hace ≤ 150 s) y luego observa 12 min. T1–T9 OK; T5b puede dar AVISO (abajo) |
| E8 | `verify_streaming.py check --allow-cloud` | OK 13/13 (regresión 2.1) |
| E9 | `verify_enriched.py report --expect-nlp --hours 0.5` | OK (regresión 2.2/2.3) |
| E10 | `gcloud logging read` del job | 0 `trend_bq_insert_failed`, `dlq_bq_insert_failed`, `trend_row_unrecognized`; **0 `GETTING_PUBSUB_SUBSCRIPTION_FAILED`** |

Comprobaciones de E7:

| Id | Qué | Esperado |
|---|---|---|
| T1 | Tablas y vista | columnas exactas de §5.5 |
| T2 | Ventanas de cada fuente | 3 fuentes con 3 marcas |
| T3 | Reglas de cada fila | 0 problemas (300 s, alineada al minuto, conteos, `avg_sentiment` en [-1, 1], 7 emociones) |
| T4 | Conciliación con `posts_enriched` | ≥ 99 % de las ventanas cerradas iguales (`n_posts`, `n_negative`, `n_positive`, `avg_sentiment`, `emotion_counts`) |
| T5 | Arranque en frío | 0 alertas en la primera hora del job |
| T5b | Ventanas del cambio de job (`[since − 15 min, since)`) | 0 alertas. **AVISO esperado una vez:** al drenar un job con el código anterior a esta corrección, sus ventanas abiertas salen parciales y alertan por una caída falsa (48 en la primera corrida). El job nuevo ya no lo hace; se comprueba en F (que drena el job de E) |
| T6 | z-scores | calculados desde la tercera ventana completa de cada clave |
| T7 | Latencia `event_time → process_time`, desde que el job se puso al día (o `since` + 10 min) | **p95 < 300 s** (KPI de HU-023) |
| T8 | Detección: fin de ventana → fila en BigQuery, por ventana × marca × fuente | p95 < 300 s; se espera ~50–90 s. Cuentan las ventanas que cerraron ≥ 5 min antes del final de la observación; las que no aparecieron entran con su cota inferior. El detalle por ventana queda en `detection.windows` del JSON |
| T9 | KPI en `dq_results` | 4 filas (`layer = stream`) con el mismo `run_id` |

Manual (desde `project/`):

```bash
gcloud dataflow jobs list --region=us-central1 --status=active          # id del job
gcloud dataflow jobs drain <id> --region=us-central1                     # y esperar a que no quede activo
date -u +%Y-%m-%dT%H:%M:%SZ               # anote: SINCE
bash scripts/dataflow_job.sh start        # buscar la línea trends_config
python scripts/verify_trends.py --json-out evidencia/dlq_check_2_4.json dlq-check --allow-cloud
python scripts/verify_trends.py --json-out evidencia/trends_report.json report --since SINCE --wait-caught-up-minutes 15 --watch-minutes 12 --write-dq
```

Consultas útiles:

```sql
-- versión vigente de las ventanas de la última hora
SELECT * FROM `project-0568d9ba-848f-413c-88b.ta_stream.trend_windows_latest`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY window_start DESC, brand, source;

-- KPI guardados por el verificador
SELECT run_ts, check_name, observed_value, threshold, passed
FROM `project-0568d9ba-848f-413c-88b.ta_stream.dq_results` ORDER BY run_ts DESC LIMIT 10;
```

## Parte F · Plantilla Flex (HU-005)

```bash
python project/scripts/validate_2_4.py template       # --skip-build si ya está publicada
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | `dataflow_template.py params` | Parámetros tomados de `terraform output`: `output_root`, `nlp_container_image`, las tres tablas y `ta-dlq-df` |
| F2 | `dataflow_template.py build` | Imagen `…/ta-images/dataflow-launcher:{commit}` (la prueba de humo de la imagen pasa) y `gs://…-ta-lake/_dataflow/templates/ta-stream.json` (~6–10 min) |
| F3 | Drena el job de E (ya con la corrección del drenado) | Sin job activo. Muestra el estado cada 2 min; espera hasta 30 min (`--cancel-after-minutes N` cancela si pasa de N) |
| F4 | `dataflow_template.py run` | `ta-stream-dev` en *Running* desde la plantilla (el lanzador tarda 5–10 min). Sigue el job por su id: si termina en *Failed* lo dice enseguida, con los errores del lanzador (incluido el traceback de Python, que llega con severidad INFO) |
| F5 | `gcloud dataflow jobs describe` | Menciona `dataflow-nlp`, `trend_windows` y `dlq_events` |
| F6 | `verify_trends.py report` con el nuevo job | T1–T8 OK, **T5b incluido**: es la prueba en GCP de que el drenado del job de E no dejó alertas falsas |
| F7 | `verify_streaming.py check --allow-cloud` | OK |
| F8 | Logs del job | Sin errores del lote |

El job lanzado desde la plantilla reemplaza al de `dataflow_job.sh`: los dos leen las mismas
suscripciones, nunca deben correr a la vez (el script se niega si ya hay uno activo).

## Apagado

```bash
bash project/scripts/dataflow_job.sh drain            # o python project/scripts/off.py
bash project/scripts/local_dataflow.sh down-all       # si corrió la parte D
python project/scripts/status.py                      # confirmar que nada sigue cobrando
```

El simulador se apaga como indica `docs/ACTIVE_SERVICES.md`. Las tablas, la vista, `ta-dlq-df`
y la plantilla no cobran mientras no se usan (BigQuery cobra por almacenamiento, unos KB).

## Cierre

**Resultado de la validación (v2.27): lote cerrado.**

| Parte | Resultado |
|---|---|
| A · Preflight | OK |
| B · Pruebas | OK (190 sin omitidas antes de las correcciones; 224 con ellas) |
| C · Terraform | OK: 10 recursos aplicados; C4 corregido (outputs con `depends_on`) |
| D · Local | No se corrió: abierto, sin bloquear |
| E · Nube | OK salvo T8 (medido durante el atraso del cambio de job; se midió bien en F6): T4 126/126, T7 p95 5,4 s, `dlq-check` 3/3, E10 sin errores |
| F · Plantilla | **F1–F8 OK** (tras descartar el atraso con `seek`): job desde la plantilla en *Running*; 446 filas, conciliación 216/216, T7 p95 5,1 s, T8 p95 61,3 s, T5 y T5b sin alertas |

Abiertos, sin bloquear: prueba del drenado en GCP; parte D; causa del reinicio de los procesos del SDK que trabó el NLP de un job.

Con A–E en OK (F recomendado): marcar el Lote 2.4 en `PLAN.md` §3, HU-005 y HU-023 en `HU.md`,
y anotar en `CHANGELOG.md` (v2.27) y `ARCHITECTURE.md` §3.16 las cifras reales: B1–B4, filas y
conciliación de D5/E7, p95 de T7 y T8, y la etiqueta de la imagen del lanzador. Con eso se cierra
la **Fase 2**.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| A4 `falta 'attach_trends(enriched_rows)'` | Ver "Antes de empezar", punto 2. |
| D3 sin `trends_config` | `bash project/scripts/local_dataflow.sh logs`: un error de importación en `pipeline.py` o el enganche no está. |
| D4 sin ventanas | ¿El simulador local está emitiendo (`local_dev.sh status`)? La primera ventana cierra ~5 min después de la primera fila con NLP. |
| E4 `trends_config` con `enabled: false` | ¿`TA_TRENDS_ENABLED=false` en el entorno? |
| E6 FALLA sin filas | ¿Se aplicó C (`ta-dlq-df`)? ¿Salió `dlq_bq_attached` al lanzar? ¿Se pasó `--dlq_table` del 2.1 (apaga la rama)? |
| E7 T4 con `faltan` | Ventanas sin fila: revise `trend_row_unrecognized` en E10 y que `attach_trends` reciba las filas con NLP. |
| La parte E no muestra nada al empezar | Está leyendo `terraform output` para el proyecto y la región, que en `/mnt/c` puede tardar. Para saltarlo: `export GCP_PROJECT=project-0568d9ba-848f-413c-88b GCP_REGION=us-central1`. |
| E4a `tiempo agotado` (versión anterior del validador) | Usaba `dataflow_job.sh drain`, que se quedaba esperando. Ahora se drena con `gcloud`. Si el job quedó a medio drenar, repita: el validador espera a un job que ya está en *Draining*. |
| E4–E7 lentos tras días con el simulador encendido y sin job | El job procesa primero el *backlog* de `ta-raw-*-df`. En dev se puede saltar (descarta esos mensajes): `for s in reddit facebook instagram; do gcloud pubsub subscriptions seek ta-raw-$s-df --time=$(date -u +%Y-%m-%dT%H:%M:%SZ); done` |
| E4a/F3 se queda en *Draining* | El validador sigue esperando (avisa cada 2 min, hasta 30). En otra terminal: `gcloud dataflow jobs describe <id> --region=us-central1 --format="value(currentState,currentStateTime)"`; en la consola, el paso con mayor *Data freshness* es el que retiene el drenado. Si pasa de 30 min: `gcloud dataflow jobs cancel <id> --region=us-central1` y repita con `--skip-build` (F) — o use `--cancel-after-minutes 20`. Al cancelar no se emiten las ventanas abiertas, así que esa corrida no prueba el drenado (F6 T5b). |
| E7 T5b AVISO | Alertas del drenado del job anterior (ver T5b). Si el job anterior ya tenía esta corrección, revise `trend_windows` en esas ventanas: `n_posts` bajo y `zscore_volume` ≤ −3. |
| E7 T8 FALLA con `no se puso al día` | El *backlog* no se terminó en 15 min (`--catch-up-minutes`). Revise la latencia del NLP (`verify_enriched.py`) o salte el *backlog* con `seek` (fila anterior). |
| Repetir solo la medición sin relanzar el job | `validate_2_4.py cloud --no-restart`: usa el job activo si sus opciones incluyen las del 2.4 (`trend_windows_table`, `dlq_df_subscription`). |
| E7 T7 > 300 s | Repita con `--since` más tardío: el *backlog* del cambio de job infla la latencia. Si persiste, revise el lote de NLP (`TA_NLP_BATCH_SECONDS`). |
| E10 `GETTING_PUBSUB_SUBSCRIPTION_FAILED` | El permiso de C no está aplicado, o el aviso nombra un topic: añada `roles/pubsub.viewer` en el topic. |
| F2 falla en la prueba de humo | La imagen no importa `streaming.pipeline`: falta una dependencia en `src/streaming/requirements.txt`. |
| F2 no encuentra la imagen base | Cambie `LAUNCHER_BASE` en `src/streaming/flex_template/Dockerfile` por la imagen de lanzador de Python 3.11 vigente en la documentación de Dataflow (no se pudo comprobar la etiqueta al preparar el lote). |
| F4 termina en *Failed* | El detalle trae los errores del lanzador. Si dice `AttributeError: 'NoneType' object has no attribute 'id'` o no encuentra `contracts`, la imagen es anterior a la corrección de v2.26: reconstrúyala (sin `--skip-build`). Logs completos: `gcloud logging read 'resource.type="dataflow_step" AND resource.labels.job_id="<id>" AND severity>=ERROR' --limit=30`; busque también `flex_template_launch` (argumentos y carpeta de trabajo). |
