# TrendAnalyzer · CHANGELOG

> Historial de versiones del proyecto: una entrada por versión, la más reciente primero. Cada entrada indica el lote que motiva los cambios.
> Fuente de verdad: [`ARCHITECTURE.md`](ARCHITECTURE.md) (versión vigente **2.24**). Las referencias `§N` de este archivo apuntan a ese documento, salvo las marcadas `PLAN.md §N`.
> Plan, trazabilidad HU y estado por lote: [`PLAN.md`](PLAN.md).

---

## v2.24 · desde v2.23

**Lote que lo motiva:** Lote 2.2 (HU-020, HU-021) · validación y cierre

| Área | Cambio |
|---|---|
| Verificación | Pruebas: 67 del lote sin omitidas, 158 en `streaming/`, 18 de `contracts/` y 116 de `common/` + simulador. **Local:** `verify_enriched.py` OK (cobertura 100 %, hash 1 a 1, 0 copias) y `check` OK. **GCP:** `verify_enriched.py` OK con BigQuery (cobertura 100 %), consultas de E11 correctas (`hash_malo = 0`, `shares` solo en Facebook, 0 identidad en columnas y `payload`) y `check --allow-cloud` OK. Detalle en §3.14. |
| Corrección durante la validación | `build_from_messages()` crea `unique` dentro de la función, así que el enganche va ahí. Para no romper las pruebas del 2.1 con `TestStream` sin `--output_root`, la rama se apaga sola fuera de Dataflow si no tiene salida (`enriched_disabled`); en Dataflow sigue fallando. Una prueba nueva (67 en total). |
| Documentación | `validation-2-2.md`: el enganche, con el código real de `build_from_messages()`; E8 aclara que `enriched_config` sale en la terminal del launcher. Nuevo `docs/ACTIVE_SERVICES.md`: qué dejar encendido, cómo encender y apagar en local y en GCP, y una sola sesión de nube para varios lotes. |
| Repositorio | El equipo cambió `local/dataflow.Dockerfile` (Lote 2.1) para instalar con `uv` y caché de BuildKit (`COPY --from=ghcr.io/astral-sh/uv:latest` y `RUN --mount=type=cache,target=/root/.cache/uv uv pip install --system …`). Recomendado: fijar la versión de `uv` en vez de `latest`. |
| Estado | Lote 2.2 ✅; HU-020 y HU-021 ✅. HU-005 sigue en 🟨 (faltan `trend_windows`, `dlq_events`, `dq_results` y la plantilla, Lote 2.4). |
| Contratos | **Sin cambios.** `schema_version` sigue en `"1.0"`. |
| Pendiente (no bloquea) | `roles/pubsub.viewer` para `sa-dataflow` (aviso `GETTING_PUBSUB_SUBSCRIPTION_FAILED`; Lote 2.4). `test_routes_and_three_workers` se cuelga en `ta-streaming` y pasa en `ta`. Validar el Lote 2.3. Siguen abiertos los de v2.21 y v2.17. |

## v2.23 · desde v2.22

**Lote que lo motiva:** Lote 2.3 (HU-022) · entrega

| Área | Cambio |
|---|---|
| Lote 2.3 | Entregado: `src/streaming/nlp.py`, `nlp_testing.py`, imagen `src/streaming/nlp_image/` (Dockerfile, `fetch_models.py`, `image_smoke.py`, `requirements-nlp.txt`, `cloudbuild.yaml`), `scripts/build_nlp_image.sh`, `scripts/local_nlp.sh`, `docker-compose.nlp.yml`, `infra/gcp/environments/dev/nlp.tf` y 22 pruebas (`tests/test_nlp*.py`). Detalle en **§3.15**; validación en `docs/validation-2-3.md`. |
| NLP (HU-022) | Sentimiento y emoción con RoBERTuito de pysentimiento mediante `RunInference` (dos `ModelHandler` propios, modelo compartido por VM), con `preprocess_tweet(lang="es")`, `common.label_map` y `nlp_model_version` con el commit de cada modelo. Un lote que falla sigue sin NLP (`nlp_failed_rows`). |
| Sin tocar el Lote 2.1 | La etapa se arma desde `attach_enriched()` (opciones `--nlp_*`, `TA_NLP_IMAGE`, `TA_NLP_ENABLED`) y en Dataflow fija `sdk_container_image` y `sdk_location=container` en las opciones del pipeline: `pipeline.py` y `dataflow_job.sh` no cambian. En local, un tercer Compose sobreescribe la imagen de `dataflow-local`. |
| Imagen | `dataflow-nlp` sobre `apache/beam_python3.11_sdk:2.77.0` con torch 2.5.1 CPU, transformers 4.46.3, pysentimiento 0.7.3 (solo el preprocesado, `--no-deps`) y los dos modelos con su commit en `models.json`. La prueba de humo verifica etiquetas y dos frases de control; si falla, no se publica. |
| Corrección del Lote 2.2 | `WriteEnrichedParquet` re-marca siempre con la hora actual. Tras `RunInference` las filas llegan con el fin de su ventana, y `max(ahora, marca)` habría desplazado los archivos de `enriched/`. Lo descubrió la prueba de la rama con NLP. |
| Verificador | `verify_enriched.py --expect-nlp`: NLP presente (≥ 99 %), etiquetas, scores, formato de versión y versión única; en BigQuery, filas sin NLP y etiquetas inválidas; como dato, distribución y acuerdo con `label_sentiment`. |
| Terraform | `artifactregistry.reader` en `ta-images` para `sa-dataflow` (1 recurso nuevo). |
| Verificación previa | 87 passed y 1 omitida (lotes 2.2 + 2.3), con dobles de `common.label_map`/`envelope`/`hashing`, una aproximación de `contracts/` y el preprocesado real de pysentimiento 0.7.3. **No** se descargaron los modelos (Hugging Face bloqueado desde el entorno de preparación) ni se construyó la imagen; **no** se corrió en local ni en GCP. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. §5.1 suma `TA_NLP_IMAGE`, `TA_NLP_ENABLED` y `TA_NLP_MODELS_DIR`; §5.8, el permiso de Artifact Registry; §5.12, el parámetro NLP. Nueva **ADR-19**. |
| Documento | Nueva §3.15; ampliadas §0, §3.14, §5.1, §5.4, §5.8, §5.11, §5.12, §6, §9 (ADR-19), PLAN.md §3, HU.md (HU-022) y `src/streaming/ENRICHED.md`. |
| Pendiente | Validar el Lote 2.2 primero; luego, los pasos de `docs/validation-2-3.md`. Si se copia el 2.3 antes de cerrar el 2.2 en GCP, lanzar con `TA_NLP_ENABLED=false` hasta publicar la imagen. |

## v2.22 · desde v2.21

**Lote que lo motiva:** Lote 2.2 (HU-020, HU-021) · entrega

| Área | Cambio |
|---|---|
| Lote 2.2 | Entregado: `src/common/{enriched_row,parquet_rows,hmac_key}.py`, `src/streaming/{enriched,enriched_check,enriched_cli,enriched_testing}.py`, `src/streaming/ENRICHED.md`, `scripts/verify_enriched.py`, `scripts/hmac_key.sh`, Terraform (`modules/bigquery_stream`, `modules/hmac_secret` y `streaming.tf`, `variables_streaming.tf`, `outputs_streaming.tf` en `environments/dev`) y 67 pruebas en `src/streaming/tests/test_enriched_*.py`. Detalle en **§3.14**. |
| Anonimización (HU-020) | `author` → `{user_hash, followers}` con HMAC-SHA256 del `user_id` (vía `common.hashing`); `user_id` y `handle` desaparecen y se rechaza cualquier salida que los contenga. La llave se lee en el worker desde `ta-hmac-key` por REST; en local, llave pública de desarrollo. |
| Normalización y carga (HU-021) | `likes`, `shares`, `comments` con `common.envelope.normalize_engagement`; `brand_primary`, `label_sentiment` y `payload` JSON. Parquet en `enriched/` (misma ventana y nombre determinista que `raw/`) y *streaming inserts* en `ta_stream.posts_enriched`. NLP en nulo hasta el Lote 2.3. |
| Integración | Dos líneas en `pipeline.py` del Lote 2.1 (import y `attach_enriched(unique)`); `test_enriched_hook.py` lo exige. `dataflow_job.sh` sin cambios: las opciones nuevas tienen valor por defecto. |
| Terraform | Dataset `ta_stream`, tabla `posts_enriched` (partición por `event_time`, 90 días, clustering `source, brand_primary`), secreto `ta-hmac-key` con `prevent_destroy`, `bigquery.dataEditor` en el dataset y `secretAccessor` en el secreto para `sa-dataflow`. Se adelantan del Lote 2.4 (HU-005), que conserva `trend_windows`, `dlq_events`, `dq_results` y la plantilla de Dataflow. `plan` esperado: 5 a crear, 0 a cambiar, 0 a destruir. |
| Verificador | `verify_enriched.py report`: columnas, ausencia de identidad, reglas por fila, copias, cobertura `raw → enriched`, `user_hash` 1 a 1 con `user_id` (sin conocer la llave) y, con el lake en `gs://`, BigQuery (columnas, ids, reglas, cobertura). Evidencia en JSON. |
| Verificación previa | 65 passed y 1 omitida (el enganche, sin `pipeline.py`) en un entorno aislado con Python 3.11 y Beam 2.77.0, con dobles de `common.envelope`/`common.hashing` y una aproximación de `contracts/`. Terraform revisado con un analizador de HCL y la alineación de `fmt`; **no** se pudo correr `terraform validate`. **No** se corrió con el pipeline real, en local ni en GCP. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. Se fijan: formato de `enriched/` (§5.4), modos y método de escritura de `posts_enriched` (§5.5), permisos ya aplicados de `sa-dataflow` (§5.8), outputs `bq_dataset`, `posts_enriched_table` y `hmac_secret_id` (§5.9) y tres parámetros de §5.12. Nueva **ADR-18**. |
| Documento | Nueva §3.14; ampliadas §0, §5.4, §5.5, §5.8, §5.9, §5.11, §5.12, §5.14, §5.15, §6, §9 (ADR-18), PLAN.md §3 y HU.md (HU-005, HU-020, HU-021). |
| Pendiente | Validar con los pasos de `src/streaming/ENRICHED.md` (enganche, pruebas con `contracts/` reales, local, `terraform apply`, `hmac_key.sh init`, job de Dataflow). Siguen abiertos los de v2.21 (`sa-exporter` a `enriched/`, *soft delete* de `_dataflow/temp`, `.gitignore`) y v2.17 (permiso de Cloud Build en Terraform, `GET /health`). |

## v2.21 · desde v2.20

**Lote que lo motiva:** Lote 2.1 (HU-018, HU-019; criterio de `DirectRunner` de HU-047) · validación y cierre

| Área | Cambio |
|---|---|
| Verificación | En WSL con Python 3.11.17 (`uv`, entorno `~/.venvs/ta-streaming`): `pytest src/streaming` **91 passed** sin omitidas con `contracts/` real; `contracts/tests` 18 passed. **Local:** `check` **OK** (13/13) y `report` con el simulador (487 / 478 / 475 líneas, 0 repetidas). **GCP:** job `ta-stream-dev` en *Running* a los ~2 min; `check --allow-cloud` **OK** a los 298 s; *backlog* de 220 / 215 / 222 mensajes en `raw/` sin repetidos. Evidencia en `evidencia/local_streaming_check.json`, `local_streaming_report.json` y `streaming_check.json`. Detalle en §3.13. |
| Corrección: deduplicación | La versión de v2.20 usaba un timer de tiempo de proceso (TTL de 15 min). En Dataflow ese timer retiene la marca de agua, así que `raw/` cerraba ~15 min tarde y el primer `check` en GCP agotó su tiempo con la DLQ ya correcta. Ahora: estado en ventanas fijas de 1 h, sin timers (`--dedup_window_seconds`, antes `--dedup_ttl_seconds`). Tres pruebas nuevas (una falla si vuelve a haber timers). |
| Corrección: empaquetado | Los entornos de `uv` no traen `setuptools` y `--setup_file` falló antes de crear el job (no se cobró nada). `requirements.txt` fija `setuptools==80.9.0` y `dataflow_job.sh preflight` lo comprueba. |
| Entorno | Ubuntu en WSL solo trae Python 3.14; el 3.11 del proyecto se instala con `uv` (`uv python install 3.11`) en un entorno aparte del de `ta`. Anotado en `src/streaming/README.md`. |
| HU.md | HU-018, HU-019 y HU-047 en ✅. HU-019: la restricción de `raw/` frente a `sa-exporter` la cumple el Lote 3.2 (criterio de HU-025). HU-047: se agrega el enlace a `LOCAL_DEV.md` desde el README en este cierre. |
| Estado | Lote 2.1 ✅ y **Fase 1 ✅** en PLAN.md §3 (su último criterio era el de `DirectRunner`). Fase 2 en 🟨; el siguiente es el **2.2**. |
| Contratos | **Sin cambios** en `contracts/` ni en §5.2–§5.8; `schema_version` sigue en `"1.0"`. §5.12 y ADR-17 describen la deduplicación por ventanas. |
| Pendiente (no bloquea) | Limitar `sa-exporter` a `enriched/` (Lote 3.2). *Soft delete* del bucket del lake frente a `_dataflow/temp`. `*.egg-info/` y `.local-lake/` en `.gitignore`. Siguen abiertos los de v2.17 (permiso de Cloud Build en Terraform y `GET /health`). El conteo de `src/ingest_check` dio 62 en esta sesión frente a 65 en v2.19: revisar que estén `stats.py`, `cli.py` y `test_ingest_cli.py` del cierre del Lote 1.5. |

## v2.20 · desde v2.19

**Lote que lo motiva:** Lote 2.1 (HU-018, HU-019; criterio de `DirectRunner` de HU-047) · entrega

| Área | Cambio |
|---|---|
| Lote 2.1 | Entregado: pipeline en `src/streaming/` (`pipeline.py`, `config.py`, `schemas.py`, `validate.py`, `transforms.py`, `records.py`), verificador (`inject.py`, `lake_report.py`, `cli.py`, `scripts/verify_streaming.py`), `src/setup.py`, `scripts/dataflow_job.sh`, `docker-compose.dataflow.yml`, `local/dataflow.Dockerfile`, `scripts/local_dataflow.sh`, `src/streaming/README.md` y 87 pruebas. Detalle en **§3.13**. |
| Pipeline | Lee las tres `ta-raw-{source}-df`; valida en orden `encoding` → `json` → `schema` → `attributes` → `routing`; deduplica por `event_id` con `id_label` (Dataflow) y con estado por ventanas fijas de 1 h, sin timers (también en DirectRunner); escribe `raw/` y `dlq/` en ventanas de tiempo de proceso (300 s; 60 s en local) con nombres deterministas; publica los rechazados en `ta-dlq`. `dlq_events` queda opcional (`--dlq_table`) hasta el Lote 2.4. |
| Local | `dataflow-local` (DirectRunner, implementación `BundleBasedDirectRunner`) y `streaming-check` se suman al Compose del Lote 1.5 con un segundo archivo, sin modificarlo. Lake en `.local-lake/`. Cierra, al validarse, el criterio pendiente de HU-047. |
| GCP | `dataflow_job.sh`: `preflight` de solo lectura, `start` (job `ta-stream-{ENV}`, 1 × e2-standard-2, Streaming Engine, `sa-dataflow`), `status`, `drain` y `cancel`. Costo del orden de 0,25 USD/h mientras está activo. |
| Verificador | `check` publica un centinela dos veces y un defecto por tipo de error, y comprueba `raw/` (centinela una sola vez), `dlq/` y `ta-dlq`; `report` resume el lake. Evidencia en JSON. Con la nube exige `--allow-cloud`. |
| Verificación previa | 87 pruebas en verde (Python 3.11.17, Beam 2.77.0) con schemas de prueba con la estructura de §5.2, porque `contracts/` no estaba disponible al prepararlo. El pipeline real corrió con el DirectRunner contra un servidor de Pub/Sub de prueba: `check` OK y 367 eventos con 40 reenvíos dieron 367 líneas únicas. *Dry run* de `DataflowRunner` correcto (job armado y `setup.py` empaquetado). **No** se construyó la imagen local ni se lanzó el job en GCP. |
| Corrección al validar en GCP | La primera versión deduplicaba con un timer de tiempo de proceso (TTL 15 min). En Dataflow ese timer retiene la marca de agua en la hora del elemento, así que `raw/` se escribía con ~15 min de atraso y el `check` agotó su tiempo (la DLQ, que no pasa por ese paso, sí llegó). Ahora el estado vive en ventanas fijas de 1 h y no hay timers; una prueba fija que el DoFn no declare timers. Opción renombrada a `--dedup_window_seconds`. También se suma `setuptools` a `requirements.txt` (los entornos de `uv` no lo traen) y al preflight. |
| Corrección durante el lote | El primer injector cortaba el JSON por bytes y podía partir una tilde: el mensaje caía como `encoding` y no como `json`. Ahora se corta por caracteres; una prueba fija que un corte por bytes es `encoding` (también puede pasar con el caos `malformed`). |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. Se fijan formatos que §5 no definía: mensajes de `ta-dlq` (cuerpo original + atributos `dlq_*`, §5.3), formato y nombre de archivos de `raw/` y `dlq/` y prefijo `_dataflow/` (§5.4), valores de `dlq_events.error_type` (§5.5), job `ta-stream-{ENV}` y parámetros de §5.12. Nueva **ADR-17**. |
| Documento | Nueva §3.13; ampliadas §0, §3.12, §5.3, §5.4, §5.5, §5.11, §5.12, §6, §9 (ADR-17), PLAN.md §3 y HU.md (HU-018, HU-019, HU-047). |
| Pendiente | Validar en WSL (local) y en GCP con los pasos de `src/streaming/README.md`. Posible `roles/pubsub.viewer` para `sa-dataflow` si Dataflow lo exige. Restringir `sa-exporter` a `enriched/` (Lote 3.2) para que `raw/` quede solo para `sa-dataflow`. Agregar `*.egg-info/` y `.local-lake/` al `.gitignore`. Siguen abiertos los de v2.17 y v2.19. |

## v2.19 · desde v2.18

**Lote que lo motiva:** Lote 1.5 (HU-047) · entrega y cierre

| Área | Cambio |
|---|---|
| Lote 1.5 (HU-047) | Entregado: `docker-compose.yml` con `pubsub-emulator`, `pubsub-init`, `simulator` y `validator` (perfil `tools`); `LOCAL_DEV.md`; `local/validator.Dockerfile` (imagen `ta-local-tools`); `scripts/local_dev.sh`, `scripts/local_pubsub_init.py` y `src/ingest_check/emulator.py`. Detalle en **§3.12**. |
| Entorno local | Sin credenciales de GCP ni de AWS. El simulador usa la misma imagen de Cloud Run (*target* `runtime`) contra el emulador, con cursor en el volumen `sim-state`. El validador es el verificador del Lote 1.4 con validación contra `envelope.v1.schema.json`. `pubsub-init` crea los nombres de §5.3 y se niega a correr fuera del emulador. |
| `DirectRunner` | El criterio de HU-047 que lo pide se comprueba con el Lote 2.1, que crea el pipeline y sumará `dataflow-local` al Compose (opción acordada). Las suscripciones `-df` ya existen en el emulador. Anotado en HU.md y en PLAN.md §1. |
| Preflight | Nuevo `scripts/preflight.sh` (estado real de la nube, solo lectura) y regla en §8: cada lote con validación trae secciones de *preflight* y de apagado, y no asume recursos encendidos. Motivo: en el Lote 1.4 la validación empezó con el simulador apagado. |
| Verificación | En WSL con Docker Desktop (Docker 29.7.2, Compose v5.5.1): `preflight` OK; `up` en ~1,5 min; `verify` **OK** (0,483 / 0,483 / 0,533 msg/s, 100 % de ids únicos, schema válido, cursor con `seed = 7`); `restart-check` **OK** con `docker compose restart simulator` (reanuda en el `seq` siguiente, 0 repetidos, cursor 100 → 150). Evidencia en `evidencia/local_measure.json` y `local_restart.json`. Sin solapamiento de instancias, a diferencia de Cloud Run (v2.18). |
| Corrección | La primera corrida dio 62 pruebas en vez de 64: el repo no tenía el diagnóstico de la v2.18 (`stats.py`, `cli.py`, `test_ingest_cli.py`), porque del cierre del Lote 1.4 solo se copiaron los documentos. Se entregan de nuevo. Además, con el emulador las instrucciones de `restart-check` ahora indican `docker compose restart simulator` y no Terraform ni gcloud (una prueba nueva: 65 en total). `LOCAL_DEV.md` aclara que el *target* `runtime` no corre las pruebas de la imagen. |
| Estado | Lote 1.5 ✅ en PLAN.md §3. HU-047 sigue en 🟨 solo por el criterio de `DirectRunner` (Lote 2.1), así que la Fase 1 se cierra con ese lote. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. §5.1 suma los valores locales; §5.12 (desarrollo local) aclara que `DirectRunner` llega con el Lote 2.1. |
| Documento | Nueva §3.12; ampliadas §0, §5.1, §5.11, §5.12, §6, §8, PLAN.md §1 y §3 y HU.md (HU-047). |
| Pendiente | Enlazar `LOCAL_DEV.md` desde el README (criterio de HU-047). Siguen abiertos los de v2.17 (permiso de Cloud Build en Terraform y `GET /health`). |

## v2.18 · desde v2.17

**Lote que lo motiva:** Lote 1.4 (HU-017) · entrega, aplicado y cierre

| Área | Cambio |
|---|---|
| Lote 1.4 (HU-017) | Entregado y cerrado: verificador de ingesta en `scripts/verify_ingest.py` y `src/ingest_check/` (`stats.py`, `cursor.py`, `schema.py`, `pubsub_io.py`, `cli.py`, `testing.py`, `requirements.txt`) con 54 pruebas sin nube. Detalle en **§3.11**. |
| Verificador | `measure` reporta por fuente la tasa, los `event_id` únicos, el `seq` mínimo y máximo, el cursor de `sim-state/` y la integridad del mensaje (JSON, atributos y, si hay `contracts/`, el schema). `restart-check` comprueba que el `seq` continúa tras un reinicio (`--restart manual`, `wait` o `gcloud`). Salida en texto y, con `--json-out`, en JSON como evidencia. |
| Suscripciones | Crea suscripciones temporales `ta-verify-{source}-{run}` (expiran solas a 1 día) y las borra al terminar. Nunca consume `ta-raw-{source}-df`, que son de Dataflow. Cada mensaje se asigna a la ventana por su `publish_time` de Pub/Sub. |
| Diagnóstico | Tras la primera corrida, que dio 0 mensajes con el servicio apagado, se añade: mensajes recibidos en total, desfase del reloj local frente a Pub/Sub (si supera 2 s, típico de WSL, la ventana se corrige) y una pista para comprobar si el simulador está encendido. |
| Verificación | `python -m pytest src/ingest_check -q`: **54 passed** con `contracts/`. Contra GCP, con el caos apagado: `measure --duration 120` **OK** en las tres fuentes (0,483 / 0,517 / 0,492 msg/s; 100 % de ids únicos; `seq` sin huecos; schema válido; cursor con `seed = 7`). `restart-check --restart gcloud` **OK**: antes del reinicio el `seq` llegó a 522, 524 y 505; después reanudó en 500 en las tres fuentes y siguió avanzando; repetidos 23, 25 y 6 con los mismos ids; cursor 450 → 550. Evidencia en `project/evidencia/ingesta_measure.json` e `ingesta_restart.json`. |
| Observación | Al cambiar de revisión, Cloud Run arranca la instancia nueva antes de apagar la vieja: hay dos emisores a la vez durante unos segundos y la nueva reanuda desde el último *checkpoint* (múltiplo de 50), no desde el `seq` del `SIGTERM`. Los repetidos tienen los mismos `event_id` (al menos una vez, §3.6) y los deduplica Dataflow (HU-018). Apagar y encender con `simulator_running` no tiene solapamiento. |
| Operación | El servicio estaba apagado (`simulator_running = false`) al empezar la validación: hubo que encenderlo. Con el servicio borrado, `terraform output simulator_url` responde *not found* porque Terraform no guarda outputs nulos (aclarado en §5.9). `--restart gcloud` deja `TA_RESTART_NONCE` fuera de Terraform; el siguiente `apply` la quita. |
| HU.md | HU-017 en ✅. |
| Lote 1.4 | Pasa a ✅ cerrado en PLAN.md §3. El siguiente es el **1.5**. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. §5.3 suma una nota sobre las suscripciones temporales `ta-verify-*` (nadie depende de ellas) y §5.9 la aclaración sobre `simulator_url` nulo. |
| Documento | Nueva §3.11; ampliadas §0, §5.3, §5.9, §5.11, §6 y PLAN.md §3. Archivos que §6 no preveía: `src/ingest_check/` completo. |
| Pendiente (no bloquea) | Siguen abiertos los de v2.17 (permiso de Cloud Build en Terraform y `GET /health`). Para el Lote 1.5: el criterio de HU-047 que pide Dataflow con `DirectRunner` depende del Lote 2.1. Recomendado: que cada lote con validación en la nube traiga una sección de *preflight* y de apagado, y un `scripts/preflight.sh` de solo lectura. Mantener el simulador apagado fuera de las demos. |

## v2.17 · desde v2.16

**Lote que lo motiva:** Lote 1.3 (HU-004, HU-016) · aplicado y cierre

| Área | Cambio |
|---|---|
| Verificación | Los seis pasos de `infra/DEPLOY_SIMULATOR.md` pasaron en WSL: `validate.sh` en verde; `apply` de la base sin cambios ni destrucciones sobre el Lote 1.0; imagen construida en Cloud Build con la etapa de pruebas en Python 3.11 sin omitidas (resuelve el pendiente de v2.14); servicio *Ready* con `min = max = 1` y CPU siempre asignada; mensajes en las tres suscripciones; cursor en `sim-state/`; tras apagar y encender, `worker_start` continúa desde el cursor. |
| Cloud Build | La primera compilación falló: la cuenta de servicio por defecto de Compute Engine, que Cloud Build usa en este proyecto, no podía leer el código subido (`storage.objects.get`). Se le concedió `roles/cloudbuild.builds.builder` con `gcloud`, **fuera de Terraform** (pendiente de declararlo). |
| `/healthz` | Desde fuera responde 404: Cloud Run reserva rutas terminadas en `z` y su *frontend* las intercepta. Las sondas sí llegan, así que el criterio de HU-016 se comprobó con el servicio *Ready*. Se documenta en §3.1; pendiente: `GET /health` para consultas externas. |
| HU.md | HU-004 y HU-016 en ✅. |
| Lote 1.3 | Pasa a ✅ cerrado en PLAN.md §3. El siguiente es el **1.4**. |
| Contratos | **Sin cambios** respecto a v2.16. §5.15 suma la verificación, las observaciones y los valores reales de los outputs. `schema_version` sigue en `"1.0"`. |
| Pendiente (no bloquea) | Declarar en Terraform el permiso de Cloud Build; agregar `GET /health` a `app.py` y usarlo en el paso 5 de `infra/DEPLOY_SIMULATOR.md`; mantener el simulador apagado (`simulator_running = false`) fuera de las demos. |

## v2.16 · desde v2.15

**Lote que lo motiva:** Lote 1.3 (HU-004, HU-016) · entrega

| Área | Cambio |
|---|---|
| Lote 1.3 (HU-004, HU-016) | Entregado: imagen del simulador (`src/simulator/Dockerfile`, `constraints.txt`, `cloudbuild.yaml`, `project/.dockerignore`, `project/.gcloudignore`, `scripts/build_simulator_image.sh`) y Terraform de GCP (5 módulos nuevos y `platform.tf`, `variables_platform.tf` y `outputs_platform.tf` en `environments/dev`). Guía en `infra/DEPLOY_SIMULATOR.md`; contrato en **§5.15**. |
| GCP | Topics `ta-raw-{reddit,facebook,instagram}`, `ta-dlq` y `ta-alerts`; suscripciones `ta-raw-{source}-df`; buckets `…-ta-datasets` y `…-ta-lake` (con `raw/` y `dlq/` a 30 días); Artifact Registry `ta-images`; `sa-simulator`, `sa-dataflow` y `sa-exporter`; Cloud Run `ta-stream-simulator` con `min = max = 1` y CPU siempre asignada. |
| Imagen | Python 3.11 slim, usuario sin privilegios, versiones exactas (Faker 40.43.0). La etapa `test` corre las pruebas de `common/` y `simulator/` contra `contracts/` y falla si alguna se omite: resuelve el pendiente de probar en Python 3.11. |
| Apagado | `simulator_running = false` borra el servicio sin tocar el cursor, para no pagar la instancia siempre encendida fuera de las demos. |
| Contratos | §5.8: `sa-dataflow` recibe `pubsub.publisher` en `ta-dlq` (los inválidos se publican ahí, HU-018; §5.8 no lo listaba). Nombres nuevos: repositorio `ta-images` e imagen `stream-simulator`. §5.9 con los outputs del entorno `dev`. Sin cambios en §5.2–§5.7 ni en `contracts/`; `schema_version` sigue en `"1.0"`. |
| Documento | Nueva §5.15; ampliadas §0, §5.8, §5.9, §5.11, §5.14 (qué no existe todavía), §6 y PLAN.md §3. |
| Verificación previa | Sintaxis HCL comprobada con un analizador y alineación al estilo de `terraform fmt`; `.gcloudignore` comprobado con `gcloud meta list-files-for-upload`; pruebas del Lote 1.2 en Python 3.11.17 en verde. **No** se pudo correr `terraform validate` ni construir la imagen al prepararlo: lo confirman `validate.sh` y el paso 3 de la guía. |
| Pendiente | Aplicar y validar (PLAN.md §3). BigQuery y el secreto HMAC para `sa-dataflow`: Lote 2.4. Token OIDC de `sa-exporter`: Lote 3.2. |

## v2.15 · desde v2.14

**Lote que lo motiva:** Lote 1.2 (HU-010 a HU-015) · entrega y cierre

| Área | Cambio |
|---|---|
| Lote 1.2 (HU-010 a HU-015) | Entregado y cerrado: núcleo del simulador en `src/simulator/` (`app.py`, `publisher.py`, `state.py`, `factory.py`, `sim_config.py`, `config/sim.yaml`, `enrich/{users,engagement,brands,chaos}.py`) y 9 archivos de prueba. Detalle en **§3.10**. |
| Verificación | Desde la raíz: `python -m pytest -c project/pytest.ini -rs`: **134 passed, 0 omitidas** (incluye `test_simulator_contract.py`: 1.000 eventos por fuente contra los `contracts/` reales, caos apagado). `python -m pytest project/contracts/tests -q`: **18 passed**, sin cambios. Entorno: WSL (Ubuntu) con entorno virtual; unos 3 min con el repo en `/mnt/c`. |
| Simulador | Tres workers asyncio (uno por fuente), cadencia de 1–3 s, cursor en `sim-state/{source}.json` cada 50 mensajes y al parar, reanudación con los mismos `event_id`, semilla distinta ⇒ `seq = 0`, y reintento del mismo `seq` si falla la publicación. `GET /healthz`; `POST /control/scenario` responde 501 hasta el Lote 6.3. |
| Determinismo | Misma semilla + fuente + `seq` ⇒ mismo evento salvo `event_time` e `ingest_time`; independiente de `PYTHONHASHSEED`. Los `comment` enlazan con un post real reciente de la misma fuente (en Facebook, de la misma marca), sin guardar estado. |
| Usuarios | Pool de 2.000 por fuente con `Faker('es_CO')` y semilla fija `users.pool_seed = 1581`, independiente de `SIM_SEED`, para que el histórico (Lote 3.1) reutilice los mismos usuarios. `user_id` = `u_{n:06d}` con un rango por fuente. |
| Caos | Usa un `Random` propio por evento: encenderlo no cambia texto, marca ni ids. `malformed` excluye los demás defectos y conserva los atributos Pub/Sub para que Dataflow lo envíe a la DLQ. |
| Dependencias | `requirements.txt` del simulador agrega `Faker` y los clientes de Pub/Sub y GCS. Recomendado fijar la versión exacta de Faker en la imagen (Lote 1.3): otra versión cambia los nombres del pool. |
| HU.md | HU-010 a HU-015 en ✅. Se pone al día la tabla resumen con HU-008, HU-009 (Lote 1.1) y HU-046 (Lote 1.0), que ya estaban cerradas en PLAN.md §3. El criterio del CSV de HU-008 se alinea con la decisión de §3.2.1 #7 (`marca,comentario,sentimiento[,categoria]`). |
| Decisiones | §9 #11 (criterios de HU-008 y HU-009) **cerrado** tras revisarlos contra `HU.md`. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. §5.1 se amplía con variables propias del simulador (`PORT`, `SIM_PUBLISHER`, `SIM_STATE_*`, `SIM_SOURCES`, `SIM_TOPIC_TEMPLATE`, `SIM_CONFIG`, `SIM_MAX_EVENTS`). |
| Documento | Nueva §3.10; ampliadas §5.1, §5.11, §6, §9 y PLAN.md §3. Archivos que §6 no preveía: `factory.py`, `sim_config.py` y `tests/_sim_helpers.py`. |
| Pendiente (no bloquea) | Publicación real en Pub/Sub y cursor en GCS: se prueban con el Lote 1.3 (infraestructura) y el Lote 1.5 (emulador). Repetir las pruebas en Python 3.11 con el `Dockerfile`. |

## v2.14 · desde v2.13

**Lote que lo motiva:** Lote 1.1 (HU-008, HU-009) · cierre

| Área | Cambio |
|---|---|
| Verificación | `python -m pytest` en `project/`: **64 passed, 0 omitidas** (con `jsonschema` 4.26.0 y `contracts/` presentes, así que las pruebas contra los schemas sí corrieron). `python -m pytest contracts/tests -q`: **18 passed**, sin cambios. Entorno: Windows, Python 3.14.7, pytest 9.1.1. |
| Corrección de una prueba | En la primera ejecución falló `test_identico_entre_procesos_con_distinto_hashseed` (63 de 64). Causa: en Windows la salida de un subproceso por tubería usa cp1252 y no soporta los emojis de los comentarios (`UnicodeEncodeError`). Era un defecto de la prueba, no del generador: ahora el subproceso se lanza con `PYTHONIOENCODING=utf-8` y se lee con `encoding="utf-8"`. |
| CLI | `scripts/generate_comments.py --n 50000 --seed 7` generó 50.000 filas en 3,5 s (unas 14.000 por segundo) en la máquina de desarrollo. |
| Decisiones | Los tres parámetros del generador (§5.12) quedan **adoptados por defecto**, igual que ADR-02 en Fase 0: se cambian aquí primero si se quiere otro valor (§9 #12). |
| Lote 1.1 | Pasa a ✅ cerrado en PLAN.md §3. El siguiente es el **1.2**. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |
| Pendiente (no bloquea) | Confirmar los criterios de aceptación de HU-008 y HU-009 contra `HU.md` (§9 #11). Las pruebas pasaron en Python 3.14, pero el proyecto apunta a 3.11 (§5.1): se repiten en 3.11 cuando haya `Dockerfile` (Lote 1.3) y CI (Lote 7.1). |

## v2.13 · desde v2.12

**Lote que lo motiva:** Lote 1.1 (HU-008, HU-009) · entrega

| Área | Cambio |
|---|---|
| Lote 1.1 (HU-008, HU-009) | Entregado: `src/common/{envelope,hashing,hashtags,label_map}.py` + `label_map.yaml`, el generador (`src/simulator/generator/`), `config/brands.yaml`, `scripts/generate_comments.py`, `pytest.ini`, `src/conftest.py` y 64 pruebas. Detalle en **§3.2.2**. |
| Generador | Los 7 hallazgos de §3.2.1 pasan de decisión a **implementado** (columna nueva de la tabla). Categoría `informativo` con 40 plantillas y 18 comparativas de dos marcas (el mínimo de §3.2.1 #3 era 15 y 15). El ruido ya no puede quitar la marca. Semilla por evento con `random.Random(f"{seed}\|{fuente}\|{seq}")`. |
| CSV del CLI | Columnas `marca,comentario,sentimiento[,categoria]`. `marca` lleva la **clave canónica** (`postobon`), no el nombre con tilde. `-n` por defecto pasa a 1000. UTF-8 sin BOM (`--bom` para Excel). |
| Corrección de contenido | Una plantilla de nostalgia no nombraba la marca, así que el texto no coincidía con la columna `marca`. Ahora es `"{m} me recuerda a cuando todo era más sencillo."`. |
| Parámetros nuevos (propuestos) | Mezcla de categorías cuando no se fija sentimiento (28 / 24 / 18 / 15 / 15 %), 30 % de comparativas dentro de `informativo` y truncado de hashtags a 100 caracteres. Registrados en §5.12; **pendientes de aprobación** (§9 #12). |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. En §5.2 la nota de `normalize_hashtag()` pasa de futuro a implementado, y §5.13 anota las dos pruebas nuevas que usan los schemas. |
| Documento | Nueva §3.2.2; ampliadas §3.2, §3.2.1 (columna de resultado), §3.3, §3.6, §3.8, §5.2, §5.11, §5.12, §5.13, §6, PLAN.md §1, §9 y PLAN.md §3. |
| Pendiente | Correr `pytest` real (las 60 pruebas que se ejecutaron usaron un ejecutor mínimo y 4 se omitieron por falta de `jsonschema` y de `contracts/`), confirmar los criterios de aceptación de HU-008 y HU-009 contra `HU.md` y aprobar los parámetros nuevos. *(Resuelto en v2.14; los criterios de HU-008 y HU-009 siguen en §9 #11.)* |

## v2.12 · desde v2.11

**Lote que lo motiva:** Lote 1.0 (HU-046) · aplicado y verificado

| Área | Cambio |
|---|---|
| Verificación | `validate.sh` pasa (`fmt` y `validate`) en las 4 raíces. `gcloud billing projects describe` confirma `billingEnabled: true` con la cuenta `01CCA0-88151A-A99D15` (abierta). |
| Aplicación | `gcp/bootstrap` y `aws/bootstrap` aplicados; los valores reales están en §5.14. En GCP el primer `apply` falló porque `alert_emails` tenía un correo sin `@`; se corrigió el `terraform.tfvars` y se repitió sin recrear el bucket de estado. |
| Entornos `dev` | `init -backend-config=backend.hcl` correcto en ambos (backends `gcs` y `s3`). El `plan` solo propone *outputs* nuevos y ningún recurso, lo esperado en entornos vacíos. |
| Decisiones | §9 #1 (facturación) y #10 (montos y `billing_account_id`) cerrados. **ADR-15:** con Terraform ≥ 1.10 el backend S3 avisa de que `dynamodb_table` está deprecado; funciona igual y la migración a `use_lockfile` queda como mejora opcional. |
| Repositorio | Los `.terraform.lock.hcl` de las 4 raíces se versionan. `tfplan`, `terraform.tfvars`, `backend.hcl` y el estado local no se versionan. Recomendado: `.gitattributes` con `*.sh`, `*.tf` y `*.hcl` en `eol=lf` (el repo se trabaja en Windows y WSL). |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |

## v2.11 · desde v2.10

**Lote que lo motiva:** Lote 1.0 (HU-046) · entrega

| Área | Cambio |
|---|---|
| Lote 1.0 (HU-046) | `project/infra/` entregado: un *bootstrap* por nube (estado local, se aplica una vez), 5 módulos (3 de GCP, 2 de AWS), entornos `dev` vacíos ya conectados al estado remoto, `validate.sh` y `BOOTSTRAP.md`. Contrato completo en **§5.14**. |
| Estado remoto (ADR-14) | GCP: bucket `{GCP_PROJECT}-ta-tfstate` con versionado. AWS: bucket `ta-{ENV}-{AWS_ACCOUNT_ID}-tfstate` y tabla de bloqueo `ta-{ENV}-tflock` (ADR-15). |
| APIs de GCP | 19 APIs habilitadas desde Terraform (lista en §5.14). |
| Presupuestos | GCP (moneda de la cuenta, `COP` por defecto) y AWS (USD) con aviso por correo en 50 %, 80/90 % y 100 % del gasto real, y en 100 % del pronóstico. Los montos del ejemplo (400000 COP y 50 USD) son provisionales (§9 #10). |
| Decisiones | **ADR-15:** bloqueo con DynamoDB en AWS. **ADR-16:** bootstrap aislado, con estado local y protecciones contra borrado. |
| Documento | Nueva §5.14; ampliadas §5.1 (nombres, proveedores, `allowed_account_ids`), §5.8 (identidades aún inexistentes), §5.9 (outputs reales), §5.11 y §5.12 (retención del estado y presupuestos); §6 (árbol de `infra/`), PLAN.md §1 (Lote 1.0), §9 y PLAN.md §3. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |
| Corrección | §5.1 decía que cada módulo expone `outputs.tf`. En el código, los módulos declaran sus outputs en su `main.tf`; `outputs.tf` existe en cada raíz (bootstrap y entorno). |
| Pendiente | Ejecutar `validate.sh`, confirmar `billing_account_id` y montos, y aplicar el bootstrap (§5.14, PLAN.md §3). No se pudo comprobar `terraform validate` al preparar esta versión. *(Resuelto en v2.12.)* |

## v2.10 · desde v2.9

**Lote que lo motiva:** Fase 0 · cierre (HU-001, HU-002, HU-003)

| Área | Cambio |
|---|---|
| Fechas | Se eliminan todas las fechas y el calendario: encabezados de fase, hitos, aprobaciones y el riesgo de tiempo. Sprint 2 y Sprint Final quedan como entregas, sin día (PLAN.md §1, PLAN.md §3). Los timestamps de §5.2 son datos de ejemplo, no fechas del plan. |
| Proyecto GCP | Nuevo ID `project-0568d9ba-848f-413c-88b`, número `63567159332`, nombre visible `proyectoDatos`. Reemplaza a `project-2911bf21-1f97-41aa-ac6` (§5.1, §9 #1). |
| Región | **ADR-12:** la región de GCP pasa a `us-central1`. Las cuotas de vCPU (p. ej. 8 vCPU en la familia C2, uso 0) y de Dataflow (p. ej. 640 *shuffle slots*) se verificaron en esa región. |
| Autenticación de Terraform en GCP | Application Default Credentials (`gcloud auth application-default login`), sin llaves JSON (§5.1). |
| Accesos y alertas | Por ahora solo el correo de Alejandro: único destinatario de alertas y único usuario con acceso. Miguel y Camila se agregan cuando lo necesiten sus lotes (HU-046). |
| Decisiones | §9 #8 (`dq_results.layer`) y #9 (`fact_posts.dt`) **aprobados** tal como se propusieron. |
| `contracts/` | Revisión del equipo completada (HU-001). |
| Prueba de sincronización §5 ↔ `contracts/` | Recomendada, no bloquea: pasa a la Fase 5 (§5.13). |
| Fase 0 | **Cerrada** (PLAN.md §3). |

## v2.9 · desde v2.8

**Lote que lo motiva:** Lote 0.1 (HU-001) · auditoría de `contracts/` contra §5; prepara el Lote 1.1

| Área | Cambio |
|---|---|
| Auditoría | Se revisó `contracts/` contra §5 (18 pruebas en verde). Lo que solo vivía en `contracts/` y el documento delegaba pasa a este documento, que vuelve a ser suficiente para trabajar sin abrir código. |
| §5.2 | Reglas de validación que aplica el schema (formatos, patrones, campos extra), invariantes por fuente y `kind`, reglas del envelope enriquecido y referencia a `normalize_hashtag()`. |
| §5.5 | `emotion_label` con sus 7 clases, scores en [0, 1] y tabla `dq_results` (11 columnas, partición y clustering). |
| §5.7 | Columnas de las 9 tablas de Curated, llaves foráneas naturales, regla de regeneración y día UTC. |
| §5.10 | `dq_results.kpi` toma los valores de esta tabla. |
| §5.13 | **Nueva.** Qué contiene `contracts/`, regla de cambio, qué cubren (y qué no) sus pruebas, y la prueba de sincronización pendiente. |
| §6 | Árbol de `contracts/` ordenado (las cuatro DDL de BigQuery juntas). |
| Lote 1.1 | Incluye `normalize_hashtag()` en `src/common/`. |
| §9 | Dos puntos abiertos: valores de `dq_results.layer` y significado de `fact_posts.dt` (#8 y #9). |
| Calendario | Nota sobre fechas vencidas (retirada en v2.10: el plan ya no usa fechas). |

## v2.8 · desde v2.7

**Lote que lo motiva:** Fase 0 · decisiones cerradas (ADR-02, ADR-13)

| Área | Cambio |
|---|---|
| Decisiones | **ADR-02** (Cloud Run) y **ADR-13** (mapeo categoría → sentimiento) **aprobados**; dejan de ser "por defecto" (§9). |
| `hashtags[]` | Forma canónica: minúsculas, **sin `#`**, sin tildes, solo `[a-z0-9_]` (1–100), sin repetidos y en orden de aparición. El texto conserva la forma original (§3.8, §5.2). Schemas, ejemplos y pruebas actualizados (ahora 45 ejemplos y 18 pruebas). |
| Autenticación | Terraform se autentica en AWS con el perfil local de AWS CLI (`aws configure`), sin claves en el código (§5.1). La de GCP queda pendiente. |
| Facturación GCP | La consola muestra el proyecto vinculado a una cuenta de facturación activa, con cargos estimados de 0 COP. Faltan las cuotas (§9). |
| Alertas | Un solo destinatario por ahora. La dirección vive en `terraform.tfvars` (`alert_emails`), que no se versiona (§5.12). |

## v2.7 · desde v2.6

**Lote que lo motiva:** Lote 0.1 (HU-001) · entrega de `contracts/`

| Área | Cambio |
|---|---|
| Contratos | `project/contracts/` con 6 JSON Schemas (envelope, envelope enriquecido, 3 payloads, atributos Pub/Sub), DDL de BigQuery (4 tablas) y Athena (Raw, Trusted, 9 tablas Curated), 45 ejemplos y 18 pruebas (cifras vigentes desde v2.8). Detalle en `project/contracts/README.md`. |
| §5.4 / §5.7 | Los Parquet de `enriched/` y `raw/` usan el **esquema plano** de `posts_enriched` (§5.5), con `payload` como JSON en string. |
| §5.7 | Raw se proyecta por `origin/source/dt/hr` (claves `part_*`; el histórico también escribe `hr`). Trusted por `source/dt` con `dt = DATE(event_time)` UTC y columna `text_clean`. |
| §5.5 | `avg_sentiment` = media con signo en [-1, 1]. `dq_results` y Curated con columnas definidas en `contracts/`; desde v2.9 también en §5.5 y §5.7. |
| §5.2 | `sim.chaos` ∈ `duplicate\|late\|null_fields\|malformed`. La forma de `hashtags[]` se cerró en v2.8. |

## v2.6 · desde v2.5

**Lote que lo motiva:** Lote 0.1 (HU-001, HU-002) · README y diagrama; estructura `docs/` + `project/`

| Área | Cambio |
|---|---|
| Repositorio | Los documentos viven en `docs/` y el código en `project/` (§6). Las rutas de este documento (`src/`, `infra/`, `contracts/`, `scripts/`) son relativas a `project/`. |
| CI/CD | Los workflows de GitHub Actions deben estar en `.github/workflows/` en la **raíz** del repositorio y usar `working-directory: project`. Lo mismo aplica a `.pre-commit-config.yaml` y `.gitignore`. |
| Diagrama | `docs/trend_analyzer_multicloud.drawio` actualizado a la v2.5 (HU-002) y exportado a `docs/trend_analyzer_multicloud.jpg`. |
| README | Reescrito con entornos, marcas, decisiones del generador, estado por fases y estructura. |

## v2.5 · desde v2.4

**Lote que lo motiva:** Fase 0 · cuentas y alertas (prepara el Lote 1.0, HU-046)

| Área | Cambio |
|---|---|
| Cuentas | GCP `project-2911bf21-1f97-41aa-ac6` (región `us-east1`, facturación con crédito a cargo de Alejandro Sepulveda; proyecto y región reemplazados en v2.10) y AWS `648617469176` (`us-east-1`). |
| Alertas | Canal por **correo**: suscriptor de `ta-alerts` → log estructurado → alerta basada en logs de Cloud Monitoring con canal de correo (sin SMTP ni secretos). |
| Etiquetas | `owner=alejandro` en todos los recursos. |
| Riesgo nuevo | Cuotas de la cuenta de crédito de GCP pueden limitar Dataflow. |

## v2.4 · desde v2.3

**Lote que lo motiva:** Fase 0 · prepara el Lote 1.0 (HU-046); crea HU-047 (Lote 1.5) y HU-048 (Lote 5.4)

| Área | Cambio |
|---|---|
| Decisiones | **ADR-12:** región de GCP `us-east1` (reemplazada por `us-central1` en v2.10). **ADR-13:** mapeo `categoria → sentimiento` (incluye la categoría nueva `informativo`). **ADR-14:** estado remoto de Terraform. ADR-02 (Cloud Run) queda adoptado por defecto. |
| Generador | Los 7 hallazgos de §3.2.1 pasan de propuesta a decisión y se incorporan a HU-008. |
| HU nuevas | **HU-046** Bootstrap de Terraform (Lote 1.0) · **HU-047** Entorno local con emulador de Pub/Sub (Lote 1.5) · **HU-048** Prueba de humo end-to-end (Lote 5.4). |
| Parámetros operativos | Nueva §5.12: ventanas tardías, arranque en frío del z-score, cooldown de alertas, zona horaria, retención, idempotencia de Glue, contratos en CI. |
| Riesgos | Dos nuevos: infraestructura desde cero y falsas alertas al arrancar. |

## v2.3 · desde v2.2

**Lote que lo motiva:** Fase 0 · respuestas previas al Lote 1.1 (HU-008, HU-009)

| Área | Cambio |
|---|---|
| Marcas v1 | **Postobón (propia), Pepsi y Sprite (competidores)**. El script real también trae CocaCola y Colombiana: quedan fuera de v1 (§3.2.1). |
| Región AWS | `us-east-1` confirmada. Proyecto y región de GCP siguen sin definir; `infra/` está vacío (partimos de cero). |
| Fuentes y NLP | Reddit, Facebook e Instagram confirmadas; ADR-05 (RoBERTuito) y su licencia académica aceptados. |
| Generador | Analizado el script real (§3.2.1): hay 5 puntos a resolver antes del Lote 1.1, entre ellos el mapeo `categoria → sentimiento` y los hashtags que el script ya inserta en el texto. |
| Contratos | Sin cambios en §5; `schema_version` sigue en `"1.0"`. |

## v2.2 · desde v2.1

**Lote que lo motiva:** Fase 0 · cambio de fuentes (afecta HU-008 a HU-014, HU-022, HU-024, HU-033 y HU-040)

| Área | v2.1 | v2.2 |
|---|---|---|
| Fuentes simuladas | Mastodon, Reddit, RSS | **Reddit, Facebook, Instagram** (`source` ∈ `reddit`, `facebook`, `instagram`) |
| Origen del texto | Datasets de emociones (TEC, GoEmotions, SemEval, DailyDialog) en `prepared/*.jsonl` | **Generador de comentarios** en Python (`marca,comentario`), en proceso (§3.2) |
| Idioma | Inglés | **Español (Colombia)**, `lang = "es"` |
| NLP | `emotion-english-distilroberta-base` | **RoBERTuito (pysentimiento)**: sentimiento y emoción en español (ADR-05) |
| Marca | Asignada al azar al ~35% de los mensajes | **Viene del generador**; el simulador solo controla los pesos en el tiempo (§3.8) |
| Envelope | `dataset`, `cycle`, `row_index`, `sim.label_raw`, `sim.label_unified` | `generator`, `seq`, `sim.label_sentiment` (opcional), `sim.scenario` |
| Pub/Sub | `ta-raw-mastodon / reddit / rss` | `ta-raw-reddit / facebook / instagram` |
| Preparación offline | `scripts/prepare_datasets.py` | Eliminada. Se agrega `scripts/generate_comments.py` (snapshots / histórico) |
| ADR | — | ADR-01 y ADR-05 actualizados; **ADR-11** nuevo |

`schema_version` se mantiene en `"1.0"` porque ningún componente consume aún el contrato (todo está ⬜ en PLAN.md §3). Si ya existe código que lo use, subir a `"1.1"` y migrar.

---

## Versiones anteriores a v2.2

- **v2.1:** cada fase y lote queda asociado a sus historias de usuario (`HU.md`, formato HU-000). Ver `PLAN.md` §1 y la matriz de trazabilidad `PLAN.md` §2. Las fuentes simuladas eran Mastodon, Reddit y RSS con datasets de emociones en inglés (evolución descrita en §1.1).

> Nota sobre `README.md`: se actualizó en v2.6 (tabla de fuentes, generador y decisiones, entornos, stack, estructura del repo, estado por fases y *quick start*); el detalle está en la entrada v2.6.