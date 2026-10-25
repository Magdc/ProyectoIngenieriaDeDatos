# Validación del Lote 2.3 (HU-022)

Paso a paso para comprobar el NLP **desde la raíz del repo** (la carpeta que contiene `docs/` y `project/`). Contrato en `docs/ARCHITECTURE.md` §3.15.

> **Estado: lote cerrado en v2.25.** Local (D1–D6) y GCP (E2–E9) en OK; resultados en `ARCHITECTURE.md` §3.15 ("Estado de la verificación (v2.25, cierre)"). Esta guía sirve para repetir la validación tras un cambio en el NLP o en la imagen.

Cada paso dice qué correr y qué debe salir. Si algo no coincide, se detiene ahí y se revisa la tabla de problemas del final.

**Requisito:** el Lote 2.2 validado (`docs/validation/validation-2-2.md`), o al menos sus partes A a D. El 2.3 corre dentro de la rama enriched.

**Si copias el 2.3 antes de cerrar el 2.2 en GCP:** lanza el job con `export TA_NLP_ENABLED=false`. Sin eso, el job buscará la imagen `dataflow-nlp`, que todavía no existe.

**Atajo automático.** `project/scripts/validate_2_3.py` corre estas mismas partes en orden y comprueba cada salida. Al final deja un resumen OK/FALLA y la evidencia en `project/evidencia/validacion_2_3.json`. Se corre desde la raíz del repo, con `~/.venvs/ta-streaming` activo:

```
python project/scripts/validate_2_3.py tests local
python project/scripts/validate_2_3.py cloud --off-after
```

- La primera línea cubre las partes B, C y D, que son gratis.
- La segunda cubre la parte E y drena el job al terminar.
- Sin argumentos corre todo.
- Construye las imágenes solo si faltan (`--rebuild-local` y `--rebuild-image` las fuerzan).
- Aplica Terraform solo si el `plan` es exactamente el permiso de Artifact Registry.

Los pasos manuales de abajo sirven para revisar un paso que falló.

| Parte | Qué cubre | Cuesta dinero |
|---|---|---|
| A | Archivos | No |
| B | Pruebas automáticas | No |
| C | Terraform sin credenciales | No |
| D | Local con modelos reales (imagen de NLP + DirectRunner) | No (descarga ~2,5 GB) |
| E | GCP: permiso, imagen, job con NLP, BigQuery | Sí: Cloud Build (~0,10 USD por construcción) y Dataflow con el simulador (~0,25 USD/h) |
| F | Apagado | — |
| G | Cierre | — |

---

## Parte A · Archivos

```bash
source ~/.venvs/ta-streaming/bin/activate          # Python 3.11, Beam 2.77 (Lote 2.1)
ls project/src/streaming/{nlp,nlp_testing}.py \
   project/src/streaming/nlp_image/{Dockerfile,fetch_models.py,image_smoke.py,requirements-nlp.txt,cloudbuild.yaml,.dockerignore,.gcloudignore} \
   project/src/streaming/tests/test_nlp{,_check,_contract,_local}.py \
   project/scripts/{build_nlp_image.sh,local_nlp.sh,nlp_bench.py,validate_2_3.py} \
   project/docker-compose.nlp.yml \
   project/infra/gcp/environments/dev/nlp.tf
chmod +x project/scripts/build_nlp_image.sh project/scripts/local_nlp.sh
git status --short
```

Esperado:
- los 19 archivos, sin `No such file`;
- `git status` muestra los nuevos (`??`) y estos modificados (`M`): `enriched.py`, `enriched_check.py`, `enriched_cli.py`, `ENRICHED.md` y los cuatro de `docs/`.

`pipeline.py` y `dataflow_job.sh` **no** cambian respecto al 2.2.

---

## Parte B · Pruebas automáticas

**B1. Preprocesado real de pysentimiento** en el entorno de pruebas. Es pequeño: solo el paquete, sin `torch` ni `datasets`.

```bash
python -m pip install --no-deps pysentimiento==0.7.3
python -m pip install emoji==2.14.0
```

**B2. Pruebas de los lotes 2.2 y 2.3:**

```bash
python -m pytest -c project/pytest.ini project/src/streaming -k "enriched or nlp" -rs -q
```

Esperado: **98 passed, 0 skipped** (67 del 2.2 + 31 del 2.3, contando las de v2.24 y v2.25). `validate_2_3.py` exige al menos 89.

| Si falla… | Significa |
|---|---|
| `test_label_map_cubre_las_etiquetas_crudas_de_los_modelos` | El `label_map.yaml` real no mapea alguna etiqueta (`NEG`, `NEU`, `POS`, `others`…). El mensaje dice cuál. |
| `test_version_cumple_el_patron_del_contrato` | El patrón de `nlp_model_version` del schema no acepta un hex de 12. Mandarme el patrón. |
| `test_etiquetas_mapeadas_son_las_del_contrato` | Los enums de `sentiment_label` / `emotion_label` del schema difieren de §5.2. |
| `test_preprocess_tweet_de_pysentimiento` omitida | Falta B1. |

**B3. Regresión completa:**

```bash
python -m pytest -c project/pytest.ini project/src/streaming -rs -q      # 189 passed (91 + 67 + 31)
python -m pytest -c project/pytest.ini project/contracts/tests -q        # 18 passed
```

---

## Parte C · Terraform sin credenciales

```bash
bash project/infra/validate.sh
```

Esperado: `fmt` y `validate` en verde.

---

## Parte D · Local con los modelos reales

Sin credenciales de GCP. La construcción necesita salida a Hugging Face y a `download.pytorch.org`.

**D1. Construir la imagen local** (descarga los modelos y corre la prueba de humo; 10 a 15 min la primera vez):

```bash
bash project/scripts/local_nlp.sh build 2>&1 | tee project/evidencia/local_nlp_build.log
```

En el log deben aparecer:
- `beam 2.77.0 torch 2.5.1… transformers 4.46.3`;
- `sentiment: pysentimiento/robertuito-sentiment-analysis@<commit> etiquetas ['NEG', 'NEU', 'POS']`;
- `emotion: … etiquetas ['anger', 'disgust', 'fear', 'joy', 'others', 'sadness', 'surprise']`;
- las dos frases de control con `POS` y `NEG`;
- `Prueba de humo OK`.

Si la prueba de humo falla, la construcción se detiene ahí.

**D2. Repetir la prueba de humo** sobre la imagen ya construida:

```bash
bash project/scripts/local_nlp.sh smoke
```

Esperado: `Prueba de humo OK`.

**D3. Levantar el pipeline local con NLP** (emulador, simulador y `dataflow-local` con la imagen nueva):

```bash
(cd project && bash scripts/local_dataflow.sh up)    # si no estaba arriba (Lote 2.1)
bash project/scripts/local_nlp.sh up
bash project/scripts/local_nlp.sh logs | grep -m4 -E "nlp_config|nlp_model_loaded|enriched_config"
```

Esperado:
- `nlp_config` con `"enabled": true` y `"reason": "modelos locales"`;
- dos `nlp_model_loaded`, uno de `sentiment` y otro de `emotion`, con su `revision`.

Salir de los logs con Ctrl+C.

**D4. Esperar unos 4 minutos** y verificar:

```bash
python project/scripts/verify_enriched.py report --local --hours 1 --expect-nlp \
  --json-out project/evidencia/local_nlp_report.json
echo "exit=$?"
```

Esperado: `Veredicto: OK`, `exit=0`.
- Todo lo del 2.2 sigue en `OK`.
- Además, por fuente: `NLP presente` (≥ 99 %) y `NLP: etiquetas, scores y versión`.
- Debajo de cada fuente sale la distribución de sentimiento y emoción y el `acuerdo con ground truth`. Es solo un dato: con comentarios de plantillas se espera alto (§3.8). La medición formal es HU-033.

**D5. Inspección manual de un Parquet:**

```bash
python - <<'EOF'
import glob, sys
sys.path.insert(0, "project/src")
from common.parquet_rows import parquet_bytes_to_rows
f = sorted(glob.glob("project/.local-lake/enriched/source=facebook/**/*.parquet", recursive=True))[-1]
for r in parquet_bytes_to_rows(open(f, "rb").read())[:5]:
    print(r["sentiment_label"], round(r["sentiment_score"], 2), r["emotion_label"], round(r["emotion_score"], 2),
          "| gt:", r["label_sentiment"], "|", r["text"][:60])
print(r["nlp_model_version"])
EOF
```

Esperado:
- cinco líneas con etiquetas en español del contrato (`positive`, `joy`, …), scores entre 0 y 1;
- una versión `robertuito-sentiment@<12 hex>+robertuito-emotion@<12 hex>` con los mismos commits del log de D1.

**D6. Regresión del Lote 2.1** con la imagen de NLP:

```bash
(cd project && bash scripts/local_dataflow.sh check)     # OK (13/13)
```

**D7. Volver a la imagen normal y apagar:**

```bash
bash project/scripts/local_nlp.sh restore                 # dataflow-local con la imagen del 2.1
(cd project && bash scripts/local_dataflow.sh down-all)
```

---

## Parte E · GCP

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b
export GCP_REGION=us-central1
gcloud config set project "$GCP_PROJECT"
```

**E1. Preflight (solo lectura):**

```bash
(cd project && bash scripts/preflight.sh)
gcloud dataflow jobs list --region "$GCP_REGION" --status=active --format='value(name,id)'
bash project/scripts/hmac_key.sh status            # 1 versión (Lote 2.2)
```

Esperado:
- ningún job activo; si hay uno, `(cd project && bash scripts/dataflow_job.sh drain)` y esperar a que termine;
- el simulador apagado;
- la llave HMAC creada.

**E2. Permiso de Artifact Registry para `sa-dataflow`:**

```bash
terraform -chdir=project/infra/gcp/environments/dev plan -out=tfplan
```

Esperado: **`Plan: 1 to add, 0 to change, 0 to destroy`**, que es `google_artifact_registry_repository_iam_member.dataflow_image_reader`.

```bash
terraform -chdir=project/infra/gcp/environments/dev apply tfplan
gcloud artifacts repositories get-iam-policy ta-images --location "$GCP_REGION" \
  --flatten='bindings[].members' --filter='bindings.role=roles/artifactregistry.reader' \
  --format='value(bindings.members)'
```

Esperado: `serviceAccount:sa-dataflow@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com`.

**E3. Construir y publicar la imagen** en Cloud Build (15 a 25 min):

```bash
bash project/scripts/build_nlp_image.sh cloudbuild 2>&1 | tee project/evidencia/nlp_image_build.log
```

Esperado: en el log, las mismas líneas de D1 (etiquetas, frases de control, `Prueba de humo OK`) y `SUCCESS`. Al final, el script imprime la línea `export TA_NLP_IMAGE=…:<commit>`.

```bash
gcloud artifacts docker images list "$GCP_REGION-docker.pkg.dev/$GCP_PROJECT/ta-images/dataflow-nlp" \
  --include-tags --format='table(version.basename(),tags)'
```

Esperado: una versión con las etiquetas `<commit>` y `latest`.

**E4. Lanzar el job con la imagen exacta:**

```bash
unset TA_NLP_ENABLED
export TA_NLP_IMAGE="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT/ta-images/dataflow-nlp:<commit>"   # la línea que imprimió E3
(cd project && bash scripts/dataflow_job.sh start) 2>&1 | tee project/evidencia/nlp_job_start.log
grep -E "nlp_config|nlp_container_image" project/evidencia/nlp_job_start.log
```

Esperado:
- `nlp_config` con `"enabled": true, "reason": "dataflow"`;
- `nlp_container_image` con la imagen de `TA_NLP_IMAGE`.

```bash
JOB_ID=$(gcloud dataflow jobs list --region "$GCP_REGION" --status=active --format='value(id)' --limit=1)
gcloud dataflow jobs describe "$JOB_ID" --region "$GCP_REGION" --full --format=json \
  | grep -o 'dataflow-nlp:[0-9A-Za-z._-]*' | sort -u
(cd project && bash scripts/dataflow_job.sh status)
```

Esperado:
- una sola línea, `dataflow-nlp:<etiqueta>`, con la misma etiqueta de `TA_NLP_IMAGE`. La descripción del job no trae un campo `sdkContainerImage`: la imagen aparece en las opciones del pipeline y en los contenedores del worker;
- el job en *Running*, que con la imagen más grande tarda unos 5 a 8 min.

**E5. Logs de los workers** (unos minutos después de *Running*):

```bash
gcloud logging read 'resource.type="dataflow_step" AND "nlp_model_loaded"' --freshness=30m --limit=4 --format='value(timestamp)'
gcloud logging read 'resource.type="dataflow_step" AND ("nlp_inference_failed" OR "enrich_error" OR "bq_insert_failed")' --freshness=30m --limit=5
```

Esperado:
- dos o más `nlp_model_loaded` (sentimiento y emoción; uno por VM gracias al modelo compartido);
- la segunda consulta vacía.

**E6. Encender el simulador:** `simulator_running = true` en `terraform.tfvars` y aplicar.

```bash
terraform -chdir=project/infra/gcp/environments/dev apply
```

Esperar **unos 10 minutos**.

**E7. Verificador con NLP** (lake en GCS y BigQuery). Correrlo **al menos 15 minutos después de que el job pase a *Running***, con `--hours` igual al tiempo transcurrido desde ese momento (aquí, ~20 min):

```bash
python project/scripts/verify_enriched.py report --hours 0.35 --expect-nlp \
  --json-out project/evidencia/nlp_report.json
echo "exit=$?"
```

Por qué esa ventana:
- La cobertura `raw → enriched` solo cuenta las ventanas de `raw/` (de 5 min) que empiezan dentro de la ventana revisada y que cerraron hace al menos 10 min. Con menos de 15 min no queda ninguna, y salen los avisos `sin archivos de raw/ cerrados en la ventana` y `user_hash … no se pudo cruzar con raw/`.
- Si la ventana empieza antes del lanzamiento, entran filas del job anterior sin NLP y `NLP presente` falla.

`validate_2_3.py cloud` ya hace las dos cosas: cuenta desde el arranque y espera lo que falte.

Esperado: `Veredicto: OK`, `exit=0`, con:
- `NLP presente`, `NLP: etiquetas, scores y versión` y `BigQuery: NLP` en `OK`;
- sin aviso de varias versiones.

Además: cobertura `raw → enriched` ≥ 95 % por fuente y `user_hash determinista (1 a 1)` en `OK`.

**E8. Consultas en BigQuery:**

```bash
bq query --use_legacy_sql=false '
SELECT source, sentiment_label, emotion_label, COUNT(*) AS n, ROUND(AVG(sentiment_score), 2) AS s, ROUND(AVG(emotion_score), 2) AS e
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE) AND nlp_model_version IS NOT NULL
GROUP BY 1, 2, 3 ORDER BY 1, n DESC'
```

Esperado:
- `sentiment_label` en `negative`, `neutral` o `positive`;
- `emotion_label` en las 7 clases;
- scores medios entre 0 y 1.

```bash
bq query --use_legacy_sql=false '
SELECT nlp_model_version, COUNT(*) AS n,
       COUNTIF(sentiment_label IS NULL OR emotion_label IS NULL) AS sin_nlp,
       APPROX_QUANTILES(TIMESTAMP_DIFF(process_time, ingest_time, MILLISECOND), 100)[OFFSET(95)] AS p95_emision_a_bq_ms
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY 1'
```

Esperado:
- una sola versión no nula, con `sin_nlp = 0`;
- `p95_emision_a_bq_ms` **< 60000**. Es el criterio de HU-022: un evento emitido aparece en BigQuery con `emotion_label` en menos de 1 minuto.

Una fila con `nlp_model_version = null` corresponde a eventos anteriores al lanzamiento.

```bash
bq query --use_legacy_sql=false '
SELECT label_sentiment, sentiment_label, COUNT(*) AS n
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
  AND label_sentiment IS NOT NULL AND nlp_model_version IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, n DESC'
```

Es una matriz de confusión preliminar, solo un dato (HU-033 la formaliza). Se espera una diagonal dominante.

**E9. Centinela de punta a punta** (regresión del 2.1 y criterio de < 1 min):

```bash
(cd project && python scripts/verify_streaming.py check --allow-cloud --json-out evidencia/streaming_check_2_3.json)
```

Esperado: **OK** (13/13). Con el `event_id` del centinela que trae el JSON:

```bash
bq query --use_legacy_sql=false '
SELECT event_id, sentiment_label, emotion_label, nlp_model_version,
       TIMESTAMP_DIFF(process_time, ingest_time, SECOND) AS segundos
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_id = "PEGAR_AQUI" AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)'
```

Esperado: una fila con NLP y `segundos` < 60.

**E10. Contadores del job** (consola de Dataflow → *Job metrics* → *Custom counters*):

| Contador | Esperado |
|---|---|
| `nlp/sentiment_rows`, `nlp/emotion_rows` | crecen igual que `enriched/enriched_rows` |
| `nlp/nlp_failed_rows` | 0 |
| `enriched/bq_failed_rows`, `enriched/enrich_errors` | 0 |

En la pestaña de recursos del worker, la memoria debe quedar holgada (< 6 GB de 8).

**E11. Licencias de los modelos.** Leer las *model cards* de los dos modelos en Hugging Face y anotar su licencia y sus datos de entrenamiento (TASS) para el informe (HU-022, HU-044):
- `huggingface.co/pysentimiento/robertuito-sentiment-analysis`
- `huggingface.co/pysentimiento/robertuito-emotion-analysis`

---

## Parte F · Apagado

```bash
(cd project && bash scripts/dataflow_job.sh drain)
```

Luego poner `simulator_running = false` en `terraform.tfvars` y aplicar:

```bash
terraform -chdir=project/infra/gcp/environments/dev apply
```

Comprobar que nada sigue corriendo:

```bash
gcloud dataflow jobs list --region "$GCP_REGION" --status=active                  # vacío
gcloud run services list --region "$GCP_REGION" --format='value(name)'            # sin ta-stream-simulator
gcloud builds list --region "$GCP_REGION" --ongoing                               # vacío
gcloud pubsub subscriptions list --format='value(name)' | grep ta-verify          # vacío
```

La imagen queda en Artifact Registry. Ocupa ~2,5 GB por versión y la política del Lote 1.3 conserva las 10 más recientes: unos centavos al mes.

---

## Parte G · Cierre

| Criterio de HU-022 | Dónde se comprueba |
|---|---|
| Cada evento recibe `emotion_label` (7 clases), `emotion_score`, `sentiment_label` y `sentiment_score` | D4, D5, E7, E8 |
| RoBERTuito de pysentimiento con `RunInference` en un contenedor personalizado, aplicando `preprocess_tweet` | B2 (`test_handler_preprocesa…`, `test_preprocess_tweet…`), D1/E3 (imagen), E4 (imagen en la descripción del job) |
| Mapeo en `src/common/label_map.yaml`, con el nombre exacto de las etiquetas verificado | B2 (`test_label_map…`), D1/E3 (etiquetas en la prueba de humo) |
| `nlp_model_version` registrado | D5, E8 (una sola versión), B2 (`test_version…`) |
| Un evento emitido aparece en BigQuery con `emotion_label` en < 1 min | E8 (`p95_emision_a_bq_ms`), E9 |
| Si la memoria no alcanza, plan B documentado | E10 (memoria). El plan B está en §3.15 y en el riesgo de §9: solo sentimiento, o NLP en Cloud Run |
| Licencia de cada modelo verificada en su *model card* | E11 |

**Evidencia en `project/evidencia/`:**
- `validacion_2_3.json` (resumen de `validate_2_3.py`; cada corrida lo reescribe)
- `local_nlp_build.log`, `local_nlp_report.json`
- `nlp_image_build.log`, `nlp_job_start.log`, `nlp_report.json`, `nlp_report_cobertura.json`
- `streaming_check_2_3.json`

**Resultado (v2.25):** lote cerrado (2.3 → ✅, HU-022 → ✅), con las licencias anotadas en `ARCHITECTURE.md` §3.15. Quedan por anotar el conteo de B2 y B3 y el pico de memoria de E10.

---

## Problemas frecuentes

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| D1/E3: `ERROR: etiquetas de … distintas` | El modelo publicado cambió sus etiquetas | Mandarme la línea; se ajusta `EXPECTED_LABELS` y `label_map.yaml`, o se fija una revisión anterior (`SENTIMENT_REVISION` / `EMOTION_REVISION`) |
| D1/E3: `ERROR: se esperaba POS` / `NEG` | La frase de control no da lo esperado con esa versión | Mandarme la línea con el resultado |
| D1/E3 falla al instalar `torch` | Sin salida a `download.pytorch.org` | Revisar la red o el proxy de la máquina de construcción |
| D3: `nlp_config` con `"enabled": false` | `dataflow-local` no usa la imagen de NLP | Correr `local_nlp.sh up`, no `local_dataflow.sh up` |
| D3: `ModuleNotFoundError: streaming` | El 2.1 monta el código en otra ruta que no es `/work` | Cambiar `PYTHONPATH` en `docker-compose.nlp.yml` a la ruta real del montaje |
| D3 OK, pero D4 da `0 filas` y D6 falla; `docker stats` muestra la CPU de `dataflow-local` cerca de 0 % y `raw/` deja de recibir archivos | El pipeline local está congelado por el modelo compartido entre procesos (código anterior a v2.25; ver §3.15, "NLP en local") | `grep -c "share_across_processes=cfg.is_dataflow" project/src/streaming/nlp.py` debe dar 1 o más; si da 0, copiar el `nlp.py` de v2.25 y repetir `local_nlp.sh up` |
| `FileNotFoundError` en `from pathlib import Path` al correr cualquier script | La carpeta actual de la terminal quedó rota (WSL con el repo en `/mnt/c` y cambios desde Windows) | `cd ~` y volver a entrar con la ruta completa; comprobar con `python -c "import os; print(os.getcwd())"` |
| E4: el job queda en *Pending* o falla al arrancar con *image pull* | Falta el permiso de E2 o la imagen no existe | Revisar E2 y E3; `TA_NLP_IMAGE` exacto |
| E4: `sdk_container_image (…) distinto de --nlp_container_image` | `dataflow_job.sh` ya pasa otra imagen | Mandarme el log |
| E5: `nlp_inference_failed` | Error en la inferencia (memoria, texto) | Mandarme el log; las filas siguen sin NLP, no se pierden |
| E4: `AVISO` con `sdkContainerImage None` | Versión anterior de `validate23.py`, que buscaba un campo que `gcloud` no devuelve | Confirmar con el `grep -o 'dataflow-nlp:…'` de E4; la versión de v2.25 ya lo hace así |
| E7: `NLP presente` con `FALLA` y filas sin NLP | Filas anteriores al lanzamiento en la ventana | Bajar `--hours` al tiempo transcurrido desde *Running* |
| E7: avisos `sin archivos de raw/ cerrados en la ventana` y `user_hash … no se pudo cruzar` | La ventana revisada es más corta que 15 min (ventana de `raw/` + asentamiento) | Esperar y repetir con `--hours` desde *Running* (≥ 0,25); no es pérdida de datos |
| E3: la etiqueta de la imagen es una fecha y no el commit | `git rev-parse` no respondió en la terminal y `build_nlp_image.sh` usa la fecha UTC | Sirve igual; si se quiere el commit, revisar `git status` en WSL (por ejemplo, `git config --global --add safe.directory <ruta del repo>`) |
| E10: memoria cerca de 8 GB | Dos modelos por proceso (no se compartieron) | Mandarme la gráfica; plan B: `--nlp_batch_size` menor o una máquina mayor |
