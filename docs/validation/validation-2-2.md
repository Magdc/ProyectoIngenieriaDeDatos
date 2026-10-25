# Validación del Lote 2.2 (HU-020, HU-021)

Paso a paso para comprobar todo el lote **desde la raíz del repo** (la carpeta que contiene `docs/` y `project/`).
Contrato en `docs/ARCHITECTURE.md` §3.14; guía de uso en `project/src/streaming/ENRICHED.md`.

Cada paso dice qué correr y qué debe salir. Si algo no coincide, se detiene ahí y se revisa la tabla de problemas del final antes de seguir.

| Parte | Qué cubre | Cuesta dinero |
|---|---|---|
| A | Archivos y enganche en `pipeline.py` | No |
| B | Pruebas automáticas | No |
| C | Terraform sin credenciales | No |
| D | Entorno local (DirectRunner + emulador) | No |
| E | GCP: Terraform, llave, Dataflow, BigQuery | Sí, mientras corran Dataflow y el simulador (~0,25 USD/h) |
| F | Apagado | — |
| G | Cierre: criterios, evidencia y documentos | — |

---

## Parte A · Archivos y enganche

**A1. Entorno de Python** (el del Lote 2.1, Python 3.11 con Beam 2.77):

```bash
source ~/.venvs/ta-streaming/bin/activate
python --version                                   # Python 3.11.x
python -c "import apache_beam, pyarrow; print(apache_beam.__version__)"   # 2.77.0
```

**A2. Archivos del lote en su sitio:**

```bash
ls project/src/common/{enriched_row,parquet_rows,hmac_key}.py \
   project/src/streaming/{enriched,enriched_check,enriched_cli,enriched_testing}.py \
   project/src/streaming/ENRICHED.md \
   project/src/streaming/tests/test_enriched_{row,hmac_key,config,pipeline,check,contract,hook}.py \
   project/scripts/{verify_enriched.py,hmac_key.sh} \
   project/infra/gcp/modules/bigquery_stream/{main.tf,schemas/posts_enriched.json} \
   project/infra/gcp/modules/hmac_secret/main.tf \
   project/infra/gcp/environments/dev/{streaming,variables_streaming,outputs_streaming}.tf
```

Esperado: los 23 archivos listados, sin `No such file`.

```bash
chmod +x project/scripts/hmac_key.sh project/scripts/verify_enriched.py
git status --short     # solo archivos nuevos (??) y los 4 de docs/ modificados (M)
```

**A3. Enganche en `pipeline.py`.** Editar `project/src/streaming/pipeline.py`. `unique` se crea **dentro** de `build_from_messages()`; la llamada va justo debajo de esa línea:

```python
from streaming.enriched import attach_enriched   # arriba, con los demás imports

    # dentro de build_from_messages():
    unique = checked.valid | "Dedup" >> DedupByEventId(cfg.dedup_window_seconds)   # ya existe
    attach_enriched(unique)                                                         # NUEVO
```

Comprobar:

```bash
grep -n "attach_enriched" project/src/streaming/pipeline.py
```

Esperado: dos líneas, el `import` y la llamada.

---

## Parte B · Pruebas automáticas

**B1. Pruebas del lote** (con `contracts/` reales):

```bash
python -m pytest -c project/pytest.ini project/src/streaming -k enriched -rs -q
```

Esperado: **67 passed, 0 skipped**. Si alguna se omite, el motivo aparece al final (`-rs`):
- `falta contracts/` → falta `project/contracts/`.
- `falta streaming/pipeline.py` → no se está corriendo en el repo correcto.

**B2. Regresión de todo `streaming/`** (Lote 2.1 + 2.2):

```bash
python -m pytest -c project/pytest.ini project/src/streaming -rs -q
```

Esperado: **158 passed** (91 del 2.1 + 67 del 2.2), 0 omitidas. Si falla una del 2.1 por la rama nueva (cuenta los archivos bajo `output_root` o arma un job de Dataflow sin `--project`), se le agrega `--enriched_enabled=false` a esa prueba (ver `ENRICHED.md` §1).

**B3. Contratos sin cambios:**

```bash
python -m pytest -c project/pytest.ini project/contracts/tests -q    # 18 passed
```

**B4. `common/` y simulador** (los tres módulos nuevos de `common/` no deben romper nada; usar el entorno que tenga Faker, el del Lote 1.2):

```bash
python -m pytest -c project/pytest.ini project/src/common project/src/simulator -q
```

Esperado: los mismos resultados que antes del lote (134 passed en v2.15), 0 omitidas.

**Qué hacer si falla una de estas tres** (son las que dependen del código real que no tuve al prepararlo):

| Prueba | Significa |
|---|---|
| `test_hash_es_hmac_sha256_del_user_id` | `common/hashing.py` no calcula HMAC-SHA256 puro del `user_id`. Mandarme ese archivo. |
| `test_ddl_bigquery_posts_enriched_coincide` / `test_ddl_athena_posts_raw_tiene_las_mismas_columnas` | La DDL de `contracts/` difiere de §5.5. El mensaje dice qué columna. Mandarme el `.sql`. |
| `test_envelopes_validos_enriquecidos_cumplen_el_schema_enriquecido` | El envelope enriquecido no cumple `envelope.enriched.v1`. El mensaje dice el campo. |

---

## Parte C · Terraform sin credenciales

```bash
bash project/infra/validate.sh
```

Esperado: `fmt` y `validate` en verde en las cuatro raíces. Si `fmt -check` marca alguno de los archivos nuevos:

```bash
terraform fmt -recursive project/infra && bash project/infra/validate.sh
```

---

## Parte D · Entorno local (sin nube)

**D1. Preflight:**

```bash
(cd project && bash scripts/local_dataflow.sh preflight)
```

Esperado: Docker y puertos OK. No hace falta ninguna credencial de GCP.

**D2. Levantar** (emulador, simulador y `dataflow-local`; el código está montado, no hay que reconstruir imágenes):

```bash
(cd project && bash scripts/local_dataflow.sh up)
(cd project && bash scripts/local_dataflow.sh logs) | grep -m3 -E "enriched_config|hmac_key_loaded"
```

Esperado:
- `enriched_config` con `"bq_table": null` y `"hmac_key": "llave local de desarrollo (pública)"`.
- `hmac_key_loaded`.

Salir de los logs con Ctrl+C si se quedan abiertos.

**D3. Esperar unos 3 minutos** (ventanas de 60 s más el cierre de la marca de agua) y comprobar que aparecen archivos:

```bash
find project/.local-lake/enriched -name 'part-*.parquet' | head
```

Esperado: archivos en `source={reddit,facebook,instagram}/dt=…/hr=…/`.

**D4. Verificador del lote:**

```bash
mkdir -p project/evidencia
python project/scripts/verify_enriched.py report --local --hours 1 \
  --json-out project/evidencia/local_enriched_report.json
echo "exit=$?"
```

Esperado: `Veredicto: OK` y `exit=0`. Revisar que aparezcan en `OK`:
- por fuente: filas recientes, columnas, sin identidad, reglas por fila, copias de `event_id` (0) y cobertura `raw → enriched` (≥ 95 %);
- global: `user_hash determinista (1 a 1)`.

**D5. Inspección manual de un Parquet** (sin identidad y con la normalización de §5.2):

```bash
python - <<'EOF'
import glob, json, sys
sys.path.insert(0, "project/src")
from common.parquet_rows import parquet_bytes_to_rows
for src in ("reddit", "facebook", "instagram"):
    f = sorted(glob.glob(f"project/.local-lake/enriched/source={src}/**/*.parquet", recursive=True))[-1]
    r = parquet_bytes_to_rows(open(f, "rb").read())[0]
    assert "user_id" not in r and "handle" not in r
    assert "user_id" not in json.loads(r["payload"]) and "handle" not in json.loads(r["payload"])
    print(src, r["user_hash"][:12], r["likes"], r["shares"], r["comments"], r["brand_primary"], r["sentiment_label"])
EOF
```

Esperado: una línea por fuente. `shares` es `None` en reddit e instagram y un entero en facebook. `sentiment_label` es `None` (el NLP llega en el Lote 2.3).

**D6. Regresión del Lote 2.1 en local:**

```bash
(cd project && bash scripts/local_dataflow.sh check)
```

Esperado: **OK** (13/13), igual que en v2.21.

**D7. Apagar lo local:**

```bash
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
(cd project && bash scripts/dataflow_job.sh preflight)
gcloud dataflow jobs list --region "$GCP_REGION" --status=active --format='value(name,id)'
```

Esperado:
- ningún job activo; si queda uno del 2.1, `(cd project && bash scripts/dataflow_job.sh drain)` y esperar a que termine;
- simulador apagado (`simulator_running = false`);
- `dataflow_job.sh preflight` OK.

**E2. Plan de Terraform:**

```bash
terraform -chdir=project/infra/gcp/environments/dev plan -out=tfplan
```

Esperado: **`Plan: 5 to add, 0 to change, 0 to destroy`**, con exactamente:

- `module.bigquery_stream.google_bigquery_dataset.this`
- `module.bigquery_stream.google_bigquery_table.posts_enriched`
- `module.bigquery_stream.google_bigquery_dataset_iam_member.writers["serviceAccount:sa-dataflow@…"]`
- `module.hmac_secret.google_secret_manager_secret.this`
- `module.hmac_secret.google_secret_manager_secret_iam_member.accessors["serviceAccount:sa-dataflow@…"]`

y los outputs nuevos `bq_dataset`, `posts_enriched_table` y `hmac_secret_id`. **Si cambia o destruye algo del Lote 1.3, no aplicar.**

**E3. Aplicar y revisar outputs:**

```bash
terraform -chdir=project/infra/gcp/environments/dev apply tfplan
terraform -chdir=project/infra/gcp/environments/dev output bq_dataset           # "ta_stream"
terraform -chdir=project/infra/gcp/environments/dev output posts_enriched_table # "project-0568…:ta_stream.posts_enriched"
terraform -chdir=project/infra/gcp/environments/dev output hmac_secret_id       # "ta-hmac-key"
```

**E4. Tabla en BigQuery** (partición, clustering y expiración):

```bash
bq show --format=prettyjson "$GCP_PROJECT:ta_stream.posts_enriched" \
  | python -c "import json,sys; t=json.load(sys.stdin); print(t['timePartitioning'], t.get('clustering'), len(t['schema']['fields']))"
```

Esperado: `{'type': 'DAY', 'field': 'event_time', 'expirationMs': '7776000000'} {'fields': ['source', 'brand_primary']} 24`.

**E5. Llave HMAC:**

```bash
bash project/scripts/hmac_key.sh status   # antes: "Versiones habilitadas: 0" → pide init
bash project/scripts/hmac_key.sh init     # crea la versión 1 sin mostrarla
bash project/scripts/hmac_key.sh status
```

Esperado tras `init`:
- `Versiones habilitadas: 1`;
- único lector: `serviceAccount:sa-dataflow@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com`.

Repetir `init` debe **negarse**.

**E6. Permisos de `sa-dataflow` en el dataset:**

```bash
bq show --format=prettyjson "$GCP_PROJECT:ta_stream" | grep -B1 -A1 sa-dataflow
```

Esperado: una entrada para `sa-dataflow` con `"role": "WRITER"` o `"roles/bigquery.dataEditor"` (es el mismo permiso). Los permisos de IAM pueden tardar unos minutos en propagarse: esperar unos 3 minutos antes de E7.

**E7. Lanzar el job con el código nuevo:**

```bash
(cd project && bash scripts/dataflow_job.sh start)
(cd project && bash scripts/dataflow_job.sh status)     # Running en ~2 min
```

**E8. Logs del worker** (llave leída y sin errores). `enriched_config` no sale aquí: se escribe en la terminal al lanzar el job (E7), con `bq_table` = `project-0568d9ba-848f-413c-88b:ta_stream.posts_enriched`. `hmac_key_loaded` aparece cuando llega el primer lote de datos, así que el simulador debe estar encendido (E9):

```bash
gcloud logging read 'resource.type="dataflow_step" AND "hmac_key_loaded"' --freshness=30m --limit=3 --format='value(timestamp)'
gcloud logging read 'resource.type="dataflow_step" AND ("enrich_error" OR "bq_insert_failed")' --freshness=30m --limit=5
```

Esperado:
- al menos un `hmac_key_loaded` (el log trae la ruta del secreto, **nunca** el valor);
- la tercera consulta vacía.

Si aparece `enrich_error` con `elemento no reconocido`, mandarme ese mensaje (trae el tipo y los atributos de `ValidEvent`).

**E9. Encender el simulador:** poner `simulator_running = true` en `project/infra/gcp/environments/dev/terraform.tfvars` y aplicar:

```bash
terraform -chdir=project/infra/gcp/environments/dev apply
```

Esperado: solo se crea el servicio de Cloud Run, como en el Lote 1.3. Esperar **unos 10 minutos** (ventanas de 300 s más el cierre).

**E10. Verificador del lote (lake en GCS + BigQuery):**

```bash
python project/scripts/verify_enriched.py report --hours 1 \
  --json-out project/evidencia/enriched_report.json
echo "exit=$?"
```

Esperado: `Veredicto: OK`, `exit=0`. Además de lo de D4:
- `BigQuery: columnas`, `BigQuery: sin identidad`, `BigQuery: filas recientes` y `BigQuery: reglas` en `OK`;
- `cobertura enriched → BigQuery` ≥ 95 %.

Un `AVISO` por pocas copias en BigQuery es aceptable: la deduplicación de los *streaming inserts* es de mejor esfuerzo y Trusted vuelve a deduplicar.

**E11. Consultas de comprobación en BigQuery:**

```bash
bq query --use_legacy_sql=false '
SELECT source,
       COUNT(*) AS filas,
       COUNT(DISTINCT event_id) AS ids,
       COUNTIF(NOT REGEXP_CONTAINS(user_hash, r"^[0-9a-f]{64}$")) AS hash_malo,
       COUNTIF(shares IS NOT NULL) AS con_shares,
       COUNTIF(sentiment_label IS NOT NULL) AS con_nlp,
       COUNTIF(label_sentiment IS NOT NULL) AS con_ground_truth
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY source ORDER BY source'
```

Esperado:
- `filas ≈ ids`;
- `hash_malo = 0`;
- `con_shares` = 0 en reddit e instagram e igual a `filas` en facebook;
- `con_nlp = 0`;
- `con_ground_truth` ≈ 70 % de `filas` (el 30 % restante son `nostalgia`/`adiccion`).

```bash
bq query --use_legacy_sql=false '
SELECT column_name FROM `project-0568d9ba-848f-413c-88b.ta_stream.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = "posts_enriched" AND column_name IN ("user_id", "handle")'
```

Esperado: **ninguna fila**.

```bash
bq query --use_legacy_sql=false '
SELECT COUNT(*) AS payload_con_identidad
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND (JSON_QUERY(payload, "$.user_id") IS NOT NULL OR JSON_QUERY(payload, "$.handle") IS NOT NULL)'
```

Esperado: `0`.

**E12. Latencia de punta a punta** (dato para el Lote 2.4, no bloquea):

```bash
bq query --use_legacy_sql=false '
SELECT APPROX_QUANTILES(TIMESTAMP_DIFF(process_time, event_time, SECOND), 100)[OFFSET(95)] AS p95_s
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)'
```

Esperado: del orden de segundos. El objetivo de HU-023, menor de 300 s, se exige en el Lote 2.4.

**E13. Regresión del Lote 2.1 en GCP:**

```bash
(cd project && python scripts/verify_streaming.py check --allow-cloud \
  --json-out evidencia/streaming_check_2_2.json)
```

Esperado: **OK** (13/13): el centinela aparece una vez en `raw/` y los cinco defectos en `dlq/` y `ta-dlq`. El centinela también debe estar en `enriched/` y en BigQuery:

```bash
python - <<'EOF'
import json
ev = json.load(open("project/evidencia/streaming_check_2_2.json"))
print(json.dumps(ev, ensure_ascii=False)[:400])   # buscar el event_id del centinela
EOF
# con ese event_id:
bq query --use_legacy_sql=false 'SELECT event_id, source, user_hash, process_time FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched` WHERE event_id = "PEGAR_AQUI" AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)'
```

Esperado: una fila.

**E14. Métricas del job** (en la consola de Dataflow, pestaña *Job metrics* → *Custom counters*; también `(cd project && bash scripts/dataflow_job.sh status)` tras unos minutos):

| Contador (`enriched/…`) | Esperado |
|---|---|
| `enriched_rows` | crece ~1,5 por segundo |
| `enrich_errors` | 0 |
| `parquet_files` | ~3 cada 5 min (uno por fuente) |
| `bq_failed_rows` | 0 |

---

## Parte F · Apagado

```bash
(cd project && bash scripts/dataflow_job.sh drain)
```

Luego poner `simulator_running = false` en `terraform.tfvars` y aplicar:

```bash
terraform -chdir=project/infra/gcp/environments/dev apply      # solo destruye el servicio de Cloud Run
```

Comprobar que nada sigue corriendo:

```bash
gcloud dataflow jobs list --region "$GCP_REGION" --status=active         # vacío
gcloud run services list --region "$GCP_REGION" --format='value(name)'   # sin ta-stream-simulator
gcloud pubsub subscriptions list --format='value(name)' | grep ta-verify # vacío (temporales del verificador)
```

El dataset, la tabla y el secreto quedan creados. Su costo es despreciable: MB de almacenamiento y centavos al mes por el secreto. **No** se destruyen: el secreto tiene `prevent_destroy` y la tabla `deletion_protection`.

---

## Parte G · Cierre

**Criterios de aceptación ↔ evidencia:**

| HU | Criterio | Dónde se comprueba |
|---|---|---|
| HU-020 | `user_id` y `handle` → `user_hash` (HMAC-SHA256 hex de 64) con la llave de `ta-hmac-key` | E5, E8, E11 (`hash_malo = 0`), D4/E10 |
| HU-020 | Ningún evento enriquecido contiene `user_id` ni `handle` | D5, E11 (columnas y `payload`), D4/E10 (`sin identidad en claro`) |
| HU-020 | El mismo usuario produce siempre el mismo hash | D4/E10 (`user_hash determinista (1 a 1)`), B1 (`test_hash_determinista_…`) |
| HU-020 | Test automático de ausencia de campos en claro | B1 (`test_enriched_row.py`, `test_enriched_pipeline.py`) |
| HU-021 | `likes`, `shares`, `comments` según §5.2 (`shares = null` en Reddit e Instagram) | D5, E11 (`con_shares`), B1 (`test_normalizacion_de_engagement`) |
| HU-021 | Escritura en `posts_enriched` y Parquet en `enriched/source=…/dt=…/hr=…` | D3, E4, E10, E11, E13 |
| HU-021 | `label_sentiment` solo como columna de evaluación y `brand_primary` calculado | E11 (`con_ground_truth`), D4/E10 (reglas por fila) |

**Evidencia que queda en `project/evidencia/`:**
- `local_enriched_report.json`
- `enriched_report.json`
- `streaming_check_2_2.json`

Si todo pasó, mandarme:
- el conteo de B1 y B2;
- el veredicto y las estadísticas por fuente de los dos `report` (`stats` del JSON);
- la salida de E2 (la línea `Plan:`) y de E11;
- cualquier `AVISO`.

Con eso cierro el lote en `PLAN.md` §3 (2.2 → ✅), `HU.md` (HU-020 y HU-021 → ✅) y `CHANGELOG.md` (v2.23).

---

## Problemas frecuentes

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `test_enriched_hook` falla | Falta el enganche en `pipeline.py` | Parte A3 |
| `argparse.ArgumentError: conflicting option string` al importar | Otra clase de opciones del 2.1 ya define un nombre igual (`--enriched_*`, `--posts_enriched_table`, `--hmac_*`) | Mandarme el nombre: renombro el del 2.2 |
| D3 sin archivos tras 5 min | La rama no está enganchada o `enriched_enabled=false` | Revisar A3 y el log `enriched_config` |
| `enrich_errors` > 0 con `elemento no reconocido` | `ValidEvent` guarda el envelope con otro nombre | Mandarme el log (trae los atributos) |
| `HmacKeyError … HTTP 403` en los logs del worker | IAM aún no propagado o falta el permiso | Esperar 3 min, revisar E5; relanzar el job |
| `HmacKeyError … HTTP 404` | El secreto no tiene versión | `bash project/scripts/hmac_key.sh init` |
| `bq_insert_failed` con *no such field* o tipo inválido | La tabla real difiere del código | Mandarme el error y la salida de `bq show` |
| `bq_insert_failed` con *Access Denied* | Falta `bigquery.dataEditor` de `sa-dataflow` | Revisar E6 |
| Cobertura `raw → enriched` entre 90 % y 95 % (AVISO) | Ventanas recién cerradas en el borde | Repetir el `report` a los 5 min o con `--settle-seconds 900` |
| `verify_enriched.py` termina con `exit=2` | Sin credenciales, bucket o tabla inexistente | `gcloud auth application-default login`; revisar E3 |
