# Rama enriched del pipeline (Lote 2.2 · HU-020, HU-021)

Anonimiza cada evento válido y único del Lote 2.1, normaliza su engagement y lo escribe en
`enriched/` (Parquet) y en BigQuery `ta_stream.posts_enriched`. Contrato en
`docs/ARCHITECTURE.md` §3.14, §5.4, §5.5 y §5.8. Rutas relativas a `project/`.

```
unique (Lote 2.1) ─► EnrichAnonymize ─► [NLP, Lote 2.3] ─┬─► enriched/source=…/dt=…/hr=…/part-*.parquet
                                                          └─► ta_stream.posts_enriched (streaming inserts)
```

---

## 1. Integración en `pipeline.py` (una vez)

Dos líneas, sin tocar nada más del Lote 2.1:

```python
# arriba, con los demás imports (registra las opciones --enriched_*, --hmac_*, --posts_enriched_table)
from streaming.enriched import attach_enriched

# dentro de build_from_messages(), justo después de crear la PCollection de válidos y únicos
unique = checked.valid | "Dedup" >> DedupByEventId(cfg.dedup_window_seconds)   # como ya está
attach_enriched(unique)             # NUEVO: lee las opciones del propio pipeline
```

`test_enriched_hook.py` falla mientras falten. Si `attach_enriched` no reconoce el tipo de
elemento de `unique`, el contador `enriched/enrich_errors` sube y el log dice
`elemento no reconocido: <tipo> (atributos: …)`; con ese mensaje se ajusta `envelope_of()`.

`dataflow_job.sh` **no cambia**: todas las opciones nuevas tienen valor por defecto.

Con el enganche, las pruebas del Lote 2.1 que corren el pipeline completo también ejecutan esta
rama. Si sus opciones no traen `--output_root` ni `--project`, la rama se apaga sola fuera de
Dataflow (log `enriched_disabled`); si los traen, escribe `enriched/` en su lake temporal con la
llave local. Si alguna prueba se rompe por eso —por ejemplo, porque cuenta todos los archivos bajo
`output_root`—, basta con pasarle `--enriched_enabled=false`.

| Opción | Por defecto |
|---|---|
| `--enriched_enabled` | `true` |
| `--enriched_root` | la raíz de `raw/` (`--output_root`) o `gs://{project}-ta-lake` |
| `--enriched_window_seconds` | la de `raw/` (300 en Dataflow, 60 en local) |
| `--posts_enriched_table` | `auto`: `{project}:ta_stream.posts_enriched` con `DataflowRunner`; apagado en local. `none` lo apaga |
| `--hmac_secret_name` / `--hmac_secret_project` | `TA_HMAC_SECRET_NAME` o `ta-hmac-key` / proyecto del job |
| `--hmac_key_env` | vacío (solo local o pruebas: nombre de una variable con la llave) |

**Llave HMAC.** Con `DataflowRunner`, o si la salida es `gs://…` o BigQuery, sale de Secret
Manager y la lee cada worker en `setup()` con `sa-dataflow`: nunca viaja como opción ni queda en
el grafo del job. En local (DirectRunner con lake local) se usa una llave **pública** de
desarrollo (`common.hmac_key.LOCAL_DEV_KEY`), válida solo para datos sintéticos.

---

## 2. Pruebas (sin nube)

```bash
# desde project/, en el entorno del Lote 2.1 (~/.venvs/ta-streaming, Python 3.11)
python -m pytest src/streaming -q          # 2.1 + 2.2; con contracts/ no debe omitirse ninguna del 2.2
python -m pytest contracts/tests -q        # 18, sin cambios
```

Las del Lote 2.2 son `src/streaming/tests/test_enriched_*.py` (67 con `contracts/` y
`pipeline.py` presentes). `test_enriched_contract.py` compara la DDL real de
`contracts/bigquery/posts_enriched.sql` y `contracts/athena/posts_raw.sql` con las columnas del
código, y valida los ejemplos de `contracts/examples/valid/` contra `envelope.enriched.v1`.

---

## 3. Local (DirectRunner + emulador)

**Preflight**

```bash
bash scripts/local_dataflow.sh preflight     # Docker, puertos, archivos (Lote 2.1)
```

No hace falta nada de GCP: ni credenciales, ni BigQuery, ni el secreto.

**Correr y verificar**

```bash
bash scripts/local_dataflow.sh up            # emulador + simulador + dataflow-local (código montado: no hay que reconstruir)
# esperar ~3 min (ventanas de 60 s y el cierre de la marca de agua)
python scripts/verify_enriched.py report --local --hours 1 --json-out evidencia/local_enriched_report.json
bash scripts/local_dataflow.sh check         # regresión del Lote 2.1: debe seguir OK
```

Salida esperada de `verify_enriched.py`: `OK` en filas recientes, columnas, sin identidad,
reglas por fila, copias de `event_id` (0), cobertura `raw → enriched` (≥ 95 %) y
`user_hash determinista (1 a 1)`. Los Parquet quedan en `.local-lake/enriched/`.

**Apagado**

```bash
bash scripts/local_dataflow.sh down          # o down-all para apagar también el emulador y el simulador
```

---

## 4. GCP (Dataflow)

**Preflight** (solo lectura)

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b
bash scripts/preflight.sh                    # qué está encendido
bash scripts/dataflow_job.sh preflight       # entorno de lanzamiento (Python 3.11, setuptools, sa-dataflow)
gcloud dataflow jobs list --region us-central1 --status=active --format='value(name,id)'
```

Debe estar: el job del Lote 2.1 **apagado** (hay que relanzarlo para que tome el código nuevo) y
el simulador apagado hasta el paso 4.

**1. Terraform** (crea dataset, tabla, secreto y dos permisos de `sa-dataflow`)

```bash
bash infra/validate.sh
terraform -chdir=infra/gcp/environments/dev plan -out=tfplan
```

`plan` esperado: **5 a crear, 0 a cambiar, 0 a destruir**:
`module.bigquery_stream.google_bigquery_dataset.this`,
`module.bigquery_stream.google_bigquery_table.posts_enriched`,
`module.bigquery_stream.google_bigquery_dataset_iam_member.writers["serviceAccount:sa-dataflow@…"]`,
`module.hmac_secret.google_secret_manager_secret.this` y
`module.hmac_secret.google_secret_manager_secret_iam_member.accessors["serviceAccount:sa-dataflow@…"]`,
más los outputs `bq_dataset`, `posts_enriched_table` y `hmac_secret_id`. Si propone cambiar o
destruir algo del Lote 1.3, **no aplicar** y revisar.

```bash
terraform -chdir=infra/gcp/environments/dev apply tfplan
```

**2. Llave HMAC** (el valor no pasa por Terraform ni por el repo)

```bash
bash scripts/hmac_key.sh init      # crea la versión 1 (se niega si ya hay una)
bash scripts/hmac_key.sh status    # 1 versión habilitada; lector: serviceAccount:sa-dataflow@…
```

Los permisos de IAM pueden tardar unos minutos en propagarse.

**3. Job de Dataflow**

```bash
bash scripts/dataflow_job.sh start     # mismo comando del Lote 2.1
bash scripts/dataflow_job.sh status    # Running en ~2 min
```

En los logs del worker debe aparecer `hmac_key_loaded` (con la ruta del secreto, nunca el valor)
y `enriched_config` con `bq_table = project-0568d9ba-848f-413c-88b:ta_stream.posts_enriched`.

**4. Tráfico y verificación**

```bash
# encender el simulador: simulator_running = true en terraform.tfvars y apply (Lote 1.3)
# esperar ~10 min (ventanas de 300 s)
python scripts/verify_enriched.py report --hours 1 --json-out evidencia/enriched_report.json
python scripts/verify_streaming.py check --allow-cloud --json-out evidencia/streaming_check_2_2.json   # regresión 2.1
bq query --use_legacy_sql=false \
  'SELECT source, COUNT(*) n, COUNT(DISTINCT event_id) ids, COUNTIF(shares IS NOT NULL) con_shares
   FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
   WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) GROUP BY source'
```

`verify_enriched.py` con el lake en `gs://` revisa también BigQuery (columnas, ids únicos,
reglas y cobertura `enriched → BigQuery`). `con_shares` debe ser 0 en Reddit e Instagram.

**Apagado**

```bash
bash scripts/dataflow_job.sh drain                    # cierra las ventanas abiertas (~0,25 USD/h mientras corre)
# simulator_running = false en terraform.tfvars y apply
gcloud dataflow jobs list --region us-central1 --status=active   # vacío
```

Lo que queda creado no cobra de forma apreciable: BigQuery cobra por almacenamiento (MB) y las
particiones expiran a los 90 días; el secreto cuesta centavos al mes. Los *streaming inserts*
cuestan 0,01 USD por 200 MB (unos 130.000 eventos al día son pocos MB).

---

## 5. Notas

- **No rotar la llave.** Cambiarla cambia todos los `user_hash` y rompe el cruce con el
  histórico. `hmac_key.sh` no tiene comando de rotación a propósito; el secreto tiene
  `prevent_destroy`.
- **NLP.** Las columnas `sentiment_*`, `emotion_*` y `nlp_model_version` van en nulo hasta el
  Lote 2.3, que se engancha con `attach_enriched(unique, nlp=<PTransform>)`.
- **Duplicados en BigQuery.** Los *streaming inserts* deduplican por mejor esfuerzo; `Trusted`
  vuelve a deduplicar por `event_id` (§5.7). El verificador avisa si hay copias y falla si
  superan el 1 %.
- **Filas rechazadas por BigQuery** (schema, tipos): contador `enriched/bq_failed_rows` y log
  `bq_insert_failed` con el `event_id`. El Parquet de `enriched/` se escribe igual.
