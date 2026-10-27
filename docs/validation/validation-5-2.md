# Validación del Lote 5.2 · HU-032, HU-033

> Calidad de datos con **Great Expectations 1.24.1** (decisión del usuario: GE de verdad, sin dbt, a demanda, sobre
> Raw, Trusted y Curated; ADR-28). El CLI `project/scripts/run_dq.py` (paquete `project/src/dq/`) corre en su propio
> venv (`~/.venvs/ta-gx`) sobre la copia local del lake, valida las 63 reglas de §5.10 y las 14 de Curated de §3.24.1,
> mide los KPI de HU-032 y la precisión del NLP (HU-033), y publica en `s3://…/quality/` y en BigQuery `dq_results`.
> Contrato en `docs/ARCHITECTURE.md` §3.24 (interfaces en §3.24.1), §5.5 y §5.10.
>
> **Automático:** `python project/scripts/validate_5_2.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_5_2.json`.
>
> **No cambia infraestructura ni el horario del lakehouse** (sigue encendido): solo lee S3 (sync) y, en la parte P,
> escribe en `quality/` y en `dq_results`.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-032 · Suites de Great Expectations según §5.10 (sin dbt) | L1 (239 comprobaciones), B1 (12 pruebas con GX) |
| HU-032 · Completitud ≥ 95 %, unicidad ≥ 99 %, duplicados < 1 % (en Trusted, tras deduplicar), anonimización 100 % | L2 / P2 (KPI por capa y origen) |
| HU-032 · Resultados en BigQuery `dq_results` y en `s3://…/quality/` | P4, P5 |
| HU-032 · Un test falla si existen columnas `handle` o `user_id` | B3 (`test_dq_identity.py`, sin GX), DQ-02 y DQ-65 en L1 |
| HU-033 · Accuracy, precisión y recall por clase (matriz de confusión) sobre las filas con `label_sentiment` | L5 (`evidencia/precision_nlp_5_2.md`), fila `NLP[accuracy]` en P5 |
| HU-033 · De la emoción solo la distribución; la limitación declarada | L5 (informe) |
| Las salidas no llevan datos de filas (`user_hash`, `event_id`, textos) | L4 |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | `sync` (lee S3) y la publicación en S3 |
| ADC de GCP | `~/.config/gcloud/application_default_credentials.json` (`gcloud auth application-default login`) | Insertar y consultar `dq_results` |
| Proyecto de GCP | `project_id` de `project/infra/gcp/environments/dev/terraform.tfvars` (o `--gcp-project`) | `{proyecto}.ta_stream.dq_results` |
| `TA_QUALITY_CACHE` | (opcional) por defecto `~/.cache/trendanalyzer/quality` | La copia local del lake |
| `--gx-python` | (opcional) por defecto `~/.venvs/ta-gx/bin/python` | El venv de Great Expectations |

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
3. El venv de Great Expectations (la primera vez, ~1 min; después no cambia nada):
   ```bash
   python project/scripts/validate_5_2.py venv
   ```
   Esperado: `V1 OK`, `V2 OK uv pip install …`, `V3 OK 1.24.1 (esperado 1.24.1)`.
4. La copia local del lake (solo lee S3; la primera vez baja Curated, ~190 objetos):
   ```bash
   python project/scripts/validate_5_2.py sync
   ```
   Esperado: `S1 OK raw: 13257 objetos, 0 bajados · trusted: 543 objetos, 0 bajados · curated: 189 objetos, …` (189 la
   primera vez bajados) y `S2 OK` (0 bajados).

### Ejecución

```bash
python project/scripts/validate_5_2.py preflight
python project/scripts/validate_5_2.py tests
python project/scripts/validate_5_2.py local            # sin nube, ~1,5 min y ~2,5 GB de memoria
python project/scripts/validate_5_2.py publish --yes    # escribe en quality/ y en dq_results
python project/scripts/validate_5_2.py status
```

Salida esperada (resumen): cada parte termina con `-- Parte X: OK` y código 0, salvo `preflight` con A7/A8 en AVISO si
faltan las credenciales de GCP o algo cobra. Con los datos del 27-oct-2026: L1 `239 comprobaciones, 238 OK · errores 0 ·
avisos 1`; L3 `1 avisos: DQ-63[origin=stream] ta_raw.posts_raw`; L5 `109365 filas evaluadas · accuracy 0.9197`; P4 y P5
con las mismas 239 filas.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, V, B, L | Nada en la nube (V baja paquetes de PyPI) | 0 |
| S | Perfil de AWS (solo lee S3) | ~13.990 LIST/GET la primera vez de cada capa (≈ 0,005 USD); después solo LIST |
| P | Perfil de AWS (escribe en `quality/`) y ADC de GCP (inserta en `dq_results`) | ~22 PUT (≈ 0,0001 USD) + streaming insert de ~0,1 MB y una consulta de < 10 MB en BigQuery (centavos) |

**Estado final:** nada nuevo encendido. Queda `{caché}/dq_runs/{run_id}/` en local, `quality/{día}/ge/{run_id}/` en S3 y
~240 filas por corrida en `dq_results` (sin expiración).

---

## Parte A · Preflight

```bash
python project/scripts/validate_5_2.py preflight
```

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 (ta-streaming) |
| A2 | Los 15 archivos del lote presentes (`src/dq/*`, `scripts/run_dq.py`, validador, pruebas y script) |
| A3 | Los 14 archivos de lotes anteriores que usa (`quality.rules`, `quality.checks`, `quality.s3sync`, `common.stream_tables`, `catalog_spec`, los dos jobs, `validate41/42/32`, `services`) |
| A4 | `great_expectations 1.24.1, pandas 3.1.0, pyarrow 25.0.1` en el venv de GX (FALLA si no existe: parte V) |
| A5 | La caché con `raw`, `trusted` y `curated` (AVISO si falta alguna: parte S) |
| A6 | `aws`, `uv` |
| A7 | Proyecto de GCP y ADC presentes (AVISO si faltan: solo los necesita P) |
| A8 | Lo que cobra (`services.status`) |

## Parte V · Venv de Great Expectations

| Paso | Esperado |
|---|---|
| V1 | `uv venv --python 3.11 ~/.venvs/ta-gx` (o «ya existe») |
| V2 | `uv pip install -r project/src/dq/requirements.txt` |
| V3 | `1.24.1` |

Manual: `uv venv --python 3.11 ~/.venvs/ta-gx && uv pip install --python ~/.venvs/ta-gx/bin/python -r project/src/dq/requirements.txt`.

## Parte B · Pruebas

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `~/.venvs/ta-gx/bin/python -m pytest src/dq -q` | **13 passed** (10 con GX: catálogo, §3.24.1 = código, lake sintético con los jobs reales, corrida limpia, defectos, privacidad, NLP, recálculos, publicación, `sync` sin «carpetas», CLI; 3 de identidad) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate52.py -q` | **20 passed** |
| B3 | `python -m pytest src/dq -q` (ta-streaming) | **3 passed** (las de GX no se recogen ahí: `src/dq/conftest.py`) |
| B4 | `python -m pytest contracts/tests -q` | 18 passed |
| B5 | `python -m pytest src/quality -q` | **165 passed** (regresión: el catálogo y las comprobaciones del 5.1) |

## Parte S · Copia local del lake

| Paso | Esperado |
|---|---|
| S1 | `run_dq.py sync`: las tres capas con `failed = 0` |
| S2 | Un segundo `sync` no baja nada (AVISO si baja: algo cambió en S3 entre los dos, p. ej. una corrida del horario con Trusted distinto) |

Manual: `~/.venvs/ta-gx/bin/python project/scripts/run_dq.py sync`.

## Parte L · Suites sobre la copia local (sin publicar)

```bash
python project/scripts/validate_5_2.py local
```

| Paso | Esperado (datos del 27-oct-2026) |
|---|---|
| L1 | `239 comprobaciones, 238 OK · errores 0 · avisos 1 · ~80 s · GX 1.24.1 · KPI: …`. FALLA si alguna regla de gravedad `error` falla |
| L2 | KPI de HU-032 todos OK: Raw, Trusted y Curated (completitud 1,0; unicidad 0,99999–1,0; duplicados 1,2 × 10⁻⁵ en Raw stream y 0 en Trusted; anonimización 1,0) |
| L3 | `1 avisos: DQ-63[origin=stream] ta_raw.posts_raw` (los 6 centinelas: esperado). AVISO si aparece otro (umbral de una regla `warning` con datos nuevos) |
| L4 | Las salidas sin hex de 64 ni UUID |
| L5 | `109365 filas evaluadas · accuracy 0.9197 · F1 macro 0.9167`; copia `nlp_precision.md` a `project/evidencia/precision_nlp_5_2.md` |
| L6 | `221637 filas de Raw, 221629 de Trusted, 221629 de fact_posts` (AVISO si cambian) |

Manual: `~/.venvs/ta-gx/bin/python project/scripts/run_dq.py run` (la última línea, `dq_done`, trae el resumen; los
archivos quedan en `~/.cache/trendanalyzer/quality/dq_runs/{run_id}/`).

## Parte P · Corrida publicada (S3 y BigQuery)

```bash
python project/scripts/validate_5_2.py publish    # pide confirmación (--yes la omite)
```

| Paso | Esperado |
|---|---|
| P1–P3 | Como L1–L3 |
| P4 | `s3://ta-dev-648617469176-lake/quality/{día}/ge/{run_id}/: 16 objetos (17 subidos con el resumen de _latest) · quality/_latest/ge/summary.json → {run_id}` (12 resultados de GX: Raw y Trusted × 2 orígenes × filas/métricas, y 4 de Curated; `dq_results.json`, `summary.json`, `nlp_precision.{json,md}`) |
| P5 | `insertadas 239 (errores 0); consultadas 239: curated 22 (22 OK, 5 con KPI); raw 109 (108 OK, 31 con KPI); trusted 108 (108 OK, 34 con KPI)` |
| P6 | Costo de centavos |

Manual:

```bash
~/.venvs/ta-gx/bin/python project/scripts/run_dq.py run --publish s3,bq
aws s3 ls s3://ta-dev-648617469176-lake/quality/_latest/ge/
bq query --use_legacy_sql=false 'SELECT layer, COUNT(*) n, COUNTIF(passed) ok FROM `<proyecto>.ta_stream.dq_results` WHERE STARTS_WITH(run_id, "dq-") AND DATE(run_ts) = CURRENT_DATE() GROUP BY layer'
```

## Status

| Paso | Esperado |
|---|---|
| S1 | El último `summary.json` publicado (`run_id`, comprobaciones, errores, avisos) |
| S2 | Las últimas corridas `dq-*` en `dq_results` con sus filas |

## Cierre

**Cerrado en v2.42** (27-oct-2026, WSL; `main` = `fdc5d9b`; `ta-streaming` y `ta-gx`; perfil `default` y ADC de GCP).

| Parte | Resultado |
|---|---|
| A · Preflight | OK (8): GX 1.24.1, pandas 3.1.0, pyarrow 25.0.1; caché con las tres capas; proyecto de GCP y ADC presentes; nada cobra |
| B · Pruebas | OK (5): B1 13, B2 20, B3 3, B4 18, B5 165 |
| S · Sync | OK (2): raw 13.257, trusted 543, curated 189 objetos; 0 bajados en los dos |
| L · Local | OK (6): **239 comprobaciones, 238 OK, 0 errores** (aviso: DQ-63 en Raw stream, los 6 centinelas); KPI de HU-032 cumplidos; salidas sin `user_hash` ni `event_id`; **NLP: 109.365 filas, accuracy 0,9197, F1 macro 0,9167**; 221.637 / 221.629 / 221.629 filas |
| P · Publicación | OK (6): `dq-20261027T132041Z-e284f0`, 239 comprobaciones, 238 OK, 0 errores, 73,6 s; **16 objetos** en `quality/2026-10-27/ge/dq-20261027T132041Z-e284f0/` + `quality/_latest/ge/summary.json`; **239 filas en `ta_stream.dq_results`** sin errores (raw 109, trusted 108, curated 22); ≈ 0,0001 USD |
| `status` | OK (2): el resumen de `_latest` y la corrida en BigQuery |

Cifras también en `ARCHITECTURE.md` §3.24 y `CHANGELOG.md` v2.42. Nada quedó encendido.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| A4 FALLA «no responde» o versión distinta | `python project/scripts/validate_5_2.py venv` |
| A5 AVISO «faltan [...]» | `python project/scripts/validate_5_2.py sync` |
| L1 FALLA con `dq_error` «faltan en la caché» | Lo mismo: `sync` |
| L1 FALLA con errores | El detalle trae cada `check_name`; mire el JSON de GX de esa suite en `{caché}/dq_runs/{run_id}/ge/` (`unexpected_count`, `observed_value`). Si es de Curated (DQ-72…DQ-76), compare con `validate_4_4.py athena` |
| L3 AVISO con un aviso nuevo | Una regla `warning` dejó de cumplir con datos nuevos: revise su umbral en §5.10 (no detiene nada) |
| La corrida muere sin mensaje (memoria) | Raw entero ocupa ~2,4 GB: cierre otras cosas en WSL o corra `run_dq.py run --layers trusted,curated` |
| P5 «sin credenciales» o `403` | `gcloud auth application-default login` y repita P |
| P5 con `errors` en el insert | Páseme el detalle: el *streaming insert* rechazó filas (tipo de `details` u otra columna) |
| S2 AVISO «bajados N» | Trusted o Curated cambiaron en S3 entre los dos `sync` (el horario corre cada hora): normal si el exportador sigue en pausa y solo se reescribieron con los mismos bytes de otro tamaño; si persiste, borre la capa en la caché y repita |
