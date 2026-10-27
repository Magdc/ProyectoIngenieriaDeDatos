# Validación del Lote 4.3 · HU-027, HU-030

> Capa Curated: el job de Glue `ta-job-trusted-to-curated` (Python shell 3.9, 0,0625 DPU, sin dbt) lee
> `trusted/posts/` y construye `fact_posts` (por `dt`), `dim_date`, `dim_source`, `dim_brand`, `dim_user`,
> `agg_emotion_by_brand_hour` y `agg_trend_daily` en `curated/`, concilia antes de escribir, recalcula todo y
> sube solo lo que cambió (idempotente), con un reporte por corrida en `quality/`. Más cinco consultas guardadas
> sobre Curated en `ta-analytics` (HU-030). Contrato en `docs/ARCHITECTURE.md` §3.22 (interfaces en §3.22.1) y §5.7.
>
> **Automático:** `python project/scripts/validate_4_3.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_4_3.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.
>
> **El horario del lakehouse queda ENCENDIDO al terminar** (decisión del usuario: es el estado normal). La guía
> lo apaga en la Preparación (paso 7) para que ninguna corrida automática caiga en medio, y el último paso de la
> Ejecución lo vuelve a encender (`on --yes`).

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-027 · Existen `fact_posts`, `dim_date`, `dim_source`, `dim_brand`, `dim_user` (solo `user_hash`) | D6–D8, E2, E5 (y L1–L3) |
| HU-027 · Existen `agg_emotion_by_brand_hour` y `agg_trend_daily` | D6–D8, E3, E4 |
| HU-027 · Construido con Glue (sin dbt, decisión del usuario), con tests | B1, B2 (pytest) y E2–E5 (SQL en Athena) |
| HU-027 · `SELECT` sobre `ta_curated.fact_posts` devuelve datos de ambos orígenes (el «Listo cuando» de la Fase 4) | E1 |
| HU-030 · Workgroup `ta-analytics` con límite de bytes escaneados | E7 |
| HU-030 · Particiones por partition projection (sin crawlers) | E1, E6 (`ta-curated-semana-vs-fin-de-semana` poda por `dt`) |
| HU-030 · Conjunto de consultas de ejemplo documentado | C2 (las cinco consultas guardadas), E6; documentadas en §3.22.1 |
| Decisión 2: filas sin NLP en `n_posts`, no en el sentimiento ni en las emociones | D6, E3, E4 (y L1) |
| Conciliación Trusted → Curated (filas de `fact_posts` = Trusted; sumas de los agregados = conteos de `fact_posts`) | D6, E2–E5 |
| Idempotencia (mismo Trusted ⇒ mismos bytes, 0 escrituras) y costo sumado al del 4.2 | F3–F5, F8 (y L2) |
| `curated/_ta_placeholder/` sin escrituras nuevas y `curated/edges_*` intactas (son del 4.4) | D7, F7 |
| Latencia frente al KPI de < 3 s (§4) | E8 (informativo: el KPI oficial es con SPICE, Lote 6.1) |
| Estado final: horario `ENABLED` (estado normal) | `on` (N1, N2), `status` (S1) |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL (los comandos se corren desde aquí) |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | Terraform, `aws` y boto3 |
| `TA_QUALITY_CACHE` | (opcional) por defecto `~/.cache/trendanalyzer/quality` | La copia local del Trusted de S3 que usa la parte L |
| Venv tipo Glue | (opcional) `/root/.cache/ta-glue39-venv/bin/python` (Python 3.9 + pyarrow 7.0.0) | `local --python …`: corre el job con las versiones de Glue |
| Bucket | `ta-dev-648617469176-lake` (o `--bucket`) | El lake |

### Preparación

1. Terminal en WSL, entrando de nuevo a la carpeta (tras copiar desde Windows puede quedar una carpeta
   «fantasma»):
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   export AWS_PROFILE=default
   aws sts get-caller-identity --query Account --output text
   ```
   Esperado: `648617469176`.
2. `git status --short` y `git log -1 --oneline`: el commit del lote en `main`, sin cambios (la evidencia
   debe salir de un commit sin `-dirty`).
3. Estado de partida (solo lee):
   ```bash
   python project/scripts/validate_4_3.py status
   python project/scripts/validate_3_2.py status
   ```
   Esperado: `S1 OK … ENABLED (estado normal …)`, `S2 OK … ninguna corrida; ningún job run`,
   `S3 OK curated/: 0 objetos … sin reporte …` (antes de la primera corrida), `S5` en AVISO si quedan marcas
   del marcador (las borra el paso 7) y el Scheduler del exportador en `PAUSED` (si sube objetos, Trusted
   cambia entre partes).
4. (Para la parte L) la copia local del Trusted de S3:
   `ls ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake/trusted/posts` muestra
   `source=facebook  source=instagram  source=reddit`. Si no existe: `python project/scripts/profile_data.py sync
   --layer trusted` (solo lee S3; ~1 min).
5. (Opcional, para correr L con las versiones de Glue)
   `/root/.cache/ta-glue39-venv/bin/python -c "import sys, pyarrow; print(sys.version.split()[0], pyarrow.__version__)"`.
   Esperado: `3.9.25 7.0.0`. Si no existe, L corre con Python 3.11 (también vale).
6. `project/infra/aws/environments/dev/backend.hcl`, `terraform.tfvars` y `gcp_exporter.auto.tfvars` presentes (desde
   el 3.1/3.2). No corra otra validación que use el estado de `infra/aws` a la vez.
7. **Apague el horario mientras valida.** Si el paso 3 mostró una corrida en curso (S2), espere a que termine
   (~3 min; repita `status`) y entonces:
   ```bash
   python project/scripts/validate_4_3.py off
   ```
   Esperado: `O1 OK deshabilitar el horario: ENABLED → DISABLED`, `O2 OK … nada en curso`, `O4 OK marcas de los
   marcadores de posición: N borradas` y `-- Parte off: OK` (O5 puede dar AVISO si algo de GCP sigue encendido).
   El último paso de la Ejecución lo vuelve a encender.

### Ejecución

```bash
python project/scripts/validate_4_3.py preflight
python project/scripts/validate_4_3.py tests
python project/scripts/validate_4_3.py local --python /root/.cache/ta-glue39-venv/bin/python   # sin nube, ~30 s
python project/scripts/validate_4_3.py terraform               # plan: 5 a crear, 2 a cambiar
python project/scripts/validate_4_3.py terraform --apply --yes # aplica el script, la descripción y las 5 consultas
python project/scripts/validate_4_3.py run --yes               # una corrida real del job (~2–4 min)
python project/scripts/validate_4_3.py athena                  # ~1–2 min, 10 consultas
python project/scripts/validate_4_3.py rerun --yes             # segunda corrida en el workflow (~5–8 min)
python project/scripts/validate_4_3.py on --yes                # estado final: horario ENCENDIDO
python project/scripts/validate_4_3.py status
```

Salida esperada (resumen; el detalle de cada paso, abajo): cada parte termina con `-- Parte X: OK (n OK)` y
código 0, salvo `terraform` sin `--apply` (AVISO: C3 «sin --apply») y C0 en AVISO si el perfil es la cuenta
raíz. Con el Trusted del 26-oct-2026: L1 y D6 `Trusted 221629 … = fact_posts 221629`, tablas `dim_date 181 ·
dim_source 3 · dim_brand 3 · dim_user 5936 · agg_emotion_by_brand_hour 39954 · agg_trend_daily 543`; E1 con
`historical: 50000` y `stream: 171629`; E3 `n_posts 221629 − con sentimiento 156366 = 65263 filas sin NLP`; F3
`archivos cambiados 0 · escritos 0 · borrados 0 · mismos sha256`; `on` `N1 OK … DISABLED → ENABLED`; `status`
`S1 OK … ENABLED (estado normal …)` y `S5 OK 0 objetos`.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada (A lee el horario y las corridas) | GCP, Docker | 0 |
| L | La copia local del Trusted de S3 | Nube | 0 |
| C | Perfil de AWS | GCP, Docker | 0 (cambia el script y la descripción del job y crea 5 consultas guardadas) |
| D | Lo de C aplicado; Trusted sin cambios durante la parte (exportador en pausa); horario apagado (paso 7) | GCP | 1 corrida del job a 0,0625 DPU (≈ 0,0005 USD) + 543 GET para contar filas y 187 objetos de Curated (≈ 0,0003 USD) |
| E | D hecho | GCP | 10 consultas de Athena (≈ 0,0005 USD; mínimo 10 MB cada una) |
| F | D hecho | GCP | 1 corrida del workflow (el job del 4.2 a 1 DPU + este; ≈ 0,02 USD) |
| `on` | — | — | El horario vuelve a cobrar ≈ 0,48 USD/día (estado normal decidido por el usuario) |

**Estado final del lote:** horario `ta-wf-raw-to-curated-cada-hora` **`ENABLED`** (cada hora corren Trusted y
Curated, ≈ 14,1 USD/mes medido en el cierre). Curated (~18 MB en S3) y los reportes de `quality/` quedan guardados (centavos
al mes).

---

## Parte A · Preflight del código y de lo encendido

```bash
python project/scripts/validate_4_3.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows: repita desde WSL. Las demás partes salen con 2 fuera de WSL |
| A1 | Python 3.11; `boto3`, `botocore`, `pyarrow` |
| A2 | Los 12 archivos del lote presentes (job, sus pruebas, las 5 consultas, `lakehouse.tf`, validador, sus pruebas y el script) |
| A3 | Los 14 archivos de lotes anteriores que usa (módulos `glue_workflow`, `athena_workgroup`, `glue_role`, `catalog_spec`, `s3_parquet`, `raw_to_trusted.py`, `contracts/athena/{curated,posts}.sql`, `brands.yaml`, `quality/s3sync.py`, `ops/{validate42,validate41,validate32,services}.py`) |
| A4 | `tables.json` al día con `contracts/athena/` |
| A5 | `terraform`, `aws`, `bash` |
| A6 | Horario `DISABLED` (lo apagó el paso 7), ninguna corrida ni job run en curso. AVISO con `ENABLED` (recuerda `off` y, al final, `on --yes`) o con algo en curso |
| A7 | `services.status` (como `status.py`): nada `ENCENDIDO` |
| A8 | La copia local del Trusted (o la salida local del 4.2) presente (AVISO si falta: solo la usa L) |

## Parte B · Pruebas

```bash
python project/scripts/validate_4_3.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/lakehouse -q -rs` | **123 passed, 0 skipped** (33 del job del 4.3, 39 del 4.2, las del 4.1 y lo común de los jobs) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate43.py -q` | **35 passed** (34 en la entrega; +1 con la corrección de E5 en v2.38) |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/ops/tests/test_ops_validate42.py -q` | **30 passed** (piezas que reutiliza; `start_job_run` con `job=`) |
| B5 | `python -m pytest src/ops/tests/test_ops_validate41.py -q` | **48 passed** |

## Parte L · El job en local contra el Trusted real (opcional, sin nube)

```bash
python project/scripts/validate_4_3.py local --python /root/.cache/ta-glue39-venv/bin/python   # o sin --python
```

Corre el job de verdad (`trusted_to_curated.py --ta_local_src … --ta_local_dst …`) sobre la copia local del
Trusted de S3 y escribe en `{caché}/trusted_to_curated_local` (fuera del repo).

| Paso | Esperado (Trusted del 26-oct-2026) |
|---|---|
| L0 | Lee `…/s3/ta-dev-648617469176-lake/trusted/posts/` (copia de S3) o, si no está, la salida local del 4.2. AVISO con el comando `sync` si no hay ninguna |
| L1 | `Trusted 221629 → fact_posts 221629 en 181 particiones · tablas {agg_emotion_by_brand_hour 39954, agg_trend_daily 543, dim_brand 3, dim_date 181, dim_source 3, dim_user 5936, fact_posts 221629} · sin NLP 65263` en ~5 s, pico ~0,41–0,43 GB con Python 3.9 + pyarrow 7 (~0,56–0,58 GB con Python 3.11). AVISO si las cifras difieren (otra foto de Trusted) |
| L2 | Segunda corrida: `escritos 0 · sin cambios 187 · archivos cambiados 0 · borrados 0` |
| L3 | Las 7 tablas con las columnas y tipos de `contracts/athena/curated.sql` (`int` → int32, `date` → date32, `boolean`, `timestamp[ms, UTC]`, `bigint` → int64, `array<string>`) |

Manual: `python project/src/lakehouse/glue_jobs/trusted_to_curated.py --lake_bucket ta-dev-648617469176-lake
--ta_local_src ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake --ta_local_dst ~/.cache/trendanalyzer/quality/trusted_to_curated_local`
(imprime tres líneas JSON; la última, `glue_job_done`).

## Parte C · Terraform (infra/aws)

```bash
python project/scripts/validate_4_3.py terraform            # validate.sh + init + plan (no aplica)
python project/scripts/validate_4_3.py terraform --apply    # aplica (pide confirmación; --yes la omite)
```

El plan va limitado con `-target=module.glue_workflow -target=module.athena`. El validador lee el plan en JSON y
**no aplica** si cambia algo distinto del script y del job de `trusted_to_curated` o crea algo distinto de las cinco
consultas guardadas, o si el job cambia algo más que la descripción.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars` (AVISO si es la cuenta raíz: no afecta) |
| C0b | `backend.hcl` y `terraform.tfvars` presentes |
| C1 | `bash infra/validate.sh` en verde |
| C2a | `terraform init` |
| C2 | **5 a crear, 2 a cambiar, 0 a destruir**: `module.athena.aws_athena_named_query.this["ta-curated-…"]` × 5, `module.glue_workflow.aws_s3_object.script["trusted_to_curated"]` (`source_hash`) y `module.glue_workflow.aws_glue_job.this["trusted_to_curated"]` (`description`); o «ya aplicado» |
| C3 | `Apply complete! Resources: 5 added, 2 changed, 0 destroyed.` |
| C3b | Un segundo plan: **No changes** |

Manual (desde `project/`):

```bash
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan -target=module.glue_workflow -target=module.athena -out=../../../../evidencia/tfplan_4_3
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_4_3
```

`evidencia/tfplan_4_3*` no se versionan (`.gitignore`).

## Parte D · Corrida real del job y conciliación Trusted → Curated

```bash
python project/scripts/validate_4_3.py run      # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| D0 | Solo aparece si el horario está `ENABLED` | AVISO: apáguelo con `off` mientras valida (la parte sigue: espera y reintenta si hay otra corrida) |
| D1 | `get-job` y baja el script de `_glue/scripts/` | `pythonshell 3.9 · 0.0625 DPU (1 GB) · library-set analytics · script de S3 idéntico al repo` (compara sin CR). FALLA «aplique C» si el script es el marcador |
| D2 | Cuenta las filas de Trusted por los pies de sus Parquet (543 GET) y lista `curated/_ta_placeholder/` y `curated/edges_*` | `trusted/posts/: 543 objetos y 221629 filas · curated/_ta_placeholder/: 0 · curated/edges_*: 0` |
| D3 | `start-job-run` (con `--ta_validacion 4.3`), espera y reintenta si Glue cuenta otra corrida | `SUCCEEDED` · segundos de ejecución · `0.0625 DPU`. Anote la duración |
| D4 | La línea `glue_job_done` en `/aws-glue/python-jobs/output` (flujo = el job run) | `leídas 221629 · fact_posts 221629 en 181 particiones · tablas {…}` · tiempos · **CPU** · **pico de memoria de 1024 MB** (AVISO si pasa de 800) · versiones de Python, pyarrow y boto3 (anótelas para el cierre) |
| D5 | Lee el reporte (`quality/{fecha}/trusted_to_curated/{corrida}.json`) y `quality/_latest/trusted_to_curated.json` | Los dos con la misma corrida, esquema `ta-trusted-to-curated-report@1` |
| D6 | Conciliación | `Trusted 221629 (pies de S3) = leídas 221629 = fact_posts 221629`; `agg_trend_daily: n_posts 221629 = filas con marca; con sentimiento 156366; sin sentimiento 65263 (= sin NLP 65263: decisión 2)`; `agg_emotion_by_brand_hour: 156366 = filas con emoción · dim_user: 221629 = filas con usuario · llaves foráneas y dim_date continua: OK`. FALLA si una comprobación del reporte no cuadra; AVISO si las cifras no son las de hoy con el mismo Trusted |
| D7 | Cuenta `curated/` por los pies, revisa el layout, las marcas y las aristas | `187 objetos (187 en el reporte; 221629 filas de fact_posts en 181 particiones)` · filas por tabla = el reporte · `dt 2026-04-27 → 2026-10-24` · `escritos 187, sin cambios 0, borrados 0` la primera vez (0 escritos si una corrida del horario ya lo había escrito) · `curated/_ta_placeholder/: 0 → 0` · `curated/edges_*: 0 → 0` |
| D8 | Baja el Parquet más chico de cada tabla y compara su esquema | `7 tablas con las columnas y tipos de contracts/athena/curated.sql` |

Manual:

```bash
aws glue start-job-run --region us-east-1 --job-name ta-job-trusted-to-curated
aws glue get-job-run --region us-east-1 --job-name ta-job-trusted-to-curated --run-id <jr_…> --query 'JobRun.[JobRunState,ExecutionTime,MaxCapacity,ErrorMessage]'
aws logs filter-log-events --region us-east-1 --log-group-name /aws-glue/python-jobs/output --log-stream-name-prefix <jr_…> --filter-pattern '"glue_job_done"'
aws s3 cp s3://ta-dev-648617469176-lake/quality/_latest/trusted_to_curated.json - | head -c 3000
aws s3 ls s3://ta-dev-648617469176-lake/curated/ --recursive | grep -v -e _ta_placeholder -e edges_ | wc -l
```

## Parte E · Athena: `ta_curated`

```bash
python project/scripts/validate_4_3.py athena
```

| Paso | Consulta | Esperado |
|---|---|---|
| E1 | `SELECT origin, COUNT(*), COUNT(DISTINCT event_id), MIN(dt), MAX(dt), count_if(cardinality(brands) > 1) FROM ta_curated.fact_posts GROUP BY origin` (el «Listo cuando» de la Fase 4) | `historical: 50000 filas, 50000 event_id … (2026-04-27 → 2026-10-23); stream: 171629 filas … (2026-10-21 → 2026-10-24) · total 221629 (reporte 221629)`; las comparativas confirman que Athena lee la lista `brands` |
| E2 | Trusted frente a `fact_posts`, `date_key`, llaves foráneas y `dim_date` | `ta_trusted.posts 221629 = fact_posts 221629 · 0 date_key ≠ dt ≠ DATE(event_time) · 0 filas sin su fecha, fuente, marca o usuario en las dimensiones · dim_date 181 días seguidos` |
| E3 | `agg_trend_daily` frente a lo recalculado desde `fact_posts` | `0 claves, conteos o promedios distintos · n_posts 221629 − con sentimiento 156366 = 65263 filas sin NLP · 6 filas (día × marca) con n_posts ≠ n_negative + n_neutral + n_positive` |
| E4 | `agg_emotion_by_brand_hour` frente a lo recalculado | `0 claves o conteos distintos · n_posts 156366 + 65263 filas sin emoción = 221629` |
| E5 | `dim_user` frente a lo recalculado desde `ta_trusted.posts` (*v2.38: `fact_posts` no tiene `followers`*) | `5936 usuarios, 0 distintos · suma de n_posts 221629` |
| E6 | Las cinco consultas guardadas (texto = `src/lakehouse/queries/curated_*.sql`, base `ta_curated`) | Cada una `SUCCEEDED` con filas (medido en el cierre): `ta-curated-posts-por-origen` 6 (2 orígenes × 3 fuentes, ~8 MB), `ta-curated-tendencia-diaria` 90 (30 días × 3 marcas), `ta-curated-emociones-por-hora-bogota` 469, `ta-curated-semana-vs-fin-de-semana` 6, `ta-curated-precision-nlp` 9; 1,0–2,1 s cada una |
| E7 | Bytes escaneados | Unos 10–60 MB en 10 consultas, la mayor muy por debajo de 1 GiB; ≈ 0,0005 USD (mínimo 10 MB por consulta) |
| E8 | Latencia de las consultas guardadas | OK si todas tardan ≤ 3.000 ms en total (con la cola de Athena); AVISO si alguna pasa (el KPI de < 3 s es del dashboard con SPICE, Lote 6.1) |

Manual (o en la consola de Athena, workgroup `ta-analytics`, «Saved queries»):

```bash
aws athena start-query-execution --region us-east-1 --work-group ta-analytics \
  --query-execution-context Database=ta_curated \
  --query-string "SELECT origin, COUNT(*) AS filas FROM ta_curated.fact_posts GROUP BY origin"
aws athena get-query-results --region us-east-1 --query-execution-id <id>
aws athena list-named-queries --region us-east-1 --work-group ta-analytics
```

## Parte F · Segunda corrida dentro del workflow: idempotencia y costo

```bash
python project/scripts/validate_4_3.py rerun    # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | Lista `curated/`, lee el último reporte y cuenta en Athena | `187 objetos · reporte <corrida de D> · 221629 filas de fact_posts en Athena · horario DISABLED`. FALLA si D no se corrió |
| F2 | `start-workflow-run` (`ta_validacion=4.3`) y espera | `COMPLETED · ta-job-raw-to-trusted SUCCEEDED (~90 s) → ta-job-trusted-to-curated SUCCEEDED` |
| F3 | El reporte del 4.3 de esta corrida | `archivos cambiados 0 · escritos 0 · borrados 0 · mismos sha256 que la corrida anterior en 187 archivos · fact_posts 221629 · Trusted de la misma corrida: 221629 filas (coincide: True)` |
| F4 | Lista otra vez | `187 → 187 objetos`, mismas claves y tamaños |
| F5 | Athena otra vez | `221629 → 221629 filas · 221629 event_id distintos` |
| F6 | `get-workflow-run-properties` | `ta_trusted_report = quality/…/raw_to_trusted/wr_….json · ta_trusted_rows = 221629 · ta_curated_report = quality/…/trusted_to_curated/wr_….json · ta_curated_rows = 221629 · ta_curated_files_changed = 0` |
| F7 | Marcas, corridas y horario | `curated/_ta_placeholder/: 0 → 0 · corridas en curso: ninguna · horario DISABLED → DISABLED (al terminar: … on --yes)` |
| F8 | Costo con lo medido | `ta-job-raw-to-trusted`: segundos × 1 DPU + S3 ≈ 0,019 USD · `ta-job-trusted-to-curated`: segundos × 0,0625 DPU (mínimo 60 s) + S3 ≈ 0,0007 USD → ≈ 0,020 USD por corrida del workflow; con el horario cada hora ≈ 0,48 USD/día, ≈ 14,3 USD/mes (estimado antes de validar; **medido en el cierre: ≈ 0,0196 USD por corrida, ≈ 0,47 USD/día, ≈ 14,1 USD/mes**) |

Manual:

```bash
aws glue start-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-properties ta_validacion=4.3
aws glue get-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…> --include-graph --query 'Run.[Status,Statistics]'
aws glue get-workflow-run-properties --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…>
```

## Final: horario encendido (estado normal)

```bash
python project/scripts/validate_4_3.py on --yes   # muestra el costo por día (el de F8 si existe)
python project/scripts/validate_4_3.py status
python project/scripts/status.py                  # lo demás que cobra (GCP y local)
```

| Paso | Esperado |
|---|---|
| N1 | `DISABLED → ENABLED; se apaga con: python project/scripts/validate_4_3.py off` |
| N2 | `ENABLED · rate(1 hour) · UTC · reintentos 0` (la definición del horario intacta) |
| S1 | `ENABLED (estado normal por decisión del usuario: cada hora corren Trusted y Curated)` |
| S2 | Ninguna corrida ni job run en curso (la de la hora siguiente puede aparecer `RUNNING`: es normal) |
| S3 | `curated/: 187 objetos {fact_posts: 181, …}` y el último reporte |
| S4, S5 | Ningún horario de prueba; `curated/_ta_placeholder/`: 0 objetos |

Queda guardado (no cobra por tiempo, centavos de S3): `curated/` (187 objetos, ~18 MB) y los reportes de
`quality/`. **No** borre `curated/`: el 4.4, el 5.2 y el 6.1 lo leen. Si hiciera falta dejar el lakehouse sin
cobrar: `python project/scripts/validate_4_3.py off` (decisión del usuario; hoy el estado normal es encendido).

## Después de esta validación

*(Hecho en v2.38: los dos puntos de abajo.)*

- **Cierre del 4.3 (v2.38):** anotar en `ARCHITECTURE.md` §3.22 el tiempo, la CPU y la memoria en Glue (D3–D4), si
  Athena leyó los tipos de Curated y corrió las cinco consultas (E1, E6), la latencia (E8) y el costo medido (F8);
  en esta guía, la tabla de cierre; actualizar `ACTIVE_SERVICES.md` con el costo medido.
- Si D4 mostró más de 800 MB o D3 pasó de unos minutos: decidir 1 DPU (`glue_job_settings`, §3.22). *(No hizo
  falta: 442 MB y 21 s.)*

## Cierre

**Cerrado en v2.38** (27-oct-2026, WSL, perfil `default` de la cuenta 648617469176; `main` = `2aec128`). Lo corrió
el maestro del lote; E se repitió tras corregir E5 en el validador.

| Parte | Resultado |
|---|---|
| `off` (Preparación, paso 7) | O1 `ENABLED → DISABLED`; O4 6 marcas viejas del marcador borradas |
| A · Preflight | OK (8); A6 horario `DISABLED` y nada en curso |
| B · Pruebas | OK: B1 123 sin omitidas, B2 34 (35 desde la corrección de E5), B3 18, B4 30, B5 48 |
| L · Local | OK (4): L1 221.629 → 221.629 en 181 particiones, tablas como D4; L2 0 escritos, 187 sin cambios; L3 esquemas = contrato |
| C · Terraform | AVISO solo por C0 (cuenta raíz): C2 5 a crear, 2 a cambiar, 0 a destruir; apply hecho; C3b sin cambios |
| D · Corrida real | OK (8): `jr_a4ea718d…` `SUCCEEDED`, **21 s a 0,0625 DPU**; `glue_job_done`: leídas 221.629, `fact_posts` 221.629 en 181 particiones, `agg_emotion_by_brand_hour` 39.954, `agg_trend_daily` 543, `dim_brand` 3, `dim_date` 181, `dim_source` 3, `dim_user` 5.936; lectura 3,7 s, total 9,1 s, **CPU 9,8 s**, **pico 442,3 MB**, **Python 3.9.10, pyarrow 7.0.0, boto3 1.21.21**; conciliación OK (156.366 con sentimiento, 65.263 sin NLP); 187 objetos, 18.061.225 bytes, escritos 187; `_ta_placeholder` y `edges_*` 0 → 0; esquemas = contrato. El reporte de la corrida suelta quedó como `manual-…` (Glue no pasa `--JOB_RUN_ID`) |
| E · Athena | **OK (8) tras la corrección.** Primera vuelta: E5 FALLA por un error del validador (`COLUMN_NOT_FOUND: … Column 'followers' cannot be resolved`: recalculaba `max_followers` desde `fact_posts`, que no tiene `followers`); el resto OK. Corregido en v2.38 (`USER_SQL` lee `ta_trusted.posts`, con su prueba) y repetido: E1 historical 50.000 (2.662 comparativas) y stream 171.629 (9.104), total 221.629; E2 Trusted = `fact_posts`, `date_key`, llaves foráneas y `dim_date` OK; E3 0 diferencias, 6 filas día × marca con `n_posts` ≠ suma (65.263 sin NLP); E4 OK; **E5 5.936 usuarios, 0 distintos**; E6 las 5 consultas guardadas OK (6, 90, 469, 6 y 9 filas); E7 26,6–32,6 MB en 10 consultas, la mayor 8,1 MB, ≈ 0,0005 USD; E8 la más lenta 1,5–2,1 s (motor 0,8–1,7 s) frente al KPI de 3 s |
| F · Idempotencia y costo | OK (8): `wr_77faf078…` `COMPLETED`, `raw_to_trusted` 88 s → `trusted_to_curated` 20 s; 0 archivos cambiados, 0 escritos, mismos sha256, 187 → 187 objetos, Athena 221.629 → 221.629; propiedades `ta_trusted_*` y `ta_curated_*`; F8 ≈ 0,0196 USD por corrida (raw_to_trusted ≈ 0,01886, trusted_to_curated ≈ 0,00074) → ≈ 0,47 USD/día y ≈ 14,1 USD/mes con el horario cada hora |
| `on` / `status` | N1 `DISABLED → ENABLED`, N2 `rate(1 hour)` UTC, 0 reintentos / 5 OK: horario `ENABLED`, nada en curso, `curated/` 187 objetos, 0 marcas |

El horario queda **encendido cada hora** (estado normal): Trusted y Curated corren solos. Cifras también en
`ARCHITECTURE.md` §3.22 y `CHANGELOG.md` v2.38.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` antes de imprimir nada | La carpeta quedó inválida en `/mnt/c` tras copiar desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A1 `Python 3.14…` o falta `pyarrow` | Está en PowerShell o sin el venv: WSL, `source ~/.venvs/ta-streaming/bin/activate`. |
| A6 / D0 AVISO por el horario `ENABLED` | Es el estado normal, pero mientras valida: `validate_4_3.py off` (Preparación, paso 7); al final, `on --yes`. |
| L0 AVISO «no hay trusted/posts/» | `python project/scripts/profile_data.py sync --layer trusted` (solo lee S3) o `validate_4_2.py local`, y repita. |
| L1 FALLA con `CuratedBuildError` | La conciliación no cuadró sobre la copia local: páseme el mensaje (trae las comprobaciones que fallaron). |
| C2 FALLA «no aplique» | El plan cambia algo fuera del job `trusted_to_curated` y las cinco consultas (el detalle muestra cada atributo `antes → después`). No aplique; páseme el detalle. |
| C3 falla con `Failed to save state` | Como en el 3.2: `terraform -chdir=project/infra/aws/environments/dev state push errored.tfstate`, `state list`, mueva `errored.tfstate` fuera del repo y repita C. |
| D1 «el script de S3 no es …» | Falta aplicar C (o el archivo cambió después): `validate_4_3.py terraform --apply`. |
| D3 `FAILED` con `MemoryError` o el job muere sin traceback | Faltó memoria a 0,0625 DPU (no debería: el pico medido fue 0,43 GB). Páseme D4 y el traceback de `/aws-glue/python-jobs/error`; la salida es `glue_job_settings = { trusted_to_curated = { max_capacity = 1 } }` en `terraform.tfvars` y repetir C con `--apply` y D. |
| D3 `FAILED` con `AccessDenied` | Mire en el traceback la operación y la clave: `ta-glue-role` debería poder (§5.8). Si es un `GetObject` de `quality/_latest/…`, el job no listó antes (no debería pasar). |
| D3 `FAILED` con `TrustedSchemaError` o `CuratedBuildError` | Un Parquet de Trusted sin el contrato, o Curated no concilia: el mensaje trae la clave o las comprobaciones. Páseme ese mensaje. |
| D3/F2 `ConcurrentRunsExceededException` repetido | Hay otra corrida del job o del workflow (¿el horario?): `validate_4_3.py status` y, si sobra, `off`. |
| D4 sin `glue_job_done` | Los logs tardan: busque a mano en `/aws-glue/python-jobs/output` con el prefijo del job run (comando de D). |
| D6 FALLA «Trusted X, leídas Y» | Trusted cambió entre D2 y la corrida (¿corrió el workflow o el exportador?). Apague (`off`, `validate_3_2.py off`) y repita D. |
| D7 FALLA por marcas o aristas | Algo escribió en `curated/_ta_placeholder/` o `curated/edges_*` durante D (¿una corrida con el script viejo?). Páseme el detalle. |
| E1 sin uno de los orígenes | Mire `ta_trusted.posts` (¿Trusted vacío o recortado?) y el reporte de D. |
| E3/E4/E5 con diferencias | Páseme el detalle (cuántas claves, conteos o promedios) y el reporte de D5. |
| E6 «no está en ta-analytics» o «el texto guardado no es …» | Falta aplicar C o el archivo cambió después: `validate_4_3.py terraform --apply`. |
| E6 `FAILED` con un error de sintaxis | La consulta no es válida en Athena (Trino): páseme el mensaje y el nombre; la corrijo en `src/lakehouse/queries/`. |
| F3 FALLA «archivos cambiados N» | Trusted cambió entre D y F (exportador) o el job no es determinista: páseme `changed_keys` (`aws s3 cp …/quality/_latest/trusted_to_curated.json -`). |
| El horario quedó `DISABLED` al terminar | `python project/scripts/validate_4_3.py on --yes` (estado normal: encendido). |
