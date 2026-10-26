# Validación del Lote 4.2 · HU-026

> Capa Trusted: el job de Glue `ta-job-raw-to-trusted` (Python shell 3.9, 1 DPU) une los dos orígenes de
> Raw, descarta los centinelas `ta-verify@*`, deduplica por `event_id` (menor `process_time`), conserva las
> filas sin NLP con NLP nulo, corrige tipos, calcula `text_clean` y escribe `trusted/posts/source=…/dt=…/`
> reconstruyendo Trusted entero en cada corrida (idempotente), con un reporte de calidad por corrida en
> `quality/`. Contrato en `docs/ARCHITECTURE.md` §3.21 (interfaces en §3.21.1) y §5.7.
>
> **Automático:** `python project/scripts/validate_4_2.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_4_2.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-026 · `ta-job-raw-to-trusted` une `origin=stream` e `origin=historical` en `ta_trusted.posts` | D2, D6, D7, E2 |
| HU-026 · Deduplica por `event_id`, corrige tipos y limpia el texto | D6, D8, E1, E3 (y L1, B1) |
| HU-026 · Aplica las reglas de calidad (las de Trusted que dependen del job; GE es del 5.2) | D6 (reporte), E3, E4 (DQ-13, DQ-48/49, DQ-54, DQ-56, DQ-57, DQ-58, DQ-63) |
| HU-026 · Duplicados < 1 % medidos en Trusted | E1 (se espera 0) |
| HU-026 · Idempotente: sobrescribe las particiones `dt`; reejecutar no duplica | F3–F5 (y L2) |
| Decisiones del usuario: sin NLP conservadas, centinelas fuera, menor `process_time` | D6, E3, B1 |
| `trusted/_ta_placeholder/` sin escrituras nuevas | D7 |
| Estado de ahorro: horario `DISABLED`, nada corriendo | A6, F7, `off`, `status` |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL (los comandos se corren desde aquí) |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | Terraform, `aws` y boto3 |
| `TA_QUALITY_CACHE` | (opcional) por defecto `~/.cache/trendanalyzer/quality` | La caché de solo lectura de Raw del 5.1 que usa la parte L |
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
3. Que nada del 4.1 esté corriendo y que el exportador siga en pausa (si sube objetos durante D, la
   conciliación cambia de foto):
   ```bash
   python project/scripts/validate_4_2.py status
   python project/scripts/validate_3_2.py status
   ```
   Esperado: `S1 OK … DISABLED`, `S2 OK … ninguna corrida; ningún job run`, `S3 OK trusted/posts/: 0 objetos ·
   sin reporte …` (antes de la primera corrida) y el Scheduler del exportador en `PAUSED`.
4. (Para la parte L) la caché de Raw del 5.1: `ls ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake/raw`
   muestra `origin=historical  origin=stream`. Si no existe: `python project/scripts/profile_data.py sync --layer raw`
   (solo lee S3; ~4 min la primera vez).
5. `project/infra/aws/environments/dev/backend.hcl`, `terraform.tfvars` y `gcp_exporter.auto.tfvars` presentes (desde
   el 3.1/3.2). No corra otra validación que use el estado de `infra/aws` a la vez.

### Ejecución

```bash
python project/scripts/validate_4_2.py preflight
python project/scripts/validate_4_2.py tests
python project/scripts/validate_4_2.py local --profile        # opcional, sin nube (~6 min con --profile)
python project/scripts/validate_4_2.py terraform               # plan: 0 a crear, 2 a cambiar
python project/scripts/validate_4_2.py terraform --apply --yes # aplica el script y la capacidad del job
python project/scripts/validate_4_2.py run --yes               # una corrida real del job (~3–6 min)
python project/scripts/validate_4_2.py athena                  # ~2–3 min (E4 recorre la proyección de Raw)
python project/scripts/validate_4_2.py rerun --yes             # segunda corrida en el workflow (~5–8 min)
python project/scripts/validate_4_2.py off
python project/scripts/validate_4_2.py status
```

Salida esperada (resumen; el detalle de cada paso, abajo): cada parte termina con `-- Parte X: OK (n OK)` y
código 0, salvo `terraform` sin `--apply` (AVISO: C3 «sin --apply») y C0 en AVISO si el perfil es la cuenta
raíz. Con el Raw del 26-oct-2026: D6 `Raw 221637 (pies de S3) = leídas 221637 = escritas 221629 + centinelas 6 +
duplicadas 2 + descartadas 0`, E1 `221629 filas · 221629 event_id distintos · duplicados 0.0000%`, F3
`particiones cambiadas 0 · borradas 0 · mismos sha256`.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada (A lee el horario y las corridas) | GCP, Docker | 0 |
| L | La caché de Raw del 5.1 | Nube | 0 |
| C | Perfil de AWS | GCP, Docker | 0 (cambia el script y la capacidad del job) |
| D | Lo de C aplicado; Raw sin cambios durante la parte (exportador en pausa) | GCP | 1 corrida del job a 1 DPU (≈ 0,01–0,02 USD) + ~13.800 GET para contar filas (≈ 0,006 USD) |
| E | D hecho | GCP | 4 consultas de Athena (≈ 0,0003 USD; E4 escanea Raw) |
| F | D hecho | GCP | 1 corrida del workflow (el job + el marcador del 4.3; ≈ 0,01–0,02 USD) |

**Estado de ahorro del lote:** nada queda encendido. El horario `ta-wf-raw-to-curated-cada-hora` sigue
`DISABLED` (este lote no lo enciende); Trusted (~67 MB en S3) y los reportes en `quality/` quedan guardados
(centavos al mes).

---

## Parte A · Preflight del código y de lo encendido

```bash
python project/scripts/validate_4_2.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows: repita desde WSL. Las demás partes salen con 2 fuera de WSL |
| A1 | Python 3.11; `boto3`, `botocore`, `pyarrow` |
| A2 | Los 7 archivos del lote presentes |
| A3 | Los 13 archivos de lotes anteriores que usa (módulos `glue_workflow` y `glue_role`, `catalog_spec`, `s3_parquet`, el marcador `trusted_to_curated.py`, `contracts/athena/posts.sql`, `quality/{layers,testing,s3sync}.py`, `ops/{validate41,validate32,services}.py`) |
| A4 | `tables.json` al día con `contracts/athena/` |
| A5 | `terraform`, `aws`, `bash` |
| A6 | Horario `DISABLED`, ninguna corrida del workflow ni job run en curso. AVISO si algo está encendido |
| A7 | `services.status` (como `status.py`): nada `ENCENDIDO` |
| A8 | La caché de Raw del 5.1 presente (AVISO si falta: solo la usa L) |

## Parte B · Pruebas

```bash
python project/scripts/validate_4_2.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/lakehouse -q -rs` | **90 passed, 0 skipped** (39 del job real con Parquet reales y el profiler del 5.1; las del 4.1) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate42.py -q` | **30 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/ops/tests/test_ops_validate41.py -q` | **48 passed** (piezas que reutiliza el validador) |
| B5 | `python -m pytest src/ops/tests/test_ops_validate24.py -q` | **52 passed** (incluye la prueba inestable arreglada en v2.35) |

## Parte L · El job en local contra la caché de Raw (opcional, sin nube)

```bash
python project/scripts/validate_4_2.py local            # --profile para perfilar la salida con el 5.1 (~5 min más)
```

Corre el job de verdad (`raw_to_trusted.py --ta_local_src … --ta_local_dst …`) con Python 3.11 sobre la caché
de solo lectura y escribe en `{caché}/raw_to_trusted_local` (fuera del repo).

| Paso | Esperado (Raw del 26-oct-2026) |
|---|---|
| L0 | La caché existe (AVISO con el comando `sync` si no) |
| L1 | `leídas 221637 · centinelas 6 · duplicadas 2 · sin NLP 65263 · escritas 221629 en 543 particiones` en ~25–30 s, pico ~0,8 GB. AVISO si las cifras difieren (otra foto de Raw) |
| L2 | Segunda corrida: `particiones cambiadas 0 · borradas 0` |
| L3 | Con `--profile`: `reglas {'cumple': 54}`, anomalías `{'error': 0, 'warning': 1, …}` (el aviso es `nlp_ausente`, esperado). OMITIDO sin `--profile` |

Manual: `python project/src/lakehouse/glue_jobs/raw_to_trusted.py --lake_bucket ta-dev-648617469176-lake
--ta_local_src ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake --ta_local_dst ~/.cache/trendanalyzer/raw_to_trusted_local`
(imprime tres líneas JSON; la última, `glue_job_done`).

## Parte C · Terraform (infra/aws)

```bash
python project/scripts/validate_4_2.py terraform            # validate.sh + init + plan (no aplica)
python project/scripts/validate_4_2.py terraform --apply    # aplica (pide confirmación; --yes la omite)
```

El plan va limitado con `-target=module.glue_workflow`. El validador lee el plan en JSON y **no aplica** si
cambia algo distinto del script y del job de `raw_to_trusted`, o si el job cambia algo más que
`max_capacity` y la descripción.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars` (AVISO si es la cuenta raíz: no afecta) |
| C0b | `backend.hcl` y `terraform.tfvars` presentes |
| C1 | `bash infra/validate.sh` en verde |
| C2a | `terraform init` |
| C2 | **0 a crear, 2 a cambiar, 0 a destruir**: `module.glue_workflow.aws_s3_object.script["raw_to_trusted"]` (`source_hash`) y `module.glue_workflow.aws_glue_job.this["raw_to_trusted"]` (`max_capacity: 0.0625 → 1` y `description`); o «ya aplicado» |
| C3 | `Apply complete! Resources: 0 added, 2 changed, 0 destroyed.` |
| C3b | Un segundo plan: **No changes** |

Manual (desde `project/`):

```bash
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan -target=module.glue_workflow -out=../../../../evidencia/tfplan_4_2
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_4_2
```

`evidencia/tfplan_4_2*` no se versionan (`.gitignore`).

## Parte D · Corrida real del job y conciliación Raw → Trusted

```bash
python project/scripts/validate_4_2.py run      # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| D1 | `get-job` y baja el script de `_glue/scripts/` | `pythonshell 3.9 · 1 DPU (16 GB) · library-set analytics · script de S3 idéntico al repo` (compara sin CR). FALLA «aplique C» si `MaxCapacity` sigue en 0.0625 o el script es el marcador |
| D2 | Cuenta las filas de Raw por los pies de sus Parquet (~13.300 GET, 1–2 min) y los objetos de `trusted/_ta_placeholder/` | `stream 1155 objetos y 171637 filas · histórico 12102 objetos y 50000 filas · total 221637` |
| D3 | `start-job-run` (con `--ta_validacion 4.2`), espera y reintenta si Glue cuenta otra corrida | `SUCCEEDED` · segundos de ejecución · `1.0 DPU`. Anote la duración |
| D4 | La línea `glue_job_done` en `/aws-glue/python-jobs/output` (flujo = el job run) | `leídas 221637 · escritas 221629 · 543 particiones` · tiempos · **pico de memoria** · **versiones de Python, pyarrow y boto3 de Glue** (anótelas para el cierre) |
| D5 | Lee el reporte (`quality/{fecha}/raw_to_trusted/{corrida}.json`) y `quality/_latest/raw_to_trusted.json` | Los dos con la misma corrida, esquema `ta-raw-to-trusted-report@1` |
| D6 | Conciliación | `Raw 221637 (pies de S3) = leídas 221637 = escritas 221629 + centinelas 6 + duplicadas 2 + descartadas 0`; `sin NLP conservadas 65263 (desde NLP_SINCE: 0)`; `copias que difieren en algo más que process_time: 1`. Si Raw cambió (exportador) pero cuadra, OK con las cifras nuevas; si Raw no cambió y las cifras no son las de hoy, AVISO |
| D7 | Cuenta `trusted/posts/` por los pies, revisa el layout y las marcas | `543 objetos (543 particiones en el reporte), 221629 filas` · por fuente `facebook 73890, instagram 73946, reddit 73793` · `dt 2026-04-27 → 2026-10-24` · `trusted/_ta_placeholder/: N → N objetos` (sin escrituras nuevas) |
| D8 | Baja el Parquet más chico de Trusted y compara su esquema | 24 columnas en el orden de `contracts/athena/posts.sql`, `timestamp[ms, UTC]`, `int64`, `list<string>`, `text_clean` |

Manual:

```bash
aws glue start-job-run --region us-east-1 --job-name ta-job-raw-to-trusted
aws glue get-job-run --region us-east-1 --job-name ta-job-raw-to-trusted --run-id <jr_…> --query 'JobRun.[JobRunState,ExecutionTime,MaxCapacity,ErrorMessage]'
aws logs filter-log-events --region us-east-1 --log-group-name /aws-glue/python-jobs/output --log-stream-name-prefix <jr_…> --filter-pattern '"glue_job_done"'
aws s3 cp s3://ta-dev-648617469176-lake/quality/_latest/raw_to_trusted.json - | head -c 2000
aws s3 ls s3://ta-dev-648617469176-lake/trusted/posts/ --recursive | wc -l
```

## Parte E · Athena: `ta_trusted.posts`

```bash
python project/scripts/validate_4_2.py athena
```

| Paso | Consulta | Esperado |
|---|---|---|
| E1 | `SELECT COUNT(*), COUNT(DISTINCT event_id) FROM ta_trusted.posts` | `221629 filas · 221629 event_id distintos · duplicados 0.0000%` = las escritas del reporte. AVISO si hay duplicados por debajo del 1 %; FALLA desde el 1 % |
| E2 | Filas, días y rango por `source` y `origin` | Cada fuente con `historical` en 180 días (2026-04-27 → 2026-10-23) y `stream` en 4 días (2026-10-21 → 2026-10-24); las sumas por fuente = el reporte |
| E3 | `count_if` de las reglas | `0 centinelas · 0 dt ≠ DATE(event_time) · 0 text_clean nulos o más largos que text · 0 claves nulas · 65263 filas sin NLP conservadas (= reporte; 0 desde 2026-10-22T23:00:00Z)` |
| E4 | `SELECT COUNT(DISTINCT event_id) FROM ta_raw.posts_raw` (DQ-58; ~70 s por la proyección) | `221629 / 221635 = 0.99997` (entre 0,99 y 1) |
| E5 | Bytes escaneados | Unos 40–60 MB en total (mínimo 10 MB por consulta) |

Manual (o en la consola de Athena, workgroup `ta-analytics`):

```bash
aws athena start-query-execution --region us-east-1 --work-group ta-analytics \
  --query-execution-context Database=ta_trusted \
  --query-string "SELECT COUNT(*) AS filas, COUNT(DISTINCT event_id) AS event_ids FROM ta_trusted.posts"
aws athena get-query-results --region us-east-1 --query-execution-id <id>
```

## Parte F · Segunda corrida dentro del workflow: idempotencia y costo

```bash
python project/scripts/validate_4_2.py rerun    # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | Lista `trusted/posts/`, lee el último reporte y cuenta en Athena | `543 objetos · reporte <corrida de D> · 221629 filas en Athena`. FALLA si D no se corrió |
| F2 | `start-workflow-run` (`ta_validacion=4.2`) y espera | `COMPLETED · ta-job-raw-to-trusted SUCCEEDED → ta-job-trusted-to-curated SUCCEEDED (marcador de posición hasta el 4.3)` |
| F3 | El reporte de esta corrida | `particiones cambiadas 0 · borradas 0 · mismos sha256 que la corrida anterior en 543 archivos · escritas 221629` |
| F4 | Lista otra vez | `543 → 543 objetos`, mismas claves y tamaños |
| F5 | Athena otra vez | `221629 → 221629 filas · 221629 event_id distintos` |
| F6 | `get-workflow-run-properties` | `ta_trusted_report = quality/…/wr_….json · ta_trusted_rows = 221629 · ta_trusted_partitions_changed = 0` |
| F7 | Borra las marcas del marcador del 4.3 y mira el horario | `marcas … borradas: 1 · corridas en curso: ninguna · horario DISABLED` |
| F8 | Costo con lo medido | Glue (segundos × 1 DPU × 0,44 USD/DPU-h, mínimo 60 s) + el marcador + S3 (≈ 13.257 GET y 543 PUT) → USD por corrida, por día y por mes **si se encendiera el horario** (estimado antes de validar: ≈ 0,015–0,025 USD por corrida, ≈ 11–17 USD/mes). Anótelo para el cierre |

Manual:

```bash
aws glue start-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-properties ta_validacion=4.2
aws glue get-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…> --include-graph --query 'Run.[Status,Statistics]'
aws glue get-workflow-run-properties --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…>
aws s3 rm s3://ta-dev-648617469176-lake/curated/_ta_placeholder/ --recursive
```

## Apagado

```bash
python project/scripts/validate_4_2.py off        # el mismo apagado del 4.1
python project/scripts/validate_4_2.py status     # horario DISABLED, nada en curso, Trusted y su último reporte
python project/scripts/status.py                  # lo demás que cobra (GCP y local)
```

| Paso | Esperado |
|---|---|
| O1 | Horario `DISABLED` |
| O2 | Ninguna corrida ni job run en curso (detiene los que haya) |
| O3 | Ningún horario de prueba del 4.1 |
| O4 | Marcas `_ta_placeholder/` borradas |
| O5 | `services.status`: nada `ENCENDIDO` |

Queda guardado (no cobra por tiempo, centavos de S3): `trusted/posts/` (543 objetos, ~67 MB) y los reportes
de `quality/`. **No** borre `trusted/posts/`: el Lote 4.3 y el cierre del 5.1 lo leen.

## Después de esta validación

- **Cierre del 5.1** (profiling real de Trusted): `python project/scripts/validate_5_1.py trusted` ya encuentra
  objetos en `trusted/posts/` y perfila el Trusted real (esperado: lo mismo que L3).
- **Cierre del 4.2 (v2.36):** anotar en `ARCHITECTURE.md` §3.21 la versión de pyarrow de Glue (D4), el tiempo y
  la memoria en Glue (D3–D4) y el costo medido (F8), y en esta guía la tabla de cierre.

## Cierre

*Pendiente de la validación del maestro.*

| Parte | Resultado |
|---|---|
| A · Preflight | |
| B · Pruebas | |
| L · Local | (antes de entregar: L1 221.629 escritas en 543 particiones, L2 sin cambios, L3 54/54 reglas) |
| C · Terraform | |
| D · Corrida real | |
| E · Athena | |
| F · Idempotencia y costo | |
| `off` / `status` | |

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` antes de imprimir nada | La carpeta quedó inválida en `/mnt/c` tras copiar desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A1 `Python 3.14…` o falta `pyarrow` | Está en PowerShell o sin el venv: WSL, `source ~/.venvs/ta-streaming/bin/activate`. |
| C2 FALLA «no aplique» | El plan cambia algo fuera del job `raw_to_trusted` (el detalle muestra cada atributo `antes → después`). No aplique; páseme el detalle. |
| C3 falla con `Failed to save state` | Como en el 3.2: `terraform -chdir=project/infra/aws/environments/dev state push errored.tfstate`, `state list`, mueva `errored.tfstate` fuera del repo y repita C. |
| D1 «el script de S3 no es …» o `MaxCapacity 0.0625` | Falta aplicar C (o el archivo cambió después): `validate_4_2.py terraform --apply`. |
| D3 `FAILED` con `ModuleNotFoundError: No module named 'pyarrow'` | Glue no trae pyarrow en este entorno: en `terraform.tfvars`, `glue_job_settings = { raw_to_trusted = { arguments = { "--additional-python-modules" = "pyarrow==7.0.0" } } }`, repita C con `--apply` y D. Páseme el error. |
| D3 `FAILED` con `MemoryError` o el job muere sin traceback | Faltó memoria (no debería con 1 DPU: el pico medido fue 0,6 GB). Páseme D4 (`peak_rss_mb`) y el traceback de `/aws-glue/python-jobs/error`. |
| D3 `FAILED` con `AccessDenied` | Mire en el traceback la operación y la clave: el rol `ta-glue-role` debería poder (§5.8). Si es un `GetObject` de `quality/_latest/…`, el job no listó antes (no debería pasar con el script del lote). |
| D3 `FAILED` con `RawSchemaError` | Un Parquet de Raw no tiene el esquema de §5.7: el mensaje trae la clave y la columna. Páseme ese mensaje. |
| D3/F2 `ConcurrentRunsExceededException` repetido | Hay otra corrida del job o del workflow de verdad: `validate_4_2.py status` y, si sobra, `off`. |
| D4 sin `glue_job_done` | Los logs tardan: busque a mano en `/aws-glue/python-jobs/output` con el prefijo del job run (comando de D). |
| D6 FALLA «el job leyó X y S3 tiene Y» | Raw cambió entre D2 y la corrida (¿corrió el exportador?). Páuselo (`validate_3_2.py off`) y repita D. |
| D7 FALLA por marcas en `trusted/_ta_placeholder/` | Algo escribió allí durante D (¿el marcador del 4.1 en una corrida del workflow?). Páseme el detalle; `off` las borra. |
| E1 FALLA (duplicados ≥ 1 %) | No debería pasar: páseme E1 y el reporte de D5. ¿Hay objetos de más en `trusted/posts/` (D7)? |
| E3 `sin_nlp ≠ reporte` | Otra corrida cambió Trusted entre D y E: repita E. |
| F3 FALLA «particiones cambiadas N» | Raw cambió entre D y F (exportador) o el job no es determinista: páseme `changed_keys` del reporte (`aws s3 cp …/quality/_latest/raw_to_trusted.json -`). |
| F2 `ta-job-trusted-to-curated` FAILED (AVISO) | Es el marcador del 4.1: mire su `ErrorMessage`; no afecta a Trusted. |
| Quedó algo corriendo o el horario encendido | `python project/scripts/validate_4_2.py off`. |
