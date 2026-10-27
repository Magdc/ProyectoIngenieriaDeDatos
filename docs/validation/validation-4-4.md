# Validación del Lote 4.4 · HU-028

> Tablas de aristas: el mismo job del 4.3, `ta-job-trusted-to-curated` (versión **1.1**, ADR-27), escribe además
> `ta_curated.edges_user_brand` (`user_hash`, `brand`, `weight` = filas del usuario que mencionan la marca, cada
> marca distinta de `brands`) y `ta_curated.edges_hashtag_cooccurrence` (`hashtag_a < hashtag_b`, `weight` =
> filas en las que aparecen los dos hashtags). Las siete tablas del 4.3 salen con los mismos bytes. Contrato en
> `docs/ARCHITECTURE.md` §3.23 (interfaces en §3.23.1) y §5.7; `contracts/athena/curated.sql` no cambia.
>
> **Automático:** `python project/scripts/validate_4_4.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_4_4.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.
>
> **El horario del lakehouse queda ENCENDIDO al terminar** (decisión del usuario: es el estado normal). La guía
> lo apaga en la Preparación (paso 6) para que ninguna corrida automática caiga en medio, y el último paso de la
> Ejecución lo vuelve a encender (`on --yes`).

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-028 · `edges_user_brand` con peso = número de menciones | D6 (Σ `weight` = menciones del reporte), E1 (recalculada desde `fact_posts`: Σ `weight` = Σ `cardinality(brands)`), L4 (recalculada aparte con pyarrow) |
| HU-028 · `edges_hashtag_cooccurrence` con pares de hashtags y su frecuencia | D6, E3 (recalculada desde `ta_trusted.posts` y Σ C(k, 2) por fila), L4 |
| HU-028 · Las dos consultables desde Athena (ADR-09: sin base de grafos) | E1–E4 (E4: dos consultas de ejemplo de «análisis de relaciones») |
| Grano y reglas de ADR-27 (cada marca y hashtag distinto una vez por fila; `hashtag_a < hashtag_b`; llaves a `dim_user` y `dim_brand`) | D6, E2, L4 |
| Las siete tablas del 4.3 no cambian (solo se escriben las aristas) | D7 (`changed_keys` solo de aristas), L1 la primera vez (2 archivos cambiados) |
| Idempotencia (mismo Trusted ⇒ mismos bytes, 0 escrituras) y costo por hora sin cambio | F3–F5, F8 (y L2) |
| Estado final: horario `ENABLED` (estado normal) | `on` (N1, N2), `status` (S1, S3) |

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

1. Terminal en WSL:
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
   python project/scripts/validate_4_4.py status
   python project/scripts/validate_3_2.py status
   ```
   Esperado: `S1 OK … ENABLED (estado normal …)`, `S2 OK … ninguna corrida; ningún job run`, `S3 AVISO curated/:
   187 objetos {…, edges_user_brand: 0, edges_hashtag_cooccurrence: 0} · último reporte … (trusted-to-curated@1.0 …)
   · sin aristas todavía` y el Scheduler del exportador en `PAUSED` (si sube objetos, Trusted cambia entre partes).
4. (Para la parte L) la copia local del Trusted de S3:
   `ls ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake/trusted/posts` muestra
   `source=facebook  source=instagram  source=reddit`. Si no existe: `python project/scripts/profile_data.py sync
   --layer trusted` (solo lee S3; ~1 min).
5. `project/infra/aws/environments/dev/backend.hcl` y `terraform.tfvars` presentes. No corra otra validación que
   use el estado de `infra/aws` a la vez.
6. **Apague el horario mientras valida.** Si el paso 3 mostró una corrida en curso (S2), espere a que termine
   (~3 min; repita `status`) y entonces:
   ```bash
   python project/scripts/validate_4_4.py off
   ```
   Esperado: `O1 OK deshabilitar el horario: ENABLED → DISABLED`, `O2 OK … nada en curso` y `-- Parte off: OK`
   (O5 puede dar AVISO si algo de GCP sigue encendido). El último paso de la Ejecución lo vuelve a encender.

### Ejecución

```bash
python project/scripts/validate_4_4.py preflight
python project/scripts/validate_4_4.py tests
python project/scripts/validate_4_4.py local --python /root/.cache/ta-glue39-venv/bin/python   # sin nube, ~30 s
python project/scripts/validate_4_4.py terraform               # plan: 0 a crear, 2 a cambiar
python project/scripts/validate_4_4.py terraform --apply --yes # aplica el script y la descripción del job
python project/scripts/validate_4_4.py run --yes               # una corrida real del job (~2–4 min)
python project/scripts/validate_4_4.py athena                  # ~1–2 min, 5 consultas
python project/scripts/validate_4_4.py rerun --yes             # segunda corrida en el workflow (~5–8 min)
python project/scripts/validate_4_4.py on --yes                # estado final: horario ENCENDIDO
python project/scripts/validate_4_4.py status
```

Salida esperada (resumen; el detalle de cada paso, abajo): cada parte termina con `-- Parte X: OK (n OK)` y
código 0, salvo `preflight` con A6 en AVISO si el horario sigue encendido, `terraform` sin `--apply` (AVISO: C3
«sin --apply») y C0 en AVISO si el perfil es la cuenta raíz. Con el Trusted del 26-oct-2026: L1 y D6
`edges_user_brand 15395 pares, 233395 menciones · edges_hashtag_cooccurrence 136 pares, 44578 coocurrencias, 25
hashtags · 16 comprobaciones (todas OK)`; D7 `archivos cambiados 2 (solo aristas)`; E1 `Σ weight 233395 = menciones
233395 = Σ cardinality(brands) 233395`; E3 `Σ weight 44578 = recalculadas 44578 = Σ C(k, 2) por fila 44578`; F3
`archivos cambiados 0 · escritos 0 · borrados 0 · mismos sha256 … en 189 archivos (2 de aristas)`; `on` `N1 OK …
DISABLED → ENABLED`; `status` `S1 OK … ENABLED` y `S3 OK curated/: 189 objetos`.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada (A lee el horario y las corridas) | GCP, Docker | 0 |
| L | La copia local del Trusted de S3 | Nube | 0 |
| C | Perfil de AWS | GCP, Docker | 0 (cambia el script y la descripción del job) |
| D | Lo de C aplicado; Trusted sin cambios durante la parte (exportador en pausa); horario apagado (paso 6) | GCP | 1 corrida del job a 0,0625 DPU (≈ 0,0005 USD) + ~550 GET para contar filas (≈ 0,0003 USD) |
| E | D hecho | GCP | 5 consultas de Athena (≈ 0,0003 USD; mínimo 10 MB cada una) |
| F | D hecho | GCP | 1 corrida del workflow (el job del 4.2 a 1 DPU + este; ≈ 0,02 USD) |
| `on` | — | — | El horario vuelve a cobrar ≈ 0,47 USD/día (estado normal decidido por el usuario) |

**Estado final del lote:** horario `ta-wf-raw-to-curated-cada-hora` **`ENABLED`** (cada hora corren Trusted y
Curated con las aristas; ≈ 14,1 USD/mes, el mismo costo que antes: el job sigue por debajo del minuto cobrado).
Curated (189 objetos, ~18,5 MB) y los reportes de `quality/` quedan guardados (centavos al mes).

---

## Parte A · Preflight del código y de lo encendido

```bash
python project/scripts/validate_4_4.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows: repita desde WSL. Las demás partes salen con 2 fuera de WSL |
| A1 | Python 3.11; `boto3`, `botocore`, `pyarrow` |
| A2 | Los 7 archivos del lote presentes (job, sus dos archivos de pruebas, `lakehouse.tf`, validador, sus pruebas y el script) |
| A3 | Los 16 archivos de lotes anteriores que usa (los 14 del 4.3, `ops/validate43.py` y `test_ops_validate43.py`, cuyo AWS falso reutilizan las pruebas) |
| A4 | `tables.json` al día con `contracts/athena/` y las dos aristas en el catálogo |
| A5 | `terraform`, `aws`, `bash` |
| A6 | Horario `DISABLED` (lo apagó el paso 6), ninguna corrida ni job run en curso. AVISO con `ENABLED` (recuerda `off` y, al final, `on --yes`) o con algo en curso |
| A7 | `services.status` (como `status.py`): nada `ENCENDIDO` |
| A8 | La copia local del Trusted presente (AVISO si falta: solo la usa L) |

## Parte B · Pruebas

```bash
python project/scripts/validate_4_4.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/lakehouse -q -rs` | **127 passed, 0 skipped** (37 del job con las aristas: 33 del 4.3 + 4 del 4.4) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate44.py -q` | **27 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/ops/tests/test_ops_validate43.py -q` | **35 passed** (su AWS falso y sus piezas los reutiliza este validador) |
| B5 | `python -m pytest src/ops/tests/test_ops_validate42.py -q` | **30 passed** |
| B6 | `python -m pytest src/ops/tests/test_ops_validate41.py -q` | **48 passed** |

## Parte L · El job en local contra el Trusted real (opcional, sin nube)

```bash
python project/scripts/validate_4_4.py local --python /root/.cache/ta-glue39-venv/bin/python   # o sin --python
```

Corre el job de verdad (`trusted_to_curated.py --ta_local_src … --ta_local_dst …`) sobre la copia local del
Trusted de S3, escribe en `{caché}/trusted_to_curated_local` (fuera del repo; la misma carpeta que el 4.3) y
recalcula las aristas aparte con pyarrow (`group_by` y `join`, no el código del job).

| Paso | Esperado (Trusted del 26-oct-2026) |
|---|---|
| L0 | Lee `…/s3/ta-dev-648617469176-lake/trusted/posts/` (copia de S3) o, si no está, la salida local del 4.2. AVISO con el comando `sync` si no hay ninguna |
| L1 | `Trusted 221629 → fact_posts 221629 · edges_user_brand 15395 pares, 233395 menciones · edges_hashtag_cooccurrence 136 pares, 44578 coocurrencias, 25 hashtags · 16 comprobaciones (todas OK) · versión trusted-to-curated@1.1 · archivos cambiados 2` la primera vez sobre la salida del 4.3 (0 las siguientes) en ~5 s, pico ~0,42–0,43 GB con Python 3.9 + pyarrow 7. AVISO si las cifras difieren (otra foto de Trusted); FALLA si la versión no es 1.1 o no hay 16 comprobaciones |
| L2 | Segunda corrida: `escritos 0 · sin cambios 189 · archivos cambiados 0 · borrados 0` |
| L3 | Las 9 tablas con las columnas y tipos de `contracts/athena/curated.sql` (aristas: `string`, `string`, `bigint` → int64) |
| L4 | `15395 pares usuario–marca con los mismos pesos (Σ 233395 = menciones del job 233395) · 136 pares de hashtags con los mismos pesos (Σ 44578) · 25 hashtags · 221629 filas de Trusted`. Las diferencias se cuentan sin mostrar `user_hash` |

Manual: `python project/src/lakehouse/glue_jobs/trusted_to_curated.py --lake_bucket ta-dev-648617469176-lake
--ta_local_src ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake --ta_local_dst ~/.cache/trendanalyzer/quality/trusted_to_curated_local`
(imprime tres líneas JSON; la última, `glue_job_done`, trae `mentions`, `cooccurrences` y `hashtags_distinct`).

## Parte C · Terraform (infra/aws)

```bash
python project/scripts/validate_4_4.py terraform            # validate.sh + init + plan (no aplica)
python project/scripts/validate_4_4.py terraform --apply    # aplica (pide confirmación; --yes la omite)
```

El plan va limitado con `-target=module.glue_workflow` (el catálogo, la proyección y las consultas guardadas no
cambian). El validador lee el plan en JSON y **no aplica** si crea, destruye o cambia algo distinto del script y
del job de `trusted_to_curated`, o si el job cambia algo más que la descripción.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars` (AVISO si es la cuenta raíz: no afecta) |
| C0b | `backend.hcl` y `terraform.tfvars` presentes |
| C1 | `bash infra/validate.sh` en verde |
| C2a | `terraform init` |
| C2 | **0 a crear, 2 a cambiar, 0 a destruir**: `module.glue_workflow.aws_s3_object.script["trusted_to_curated"]` (`source_hash`) y `module.glue_workflow.aws_glue_job.this["trusted_to_curated"]` (`description`: «… HU-027 y HU-028, Lotes 4.3 y 4.4 … y aristas»); o «ya aplicado» |
| C3 | `Apply complete! Resources: 0 added, 2 changed, 0 destroyed.` |
| C3b | Un segundo plan: **No changes** |

Manual (desde `project/`):

```bash
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan -target=module.glue_workflow -out=../../../../evidencia/tfplan_4_4
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_4_4
```

`evidencia/tfplan_4_4*` no se versionan (`.gitignore`).

## Parte D · Corrida real del job y conciliación de las aristas

```bash
python project/scripts/validate_4_4.py run      # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| D0 | Solo aparece si el horario está `ENABLED` | AVISO: apáguelo con `off` mientras valida |
| D1 | `get-job` y baja el script de `_glue/scripts/` (`validate43.check_job`) | `pythonshell 3.9 · 0.0625 DPU (1 GB) · library-set analytics · script de S3 idéntico al repo`. FALLA «aplique C» si sigue el script 1.0 |
| D2 | Cuenta las filas de Trusted por los pies (543 GET) y lista las nueve tablas | `trusted/posts/: 543 objetos y 221629 filas · curated/ (nueve tablas): 187 objetos, aristas 0` la primera vez |
| D3 | `start-job-run` (con `--ta_validacion 4.4`), espera y reintenta si Glue cuenta otra corrida | `SUCCEEDED` · segundos de ejecución · `0.0625 DPU`. Anote la duración |
| D4 | La línea `glue_job_done` | `leídas 221629 · fact_posts 221629 · edges_user_brand 15395 (233395 menciones) · edges_hashtag_cooccurrence 136 (44578 coocurrencias, 25 hashtags)` · tiempos · **CPU** · **pico de memoria de 1024 MB** (AVISO si pasa de 800) · versiones |
| D5 | Lee el reporte de la corrida y `quality/_latest/trusted_to_curated.json` | Los dos con la misma corrida, **`trusted-to-curated@1.1`**, esquema `ta-trusted-to-curated-report@1` |
| D6 | Conciliación (16 comprobaciones; 5 de aristas) | `Trusted 221629 (pies de S3) = leídas 221629 = fact_posts 221629`; `edges_user_brand: Σ weight 233395 = menciones 233395 (15395 pares; por marca {pepsi 69434, postobon 103436, sprite 60525}) · llaves en dim_user y dim_brand OK · filas con usuario sin marcas 0`; `edges_hashtag_cooccurrence: Σ weight 44578 = Σ C(k, 2) 44578 (136 pares, 25 hashtags; filas por nº de hashtags {0: 132578, 1: 55725, 2: 27700, 3: 5626}) · hashtag_a < hashtag_b en todas`. FALLA si una comprobación no cuadra o no hay 16 (script viejo) |
| D7 | Cuenta las nueve tablas por los pies, revisa el layout y qué cambió | `187 → 189 objetos (189 en el reporte)` · filas por tabla = el reporte · **`archivos cambiados 2 (solo aristas)`** · `escritos 2, sin cambios 187, borrados 0` la primera vez (0 cambiados si una corrida del horario ya lo había escrito) · `curated/_ta_placeholder/: 0 → 0`. FALLA si cambió un archivo de las siete tablas del 4.3 |
| D8 | Baja el Parquet más chico de cada tabla y compara su esquema | `9 tablas con las columnas y tipos de contracts/athena/curated.sql` |

Manual:

```bash
aws glue start-job-run --region us-east-1 --job-name ta-job-trusted-to-curated
aws glue get-job-run --region us-east-1 --job-name ta-job-trusted-to-curated --run-id <jr_…> --query 'JobRun.[JobRunState,ExecutionTime,MaxCapacity,ErrorMessage]'
aws logs filter-log-events --region us-east-1 --log-group-name /aws-glue/python-jobs/output --log-stream-name-prefix <jr_…> --filter-pattern '"glue_job_done"'
aws s3 cp s3://ta-dev-648617469176-lake/quality/_latest/trusted_to_curated.json - | python -c "import json,sys; r=json.load(sys.stdin); print(r['version'], r['output']['changed_keys'], {k: v for k, v in r['checks'].items() if 'edge' in k})"
aws s3 ls s3://ta-dev-648617469176-lake/curated/ --recursive | grep edges_
```

## Parte E · Athena: las aristas

```bash
python project/scripts/validate_4_4.py athena
```

| Paso | Consulta (`validate44.*_SQL`) | Esperado |
|---|---|---|
| E1 | `UB_SQL`: `edges_user_brand` frente a lo recalculado desde `fact_posts` con `CROSS JOIN UNNEST(array_distinct(brands))` | `15395 pares, 0 claves o pesos distintos · Σ weight 233395 = menciones 233395 = Σ cardinality(brands) 233395 (= reporte)` |
| E2 | `LINKS_SQL`: llaves foráneas, nulos, orden y unicidad | `0 aristas sin su usuario en dim_user o su marca en dim_brand · 0 nulos o pesos < 1 · 0 pares fuera de orden, no canónicos (^[a-z0-9_]{1,100}$) o repetidos` |
| E3 | `HT_SQL`: `edges_hashtag_cooccurrence` frente a lo recalculado desde **`ta_trusted.posts`** (autounión por `event_id` con `a.h < b.h`; `fact_posts` no tiene `hashtags`) | `136 pares, 0 claves o pesos distintos · Σ weight 44578 = recalculadas 44578 = Σ C(k, 2) por fila 44578 · 25 hashtags` |
| E4 | `EXAMPLE_SQL`: los 10 pares de hashtags más frecuentes y los usuarios por nº de marcas que mencionan (sin `user_hash`) | `par de hashtags más frecuente: colombia + gaseosa (3898) · usuarios por nº de marcas mencionadas: {1: 479, 2: 1455, 3: 4002}` (medido en local) |
| E5 | Bytes escaneados | Unos pocos MB en 5 consultas, la mayor muy por debajo de 1 GiB; ≈ 0,0003 USD (mínimo 10 MB por consulta) |
| E6 | Latencia de las consultas de ejemplo | OK si las dos tardan ≤ 3.000 ms en total; AVISO si alguna pasa (el KPI de < 3 s es del dashboard con SPICE, Lote 6.1) |

Manual (o en la consola de Athena, workgroup `ta-analytics`, base `ta_curated`):

```bash
aws athena start-query-execution --region us-east-1 --work-group ta-analytics \
  --query-execution-context Database=ta_curated \
  --query-string "SELECT hashtag_a, hashtag_b, weight FROM ta_curated.edges_hashtag_cooccurrence ORDER BY weight DESC LIMIT 10"
aws athena get-query-results --region us-east-1 --query-execution-id <id>
```

## Parte F · Segunda corrida dentro del workflow: idempotencia y costo

```bash
python project/scripts/validate_4_4.py rerun    # pide confirmación (--yes la omite)
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | Lista las nueve tablas, lee el último reporte y cuenta en Athena (`COUNT_SQL`) | `189 objetos · reporte <corrida de D> (trusted-to-curated@1.1) · Athena: fact_posts 221629, edges_user_brand 15395 (Σ 233395), edges_hashtag_cooccurrence 136 (Σ 44578) · horario DISABLED`. FALLA si D no se corrió (sin aristas) |
| F2 | `start-workflow-run` (`ta_validacion=4.4`) y espera | `COMPLETED · ta-job-raw-to-trusted SUCCEEDED (~90 s) → ta-job-trusted-to-curated SUCCEEDED` |
| F3 | El reporte de esta corrida | `archivos cambiados 0 · escritos 0 · borrados 0 · mismos sha256 que la corrida anterior en 189 archivos (2 de aristas) · menciones 233395 · coocurrencias 44578 · Trusted de la misma corrida: 221629 filas (coincide: True)` |
| F4 | Lista otra vez | `189 → 189 objetos`, mismas claves y tamaños |
| F5 | Athena otra vez | Mismas filas y pesos que F1 |
| F6 | `get-workflow-run-properties` | `ta_trusted_report …, ta_trusted_rows = 221629, ta_curated_report …, ta_curated_rows = 221629, ta_curated_files_changed = 0` |
| F7 | Marcas, corridas y horario | `curated/_ta_placeholder/: 0 → 0 · corridas en curso: ninguna · horario DISABLED → DISABLED (al terminar: … on --yes)` |
| F8 | Costo con lo medido | Como el 4.3: ≈ 0,0196 USD por corrida del workflow, ≈ 0,47 USD/día, ≈ 14,1 USD/mes (el job sigue por debajo del mínimo de 60 s cobrados; las aristas suman 2 PUT solo cuando cambian) |

Manual:

```bash
aws glue start-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-properties ta_validacion=4.4
aws glue get-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…> --include-graph --query 'Run.[Status,Statistics]'
aws glue get-workflow-run-properties --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…>
```

## Final: horario encendido (estado normal)

```bash
python project/scripts/validate_4_4.py on --yes   # muestra el costo por día (el de F8 si existe)
python project/scripts/validate_4_4.py status
python project/scripts/status.py                  # lo demás que cobra (GCP y local)
```

| Paso | Esperado |
|---|---|
| N1 | `DISABLED → ENABLED; se apaga con: python project/scripts/validate_4_4.py off` |
| N2 | `ENABLED · rate(1 hour) · UTC · reintentos 0` |
| S1 | `ENABLED (estado normal por decisión del usuario: cada hora corren Trusted y Curated con las aristas)` |
| S2 | Ninguna corrida ni job run en curso (la de la hora siguiente puede aparecer `RUNNING`: es normal) |
| S3 | `curated/: 189 objetos {fact_posts: 181, …, edges_user_brand: 1, edges_hashtag_cooccurrence: 1}` y el último reporte (`trusted-to-curated@1.1`, aristas, menciones y coocurrencias) |
| S4, S5 | Ningún horario de prueba; `curated/_ta_placeholder/`: 0 objetos |

**No** borre `curated/`: el 5.2 y el 6.1 lo leen. Si hiciera falta dejar el lakehouse sin cobrar:
`python project/scripts/validate_4_4.py off` (decisión del usuario; hoy el estado normal es encendido).

## Después de esta validación

- **Cierre del 4.4 (v2.40):** anotar en `ARCHITECTURE.md` §3.23 el tiempo, la CPU y la memoria en Glue con las
  aristas (D3–D4), lo de Athena (E1–E6) y el costo (F8); en esta guía, la tabla de cierre; PLAN §3 (4.4 y la
  Fase 4) y HU-028.

## Cierre

*(Se completa en v2.40.)*

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` antes de imprimir nada | La carpeta quedó inválida en `/mnt/c` tras copiar desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A1 `Python 3.14…` o falta `pyarrow` | Está en PowerShell o sin el venv: WSL, `source ~/.venvs/ta-streaming/bin/activate`. |
| A6 / D0 AVISO por el horario `ENABLED` | Es el estado normal, pero mientras valida: `validate_4_4.py off` (Preparación, paso 6); al final, `on --yes`. |
| L0 AVISO «no hay trusted/posts/» | `python project/scripts/profile_data.py sync --layer trusted` (solo lee S3) y repita. |
| L4 FALLA «N faltan / sobran / con otro peso» | El job y el recálculo con pyarrow no coinciden: páseme el detalle (no trae `user_hash`) y la línea `glue_job_done` de L1. |
| C2 FALLA «no aplique» | El plan cambia algo fuera del script y la descripción del job (el detalle muestra cada atributo `antes → después`). No aplique; páseme el detalle. |
| D1 «el script de S3 no es …» | Falta aplicar C: `validate_4_4.py terraform --apply`. |
| D5/D6 FALLA «versión 1.0» o «11 comprobaciones (¿script viejo?)» | La corrida usó el script del 4.3: aplique C y repita D. |
| D6 FALLA en `edges_*` o `fk_edge_*` | Una arista no concilia: el job no escribió nada (falla antes); páseme el detalle de D6 y el traceback de `/aws-glue/python-jobs/error`. |
| D7 FALLA «cambiaron tablas del 4.3» | Cambió Trusted entre la corrida anterior y esta (¿el exportador?) o el job dejó de ser determinista: páseme `changed_keys` del reporte. |
| E1 «Σ weight … ≠ menciones del reporte» | Athena ve otra foto que el reporte (¿corrió el horario entre D y E?): repita D y E con el horario apagado. |
| E3 con diferencias | Páseme el detalle; compare con L4 (si L4 da OK, el problema es de lectura en Athena o de otra foto de Trusted). |
| F3 FALLA «archivos cambiados N» | Trusted cambió entre D y F (exportador) o el job no es determinista: páseme `changed_keys`. |
| El horario quedó `DISABLED` al terminar | `python project/scripts/validate_4_4.py on --yes` (estado normal: encendido). |
