# Validación del Lote 4.1 · HU-007, HU-029

> Lakehouse de AWS: catálogo de Glue (`ta_raw`, `ta_trusted`, `ta_curated` con las tablas de
> `contracts/athena/` y partition projection), workgroup de Athena `ta-analytics` con límite de bytes,
> roles `ta-glue-role` y `ta-quicksight-role`, workflow de Glue `ta-wf-raw-to-curated` (dos jobs que en
> este lote son **marcadores de posición**), horario de EventBridge Scheduler `rate(1 hour)` creado
> **deshabilitado** (estado de ahorro) y fallos de los jobs en CloudWatch. Contrato en
> `docs/ARCHITECTURE.md` §3.19 (interfaces en §3.19.1), §5.7 y §5.8.
>
> **Automático:** `python project/scripts/validate_4_1.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_4_1.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-007 · Bucket con `raw/`, `trusted/`, `curated/`, `quality/`, `athena-results/`, cifrado con KMS | D4 (y E5 para `athena-results/`) |
| HU-007 · Bases `ta_raw`, `ta_trusted`, `ta_curated` con tablas con partition projection | C2–C3, D1–D2, E1, E6 |
| HU-007 · Workgroup `ta-analytics` con límite de bytes por consulta | D3, E4 |
| HU-007 · `ta-glue-role`, `ta-historical-loader`, `ta-quicksight-role` con mínimo privilegio | D5–D9 |
| HU-007 · Outputs `lake_bucket`, `glue_db_names`, `athena_workgroup`, `exporter_role_arn`, `glue_role_arn`, `kms_key_arn` | C4 |
| Demostración: `SELECT` en Athena sobre `ta_raw.posts_raw` con los dos orígenes, sin pasar del límite | E1–E4 |
| HU-029 · EventBridge `rate(1 hour)` dispara `ta-wf-raw-to-curated` | F2 (configuración), F8 (un horario de prueba con el mismo rol y destino arranca el workflow) |
| HU-029 · El workflow corre en orden `ta-job-raw-to-trusted` → `ta-job-trusted-to-curated` | F1, F4–F5 |
| HU-029 · Los fallos quedan registrados en CloudWatch | F3, F6–F7 |
| Estado de ahorro: el horario se crea apagado y se vuelve a apagar | F2, F10, `off` |

## Antes de empezar

1. Copie los archivos del lote en sus rutas (`project/…` y `docs/…`). No cambia archivos de lotes
   anteriores salvo un comentario de `project/contracts/athena/posts.sql`.
2. Terminal en WSL, **entrando de nuevo a la carpeta** (tras copiar desde Windows la terminal puede
   quedar en una carpeta "fantasma" y Python falla con `FileNotFoundError`):
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   export AWS_PROFILE=default
   aws sts get-caller-identity --query Account --output text
   ```
   Esperado: `648617469176`. El venv ya trae `boto3` y `pyarrow`, lo único que usa el lote.
3. `project/infra/aws/environments/dev/backend.hcl` y `terraform.tfvars` (no se versionan; ya existen
   desde el 3.1) y `gcp_exporter.auto.tfvars` del 3.2 (sin él, un plan **sin** `-target` propondría
   destruir `ta-gcp-exporter-role`; el validador usa `-target` y no lo toca).
4. No corra otra validación que use el estado de Terraform de `infra/aws` a la vez (bloqueo de DynamoDB).

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada | Nube, Docker | 0 |
| C | Perfil de AWS | GCP, Docker, simulador, Dataflow | 0 al aplicar (recursos sin costo fijo) |
| D | Lo de C aplicado | GCP, Docker | 0 (lecturas y 36 simulaciones de IAM) |
| E | Lo de C aplicado; datos en `raw/` (3.1 y 3.2) | GCP, Docker, **el exportador** (si sube objetos durante E, E3 no concilia) | ~13 consultas de Athena (≈ 0,001 USD) y ~1.200 GET de S3 (≈ 0,0005 USD; ~12.000 más con `--reconcile-historical`) |
| F | Lo de C aplicado | GCP, Docker | 3 corridas del workflow (5 job runs Python shell, ≈ 0,003 USD en total) |

Compruebe el estado con `python project/scripts/status.py`, `python project/scripts/validate_3_2.py status`
y `python project/scripts/validate_4_1.py status`. **Este lote no necesita nada de GCP**: si el simulador
o Dataflow están encendidos, apáguelos (`python project/scripts/off.py`); el Scheduler del exportador debe
seguir en `PAUSED` (`validate_3_2.py off`) para que `raw/origin=stream/` no cambie durante E.

**Estado de ahorro del lote:** el horario `ta-wf-raw-to-curated-cada-hora` se crea **`DISABLED`**. F no
lo enciende: prueba el disparo con un horario de prueba de una sola vez que se borra solo. Los jobs no
cobran si no corren.

---

## Parte A · Preflight del código y de lo encendido

```bash
python project/scripts/validate_4_1.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows/PowerShell: repita desde WSL. Las demás partes ni arrancan fuera de WSL (salen con 2) |
| A1 | Python 3.11; `boto3`, `botocore`, `pyarrow` |
| A2 | Los 25 archivos del lote presentes |
| A3 | Los 13 archivos de lotes anteriores que usa (raíz de `infra/aws`, `lake.tf`, `exporter.tf`, `contracts/athena/*.sql`, `common/enriched_row.py`, `ops/services.py`, `ops/validate32.py`, `status.py`, `validate.sh`) |
| A4 | `tables.json` al día con `contracts/athena/` (11 tablas) |
| A5 | `terraform` (con versión), `aws`, `bash`; `gcloud` opcional (solo para A7) |
| A6 | Horario `NO_EXISTE` (antes de C) o `DISABLED`; ninguna corrida del workflow en curso. AVISO si está `ENABLED` |
| A7 | Lo que dice `services.status` (como `status.py`): nada `ENCENDIDO`. AVISO si el simulador o Dataflow cobran |
| A8 | `gcp_exporter.auto.tfvars` presente (AVISO si falta) |

Manual: `cd project/src && python -m lakehouse.catalog_spec --check` (esperado `tables.json al día`) y
`aws scheduler get-schedule --name ta-wf-raw-to-curated-cada-hora --region us-east-1` (esperado
`ResourceNotFoundException` antes de C).

## Parte B · Pruebas

```bash
python project/scripts/validate_4_1.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/lakehouse -q -rs` | **50 passed, 0 skipped** (incluye `terraform fmt` de los archivos del lote y `common.enriched_row` real) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate41.py -q` | **48 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/historical/tests/test_hist_infra.py src/exporter/tests/test_exp_infra.py -q` | **27 passed** (las raíces del 3.1 y 3.2 siguen encajando) |

Si B1 tiene una omitida, la línea `SKIPPED` dice qué falta (`terraform` en el PATH o `pyarrow`). Si falla
`test_tables_json_al_dia_con_las_ddl`, alguien cambió `contracts/athena/` sin regenerar:
`cd project/src && python -m lakehouse.catalog_spec --write` y repita (es lo que debe ir en el commit).

## Parte C · Terraform (infra/aws)

```bash
python project/scripts/validate_4_1.py terraform            # validate.sh + init + plan (no aplica)
python project/scripts/validate_4_1.py terraform --apply    # aplica (pide confirmación)
```

El plan va limitado con `-target` a los 8 bloques del lote (`module.glue_catalog`, `aws_s3_object.lake_layer`,
`module.athena`, `module.glue_role`, `module.quicksight_role`, `module.glue_workflow`,
`module.lakehouse_schedule`, `module.glue_failure_events`). El validador lee el plan en JSON y **no
aplica** si cambia cualquier otra cosa.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars`. AVISO si es la cuenta raíz: aquí **no afecta** (los roles del lote los asumen Glue, Scheduler y QuickSight, no su perfil) |
| C0b | `backend.hcl` y `terraform.tfvars` presentes |
| C1 | `bash infra/validate.sh` en verde |
| C2a | `terraform init -backend-config=backend.hcl` (instala los 7 módulos nuevos) |
| C2 | **40 a crear, 0 a cambiar, 0 a destruir** (3 bases, 11 tablas, 3 carpetas, workgroup + 2 consultas, 3 roles + 3 políticas, horario, 2 scripts, 2 jobs, workflow, 2 disparadores, 2 grupos de logs de Glue, grupo + regla + destino + política de fallos), o "ya aplicado" |
| C3 | `Apply complete! Resources: 40 added, 0 changed, 0 destroyed.` |
| C3b | Un segundo plan con los mismos `-target`: **No changes** (si una tabla de Glue o el horario dejan deriva, FALLA con el atributo antes → después) |
| C4 | Outputs: `glue_db_names` = `{curated = "ta_curated", raw = "ta_raw", trusted = "ta_trusted"}`, `athena_workgroup` = `ta-analytics`, `athena_bytes_scanned_cutoff` = `1073741824`, `glue_role_arn`, `quicksight_role_arn`, `glue_workflow_name`, `glue_job_names`, `lakehouse_schedule_name`, `lakehouse_scheduler_role_arn`, `glue_failures_log_group`, y de lotes anteriores `lake_bucket`, `kms_key_arn` y `exporter_role_arn` |

Manual (desde `project/`):

```bash
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan -target=module.glue_catalog -target=aws_s3_object.lake_layer \
  -target=module.athena -target=module.glue_role -target=module.quicksight_role -target=module.glue_workflow \
  -target=module.lakehouse_schedule -target=module.glue_failure_events -out=../../../../evidencia/tfplan_4_1
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_4_1
terraform -chdir=infra/aws/environments/dev output
```

`evidencia/tfplan_4_1*` pueden contener valores de `terraform.tfvars`: no los versione.

## Parte D · Catálogo, workgroup, carpetas e IAM

```bash
python project/scripts/validate_4_1.py catalog
```

| Paso | Esperado |
|---|---|
| D1 | `ta_raw`, `ta_trusted`, `ta_curated` con `LocationUri` = `s3://ta-dev-648617469176-lake/{raw,trusted,curated}/` |
| D2 | Las 11 tablas de Glue = las DDL de `contracts/athena/` (columnas y tipos en orden, particiones, ubicación, Parquet, cada `projection.*`); proyección en `ta_raw.posts_raw`, `ta_trusted.posts`, `ta_curated.fact_posts`. AVISO si hay tablas que no están en `contracts/` |
| D3 | `ta-analytics`: `ENABLED`, configuración forzada, límite **1073741824 B (1,00 GiB)**, resultados en `athena-results/` con SSE_KMS y la llave del lake, motor v3, las 2 consultas guardadas |
| D4 | `raw/`, `trusted/`, `curated/`, `quality/` existen; cifrado por defecto `aws:kms` con la llave del lake |
| D5 | Los 3 roles con confianza solo de su servicio (`glue`, `quicksight`, `scheduler`) y **exactamente** las acciones de §5.8 (sin comodines) |
| D6 | `ta-glue-role`, **20 de 20**: permitidos leer `raw/`, listar `raw/`, escribir `trusted/`, borrar `curated/`, escribir `quality/`, leer `_glue/scripts/`, `kms:Decrypt`/`GenerateDataKey` vía S3, `glue:GetTable`, `GetWorkflowRunProperties`, `PutLogEvents` en el grupo `/aws-glue/python-jobs/output`; denegados escribir `raw/origin=stream/`, borrar `raw/origin=historical/`, escribir `athena-results/`, listar `export-state/`, KMS sin S3, `glue:CreateTable`, `glue:DeleteTable`, `glue:StartWorkflowRun`, `PutLogEvents` en `/aws/events/ta-glue-job-fallos`. Los logs se simulan con el ARN del **grupo**: con el de un flujo el simulador da `implicitDeny` aunque la política permita `*` (la escritura real la prueba F5). Cada caso queda en `data.cases` (recurso, contexto, esperado, decisión) |
| D7 | `ta-quicksight-role`, **13 de 13**: permitidos consultar en `ta-analytics`, `GetTable` de `ta_curated`, leer `curated/`, escribir `athena-results/`, listar `curated/`, KMS vía S3; denegados consultar en `primary`, borrar el workgroup, `GetTable` de `ta_raw`, leer `raw/` y `trusted/`, escribir `curated/` |
| D8 | `ta-lakehouse-scheduler-role`, **3 de 3**: `StartWorkflowRun` solo de `ta-wf-raw-to-curated` |
| D9 | `ta-historical-loader` y `ta-gcp-exporter-role` siguen con `s3:PutObject`, `s3:ListBucket` y `kms:GenerateDataKey` (AVISO si cambiaron) |

Manual:

```bash
aws glue get-tables --database-name ta_raw --region us-east-1 --query 'TableList[].[Name,StorageDescriptor.Location]'
aws athena get-work-group --work-group ta-analytics --region us-east-1
aws iam get-role-policy --role-name ta-glue-role --policy-name ta-glue-role-lake
aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::648617469176:role/ta-glue-role \
  --action-names s3:PutObject --resource-arns arn:aws:s3:::ta-dev-648617469176-lake/raw/origin=stream/x.parquet
```

## Parte E · Athena: `SELECT` sobre `ta_raw.posts_raw` con los dos orígenes

```bash
python project/scripts/validate_4_1.py athena          # --reconcile-historical para contar también los 12.102 Parquet del histórico
```

| Paso | Qué hace | Esperado |
|---|---|---|
| E1 | Corre `src/lakehouse/queries/raw_conteo_por_origen.sql` en `ta-analytics` (filas y `event_id` distintos por origen y fuente, con `desde`/`hasta`) | `SUCCEEDED` con filas de **historical y stream**. La proyección enumera ~43.000 particiones: **≈ 72 s de motor** (medido en la primera validación: 72,4 s) |
| E2 | Histórico | **50.000** filas y 50.000 `event_id` distintos (facebook 16.667, instagram 16.666, reddit 16.667). Con `--reconcile-historical`, además = los pies de los 12.102 Parquet (medido antes de entregar: 50.000 en 84 s) |
| E3 | Stream: suma `num_rows` de los pies de los Parquet de `raw/origin=stream/` (boto3, su perfil) y compara por fuente con Athena | Iguales. Medido antes de entregar: **1.155 objetos, 171.637 filas** (facebook 57.224, instagram 57.281, reddit 57.132) en 10 s; si el exportador subió algo después, cambian los dos lados por igual. Los `event_id` distintos pueden ser menos que las filas: Raw no deduplica (lo hace Trusted, 4.2) |
| E4 | Bytes escaneados de E1 | ≤ 1.073.741.824 (medido: **12,7 MB**, el 1,2 % del límite; Athena cobra mínimo 10 MB) |
| E5 | `head-object` del resultado de E1 | En `athena-results/…csv`, `aws:kms` con la llave del lake |
| E6 | `SELECT COUNT(*)` de las otras 10 tablas | Las 10 `SUCCEEDED` con 0 filas (Trusted y Curated llegan con 4.2 y 4.3) |
| E7 | La consulta guardada `ta-raw-sentimiento-7-dias` | `SUCCEEDED`; AVISO si no hay datos de los últimos 7 días (el stream no corrió esa semana) |

Manual (o en la consola de Athena, workgroup `ta-analytics`, consulta guardada `ta-raw-conteo-por-origen`):

```bash
aws athena start-query-execution --region us-east-1 --work-group ta-analytics \
  --query-execution-context Database=ta_raw \
  --query-string "SELECT part_origin, part_source, COUNT(*) FROM ta_raw.posts_raw GROUP BY 1, 2"
aws athena get-query-execution --region us-east-1 --query-execution-id <id> --query 'QueryExecution.[Status.State,Statistics.DataScannedInBytes]'
aws athena get-query-results --region us-east-1 --query-execution-id <id>
```

## Parte F · Workflow de Glue, fallos en CloudWatch y horario

```bash
python project/scripts/validate_4_1.py workflow        # --no-tick para saltar el horario de prueba (F8)
```

Pide confirmación una vez antes de correr el workflow (`--yes` la omite). Tarda ~10–20 min. Si la corta
con Ctrl+C, corra `validate_4_1.py off`.

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | `get-trigger`, `get-job` y baja los dos scripts de `_glue/scripts/` | `ta-wf-raw-to-curated-inicio` (`ON_DEMAND`) → `ta-job-raw-to-trusted`; `ta-wf-raw-to-curated-trusted-to-curated` (`CONDITIONAL`, `ACTIVATED`, `SUCCEEDED`) → `ta-job-trusted-to-curated`; jobs `pythonshell` con `ta-glue-role` y 0 reintentos; scripts de S3 idénticos a `src/lakehouse/glue_jobs/` |
| F2 | `get-schedule` | `DISABLED`, `rate(1 hour)` UTC, destino `arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun` con `{"Name": "ta-wf-raw-to-curated"}`, rol `ta-lakehouse-scheduler-role`, 0 reintentos, ventana flexible `OFF` |
| F3 | Regla y grupo de fallos | `ta-glue-job-fallos` `ENABLED` con `Glue Job State Change` y `FAILED`…; destino `/aws/events/ta-glue-job-fallos` (30 días); política `ta-glue-job-fallos-events` |
| F4 | `start-workflow-run` y espera | `COMPLETED`: los dos jobs `SUCCEEDED`, el segundo arrancó después de que terminó el primero; anote la duración de cada job |
| F5 | Logs y marcas | En `/aws-glue/python-jobs/output`, `glue_placeholder_done` de los dos (el primero cuenta los ~13.257 objetos de `raw/` y lee `PAR1`; el segundo **lee la marca del primero**); en S3, las dos marcas `_ta_placeholder/{job}/{corrida}.json` |
| F6 | Corrida con `ta_force_fail=raw_to_trusted`. Como F4, primero espera a que no haya corridas `RUNNING` y, si Glue responde `ConcurrentRunsExceededException` (tarda unos segundos en liberar el cupo de la corrida anterior), reintenta cada 20 s hasta 9 veces | `ta-job-raw-to-trusted` `FAILED` y `ta-job-trusted-to-curated` **no corre**. Puede decir «arrancó al intento N»: es normal |
| F7 | CloudWatch | Un evento `FAILED` de ese job run en `/aws/events/ta-glue-job-fallos` (EventBridge tarda hasta un par de minutos) y el fallo en los logs del job (traceback en `/aws-glue/python-jobs/error`). AVISO si solo aparece el evento. **OMITIDO** (con el motivo) si F6 no pudo arrancar la corrida |
| F8 | Espera a que no haya corridas en curso y crea `ta-wf-raw-to-curated-validacion-…` con `at(…)` (≥ 90 s después), el **mismo destino y rol** que el horario y `ActionAfterCompletion = DELETE` | Una corrida nueva arranca sola, sus dos jobs terminan bien y el horario de prueba **se borra solo** |
| F9 | Borra `trusted/_ta_placeholder/` y `curated/_ta_placeholder/` | Las marcas de F4 y F8 borradas (y las 2 de la corrida que disparó el primer `apply`, `wr_0ab6…`, si siguen ahí) |
| F10 | `get-schedule` | Sigue `DISABLED` |
| F11 | Costo con lo medido | ≥ 60 s cobrados × 0,0625 DPU × 0,44 USD/DPU-h por job → ≈ 0,0009 USD por corrida y ≈ 0,022 USD/día con el horario encendido (con los marcadores de posición) |

Manual (desde la raíz):

```bash
aws glue start-workflow-run --region us-east-1 --name ta-wf-raw-to-curated
aws glue get-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-id <wr_…> --include-graph \
  --query 'Run.[Status,Statistics]'
aws glue start-workflow-run --region us-east-1 --name ta-wf-raw-to-curated --run-properties ta_force_fail=raw_to_trusted
aws logs filter-log-events --region us-east-1 --log-group-name /aws/events/ta-glue-job-fallos --start-time <ms>
aws s3 rm s3://ta-dev-648617469176-lake/trusted/_ta_placeholder/ --recursive
aws s3 rm s3://ta-dev-648617469176-lake/curated/_ta_placeholder/ --recursive
```

## Apagado

```bash
python project/scripts/validate_4_1.py off        # deshabilita el horario, detiene corridas, borra horarios de prueba y marcas
python project/scripts/validate_4_1.py status     # esperado: horario DISABLED, nada en curso, sin horarios de prueba
python project/scripts/status.py                  # lo demás que cobra (GCP y local)
```

| Paso | Esperado |
|---|---|
| O1 | Horario `DISABLED` (lo deshabilita si estaba `ENABLED`, reenviando su definición completa) |
| O2 | Ninguna corrida ni job run en curso (detiene los que haya) |
| O3 | Ningún horario `ta-wf-raw-to-curated-validacion-*` |
| O4 | Marcas `_ta_placeholder/` borradas |
| O5 | `services.status`: nada `ENCENDIDO`; AVISO con el comando para apagarlo si algo cobra |

Lo que queda creado y no cobra por tiempo: el catálogo, el workgroup, los roles, el workflow y sus jobs, la
regla de fallos y los grupos de logs (centavos). Para encender el horario de forma permanente (cuando
existan 4.2 y 4.3): `python project/scripts/validate_4_1.py on` (pide confirmación; `off` para volver).

## Cierre

**Cerrado en v2.34** (26-oct-2026, WSL, perfil `default` de la cuenta 648617469176). Lo corrió el maestro
del lote; las correcciones de D6 y F6 están en `CHANGELOG.md` v2.32.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (8 OK); A6 «NO_EXISTE» antes del apply |
| B · Pruebas | OK: B1 50 sin omitidas, B2 48, B3 18, B4 27 |
| C · Terraform | AVISO solo por C0 (cuenta raíz): C2 40 a crear, 0 a cambiar, 0 a destruir; C3 «Apply complete! Resources: 40 added»; C3b sin deriva; C4 los outputs de §5.9 |
| D · Catálogo e IAM | OK (9 OK): 11 tablas = DDL, workgroup forzado con 1 GiB, D6 20/20, D7 13/13, D8 3/3, roles de 3.1 y 3.2 intactos |
| E · Athena | OK (7 OK): histórico 50.000 (16.667 / 16.666 / 16.667); stream 171.637 = pies de los 1.155 Parquet; 12,7 MB escaneados y 72,4 s de motor en E1; las otras 10 tablas en 0 filas |
| F · Workflow | OK (11 OK): F4 en orden (16 s y 11 s); F6 fallo provocado al intento 2 y el segundo job no corrió; F7 evento `FAILED` en `/aws/events/ta-glue-job-fallos`; F8 corrida creada por el horario de prueba; F11 ≈ 0,00092 USD por corrida |
| `off` / `status` | OK: horario `DISABLED`, nada en curso, sin marcas ni horarios de prueba |

Nota: el primer `apply` corrió una vez el workflow (disparador `ON_DEMAND` con `enabled = true`); se
corrigió en el módulo `glue_workflow` sin cambios en el plan. Cifras también en `ARCHITECTURE.md` §3.19.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` en `from pathlib import Path` antes de imprimir nada | La carpeta actual quedó inválida en `/mnt/c` tras copiar archivos desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A0/A1 `Python 3.14…` o faltan `boto3`/`pyarrow` | Está en PowerShell (Windows), no en WSL. Abra WSL, entre a la carpeta, `source ~/.venvs/ta-streaming/bin/activate` y repita. |
| A4 / B1 `tables.json` desactualizado | `contracts/athena/` cambió: `cd project/src && python -m lakehouse.catalog_spec --write`, revise el `git diff` de `tables.json` y repita C (cambia esas tablas). |
| C2 FALLA "no aplique" | El plan cambia algo fuera del lote (el detalle muestra cada atributo `antes → después`). **No aplique**; revise con `git status` qué `.tf` cambió y páseme el detalle. |
| C3 FALLA con `Failed to save state` / C dice "hay un estado sin guardar" | Como en el 3.2: desde `project/`, `terraform -chdir=infra/aws/environments/dev state push errored.tfstate`, `state list` y mueva `errored.tfstate` fuera del repo; luego repita C. |
| C3 falla al crear un job (`GlueVersion` o `Python version`) | Glue no aceptó el job Python shell sin versión: páseme el error (el ajuste va en el módulo `glue_workflow` o en `glue_job_settings`). |
| C3b FALLA (deriva tras aplicar) | Un recurso no queda como lo declara Terraform (p. ej. una tabla de Glue). Páseme el detalle `antes → después`; no vuelva a aplicar a ciegas. |
| C2a `Backend configuration changed` | `terraform -chdir=project/infra/aws/environments/dev init -reconfigure -backend-config=backend.hcl`. |
| D2 FALLA en una tabla | Alguien la cambió a mano (consola de Glue o un `CREATE TABLE` en Athena) o `contracts/` cambió sin aplicar: repita C. |
| E1 `FAILED` con `HIVE_BAD_DATA` o de tipos | Un Parquet de `raw/` no tiene el esquema de `posts_enriched` (p. ej. un tipo de marca de tiempo): páseme el `reason` y la clave. |
| E1 `CANCELLED` por el límite | La consulta pasó de 1 GiB: no debería con los datos de hoy (~183 MB en `raw/`); páseme los bytes de E4. |
| E1 tarda más de ~2 min | Lo normal es ≈ 72 s de motor: la proyección enumera ~43.000 particiones de `posts_raw`. Si molesta, se acota `projection.part_dt.range` (§5.7). |
| D6 FALLA en `logs:PutLogEvents` con un ARN `…:log-stream:…` | Es el simulador, no la política (con el ARN de un flujo da `implicitDeny` aunque se permita `*`). Desde v2.32 D6 simula con el ARN del grupo; si vuelve a aparecer, revise que tenga el `validate41.py` del lote. |
| F4/F6 `ConcurrentRunsExceededException … Concurrent runs exceeded for workflow` | Glue marca la corrida anterior `COMPLETED` y libera el cupo (`MaxConcurrentRuns = 1`) unos segundos después: en la primera validación, `StartWorkflowRun` 9 s después del `CompletedOn` fue rechazado. Desde v2.32 el validador espera y reintenta (20 s × 9). Si aun así falla, hay otra corrida en curso de verdad: `validate_4_1.py status` y, si sobra, `off`. |
| Tras un `apply` aparece una corrida del workflow que nadie lanzó | Un disparador `ON_DEMAND` creado con `enabled = true` se ejecuta al crearse. Desde v2.32 el de inicio tiene `enabled = false`; las marcas que deje esa corrida las borra `off`. |
| E3 no concilia | ¿Corrió el exportador durante E (`validate_3_2.py status`)? Páuselo y repita E. Si persiste, páseme `data` de E3 (por fuente). |
| F1 FALLA "el script de S3 no es …" | El archivo local cambió después del `apply`: repita C con `--apply` (debe dar 1 a cambiar por script). |
| F4 un job `FAILED` | Mire su `ErrorMessage` en el detalle y `/aws-glue/python-jobs/error`; un `AccessDenied` apunta a `ta-glue-role` (D6 debería haberlo visto). |
| F5 sin logs | Los flujos de Python shell no se llaman como el job run, o tardan: busque a mano con `aws logs describe-log-streams --log-group-name /aws-glue/python-jobs/output --order-by LastEventTime --descending --max-items 5` y páseme los nombres. |
| F7 sin evento en `/aws/events/ta-glue-job-fallos` | Revise que exista la política `ta-glue-job-fallos-events` (`aws logs describe-resource-policies`) y el destino de la regla (F3). EventBridge puede tardar unos minutos: repita F con `--no-tick`. |
| F8 no aparece la corrida | Revise la métrica `TargetErrorCount` de `AWS/Scheduler` y D8; el validador ya borró el horario de prueba. |
| Quedó el horario encendido o una corrida en curso | `python project/scripts/validate_4_1.py off`. |
