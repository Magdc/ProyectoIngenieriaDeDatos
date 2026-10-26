# TrendAnalyzer · CHANGELOG

> Historial de versiones del proyecto: una entrada por versión, la más reciente primero. Cada entrada indica el lote que motiva los cambios.
> Fuente de verdad: [`ARCHITECTURE.md`](ARCHITECTURE.md) (versión vigente **2.34**). Las referencias `§N` de este archivo apuntan a ese documento, salvo las marcadas `PLAN.md §N`.
> Plan, trazabilidad HU y estado por lote: [`PLAN.md`](PLAN.md).

---

## v2.34 · desde v2.33

**Lote que lo motiva:** Lote 4.1 (HU-007, HU-029) · cierre; Lote 5.1 (HU-031) · validación de Raw

| Área | Cambio |
|---|---|
| Trabajo en paralelo | Los Lotes 4.1 y 5.1 se hicieron a la vez: el 4.1 en `main` (entrega v2.32) y el 5.1 en la rama `lote-5.1` (entrega v2.33, §3.20 y ADR-24), fusionada en `main`. Por eso el cierre del 4.1, que v2.32 anunciaba como v2.33, es esta v2.34. Los choques de texto (cabeceras, §0, §5.1, §5.7, §5.11, §5.12, §6, §9 y este archivo) se resolvieron conservando lo de los dos lotes. |
| Cierre del Lote 4.1 | Validado con `docs/validation/validation-4-1.md`: A OK; B 50 + 48 + 18 + 27 sin omitidas; C en AVISO solo por la cuenta raíz (40 creados, 0 cambiados, 0 destruidos, sin deriva, outputs de §5.9); D 9 OK; E 7 OK; F 11 OK; `off` y `status` OK. Correcciones durante la validación (detalle en v2.32): D6 era el simulador de IAM, F6 la liberación tardía del cupo del workflow, y el disparador `ON_DEMAND` corría el workflow al crearse. |
| Cifras del 4.1 | Athena: histórico 50.000 filas (16.667 / 16.666 / 16.667) y stream 171.637, igual a los pies de los 1.155 Parquet; E1 escanea 12,7 MB en 72,4 s de motor (la proyección enumera ~43.000 particiones). Glue: jobs de marcador de 16–19 s y 11 s; ≈ 0,00092 USD por corrida, ≈ 0,022 USD/día con el horario encendido. Queda en estado de ahorro: horario `DISABLED`. |
| Validación del Lote 5.1 | `validate_5_1.py all` con código 0 en la rama (A 7 OK; B 165 + 24 + 18 + 23; C 9 OK, 221.637 filas en 177 s; D en AVISO solo por D1, sin Trusted) y A y B en OK sobre `main` tras la fusión. Sigue 🟨: el profiling real de Trusted espera al 4.2. |
| Fase 4 | Sigue 🟨: faltan 4.2, 4.3 y 4.4. El "Listo cuando" (`SELECT` sobre `ta_curated.fact_posts`) es del 4.3. |
| Documento | §3.19 y §3.20 con la nota de validación; PLAN.md §3 (4.1 ✅, Fase 4 🟨, 5.1 🟨 validado en Raw, Fase 5 🟨); HU.md (HU-007 y HU-029 ✅, estado de HU-031); tablas de cierre en las dos guías; cabeceras a 2.34. |
| Abierto, sin bloquear | **A confirmar por el equipo:** el límite de 1 GiB de `ta-analytics` y cuándo encender el horario (hoy con marcadores de posición). **Para el 4.2:** qué hacer con las 65.264 filas sin NLP y los 6 centinelas `ta-verify@2.1` (§3.19.1 y §3.20). **Para el 5.2:** correr por primera vez en Athena los predicados SQL de las reglas de Raw de §5.10. `ta-quicksight-role` frente a QuickSight real (Lote 6.1). **Repositorio:** `project/evidencia/tfplan_*` está versionado (las guías dicen que no) y los planes de GCP llevan un correo personal; `project/.local-lake/` está versionado (§3.13 dice que no); `project/project/src/streaming/` tiene copias idénticas de `nlp.py` y `test_nlp_local.py`. `tfplan_4_1` no se versionó. *(Resuelto tras v2.34, sin reescribir el historial: `tfplan*` y `project/.local-lake/` quedan en `.gitignore` y fuera del índice, y se borraron las copias de `project/project/`. Los commits anteriores todavía contienen esos archivos.)* **Prueba inestable (de antes de estos lotes):** `src/ops/tests/test_ops_validate24.py::test_default_run_muestra_la_salida_en_vivo` falla a veces por el orden de dos líneas (`['dos', 'uno']`). |

## v2.33 · desde v2.32

**Lote que lo motiva:** Lote 5.1 (HU-031) · entrega (Raw perfilado; Trusted pendiente del Lote 4.2)

| Área | Cambio |
|---|---|
| Lote 5.1 | Entregado: `src/quality/` (`layers`, `checks`, `text_stats`, `profile_core`, `profiler`, `anomalies`, `rules`, `report_md`, `s3sync`, `publish`, `testing`, `cli`, `requirements.txt`), `scripts/{profile_data.py,validate_5_1.py}`, `src/ops/validate51.py`, `docs/validation/validation-5-1.md` y 189 pruebas (165 en `src/quality/tests/`, 24 en `src/ops/tests/test_ops_validate51.py`). Detalle en **§3.20** (interfaces en §3.20.1). |
| Profiling (HU-031) | Por capa, origen y fuente: nulos, cardinalidad exacta, tipos lógicos y físicos, distribuciones (valores frecuentes, cuantiles, longitudes, fechas por hora y día, listas), 70 comprobaciones por fila, KPI preliminares, diversidad léxica (TTR, MATTR, Herdan, distinct-2, hapax) y texto repetido (textos y esqueletos de plantilla), duplicados por `event_id` (no por texto), archivos y esquema, stream frente a histórico y anomalías. Lee una caché local de solo lectura de S3 (fuera del repo); la evidencia lleva solo agregados. |
| Profiling real de Raw | 26-oct-2026: 13.257 archivos (182,7 MB) y 221.637 filas (histórico 50.000; stream 171.637) en 168 s. Completitud y anonimización 100 % en los dos orígenes; esquema único igual al contrato; 0 anomalías de error. Hallazgos: 65.264 filas del stream sin NLP (procesadas antes de 2026-10-22T23:00Z, jobs sin NLP), 6 centinelas `ta-verify@2.1` en Raw stream, 2 `event_id` repetidos en horas distintas, `process_time` constante en el histórico, 12.102 archivos pequeños, vocabulario acotado (3.618 tipos; 99,98 % de textos distintos). Evidencia en `evidencia/profiling_raw_5_1.{json,md}`. |
| Trusted | Trusted no existe (Lote 4.2): el profiler queda listo con el contrato de §5.7 (`source` de la partición, `text_clean`, `dt = DATE(event_time)`) y se prueba con un Trusted simulado desde la caché de Raw y con 4 defectos inyectados. **Pendiente:** el profiling real cuando exista el 4.2 (`validate_5_1.py trusted` lo hace solo al encontrar objetos). |
| Reglas para HU-032 | Catálogo **DQ-01…DQ-63** en **§5.10** (capa, alcance, columna, comprobación, umbral, KPI, gravedad, expectativa de GE sugerida, predicado SQL de Athena, observado y motivo) y métricas agregadas con su SQL de referencia; en código, `quality.rules.RULES` (una prueba exige que coincidan). Hoy en Raw cumplen 55 de 56; no cumple DQ-63 (aviso). Las expectativas de GE y los SQL están **sin comprobar** (GE llega con el 5.2 y Athena con el 4.1). |
| Validador | `validate_5_1.py preflight / tests / raw / trusted / all / clean` (§8): A preflight (capas = `contracts/athena`, caché fuera del repo, `ops.services.status()`), B pruebas, C caché de S3 y profiling real de Raw (cobertura de HU-031, resultados, reglas, privacidad de la evidencia y §5.10 = código), D Trusted real o simulado con detección de defectos; `clean` borra la caché local. Solo lee de la nube. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. §5.1 (`TA_QUALITY_CACHE`), §5.7 (`quality/{run_date}/profiling/{capa}/` opcional; notas medidas de Raw histórico, Raw stream y lo que Trusted debe resolver), §5.10 (catálogo de reglas), §5.11 (fila de Calidad) y §5.12 (parámetros del profiling). Nueva **ADR-24**. |
| Documento | Nueva §3.20 con §3.20.1; ampliadas §0, §1.4, §4, §5.1, §5.7, §5.10, §5.11, §5.12, §6 y §9 (ADR-24; riesgos de texto repetido, archivos pequeños y filas sin NLP o centinelas en Raw stream); PLAN.md §3 (Lote 5.1 🟨, Fase 5 🟨); HU.md (HU-031 🟨 y dependencia de HU-026 para la parte de Trusted); `docs/ACTIVE_SERVICES.md` (costo del `sync`); `GLOSSARY.md` (Profiling: términos del informe y `quality/`); nueva `docs/validation/validation-5-1.md`. |
| Correcciones de documentación | §6 decía `CHANGELOG.md (v2.2 → v2.30)` con la versión vigente en 2.31: ahora v2.33. §6 describía `src/quality/` solo como Great Expectations: ahora el profiling (5.1) y GE (5.2). HU-031 dependía solo de HU-025, pero su criterio de Trusted necesita HU-026. |
| Pendiente | Profiling real de Trusted (Lote 4.2). Decidir con el 4.2 qué hace Trusted con las filas sin NLP y con los centinelas (DQ-13, DQ-63; *a confirmar*). Verificar en el 5.2 los nombres de expectativa de GE y en Athena (4.1) los predicados SQL de §5.10. Siguen abiertos los de v2.32 y anteriores. |

## v2.32 · desde v2.31

**Lote que lo motiva:** Lote 4.1 (HU-007, HU-029) · entrega

| Área | Cambio |
|---|---|
| Lote 4.1 | Entregado: Terraform de AWS (`modules/{glue_catalog,athena_workgroup,glue_role,quicksight_role,glue_workflow,lakehouse_schedule,glue_failure_events}`, `glue_catalog/tables.json` y `environments/dev/{lakehouse,variables_lakehouse,outputs_lakehouse}.tf`), `src/lakehouse/` (`catalog_spec`, `s3_parquet`, `glue_jobs/{raw_to_trusted,trusted_to_curated}.py`, `queries/*.sql`, 50 pruebas), `scripts/validate_4_1.py`, `src/ops/validate41.py` (48 pruebas) y `docs/validation/validation-4-1.md`. Detalle en **§3.19** e interfaces en **§3.19.1**. |
| Catálogo (HU-007) | Bases `ta_raw`, `ta_trusted`, `ta_curated` y las 11 tablas de §5.7 con partition projection en `posts_raw`, `posts` y `fact_posts`, creadas por Terraform **desde `contracts/athena/*.sql`**: `tables.json` se genera con `python -m lakehouse.catalog_spec --write` y una prueba falla si no coincide con las DDL. Carpetas `trusted/`, `curated/` y `quality/` como objetos vacíos. |
| Athena (HU-007) | Workgroup `ta-analytics` (motor v3) con configuración forzada, resultados en `athena-results/` cifrados con SSE-KMS de la llave del lake y **límite de 1 GiB por consulta** (§5.12, a confirmar); consultas guardadas `ta-raw-conteo-por-origen` y `ta-raw-sentimiento-7-dias`. |
| IAM (HU-007) | `ta-glue-role`, `ta-quicksight-role` y `ta-lakehouse-scheduler-role` (§5.8). `ta-historical-loader` y `ta-gcp-exporter-role` sin cambios. |
| Orquestación (HU-029) | EventBridge Scheduler `ta-wf-raw-to-curated-cada-hora` (`rate(1 hour)`, UTC, sin reintentos) → `glue:StartWorkflowRun` → workflow `ta-wf-raw-to-curated`: `ta-job-raw-to-trusted` y, si terminó bien, `ta-job-trusted-to-curated`. **El horario se crea `DISABLED`** (estado de ahorro; Terraform ignora su estado) y se enciende con `validate_4_1.py on`. Jobs Glue Python shell 3.9 de 0,0625 DPU; sus scripts son **marcadores de posición** que prueban lectura, escritura, KMS, el orden y los fallos sin tocar datos (ADR-23): 4.2 y 4.3 solo reemplazan el archivo. Fallos de `ta-job-*` → regla `ta-glue-job-fallos` → `/aws/events/ta-glue-job-fallos`; logs de los jobs en `/aws-glue/python-jobs/*` (30 días). |
| Validador | `validate_4_1.py preflight / tests / terraform [--apply] / catalog / athena [--reconcile-historical] / workflow [--no-tick] / status / off / on` (§8): A preflight (incluye el horario y `services.status`), B pruebas, C Terraform con el plan leído en JSON y un segundo plan tras aplicar (sin deriva), D Glue = DDL, workgroup, carpetas e IAM (36 casos simulados y los roles de 3.1/3.2 intactos), E el `SELECT` de demostración sobre `ta_raw.posts_raw` (50.000 filas del histórico; el stream conciliado contra los pies de los Parquet de S3; bytes ≤ límite; resultado con la llave del lake; las otras 10 tablas consultables), F el workflow en orden, un fallo provocado que llega a CloudWatch, un horario de prueba de una sola vez que arranca el workflow y se borra solo, limpieza y costo. Reutiliza piezas de `ops/validate32.py` y `ops/services.py`. |
| Verificación previa | WSL, Python 3.11.17, Terraform 1.16.6 (`aws` 5.100.0): `pytest src/lakehouse` **50 passed, 0 omitidas**; `test_ops_validate41.py` **44 passed**; `contracts/tests` 18; regresión `test_hist_infra.py` + `test_exp_infra.py` 27. `fmt -check`, `validate` e `infra/validate.sh` en verde; `init` con el backend real y **`plan` completo y con `-target`: 40 a crear, 0 a cambiar, 0 a destruir**. `validate_4_1.py preflight` y `tests` OK; `terraform` sin `--apply` en AVISO (cuenta raíz y sin aplicar). **No** se aplicó ni se corrió nada en AWS (puntos a confirmar en §3.19). |
| Contratos | `contracts/` sin cambios de interfaz (solo el comentario de `athena/posts.sql` sobre el `dt` de Raw, abajo); `schema_version` sigue en `"1.0"`. **§5.7:** prefijos `_glue/scripts/`, `_glue/temp/` y `{trusted,curated}/_ta_placeholder/`, carpetas, horario, workflow, jobs, fallos y workgroup. **§5.8 (más estricto, compatible):** `ta-glue-role` lee `raw/` pero no escribe ahí (decía R/W en `raw/`); `ta-quicksight-role` con la lista de acciones de consulta en lugar de `athena:*` (que incluía borrar y cambiar el workgroup); nuevo `ta-lakehouse-scheduler-role`. §5.1 (`AWS_PROFILE`), §5.9 (outputs), §5.11, §5.12 (límite de Athena y orquestación), §5.13 (`tables.json`) y §5.14. Nueva **ADR-23**. |
| Documento | Nueva §3.19 con §3.19.1; ampliadas §0, §1.2, §1.4, §4, §5.1, §5.7, §5.8, §5.9, §5.11–§5.14, §6 y §9 (ADR-23 y tres riesgos); PLAN.md §3 (Lote 4.1 y Fase 4 🟨); HU.md (HU-007 y HU-029 🟨, nota en HU-030); `ACTIVE_SERVICES.md` (lakehouse: qué cobra, cómo encender y apagar el horario). |
| Corrección (D6, simulación de logs) | Primera validación: A, B, C (40 creados, C3b sin cambios) y E OK (50.000 + 171.637 filas, 12,7 MB, 72,4 s de motor en E1). D6 dio FALLA en `logs:PutLogEvents en /aws-glue/`: no era la política sino el simulador de IAM, que responde `implicitDeny` para `logs:PutLogEvents` sobre el ARN de un flujo de logs aunque la política permita `"Resource": "*"` (comprobado con `simulate-custom-policy`); sobre el ARN del grupo, `allowed`, y los job runs reales escribieron sus flujos `jr_…`. D6 simula ahora con el ARN del grupo y suma un caso denegado (`/aws/events/ta-glue-job-fallos`): 20 casos; D6–D8 guardan en `data.cases` recurso, contexto, esperado y decisión. Pruebas: 50 + 45. Sin cambios en AWS. |
| Corrección (disparador `ON_DEMAND`) | El `apply` del 4.1 **ejecutó el workflow**: con `enabled = true` (el valor por defecto) el proveedor llama `StartTrigger` al crear un disparador `ON_DEMAND` (CloudTrail: `StartTrigger` 1 s después de `CreateTrigger`; corrida `wr_0ab6…`, los dos marcadores `SUCCEEDED` en 17 y 11 s, dos marcas en `_ta_placeholder/`). `ta-wf-raw-to-curated-inicio` se declara con `enabled = false` e `ignore_changes = [enabled]`, así que el plan no cambia nada (no hace falta aplicar) y una recreación futura no corre el workflow. Confirmado de paso: Glue acepta Python shell sin `GlueVersion`, el condicional queda `ACTIVATED` y los flujos de logs se llaman como el job run. |
| Corrección (F6, concurrencia) | Parte F: F1–F5 y F8–F11 OK (corrida `wr_015a…` en orden, jobs de 19 y 11 s; el horario de prueba arrancó `wr_2268…` y se borró solo; ≈ 0,00092 USD por corrida), `off` y `status` OK. **F6 dio FALLA** con `ConcurrentRunsExceededException` y F7 no apareció. Causa (lecturas y CloudTrail): `MaxConcurrentRuns = 1` y Glue libera el cupo unos segundos después de marcar la corrida `COMPLETED`: `wr_015a…` terminó a las 15:36:07,6 UTC y el `StartWorkflowRun` de F6, a las 15:36:16, fue rechazado. No es Terraform (una corrida a la vez es lo buscado). `start_run` espera ahora a que no haya corridas `RUNNING`/`STOPPING` y reintenta ante `ConcurrentRunsExceededException` cada 20 s hasta 9 veces (`-> (run_id, error, intentos)`); F8 espera a que el workflow esté libre antes de crear el horario de prueba (que, como el real, no reintenta); si F6 no arranca, F7 queda **OMITIDO** con el motivo. Pruebas del validador: 48 (el doble de AWS rechaza `StartWorkflowRun` tras cada corrida y muestra corridas `RUNNING`). |
| Corrección (documentación) | `contracts/athena/posts.sql` decía que el `dt` de Raw es la fecha de proceso; en el histórico es la de `event_time` (§5.7, v2.28): se corrige el comentario. `ARCHITECTURE.md` §6 decía que el CHANGELOG llegaba a v2.30. |
| Pendiente | Validar con `docs/validation/validation-4-1.md` (A–F, `off`) y anotar las cifras reales en v2.33. **A confirmar por el equipo:** el límite de 1 GiB por consulta y cuándo encender el horario (con los marcadores de posición no produce Trusted ni Curated). Probar `ta-quicksight-role` con QuickSight real (Lote 6.1; la cuenta no tiene QuickSight). Los `evidencia/tfplan_*` de lotes anteriores están versionados aunque las guías dicen que no se versionen (pueden llevar valores de `terraform.tfvars`): decidir si se agregan al `.gitignore`. Siguen los abiertos de v2.31 y anteriores. |

## v2.31 · desde v2.30

**Lote que lo motiva:** Lote 3.2 (HU-006, HU-025) · cierre

| Área | Cambio |
|---|---|
| Cierre del Lote 3.2 | Validado con `docs/validation/validation-3-2.md` por A (asignado a M). A en AVISO solo porque el simulador estaba encendido (este lote no lo usa); B OK con el código final (B1 120 sin omitidas, B2 68, B3 18, B4 11); C en AVISO solo por la cuenta raíz (AWS: rol y política; GCP: 2 a crear y 1 a destruir; confianza, política simulada 8/8 e IAM real OK); D OK (claims reales: `azp = sub` = uniqueId de `sa-exporter`, `aud = ta-gcp-exporter-role`; asunción real; 20 objetos subidos y verificados; idempotencia 1.155 → 1.135 por subir); E OK en la tercera corrida (16/16; las dos primeras pararon en E3 y E2, ver las correcciones de v2.30); F OK; `off` y `status` OK. |
| Cifras | Sonda 13/13 en Cloud Run (`raw/` de GCS y un objeto real de `raw/origin=stream/` denegados). Primera ejecución: 1.135 objetos (51 MB) en 107 s, 0 fallidos; verificador 1.155 de 1.155 con el mismo tamaño, 50 de 50 con SSE-KMS, la llave del lake y el CRC32C de GCS; segunda ejecución 0 subidos; canario 1 subido y borrado; disparo real del Scheduler con `sa-exporter`, terminó bien y volvió a `PAUSED`; 0 errores en los logs. En S3: 12.102 objetos del histórico (130,9 MB) y 1.155 del stream (51,9 MB, 385 por fuente), con las mismas 24 columnas. Duraciones 36–118 s → ≈ 0,0016 USD por ejecución y **≈ 0,16 USD/día** con el Scheduler encendido (la estimación de v2.30, ~0,1, suponía ejecuciones de 1 min). |
| Fase 3 | Cerrada: el "Listo cuando" (datos de ambos orígenes en `s3://…/raw/`) lo comprueba F4 con el mismo esquema en los dos. |
| Documento | §3.18 con la nota de cierre y el costo medido; PLAN.md §3 (Lote 3.2 ✅ y Fase 3 ✅); HU.md (HU-006 y HU-025 ✅); `ACTIVE_SERVICES.md` §0 y §2.7 con el costo medido; tabla de cierre en la guía. |
| Abierto, sin bloquear | Encender el Scheduler de forma permanente lo decide el equipo (queda en `PAUSED`). Integrar el exportador en `on.py`/`off.py`/`status.py` (Lote 7.3). Una ejecución sin nada que subir tardó de 36 a 118 s: si se enciende, conviene ver en qué se va ese tiempo. La causa del fallo al guardar el estado en E3 no quedó registrada (el `state push` posterior funcionó). **Simulador fuera del estado:** al apagar, `off.py` dejó `simulator_running = false`, pero dijo "simulador: sin cambios" y `ta-stream-simulator` siguió encendido. El servicio existe en GCP (creado el 24 oct a las 14:01 UTC con la cuenta de A y las etiquetas que pone Terraform), pero no está en el estado de `infra/gcp`. El plan completo no propone crear nada más, así que solo falta ese. Se apaga con `gcloud run services delete` (`ACTIVE_SERVICES.md` §5). Falta saber, con las versiones del estado en GCS, si salió del estado con el `state push` de E3. Siguen los abiertos de v2.29 y anteriores. |

## v2.30 · desde v2.29

**Lote que lo motiva:** Lote 3.2 (HU-006, HU-025) · entrega

| Área | Cambio |
|---|---|
| Lote 3.2 | Entregado: `src/exporter/` (`config`, `keys`, `watermark`, `core`, `gcs_io`, `s3_io`, `identity`, `probe`, `verify`, `lake_check`, `cli`, `jsonlog`, `testing`, `Dockerfile`, `cloudbuild.yaml`, `requirements.txt`, `EXPORTER.md`), `scripts/{export_gcs_to_s3.py,build_exporter_image.sh,validate_3_2.py}`, `src/ops/validate32.py`, Terraform de AWS (`modules/gcp_exporter_role`, `environments/dev/{exporter,variables_exporter,outputs_exporter}.tf`) y de GCP (`modules/cloud_run_exporter`, `environments/dev/{exporter,variables_exporter,outputs_exporter}.tf`), los `*.auto.tfvars.example` del enlace entre nubes, `docs/validation/validation-3-2.md` y 188 pruebas (120 en `src/exporter/tests/`, 68 en `src/ops/tests/test_ops_validate32.py`). Detalle en **§3.18**. |
| Federación (HU-006) | `sa-exporter` asume `ta-gcp-exporter-role` con su ID token de Google y `AssumeRoleWithWebIdentity`, sin llaves. Google es un proveedor OIDC **integrado** en AWS (`accounts.google.com`): no se crea un *identity provider*. Confianza con `accounts.google.com:aud` ∈ {uniqueId, correo, audiencia} (AWS lo compara con el claim `azp`), `oaud` = `ta-gcp-exporter-role` y `sub` = uniqueId de `sa-exporter`. Para validar desde la máquina del equipo, la cuenta del desarrollador emite tokens de `sa-exporter` (`roles/iam.serviceAccountOpenIdTokenCreator`, solo sobre esa SA). |
| Exportador (HU-025) | Cloud Run Job `ta-gcs-to-s3` y Cloud Scheduler `ta-gcs-to-s3-cada-15-min` (`*/15`, UTC), **creado en pausa** (estado de ahorro; Terraform ignora `paused`). Lee solo `enriched/` desde el watermark (`timeCreated`, solape de 1 h, margen de 2 h para los paneles tardíos), escribe `raw/origin=stream/…/{mismo nombre}.parquet` con un PUT de una parte con SSE-KMS, *bucket key* y el CRC32C de GCS, omite lo que ya está con el mismo tamaño y escribe el watermark solo si todo subió (con precondición de generación). Imagen `gcs-to-s3:{commit o fecha}-{huella}` construida en Cloud Build. |
| IAM de GCP | Se quita de `platform.tf` (Lote 1.3) `exporter_lake_read`, el `objectViewer` sin condición de `sa-exporter` en todo el lake, y `exporter.tf` lo reemplaza por uno con condición: listar el bucket y leer solo objetos de `enriched/` (pendiente del Lote 2.1, criterio de HU-019 y HU-025). `run.invoker` de `sa-exporter` sobre el job. Planes esperados: AWS 2 a crear; GCP 2 a crear y 1 a destruir; job 3 a crear. |
| Validador | `validate_3_2.py preflight / tests / terraform [--apply] / federation / job / lake / status / off` (§8): A preflight (incluye contenedores y nube encendidos), B pruebas, C Terraform de las dos nubes con el plan leído en JSON, D federación real desde la máquina del equipo (claims del token frente a la confianza, asunción real, sondas y una corrida de 20 objetos con un watermark de prueba), E imagen, job, sonda como `sa-exporter` en Cloud Run, atraso, repetición, canario, un disparo del Scheduler y la pausa, F ambos orígenes en S3. `off` pausa el Scheduler, cancela ejecuciones, borra canarios y lo confirma. |
| Verificación previa | Python 3.11.17 con un repositorio simulado (el `platform.tf` real con el cambio; raíces de Terraform, `common/enriched_row.py` y scripts de operación con la forma de §3.14, §5.14 y §5.15): **B1 118 passed, 0 omitidas; B2 62 passed** (120 en B1 tras la corrección de A6). OpenTofu 1.8.5: `fmt -check` en verde; sin `validate` ni `plan` (sin acceso al registro de proveedores). Con moto: `PutObject` con SSE-KMS y `AssumeRoleWithWebIdentity` sin firmar. Etapa de pruebas de la imagen en un entorno con solo `requirements.txt`: 94 passed, 0 omitidas. **No** se corrió contra GCP ni AWS. |
| Revisión antes de entregar | Una revisión independiente del código, el Terraform y los comandos de `gcloud`/`aws` no encontró defectos bloqueantes; se corrigieron: D6 del validador (planeaba con el cursor de D4 y podía dar un falso FALLA), el tiempo máximo de espera de una ejecución (840 s × 2 intentos + margen), E8 tras un E6 cortado por `max_objects`, hasta 10 subidas en paralelo (pools HTTP), la llave del lake leída solo cuando se crea el rol, llaves multirregión (`mrk-`) y los errores inesperados como log `export_error`. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. **§5.8 y HU-006:** `s3:HeadObject` no es una acción de IAM (un HEAD exige `s3:GetObject`): el rol tiene `s3:ListBucket` con `s3:prefix` = `raw/origin=stream/*` y la idempotencia se hace listando la partición; además `kms:GenerateDataKey` con `kms:ViaService` = S3. **HU-006:** el "OIDC identity provider de Google" es el integrado de AWS. §5.1 (variables del exportador), §5.4 (watermark), §5.6 (formato del watermark, claves, confianza, transporte y logs), §5.8, §5.9 (outputs del lote), §5.11, §5.12 (parámetros del exportador) y §5.14. Nueva **ADR-22**. |
| Documento | Nueva §3.18; ampliadas §0, §1.2, §1.4, §3.13 (observación de `sa-exporter` resuelta), §4, §5.1, §5.4, §5.6, §5.8, §5.9, §5.11, §5.12, §5.14, §6 y §9 (ADR-22 y el riesgo de la federación); PLAN.md §3 (Lote 3.2 🟨); HU.md (HU-006 y HU-025 🟨, nota en HU-019); `docs/ACTIVE_SERVICES.md` (exportador: qué se enciende, cuánto cuesta y cómo pausarlo). |
| Corrección (documentación) | `ARCHITECTURE.md` no bastaba para usar el lote sin leer el código: faltaban las interfaces. Nueva **§3.18.1** con la ejecución del job, el CLI (subcomandos, opciones, credenciales, salidas JSON y códigos de salida), el reporte de una corrida, los logs con sus campos, la API de Python, el script de la imagen, las variables, módulos y direcciones de Terraform, el enlace `*.auto.tfvars` y el validador (partes, opciones, archivos que escribe y forma de `validacion_3_2.json`). §5.7 describe el Raw stream. |
| Corrección (preflight en Windows) | La primera corrida de A fue desde PowerShell (Python 3.14 de Windows, sin el venv y sin poder ejecutar `gcloud` desde Python): A1 y A6 fallaron por el entorno, no por el código, y A10 dio OK con "sin dato". Ahora A0 detecta Windows y dice cómo entrar a WSL, las demás partes no corren fuera de WSL (salen con 2 sin tocar la nube) y A10 da AVISO si no pudo comprobar Dataflow, Cloud Run o el Scheduler. Pruebas del validador: 62. |
| Corrección (sonda A6) | Parte D: D0–D1 OK (claims reales; federación aceptada) y D2 dio FALLA en A6 con `error NoSuchKey`. No era un permiso de más: la sonda pedía una clave inexistente y, como el rol tiene `s3:ListBucket` en ese prefijo, S3 responde 404 en vez de 403. Ahora A6 busca un objeto existente (`MaxKeys = 1`) y le pide 1 byte esperando `AccessDenied`; con el prefijo vacío da OK con `got = sin objetos` (la cubre C5b; en vivo, E5). El doble imita a S3 (404 para una clave inexistente con listado, 403 para una existente). Pruebas: 120 + 62 (imagen 96). |
| Corrección (E3, estado sin guardar) | El primer `apply` de E creó el job, `run.invoker` y el Scheduler (en pausa), pero Terraform no pudo escribir el estado en el bucket y lo dejó en `errored.tfstate`; E3 mostró solo el final del mensaje, con colores. La causa no quedó registrada: el `state push` posterior funcionó con las mismas credenciales, así que fue pasajera. Se recuperó con `terraform state push errored.tfstate` (`state list`: los 3 recursos). Ahora el validador no planea si hay un `errored.tfstate` (un plan sin ese estado volvería a crear lo que existe) y explica cómo subirlo; con un plan o `apply` fallido muestra el bloque `Error:` sin colores. |
| Corrección (E2, Scheduler) | Tras recuperar el estado, E2 dio "1 a cambiar" en `google_cloud_scheduler_job.this` y no aplicó (correcto). La causa era `retry_config { retry_count = 0 }`: la API no devuelve el bloque cuando vale lo mismo que el valor por defecto, y cada plan proponía añadirlo. Se quita el bloque, porque el valor por defecto ya es 0 reintentos, y el comportamiento no cambia. Si un plan cambia algo inesperado, C2, C6 y E2 muestran ahora el atributo y su valor antes → después (también en `data.diffs`). Pruebas del validador: 67. |
| Corrección (apagado, O5) | `off` dio O5 OK aunque `status.py` mostraba `GCP simulador: ENCENDIDO (cobra)`: solo miraba el código de salida. Ahora O5 da AVISO si alguna línea dice `ENCENDIDO` y sugiere `python project/scripts/off.py` (o `--keep-simulator`). Pruebas del validador: 68. |
| Corrección (C8 sin apply) | Primera corrida de C sin `--apply`: C0–C6 como se esperaba (GCP: 2 a crear y 1 a destruir), pero C8 dio FALLA porque comparó el IAM real, que todavía era el de antes del plan. Ahora C8 solo compara si el plan de GCP quedó aplicado; si no, OMITIDO. |
| Pendiente | Validar con `docs/validation/validation-3-2.md` (A–F) y anotar las cifras reales en v2.31. Integrar el exportador en `status.py`/`off.py` (Lote 7.3); mientras tanto, `validate_3_2.py status` y `off`. Siguen abiertos los de v2.29 y anteriores. |

## v2.29 · desde v2.28

**Lote que lo motiva:** Lote 3.1 (HU-024) · cierre

| Área | Cambio |
|---|---|
| Cierre del Lote 3.1 | Validado con `docs/validation/validation-3-1.md`. A OK (A1–A8); B OK (B1 142, B2 39, B3 18, B4 28); C en AVISO solo por la cuenta raíz (10 recursos aplicados, SSE-KMS y acceso público bloqueado); D OK (600 filas con NLP real, H1–H12, mismo digest); E OK salvo un falso aviso de H7, ya corregido (50.000 filas, digest `939248b3…`, 0 sin NLP, 12.102 archivos, H1–H6 y H8–H13 OK); F en AVISO solo por la cuenta raíz: **12.102 objetos** subidos en 165 s, política del rol 6/6 (simulada), **H1–H12 y H14 OK en S3 con el verificador corregido** y la segunda subida no sube nada. |
| Cifras | Estacionalidad semanal r = 0,991 y horaria r = 1,000; crecimiento 1,36 frente a 1,34; `campana_sprite` ×4,69 y `crisis_postobon` ×1,45 de volumen; 4.547 `user_hash` 1 a 1; acuerdo del NLP con `label_sentiment` 91,8 %; 500 de 500 `user_hash` más activos en el stream (136.323 de 176.715 filas). NLP en la máquina del equipo: ~4 filas/s por proceso con 4 procesos, con el entorno local del stream apagado. |
| Documento | §3.17 con la nota de cierre; §5.12 (NLP del histórico: lotes de 16 con la espera del pipeline y procesos en paralelo, no "sin espera"); PLAN.md §3 (Lote 3.1 ✅; la Fase 3 sigue 🟨 hasta el 3.2); HU.md (HU-024 ✅). |
| Abierto, sin bloquear | Probar la asunción real de `ta-historical-loader` con un usuario IAM o SSO (con la cuenta raíz solo se pudo simular). Siguen los abiertos de v2.27 y anteriores. |

## v2.28 · desde v2.27

**Lote que lo motiva:** Lote 3.1 (HU-024) · entrega

| Área | Cambio |
|---|---|
| Lote 3.1 | Entregado: `src/historical/` (`plan`, `adapters`, `generate`, `nlp_batch`, `writer`, `verify`, `bq`, `s3io`, `doctor`, `cli`, `schema`, `work`, `testing`, `config/historical.yaml`, `requirements.txt`, `HISTORICAL.md`), `scripts/{load_historical.py,historical_nlp.sh,validate_3_1.py}`, `src/ops/validate31.py`, Terraform de AWS (`modules/{kms_key,s3_lake,historical_loader_role}` y `environments/dev/{lake,variables_lake,outputs_lake}.tf`), `docs/validation/validation-3-1.md` y 154 pruebas (124 en `src/historical/tests/`, 30 en `src/ops/tests/test_ops_validate31.py`). Detalle en **§3.17**. |
| Histórico (HU-024) | 50.000 registros (configurable) con `EventFactory` del simulador y `HIST_SEED = 3101`: mismo generador, pool de usuarios, payloads y engagement; `origin = historical` y `event_id` de la fórmula de §3.6. Plan de 180 días con estacionalidad semanal y horaria (hora local), crecimiento, tendencias de marca (Pepsi +35 %, Postobón −10 %) y dos episodios (`campana_sprite`, `crisis_postobon`), aplicado por **aceptación** de los eventos del simulador sin modificarlo. |
| Mismo código | Anonimización con `enrich_envelope()` y la llave `ta-hmac-key` (cruce de los `user_hash` con `posts_enriched` antes de publicar la etapa); NLP con el PTransform de `streaming.nlp.nlp_from_options()` en lote (`BundleBasedDirectRunner`) dentro de la imagen `ta-dataflow-nlp-local:local`, sin credenciales; fila y Parquet con `to_posts_enriched_row()` y `rows_to_parquet_bytes()`. |
| Reejecutable | Etapas reanudables (`generate` → `nlp` → `write` → `verify` → `upload`); digest del dataset; claves deterministas `raw/origin=historical/source=…/dt=…/hr=…/part-historical-s{seed}-{AAAAMMDD}T{HH}.parquet` con `dt`/`hr` de `event_time`; subida que omite lo que ya está con el mismo tamaño. |
| AWS (adelantado del 4.1, HU-007) | Bucket `ta-dev-648617469176-lake` (SSE-KMS con *bucket key*, sin acceso público, `BucketOwnerEnforced`, solo TLS, `athena-results/` a 7 días), llave `alias/ta-dev-lake` con rotación y rol `ta-historical-loader` (`PutObject` y `ListBucket` solo bajo `raw/origin=historical/`, `GenerateDataKey`). Plan con `-target`: 10 a crear, 0 a cambiar, 0 a destruir. ADR-21. |
| Verificador | `load_historical.py verify`: H1–H12 (archivos y particiones, columnas, conteos, fórmula del `event_id`, ventana, estacionalidad, tendencias, episodios, anonimización, engagement, NLP, marcas); `--bq` suma H13 (`user_hash` y `nlp_model_version` compartidos con el stream) y `--s3` H14 (objetos y SSE-KMS). `doctor` comprueba los adaptadores contra el código real antes de la corrida larga. |
| Validador | `validate_3_1.py preflight / tests / terraform [--apply] / local [--build] / load / upload` (§8). Como el 2.4, no usa `ops/services.py`. |
| Verificación previa | Python 3.11 y Beam 2.77.0. Sin el repositorio: 143 passed y 9 omitidas (las 8 de `test_hist_repo.py` y `terraform fmt`). Con un repositorio simulado con las firmas de §3.2.2, §3.10, §3.14 y §3.15 y OpenTofu como `terraform`: **154 passed, 0 omitidas**, y la CLI de punta a punta con el motor Beam. OpenTofu 1.8.5 + proveedor `aws` 5.80: `fmt -check`, `validate` y `plan` sin conexión (10 a crear). **No** se corrió con el código real de los lotes 1.1–2.3, ni con los modelos de NLP, ni contra GCP o AWS. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. §5.1 (`HIST_SEED`, `HIST_N`, `TA_HIST_ROOT`, `TA_LAKE_*`, `TA_HIST_LOADER_ROLE_ARN`, `AWS_PROFILE`), §5.7 (rutas y particiones del Raw histórico; el bucket existe), §5.8 (`ta-historical-loader`), §5.9 (outputs de `infra/aws`), §5.11, §5.12 (parámetros del histórico) y §5.14. Nueva **ADR-21**. |
| Documento | Nueva §3.17; ampliadas §0, §1.2, §4, §5.1, §5.7, §5.8, §5.9, §5.11, §5.12, §5.14, §6 y §9; PLAN.md §3 (Lote 3.1 🟨); HU.md (HU-024 🟨); `docs/ACTIVE_SERVICES.md` (costos de AWS y contenedor de NLP del histórico). |
| Corrección (validación A y C) | A: faltaban `faker` y `boto3` en `ta-streaming` (entorno, no código); la instrucción de instalación ahora sirve desde la raíz y A6 distingue Docker apagado de imagen ausente. C2a: `terraform init` del entorno AWS sin `-backend-config=backend.hcl` (`Missing region value`): el validador lo pasa, y un paso nuevo (C0b) exige `backend.hcl` y `terraform.tfvars`, que en esta copia del repo no existían. C0 mostró la **cuenta raíz**, que no puede asumir roles: C0 avisa, F sube con `--no-role` y F3 prueba el rol con `iam simulate-principal-policy` (6 casos; con un usuario IAM además la sonda real), y `upload` rechaza `--role-arn` con la raíz explicando por qué. Pruebas: 125 + 35. |
| Corrección (E3) | `generate` falló con `llave HMAC vacía` (lo lanza el código del Lote 2.2). `load_key()` aceptaba como llave un resultado vacío y `make_enricher()` solo pasaba la llave a parámetros con nombres exactos (un parámetro con otro nombre y valor por defecto vacío la recibía vacía). Ahora un resultado vacío cuenta como fallo y se prueba el siguiente camino (`load_hmac_key` → `access_secret` → `gcloud`), el parámetro de la llave se busca por nombre parecido y, si no hay ninguno, falla mostrando la firma; `generate` imprime `llave: <camino> · huella` y `doctor` informa los intentos (tipo y largo, nunca el valor). Pruebas: 130 + 35. |
| Corrección (E4, velocidad del NLP) | E3 OK (500 de 500 `user_hash` coinciden con el stream). E4 iba a ~2,3 filas/s (≈ 6 h): el loader forzaba `--nlp_batch_seconds 0` y los lotes de RunInference no se llenaban. Ya no se fuerza (queda la espera del pipeline); la salida del NLP se recoge con un `Map` en el mismo proceso, porque con lotes por tiempo el último lote incompleto llegaba tarde al `GroupByKey` de `WriteToText` y el `BundleBasedDirectRunner` fallaba (`AssertionError`); nuevos `--runner bundle|fnapi`, `--batch-seconds`, registro del tamaño real de los lotes (`batch_mean`) y `historical_nlp.sh doctor --nlp-bench N`; `validate_3_1.py` pasa `--nlp-runner` y `--nlp-batch-seconds`. Lo ya procesado se conserva. Pruebas: 134 + 36. Medición posterior (`--nlp-bench 160`): con lotes de 16 un proceso sigue en ~3 filas/s (`bundle` 2,89, `fnapi` 3,06), así que se agrega `--workers N` (procesos `spawn` con sus modelos y `núcleos / N` hilos de torch; `validate_3_1.py --nlp-workers`) y el benchmark mide también 4 procesos en paralelo y el porcentaje del tiempo que es inferencia. Pruebas: 136 + 36. |
| Corrección (E4, runner del NLP) | Segundo `--nlp-bench 160`: la inferencia es el 98–99 % del tiempo (1 proceso: 2,60 filas/s; `fnapi` 2,67) y la prueba de 4 procesos con `fnapi` falló con `time-pending bundles did not execute`. Reproducido con Beam 2.77.0 y un transform de la misma forma: con dos agrupaciones con estado encadenadas y un último lote parcial, el `FnApiRunner` deja el segundo lote parcial pendiente para siempre (falla) o lo **pierde sin error**. Se quita `fnapi`: el NLP en lote usa siempre el `BundleBasedDirectRunner` (`--runner fnapi` y `--nlp-runner fnapi` se rechazan con la explicación), `historical_nlp.sh` ya no pasa `TA_HIST_NLP_RUNNER`, el benchmark compara 1 proceso con 2 y 4 en paralelo (`--bench-workers`), avisa si faltan filas y no se queda esperando a un proceso muerto (código −9 = memoria). Pruebas: 139 + 37. Detalle en §3.17. |
| Corrección (E4, causa de la lentitud) | La lentitud venía del entorno local del stream (`trendanalyzer-local-*`), que seguía encendido en Docker: su `dataflow-local` corría el mismo NLP sin parar (778 % de CPU, 1,6 GB de los 7,4 GB de Docker). Apagado con `local_dataflow.sh down-all`: `nlp_bench.py` 41 comentarios/s con lote 16; loader 35,3 filas/s con 1 proceso (antes 2,6) y 45,5 con 4 procesos. Nuevo **A8** en el preflight (avisa si hay contenedores que compiten por la CPU) y **D5a/E4a** (no arrancan el NLP con el entorno del stream u otro contenedor de NLP encendido; `--allow-busy` corre igual); la guía exige ese entorno apagado en D y E. En la carga real, 4 procesos hicieron ~4 filas/s cada uno (≈ 16 en total, ~50 min para 50.000): el benchmark medía con textos sintéticos cortos y sobreestimaba; ahora usa filas reales de `stage1/` cuando existen. Pruebas: 140 + 39. Detalle en §3.17. |
| Corrección (E6, H7) | E4–E7 corrieron: 50.000 filas, 12.102 archivos, mismo digest; H1–H6 y H8–H13 OK. H7 dio un **falso AVISO** (`sprite: cambio +0.033 vs esperado -0.021`): lo esperado suponía que el simulador entrega las marcas en proporción fija, pero su `BrandSelector` mueve la mezcla con `seq` (§3.10) y el plan solo la multiplica. Ahora H7 reconstruye los candidatos del simulador en el primer y el último mes (`verify` usa `SimulatorEnvelopes`; si no carga, vuelve al cálculo uniforme y lo dice), compara la participación observada con la esperada y muestra la tendencia aplicada frente a la marca sin tendencia. Los datos no cambian. Pruebas: 142 + 39. Detalle en §3.17. |
| Pendiente | Validar con `docs/validation/validation-3-1.md` (A–F) y anotar las cifras reales. Agregar `.local-s3/` al `.gitignore` de la raíz. Siguen abiertos los de v2.27 (drenado en GCP, parte D del 2.4, reinicio del SDK) y anteriores. |

## v2.27 · desde v2.26

**Lote que lo motiva:** Lote 2.4 (HU-005, HU-023) · cierre (cierra la Fase 2)

| Área | Cambio |
|---|---|
| Cierre del Lote 2.4 | Validado con `docs/validation/validation-2-4.md`. A y B OK; C OK (10 recursos: tablas `trend_windows`, `dlq_events`, `dq_results`, vista `trend_windows_latest`, suscripción `ta-dlq-df` y permisos de `sa-dataflow`). E (segunda corrida): E1–E6 y E8–E10 OK; T1–T7 y T9 OK: 410 filas sin problemas, conciliación 126/126, latencia p95 `event_time → process_time` de **5,4 s** (KPI de HU-023 < 5 min), 0 alertas en el arranque en frío, `dlq-check` 3/3, 0 `GETTING_PUBSUB_SUBSCRIPTION_FAILED`; detección p50 52,6 s. F1–F5 OK: imagen `dataflow-launcher` construida y probada, plantilla publicada y **job lanzado desde la plantilla en *Running*** con `dataflow-nlp` y las tablas del 2.4. F6 OK con el job de la plantilla, tras descartar el atraso de las suscripciones (`gcloud pubsub subscriptions seek`): 446 filas sin problemas, conciliación **216/216**, 0 alertas en el arranque y en el cambio de job, latencia p95 `event_time → process_time` de **5,1 s** y **detección p95 de 61,3 s** (p50 44,0 s, 63 claves); F7 y F8 OK. |
| Fase 2 | **Cerrada**: lotes 2.1 a 2.4 cerrados. Se cumple el "Listo cuando" de la fase: un evento llega a BigQuery con `emotion_label` en segundos (centinela a los 5 s en el 2.3) y p95 de latencia < 5 min. |
| F6, primer intento | El job de la plantilla arrancó con más de una hora de atraso (el job anterior se trabó en el NLP y se canceló con el simulador encendido); T4/T7/T8 midieron el atraso. Se repitió tras descartar el atraso con `seek` y pasó completo. |
| Abierto, sin bloquear | (1) la prueba en GCP de que el drenado no deja alertas falsas (cubierta por la prueba con `TestStream`); (2) la parte D (local con DirectRunner); (3) la causa del reinicio de los procesos del SDK que trabó el NLP de un job (19:15 UTC del 23-oct; sin mensajes de memoria en los logs; pico de memoria pendiente desde el 2.3). |
| Operación | Recomendación: apagar el simulador cuando el job de Dataflow esté apagado (`off.py`) o, en desarrollo, descartar el atraso con `gcloud pubsub subscriptions seek … --time=<ahora>` antes de relanzar. |
| Contratos | Sin cambios respecto a v2.26. |
| Documento | §3.16 (cierre), §1.4 y §6; PLAN.md §3 (Fase 2 y Lote 2.4 ✅); HU.md (HU-005 y HU-023 ✅); `docs/validation/validation-2-4.md` (resultado de la validación). |

## v2.26 · desde v2.25

**Lote que lo motiva:** Lote 2.4 (HU-005, HU-023) · entrega

| Área | Cambio |
|---|---|
| Lote 2.4 | Entregado: `src/streaming/{trends,trends_core,bq_sink,dlq_bq,trends_check,trends_bq,trends_cli,template_tools,flex_main}.py`, `src/streaming/flex_template/{Dockerfile,metadata.json}`, `src/streaming/TRENDS.md`, `src/common/stream_tables.py`, `scripts/{verify_trends,dataflow_template,validate_2_4}.py`, `src/ops/validate24.py`, Terraform (`modules/bigquery_stream_tables` y `environments/dev/{trends,variables_trends,outputs_trends}.tf`), `docs/validation/validation-2-4.md` y 190 pruebas (155 en `src/streaming/tests/`, 35 en `src/ops/tests/`). Detalle en **§3.16**. |
| Tendencias (HU-023) | Ventanas deslizantes de 5 min (paso 1 min) en tiempo del evento por `brand_primary` × `source`, con `AfterWatermark`, paneles tardíos acumulativos (Dataflow: uno por minuto; local: uno por dato) y *allowed lateness* de 30 min; lo más tardío cuenta en `late_events` y no recalcula. Baseline de 60 ventanas en un DoFn con estado: z-score de volumen y sentimiento, `is_alert` con baseline completo, `n_posts ≥ 5` y \|z\| ≥ 3; log `trend_alert`. Una fila por panel; la vista `trend_windows_latest` da la versión vigente (mayor `n_posts`). |
| `dlq_events` (HU-018, pendiente del 2.1) | Suscripción nueva `ta-dlq-df` que lee el mismo job; la fila sale de los atributos `dlq_*` de §5.3, sin tocar el código del 2.1. Si se pasa `--dlq_table`, esta rama se apaga sola. |
| Recursos (HU-005) | `trend_windows` (partición por `window_start`, 90 días, clustering `brand, source`), `dlq_events` (por `received_at`, 30 días), `dq_results` (por `run_ts`, sin expiración, clustering `layer, kpi`) y la vista. `sa-dataflow`: `roles/pubsub.viewer` en las cuatro suscripciones que lee (pendiente del 2.2) y `subscriber` en `ta-dlq-df`. Plan esperado con `-target`: 10 a crear, 0 a cambiar, 0 a destruir. |
| Plantilla Flex (HU-005) | Imagen `dataflow-launcher` (Cloud Build, contexto propio, prueba de humo) y especificación en `gs://…-ta-lake/_dataflow/templates/`; `dataflow_template.py params/build/run` toma todos los parámetros de `terraform output` y lanza `ta-stream-{ENV}` igual que `dataflow_job.sh start`. Los workers siguen en `dataflow-nlp`. |
| Enganche | Dos líneas en `pipeline.py` (`attach_trends(enriched_rows)` con lo que devuelve `attach_enriched(unique)`) y, si falta, `return rows` en `attach_enriched()`. `dataflow_job.sh` no cambia: las opciones nuevas tienen valor por defecto y `TA_TRENDS_ENABLED` / `TA_DLQ_BQ_ENABLED` las apagan. |
| Verificador | `verify_trends.py tables` (T1), `report` (T2–T9: fuentes, reglas, conciliación ≥ 99 % con `posts_enriched` o `enriched/`, arranque en frío, z-scores, latencia p95 sin el arranque, detección con `--watch-minutes`, KPI en `dq_results` con `--write-dq`) y `dlq-check --allow-cloud` (D1–D3). |
| Validador | `validate_2_4.py preflight / tests / terraform [--apply] / local / cloud / template`, siguiendo la convención de §8. No usa `ops/services.py` (no estaba disponible al prepararlo): llama a los scripts documentados y a `gcloud`/`terraform`. El `plan` va con `-target` del lote para no aplicar deriva de otros lotes. |
| Hallazgos | Con `TestStream`, el `BundleBasedDirectRunner` gira al 100 % de CPU si los paneles tardíos usan `AfterProcessingTime` → en local, `AfterCount(1)`. En streaming, el DirectRunner pierde las métricas de los *bundles* sin salidas → los contadores se prueban llamando a los DoFn. |
| Verificación previa | Python 3.11.15 y Beam 2.77.0: **181 passed, 9 omitidas** (necesitan `contracts/`, `pipeline.py`, `enriched.py`, `platform.tf`, `streaming.tf` y `common.label_map`); con un repositorio simulado, las 23 de esos archivos pasan y `validate_2_4.py tests` da B1 = 190. OpenTofu 1.8.5 + proveedor `google` 6.10: `fmt -check` y `validate` en verde. Grafo de Dataflow con los sumideros reales traducido con `--dry_run=True` (Runner v2). **No** se corrió con el código real, ni en local, ni en GCP; no se construyó la imagen del lanzador. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. §5.1 (`TA_TRENDS_ENABLED`, `TA_DLQ_BQ_ENABLED`), §5.3 (`ta-dlq-df`), §5.4 (`_dataflow/templates/`, `trend_windows/` local), §5.5 (semántica de `trend_windows`, vista, particiones, `dlq_events` y `dq_results`), §5.8, §5.9, §5.11, §5.12 y §5.15. Nueva **ADR-20**. |
| Documento | Nueva §3.16; ampliadas §0, §4, §5.1, §5.3, §5.4, §5.5, §5.8, §5.9, §5.11, §5.12, §5.14, §5.15, §6 y §9; PLAN.md §3; HU.md (HU-005, HU-023). |
| Corrección (validación A–C) | A OK; B1 190, B2 345, B3 18, B4 72; C1–C3 OK (10 recursos aplicados). C4 falló: con `-target`, Terraform no guarda los outputs que solo usan variables (`dataflow_template_spec`, `dataflow_launcher_image`). Se les añade `depends_on` a `ta-dlq-df`; el validador aplica también cuando el plan solo cambia outputs y C4 marca AVISO si faltan solo esos dos (`dataflow_template.py` los deriva). Pruebas del lote: 194 (B2 346, B4 75). Reproducido con OpenTofu y `terraform_data`. |
| Corrección (primer intento de E) | El validador leía `terraform output` sin mostrar nada (lento en `/mnt/c`) y E4a agotó 300 s en `dataflow_job.sh drain`. Ahora drena con `gcloud dataflow jobs drain` y espera consultando el estado (también si el job ya estaba drenando), corre los comandos sin entrada estándar, muestra en vivo la salida de los pasos largos y avisa al leer `terraform output`. Pruebas del lote: 200 (B4 81). |
| Corrección (primera corrida completa de E) | E1–E6, E8–E10 OK; E7 con T8 FALLA (p95 = máx = 310,5 s: una ventana de 12 observada durante el *backlog* del cambio de job) y 48 alertas del job anterior **al drenarse**. Las ventanas emitidas antes de terminar en tiempo real ya no alertan (reproducido con `TestStream`); T8 espera a que el job se ponga al día, cuenta solo ventanas con 5 min de margen y no ignora las que no aparecieron; nuevo T5b (alertas en las ventanas del cambio de job, AVISO); `--no-restart` verifica las opciones del job. Pruebas del lote: 213 (B2 357, B4 83). |
| Corrección (parte F) | F1 y F2 OK: imagen `dataflow-launcher:20261023-1918` construida y probada, plantilla publicada. F3 parecía congelado esperando el drenado del job de E: ahora informa cada 2 min, da el tiempo de drenado y acepta `--cancel-after-minutes`. Pruebas del lote: 215 (B4 85). |
| Corrección (parte F, lanzamiento) | F4: el job de la plantilla no llegó a *Running*. Con `--template_location` (lo pasa el lanzador Flex), Beam 2.77 devuelve un resultado sin job y `result.job_id()` lanza `AttributeError`; además el lanzador no corría desde `src/` (rutas relativas del 2.1). `flex_main.py` trabaja desde `src/`, pasa `nlp_container_image` a `TA_NLP_IMAGE` y parchea `job_id()`; el Dockerfile usa `WORKDIR` en `src/` y su prueba de humo lo comprueba. El validador sigue el job por su id y falla enseguida con los errores del lanzador. Antes, el drenado del job de E no terminó: sus procesos del SDK se reiniciaron a las 19:15:54 UTC y la marca de agua quedó detenida en el NLP (causa por confirmar). El validador también trae el traceback del lanzador (llega con severidad INFO). Causa confirmada en los logs del lanzador (`AttributeError` con `--template_location`). `flex_main.py` además separa las etiquetas que el lanzador pasa juntas (`--labels=a=b,c=d`). Pruebas del lote: 224 (B2 362, B4 89). |
| Pendiente | Validar con `docs/validation/validation-2-4.md` (A–E; F recomendado para el criterio de plantilla de HU-005). Siguen abiertos los de v2.25 (cifras de B y C y pico de memoria del 2.3) y v2.21/v2.17. |

## v2.25 · desde v2.24

**Lote que lo motiva:** Lote 2.3 (HU-022) · validación y cierre

| Área | Cambio |
|---|---|
| Corrección: NLP en local | El pipeline local se congelaba unos segundos después de cargar los modelos: D4 sin filas en `enriched/` y D6 (el `check` del 2.1) en falla. **Causa:** `share_model_across_processes() = True` en el `BundleBasedDirectRunner`, que tiene un solo hilo de trabajo y rehace el DoFn en cada *bundle*. El servidor de `MultiProcessShared` vive en un hilo del mismo proceso, y el `_decref` de los *proxies* viejos termina bloqueando el hilo que acepta conexiones. **Arreglo:** el modelo se comparte solo en Dataflow (`share_across_processes = cfg.is_dataflow`), y en local `load_model()` lo carga una vez por proceso; sin esa caché se recargaba 33 veces en 3 min. Se reprodujo con Beam 2.77 y el código real (modelo falso con el costo de `nlp_bench.py`). 4 pruebas nuevas en `test_nlp_local.py`; dos fallan con el código de v2.24. Detalle en §3.15, "NLP en local". |
| Verificación local | D1–D6 OK con `validate_2_3.py local`. Imagen con prueba de humo OK (`robertuito-sentiment-analysis@a2cc0f67ebd7`, `robertuito-emotion-analysis@bcd6835f4d1a`), `nlp_model_loaded × 2`, `verify_enriched.py --local --expect-nlp` OK y `local_dataflow.sh check` OK con la imagen de NLP. |
| Verificación GCP | E2–E9 OK con `validate_2_3.py cloud`. El job usa la imagen `dataflow-nlp` publicada; `nlp_model_loaded × 2` y 0 errores. En BigQuery: una sola versión, 0 filas sin NLP y p95 de emisión a BigQuery de 27,4 s. El centinela llegó a BigQuery con NLP a los 5 s (criterio de HU-022: < 1 min). Con `verify_enriched.py --hours 0.35`: 473 / 469 / 473 filas en `enriched/`, cobertura `raw → enriched` y `enriched → BigQuery` del 100 %, NLP en todas las filas, 0 copias, `user_hash` 1 a 1 en 413 usuarios y acuerdo con `label_sentiment` de 90–92 % (solo un dato). Evidencia en `evidencia/validacion_2_3.json`, `nlp_report.json`, `nlp_report_cobertura.json` y `streaming_check_2_3.json`. |
| Verificador | `enriched_check.py`: la cobertura `raw → enriched` ignora las ventanas de `raw/` que empiezan antes de la ventana revisada, que pueden ser de un job anterior sin la rama o sin NLP (prueba `test_raw_anterior_a_la_ventana_no_cuenta_en_la_cobertura`). `validate23.py`: E4 busca la imagen en toda la descripción del job (antes daba `AVISO` con `sdkContainerImage None`); E7 cuenta desde el arranque del job y espera a tener una ventana completa de `raw/` (antes la ventana de 10 min era igual al asentamiento y la cobertura salía en aviso); E2 distingue el permiso nuevo de la deriva del simulador. 3 pruebas nuevas en `test_ops_validate23.py`. |
| Licencias (E11) | Las *model cards* no declaran licencia. pysentimiento advierte que sus modelos quedan sujetos a las licencias de sus datos (TASS 2020; la emoción, también EmoEvent), muchas de uso no comercial. Es compatible con el uso académico del proyecto (ADR-05). Citas en §3.15. |
| Operación | Se documentan en ARCHITECTURE (§0, §3.15, §5.11 y §6) los scripts de `src/ops/` (`on.py`, `off.py`, `status.py`, `validate_2_3.py`) y `docs/ACTIVE_SERVICES.md`, que no figuraban. `validate_2_3.py`: D4 reintenta y diagnostica el contenedor, y D6 muestra el error real sin el ruido de `docker compose`. |
| Estado | Lote 2.3 ✅ en PLAN.md §3. HU-022 ✅, y HU-020 y HU-021 ✅ (Lote 2.2, cerrado en v2.24). La Fase 2 sigue en 🟨; el siguiente es el **2.4**. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. §5.1 suma `TA_NLP_BATCH_SECONDS` y `TA_NLP_THREADS` (v2.24 los usaba sin listarlos). §5.12 aclara el modelo por VM solo en Dataflow y la etiqueta de la imagen. ADR-19 ampliado. |
| Guías de validación | Pasan a `docs/validation/` (`validation-2-2.md` y `validation-2-3.md`). §8 fija la convención para cada lote que se valida: guía en `docs/validation/validation-{fase}-{lote}.md`, script en `project/scripts/validate_{fase}_{lote}.py`, lógica y pruebas en `project/src/ops/` y evidencia en `project/evidencia/`. Las entradas anteriores de este CHANGELOG conservan las rutas viejas. |
| Documento | §0, §3.14, §3.15, §5.1, §5.11, §5.12, §6, §8 (validación) y §9 (ADR-19 y riesgos); `docs/validation/validation-2-3.md` (conteos esperados, E4, E7 y problemas); PLAN.md §3; HU.md (HU-020 a HU-022). |
| Entorno | En WSL con el repo en `/mnt/c`, un `FileNotFoundError` en `from pathlib import Path` significa que la carpeta actual de la terminal quedó rota por cambios hechos desde Windows; se arregla volviendo a entrar con `cd`. `build_nlp_image.sh` etiquetó la imagen con la fecha UTC porque `git rev-parse` no respondió en esa terminal. |
| Pendiente (no bloquea) | Anotar las cifras de las partes B y C y el pico de memoria del worker (E10). Medir en el 2.4 la latencia `event_time → process_time` (p95 de 134–152 s en esta corrida, probablemente por el *backlog* del cambio de job). `roles/pubsub.viewer` para `sa-dataflow` (Lote 2.4). Siguen abiertos los de v2.21 y v2.17. |

## v2.24 · desde v2.23

**Lote que lo motiva:** Lote 2.2 (HU-020, HU-021) · validación y cierre; ajustes del Lote 2.3 antes de validarlo

| Área | Cambio |
|---|---|
| Verificación del 2.2 | **Pruebas:** del lote, 67 passed y 0 omitidas con `pipeline.py`, `common/` y `contracts/` reales; `streaming/` 158 passed (91 + 67); `contracts/tests` 18; `common/` + simulador (entorno `ta`) 116. **Local:** `verify_enriched.py --local` OK (262 / 259 / 267 filas, 0 copias, cobertura `raw → enriched` del 100 %, 46 usuarios con hash 1 a 1, p95 de 5,7 a 7,1 s); `local_dataflow.sh check` OK (11/11). **GCP:** `verify_enriched.py` OK, también en BigQuery, con cobertura `enriched → BigQuery` del 100 % (551 / 543 / 552). En BigQuery, en 1 h: ids únicos, `hash_malo = 0`, `shares` solo en Facebook, NLP en nulo, ground truth en el 71 %, sin columnas `user_id`/`handle`, `payload_con_identidad = 0` y p95 de 5 s. `verify_streaming.py check --allow-cloud` OK (13/13). Evidencia en `evidencia/local_enriched_report.json`, `enriched_report.json` y `streaming_check_2_2.json`. Detalle en §3.14. |
| Correcciones del 2.2 | Fuera de Dataflow, la rama enriched se apaga sola si no tiene salida (`enriched_disabled`), para no romper las pruebas del 2.1 que usan `TestStream` sin `--output_root`. El equipo cambió `local/dataflow.Dockerfile` para instalar con `uv` y caché de BuildKit. |
| Observaciones del 2.2 | El primer job no recibió datos porque el simulador estaba apagado, y por eso no había `hmac_key_loaded` (`docs/ACTIVE_SERVICES.md`). `enriched_config` se registra en la terminal del launcher, no en Cloud Logging (corregido en `validation-2-2.md`). Los workers avisan `PERMISSION_DENIED` al leer la configuración de `ta-raw-*-df`: a `sa-dataflow` le falta `roles/pubsub.viewer` (se declara en el 2.4). `test_routes_and_three_workers` se cuelga en el entorno `ta-streaming` y pasa en `ta`. |
| Lote 2.3 (antes de validarlo) | Lotes de NLP de `--nlp_batch_size` (16) con espera máxima de `--nlp_batch_seconds` (5 s; `TA_NLP_BATCH_SECONDS`), usando la agrupación con estado de `BatchElements`. Torch usa todos los núcleos (`TA_NLP_THREADS`). Motivo: `scripts/nlp_bench.py` midió en la máquina del equipo ~3 comentarios/s con lote 1 frente a ~23/s con lote 16, para ~1,5/s de entrada, y con lote 1 el pipeline local acumuló un retraso creciente (p95 de 78 min). Pruebas nuevas de hilos y de lotes por tiempo. |
| Documento | §3.14 con el cierre del 2.2; §3.15 y §5.12 con los lotes por tiempo. En esa entrega no se actualizaron este CHANGELOG, PLAN.md ni HU.md: se ponen al día en v2.25. |
| Contratos | `contracts/` **sin cambios**; `schema_version` sigue en `"1.0"`. |

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
| Lote 2.2 | Entregado: `src/common/{enriched_row,parquet_rows,hmac_key}.py`, `src/streaming/{enriched,enriched_check,enriched_cli,enriched_testing}.py`, `src/streaming/ENRICHED.md`, `scripts/verify_enriched.py`, `scripts/hmac_key.sh`, Terraform (`modules/bigquery_stream`, `modules/hmac_secret` y `streaming.tf`, `variables_streaming.tf`, `outputs_streaming.tf` en `environments/dev`) y 66 pruebas en `src/streaming/tests/test_enriched_*.py`. Detalle en **§3.14**. |
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