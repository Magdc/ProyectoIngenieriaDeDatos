# TrendAnalyzer · PLAN

> Plan por fases y lotes, matriz de trazabilidad Fase · Lote → HU y estado del proyecto. Alineado con `ARCHITECTURE.md` **v2.15**.
> **Convención de actualización:** cerrar un lote = tocar **solo §3** de este archivo (la fila del lote). Cambiar una HU (alta, baja o reasignación) = actualizar `HU.md` y **§2** en el mismo commit.
> Este archivo no define contratos: para cualquier interfaz (formatos, nombres, topics, tablas, IAM) la fuente es **§5 de [`ARCHITECTURE.md`](ARCHITECTURE.md)**. Historial de versiones: [`CHANGELOG.md`](CHANGELOG.md).

---

## 1. Plan por fases (lotes)

> El plan se organiza en fases y lotes, sin fechas: lo que importa es el orden y las dependencias. Hay dos entregas: **Sprint 2** y **Sprint Final** (ver los marcadores ★ y §2). Responsables sugeridos según el README: **M** = Miguel (infra/IAM/CI-CD), **C** = Camila (pipelines), **A** = Alejandro (simulador/calidad). Las fases 1, 2 y 3 se pueden trabajar en paralelo una vez cerrada la Fase 0.

**Trazabilidad:** cada fase y cada lote indica las **HU** que cubre (definidas en `HU.md`, con criterios de aceptación). Una fase se considera cerrada cuando todas sus HU cumplen sus criterios de aceptación y su estado se marca en §3. La matriz completa está en §2.

Cada **lote** es una petición mía de tamaño razonable: yo entrego un conjunto cerrado de archivos con pruebas, ustedes los integran y validan, y pasamos al siguiente.

### Fase 0 · Alineación (todos)
**HU cubiertas:** HU-001, HU-002, HU-003

**Objetivo:** congelar decisiones y contratos.
- **Lote 0.1** → `contracts/*`, README actualizado (hecho en v2.6), `docs/trend_analyzer_multicloud.drawio` actualizado a la v2.5 (hecho en v2.6). · **HU:** HU-001, HU-002
- **Ustedes me dan:** respuestas a §9 de `ARCHITECTURE.md` (preguntas abiertas), estado real de `infra/` (qué ya está aplicado), proyecto GCP y región.
- **Listo cuando:** ADRs aprobados y contratos en el repo.

### Fase 1 · Simulador (A, con M para infra)
**HU cubiertas:** HU-004, HU-008, HU-009, HU-010, HU-011, HU-012, HU-013, HU-014, HU-015, HU-016, HU-017, HU-046, HU-047

- **Lote 1.0** → *bootstrap* de Terraform: configuración del proyecto GCP existente (región `us-central1`), APIs habilitadas, estado remoto en GCS y S3, `terraform.tfvars.example`, presupuesto mínimo. · **HU:** HU-046
- **Lote 1.1** → `src/common/` (envelope, hashing, label_map, `normalize_hashtag()`) + adaptación del generador de comentarios (§3.2: función pura con `rng`, parámetros `marca`/`sentimiento`, marca canónica) y `scripts/generate_comments.py`. · **HU:** HU-008, HU-009
- **Lote 1.2** → núcleo del simulador (cursor, selección de marca con pesos, usuarios, engagement por red, hashtags, caos, publisher asyncio) + tests unitarios (incluye test de determinismo: misma semilla + `seq` ⇒ mismo `event_id` y mismo texto; y test de reanudación del cursor). · **HU:** HU-010, HU-011, HU-012, HU-013, HU-014, HU-015
- **Lote 1.3** → `Dockerfile` + Terraform GCP: Cloud Run `min=max=1` sin throttling, topics, suscripciones, DLQ, buckets, service accounts. · **HU:** HU-004, HU-016
- **Lote 1.4** → script de verificación: se suscribe y reporta tasa de mensajes, ids únicos y `seq`. · **HU:** HU-017
- **Lote 1.5** → entorno local: `docker-compose` con emulador de Pub/Sub, simulador y consumidor de validación contra los schemas. · **HU:** HU-047
- **Listo cuando:** los 3 topics reciben ~0.5 msg/s cada uno, el cursor sobrevive a un reinicio, y el test de determinismo pasa.

### Fase 2 · Streaming en GCP (C)
**HU cubiertas:** HU-005, HU-018, HU-019, HU-020, HU-021, HU-022, HU-023

- **Lote 2.1** → esqueleto Dataflow: lectura, validación, dedup por `event_id`, DLQ, escritura a `raw/`. · **HU:** HU-018, HU-019
- **Lote 2.2** → anonimización HMAC + normalización de engagement + sink a BigQuery `posts_enriched` + `enriched/` Parquet. · **HU:** HU-020, HU-021
- **Lote 2.3** → NLP con `RunInference` (contenedor personalizado) + `nlp_model_version`. · **HU:** HU-022
- **Lote 2.4** → ventanas `trend_windows` + Terraform (BigQuery, plantilla de Dataflow, secret HMAC). · **HU:** HU-005, HU-023
- **Listo cuando:** un evento emitido aparece en BigQuery con `emotion_label` en < 1 min, y p95 de latencia < 5 min.

### Fase 3 · Histórico y puente a AWS (A + M)
**HU cubiertas:** HU-006, HU-024, HU-025

- **Lote 3.1** → `historical/`: usa el generador en batch (el mismo código del simulador, con otra semilla) para producir N registros (por defecto ~50k, configurable) repartidos entre `reddit`, `facebook` e `instagram`, con fechas en los últimos 180 días (estacionalidad semanal/horaria y tendencias de marca, para que haya historia que comparar), mismo envelope, anonimiza, corre el NLP en lote y escribe Parquet a `raw/origin=historical/`. · **HU:** HU-024
- **Lote 3.2** → exportador `gcs-to-s3` + federación OIDC GCP→AWS + Terraform en ambas nubes. · **HU:** HU-006, HU-025
- **Listo cuando:** datos de ambos orígenes aparecen en `s3://…/raw/`.

### Fase 4 · Lakehouse AWS (C + M)
**HU cubiertas:** HU-007, HU-026, HU-027, HU-028, HU-029, HU-030

- **Lote 4.1** → Terraform: Glue DBs, tablas con partition projection, workgroup Athena, EventBridge, KMS/IAM. · **HU:** HU-007, HU-029
- **Lote 4.2** → Glue job `raw→trusted` (unión, dedup, tipos, limpieza). · **HU:** HU-026
- **Lote 4.3** → `trusted→curated` (dbt-athena o Glue): hechos, dimensiones, agregados. · **HU:** HU-027, HU-030
- **Lote 4.4** → tablas de aristas para análisis de relaciones. · **HU:** HU-028
- **Listo cuando:** `SELECT` en Athena sobre `ta_curated.fact_posts` devuelve datos de stream e histórico.

### Fase 5 · Calidad y gobernanza (A, en paralelo)
**HU cubiertas:** HU-031, HU-032, HU-033, HU-034, HU-035, HU-036, HU-048

- **Lote 5.1** → *profiling* (US-14) sobre Raw/Trusted. · **HU:** HU-031
- **Lote 5.2** → suites de Great Expectations + tests dbt según §5.10 de `ARCHITECTURE.md`; escritura en `dq_results`. · **HU:** HU-032, HU-033
- **Lote 5.3** → catálogo (descripciones de tablas/columnas en Glue), documento de linaje (dbt docs + diagrama), mapeo DAMA-DMBOK y nota Ley 1581 (los datos son sintéticos; igualmente se documenta el tratamiento). · **HU:** HU-034, HU-035, HU-036
- **Lote 5.4** → `scripts/smoke_e2e.py`: prueba de humo del flujo completo con evento centinela, latencia por etapa y evidencia para el informe. · **HU:** HU-048

> ### ★ Entrega Sprint 2
> Debe poder demostrarse: simulador → Pub/Sub → Dataflow → BigQuery; exportación a S3; Glue Raw→Trusted→Curated; consulta en Athena; reportes de calidad; catálogo y linaje documentados.

### Fase 6 · Serving, alertas y escenarios (C + A)
**HU cubiertas:** HU-037, HU-038, HU-039, HU-040

- **Lote 6.1** → datasets y dashboard de QuickSight (guía + definiciones). · **HU:** HU-037, HU-038
- **Lote 6.2** → alertas: `trend_windows.is_alert` → `ta-alerts` → notificación. · **HU:** HU-039
- **Lote 6.3** → endpoint `/control/scenario` y guion de demo de picos. · **HU:** HU-040

### Fase 7 · CI/CD, observabilidad y control de costos (M)
**HU cubiertas:** HU-041, HU-042, HU-043

- **Lote 7.1** → GitHub Actions: tests, `terraform fmt/validate/plan`, build y push de imágenes. · **HU:** HU-041
- **Lote 7.2** → dashboards/alertas de Cloud Monitoring y CloudWatch; métricas de latencia y DLQ. · **HU:** HU-042
- **Lote 7.3** → scripts de encendido/apagado (Dataflow y simulador) y alertas de presupuesto. · **HU:** HU-043

### Fase 8 · Informe y sustentación (todos)
**HU cubiertas:** HU-044, HU-045

- **Lote 8.1** → estructura del informe técnico, tabla de KPIs medidos, guion de sustentación, runbook. · **HU:** HU-044, HU-045
> ### ★ Sprint Final

---

## 2. Matriz de trazabilidad Fase · Lote → HU

> Fuente de verdad de las HU: `HU.md`. Si se agrega o reasigna una HU, se actualiza `HU.md` y esta §2 en el mismo commit.

| Fase | Lote | HU | Resp. |
|---|---|---|---|
| 0 | 0.1 | HU-001, HU-002 | Todos |
| 0 | — | HU-003 | Todos |
| 1 | 1.0 | HU-046 | M |
| 1 | 1.1 | HU-008, HU-009 | A |
| 1 | 1.2 | HU-010, HU-011, HU-012, HU-013, HU-014, HU-015 | A |
| 1 | 1.3 | HU-004, HU-016 | M + A |
| 1 | 1.4 | HU-017 | A |
| 1 | 1.5 | HU-047 | A |
| 2 | 2.1 | HU-018, HU-019 | C |
| 2 | 2.2 | HU-020, HU-021 | C |
| 2 | 2.3 | HU-022 | C |
| 2 | 2.4 | HU-005, HU-023 | C + M |
| 3 | 3.1 | HU-024 | A |
| 3 | 3.2 | HU-006, HU-025 | M |
| 4 | 4.1 | HU-007, HU-029 | M |
| 4 | 4.2 | HU-026 | C |
| 4 | 4.3 | HU-027, HU-030 | C |
| 4 | 4.4 | HU-028 | C |
| 5 | 5.1 | HU-031 | A |
| 5 | 5.2 | HU-032, HU-033 | A |
| 5 | 5.3 | HU-034, HU-035, HU-036 | A |
| 5 | 5.4 | HU-048 | A |
| 6 | 6.1 | HU-037, HU-038 | C |
| 6 | 6.2 | HU-039 | C |
| 6 | 6.3 | HU-040 | A |
| 7 | 7.1 | HU-041 | M |
| 7 | 7.2 | HU-042 | M |
| 7 | 7.3 | HU-043 | M |
| 8 | 8.1 | HU-044, HU-045 | Todos |

**Cobertura:** 48 HU (HU-001 a HU-048), cada una asignada a exactamente un lote o a la Fase 0. HU de Sprint 2: HU-001 a HU-036 y HU-046 a HU-048. HU de Sprint Final: HU-037 a HU-045.

**Equivalencia con las US del README:** US-10 → HU-010 · US-11 → HU-011 · US-12 → HU-012 · US-13 → HU-032 · US-14 → HU-031. *(El README v2.2 ya no presenta US-10/11/12 como conectores externos.)*

> ✅ **`HU.md` alineado con la v2.2** (HU-008 a HU-014, HU-022, HU-024, HU-033 y HU-040 reescritas; HU-010 = Facebook, HU-011 = Reddit, HU-012 = Instagram).

---

## 3. Estado del proyecto

> Única tabla de estado (✅ cerrado · 🟨 en curso · ⬜ pendiente). Las filas en negrita son las fases y los hitos; debajo de cada fase van sus lotes. Las notas de lote concentran lo que antes se repetía en el plan y en el estado: qué se cerró, con qué verificación y qué contratos se tocaron. Al cerrar un lote solo se edita su fila (y la de su fase si cambia).

| Fase · Lote | Estado | Resp. | HU | Notas |
|---|---|---|---|---|
| **0 · Alineación** | ✅ | Todos | HU-001 a HU-003 | Proyecto GCP `project-0568d9ba-848f-413c-88b` y región `us-central1` confirmados con cuotas verificadas; Terraform se autentica en GCP con Application Default Credentials; `contracts/` revisado (HU-001); HU-002 hecha; ADR-02, ADR-12 y ADR-13 aprobados; §9 #8 y #9 aprobados. Diferido (no bloquea): accesos de Miguel y Camila, conector QuickSight → BigQuery y prueba de sincronización (Fase 5) |
| 0.1 | ✅ | Todos | HU-001, HU-002 | `contracts/*`, README y `docs/trend_analyzer_multicloud.drawio` (v2.5) actualizados |
| — | ✅ | Todos | HU-003 | Sin lote propio; cubierta por el cierre de la Fase 0 |
| **1 · Simulador** | 🟨 | A (+M) | HU-004, HU-008 a HU-017, HU-046, HU-047 | Lotes 1.0, 1.1 y 1.2 cerrados; el siguiente es el **1.3** |
| 1.0 | ✅ | M | HU-046 | Cerrado en v2.12 (código y guía en `project/infra/`; contrato en `ARCHITECTURE.md` §5.14). `validate.sh` en verde, facturación confirmada, bootstraps de GCP y AWS aplicados y entornos `dev` conectados al estado remoto, con `plan` sin recursos. Valores reales en `ARCHITECTURE.md` §5.9. **Contratos tocados:** ninguno de §5.2–§5.8; se añadió §5.14 y se ampliaron §5.1, §5.8, §5.9, §5.11 y §5.12 |
| 1.1 | ✅ | A | HU-008, HU-009 | Cerrado en v2.14 (código en `project/src/common/`, `project/src/simulator/generator/` y `project/scripts/`; descripción en `ARCHITECTURE.md` §3.2.2). `python -m pytest`: 64 passed, 0 omitidas; `python -m pytest contracts/tests -q`: 18 passed. Parámetros del generador adoptados por defecto (§5.12). **Abierto, sin bloquear:** confirmar los criterios de aceptación de HU-008 y HU-009 contra `HU.md` (§9 #11). **Contratos tocados:** ninguno de §5.2–§5.8 ni `contracts/`; se completaron notas en §5.2 (`normalize_hashtag()`), §5.11, §5.12 y §5.13. **Archivos que §6 no preveía:** `config/brands.yaml`, `common/label_map.yaml`, `pytest.ini` y `src/conftest.py` |
| 1.2 | ✅ | A | HU-010 a HU-015 | Cerrado en v2.15 (código en `project/src/simulator/`; descripción en `ARCHITECTURE.md` §3.10). Desde la raíz: `python -m pytest -c project/pytest.ini -rs`: **134 passed, 0 omitidas** (incluye `test_simulator_contract.py`, 1.000 eventos por fuente contra `contracts/`); `python -m pytest project/contracts/tests -q`: 18 passed. **Contratos tocados:** ninguno de §5.2–§5.8 ni `contracts/`; §5.1 se amplía con variables propias del simulador. **Archivos que §6 no preveía:** `factory.py`, `sim_config.py` y `tests/_sim_helpers.py` |
| 1.3 | ⬜ | M + A | HU-004, HU-016 | |
| 1.4 | ⬜ | A | HU-017 | |
| 1.5 | ⬜ | A | HU-047 | |
| **2 · Streaming GCP** | ⬜ | C | HU-005, HU-018 a HU-023 | |
| 2.1 | ⬜ | C | HU-018, HU-019 | |
| 2.2 | ⬜ | C | HU-020, HU-021 | |
| 2.3 | ⬜ | C | HU-022 | |
| 2.4 | ⬜ | C + M | HU-005, HU-023 | |
| **3 · Histórico + puente** | ⬜ | A + M | HU-006, HU-024, HU-025 | |
| 3.1 | ⬜ | A | HU-024 | |
| 3.2 | ⬜ | M | HU-006, HU-025 | |
| **4 · Lakehouse AWS** | ⬜ | C + M | HU-007, HU-026 a HU-030 | |
| 4.1 | ⬜ | M | HU-007, HU-029 | |
| 4.2 | ⬜ | C | HU-026 | |
| 4.3 | ⬜ | C | HU-027, HU-030 | |
| 4.4 | ⬜ | C | HU-028 | |
| **5 · Calidad y gobernanza** | ⬜ | A | HU-031 a HU-036, HU-048 | |
| 5.1 | ⬜ | A | HU-031 | |
| 5.2 | ⬜ | A | HU-032, HU-033 | |
| 5.3 | ⬜ | A | HU-034, HU-035, HU-036 | |
| 5.4 | ⬜ | A | HU-048 | |
| **★ Sprint 2** | ⬜ | | HU-001 a HU-036, HU-046 a HU-048 | |
| **6 · Serving y alertas** | ⬜ | C + A | HU-037 a HU-040 | |
| 6.1 | ⬜ | C | HU-037, HU-038 | |
| 6.2 | ⬜ | C | HU-039 | |
| 6.3 | ⬜ | A | HU-040 | |
| **7 · CI/CD y costos** | ⬜ | M | HU-041 a HU-043 | |
| 7.1 | ⬜ | M | HU-041 | |
| 7.2 | ⬜ | M | HU-042 | |
| 7.3 | ⬜ | M | HU-043 | |
| **8 · Informe** | ⬜ | Todos | HU-044, HU-045 | |
| 8.1 | ⬜ | Todos | HU-044, HU-045 | |
| **★ Sprint Final** | ⬜ | | HU-037 a HU-045 | |