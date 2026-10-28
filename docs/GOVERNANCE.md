# TrendAnalyzer · Gobernanza de datos (DAMA-DMBOK y Ley 1581)

> **HU-036, Lote 5.3 (v2.43).** Mapeo de las áreas de conocimiento de DAMA-DMBOK a los componentes del proyecto, nota
> de cumplimiento de la Ley 1581 de 2012 y declaración de datos sintéticos. Decisiones en `ARCHITECTURE.md` §3.25 y
> ADR-29; catálogo en [`CATALOG.md`](CATALOG.md); linaje en [`LINEAGE.md`](LINEAGE.md). Los controles marcados «(G…)»
> los comprueba `validate_5_3.py controls` en la nube (solo lectura); el resultado está en la sección 6.
> Este documento es académico: **no es una opinión legal**.

## 1. Los datos son sintéticos (declaración)

**Todos los datos del proyecto son sintéticos.** No hay comentarios, usuarios ni cuentas reales de Reddit, Facebook o
Instagram: el simulador (`project/src/simulator/`) inventa cada comentario con plantillas y un sentimiento de verdad, y
los autores salen de un pool fijo de 2.000 usuarios por red generado con `Faker('es_CO')` (semilla 1581): `user_id`
(`u_000000`…), `handle` y seguidores son inventados. Lo mismo vale para la carga histórica (50.000 filas, el mismo
generador con otra semilla).

**Limitaciones que hay que leer con cualquier cifra del proyecto:**
- **Comentarios de plantillas con vocabulario acotado:** el texto sale de un conjunto finito de plantillas, marcas,
  emojis y hashtags; la diversidad léxica es baja (medida en el profiling del 5.1, §3.20) y los textos se repiten.
- **Asociación marca → sentimiento artificial:** la marca y el sentimiento los elige el simulador (con tendencias y
  escenarios programados, §3.17), así que las conclusiones de negocio («a la gente le gusta más X») no son válidas.
- **Métricas de NLP optimistas:** RoBERTuito acierta 0,9197 (F1 macro 0,9167) sobre 109.365 filas (HU-033, §3.24) porque
  las plantillas son claras y repetitivas; con comentarios reales la precisión sería menor. `label_sentiment` es la
  verdad del simulador y nunca se usa como feature.
- El objetivo del proyecto es la **ingeniería de datos** (ingesta, calidad, linaje, latencia, costos), no el análisis de
  opinión real.

Aun así, el proyecto **trata los datos como si fueran personales** (anonimización, acceso restringido, minimización):
la sección 4 lo documenta frente a la Ley 1581.

## 2. Mapeo DAMA-DMBOK

DAMA-DMBOK (2.ª edición) organiza la gestión de datos en 11 áreas de conocimiento con el gobierno de datos al centro.

| Área de conocimiento | Qué pide | Cómo lo cubre el proyecto | Evidencia | Brechas |
|---|---|---|---|---|
| Gobierno de datos | Decisiones, roles, políticas y control sobre los datos | Los documentos son la fuente de verdad: contratos en `ARCHITECTURE.md` §5 («regla de oro»: una interfaz cambia primero ahí), decisiones como ADR (§9), estado por lote en `PLAN.md` §3, historias con criterios en `HU.md`; cada lote se cierra con un validador sobre un commit limpio | `docs/ARCHITECTURE.md`, `docs/PLAN.md`, `docs/CHANGELOG.md`, `docs/validation/` | Sin comité ni política formal escrita aparte; responsables por lote (A, C, M en `PLAN.md`) |
| Arquitectura de datos | Modelo de referencia, flujos y capas | Arquitectura multinube con capas Raw → Trusted → Curated (medallón) y diagrama | `ARCHITECTURE.md` §2 y §4, `docs/trend_analyzer_multicloud.drawio`, `LINEAGE.md` | — |
| Modelado y diseño de datos | Modelos conceptuales, lógicos y físicos | Envelope canónico versionado (JSON Schema), esquema estrella de Curated con llaves naturales, aristas para relaciones | `project/contracts/envelope.v1.schema.json`, `project/contracts/athena/curated.sql`, §5.2, §5.7 | — |
| Almacenamiento y operaciones de datos | Persistencia, respaldo, retención y operación | Parquet + Snappy en S3 con SSE-KMS; BigQuery con particiones y expiración; retención acordada (§5.12); jobs idempotentes cada hora; encendido y apagado con `on.py`/`off.py`/`status.py` | `project/infra/aws/modules/s3_lake/`, `docs/ACTIVE_SERVICES.md`, §5.12 | El lake de S3 no tiene versionado: un borrado por error no se recupera (Raw se puede regenerar desde GCS `enriched/` o el loader) |
| Seguridad de datos | Confidencialidad, acceso mínimo, cifrado y auditoría | Anonimización HMAC en el primer paso (ADR-06), roles de mínimo privilegio por prefijo (§5.8), bucket privado con SSE-KMS y TLS obligatorio, federación OIDC sin llaves estáticas, llave HMAC en Secret Manager | §5.8, `project/src/common/hashing.py`, `project/infra/aws/modules/glue_role/`, controles G1–G7 | El desarrollador opera AWS con la **cuenta raíz** (aviso en todos los validadores); Lake Formation en modo IAM; la llave HMAC no se rota |
| Integración e interoperabilidad de datos | Mover y transformar datos entre sistemas | Pub/Sub → Dataflow → BigQuery/GCS; exportador GCS → S3 con OIDC; jobs de Glue Raw → Trusted → Curated; contratos verificados por pruebas | `project/src/streaming/`, `project/src/exporter/`, `project/src/lakehouse/glue_jobs/`, `project/contracts/tests/` | — |
| Gestión de documentos y contenido | Datos no estructurados y su ciclo de vida | El texto libre (`text`) se conserva en Raw y Trusted, normalizado en `text_clean`; Curated no lleva texto; `raw/` de GCS (con el handle simulado) expira a los 30 días | §5.7 (`text-clean@1`), §5.12, `CATALOG.md` (clase `texto_libre`) | No hay detección de datos personales dentro del texto (en datos reales haría falta) |
| Datos de referencia y maestros | Listas controladas y entidades maestras | Marcas en `brands.yaml` → `dim_brand`; redes en `sim.yaml` → `dim_source`; calendario `dim_date`; autor maestro seudonimizado `dim_user`; catálogos de etiquetas de NLP | `project/src/simulator/config/brands.yaml`, `project/src/simulator/config/sim.yaml`, `project/src/common/label_map.yaml` | — |
| Data warehousing e inteligencia de negocio | Modelo analítico y consumo | Curated en Athena (workgroup `ta-analytics` con límite de 1 GiB por consulta) y consultas guardadas; tablero de QuickSight con SPICE (Lote 6.1: `ta-tablero-tendencias`, 4 datasets sobre `ta_curated`, horas en Bogotá, refresco cada hora) | `project/src/lakehouse/queries/`, `project/src/serving/`, §3.22.1, §3.27, §5.7 | — |
| Metadatos | Catálogo, diccionario y linaje | Glue Data Catalog con descripción de tabla, comentario por columna y clase de privacidad (HU-034); diccionario generado; linaje documentado (HU-035); reglas de calidad catalogadas | `CATALOG.md`, `LINEAGE.md`, `project/infra/aws/modules/glue_catalog/tables.json`, §5.10 | El linaje es manual (sin OpenLineage); Glue no guarda el linaje |
| Calidad de datos | Dimensiones, reglas, medición y mejora | 77 reglas (DQ-01…DQ-77) con KPI (completitud, unicidad, duplicados, anonimización, latencia, precisión del NLP) medidas con Great Expectations en Raw, Trusted y Curated; profiling; conciliación dentro de los jobs | `project/src/dq/`, `project/src/quality/`, §3.20, §3.24, §5.10 | GX corre a demanda, no en el horario |

## 3. Clasificación de los datos

Cada columna del catálogo lleva una de estas clases (al inicio de su comentario en Glue; detalle en `CATALOG.md`):

| Clase | Columnas | Tratamiento |
|---|---|---|
| `anonimizado` | `user_hash` en `ta_raw.posts_raw`, `ta_trusted.posts`, `ta_curated.fact_posts`, `dim_user` y `edges_user_brand` (5) | Seudónimo HMAC-SHA256; nunca se publica en evidencia, reportes de calidad ni logs |
| `cuasi_identificador` | `followers`, `payload` (Raw y Trusted) y la actividad de `dim_user` (`first_seen`, `last_seen`, `n_posts`, `max_followers`) (8) | No salen de las capas del lake; las salidas de calidad solo llevan agregados |
| `texto_libre` | `text` (Raw y Trusted) y `text_clean` (Trusted) (3) | Curated no lo lleva; el profiling solo guarda 5 textos de muestra recortados a 60 caracteres |
| `sin_dato_personal` | El resto (92) | Sin restricción adicional |

El identificador en claro (`user_id`, `handle`) **solo existe** en el simulador, en Pub/Sub (7 días), en GCS `raw/`
(30 días) y en la memoria de Dataflow y del loader; ninguna tabla del catálogo lo tiene (lo exige una prueba del 5.3 y DQ-02/DQ-65).

## 4. Nota de cumplimiento: Ley 1581 de 2012

La Ley Estatutaria 1581 de 2012 (protección de datos personales en Colombia; reglamentada por el Decreto 1377 de 2013,
hoy en el Decreto 1074 de 2015) se aplica al tratamiento de datos de personas naturales. **Aquí no hay titulares reales**
(sección 1), así que la ley no se activa; la nota documenta cómo se trataría el dato si lo fuera.

**Principios (artículo 4) → cómo se cumplirían.**

| Principio | Control en el proyecto |
|---|---|
| Legalidad y finalidad | Finalidad única y declarada: análisis agregado de menciones de marca y su sentimiento; ningún uso individual del autor |
| Libertad (autorización) | No aplica a datos sintéticos; con datos reales haría falta autorización del titular o una de las excepciones de la ley (p. ej. datos de naturaleza pública), *a evaluar caso por caso* |
| Veracidad o calidad | Reglas de calidad y KPI medidos (§5.10, §3.24); deduplicación y conciliación en cada capa |
| Transparencia | Este documento, el catálogo y el linaje (repositorio público) |
| Acceso y circulación restringida | Acceso por rol y prefijo (abajo); solo `enriched/` (anonimizado) sale de GCP |
| Seguridad | Cifrado en reposo (SSE-KMS en S3, cifrado por defecto de Google) y en tránsito (TLS obligatorio), mínimo privilegio, sin llaves estáticas entre nubes |
| Confidencialidad | El repositorio y la evidencia no llevan secretos, `user_hash`, textos completos ni URLs prefirmadas (regla de todos los validadores) |

**Anonimización.** `user_hash = HMAC-SHA256(llave, user_id)` en hex, calculado en el **primer paso** de cada plano
(Dataflow en el stream, el loader en el histórico; ADR-06): el dato en claro nunca cruza de nube. La llave vive en
Secret Manager (`ta-hmac-key`) y solo `sa-dataflow` puede leerla (G7); el loader la lee con las credenciales del
desarrollador. Técnicamente es **seudonimización**, no anonimización irreversible: con la llave y la lista de `user_id`
se podría recalcular el vínculo, por eso la llave está restringida y no se rota (rotarla rompería la unión stream ↔
histórico, §5.12). Además, el `text` y el `payload` (ids y enlace del post) permitirían, con datos reales, encontrar al
autor en la red social: por eso están clasificados como `texto_libre` y `cuasi_identificador` y no llegan a Curated.

**Acceso restringido a `raw/`.**
- **GCS `raw/`** (el único lugar persistente con el handle simulado): bucket con acceso uniforme y prevención de acceso
  público (G5), expira a los **30 días** (G5); escribe `sa-dataflow`; `sa-exporter` solo puede leer `enriched/` (condición
  de IAM, §5.8); ningún miembro público en la política del bucket (G6).
- **S3 `raw/`** (ya anonimizado): bucket privado con bloqueo total de acceso público (G1), SSE-KMS (G2) y política que
  niega todo sin TLS (G3). Escriben solo los productores (`ta-gcp-exporter-role` en `raw/origin=stream/`,
  `ta-historical-loader` en `raw/origin=historical/`), sin poder leer ni borrar; `ta-glue-role` lo lee pero no puede
  escribirlo; `ta-quicksight-role` no puede leer `raw/` ni `trusted/`, solo `curated/` (G4, simulado con IAM).

**Minimización.**
- `user_id` y `handle` se descartan al anonimizar; del autor solo pasan `user_hash` y `followers`.
- Solo `enriched/` sale de GCP hacia AWS; Curated no tiene `text`, `text_clean`, `payload` ni `followers` por fila
  (`dim_user` guarda solo el máximo de seguidores por seudónimo).
- Retención limitada (§5.12): Pub/Sub 7 días, GCS `raw/` y `dlq/` 30 días, `posts_enriched` y `trend_windows` 90 días,
  `athena-results/` 7 días.
- Las salidas de calidad, profiling y reportes de los jobs solo llevan conteos y métricas (sin filas).

**Otros puntos de la ley.**
- **Datos sensibles (art. 5):** el proyecto no recoge datos sensibles; en datos reales el texto libre podría traerlos
  (salud, opiniones políticas) y haría falta detectarlos antes de Trusted.
- **Derechos del titular (art. 8):** con datos reales, una solicitud de supresión se atendería recalculando el
  `user_hash` del titular con la llave y borrando sus filas en Raw (Trusted y Curated se reconstruyen solos cada hora);
  hoy no hay procedimiento escrito ni titulares.
- **Transferencia internacional (art. 26):** los datos se procesan en EE. UU. (GCP `us-central1`, AWS `us-east-1`). La
  ley prohíbe transferir datos personales a países sin nivel adecuado de protección; la Circular Externa 005 de 2017 de la
  SIC incluyó a Estados Unidos en su lista de países adecuados. *Sin comprobar contra la Circular Única vigente*: con
  datos reales, confirmar antes de operar.
- **Registro Nacional de Bases de Datos y política de tratamiento:** no aplican a datos sintéticos; con datos reales,
  el responsable debería publicar su política de tratamiento y evaluar si debe registrar la base (*a confirmar*).

**Riesgos y brechas abiertas.** Cuenta raíz de AWS para operar (recomendación: un usuario IAM o SSO con MFA); la llave
HMAC no se rota; Lake Formation en modo IAM (sin permisos por columna); el lake de S3 sin versionado; el pre-commit con
`gitleaks` acordado en §5.12 no está configurado en el repositorio (no hay `.pre-commit-config.yaml`; *sin comprobar*
que exista en otro lugar).

## 5. Roles

| Rol (DAMA) | Quién | Qué hace |
|---|---|---|
| Dueño de los datos | El equipo del curso | Decide finalidad y alcance (HU, ADR) |
| Administrador (*steward*) de calidad y gobernanza | A (Fase 5) | Reglas de calidad, catálogo, linaje y esta nota |
| Custodio técnico | A, C y M según la fase (`PLAN.md`) | Infraestructura, jobs y permisos |
| Consumidores | Analistas: Athena y el tablero de QuickSight (Lote 6.1) por los grupos `ta-autores` (rol IAM `ta-quicksight-autor`) y `ta-lectores` (`ta-quicksight-lector`) | Solo `ta_curated` vía `ta-quicksight-role`; ningún dataset ni visual muestra `user_hash`; los lectores solo ven el tablero (§3.27) |

## 6. Controles verificados (`validate_5_3.py controls`)

Verificado el 27-oct-2026 (cierre del 5.3, `main` = `4a48a61`; evidencia en `project/evidencia/validacion_5_3.json`):

| Paso | Control | Resultado |
|---|---|---|
| G1 | S3 del lake: bloqueo total de acceso público | ✅ los 4 bloqueos activos |
| G2 | S3 del lake: cifrado por defecto SSE-KMS con *bucket key* | ✅ `aws:kms` con la llave del lake y *bucket key* |
| G3 | S3 del lake: la política niega todo sin TLS (`aws:SecureTransport`) | ✅ `Deny` con `aws:SecureTransport = false` |
| G4 | IAM (simulado): quién puede leer y escribir `raw/`, `trusted/` y `curated/` | ✅ 16 casos como se esperaba (los productores solo escriben su prefijo de `raw/`, sin leer ni borrar; Glue lee `raw/` sin escribirlo; QuickSight solo lee `curated/`) |
| G5 | GCS del lake: acceso uniforme, prevención de acceso público y `raw/`/`dlq/` a 30 días | ✅ uniforme, `enforced`, `raw/` y `dlq/` expiran a 30 días |
| G6 | GCS del lake: sin miembros públicos (`allUsers`, `allAuthenticatedUsers`) | ✅ ninguno; solo los roles heredados del proyecto (dueños, editores, lectores) y las service accounts del proyecto |
| G7 | Secret Manager `ta-hmac-key`: solo `sa-dataflow` como `secretAccessor` | ✅ solo `sa-dataflow` (los dueños del proyecto también, por herencia) |
