# TrendAnalyzer

> Plataforma de analisis de tendencias y sentimiento en redes sociales y medios digitales.  
> **Asignatura:** SI4002 -- Proyecto de Ingenieria de Datos · Universidad EAFIT · 2026-2  
> **Profesor:** Jose Fabio Jaramillo Castro

---

## Descripcion

**TrendAnalyzer** centraliza y procesa comentarios sobre una marca, sus productos y sus competidores para apoyar los procesos de investigacion de mercado y toma de decisiones de una empresa colombiana simulada del sector de bebidas y productos de consumo masivo. Las marcas simuladas son **Postobon (marca propia)**, **Pepsi** y **Sprite** (competidores).

Como las APIs reales de Reddit, Facebook e Instagram imponen aprobaciones y limites de acceso, el **streaming se simula**: un generador de comentarios en Python (`marca,comentario`) alimenta tres fuentes simuladas -- **Reddit, Facebook e Instagram** -- que publican a Pub/Sub cada 1-3 s. La plataforma anonimiza, aplica NLP preentrenado en espanol para estimar sentimiento y emocion, identifica tendencias emergentes y genera alertas (por correo) ante cambios en la conversacion digital.

> Todo lo que ocurre despues de Pub/Sub es igual al pipeline que se usaria con fuentes reales; lo unico simulado es el origen del dato.

---

## Documentacion

| Documento | Contenido |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | **Fuente de verdad** (version vigente 2.25): arquitectura, contratos de comunicacion (sec. 5), decisiones (ADR), riesgos y preguntas abiertas |
| [`docs/PLAN.md`](docs/PLAN.md) | Plan por fases y lotes, matriz de trazabilidad Fase · Lote -> HU y estado del proyecto (sec. 3) |
| [`docs/CHANGELOG.md`](docs/CHANGELOG.md) | Historial de versiones (v2.2 -> v2.25), una entrada por version |
| [`docs/HU.md`](docs/HU.md) | Backlog: 48 historias de usuario (HU-001 a HU-048) con criterios de aceptacion |
| [`docs/ACTIVE_SERVICES.md`](docs/ACTIVE_SERVICES.md) | Que cuesta dinero y como encender, comprobar y apagar cada servicio, en local y en GCP |
| [`docs/validation/`](docs/validation/) | Guias de validacion paso a paso, una por lote (`validation-2-2.md`, `validation-2-3.md`) |
| [`project/LOCAL_DEV.md`](project/LOCAL_DEV.md) | Entorno local sin nube: emulador de Pub/Sub, simulador y validador |
| [`project/infra/BOOTSTRAP.md`](project/infra/BOOTSTRAP.md) · [`project/infra/DEPLOY_SIMULATOR.md`](project/infra/DEPLOY_SIMULATOR.md) | Bootstrap de Terraform (Lote 1.0) · base de GCP y despliegue del simulador (Lote 1.3) |
| [`project/src/streaming/README.md`](project/src/streaming/README.md) · [`ENRICHED.md`](project/src/streaming/ENRICHED.md) | Pipeline de streaming (Lote 2.1) · rama enriched: anonimizacion, `enriched/` y BigQuery (Lote 2.2) |
| [`docs/GLOSSARY.md`](docs/GLOSSARY.md) | Glosario del proyecto |
| [`docs/NOTAS.md`](docs/NOTAS.md) | Notas de trabajo |
| [`docs/trend_analyzer_multicloud.drawio`](docs/trend_analyzer_multicloud.drawio) | Diagrama editable en [diagrams.net](https://app.diagrams.net) (v2.5) |
| [`docs/trend_analyzer_multicloud.jpg`](docs/trend_analyzer_multicloud.jpg) | Exportacion del diagrama |

![Arquitectura multicloud](docs/trend_analyzer_multicloud.jpg)

---

## Arquitectura

```
  Reddit · Facebook · Instagram   (simulados)
        │  generador de comentarios:  marca,comentario
        ▼
  Cloud Run  ta-stream-simulator   (1 mensaje cada 1-3 s por fuente)
        │
        ▼
  ┌─ Google Cloud Platform · ingesta y near-real-time ───────────────────┐
  │ Pub/Sub ──► Dataflow (validar · dedup · anonimizar · NLP · ventanas) │
  │               ├──► BigQuery (posts_enriched · trend_windows)         │
  │               └──► Cloud Storage (raw · enriched · dlq)              │
  │ Cloud Scheduler ──► Cloud Run Job gcs-to-s3                          │
  │ Secret Manager (llave HMAC) · Monitoring + Logging (alertas correo)  │
  └──────────────────────────────────────────────────────────────────────┘
        │ Parquet · cada 15 min (HTTPS/TLS · OIDC)
        ▼
                      ┌─ Amazon Web Services · lakehouse y analitica ──────────────┐
                      │ S3 (Raw → Trusted → Curated) · Glue ETL · Athena           │
  Loader historico ──►│ QuickSight (dashboards) · EventBridge (cada hora)          │
  (mismo generador)   │ CloudWatch · IAM + KMS                                     │
                      └────────────────────────────────────────────────────────────┘
        │
        ▼  Consumo
  Marketing · Gerencia · Analistas · Analisis de relaciones
```

**GCP** es el plano de ingesta y procesamiento near-real-time. **AWS** es el Lakehouse y la capa analitica. El detalle (diagrama Mermaid, contratos y decisiones) esta en [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

**Que funciona hoy (v2.25):** simulador -> Pub/Sub -> Dataflow (validacion, deduplicacion, DLQ, anonimizacion y NLP) -> `raw/`, `dlq/` y `enriched/` en GCS y `posts_enriched` en BigQuery, en local y en GCP. Faltan las ventanas de tendencias (`trend_windows`, Lote 2.4), el historico, el puente a AWS, el lakehouse, el dashboard y las alertas.

---

## Entornos

| Elemento | Valor |
|---|---|
| Proyecto GCP | `project-0568d9ba-848f-413c-88b` (nombre `proyectoDatos`, numero `63567159332`; facturacion con credito, a cargo de Alejandro Sepulveda) |
| Region GCP | `us-central1` (ADR-12; cuotas de vCPU y Dataflow verificadas en esa region) |
| Region AWS | `us-east-1` (el ID de cuenta va en `terraform.tfvars`, no en el repositorio) |
| Simulador | Cloud Run `ta-stream-simulator` (`min = max = 1`, CPU siempre asignada). Apagado por defecto (`simulator_running = false`); el cursor se conserva en GCS |
| Streaming | Dataflow `ta-stream-dev`: 1 × `e2-standard-2`, Streaming Engine. Workers con la imagen `dataflow-nlp` (Artifact Registry `ta-images`). Se drena fuera de validaciones y demos |
| Datos en GCP | Buckets `…-ta-lake` (`raw/`, `dlq/`, `enriched/`) y `…-ta-datasets` (cursor del simulador) · BigQuery `ta_stream.posts_enriched` · secreto `ta-hmac-key` |
| Costos mientras estan encendidos | Dataflow ~0,25 USD/h · simulador: una instancia siempre encendida · Cloud Build ~0,10 USD por imagen de NLP |
| Etiquetas | `project=trend-analyzer` · `env=dev` · `owner=alejandro` |
| Alertas | Correo, via Cloud Monitoring (las direcciones van en `terraform.tfvars`). Por ahora un solo destinatario |
| Autenticacion de Terraform | AWS: perfil local de AWS CLI (`aws configure`). GCP: Application Default Credentials (`gcloud auth application-default login`). Sin llaves en el codigo |
| Estado de Terraform | Remoto (ADR-14): GCS con versionado (GCP) y S3 con bloqueo en DynamoDB (AWS, ADR-15). Los *bootstraps* ya estan aplicados (Lote 1.0) |
| Presupuestos | GCP (400000 COP) y AWS (50 USD) con aviso por correo al 50 %, 80/90 % y 100 % del gasto |
| Codigo | Python 3.11 · Apache Beam 2.77 · Terraform >= 1.6. En WSL el 3.11 se instala con `uv`: entorno `ta` (simulador y `common/`) y `~/.venvs/ta-streaming` (Beam y validaciones) |

---

## Fuentes de datos

| Fuente | Mecanismo | Frecuencia | Estado |
|---|---|---|---|
| **Reddit** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-reddit` · payload estilo Reddit (subreddit, score, num_comments) · 30% posts / 70% comentarios | 1 mensaje cada 1-3 s | Operativa (Lotes 1.2 y 1.3) |
| **Facebook** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-facebook` · payload estilo Meta (pagina, reacciones, shares) · 15% posts / 85% comentarios | 1 mensaje cada 1-3 s | Operativa (Lotes 1.2 y 1.3) |
| **Instagram** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-instagram` · payload estilo Meta (cuenta, media, like_count) · 100% comentarios | 1 mensaje cada 1-3 s | Operativa (Lotes 1.2 y 1.3) |
| **Historico** | Mismo generador en batch (~50k registros, ultimos 180 dias) -> S3 `raw/origin=historical/` | Manual / one-shot | Pendiente (Lote 3.1, Sprint 2) |

### Generador de comentarios

El texto de todas las fuentes sale de un generador de Python (entregado en el Lote 1.1) que produce pares `marca,comentario` en espanol colombiano (jerga, emojis, abreviaturas) en 5 categorias: `gusto`, `asco`, `informativo`, `nostalgia` y `adiccion`. El CLI `scripts/generate_comments.py` escribe un CSV con las columnas `marca,comentario,sentimiento[,categoria]`; `marca` lleva la clave canonica (`postobon`, `pepsi`, `sprite`), no el nombre con tilde:

```csv
marca,comentario,sentimiento
postobon,Opinión personal: soy fan d Postobón y no pienso cambiar. Volvería mil veces. 🙌,positive
pepsi,Mi aporte del día: no tolero Pepsi Cola en el trancón. Mejor me hubiera tomado un agua. Es mi opinión.,negative
sprite,Vamos al punto: me chifla Sprite sobre todo con el frío de Bogotá. Eso sí es sabor. 🥤,positive
```

> Ejemplo ilustrativo del formato; el texto real depende de la semilla.

El simulador agrega lo que el texto no trae (red social, usuario, fechas, engagement, hashtags) y controla que marca se menciona en cada momento para crear tendencias reproducibles.

**Decisiones adoptadas sobre el generador** (detalle en `docs/ARCHITECTURE.md` sec. 3.2.1, 3.2.2 y ADR-13):

| Tema | Decision |
|---|---|
| Marcas v1 | `postobon` (propia), `pepsi` y `sprite` (competidores). CocaCola y Colombiana quedan fuera de v1 |
| Sentimiento (*ground truth*) | `gusto` -> positive · `asco` -> negative · `informativo` -> neutral · `nostalgia` y `adiccion` -> sin etiqueta |
| Funcion pura | `generate(rng, marca, sentimiento)` con `random.Random` inyectado; sin `sleep`, `print` ni semilla global |
| Variedad | Categoria `informativo` (40 plantillas) y 18 plantillas comparativas con dos marcas ("Pepsi vs Postobon"); 99,995 % de textos unicos en 20.000 comentarios |
| Mezcla de categorias | Sin sentimiento fijo: `gusto` 28 % · `asco` 24 % · `informativo` 18 % · `nostalgia` 15 % · `adiccion` 15 %. 30 % de los `informativo` son comparativas |
| Ruido | Probabilidad de ruido 20 %; nunca quita la marca del texto. Los hashtags del texto se extraen con regex y se normalizan (minusculas, sin `#`, sin tildes, maximo 100 caracteres) |
| Reproducibilidad | `Random(f"{semilla}|{fuente}|{seq}")` por evento; `event_id` determinista |

> Los datos son 100% sinteticos: la asociacion marca-sentimiento es artificial y las metricas de NLP seran optimistas. El objetivo del proyecto es ingenieria de datos (ingesta, calidad, linaje, latencia), no conclusiones de negocio.

---

## Stack tecnologico

| Capa | Tecnologias |
|---|---|
| **Ingesta** | Cloud Run (`ta-stream-simulator`, Python 3.11 · asyncio) · generador de comentarios · Pub/Sub |
| **Procesamiento** | Dataflow (Apache Beam 2.77, Streaming Engine) · AWS Glue (Spark) |
| **NLP** | RoBERTuito (pysentimiento): sentimiento y emocion en espanol, con `RunInference` en Dataflow y los modelos horneados en la imagen `dataflow-nlp` |
| **Almacenamiento Raw** | Cloud Storage (GCS) · Amazon S3 |
| **Analitica** | BigQuery · Amazon Athena |
| **Orquestacion** | Cloud Scheduler · Amazon EventBridge |
| **Gobernanza** | AWS Glue Data Catalog · dbt docs (linaje) · DAMA-DMBOK |
| **Calidad** | Great Expectations · dbt · verificadores por lote (`verify_*.py`) · validaciones automaticas (`validate_*.py`) · prueba de humo end-to-end · contratos verificados con `jsonschema` |
| **Seguridad** | Secret Manager (llave HMAC de anonimizacion) · IAM + KMS (AWS) · gitleaks (pre-commit) |
| **IaC / CI-CD** | Terraform >= 1.6 (estado remoto: GCS y S3 + DynamoDB) · GitHub Actions · Docker · Cloud Build · Artifact Registry |
| **Desarrollo local** | Docker Compose · emulador de Pub/Sub · Dataflow `DirectRunner` (tambien con NLP) |
| **Visualizacion** | Amazon QuickSight |
| **Observabilidad** | GCP Monitoring + Logging (alertas por correo) · AWS CloudWatch |

> Fuera del alcance v1: Firestore, Cloud SQL, Neptune y Dataplex (el analisis de relaciones se cubre con tablas de aristas en Curated). Ver `docs/ARCHITECTURE.md` sec. 1.3.

---

## Estructura del repositorio

> Estructura vigente (v2.25). Las carpetas de las fases 3 a 5 se crean con sus lotes (ver `docs/PLAN.md` sec. 1). Rutas completas en `docs/ARCHITECTURE.md` sec. 6.

```
.                                          <- raiz del repositorio
├── README.md                              <- este archivo
├── .gitignore                             <- incluye terraform.tfvars
├── .pre-commit-config.yaml                <- gitleaks, formato
├── .github/workflows/                     <- CI/CD (en la raiz; usan working-directory: project)
│
├── docs/
│   ├── ARCHITECTURE.md                    <- arquitectura y contratos (version vigente)
│   ├── CHANGELOG.md                       <- historial de versiones
│   ├── PLAN.md                            <- fases y lotes, matriz HU y estado del proyecto
│   ├── HU.md                              <- historias de usuario (HU-001 a HU-048)
│   ├── ACTIVE_SERVICES.md                 <- encender, comprobar y apagar lo que cobra
│   ├── validation/                        <- guias de validacion por lote (validation-2-2.md, validation-2-3.md)
│   ├── GLOSSARY.md                        <- glosario del proyecto
│   ├── NOTAS.md                           <- notas de trabajo
│   ├── trend_analyzer_multicloud.drawio   <- diagrama editable (v2.5)
│   └── trend_analyzer_multicloud.jpg      <- exportacion del diagrama
│
└── project/                               <- codigo, contratos e infraestructura
    ├── docker-compose.yml                 <- local: emulador Pub/Sub + simulador + validador (Lote 1.5)
    ├── docker-compose.dataflow.yml        <- local: dataflow-local (DirectRunner) y streaming-check (Lote 2.1)
    ├── docker-compose.nlp.yml             <- local: dataflow-local con la imagen de NLP (Lote 2.3)
    ├── LOCAL_DEV.md                       <- guia del entorno local
    ├── local/                             <- Dockerfiles de las herramientas locales
    ├── pytest.ini                         <- pythonpath = src, testpaths = src
    ├── contracts/                         <- copia legible por maquina de la sec. 5 (schemas, DDL de BigQuery y Athena, ejemplos, pruebas)
    ├── infra/                             <- Terraform: bootstrap (1.0), base de GCP y simulador (1.3), BigQuery y secreto HMAC (2.2), permiso de la imagen de NLP (2.3)
    │   ├── BOOTSTRAP.md  DEPLOY_SIMULATOR.md  validate.sh
    │   ├── gcp/{bootstrap,modules,environments/dev}/
    │   └── aws/{bootstrap,modules,environments/dev}/
    ├── scripts/
    │   ├── generate_comments.py           <- CLI del generador (Lote 1.1)
    │   ├── build_simulator_image.sh       <- imagen del simulador (Lote 1.3)
    │   ├── verify_ingest.py               <- verificador de ingesta (Lote 1.4)
    │   ├── local_dev.sh  preflight.sh     <- entorno local y estado real de la nube (Lote 1.5)
    │   ├── dataflow_job.sh  local_dataflow.sh  verify_streaming.py   <- pipeline de streaming (Lote 2.1)
    │   ├── verify_enriched.py  hmac_key.sh                           <- rama enriched (Lote 2.2)
    │   ├── build_nlp_image.sh  local_nlp.sh  nlp_bench.py            <- NLP (Lote 2.3)
    │   ├── on.py  off.py  status.py       <- encender, apagar y ver lo que cobra
    │   ├── validate_2_3.py                <- validacion automatica del Lote 2.3
    │   └── smoke_e2e.py                   <- prueba de humo extremo a extremo (Lote 5.4)
    ├── evidencia/                         <- JSON y logs que dejan las validaciones
    └── src/
        ├── conftest.py                    <- fixture envelope_validator (valida contra contracts/)
        ├── setup.py                       <- paquete que Dataflow instala en los workers (streaming + common)
        ├── common/                        <- envelope, hashing, hashtags, label_map; fila enriquecida, Parquet y llave HMAC (Lote 2.2)
        ├── simulator/                     <- ta-stream-simulator: generador, enriquecimiento, caos, Dockerfile (Lotes 1.1 a 1.3)
        ├── ingest_check/                  <- verificador de ingesta y topologia del emulador (Lotes 1.4 y 1.5)
        ├── streaming/                     <- Dataflow: validacion, dedup, DLQ y raw/ (2.1) · anonimizacion, enriched/ y BigQuery (2.2) · NLP e imagen dataflow-nlp (2.3)
        ├── ops/                           <- operacion (on/off/status) y validaciones automaticas
        ├── historical/                    <- full load historico (mismo generador; Lote 3.1)
        ├── exporter/                      <- gcs-to-s3 (Lote 3.2)
        ├── lakehouse/                     <- Glue jobs y dbt (Fase 4)
        ├── quality/                       <- Great Expectations (Fase 5)
        └── collectors/                    <- LEGADO: conectores reales Mastodon/Reddit/RSS (no se despliega)
```

> **`LocalContext/`** (si existe en tu copia local) contiene documentos de referencia que no se suben al repositorio (ver `.gitignore`). `FUENTES.md` y `tanteo_fuentes.py` corresponden al enfoque anterior (Mastodon/Reddit/RSS reales).

---

## Equipo

| Integrante | Rol | Responsabilidades |
|---|---|---|
| **Miguel Alejandro Gomez Duque** | Lider Tecnico · Arquitecto · DataOps | Arquitectura GCP/AWS · IAM · Lakehouse · Scrum Master · Git · CI/CD · Observabilidad |
| **Camila Martinez Montoya** | Ingeniero de Pipelines / Big Data | ETL/ELT · Dataflow · Spark/Flink · DAGs · Kafka/Pub/Sub |
| **Alejandro Sepulveda Posada** | Gobernanza y Calidad · Simulador | Simulador de streaming y generador de comentarios (Reddit/Facebook/Instagram) · Profiling (HU-031) · Calidad (HU-032) · DAMA-DMBOK · Catalogo · Linaje · Ley 1581 · Facturacion/credito GCP |

> Por ahora solo el correo de Alejandro tiene acceso al proyecto y recibe alertas; el acceso de Miguel y Camila se otorga cuando lo necesiten sus lotes (HU-046).

---

## Entregas

El plan ya no usa fechas (ver `docs/CHANGELOG.md`, v2.10): las entregas se definen por contenido.

| Entrega | Peso | Entregable |
|---|---|---|
| **Sprint 0** | 10% | Propuesta · Canvas · arquitectura conceptual · KPIs · data assessment |
| **Sprint 1** | 30% | Infra provisionada · ingesta funcional · capa Raw · profiling · calidad basica |
| **Sprint 2** | 30% | Pipelines ETL/ELT · bases optimizadas · catalogo · linaje · serving layer |
| **Sprint Final** | 30% | CI/CD · monitoreo · dashboard gobernado · informe tecnico · sustentacion |

---

## Estado del proyecto

Plan por fases (detalle y lotes en [`docs/PLAN.md`](docs/PLAN.md) sec. 1; estado vivo, lote por lote, en sec. 3):

| Fase | Contenido | HU | Estado |
|---|---|---|---|
| 0 · Alineacion | Contratos, README y diagrama, decisiones | HU-001 a HU-003 | ✅ Cerrada |
| 1 · Simulador | Bootstrap, generador, simulador, despliegue, verificador, entorno local | HU-004, HU-008 a HU-017, HU-046, HU-047 | ✅ Cerrada (v2.21) |
| 2 · Streaming GCP | Dataflow, anonimizacion, NLP, ventanas | HU-005, HU-018 a HU-023 | 🟨 En curso: Lotes 2.1, 2.2 y 2.3 cerrados; siguiente, el 2.4 (ventanas de tendencias) |
| 3 · Historico y puente | Full load y exportador GCS -> S3 | HU-006, HU-024, HU-025 | Pendiente |
| 4 · Lakehouse AWS | Trusted, Curated, aristas, orquestacion | HU-007, HU-026 a HU-030 | Pendiente |
| 5 · Calidad y gobernanza | Profiling, reglas, catalogo, linaje, prueba E2E | HU-031 a HU-036, HU-048 | Pendiente |
| 6 · Serving y alertas | QuickSight, alertas, escenarios de demo | HU-037 a HU-040 | Pendiente |
| 7 · CI/CD y costos | Actions, observabilidad, presupuesto | HU-041 a HU-043 | Pendiente |
| 8 · Informe | Informe tecnico, sustentacion, runbook | HU-044, HU-045 | Pendiente |

**Hecho hasta ahora:**
- **Lote 1.0 (HU-046):** *bootstraps* de Terraform aplicados en GCP y AWS (19 APIs de GCP, estado remoto, presupuestos con aviso por correo) y entornos `dev` conectados al estado remoto.
- **Lote 1.1 (HU-008, HU-009):** `src/common/` y generador de comentarios con 5 categorias.
- **Lote 1.2 (HU-010 a HU-015):** nucleo del simulador: tres workers asyncio, cursor en GCS que sobrevive a reinicios, eventos deterministas, usuarios, engagement por red y caos controlado.
- **Lote 1.3 (HU-004, HU-016):** imagen del simulador y base de GCP en Terraform (topics, suscripciones, buckets, service accounts, Artifact Registry y Cloud Run).
- **Lote 1.4 (HU-017):** verificador de ingesta: ~0,5 msg/s por fuente, 100 % de ids unicos y cursor que continua tras un reinicio.
- **Lote 1.5 (HU-047):** entorno local con emulador de Pub/Sub, simulador y validador, sin credenciales.
- **Lote 2.1 (HU-018, HU-019):** Dataflow valida, deduplica por `event_id`, manda los invalidos a la DLQ y escribe `raw/`, en local (`DirectRunner`) y en GCP.
- **Lote 2.2 (HU-020, HU-021):** anonimizacion HMAC-SHA256 (`user_hash`), engagement normalizado, Parquet en `enriched/` y filas en BigQuery `posts_enriched`, con cobertura del 100 %.
- **Lote 2.3 (HU-022):** sentimiento y emocion con RoBERTuito en el 100 % de las filas; un evento llega a BigQuery con NLP en unos 5 s. En local, el modelo ya no se comparte entre procesos, porque el `DirectRunner` se congelaba (v2.25).

**Abierto, sin bloquear:**
- Declarar en Terraform el permiso de Cloud Build y agregar `GET /health` al simulador (v2.17).
- Limitar `sa-exporter` a `enriched/` (Lote 3.2), *soft delete* del bucket del lake frente a `_dataflow/temp`, y `*.egg-info/` y `.local-lake/` en `.gitignore` (v2.21).
- `roles/pubsub.viewer` para `sa-dataflow` y medicion formal de la latencia p95 (Lote 2.4).
- Anotar los conteos de pruebas del Lote 2.3 y el pico de memoria del worker de Dataflow.

**Diferido (no bloquea):** acceso de Miguel y Camila, verificar el conector QuickSight -> BigQuery y la prueba de sincronizacion sec. 5 <-> `contracts/` (Fase 5).

---

## KPIs clave

- Latencia de deteccion de tendencias **< 5 minutos** (95% de eventos validos)
- Completitud **>= 95%** · Unicidad **>= 99%** · Duplicados **< 1%** en capa confiable
- Tiempo de respuesta del dashboard **< 3 segundos**
- **100%** de identificadores de usuario anonimizados antes de la capa analitica
- Reduccion **>= 50%** del tiempo de elaboracion de informes de marketing

**Medido hasta ahora en streaming (Lotes 2.2 y 2.3, en GCP):** 0 identificadores en claro en `enriched/` y en BigQuery; 0 copias de `event_id`; NLP en el 100 % de las filas; p95 de emision a BigQuery de 27 s. La latencia de deteccion de tendencias se mide con el Lote 2.4.

---

## Inicio rapido

> Cada bloque dice desde donde se corre. Los scripts de operacion y validacion (`on.py`, `off.py`, `status.py`, `validate_*.py`) se corren desde la **raiz del repositorio**; el resto, desde `project/`. En WSL, activar antes el entorno de Python 3.11 (`source ~/.venvs/ta-streaming/bin/activate`; pasos en `project/src/streaming/README.md`).

### Validar infraestructura (sin credenciales reales)

Necesita Terraform >= 1.6. Corre `terraform fmt -check` y `init -backend=false` + `validate` en las raices de Terraform:

```bash
cd project
bash infra/validate.sh
```

Para aplicar el *bootstrap* de cada nube y conectar los entornos `dev` al estado remoto, ver `project/infra/BOOTSTRAP.md` y `docs/ARCHITECTURE.md` sec. 5.14. Los valores reales (`terraform.tfvars`, `backend.hcl`) no se versionan: se copian desde los `*.example`.

### Generar comentarios de ejemplo

```bash
cd project
pip install pyyaml
# Imprime 10 filas con semilla fija
python scripts/generate_comments.py --n 10 --seed 7
# Escribe un CSV grande (UTF-8 sin BOM; --bom para abrirlo en Excel)
python scripts/generate_comments.py --n 50000 --seed 7 --out comentarios.csv
```

### Correr pruebas

```bash
cd project
python -m pytest src/common src/simulator -q     # entorno ta
python -m pytest src/streaming src/ops -q        # entorno ~/.venvs/ta-streaming (Beam 2.77)
python -m pytest src/ingest_check -q
python -m pytest contracts/tests -q              # contratos: 18 pruebas
```

Los conteos esperados de cada lote estan en `docs/PLAN.md` sec. 3 y en su guia de `docs/validation/`.

### Entorno local sin nube

Sin credenciales de GCP ni de AWS. Guia completa en [`project/LOCAL_DEV.md`](project/LOCAL_DEV.md).

```bash
cd project
bash scripts/local_dev.sh up             # emulador de Pub/Sub + simulador
bash scripts/local_dataflow.sh up        # + pipeline con DirectRunner (lake en project/.local-lake)
bash scripts/local_dataflow.sh check     # centinela y defectos de prueba (Lote 2.1)
bash scripts/local_nlp.sh build          # imagen con los modelos de NLP (una vez; ~2,5 GB)
bash scripts/local_nlp.sh up             # dataflow-local con NLP (Lote 2.3)
bash scripts/local_dataflow.sh down-all  # apagar todo lo local
```

### Encender, comprobar y apagar en GCP

Desde la raiz del repositorio. Detalle y comandos manuales en [`docs/ACTIVE_SERVICES.md`](docs/ACTIVE_SERVICES.md).

```bash
python project/scripts/status.py          # que esta encendido (solo lectura)
python project/scripts/on.py cloud        # simulador + job de Dataflow (con NLP si hay imagen publicada)
python project/scripts/off.py             # apaga lo que cobra en GCP: drena Dataflow y apaga el simulador
```

> Dataflow cobra ~0,25 USD/h y el simulador mantiene una instancia encendida: apagar al terminar cada sesion.

### Validar un lote

Desde la raiz del repositorio. Cada lote con validacion trae su guia en `docs/validation/` y su script en `project/scripts/`; la evidencia queda en `project/evidencia/`.

```bash
python project/scripts/validate_2_3.py tests local       # pruebas, Terraform y local (gratis)
python project/scripts/validate_2_3.py cloud --off-after # GCP: cobra mientras corre y drena el job al final
```

### Desplegar el simulador por primera vez

Pasos completos (imagen, `terraform.tfvars` y verificacion) en [`project/infra/DEPLOY_SIMULATOR.md`](project/infra/DEPLOY_SIMULATOR.md). En resumen: `validate.sh`, `apply` de la base, `bash scripts/build_simulator_image.sh` para construir, probar y publicar la imagen, y `simulator_image` en `terraform.tfvars` con otro `apply`. Despues, `on.py` y `off.py` lo encienden y apagan.

### Prueba de humo end-to-end (Lote 5.4, pendiente)

```bash
cd project
# Sigue un evento centinela desde el simulador hasta Athena y reporta la latencia de cada etapa
python scripts/smoke_e2e.py
```

---

## Notas importantes

> [!IMPORTANT]
> `terraform.tfvars` y `backend.hcl` estan en `.gitignore` -- nunca subir credenciales, el ID de cuenta de AWS ni los correos de alerta al repositorio. Se versionan solo los `*.example` y los `.terraform.lock.hcl`.

> [!WARNING]
> Si conservas `LocalContext/tanteo_fuentes.py`, contiene un token de Mastodon hardcodeado (linea 34). Ya no se usa Mastodon, pero si el token sigue activo, revocarlo en mastodon.social.

> [!NOTE]
> `LocalContext/` no se sube al repositorio (`.gitignore`). Hacer copia de seguridad independiente si se necesita persistencia.

> [!NOTE]
> El repositorio se trabaja en Windows y WSL: se recomienda un `.gitattributes` con `*.sh`, `*.tf` y `*.hcl` en `eol=lf` para que `validate.sh` no falle por finales de linea CRLF. Si un script de Python falla con `FileNotFoundError` al importar, la carpeta actual de la terminal quedo rota por cambios hechos desde Windows: volver a entrar con `cd`.

> [!NOTE]
> Los datos son sinteticos, pero el pipeline los trata como datos reales: los identificadores de usuario se anonimizan con HMAC-SHA256 en el primer paso de procesamiento (Dataflow desde el Lote 2.2; loader historico en el 3.1), antes de salir de GCP.

> [!NOTE]
> Los modelos de NLP (RoBERTuito de pysentimiento) no declaran licencia en su *model card* y quedan sujetos a la licencia de sus datos de entrenamiento (TASS 2020, EmoEvent), muchas de uso no comercial. El proyecto es academico y sin uso comercial. Citas en `docs/ARCHITECTURE.md` sec. 3.15.

---

## Licencia

Proyecto academico -- Universidad EAFIT · SI4002 · 2026-2. Uso educativo.