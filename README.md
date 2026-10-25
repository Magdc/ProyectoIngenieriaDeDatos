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
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | **Fuente de verdad** (version vigente 2.14): arquitectura, contratos de comunicacion (sec. 5), decisiones (ADR), riesgos y preguntas abiertas |
| [`docs/PLAN.md`](docs/PLAN.md) | Plan por fases y lotes, matriz de trazabilidad Fase · Lote -> HU y estado del proyecto |
| [`docs/CHANGELOG.md`](docs/CHANGELOG.md) | Historial de versiones (v2.2 -> v2.14), una entrada por version |
| [`docs/HU.md`](docs/HU.md) | Backlog: 48 historias de usuario (HU-001 a HU-048) con criterios de aceptacion |
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

---

## Entornos

| Elemento | Valor |
|---|---|
| Proyecto GCP | `project-0568d9ba-848f-413c-88b` (nombre `proyectoDatos`, numero `63567159332`; facturacion con credito, a cargo de Alejandro Sepulveda) |
| Region GCP | `us-central1` (ADR-12; cuotas de vCPU y Dataflow verificadas en esa region) |
| Region AWS | `us-east-1` (el ID de cuenta va en `terraform.tfvars`, no en el repositorio) |
| Etiquetas | `project=trend-analyzer` · `env=dev` · `owner=alejandro` |
| Alertas | Correo, via Cloud Monitoring (las direcciones van en `terraform.tfvars`). Por ahora un solo destinatario |
| Autenticacion de Terraform | AWS: perfil local de AWS CLI (`aws configure`). GCP: Application Default Credentials (`gcloud auth application-default login`). Sin llaves en el codigo |
| Estado de Terraform | Remoto (ADR-14): GCS con versionado (GCP) y S3 con bloqueo en DynamoDB (AWS, ADR-15). Los *bootstraps* ya estan aplicados (Lote 1.0) |
| Presupuestos | GCP (400000 COP) y AWS (50 USD) con aviso por correo al 50 %, 80/90 % y 100 % del gasto |
| Codigo | Python 3.11 · Terraform >= 1.6 |

---

## Fuentes de datos

| Fuente | Mecanismo | Frecuencia | Estado |
|---|---|---|---|
| **Reddit** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-reddit` · payload estilo Reddit (subreddit, score, num_comments) · 30% posts / 70% comentarios | 1 mensaje cada 1-3 s | Pendiente (Lote 1.2) |
| **Facebook** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-facebook` · payload estilo Meta (pagina, reacciones, shares) · 15% posts / 85% comentarios | 1 mensaje cada 1-3 s | Pendiente (Lote 1.2) |
| **Instagram** (simulado) | Worker del `ta-stream-simulator` -> Pub/Sub `ta-raw-instagram` · payload estilo Meta (cuenta, media, like_count) · 100% comentarios | 1 mensaje cada 1-3 s | Pendiente (Lote 1.2) |
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
| **Procesamiento** | Dataflow (Beam) · AWS Glue (Spark) |
| **NLP** | RoBERTuito (pysentimiento): sentimiento y emocion en espanol |
| **Almacenamiento Raw** | Cloud Storage (GCS) · Amazon S3 |
| **Analitica** | BigQuery · Amazon Athena |
| **Orquestacion** | Cloud Scheduler · Amazon EventBridge |
| **Gobernanza** | AWS Glue Data Catalog · dbt docs (linaje) · DAMA-DMBOK |
| **Calidad** | Great Expectations · dbt · prueba de humo end-to-end · contratos verificados con `jsonschema` |
| **Seguridad** | Secret Manager (llave HMAC de anonimizacion) · IAM + KMS (AWS) · gitleaks (pre-commit) |
| **IaC / CI-CD** | Terraform >= 1.6 (estado remoto: GCS y S3 + DynamoDB) · GitHub Actions · Docker · Artifact Registry |
| **Desarrollo local** | docker-compose · emulador de Pub/Sub · Dataflow `DirectRunner` |
| **Visualizacion** | Amazon QuickSight |
| **Observabilidad** | GCP Monitoring + Logging (alertas por correo) · AWS CloudWatch |

> Fuera del alcance v1: Firestore, Cloud SQL, Neptune y Dataplex (el analisis de relaciones se cubre con tablas de aristas en Curated). Ver `docs/ARCHITECTURE.md` sec. 1.3.

---

## Estructura del repositorio

> Estructura vigente (v2.14). Varias carpetas de `project/` se crean por lotes (ver `docs/PLAN.md` sec. 1). Rutas completas y archivos nuevos en `docs/ARCHITECTURE.md` sec. 6.

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
│   ├── GLOSSARY.md                        <- glosario del proyecto
│   ├── NOTAS.md                           <- notas de trabajo
│   ├── trend_analyzer_multicloud.drawio   <- diagrama editable (v2.5)
│   └── trend_analyzer_multicloud.jpg      <- exportacion del diagrama
│
└── project/                               <- codigo, contratos e infraestructura
    ├── docker-compose.yml                 <- entorno local: emulador Pub/Sub + simulador (Lote 1.5)
    ├── pytest.ini                         <- pythonpath = src, testpaths = src
    ├── contracts/                         <- copia legible por maquina de la sec. 5 (schemas, DDL de BigQuery y Athena, ejemplos, pruebas)
    ├── infra/                             <- Infraestructura como codigo (Terraform), Lote 1.0 entregado
    │   ├── BOOTSTRAP.md                   <- guia de ejecucion del bootstrap
    │   ├── validate.sh                    <- fmt + validate sin credenciales
    │   ├── gcp/
    │   │   ├── bootstrap/                 <- estado local: APIs, bucket de estado, presupuesto
    │   │   ├── modules/{project_services,tfstate_bucket,budget}/
    │   │   └── environments/dev/          <- estado remoto (GCS)
    │   └── aws/
    │       ├── bootstrap/                 <- estado local: bucket de estado, tabla de bloqueo, presupuesto
    │       ├── modules/{tfstate_backend,budget}/
    │       └── environments/dev/          <- estado remoto (S3)
    ├── scripts/
    │   ├── generate_comments.py           <- CLI del generador (snapshots CSV / historico)
    │   └── smoke_e2e.py                   <- prueba de humo extremo a extremo (Lote 5.4)
    └── src/
        ├── conftest.py                    <- fixture envelope_validator (valida contra contracts/)
        ├── common/                        <- envelope, hashing, hashtags, label_map (el NLP llega en el Lote 2.3)
        ├── simulator/                     <- ta-stream-simulator (Cloud Run)
        │   ├── generator/                 <- comment_generator.py, brands.py, noise.py, templates/ (Lote 1.1)
        │   ├── config/                    <- brands.yaml (Lote 1.1), sim.yaml (Lote 1.2)
        │   ├── app.py  publisher.py  state.py   <- Lote 1.2
        │   ├── enrich/                    <- usuarios, engagement por red, marcas, caos (Lote 1.2)
        │   └── tests/  Dockerfile  requirements.txt   <- Dockerfile en el Lote 1.3
        ├── streaming/                     <- Dataflow (validacion, dedup, anonimizacion, NLP, ventanas)
        ├── historical/                    <- full load historico (mismo generador)
        ├── exporter/                      <- gcs-to-s3
        ├── lakehouse/                     <- Glue jobs y dbt
        ├── quality/                       <- Great Expectations
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
| 1 · Simulador | Bootstrap, generador, simulador, despliegue, entorno local | HU-004, HU-008 a HU-017, HU-046, HU-047 | 🟨 En curso: Lotes 1.0 y 1.1 cerrados; siguiente, el 1.2 |
| 2 · Streaming GCP | Dataflow, anonimizacion, NLP, ventanas | HU-005, HU-018 a HU-023 | Pendiente |
| 3 · Historico y puente | Full load y exportador GCS -> S3 | HU-006, HU-024, HU-025 | Pendiente |
| 4 · Lakehouse AWS | Trusted, Curated, aristas, orquestacion | HU-007, HU-026 a HU-030 | Pendiente |
| 5 · Calidad y gobernanza | Profiling, reglas, catalogo, linaje, prueba E2E | HU-031 a HU-036, HU-048 | Pendiente |
| 6 · Serving y alertas | QuickSight, alertas, escenarios de demo | HU-037 a HU-040 | Pendiente |
| 7 · CI/CD y costos | Actions, observabilidad, presupuesto | HU-041 a HU-043 | Pendiente |
| 8 · Informe | Informe tecnico, sustentacion, runbook | HU-044, HU-045 | Pendiente |

**Hecho hasta ahora:**
- **Lote 1.0 (HU-046):** *bootstraps* de Terraform aplicados en GCP y AWS (19 APIs de GCP, estado remoto, presupuestos con aviso por correo) y entornos `dev` conectados al estado remoto. Contrato en `docs/ARCHITECTURE.md` sec. 5.14.
- **Lote 1.1 (HU-008, HU-009):** `src/common/` y generador de comentarios con 5 categorias. `python -m pytest`: 64 pruebas en verde, mas las 18 de `contracts/`.

**Diferido (no bloquea):** acceso de Miguel y Camila, verificar el conector QuickSight -> BigQuery y la prueba de sincronizacion sec. 5 <-> `contracts/` (Fase 5). **Abierto:** confirmar los criterios de aceptacion de HU-008 y HU-009 contra `HU.md`.

---

## KPIs clave

- Latencia de deteccion de tendencias **< 5 minutos** (95% de eventos validos)
- Completitud **>= 95%** · Unicidad **>= 99%** · Duplicados **< 1%** en capa confiable
- Tiempo de respuesta del dashboard **< 3 segundos**
- **100%** de identificadores de usuario anonimizados antes de la capa analitica
- Reduccion **>= 50%** del tiempo de elaboracion de informes de marketing

---

## Inicio rapido

> Todos los comandos se ejecutan desde la carpeta `project/`.

### Validar infraestructura (sin credenciales reales)

Necesita Terraform >= 1.6. Corre `terraform fmt -check` y `init -backend=false` + `validate` en las cuatro raices (bootstrap y entorno `dev`, de GCP y AWS):

```bash
cd project
bash infra/validate.sh
```

Para aplicar el *bootstrap* de cada nube y conectar los entornos `dev` al estado remoto, ver `project/infra/BOOTSTRAP.md` y `docs/ARCHITECTURE.md` sec. 5.14. Los valores reales (`terraform.tfvars`, `backend.hcl`) no se versionan: se copian desde los `*.example`.

### Generar comentarios de ejemplo (disponible desde el Lote 1.1)

```bash
cd project
pip install pyyaml
# Imprime 10 filas con semilla fija
python scripts/generate_comments.py --n 10 --seed 7
# Escribe un CSV grande (UTF-8 sin BOM; --bom para abrirlo en Excel)
python scripts/generate_comments.py --n 50000 --seed 7 --out comentarios.csv
```

### Correr pruebas (Lote 1.1: `common/` y generador)

```bash
cd project
pip install pyyaml pytest jsonschema referencing
python -m pytest                      # usa pytest.ini (pythonpath = src): 64 pruebas
python -m pytest contracts/tests -q   # contratos: 18 pruebas
```

### Entorno local sin nube (disponible desde el Lote 1.5)

```bash
cd project
# Emulador de Pub/Sub + simulador, sin credenciales de GCP ni AWS
docker-compose up
```

### Prueba de humo end-to-end (disponible desde el Lote 5.4)

```bash
cd project
# Sigue un evento centinela desde el simulador hasta Athena y reporta la latencia de cada etapa
python scripts/smoke_e2e.py
```

### Construir y desplegar el simulador (disponible desde el Lote 1.3)

```bash
# 1. Autenticarse con Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# 2. Construir la imagen
cd project/src/simulator
docker build -t us-central1-docker.pkg.dev/project-0568d9ba-848f-413c-88b/REPO/ta-stream-simulator:v1 .
docker push  us-central1-docker.pkg.dev/project-0568d9ba-848f-413c-88b/REPO/ta-stream-simulator:v1

# 3. Actualizar terraform.tfvars y aplicar
cd ../../infra/gcp/environments/dev
# Editar la variable de imagen del contenedor con la nueva imagen
terraform apply
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
> El repositorio se trabaja en Windows y WSL: se recomienda un `.gitattributes` con `*.sh`, `*.tf` y `*.hcl` en `eol=lf` para que `validate.sh` no falle por finales de linea CRLF.

> [!NOTE]
> Los datos son sinteticos, pero el pipeline los trata como datos reales: los identificadores de usuario se anonimizan con HMAC-SHA256 en el primer paso de procesamiento (Dataflow / loader historico), antes de salir de GCP.

---

## Licencia

Proyecto academico -- Universidad EAFIT · SI4002 · 2026-2. Uso educativo.