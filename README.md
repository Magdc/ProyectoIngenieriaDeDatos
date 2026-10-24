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
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Arquitectura, contratos de comunicacion, decisiones (ADR), plan por fases y estado |
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
  Cloud Run  stream-simulator   (1 mensaje cada 1-3 s por fuente)
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
| Proyecto GCP | `project-2911bf21-1f97-41aa-ac6` (facturacion con credito, a cargo de Alejandro Sepulveda) |
| Region GCP | `us-east1` (ADR-12) |
| Region AWS | `us-east-1` (el ID de cuenta va en `terraform.tfvars`, no en el repositorio) |
| Etiquetas | `project=trend-analyzer` · `env=dev` · `owner=alejandro` |
| Alertas | Correo, via Cloud Monitoring (las direcciones van en `terraform.tfvars`) |
| Estado de Terraform | Remoto: GCS con versionado (GCP) y S3 con bloqueo (AWS) |

---

## Fuentes de datos

| Fuente | Mecanismo | Frecuencia | Estado |
|---|---|---|---|
| **Reddit** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-reddit` · payload estilo Reddit (subreddit, score, num_comments) · 30% posts / 70% comentarios | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Facebook** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-facebook` · payload estilo Meta (pagina, reacciones, shares) · 15% posts / 85% comentarios | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Instagram** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-instagram` · payload estilo Meta (cuenta, media, like_count) · 100% comentarios | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Historico** | Mismo generador en batch (~50k registros, ultimos 180 dias) -> S3 `raw/origin=historical/` | Manual / one-shot | Sprint 2 |

### Generador de comentarios

El texto de todas las fuentes sale de un script de Python que produce pares `marca,comentario` en espanol colombiano (jerga, emojis, abreviaturas):

```csv
marca,comentario
Postobón,Opinión personal: soy fan d Postobón y no pienso cambiar. Volvería mil veces. 🙌
Pepsi,Mi aporte del día: no tolero Pepsi Cola en el trancón. Mejor me hubiera tomado un agua. Es mi opinión.
Sprite,Vamos al punto: me chifla Sprite sobre todo con el frío de Bogotá. Eso sí es sabor. 🥤
```

El simulador agrega lo que el texto no trae (red social, usuario, fechas, engagement, hashtags) y controla que marca se menciona en cada momento para crear tendencias reproducibles.

**Decisiones adoptadas sobre el script** (detalle en `docs/ARCHITECTURE.md` sec. 3.2.1 y ADR-13):

| Tema | Decision |
|---|---|
| Marcas v1 | `postobon` (propia), `pepsi` y `sprite` (competidores). CocaCola y Colombiana quedan fuera de v1 |
| Sentimiento (*ground truth*) | `gusto` -> positive · `asco` -> negative · `informativo` -> neutral (categoria nueva) · `nostalgia` y `adiccion` -> sin etiqueta |
| Funcion pura | `generate(rng, marca, sentimiento)` con `random.Random` inyectado; sin `sleep`, `print` ni semilla global |
| Variedad | Categoria `informativo` y plantillas comparativas con dos marcas ("Pepsi vs Postobon") |
| Ruido | Los nombres de marca se protegen del *typo*; los hashtags del texto se extraen con regex |
| Reproducibilidad | `Random(f"{semilla}|{fuente}|{seq}")` por evento; `event_id` determinista |

> Los datos son 100% sinteticos: la asociacion marca-sentimiento es artificial y las metricas de NLP seran optimistas. El objetivo del proyecto es ingenieria de datos (ingesta, calidad, linaje, latencia), no conclusiones de negocio.

---

## Stack tecnologico

| Capa | Tecnologias |
|---|---|
| **Ingesta** | Cloud Run (`stream-simulator`, Python 3.11 · asyncio) · generador de comentarios · Pub/Sub |
| **Procesamiento** | Dataflow (Beam) · AWS Glue (Spark) |
| **NLP** | RoBERTuito (pysentimiento): sentimiento y emocion en espanol |
| **Almacenamiento Raw** | Cloud Storage (GCS) · Amazon S3 |
| **Analitica** | BigQuery · Amazon Athena |
| **Orquestacion** | Cloud Scheduler · Amazon EventBridge |
| **Gobernanza** | AWS Glue Data Catalog · dbt docs (linaje) · DAMA-DMBOK |
| **Calidad** | Great Expectations · dbt · prueba de humo end-to-end |
| **Seguridad** | Secret Manager (llave HMAC de anonimizacion) · IAM + KMS (AWS) · gitleaks (pre-commit) |
| **IaC / CI-CD** | Terraform (estado remoto) · GitHub Actions · Docker · Artifact Registry |
| **Desarrollo local** | docker-compose · emulador de Pub/Sub · Dataflow `DirectRunner` |
| **Visualizacion** | Amazon QuickSight |
| **Observabilidad** | GCP Monitoring + Logging (alertas por correo) · AWS CloudWatch |

> Fuera del alcance v1: Firestore, Cloud SQL, Neptune y Dataplex (el analisis de relaciones se cubre con tablas de aristas en Curated). Ver `docs/ARCHITECTURE.md` sec. 1.3.

---

## Estructura del repositorio

> Estructura objetivo (v2.6). Varias carpetas de `project/` se crean por lotes (ver `docs/ARCHITECTURE.md` sec. 7).

```
.                                          <- raiz del repositorio
├── README.md                              <- este archivo
├── .gitignore                             <- incluye terraform.tfvars
├── .pre-commit-config.yaml                <- gitleaks, formato
├── .github/workflows/                     <- CI/CD (en la raiz; usan working-directory: project)
│
├── docs/
│   ├── ARCHITECTURE.md                    <- arquitectura, contratos y plan por lotes
│   ├── HU.md                              <- historias de usuario (HU-001 a HU-048)
│   ├── GLOSSARY.md                        <- glosario del proyecto
│   ├── NOTAS.md                           <- notas de trabajo
│   ├── trend_analyzer_multicloud.drawio   <- diagrama editable (v2.5)
│   └── trend_analyzer_multicloud.jpg      <- exportacion del diagrama
│
└── project/                               <- codigo, contratos e infraestructura
    ├── docker-compose.yml                 <- entorno local: emulador Pub/Sub + simulador
    ├── contracts/                         <- contratos legibles por maquina (envelope, payloads, BigQuery, Athena)
    ├── infra/                             <- Infraestructura como codigo (Terraform)
    │   ├── README.md                      <- instrucciones de validacion local
    │   ├── gcp/environments/dev/          <- GCP: Cloud Run, Pub/Sub, GCS, Scheduler, Secret Manager
    │   └── aws/environments/dev/          <- AWS: S3, Glue, Athena, IAM
    ├── scripts/
    │   ├── generate_comments.py           <- CLI del generador (snapshots CSV / historico)
    │   └── smoke_e2e.py                   <- prueba de humo extremo a extremo
    └── src/
        ├── common/                        <- envelope, anonimizacion HMAC, NLP, label_map
        ├── simulator/                     <- stream-simulator (Cloud Run)
        │   ├── app.py  publisher.py  state.py
        │   ├── generator/                 <- comment_generator.py (script de comentarios marca,comentario)
        │   ├── enrich/                    <- usuarios, engagement por red, marcas, hashtags, caos
        │   ├── config/                    <- sim.yaml, brands.yaml
        │   └── tests/  Dockerfile  requirements.txt
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

---

## Cronograma de sprints

| Sprint | Fecha | Peso | Entregable |
|---|---|---|---|
| **Sprint 0** | 29 jul 2026 | 10% | Propuesta · Canvas · arquitectura conceptual · KPIs · data assessment |
| **Sprint 1** | 26 ago 2026 | 30% | Infra provisionada · ingesta funcional · capa Raw · profiling · calidad basica |
| **Sprint 2** | 28 oct 2026 | 30% | Pipelines ETL/ELT · bases optimizadas · catalogo · linaje · serving layer |
| **Sprint Final** | 11 nov 2026 | 30% | CI/CD · monitoreo · dashboard gobernado · informe tecnico · sustentacion |

---

## Estado del proyecto

Plan por fases (detalle y lotes en `docs/ARCHITECTURE.md` sec. 7; estado vivo en sec. 10):

| Fase | Contenido | HU | Estado |
|---|---|---|---|
| 0 · Alineacion | Contratos, README y diagrama, decisiones | HU-001 a HU-003 | En curso |
| 1 · Simulador | Bootstrap, generador, simulador, despliegue, entorno local | HU-004, HU-008 a HU-017, HU-046, HU-047 | Pendiente |
| 2 · Streaming GCP | Dataflow, anonimizacion, NLP, ventanas | HU-005, HU-018 a HU-023 | Pendiente |
| 3 · Historico y puente | Full load y exportador GCS -> S3 | HU-006, HU-024, HU-025 | Pendiente |
| 4 · Lakehouse AWS | Trusted, Curated, aristas, orquestacion | HU-007, HU-026 a HU-030 | Pendiente |
| 5 · Calidad y gobernanza | Profiling, reglas, catalogo, linaje, prueba E2E | HU-031 a HU-036, HU-048 | Pendiente |
| 6 · Serving y alertas | QuickSight, alertas, escenarios de demo | HU-037 a HU-040 | Pendiente |
| 7 · CI/CD y costos | Actions, observabilidad, presupuesto | HU-041 a HU-043 | Pendiente |
| 8 · Informe | Informe tecnico, sustentacion, runbook | HU-044, HU-045 | Pendiente |

**Pendiente para cerrar la Fase 0:** correos de alerta y accesos del equipo, verificar facturacion y cuotas de GCP, aprobar ADR-02 y ADR-13, y generar `contracts/` (HU-001). El README y el diagrama (HU-002) ya estan actualizados.

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

```bash
cd project

# GCP
cd infra/gcp/environments/dev
terraform init -backend=false
terraform validate

# AWS
cd ../../../../infra/aws/environments/dev
terraform init -backend=false
terraform validate
```

### Generar comentarios de ejemplo (disponible desde el Lote 1.1)

```bash
cd project
# Imprime 10 filas con semilla fija
python scripts/generate_comments.py --n 10 --seed 7
```

### Entorno local sin nube (disponible desde el Lote 1.5)

```bash
cd project
# Emulador de Pub/Sub + simulador, sin credenciales de GCP ni AWS
docker-compose up
```

### Correr pruebas unitarias del simulador (disponible desde el Lote 1.2)

```bash
cd project
pip install -r src/simulator/requirements.txt pytest
python -m pytest src/simulator/ -v
```

### Prueba de humo end-to-end (disponible desde el Lote 5.4)

```bash
cd project
# Sigue un evento centinela desde el simulador hasta Athena y reporta la latencia de cada etapa
python scripts/smoke_e2e.py
```

### Construir y desplegar el simulador (cuando el Lote 1.0 deje listo el proyecto GCP)

```bash
# 1. Autenticarse con Artifact Registry
gcloud auth configure-docker us-east1-docker.pkg.dev

# 2. Construir la imagen
cd project/src/simulator
docker build -t us-east1-docker.pkg.dev/project-2911bf21-1f97-41aa-ac6/REPO/ta-stream-simulator:v1 .
docker push  us-east1-docker.pkg.dev/project-2911bf21-1f97-41aa-ac6/REPO/ta-stream-simulator:v1

# 3. Actualizar terraform.tfvars y aplicar
cd ../../infra/gcp/environments/dev
# Editar la variable de imagen del contenedor con la nueva imagen
terraform apply
```

---

## Notas importantes

> [!IMPORTANT]
> `terraform.tfvars` esta en `.gitignore` -- nunca subir credenciales, el ID de cuenta de AWS ni los correos de alerta al repositorio. Se versiona solo `terraform.tfvars.example`.

> [!WARNING]
> Si conservas `LocalContext/tanteo_fuentes.py`, contiene un token de Mastodon hardcodeado (linea 34). Ya no se usa Mastodon, pero si el token sigue activo, revocarlo en mastodon.social.

> [!NOTE]
> `LocalContext/` no se sube al repositorio (`.gitignore`). Hacer copia de seguridad independiente si se necesita persistencia.

> [!NOTE]
> Los datos son sinteticos, pero el pipeline los trata como datos reales: los identificadores de usuario se anonimizan con HMAC-SHA256 en el primer paso de procesamiento (Dataflow / loader historico), antes de salir de GCP.

---

## Licencia

Proyecto academico -- Universidad EAFIT · SI4002 · 2026-2. Uso educativo.