# TrendAnalyzer

> Plataforma de analisis de tendencias y sentimiento en redes sociales y medios digitales.  
> **Asignatura:** SI4002 -- Proyecto de Ingenieria de Datos · Universidad EAFIT · 2026-2  
> **Profesor:** Jose Fabio Jaramillo Castro

---

## Descripcion

**TrendAnalyzer** centraliza y procesa comentarios sobre una marca, sus productos y sus competidores para apoyar los procesos de investigacion de mercado y toma de decisiones de una empresa colombiana simulada del sector de bebidas y productos de consumo masivo.

Como las APIs reales de Reddit, Facebook e Instagram imponen aprobaciones y limites de acceso, el **streaming se simula**: un generador de comentarios en Python (`marca,comentario`) alimenta tres fuentes simuladas -- **Reddit, Facebook e Instagram** -- que publican a Pub/Sub cada 1-3 s. La plataforma anonimiza, aplica NLP preentrenado en espanol para estimar sentimiento y emocion, identifica tendencias emergentes y genera alertas ante cambios en la conversacion digital.

> Todo lo que ocurre despues de Pub/Sub es igual al pipeline que se usaria con fuentes reales; lo unico simulado es el origen del dato. Arquitectura, contratos y plan por lotes en [`ARCHITECTURE.md`](ARCHITECTURE.md).

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
  │ Secret Manager (llave HMAC) · GCP Monitoring + Logging               │
  └──────────────────────────────────────────────────────────────────────┘
        │ Parquet · periodico (HTTPS/TLS)
        ▼
                      ┌─ Amazon Web Services · lakehouse y analitica ──────────────┐
                      │ S3 (Raw → Trusted → Curated) · Glue ETL · Athena           │
  Loader historico ──►│ QuickSight (dashboards) · EventBridge                      │
  (mismo generador)   │ CloudWatch · IAM + KMS                                     │
                      └────────────────────────────────────────────────────────────┘
        │
        ▼  Consumo
  Marketing · Gerencia · Analistas · Analisis de relaciones
```

**GCP** es el plano de ingesta y procesamiento near-real-time. **AWS** es el Lakehouse y la capa analitica. Ver el detalle (diagrama Mermaid, contratos y decisiones) en [`ARCHITECTURE.md`](ARCHITECTURE.md). El diagrama [`LocalContext/trend_analyzer_multicloud.drawio`](LocalContext/trend_analyzer_multicloud.drawio) (abrir en [diagrams.net](https://app.diagrams.net)) corresponde a la version anterior y esta pendiente de actualizar (ver `ARCHITECTURE.md` sec. 1.2).

---

## Fuentes de datos

| Fuente | Mecanismo | Frecuencia | Estado |
|---|---|---|---|
| **Reddit** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-reddit` · payload estilo Reddit (subreddit, score, num_comments) | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Facebook** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-facebook` · payload estilo Meta (pagina, reacciones, shares) | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Instagram** (simulado) | Worker del `stream-simulator` -> Pub/Sub `ta-raw-instagram` · payload estilo Meta (cuenta, media, like_count) | 1 mensaje cada 1-3 s | Pendiente (Fase 1) |
| **Historico** | Mismo generador en batch (~50k registros, ultimos 180 dias) -> S3 `raw/origin=historical/` | Manual / one-shot | Sprint 2 |

### Generador de comentarios

El texto de todas las fuentes sale de un script de Python que produce pares `marca,comentario` en espanol colombiano (jerga, emojis, abreviaturas):

```csv
marca,comentario
Postobón,Opinión personal: soy fan d Postobón y no pienso cambiar. Volvería mil veces. 🙌
Pepsi,Mi aporte del día: no tolero Pepsi Cola en el trancón. Mejor me hubiera tomado un agua. Es mi opinión.
Sprite,Vamos al punto: me chifla Sprite sobre todo con el frío de Bogotá. Eso sí es sabor. 🥤
```

El simulador agrega lo que el texto no trae (red social, usuario, fechas, engagement, hashtags) y controla que marca se menciona en cada momento para crear tendencias. Los cambios propuestos al script (funcion pura con semilla, parametros `marca`/`sentimiento`, etiqueta opcional de sentimiento, mas variedad de plantillas) estan en [`ARCHITECTURE.md`](ARCHITECTURE.md) sec. 3.2.

> Los datos son 100% sinteticos: la asociacion marca-sentimiento es artificial. El objetivo del proyecto es ingenieria de datos (ingesta, calidad, linaje, latencia), no conclusiones de negocio.

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
| **Calidad** | Great Expectations · dbt |
| **Seguridad** | Secret Manager (llave HMAC de anonimizacion) · IAM + KMS (AWS) |
| **IaC / CI-CD** | Terraform · GitHub Actions · Docker · Artifact Registry |
| **Visualizacion** | Amazon QuickSight |
| **Observabilidad** | GCP Monitoring + Logging · AWS CloudWatch |

> Fuera del alcance v1: Firestore, Cloud SQL, Neptune y Dataplex (el analisis de relaciones se cubre con tablas de aristas en Curated). Ver `ARCHITECTURE.md` sec. 1.3.

---

## Estructura del repositorio

> Estructura objetivo (v2.2). Varias carpetas se crean por lotes (ver `ARCHITECTURE.md` sec. 7).

```
.
├── README.md                        <- este archivo
├── ARCHITECTURE.md                  <- arquitectura, contratos y plan por lotes
├── HU.md                            <- historias de usuario (HU-001 a HU-045)
├── TODO.md                          <- mis tareas
├── .gitignore
│
├── contracts/                       <- contratos legibles por maquina (envelope, payloads, BigQuery, Athena)
│
├── infra/                           <- Infraestructura como codigo (Terraform)
│   ├── README.md                    <- instrucciones de validacion local
│   ├── gcp/environments/dev/        <- GCP: Cloud Run, Pub/Sub, GCS, Scheduler, Secret Manager
│   └── aws/environments/dev/        <- AWS: S3, Glue, Athena, IAM
│
├── scripts/
│   └── generate_comments.py         <- CLI del generador (snapshots CSV / historico)
│
└── src/
    ├── common/                      <- envelope, anonimizacion HMAC, NLP, label_map
    ├── simulator/                   <- stream-simulator (Cloud Run)
    │   ├── app.py  publisher.py  state.py
    │   ├── generator/               <- comment_generator.py (script de comentarios marca,comentario)
    │   ├── enrich/                  <- usuarios, engagement por red, marcas, hashtags, caos
    │   ├── config/                  <- sim.yaml, brands.yaml
    │   └── tests/  Dockerfile  requirements.txt
    ├── streaming/                   <- Dataflow (validacion, dedup, anonimizacion, NLP, ventanas)
    ├── historical/                  <- full load historico (mismo generador)
    ├── exporter/                    <- gcs-to-s3
    ├── lakehouse/                   <- Glue jobs y dbt
    ├── quality/                     <- Great Expectations
    └── collectors/                  <- LEGADO: conectores reales Mastodon/Reddit/RSS (no se despliega)
```

> **`LocalContext/`** contiene documentos de referencia del proyecto (no se sube al repositorio -- ver `.gitignore`): README de contexto, analisis de fuentes, esquemas de datos, diagrama de arquitectura y script de tanteo. `FUENTES.md` y `tanteo_fuentes.py` corresponden al enfoque anterior (Mastodon/Reddit/RSS reales).

---

## Equipo

| Integrante | Rol | Responsabilidades |
|---|---|---|
| **Miguel Alejandro Gomez Duque** | Lider Tecnico · Arquitecto · DataOps | Arquitectura GCP/AWS · IAM · Lakehouse · Scrum Master · Git · CI/CD · Observabilidad |
| **Camila Martinez Montoya** | Ingeniero de Pipelines / Big Data | ETL/ELT · Dataflow · Spark/Flink · DAGs · Kafka/Pub/Sub |
| **Alejandro Sepulveda Posada** | Gobernanza y Calidad · Simulador | Simulador de streaming y generador de comentarios (Reddit/Facebook/Instagram) · Profiling (HU-031) · Calidad (HU-032) · DAMA-DMBOK · Catalogo · Linaje · Ley 1581 |

---

## Cronograma de sprints

| Sprint | Fecha | Peso | Entregable |
|---|---|---|---|
| **Sprint 0** | 29 jul 2026 | 10% | Propuesta · Canvas · arquitectura conceptual · KPIs · data assessment |
| **Sprint 1** | 26 ago 2026 | 30% | Infra provisionada · ingesta funcional · capa Raw · profiling · calidad basica |
| **Sprint 2** | 28 oct 2026 | 30% | Pipelines ETL/ELT · bases optimizadas · catalogo · linaje · serving layer |
| **Sprint Final** | 11 nov 2026 | 30% | CI/CD · monitoreo · dashboard gobernado · informe tecnico · sustentacion |

---

## KPIs clave

- Latencia de deteccion de tendencias **< 5 minutos** (95% de eventos validos)
- Completitud **>= 95%** · Unicidad **>= 99%** · Duplicados **< 1%** en capa confiable
- Tiempo de respuesta del dashboard **< 3 segundos**
- **100%** de identificadores de usuario anonimizados antes de la capa analitica
- Reduccion **>= 50%** del tiempo de elaboracion de informes de marketing

---

## Inicio rapido

### Validar infraestructura (sin credenciales reales)

```bash
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
# Imprime 10 filas marca,comentario con semilla fija
python scripts/generate_comments.py --n 10 --seed 7
```

### Correr pruebas unitarias del simulador (disponible desde el Lote 1.2)

```bash
pip install -r src/simulator/requirements.txt pytest

# Desde la raiz del repo
python -m pytest src/simulator/ -v
```

### Construir y desplegar el simulador (cuando existan credenciales GCP)

```bash
# 1. Autenticarse con Artifact Registry
gcloud auth configure-docker REGION-docker.pkg.dev

# 2. Construir la imagen
cd src/simulator
docker build -t REGION-docker.pkg.dev/PROJECT_ID/REPO/ta-stream-simulator:v1 .
docker push  REGION-docker.pkg.dev/PROJECT_ID/REPO/ta-stream-simulator:v1

# 3. Actualizar terraform.tfvars y aplicar
cd ../../infra/gcp/environments/dev
# Editar la variable de imagen del contenedor con la nueva imagen
terraform apply
```

---

## Notas importantes

> [!IMPORTANT]
> `terraform.tfvars` esta en `.gitignore` -- nunca subir credenciales al repositorio.

> [!WARNING]
> El archivo `LocalContext/tanteo_fuentes.py` contiene un token de Mastodon hardcodeado en la linea 34. Ya no se usa Mastodon, pero si el token sigue activo, revocarlo en mastodon.social.

> [!NOTE]
> `LocalContext/` no se sube al repositorio (`.gitignore`). Los documentos de referencia viven solo localmente -- hacer copia de seguridad independiente si se necesita persistencia.

> [!NOTE]
> Los datos son sinteticos, pero el pipeline los trata como datos reales: los identificadores de usuario se anonimizan con HMAC-SHA256 en el primer paso de procesamiento (Dataflow / loader historico), antes de salir de GCP.

---

## Licencia

Proyecto academico -- Universidad EAFIT · SI4002 · 2026-2. Uso educativo.
