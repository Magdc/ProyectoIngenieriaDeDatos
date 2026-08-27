# TrendAnalyzer

> Plataforma de análisis de tendencias y sentimiento en redes sociales y medios digitales.  
> **Asignatura:** SI4002  Proyecto de Ingeniería de Datos · Universidad EAFIT · 2026-2  
> **Profesor:** Jose Fabio Jaramillo Castro

---

## Descripción

**TrendAnalyzer** centraliza y procesa información pública proveniente de redes sociales, foros y medios de noticias para apoyar los procesos de investigación de mercado y toma de decisiones de una empresa colombiana simulada del sector de bebidas y productos de consumo masivo.

La plataforma recolecta, transforma y analiza publicaciones relacionadas con una marca, sus productos y competidores, aplicando NLP preentrenado para estimar sentimiento, identificar tendencias emergentes y generar alertas ante cambios en la conversación digital.

---

## Arquitectura

```
Fuentes de datos
  Mastodon 
  Reddit (OAuth)  
  RSS / Noticias  Cloud Run (collectors)  Pub/Sub  Dataflow
  Datasets históricos 
                                                                            
   Google Cloud Platform    
    Cloud Scheduler  Cloud Run  Pub/Sub  Dataflow                     
    Cloud Storage (GCS) · BigQuery · Firestore · Cloud SQL               
    Secret Manager · Dataplex · GCP Monitoring                           
     
             Parquet · periódico (HTTPS/TLS)                              
                                                                          
   Amazon Web Services    
    S3 (Raw  Trusted  Curated) · Glue ETL · Athena                  
    Neptune (grafos) · QuickSight (dashboards)                         
    EventBridge · CloudWatch · IAM + KMS                              
  
            
              Consumo
  Marketing · Gerencia · Analistas · Análisis de relaciones
```

**GCP** es el plano de ingesta y procesamiento near-real-time. **AWS** es el Lakehouse y la capa analítica. Ver el diagrama completo en [`LocalContext/trend_analyzer_multicloud.drawio`](LocalContext/trend_analyzer_multicloud.drawio) (abrir en [diagrams.net](https://app.diagrams.net)).

---

## Fuentes de datos

| Fuente | Mecanismo | Frecuencia | Estado |
|---|---|---|---|
| **Mastodon** | Polling REST · `GET /api/v1/timelines/tag/:hashtag` | Cloud Scheduler configurable |  Operativo (HTTP 200 confirmado) |
| **Reddit** | OAuth2 client_credentials · `GET /r/{sub}/new` | cada 15 min |  Código listo · (pendiente) OAuth pendiente aprobación |
| **RSS / Noticias** | feedparser · feeds colombianos públicos | cada 30 min |  Operativo |
| **Datasets históricos** | Carga batch a S3 | Manual / one-shot | (pendiente) Sprint 2 |

> **Nota sobre Reddit:** desde mayo 2026 el endpoint `.json` sin autenticación está cerrado. Se requiere aprobación OAuth manual. Si no llega a tiempo, activar el Plan B (archivo tipo-Pushshift). Ver análisis completo en [`LocalContext/FUENTES.md`](LocalContext/FUENTES.md).

---

## Stack tecnológico

| Capa | Tecnologías |
|---|---|
| **Ingesta** | Cloud Run (Python · Flask · Gunicorn) · Cloud Scheduler · Pub/Sub |
| **Procesamiento** | Dataflow (Beam) · AWS Glue (Spark) |
| **Almacenamiento Raw** | Cloud Storage (GCS) · Amazon S3 |
| **Analítica** | BigQuery · Amazon Athena |
| **Documental** | Firestore |
| **Grafos** | Amazon Neptune |
| **Transaccional** | Cloud SQL (PostgreSQL) |
| **Gobernanza** | GCP Dataplex · AWS Glue Data Catalog · OpenLineage |
| **Calidad** | Great Expectations · dbt |
| **Seguridad** | Secret Manager (GCP) · IAM + KMS (AWS) |
| **IaC / CI-CD** | Terraform · GitHub Actions · Docker · Artifact Registry |
| **Visualización** | Amazon QuickSight |
| **Observabilidad** | GCP Monitoring + Logging · AWS CloudWatch |

---

## Estructura del repositorio

```
.
 README.md                         este archivo
 TODO.md                           mis tareas (US-10 al US-14)
 .gitignore

 infra/                            Infraestructura como código (Terraform)
    README.md                     instrucciones de validación local
    gcp/environments/dev/         GCP: Cloud Run, Pub/Sub, GCS, Scheduler, Secret Manager
    aws/environments/dev/         AWS: S3, Glue, Athena, IAM

 src/
     collectors/                   Conectores de ingesta (US-10, US-11, US-12)
         README.md                 mecanismo completo de polling, flujos y decisiones de diseño
         mastodon/                 US-10: conector Mastodon
            main.py
            test_mastodon.py
            requirements.txt
            Dockerfile
         reddit/                   US-11: conector Reddit (OAuth)
            main.py
            test_reddit.py
            requirements.txt
            Dockerfile
         news/                     US-12: conector RSS / News
             main.py
             test_news.py
             requirements.txt
             Dockerfile
```

> **`LocalContext/`** contiene documentos de referencia del proyecto (no se sube al repositorio  ver `.gitignore`): README de contexto, análisis de fuentes, esquemas de datos, diagrama de arquitectura y script de tanteo.

---

## Equipo

| Integrante | Rol | Responsabilidades |
|---|---|---|
| **Miguel Alejandro Gómez Duque** | Líder Técnico · Arquitecto · DataOps | Arquitectura GCP/AWS · IAM · Lakehouse · Scrum Master · Git · CI/CD · Observabilidad |
| **Camila Martínez Montoya** | Ingeniero de Pipelines / Big Data | ETL/ELT · Dataflow · Spark/Flink · DAGs · Kafka/Pub/Sub |
| **Alejandro Sepulveda Posada** | Gobernanza y Calidad · Conectores | Conectores de ingesta (US-10/11/12) · Profiling (US-14) · Calidad (US-13) · DAMA-DMBOK · Catálogo · Linaje · Ley 1581 |

---

## Cronograma de sprints

| Sprint | Fecha | Peso | Entregable |
|---|---|---|---|
| **Sprint 0** | 29 jul 2026 | 10% | Propuesta · Canvas · arquitectura conceptual · KPIs · data assessment |
| **Sprint 1** | 26 ago 2026 | 30% | Infra provisionada · ingesta funcional · capa Raw · profiling · calidad básica |
| **Sprint 2** | 28 oct 2026 | 30% | Pipelines ETL/ELT · bases optimizadas · catálogo · linaje · serving layer |
| **Sprint Final** | 11 nov 2026 | 30% | CI/CD · monitoreo · dashboard gobernado · informe técnico · sustentación |

---

## KPIs clave

- Latencia de detección de tendencias **< 5 minutos** (95% de eventos válidos)
- Completitud ** 95%** · Unicidad ** 99%** · Duplicados **< 1%** en capa confiable
- Tiempo de respuesta del dashboard **< 3 segundos**
- **100%** de identificadores de usuario anonimizados antes de la capa analítica
- Reducción ** 50%** del tiempo de elaboración de informes de marketing

---

## Inicio rápido

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

### Correr pruebas unitarias de los conectores

```bash
# Instalar dependencias de prueba
pip install pytest flask requests feedparser beautifulsoup4

# Desde la raíz del repo
python -m pytest src/collectors/ -v
```

### Construir y desplegar un conector (cuando existan credenciales GCP)

```bash
# 1. Autenticarse con Artifact Registry
gcloud auth configure-docker REGION-docker.pkg.dev

# 2. Construir la imagen (ejemplo: Mastodon)
cd src/collectors/mastodon
docker build -t REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1 .
docker push  REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1

# 3. Actualizar terraform.tfvars y aplicar
cd ../../../infra/gcp/environments/dev
# Editar: collector_container_image = "REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1"
terraform apply
```

---

## Notas importantes

> [!IMPORTANT]
> `terraform.tfvars` está en `.gitignore`  nunca subir credenciales al repositorio.

> [!WARNING]
> El archivo `LocalContext/tanteo_fuentes.py` contiene un token de Mastodon hardcodeado en la línea 34. **Revocar y rotar ese token** en mastodon.social  Preferencias  Desarrollo antes de cualquier uso en producción.

> [!NOTE]
> `LocalContext/` no se sube al repositorio (`.gitignore`). Los documentos de referencia viven solo localmente  hacer copia de seguridad independiente si se necesita persistencia.

---

## Licencia

Proyecto académico  Universidad EAFIT · SI4002 · 2026-2. Uso educativo.
