# Collectors — Ingesta de Datos TrendAnalyzer

> **Responsable:** lejandro Sepulveda Posada (US-10, US-11, US-12)  
> **Sprint:** Sprint 1 — entrega 26 de agosto de 2026

---

## Tabla de contenidos

1. [Visión general](#1-visión-general)
2. [¿Polling o WebSockets? — decisión de diseño](#2-polling-o-websockets--decisión-de-diseño)
3. [Mecanismo de activación](#3-mecanismo-de-activación)
4. [Flujo de datos end-to-end](#4-flujo-de-datos-end-to-end)
5. [Estructura de directorios](#5-estructura-de-directorios)
6. [Esquema común de ingesta](#6-esquema-común-de-ingesta)
7. [Conectores](#7-conectores)
   - [Mastodon (US-10)](#71-mastodon-us-10)
   - [Reddit (US-11)](#72-reddit-us-11)
   - [RSS / News (US-12)](#73-rss--news-us-12)
8. [Variables de entorno](#8-variables-de-entorno)
9. [Seguridad y credenciales](#9-seguridad-y-credenciales)
10. [Rate limits](#10-rate-limits)
11. [Deduplicación](#11-deduplicación)
12. [Cómo construir y desplegar las imágenes](#12-cómo-construir-y-desplegar-las-imágenes)
13. [Cómo correr las pruebas](#13-cómo-correr-las-pruebas)
14. [Plan B — Reddit sin OAuth](#14-plan-b--reddit-sin-oauth)

---

## 1. Visión general

Cada conector es un **servicio Cloud Run independiente** que expone un endpoint HTTP `POST /ingest`. Cuando se invoca, el conector:

1. Lee sus credenciales desde variables de entorno (inyectadas por Secret Manager vía Terraform).
2. Consulta la API de la fuente correspondiente (Mastodon, Reddit, RSS).
3. Mapea los campos al **esquema común de ingesta** (mismo JSON para las tres fuentes).
4. Publica cada evento como mensaje JSON al topic **Pub/Sub** `*-raw-events`.
5. Escribe el JSON crudo original en **Cloud Storage (GCS)** bajo `RAW_BUCKET_NAME/<fuente>/YYYY/MM/DD/`.

Los tres conectores comparten la misma imagen base (Python 3.12-slim + Gunicorn) pero son servicios Cloud Run separados, cada uno con su propia service account y sus propios secretos.

---

## 2. ¿Polling o WebSockets? — decisión de diseño

### Por qué usamos polling HTTP y no WebSockets

| Criterio | WebSockets / Streaming real | Polling HTTP (elegido) |
|---|---|---|
| **Modelo de cómputo** | Requiere proceso **persistente** (conexión abierta indefinida) | Compatible con **Cloud Run stateless** (instancia efímera) |
| **Costo** | Instancia activa 24/7 (GCE/GKE) | Instancia vive solo mientras procesa (~segundos) |
| **Complejidad** | Reconexión, heartbeat, manejo de backpressure | HTTP estándar con reintentos del Scheduler |
| **Disponibilidad en las APIs** | Mastodon sí ofrece SSE (`/api/v1/streaming`) · Reddit y RSS **no** | Las tres APIs soportan polling REST |
| **Latencia objetivo** | < 1 s | **< 5 min** (requisito del proyecto) |

**Conclusión:** el proyecto define una latencia objetivo de **< 5 minutos**. El polling cada 5-15 minutos activado por Cloud Scheduler cumple ese requisito con una arquitectura mucho más simple y económica que mantener procesos persistentes. El "streaming real" ocurre **después** del conector, en la capa **Dataflow → Pub/Sub** (responsabilidad de Camila).

### Flujo de latencia real

```
Evento publicado en Mastodon
        │
        │  ≤ 5 min (próxima ejecución del Scheduler)
        ▼
Cloud Run consume la API
        │
        │  ~1-2 s
        ▼
Pub/Sub recibe el mensaje
        │
        │  < 1 s (Dataflow en modo streaming)
        ▼
BigQuery / GCS disponible para analítica
```

---

## 3. Mecanismo de activación

### Cloud Scheduler → Cloud Run

```
┌──────────────────────────────────────────────────────────────┐
│                     Cloud Scheduler                          │
│                                                              │
│  reddit-ingestion-job   →  */15 * * * *  (America/Bogota)   │
│  news-ingestion-job     →  */30 * * * *  (America/Bogota)   │
└─────────────────────┬────────────────────────────────────────┘
                      │  POST /ingest
                      │  OIDC token (scheduler_invoker SA)
                      ▼
┌─────────────────────────────────────────┐
│            Cloud Run Collector          │
│  (instancia efímera — vive ~segundos)   │
└─────────────────────────────────────────┘
```

- **Reddit** y **RSS/News** son disparados por Cloud Scheduler (configurado en [`scheduler.tf`](../../infra/gcp/environments/dev/scheduler.tf)).
- **Mastodon** puede dispararse igual o invocarse manualmente vía `POST /ingest` (el Scheduler se puede agregar si se requiere cadencia fija).
- La autenticación entre Scheduler y Cloud Run usa **OIDC tokens** con la service account `scheduler_invoker` — no hay endpoints públicos desprotegidos.

### Body del request

```json
{
  "hashtag":    "colombia",       // Mastodon: hashtag a consultar (opcional)
  "subreddits": ["colombia"],     // Reddit: lista de subreddits (opcional)
  "feeds":      ["https://..."]   // RSS: lista de feeds (opcional)
}
```

Si el body está vacío, cada conector usa sus valores por defecto hardcodeados.

---

## 4. Flujo de datos end-to-end

```
                    ┌─────────────────────────────────────────────────────┐
                    │              GCP — Ingesta y procesamiento           │
                    │                                                     │
Mastodon API ──────►│                                                     │
(HTTPS REST)        │  Cloud Run          Pub/Sub         Dataflow        │
                    │  *-mastodon  ──────► *-raw-events ──► (streaming)   │
Reddit API ────────►│  *-reddit    ──────►               │               │
(OAuth2 REST)       │  *-news      ──────►               ▼               │
RSS Feeds ─────────►│                               BigQuery             │
(HTTP feedparser)   │                               Firestore            │
                    │                               Cloud Storage ────────┼──► Amazon S3
                    │                                    (GCS Raw)        │     (batch)
                    │  Secret Manager  Cloud SQL  Cloud Scheduler         │
                    │  (credenciales)  (checkpts)  (activación)           │
                    └─────────────────────────────────────────────────────┘
```

### Por cada evento publicado en Pub/Sub

```
JSON del esquema común
        │
        ├─► Pub/Sub topic: trend-dev-raw-events
        │         └─► suscripción dataflow → Dataflow pipeline (streaming)
        │                   ├─► BigQuery dataset (analítica)
        │                   └─► Firestore (documentos crudos)
        │
        └─► GCS bucket: trend-dev-{project_id}-raw
                  └─► <fuente>/YYYY/MM/DD/raw_HHMMSS.json
                            └─► (periódico) Amazon S3 batch-raw
                                      └─► AWS Glue ETL → Athena → QuickSight
```

---

## 5. Estructura de directorios

```
src/collectors/
├── README.md               ← este archivo
├── mastodon/               ← US-10
│   ├── main.py             ← Flask app + lógica de ingesta
│   ├── test_mastodon.py    ← pruebas unitarias (no requieren GCP)
│   ├── requirements.txt
│   └── Dockerfile
├── reddit/                 ← US-11
│   ├── main.py
│   ├── test_reddit.py
│   ├── requirements.txt
│   └── Dockerfile
└── news/                   ← US-12
    ├── main.py
    ├── test_news.py
    ├── requirements.txt
    └── Dockerfile
```

---

## 6. Esquema común de ingesta

Los tres conectores publican exactamente este JSON al topic Pub/Sub:

```json
{
  "source":          "mastodon | reddit | news",
  "id":              "identificador único de la publicación",
  "created_at":      "2026-08-25T13:45:08.445Z",
  "text":            "texto limpio (sin HTML)",
  "url":             "https://enlace-original",
  "language":        "es | null",
  "tags":            ["tag1", "tag2"],
  "author_name":     "nombre o handle del autor | null",
  "engagement": {
    "replies":       0,
    "reposts":       0,
    "likes":         0
  },
  "raw_metadata":    { }
}
```

> **`raw_metadata`** es un sub-objeto opcional con campos específicos de cada fuente que no pertenecen al esquema común (p. ej. `visibility` de Mastodon, `upvote_ratio` de Reddit, `feed_url` de RSS).

### Diferencias por fuente

| Campo | Mastodon | Reddit | RSS/News |
|---|---|---|---|
| `id` | toot `id` (string numérico) | fullname `t3_abc123` | URL del artículo |
| `created_at` | ISO8601 UTC directo | UNIX timestamp → UTC | RFC2822 `-05:00` → UTC |
| `text` | `content` limpiado de HTML | `title + " " + selftext` | `title + " " + summary` (HTML limpiado) |
| `language` | campo `language` de la API | `null` (no expuesto) | `null` (inferir en Trusted) |
| `tags` | `tags[].name` | `[subreddit]` | `tags[].term` |
| `author_name` | `account.username` | `author` | `null` (RSS inconsistente) |
| `engagement` | replies/reblogs/favourites | num_comments/0/score | `null` (RSS no tiene métricas) |

---

## 7. Conectores

### 7.1 Mastodon (US-10)

**API:** `GET /api/v1/timelines/tag/:hashtag` en `mastodon.social`  
**Autenticación:** Bearer token (access token de app registrada)  
**Paginación:** parámetro `max_id` (id del último toot de la página anterior)  
**Rate limit:** 300 requests / 5 minutos por token

```
POST /ingest
Body: {"hashtag": "colombia"}   ← hashtag sin el #

Respuesta 200:
{
  "source": "mastodon",
  "hashtag": "colombia",
  "fetched": 80,
  "published": 80,
  "errors": 0,
  "raw_gcs": "gs://trend-dev-xxx-raw/mastodon/2026/08/25/raw_134508.json"
}
```

**Detalles técnicos importantes:**
- El campo `content` de la API viene con HTML (`<p>`, `<a>`, `<span class="h-card">`, etc.) — se limpia con **BeautifulSoup** en `strip_html()`.
- El campo `created_at` ya viene en ISO8601 UTC — no requiere conversión.
- La paginación itera hasta 5 páginas por invocación (configurable con `max_pages`).

---

### 7.2 Reddit (US-11)

**API:** `GET /r/{subreddit}/new` en `oauth.reddit.com`  
**Autenticación:** OAuth2 `client_credentials` — token cacheado en memoria entre invocaciones del Scheduler  
**Rate limit:** 100 requests / minuto — monitoreado via header `X-Ratelimit-Remaining`

> ⚠️ **Desde mayo 2026** Reddit cerró el endpoint `.json` sin autenticación. Se requiere OAuth aprobado. Ver [Plan B](#14-plan-b--reddit-sin-oauth) si el acceso no está disponible.

```
POST /ingest
Body: {"subreddits": ["colombia", "cafe"]}

Respuesta 200:
{
  "source": "reddit",
  "subreddits": ["colombia", "cafe"],
  "fetched": 48,
  "published": 48,
  "errors": 0,
  "raw_gcs": "gs://trend-dev-xxx-raw/reddit/2026/08/25/raw_134508.json"
}
```

**Detalles técnicos importantes:**
- `created_utc` es un timestamp UNIX float en UTC → se convierte con `datetime.fromtimestamp(..., tz=timezone.utc).isoformat()`.
- El `id` en el esquema común es el **fullname** de Reddit (`t3_abc123`), no el `id` corto — garantiza unicidad global.
- `reposts` siempre es `0` (Reddit no tiene reposts directos en su API).
- El token OAuth se cachea en la instancia Cloud Run (variable global `_reddit_token`) y se renueva automáticamente antes de que expire.

---

### 7.3 RSS / News (US-12)

**Protocolo:** RSS 2.0 / Atom vía `feedparser`  
**Autenticación:** ninguna (feeds públicos) — la `NEWS_API_KEY_SECRET` queda disponible para ampliar a News API si se requiere  
**Frecuencia:** cada 30 minutos (Cloud Scheduler `news-ingestion-job`)

```
POST /ingest
Body: {"feeds": ["https://www.eltiempo.com/rss/economia.xml"]}

Respuesta 200:
{
  "source": "news",
  "feeds": 5,
  "fetched": 23,
  "published": 23,
  "errors": 0,
  "raw_gcs": "gs://trend-dev-xxx-raw/news/2026/08/25/raw_134508.json"
}
```

**Feeds por defecto configurados** (sector bebidas / consumo masivo Colombia):
- `eltiempo.com/rss/economia.xml`
- `elcolombiano.com/rss/economia.xml`
- `portafolio.co/rss/portafolio.xml`
- `elespectador.com/rss/economia/`
- `agronegocios.co/feed/`

**Detalles técnicos importantes (hallazgos del `output.txt`):**
- `published` viene en `"-05:00"` (Colombia) — se convierte a UTC con `published_parsed` de feedparser o `parsedate_to_datetime()`.
- `summary` puede contener HTML (confirmado: `"summary_detail.type": "text/html"`) — se limpia igual que `content` de Mastodon.
- `tags[].term` es el campo correcto (no `.label` ni `.scheme`).
- `id` = URL completa del artículo → clave de deduplicación dentro de la ejecución con un `set()`.

---

## 8. Variables de entorno

Todas las variables son inyectadas por la infra Terraform en `cloud-run.tf`. **Nunca hardcodear** valores en el código.

| Variable | Conector | Descripción |
|---|---|---|
| `MASTODON_ACCESS_TOKEN_SECRET` | mastodon | Access token de la app Mastodon |
| `REDDIT_CLIENT_ID_SECRET` | reddit | Client ID OAuth de Reddit |
| `REDDIT_CLIENT_SECRET_SECRET` | reddit | Client secret OAuth de Reddit |
| `REDDIT_USER_AGENT_SECRET` | reddit | User-Agent string de la app |
| `NEWS_API_KEY_SECRET` | news | API key de News API (opcional en RSS puro) |
| `PUBSUB_TOPIC` | todos | Nombre del topic Pub/Sub (no el ID completo) |
| `RAW_BUCKET_NAME` | todos | Nombre del bucket GCS raw |
| `GCP_PROJECT_ID` | todos | ID del proyecto GCP |
| `SOURCE_NAME` | todos | `"mastodon"`, `"reddit"` o `"news"` |

---

## 9. Seguridad y credenciales

- Los secretos se definen en `secret-manager.tf` y se **inyectan como variables de entorno** al contenedor por `cloud-run.tf`.
- Cada conector tiene su propia **service account** con permisos mínimos:
  - `pubsub.publisher` al topic `*-raw-events`
  - `storage.objectCreator` al bucket `*-raw`
  - `secretmanager.secretAccessor` solo a sus propios secretos
- ⚠️ El archivo `LocalContext/tanteo_fuentes.py` tiene un **token de Mastodon hardcodeado en la línea 34** — ese token debe **revocarse y rotarse** en mastodon.social → Preferencias → Desarrollo.

---

## 10. Rate limits

| Fuente | Límite | Estrategia implementada |
|---|---|---|
| Mastodon | 300 req / 5 min / token | Paginación de máx. 5 páginas por invocación; el Scheduler controla la frecuencia |
| Reddit | 100 req / min | Header `X-Ratelimit-Remaining` — log de advertencia si queda < 5; token OAuth cacheado para no gastar requests en reautenticación |
| RSS | Sin límite formal | feedparser con timeout de 15 s por feed; manejo de excepciones por feed individual |

---

## 11. Deduplicación

La deduplicación **dentro de una misma invocación** se hace con un `set()` de ids en memoria. La deduplicación **entre invocaciones** (no reprocesar lo ya publicado) es responsabilidad de:

- **Dataflow** (capa de Camila): idempotencia por `id` al escribir en BigQuery.
- **Cloud SQL** (`gcp_sql` en el diagrama): los checkpoints de `max_id` para paginación de Mastodon se pueden persistir aquí en una iteración futura.

---

## 12. Cómo construir y desplegar las imágenes

La variable `collector_container_image` en `terraform.tfvars` apunta actualmente a la imagen placeholder de Google. Cuando el código esté listo:

```bash
# 1. Autenticarse con Artifact Registry
gcloud auth configure-docker REGION-docker.pkg.dev

# 2. Construir y publicar (ejemplo Mastodon)
cd src/collectors/mastodon
docker build -t REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1 .
docker push  REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1

# 3. Actualizar terraform.tfvars y hacer apply
# collector_container_image = "REGION-docker.pkg.dev/PROJECT_ID/REPO/mastodon-collector:v1"
cd infra/gcp/environments/dev
terraform apply
```

> Los tres colectores **comparten la misma variable** `collector_container_image` en la infra actual (el `for_each` de `cloud-run.tf` usa la misma imagen para los tres). Si se quieren imágenes separadas, se debe agregar `container_image` al mapa `cloud_run_collectors` en `cloud-run.tf`.

---

## 13. Cómo correr las pruebas

Las pruebas **no requieren credenciales reales ni conexión a GCP o internet** — usan stubs de variables de entorno y no instancian clientes GCP.

```bash
# Desde la raíz del repo
pip install pytest flask requests feedparser beautifulsoup4

python -m pytest src/collectors/mastodon/test_mastodon.py -v
python -m pytest src/collectors/news/test_news.py       -v
python -m pytest src/collectors/reddit/test_reddit.py   -v

# O todos a la vez
python -m pytest src/collectors/ -v
```

---

## 14. Plan B — Reddit sin OAuth

Si la aprobación de la app OAuth de Reddit no llega antes del Sprint 1:

1. **Descargar** un dump académico tipo Pushshift (formato NDJSON) y subirlo manualmente al bucket GCS.
2. **Implementar** `POST /ingest-pushshift` en el conector Reddit que lea el archivo línea a línea con `ijson` y aplique el mismo `map_post()`.
3. **Documentar** la decisión en la matriz de riesgos del proyecto (riesgo ya identificado en el README del proyecto).
4. **Notificar** al equipo para que Dataflow y la capa Trusted traten esta fuente como batch en lugar de near-real-time.

El código base del Plan B está comentado al final de [`reddit/main.py`](reddit/main.py).
