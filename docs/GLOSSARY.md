# TrendAnalyzer · GLOSSARY

> Glosario de componentes, tecnologías y conceptos del proyecto.
> Cada entrada tiene dos partes: **Qué es** (la tecnología explicada en general) y **Rol en TrendAnalyzer** (qué hace concretamente aquí).
> Basado en `README.md`, `ARCHITECTURE.md` (v2.0) y `trend_analyzer_arquitectura_v2.drawio`.

---

## Índice

1. [Visión general en 30 segundos](#1-visión-general-en-30-segundos)
2. [Fuentes y simulación](#2-fuentes-y-simulación)
3. [Google Cloud Platform (plano caliente)](#3-google-cloud-platform-plano-caliente)
4. [Puente GCP → AWS](#4-puente-gcp--aws)
5. [Amazon Web Services (plano de lakehouse)](#5-amazon-web-services-plano-de-lakehouse)
6. [Procesamiento y NLP](#6-procesamiento-y-nlp)
7. [DataOps e infraestructura](#7-dataops-e-infraestructura)
8. [Calidad y gobernanza](#8-calidad-y-gobernanza)
9. [Conceptos transversales](#9-conceptos-transversales)
10. [Fuera del alcance (mencionados pero no usados en v2)](#10-fuera-del-alcance-mencionados-pero-no-usados-en-v2)
11. [Tabla resumen: componente → rol](#11-tabla-resumen-componente--rol)

---

## 1. Visión general en 30 segundos

TrendAnalyzer analiza **tendencias y emociones** en conversaciones digitales sobre una marca colombiana simulada de bebidas. Funciona en **dos nubes**:

- **GCP** = plano *caliente*: recibe los eventos en segundos, los limpia, anonimiza, les aplica NLP y detecta tendencias en minutos.
- **AWS** = plano de *lakehouse*: guarda todo en capas (Raw → Trusted → Curated), lo modela y lo sirve a dashboards.

En la versión 2 las fuentes (Mastodon, Reddit, RSS) están **simuladas** con datasets de emociones, pero todo lo que viene después es un pipeline real.

```
Datasets → stream-simulator → Pub/Sub → Dataflow → BigQuery + GCS
                                                        │
                                              gcs-to-s3 (cada 15 min)
                                                        ▼
                         S3 (Raw→Trusted→Curated) ← Glue → Athena → QuickSight
```

---

## 2. Fuentes y simulación

### Mastodon / Reddit / RSS (fuentes simuladas)

**Qué es:** Mastodon es una red social descentralizada tipo microblog; Reddit es un foro organizado en subreddits; RSS es un formato estándar para publicar titulares de noticias.

**Rol en TrendAnalyzer:** representan los tres tipos de fuente pública que la empresa querría monitorear (microblog, foro, medios). En v2 **no se consumen por API**: cada una se *simula* reproduciendo un dataset. Esto evita depender de aprobaciones OAuth (Reddit), límites de tasa y polling REST, que no es streaming real. El campo `source` del evento indica qué red se está simulando.

### Datasets de emociones (TEC, GoEmotions, SemEval-2007, DailyDialog, ISEAR)

**Qué es:** colecciones públicas de textos ya etiquetados con una emoción (alegría, tristeza, ira...), creadas originalmente para investigación en NLP.

**Rol en TrendAnalyzer:** son la "materia prima" del simulador. Cada fuente usa el dataset que más se parece a su naturaleza:

| Fuente | Dataset | Tamaño aprox. |
|---|---|---|
| Mastodon | **TEC** (Twitter Emotion Corpus) | ~21k tweets |
| Reddit | **GoEmotions** | ~58k comentarios |
| RSS / Noticias | **SemEval-2007 Task 14** (titulares) | ~1.25k |
| Histórico (full load) | **DailyDialog** | ~100k enunciados |
| Reserva | **ISEAR** | ~7.7k |

Sus etiquetas viajan en el campo `sim.label_*` como *ground truth* para **medir la precisión del modelo de NLP**; no se usan como la respuesta oficial del pipeline. Los datasets no se suben al repositorio.

### `stream-simulator`

**Qué es:** un servicio propio (Python, asyncio) empaquetado como contenedor y desplegado en Cloud Run.

**Rol en TrendAnalyzer:** es el "falso internet". Corre siempre encendido (`min=max=1`) con **3 workers**, uno por fuente. Cada uno lee su dataset de GCS, construye un evento y lo publica a Pub/Sub cada 1 a 3 segundos (~1.5 mensajes/s en total). Al terminar el dataset vuelve al inicio e incrementa el `cycle`. Vive en GCP, junto a Pub/Sub, para no cruzar credenciales entre nubes. Además fabrica los campos que los datasets no traen: usuarios, seguidores, engagement, marcas, hashtags y fechas.

### Cursor (`sim-state`) y `cycle`

**Qué es:** un cursor es un marcador de "voy por aquí"; `cycle` cuenta cuántas veces se ha recorrido el dataset completo.

**Rol en TrendAnalyzer:** el simulador guarda su posición (`cycle`, `row_index`) en `gs://…-ta-datasets/sim-state/{source}.json` cada 50 mensajes. Si el servicio se reinicia, continúa donde iba.

### Caos controlado (`SIM_CHAOS_ENABLED`)

**Qué es:** inyección deliberada de defectos en los datos.

**Rol en TrendAnalyzer:** produce duplicados (0.5%), eventos tardíos (1%), campos nulos (1%) y mensajes malformados (0.2%) para que los módulos de **calidad** tengan algo real que detectar y medir. Se puede apagar para obtener la línea base de los KPIs.

### Envelope canónico (v1.0)

**Qué es:** el "sobre" JSON estándar que envuelve cada evento: id, origen, fuente, fechas, autor, texto, marcas, hashtags y un `payload` específico de cada red.

**Rol en TrendAnalyzer:** es **el contrato más importante del proyecto**. Tanto el stream como el histórico lo usan, por eso ambos terminan en la misma tabla Trusted. Si dos componentes se comunican, su interfaz está definida aquí.

### `event_id`

**Qué es:** un identificador único por evento, calculado con `uuid5` a partir de origen, fuente, dataset, `cycle` y `row_index`.

**Rol en TrendAnalyzer:** es **determinista**: reenviar el mismo evento tras un reinicio produce el mismo id, lo que permite a Dataflow deduplicar. Es la clave de unicidad de todo el sistema.

---

## 3. Google Cloud Platform (plano caliente)

### Cloud Storage (GCS)

**Qué es:** almacenamiento de objetos (archivos) en la nube de Google: buckets que contienen carpetas y archivos de cualquier tipo, baratos y duraderos.

**Rol en TrendAnalyzer:** se usa con dos buckets:

- `{GCP_PROJECT}-ta-datasets`: **entrada del simulador**. Contiene los datasets preparados (`prepared/*.jsonl`) y el cursor (`sim-state/`).
- `{GCP_PROJECT}-ta-lake`: **salida de Dataflow**. Contiene `raw/` (el evento tal cual llegó, para auditoría y replay), `enriched/` (Parquet anonimizado y con NLP, que es lo único que sale hacia AWS), `dlq/` (mensajes inválidos) y `export-state/` (watermark del exportador).

### Cloud Run

**Qué es:** servicio de Google para ejecutar contenedores sin administrar servidores. Escala automáticamente y se paga por uso.

**Rol en TrendAnalyzer:** aloja el `stream-simulator` como servicio always-on (CPU siempre asignada, una sola instancia para evitar emisiones duplicadas). Expone `GET /healthz` y, opcionalmente, `POST /control/scenario` para disparar picos de demostración.

### Cloud Run Job

**Qué es:** variante de Cloud Run para tareas que *corren, terminan y se apagan* (en lugar de quedarse escuchando).

**Rol en TrendAnalyzer:** ejecuta `ta-gcs-to-s3`, el exportador que copia datos enriquecidos de GCS a S3.

### Cloud Scheduler

**Qué es:** un "cron" administrado: dispara tareas según un horario.

**Rol en TrendAnalyzer:** cada 15 minutos lanza el exportador `gcs-to-s3`. Ya no dispara polling de APIs (eso se eliminó en v2). Opcionalmente puede lanzar escenarios de demo.

### Pub/Sub

**Qué es:** servicio de mensajería asíncrona. Un productor publica mensajes en un **topic** y uno o más consumidores los reciben mediante una **suscripción**. Desacopla a quien produce de quien consume y absorbe picos de tráfico.

**Rol en TrendAnalyzer:** es el "tubo" entre simulación y procesamiento.

| Recurso | Para qué |
|---|---|
| `ta-raw-mastodon`, `ta-raw-reddit`, `ta-raw-rss` | Un topic por fuente; el simulador publica aquí. |
| `ta-raw-{source}-df` | Suscripciones que consume Dataflow. |
| `ta-dlq` | *Dead-letter queue*: mensajes que no pasan validación. |
| `ta-alerts` | Alertas de tendencias (Fase 6). |

Retiene mensajes 7 días y entrega **al menos una vez**, por eso la deduplicación es obligatoria. El atributo `event_id` del mensaje se usa como `id_label`.

### Dataflow

**Qué es:** servicio administrado de Google para ejecutar pipelines de **Apache Beam**, tanto en modo streaming (datos continuos) como batch.

**Rol en TrendAnalyzer:** es el **cerebro del plano caliente**. Lee de Pub/Sub y, en segundos, por cada evento:

1. Valida el envelope (los inválidos van a la DLQ).
2. Deduplica por `event_id`.
3. Anonimiza el autor con HMAC.
4. Normaliza el engagement (`likes`, `shares`, `comments`).
5. Aplica NLP (emoción y sentimiento).
6. Escribe en BigQuery y en GCS.
7. Calcula ventanas de tendencia.

### BigQuery

**Qué es:** almacén de datos analítico de Google. Permite consultar con SQL volúmenes enormes con muy baja latencia, sin administrar servidores.

**Rol en TrendAnalyzer:** es la **analítica caliente** en GCP (dataset `ta_stream`). Contiene:

- `posts_enriched`: un registro por post, ya anonimizado y con emoción/sentimiento. Particionada por fecha, con clustering por `source` y `brand_primary`.
- `trend_windows`: ventanas de 5 minutos por marca y fuente, con volumen, sentimiento promedio, z-scores y la bandera `is_alert`.
- `dlq_events`: mensajes rechazados.
- `dq_results`: resultados de calidad.

### Secret Manager

**Qué es:** bóveda para guardar secretos (llaves, contraseñas, tokens) con control de acceso y versionado.

**Rol en TrendAnalyzer:** en v2 ya no guarda tokens de Mastodon o Reddit. Guarda la **llave HMAC de anonimización** (`ta-hmac-key`), que Dataflow y el loader histórico leen para generar el `user_hash`.

### Cloud Monitoring + Logging

**Qué es:** servicios de Google para métricas, dashboards, alertas y recolección de logs.

**Rol en TrendAnalyzer:** observabilidad del plano GCP. Permite vigilar la latencia (KPI p95 < 5 min), la tasa de mensajes del simulador y el tamaño de la DLQ. Los logs de todo el código salen en JSON a stdout.

### Service accounts (cuentas de servicio)

**Qué es:** identidades no humanas que usan los programas para autenticarse y tener permisos.

**Rol en TrendAnalyzer:** aplican **mínimo privilegio**: `sa-simulator` solo publica en `ta-raw-*` y lee/escribe en el bucket de datasets; `sa-dataflow` consume Pub/Sub, escribe en BigQuery/lake y lee la llave HMAC; `sa-exporter` solo lee `enriched/` y puede emitir un token OIDC.

---

## 4. Puente GCP → AWS

### `gcs-to-s3` (exportador)

**Qué es:** un Cloud Run Job propio que copia archivos de un bucket de GCS a un bucket de S3.

**Rol en TrendAnalyzer:** es el **único camino de datos entre las dos nubes**. Cada 15 minutos lee `enriched/` en GCS (usando un *watermark* para saber qué ya copió) y sube los Parquet a `s3://…/raw/origin=stream/`. Es idempotente: si el objeto ya existe con el mismo tamaño, lo omite. Solo sale el dato ya anonimizado.

### OIDC / `AssumeRoleWithWebIdentity`

**Qué es:** mecanismo de federación de identidad. En lugar de guardar llaves permanentes de AWS, el exportador presenta un token firmado por Google y AWS le entrega credenciales temporales.

**Rol en TrendAnalyzer:** permite que `sa-exporter` (GCP) asuma el rol `ta-gcp-exporter-role` (AWS) **sin llaves estáticas**. El plan B, si la federación no funciona a tiempo, es un usuario IAM con llaves en Secret Manager (menos seguro, se documentaría como deuda técnica).

---

## 5. Amazon Web Services (plano de lakehouse)

### Amazon S3

**Qué es:** almacenamiento de objetos de AWS, equivalente a Cloud Storage. Es la base de casi cualquier data lake.

**Rol en TrendAnalyzer:** es el **Lakehouse**. Un único bucket (`ta-{ENV}-{account_id}-lake`) organizado en capas:

| Carpeta | Contenido |
|---|---|
| `raw/` | Datos tal como llegan (ya anonimizados), de origen `stream` e `historical`. |
| `trusted/` | Datos unificados, deduplicados, con tipos correctos y reglas de calidad aplicadas. |
| `curated/` | Modelo analítico listo para consumo (hechos, dimensiones, agregados, aristas). |
| `quality/` | Reportes de Great Expectations. |
| `athena-results/` | Resultados de consultas de Athena. |

Todo se guarda en **Parquet + Snappy**.

### AWS Glue ETL

**Qué es:** servicio de integración de datos administrado. Ejecuta jobs de transformación (ETL) con **Apache Spark** sin administrar un clúster.

**Rol en TrendAnalyzer:** mueve los datos entre capas, orquestado por el workflow `ta-wf-raw-to-curated`:

- `ta-job-raw-to-trusted`: une stream e histórico, deduplica por `event_id`, corrige tipos y limpia texto.
- `ta-job-trusted-to-curated`: construye el modelo analítico (con Glue o dbt).

### Glue Data Catalog

**Qué es:** catálogo central de metadatos: qué tablas existen, sus columnas, tipos y ubicación de los datos.

**Rol en TrendAnalyzer:** es el **catálogo oficial** del proyecto (reemplaza a Dataplex). Define las bases `ta_raw`, `ta_trusted` y `ta_curated`, que Athena consulta. Es donde se documentan descripciones de tablas y columnas (entregable de gobernanza).

### Amazon Athena

**Qué es:** servicio para consultar archivos en S3 con **SQL estándar**, sin servidores ni cargas previas. Se paga por datos escaneados.

**Rol en TrendAnalyzer:** es la **capa de consulta** sobre el lakehouse. Usa un workgroup propio (`ta-analytics`) con límite de bytes escaneados. Las particiones se resuelven con **partition projection** (sin crawlers), más simple y barato. QuickSight se conecta a través de Athena.

### Amazon EventBridge

**Qué es:** bus de eventos y programador de AWS. Reacciona a eventos o dispara acciones según un horario.

**Rol en TrendAnalyzer:** una regla `rate(1 hour)` dispara cada hora el workflow de Glue. Resuelve la "flecha huérfana" que tenía el diagrama anterior.

### Amazon QuickSight

**Qué es:** herramienta de inteligencia de negocios (BI) de AWS para crear dashboards interactivos.

**Rol en TrendAnalyzer:** es la **capa de visualización** para Marketing, Gerencia y Analistas. Sus datasets leen `ta_curated` vía Athena y usan **SPICE** para cumplir el KPI de respuesta < 3 segundos. El acceso se controla por roles IAM.

### SPICE

**Qué es:** el motor en memoria de QuickSight. Importa una copia de los datos para que los dashboards respondan casi instantáneamente.

**Rol en TrendAnalyzer:** es la razón por la que se puede cumplir el KPI de dashboard < 3 s sin depender de la velocidad de cada consulta a Athena.

### Amazon CloudWatch

**Qué es:** servicio de métricas, logs y alarmas de AWS.

**Rol en TrendAnalyzer:** observabilidad del plano AWS (jobs de Glue, ejecuciones, errores). Es el equivalente de Cloud Monitoring en GCP.

### IAM + KMS

**Qué es:** **IAM** gestiona identidades y permisos. **KMS** gestiona llaves de cifrado.

**Rol en TrendAnalyzer:** IAM define quién puede hacer qué (`ta-glue-role`, `ta-historical-loader`, `ta-quicksight-role`, `ta-gcp-exporter-role`, todos con mínimo privilegio). KMS cifra los objetos en S3 (SSE-KMS).

### Loader histórico (`historical/loader`)

**Qué es:** un script que genera un lote grande de registros de una sola vez (*full load*).

**Rol en TrendAnalyzer:** simula el **dataset histórico** (~50k registros por defecto, repartidos en los últimos 180 días, a partir de DailyDialog). Usa el mismo envelope, anonimiza con HMAC, corre el NLP en lote y sube Parquet a `s3://…/raw/origin=historical/`. Entra directo a AWS, sin pasar por Pub/Sub.

---

## 6. Procesamiento y NLP

### Apache Beam

**Qué es:** framework de código abierto para definir pipelines de datos en un modelo unificado (batch y streaming). Dataflow es uno de sus *runners*.

**Rol en TrendAnalyzer:** el pipeline de `src/streaming/` está escrito en Beam y se prueba localmente con `DirectRunner` antes de ejecutarlo en Dataflow.

### NLP preentrenado (`RunInference`)

**Qué es:** *Natural Language Processing* es la rama de la IA que interpreta texto. Un modelo **preentrenado** ya fue entrenado por otros y solo se aplica. `RunInference` es el transform de Beam para ejecutar un modelo dentro del pipeline.

**Rol en TrendAnalyzer:** para cada texto calcula `emotion_label` (`joy, sadness, anger, fear, disgust, surprise, neutral`) y `sentiment_label` (`negative/neutral/positive`), con sus puntajes. Usa el modelo `j-hartmann/emotion-english-distilroberta-base` (en inglés, ADR-05). Se registra la `nlp_model_version` en cada registro para trazabilidad.

### Anonimización HMAC-SHA256

**Qué es:** HMAC es una función de hash con llave secreta. Convierte un dato en un código irreversible y consistente: el mismo usuario siempre produce el mismo hash, pero no se puede revertir sin la llave.

**Rol en TrendAnalyzer:** convierte `author.user_id` y `author.handle` en `user_hash`. Se hace en el **primer paso de cada plano** (Dataflow o loader), de modo que ningún dato personal cruza a AWS. Cumple el KPI de 100% de identificadores anonimizados antes de la capa analítica, y mantiene el análisis de relaciones (el mismo usuario sigue siendo identificable como hash).

### Deduplicación

**Qué es:** eliminar registros repetidos.

**Rol en TrendAnalyzer:** se hace dos veces: en Dataflow (por `event_id`, usando el `id_label` de Pub/Sub) y de nuevo en Glue al construir Trusted. Necesaria porque Pub/Sub entrega *al menos una vez* y porque el caos del simulador inyecta duplicados.

### Dead-letter queue (DLQ)

**Qué es:** destino alterno para mensajes que no se pudieron procesar.

**Rol en TrendAnalyzer:** los mensajes malformados no se pierden ni rompen el pipeline: van al topic `ta-dlq`, a `gs://…/dlq/` y a la tabla `dlq_events`, con tipo y detalle del error.

### Ventanas deslizantes y `trend_windows`

**Qué es:** en streaming, una **ventana** agrupa eventos por tiempo. Una ventana *deslizante* se solapa: por ejemplo, 5 minutos de duración que avanza cada 1 minuto.

**Rol en TrendAnalyzer:** calcula por marca y fuente el volumen, la proporción positiva/negativa y el sentimiento promedio. Compara contra una línea base móvil mediante **z-scores**; cuando se desvía lo suficiente, marca `is_alert`. Es lo que sustenta el KPI de detección de tendencias en menos de 5 minutos.

### Watermark

**Qué es:** en Dataflow, la noción de "hasta qué momento del tiempo creo que ya llegaron todos los datos". En el exportador, el término se usa para el marcador de lo último ya copiado.

**Rol en TrendAnalyzer:** permite manejar los eventos tardíos que genera el caos controlado. En `export-state/watermark.json` el exportador guarda su avance para no recopiar archivos.

### Parquet

**Qué es:** formato de archivo **columnar** y comprimido, muy eficiente para analítica.

**Rol en TrendAnalyzer:** es el formato de todo lo que vive en `enriched/` (GCS) y en S3. Reduce costo y acelera Athena.

---

## 7. DataOps e infraestructura

### Terraform

**Qué es:** herramienta de **Infraestructura como Código (IaC)**: la infraestructura se describe en archivos y se crea o modifica con comandos.

**Rol en TrendAnalyzer:** aprovisiona todo en `infra/gcp/` e `infra/aws/` (un módulo por recurso lógico). Cada módulo expone `outputs` (nombres de topics, buckets, ARNs) que el código consume por variables de entorno en lugar de hardcodearlos.

### GitHub y GitHub Actions

**Qué es:** GitHub aloja el repositorio. GitHub Actions ejecuta flujos automáticos (CI/CD) cuando hay un push o pull request.

**Rol en TrendAnalyzer:** versiona el código y los contratos; ejecuta tests, `terraform fmt/validate/plan`, y construye y publica las imágenes Docker. Un test en CI verifica que no existan columnas `handle`/`user_id` en los datos publicados.

### Docker y Artifact Registry

**Qué es:** Docker empaqueta una aplicación con todas sus dependencias en una *imagen*. Artifact Registry es donde Google guarda esas imágenes.

**Rol en TrendAnalyzer:** el simulador, el exportador y el contenedor de Dataflow (con el modelo NLP) se construyen como imágenes y se despliegan desde Artifact Registry.

### Great Expectations

**Qué es:** librería para definir y ejecutar pruebas de calidad sobre los datos ("este campo no puede ser nulo", "este id debe ser único").

**Rol en TrendAnalyzer:** valida Trusted y BigQuery contra los KPIs: completitud ≥ 95%, unicidad ≥ 99%, duplicados < 1% y anonimización total. Escribe los resultados en `dq_results` y en `s3://…/quality/`.

### dbt

**Qué es:** herramienta para transformar datos con SQL de manera versionada, con tests y documentación automática.

**Rol en TrendAnalyzer:** construye el modelo Curated (`dbt-athena`) y sus pruebas. Sus *docs* generan el **linaje** de datos, que es el reemplazo de OpenLineage.

### Prefijo `ta-` y etiquetas

**Qué es:** convención de nombres.

**Rol en TrendAnalyzer:** todo recurso lleva el prefijo `ta-` y las etiquetas `project=trend-analyzer`, `env=dev`, `owner=<persona>`, lo que facilita rastrear costos y responsables.

---

## 8. Calidad y gobernanza

### Profiling (US-14)

**Qué es:** análisis exploratorio de un conjunto de datos para conocer su estructura, nulos, distribuciones y anomalías.

**Rol en TrendAnalyzer:** se aplica sobre Raw y Trusted para entender el estado real de los datos antes de definir reglas de calidad.

### Calidad de datos y KPIs

**Qué es:** medición de si los datos son completos, únicos, consistentes y oportunos.

**Rol en TrendAnalyzer:** cada KPI del proyecto tiene una regla medible (completitud, unicidad, duplicados, anonimización, latencia, dashboard < 3 s). Además se mide la **precisión del NLP** comparando `emotion_label` contra `label_unified` (extra).

### Catálogo y linaje

**Qué es:** el *catálogo* describe qué datos existen y qué significan. El *linaje* muestra de dónde vienen y cómo se transforman.

**Rol en TrendAnalyzer:** el catálogo vive en el Glue Data Catalog; el linaje se documenta con dbt docs y un diagrama.

### DAMA-DMBOK

**Qué es:** guía de referencia internacional de buenas prácticas en gestión de datos.

**Rol en TrendAnalyzer:** el marco con el que se estructura y documenta la gobernanza del proyecto.

### Ley 1581 (Colombia)

**Qué es:** ley colombiana de protección de datos personales (Habeas Data).

**Rol en TrendAnalyzer:** aunque los datos son sintéticos, se documenta el tratamiento como si fueran reales (anonimización, acceso restringido). Es el motivo por el que `raw/` en GCS tiene acceso restringido.

---

## 9. Conceptos transversales

### Multicloud

**Qué es:** usar más de un proveedor de nube en una misma solución.

**Rol en TrendAnalyzer:** GCP ofrece Pub/Sub y Dataflow para streaming; AWS ofrece Glue, Athena y QuickSight para el lakehouse. El único punto de integración es `gcs-to-s3`.

### Streaming vs. batch

**Qué es:** *streaming* procesa datos continuamente, evento por evento; *batch* los procesa en bloques periódicos.

**Rol en TrendAnalyzer:** el plano GCP es streaming (segundos). El plano AWS es batch (cada hora). El histórico es un *full load* batch único.

### Near-real-time

**Qué es:** procesamiento con latencia de segundos a pocos minutos, sin ser instantáneo.

**Rol en TrendAnalyzer:** es el objetivo del plano GCP; el KPI exige detectar tendencias en menos de 5 minutos (p95).

### Lakehouse

**Qué es:** arquitectura que combina el almacenamiento barato y flexible de un *data lake* con la estructura y consultas SQL de un *data warehouse*.

**Rol en TrendAnalyzer:** es lo que implementa AWS: archivos Parquet en S3 + catálogo en Glue + consultas SQL en Athena.

### Arquitectura por capas (Raw → Trusted → Curated)

**Qué es:** organizar el lago en niveles de refinamiento creciente (similar a la "arquitectura medallón").

**Rol en TrendAnalyzer:**

- **Raw:** tal cual llega.
- **Trusted:** limpio, deduplicado y validado.
- **Curated:** modelado para análisis.

### Modelo estrella (`fact_posts`, `dim_*`)

**Qué es:** diseño analítico con una tabla central de hechos rodeada de tablas de dimensiones.

**Rol en TrendAnalyzer:** `fact_posts` guarda un registro por post con sus métricas; `dim_date`, `dim_source`, `dim_brand` y `dim_user` describen el contexto. Se complementa con agregados (`agg_emotion_by_brand_hour`, `agg_trend_daily`).

### Tablas de aristas (`edges_*`)

**Qué es:** representación de relaciones como pares origen-destino con un peso (la base de un grafo).

**Rol en TrendAnalyzer:** `edges_user_brand` y `edges_hashtag_cooccurrence` cubren el "análisis de relaciones y propagación" sin necesidad de una base de grafos.

### Ground truth

**Qué es:** la respuesta "correcta" conocida, usada para evaluar un modelo.

**Rol en TrendAnalyzer:** las etiquetas originales de los datasets (`sim.label_unified`) sirven para medir qué tan bien clasifica el modelo de NLP. Nunca se usan como feature.

### Idempotencia

**Qué es:** propiedad de una operación que, aplicada varias veces, da el mismo resultado que aplicada una.

**Rol en TrendAnalyzer:** el `event_id` determinista y el exportador que omite objetos ya copiados permiten reintentos seguros sin duplicar datos.

### Envelope v1 vs. ADR

**Qué es:** un **ADR** (*Architecture Decision Record*) documenta una decisión de arquitectura y la alternativa descartada.

**Rol en TrendAnalyzer:** `ARCHITECTURE.md` §9 recoge 10 ADRs (por ejemplo, ADR-01: fuentes simuladas; ADR-02: simulador en Cloud Run; ADR-09: aristas en lugar de Neptune).

---

## 10. Fuera del alcance (mencionados pero no usados en v2)

| Componente | Qué es | Por qué quedó fuera |
|---|---|---|
| **Firestore** | Base de datos documental de GCP | El estado del simulador vive en un objeto de GCS. |
| **Cloud SQL** | Base relacional administrada (PostgreSQL) | No hay datos transaccionales en v1. |
| **Amazon Neptune** | Base de datos de grafos de AWS | Se cubre con tablas de aristas consultables en Athena; queda como extensión. |
| **Dataplex / OpenLineage** | Gobernanza y linaje de GCP / estándar abierto de linaje | Se usa Glue Data Catalog y dbt docs. |
| **`src/collectors/`** | Conectores REST reales de Mastodon, Reddit y RSS | Quedan como código legado/opcional; no se despliegan. |

---

## 11. Tabla resumen: componente → rol

| Componente | Nube | Rol en una frase |
|---|---|---|
| Datasets de emociones | GCP (GCS) | Materia prima del simulador y *ground truth* para evaluar el NLP. |
| `stream-simulator` | GCP (Cloud Run) | Reproduce los datasets como si fueran redes sociales en vivo. |
| Cloud Storage | GCP | Guarda datasets, estado del simulador y el lake de GCP (raw, enriched, dlq). |
| Cloud Run / Run Job | GCP | Ejecuta el simulador (siempre activo) y el exportador (por lotes). |
| Cloud Scheduler | GCP | Dispara el exportador cada 15 minutos. |
| Pub/Sub | GCP | Mensajería que desacopla el simulador de Dataflow. |
| Dataflow | GCP | Valida, deduplica, anonimiza, aplica NLP y calcula tendencias en tiempo casi real. |
| BigQuery | GCP | Analítica caliente: posts enriquecidos y ventanas de tendencia. |
| Secret Manager | GCP | Custodia la llave HMAC de anonimización. |
| Monitoring + Logging | GCP | Observabilidad del plano caliente. |
| `gcs-to-s3` + OIDC | GCP → AWS | Único puente entre nubes; copia solo datos anonimizados. |
| Loader histórico | AWS | Carga masiva única del dataset histórico a S3. |
| Amazon S3 | AWS | Lakehouse por capas Raw → Trusted → Curated. |
| AWS Glue ETL | AWS | Transforma entre capas con Spark. |
| Glue Data Catalog | AWS | Catálogo oficial de tablas y metadatos. |
| Athena | AWS | Consultas SQL sobre S3. |
| EventBridge | AWS | Dispara el workflow de Glue cada hora. |
| QuickSight (+ SPICE) | AWS | Dashboards para Marketing, Gerencia y Analistas. |
| CloudWatch | AWS | Observabilidad del plano AWS. |
| IAM + KMS | AWS | Permisos mínimos y cifrado. |
| Terraform | DataOps | Crea toda la infraestructura como código. |
| GitHub + Actions | DataOps | Versionado y CI/CD. |
| Docker + Artifact Registry | DataOps | Empaqueta y distribuye los contenedores. |
| Great Expectations | DataOps | Pruebas de calidad contra los KPIs. |
| dbt | DataOps | Modelo Curated, tests y linaje. |
| Modelo NLP (DistilRoBERTa) | GCP (Dataflow) | Clasifica emoción y sentimiento de cada texto. |
| HMAC-SHA256 | GCP / AWS | Anonimiza usuarios antes de que el dato salga de GCP. |