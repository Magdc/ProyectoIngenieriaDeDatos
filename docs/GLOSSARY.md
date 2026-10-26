# TrendAnalyzer · GLOSSARY

> Glosario de componentes, tecnologías, conceptos y **documentos del repositorio**.
> Cada entrada tiene dos partes: **Qué es** (la tecnología o término explicado en general) y **Rol en TrendAnalyzer** (qué hace concretamente aquí).
> **Versión 2.8 · 17 oct 2026** · Alineado con `ARCHITECTURE.md` v2.8 (fuentes Reddit, Facebook e Instagram simuladas con un generador de comentarios) y con `docs/trend_analyzer_multicloud.drawio` v2.5.
> Si este glosario y `ARCHITECTURE.md` discrepan, manda `ARCHITECTURE.md`; este archivo se corrige en el mismo commit.

---

## Índice

1. [Visión general en 30 segundos](#1-visión-general-en-30-segundos)
2. [El repositorio y sus documentos](#2-el-repositorio-y-sus-documentos)
3. [Fuentes y simulación](#3-fuentes-y-simulación)
4. [Google Cloud Platform (plano caliente)](#4-google-cloud-platform-plano-caliente)
5. [Puente GCP → AWS](#5-puente-gcp--aws)
6. [Amazon Web Services (plano de lakehouse)](#6-amazon-web-services-plano-de-lakehouse)
7. [Procesamiento y NLP](#7-procesamiento-y-nlp)
8. [DataOps e infraestructura](#8-dataops-e-infraestructura)
9. [Calidad y gobernanza](#9-calidad-y-gobernanza)
10. [Conceptos transversales](#10-conceptos-transversales)
11. [Fuera del alcance (mencionados pero no usados en v2)](#11-fuera-del-alcance-mencionados-pero-no-usados-en-v2)
12. [Tabla resumen: componente → rol](#12-tabla-resumen-componente--rol)

---

## 1. Visión general en 30 segundos

TrendAnalyzer analiza **tendencias y emociones** en conversaciones digitales sobre marcas colombianas de bebidas (Postobón como marca propia; Pepsi y Sprite como competidores). Funciona en **dos nubes**:

- **GCP** = plano *caliente*: recibe los eventos en segundos, los limpia, anonimiza, les aplica NLP en español y detecta tendencias en minutos.
- **AWS** = plano de *lakehouse*: guarda todo en capas (Raw → Trusted → Curated), lo modela y lo sirve a dashboards.

Las fuentes (**Reddit, Facebook e Instagram**) están **simuladas**: un generador de comentarios en español colombiano produce el texto y el simulador inventa el resto (usuarios, engagement, fechas). Todo lo que viene después es un pipeline real. El objetivo es **ingeniería de datos** (ingesta, calidad, linaje, latencia), no conclusiones de negocio válidas.

```
Generador de comentarios → stream-simulator → Pub/Sub → Dataflow → BigQuery + GCS
                                                                        │
                                                              gcs-to-s3 (cada 15 min)
                                                                        ▼
                                 S3 (Raw→Trusted→Curated) ← Glue → Athena → QuickSight
```

---

## 2. El repositorio y sus documentos

El repositorio se organiza en dos carpetas con una regla simple: **en `docs/` se documenta; en `project/` se construye**. Los workflows de CI, `.gitignore` y `.pre-commit-config.yaml` viven en la raíz.

```
.                       README.md · .gitignore · .pre-commit-config.yaml · .github/workflows/
├── docs/               ARCHITECTURE.md · GLOSSARY.md · HU.md · NOTAS.md · diagrama .drawio / .jpg
└── project/            contracts/ · infra/ · scripts/ · src/ · docker-compose.yml
```

### `ARCHITECTURE.md` — la fuente de verdad

**Qué es:** el documento maestro de arquitectura, contratos y plan de trabajo (versión actual 2.8). Un *documento vivo*: se versiona, tiene registro de cambios por versión (§1) y se actualiza con cada decisión.

**Rol en TrendAnalyzer:** es **lo más importante del repositorio**, porque está pensado para que baste con leerlo (o adjuntarlo a una petición) para construir cualquier pieza sin tener que enviar el código ya hecho. Contiene, entre otros:

| Sección | Para qué sirve |
|---|---|
| §1 | Qué cambió y por qué (registro de versiones). |
| §2 | Diagrama de la arquitectura objetivo. |
| §3 | Cómo funciona el simulador y el generador de comentarios. |
| **§5** | **Los contratos**: formatos, nombres, topics, tablas, IAM. Es la sección que hace innecesario enviar código. |
| §7 | Fases, lotes y matriz de trazabilidad hacia las HU. |
| §8 | Protocolo para pedir un lote de trabajo. |
| §9 | ADR, riesgos y preguntas abiertas. |
| §10 | Estado del proyecto por fase. |

**Regla de oro:** si dos componentes se comunican, su interfaz vive en §5; si algo no está en §5, no es una interfaz y ningún otro componente debe depender de él. **Cuando una interfaz cambia, se cambia primero en `ARCHITECTURE.md` y luego en el código**, y en el mismo commit. Por eso también es lo primero que se adjunta al pedir cualquier trabajo.

### `GLOSSARY.md` — este archivo

**Qué es:** el vocabulario del proyecto: tecnologías, componentes y términos propios.

**Rol en TrendAnalyzer:** da un lenguaje común al equipo (Miguel, Camila y Alejandro) y sirve de apoyo para el informe y la sustentación. Explica el *qué* y el *para qué* de cada pieza; el *cómo exacto* (formatos, nombres) está en `ARCHITECTURE.md` §5. Se actualiza cada vez que aparece un término, componente o decisión nueva.

### `HU.md` — historias de usuario

**Qué es:** una **historia de usuario (HU)** describe una necesidad desde quien la tiene ("como analista quiero…") junto con sus **criterios de aceptación**, que dicen cuándo está cumplida. `HU.md` las reúne en formato **HU-000** (HU-001 a HU-048) y trae una tabla resumen de estado.

**Rol en TrendAnalyzer:** define **qué significa "terminado"**. Cada fase y cada lote de `ARCHITECTURE.md` §7 declara las HU que cubre, y la **matriz de trazabilidad** (§7.1) asigna cada HU a exactamente un lote (o a la Fase 0). Una fase se cierra cuando todas sus HU cumplen sus criterios. `HU.md` es la fuente de verdad de las historias: si se agrega o reasigna una, se cambia primero allí y la matriz en el mismo commit. Las antiguas *US* del README se relacionan con las HU (p. ej. US-14 → HU-031).

### `README.md`

**Qué es:** la puerta de entrada del repositorio.

**Rol en TrendAnalyzer:** resume entornos, marcas, decisiones del generador, stack, estado por fases, estructura y un *quick start*. Es breve y apunta a `ARCHITECTURE.md` para el detalle; no debe contradecirlo.

### `NOTAS.md`

**Qué es:** notas de trabajo del equipo.

**Rol en TrendAnalyzer:** espacio informal para apuntes. **No es fuente de verdad**: lo que se decida y deba respetar el código se pasa a `ARCHITECTURE.md`.

### Diagrama (`trend_analyzer_multicloud.drawio` y `.jpg`)

**Qué es:** el diagrama editable de la arquitectura (formato draw.io) y su exportación a imagen.

**Rol en TrendAnalyzer:** la vista gráfica de §2. Está alineado con la v2.5; se actualiza cuando cambia la arquitectura.

### `project/contracts/`

**Qué es:** la copia **legible por máquina** de los contratos de §5: JSON Schemas (envelope, envelope enriquecido, un payload por red social, atributos de Pub/Sub), DDL de BigQuery y Athena, ejemplos válidos e inválidos y pruebas.

**Rol en TrendAnalyzer:** convierte los contratos en algo **verificable**: los generadores y el CI validan eventos reales contra estos schemas, y un test comprueba que BigQuery, Athena y el envelope son coherentes entre sí. Se entregó en el Lote 0.1 (HU-001). Su `README.md` lista las decisiones tomadas y lo pendiente.

### Otras carpetas de `project/`

| Ruta | Contenido |
|---|---|
| `infra/{gcp,aws}/` | Terraform: un módulo por recurso lógico y entornos en `environments/dev/`. |
| `scripts/` | CLI del generador (`generate_comments.py`) y prueba de humo extremo a extremo (`smoke_e2e.py`). |
| `src/common/` | Librería compartida (envelope, anonimización, NLP, mapeo de etiquetas) que usan Dataflow **y** el loader histórico, para que stream e histórico sean comparables. |
| `src/simulator/` | Simulador y generador de comentarios. |
| `src/streaming/`, `src/historical/`, `src/exporter/`, `src/lakehouse/`, `src/quality/` | Dataflow, loader histórico, exportador, Glue/dbt y calidad. |
| `src/collectors/` | **Legado**: conectores reales de Mastodon, Reddit y RSS; no se despliega. |
| `docker-compose.yml` | Entorno local con emulador de Pub/Sub, sin credenciales. |

### Lote

**Qué es:** una unidad de trabajo de tamaño razonable, numerada `fase.lote` (p. ej. Lote 2.2).

**Rol en TrendAnalyzer:** cada lote entrega un conjunto cerrado de archivos con pruebas, en las rutas exactas de §6, junto con la confirmación de qué criterios de aceptación de cada HU quedan cubiertos y una lista de **"contratos tocados"** (cambios que exigen actualizar §5 primero). Se pide con la **plantilla de §8** (lote, HU, contexto, entorno real, restricciones).

### Fase

**Qué es:** agrupación de lotes por objetivo (Fase 0 Alineación … Fase 8 Informe).

**Rol en TrendAnalyzer:** ordena el plan hacia los dos hitos: **Sprint 2 (28 oct)** y **Sprint Final (11 nov)**. Las fases 1, 2 y 3 pueden trabajarse en paralelo una vez cerrada la Fase 0. El estado de cada una (⬜ pendiente · 🟨 en curso · ✅ completada) se marca en §10.

### Responsables (M, C, A)

**Qué es:** iniciales de reparto de trabajo: **M** = Miguel (infra/IAM/CI-CD), **C** = Camila (pipelines), **A** = Alejandro (simulador/calidad).

**Rol en TrendAnalyzer:** aparecen en la matriz de trazabilidad y en la tabla de estado; el valor `owner=alejandro` también va como etiqueta en los recursos.

### ADR (*Architecture Decision Record*)

**Qué es:** registro corto de una decisión de arquitectura, con la alternativa descartada.

**Rol en TrendAnalyzer:** `ARCHITECTURE.md` §9 recoge 14 ADR (p. ej. ADR-01 fuentes simuladas, ADR-02 simulador en Cloud Run, ADR-05 NLP en español, ADR-11 generador integrado en el simulador, ADR-12 región `us-east1`, ADR-13 mapeo categoría → sentimiento, ADR-14 estado remoto de Terraform). ADR-02 y ADR-13, que se habían adoptado "por defecto", fueron aprobados el 17 oct.

### Versionado de documentos (`vX.Y`) y `schema_version`

**Qué es:** dos números distintos. La versión del documento (p. ej. v2.8) cuenta los cambios de `ARCHITECTURE.md`. `schema_version` es la versión del **contrato del envelope** (hoy `"1.0"`).

**Rol en TrendAnalyzer:** el documento sube de versión con cualquier cambio; `schema_version` solo cambia con un cambio **incompatible** del envelope (agregar un campo opcional no la cambia, pero sí exige actualizar el schema primero).

### Contratos en CI y pre-commit

**Qué es:** controles automáticos antes de aceptar un cambio.

**Rol en TrendAnalyzer:** la salida del generador y del simulador (≥ 1.000 eventos por fuente, con el caos apagado) se valida contra `contracts/*.schema.json`; `gitleaks` busca secretos antes del commit; `terraform.tfvars` está en `.gitignore` y se versiona `terraform.tfvars.example`.

---

## 3. Fuentes y simulación

### Reddit / Facebook / Instagram (fuentes simuladas)

**Qué es:** Reddit es un foro organizado en subreddits; Facebook es una red social con páginas de marca, publicaciones y reacciones; Instagram es una red visual centrada en publicaciones y sus comentarios.

**Rol en TrendAnalyzer:** son las tres fuentes públicas que la empresa querría monitorear. Solo se pueden consumir por APIs REST/OAuth (polling), que no es streaming real, y dependen de aprobaciones y límites de acceso (OAuth en Reddit, revisión de app y permisos en Meta). Por eso **no se consumen por API**: cada una se *simula*. El campo `source` indica qué red se simula (`reddit`, `facebook` o `instagram`) y el `payload` imita el estilo de cada red.

| Fuente | `kind` | Contexto simulado |
|---|---|---|
| Reddit | `post` 30 % · `comment` 70 % | Subreddit de una lista (`r/colombia`, `r/medellin`…) |
| Facebook | `post` 15 % · `comment` 85 % | Página de la marca comentada |
| Instagram | `comment` 100 % | Publicación de la cuenta de la marca |

### Generador de comentarios

**Qué es:** un módulo Python (`src/simulator/generator/comment_generator.py`) que produce pares `marca,comentario` a partir de plantillas, en español coloquial colombiano, con jerga, abreviaturas, typos y emojis.

**Rol en TrendAnalyzer:** es el **origen del texto**. No es un servicio aparte: corre en el mismo proceso del simulador y también en batch para el histórico. Se expone como una función pura `generate(rng, marca=None, sentimiento=None)` con un `random.Random` inyectado, lo que da determinismo. Sus **categorías** (`gusto`, `asco`, `nostalgia`, `adiccion` e `informativo`) se mapean a sentimiento (`gusto → positive`, `asco → negative`, `informativo → neutral`; `nostalgia` y `adiccion → null`, por ambiguas; ADR-13). Reemplazó a los datasets de emociones en inglés de la v2.1, que no hablaban de las marcas del negocio.

### Marcas y `brands.yaml`

**Qué es:** el archivo de configuración (`src/simulator/config/brands.yaml`) que define cada marca: clave canónica, nombre visible, alias, rol, página de Facebook y cuenta de Instagram.

**Rol en TrendAnalyzer:** v1 usa **Postobón** (`postobon`, propia), **Pepsi** (`pepsi`) y **Sprite** (`sprite`), ambas competidoras. CocaCola y Colombiana aparecen en el script original pero quedan fuera de v1. La **clave canónica** (minúsculas, sin tildes) es la que viaja en `brands[]` y la que usan los joins y `dim_brand`. El simulador elige la marca con pesos que varían con `seq`, para crear tendencias reproducibles.

### Hashtags (`hashtags[]`)

**Qué es:** etiquetas con `#` que las personas escriben en sus publicaciones (`#Postobón`, `#Colombia`).

**Rol en TrendAnalyzer:** el texto conserva cada hashtag tal como se escribió, y el campo `hashtags[]` los guarda en una **forma canónica**: minúsculas, sin `#`, sin tildes, solo `[a-z0-9_]`, sin repetidos y en orden de aparición (`#Postobón` ⇒ `"postobon"`). Así las claves de `edges_hashtag_cooccurrence` y los joins son estables, igual que las claves de marca.

### `stream-simulator`

**Qué es:** un servicio propio (Python, asyncio) empaquetado como contenedor y desplegado en Cloud Run.

**Rol en TrendAnalyzer:** es el "falso internet". Corre siempre encendido (`min=max=1`) con **3 workers**, uno por fuente. Cada uno llama al generador, construye el envelope y lo publica a Pub/Sub cada 1 a 3 segundos (~0.5 mensajes/s por fuente, ~1.5 en total). El generador es inagotable, así que ya no hay "vueltas" de dataset. Vive en GCP, junto a Pub/Sub, para no cruzar credenciales entre nubes. Fabrica los campos que el generador no trae: usuarios, seguidores, engagement, hashtags, fechas, subreddit/página/cuenta, `kind` y `parent_id`.

### Semilla (`SIM_SEED`) y reproducibilidad

**Qué es:** el valor inicial del que sale todo el aleatorio.

**Rol en TrendAnalyzer:** misma semilla + fuente + `seq` ⇒ mismo comentario. Cada evento usa su propio `Random(f"{seed}|{fuente}|{seq}")`; **no se usa `hash()` de Python**, que varía entre procesos y rompería la reproducibilidad. Los escenarios de demo son la excepción y quedan marcados en `sim.scenario`.

### Cursor (`sim-state`) y `seq`

**Qué es:** un cursor es un marcador de "voy por aquí"; `seq` es la posición del evento en la secuencia de su fuente.

**Rol en TrendAnalyzer:** el simulador guarda `{"seed", "seq", "updated_at"}` en `gs://…-ta-datasets/sim-state/{source}.json` cada 50 mensajes. Si el servicio se reinicia, continúa donde iba (puede repetir hasta 50 mensajes, lo cual es deduplicable). Si la semilla configurada no coincide con la del cursor, empieza de cero.

### Escenarios de demo (`/control/scenario`)

**Qué es:** un endpoint protegido que fuerza durante un tiempo una marca y un sentimiento (por ejemplo, una ola de comentarios negativos sobre `postobon`) y sube la frecuencia.

**Rol en TrendAnalyzer:** permite mostrar alertas en la sustentación sin esperar una tendencia real. Los eventos quedan marcados en `sim.scenario`. Es opcional (Fase 6).

### Caos controlado (`SIM_CHAOS_ENABLED`)

**Qué es:** inyección deliberada de defectos en los datos.

**Rol en TrendAnalyzer:** produce duplicados (0.5 %), eventos tardíos (1 %), campos nulos (1 %) y mensajes malformados (0.2 %) para que los módulos de **calidad** tengan algo real que detectar y medir. Se puede apagar para obtener la línea base de los KPIs. Los defectos se registran en `sim.chaos`.

### Envelope canónico (v1.0)

**Qué es:** el "sobre" JSON estándar que envuelve cada evento: id, origen, fuente, generador, `seq`, fechas, autor, texto, marcas, hashtags, un `payload` específico de cada red y el bloque `sim`.

**Rol en TrendAnalyzer:** es **el contrato más importante del proyecto**. Tanto el stream como el histórico lo usan, por eso ambos terminan en la misma tabla Trusted. Existe en dos formas: el **envelope** (con `author.user_id` y `author.handle`, solo hasta Dataflow/loader) y el **envelope enriquecido** (sin esos campos, con `user_hash`, engagement normalizado y resultados de NLP).

### `event_id`

**Qué es:** un identificador único por evento, calculado con `uuid5` a partir de origen, fuente, semilla y `seq`.

**Rol en TrendAnalyzer:** es **determinista**: reenviar el mismo evento tras un reinicio produce el mismo id, lo que permite a Dataflow deduplicar. Es la clave de unicidad de todo el sistema. Los duplicados se miden por `event_id`, **no por texto**: el generador usa plantillas y el mismo texto puede repetirse sin ser un duplicado.

### Ground truth (`sim.label_sentiment`)

**Qué es:** la respuesta "correcta" conocida, usada para evaluar un modelo.

**Rol en TrendAnalyzer:** el generador conoce el sentimiento del comentario que produjo (`positive`, `negative`, `neutral` o `null`). Sirve **solo** para medir la precisión del NLP; nunca se usa como feature ni como respuesta del pipeline. Como la asociación marca→sentimiento es artificial y el vocabulario acotado, las métricas serán optimistas, y el informe lo declara.

### Histórico (*full load*)

**Qué es:** una carga masiva única de registros pasados.

**Rol en TrendAnalyzer:** el loader genera ~50.000 registros (configurable) con el **mismo generador** y otra semilla, con fechas en los últimos 180 días, para tener historia que comparar. Usa el mismo envelope (`origin = historical`).

---

## 4. Google Cloud Platform (plano caliente)

### Cloud Storage (GCS)

**Qué es:** almacenamiento de objetos (archivos) en la nube de Google: buckets que contienen carpetas y archivos de cualquier tipo, baratos y duraderos.

**Rol en TrendAnalyzer:** se usa con dos buckets:

- `{GCP_PROJECT}-ta-datasets`: **estado del simulador** (`sim-state/`) y snapshots opcionales del generador (`snapshots/`, no se versionan en git).
- `{GCP_PROJECT}-ta-lake`: **salida de Dataflow**. Contiene `raw/` (el evento tal cual llegó, para auditoría y replay), `enriched/` (Parquet anonimizado y con NLP, que es lo único que sale hacia AWS), `dlq/` (mensajes inválidos) y `export-state/` (watermark del exportador).

### Cloud Run

**Qué es:** servicio de Google para ejecutar contenedores sin administrar servidores. Escala automáticamente y se paga por uso.

**Rol en TrendAnalyzer:** aloja el `stream-simulator` como servicio always-on (CPU siempre asignada, una sola instancia para evitar emisiones duplicadas y que el cursor se pise). Expone `GET /healthz` y, opcionalmente, `POST /control/scenario`. Si el costo molesta, el plan B es una VM `e2-small` con el mismo contenedor (ADR-02).

### Cloud Run Job

**Qué es:** variante de Cloud Run para tareas que *corren, terminan y se apagan* (en lugar de quedarse escuchando).

**Rol en TrendAnalyzer:** ejecuta `ta-gcs-to-s3`, el exportador que copia datos enriquecidos de GCS a S3.

### Cloud Scheduler

**Qué es:** un "cron" administrado: dispara tareas según un horario.

**Rol en TrendAnalyzer:** cada 15 minutos lanza el exportador `gcs-to-s3`. Opcionalmente puede lanzar escenarios de demo.

### Pub/Sub

**Qué es:** servicio de mensajería asíncrona. Un productor publica mensajes en un **topic** y uno o más consumidores los reciben mediante una **suscripción**. Desacopla a quien produce de quien consume y absorbe picos de tráfico.

**Rol en TrendAnalyzer:** es el "tubo" entre simulación y procesamiento.

| Recurso | Para qué |
|---|---|
| `ta-raw-reddit`, `ta-raw-facebook`, `ta-raw-instagram` | Un topic por fuente; el simulador publica aquí. |
| `ta-raw-{source}-df` | Suscripciones que consume Dataflow. |
| `ta-dlq` | *Dead-letter queue*: mensajes que no pasan validación. |
| `ta-alerts` | Alertas de tendencias (Fase 6). |

Retiene mensajes 7 días y entrega **al menos una vez**, por eso la deduplicación es obligatoria. Cada mensaje lleva los atributos `event_id`, `source`, `origin` y `schema_version`; `event_id` se usa como `id_label`.

### Emulador de Pub/Sub y entorno local

**Qué es:** una versión local de Pub/Sub que corre en Docker, sin credenciales de nube.

**Rol en TrendAnalyzer:** el `docker-compose` levanta el emulador, el simulador y un consumidor que valida contra los schemas, de modo que el simulador se desarrolle y pruebe antes de que exista la infraestructura (HU-047).

### Dataflow

**Qué es:** servicio administrado de Google para ejecutar pipelines de **Apache Beam**, tanto en modo streaming (datos continuos) como batch.

**Rol en TrendAnalyzer:** es el **cerebro del plano caliente**. Lee de Pub/Sub y, en segundos, por cada evento:

1. Valida el envelope (los inválidos van a la DLQ).
2. Deduplica por `event_id`.
3. Anonimiza el autor con HMAC.
4. Normaliza el engagement (`likes`, `shares`, `comments`).
5. Aplica NLP (sentimiento y emoción).
6. Escribe en BigQuery y en GCS.
7. Calcula ventanas de tendencia.

### BigQuery

**Qué es:** almacén de datos analítico de Google. Permite consultar con SQL volúmenes enormes con muy baja latencia, sin administrar servidores.

**Rol en TrendAnalyzer:** es la **analítica caliente** en GCP (dataset `ta_stream`). Contiene:

- `posts_enriched`: un registro por post, ya anonimizado y con sentimiento/emoción. Particionada por fecha (expira a 90 días), con clustering por `source` y `brand_primary`.
- `trend_windows`: ventanas de 5 minutos por marca y fuente, con volumen, sentimiento promedio (con signo, de −1 a 1), z-scores y la bandera `is_alert`.
- `dlq_events`: mensajes rechazados.
- `dq_results`: resultados de calidad, una fila por verificación.

### Secret Manager

**Qué es:** bóveda para guardar secretos (llaves, contraseñas, tokens) con control de acceso y versionado.

**Rol en TrendAnalyzer:** guarda la **llave HMAC de anonimización** (`ta-hmac-key`), que Dataflow y el loader histórico leen para generar el `user_hash`. Ya no hay tokens de redes sociales.

### Cloud Monitoring + Logging

**Qué es:** servicios de Google para métricas, dashboards, alertas y recolección de logs.

**Rol en TrendAnalyzer:** observabilidad del plano GCP: latencia (KPI p95 < 5 min), tasa de mensajes del simulador y tamaño de la DLQ. Los logs de todo el código salen en JSON a stdout. También envía las **alertas por correo**: un suscriptor de `ta-alerts` escribe un log estructurado y una alerta basada en logs avisa por un canal de tipo email (sin SMTP ni secretos).

### Service accounts (cuentas de servicio)

**Qué es:** identidades no humanas que usan los programas para autenticarse y tener permisos.

**Rol en TrendAnalyzer:** aplican **mínimo privilegio**: `sa-simulator` solo publica en `ta-raw-*` y accede al bucket de datasets; `sa-dataflow` consume Pub/Sub, escribe en BigQuery/lake y lee la llave HMAC; `sa-exporter` solo lee `enriched/` y puede emitir un token OIDC.

---

## 5. Puente GCP → AWS

### `gcs-to-s3` (exportador)

**Qué es:** un Cloud Run Job propio que copia archivos de un bucket de GCS a un bucket de S3.

**Rol en TrendAnalyzer:** es el **único camino de datos entre las dos nubes**. Cada 15 minutos lee `enriched/` en GCS (usando un *watermark* para saber qué ya copió) y sube los Parquet a `s3://…/raw/origin=stream/`. Es idempotente: si el objeto ya existe con el mismo tamaño, lo omite. Solo sale el dato ya anonimizado.

### OIDC / `AssumeRoleWithWebIdentity`

**Qué es:** mecanismo de federación de identidad. En lugar de guardar llaves permanentes de AWS, el exportador presenta un token firmado por Google y AWS le entrega credenciales temporales.

**Rol en TrendAnalyzer:** permite que `sa-exporter` (GCP) asuma el rol `ta-gcp-exporter-role` (AWS) **sin llaves estáticas**. El plan B, si la federación no funciona a tiempo, es un usuario IAM con llaves en Secret Manager (menos seguro; se documentaría como deuda técnica).

---

## 6. Amazon Web Services (plano de lakehouse)

### Amazon S3

**Qué es:** almacenamiento de objetos de AWS, equivalente a Cloud Storage. Es la base de casi cualquier data lake.

**Rol en TrendAnalyzer:** es el **Lakehouse**. Un único bucket (`ta-{ENV}-{account_id}-lake`) organizado en capas:

| Carpeta | Contenido |
|---|---|
| `raw/` | Datos tal como llegan (ya anonimizados), de origen `stream` e `historical`. |
| `trusted/` | Datos unificados, deduplicados, con tipos correctos y reglas de calidad aplicadas. |
| `curated/` | Modelo analítico listo para consumo (hechos, dimensiones, agregados, aristas). |
| `quality/` | Reportes de Great Expectations (Lote 5.2) y, opcional, el informe del profiling (`quality/{fecha}/profiling/`, apagado por defecto; §3.20). |
| `athena-results/` | Resultados de consultas de Athena (se borran a los 7 días). |

Todo se guarda en **Parquet + Snappy**.

### AWS Glue ETL

**Qué es:** servicio de integración de datos administrado. Ejecuta jobs de transformación (ETL) con **Apache Spark** sin administrar un clúster.

**Rol en TrendAnalyzer:** mueve los datos entre capas, orquestado por el workflow `ta-wf-raw-to-curated`:

- `ta-job-raw-to-trusted`: une stream e histórico, deduplica por `event_id`, corrige tipos y limpia texto.
- `ta-job-trusted-to-curated`: construye el modelo analítico (con Glue o dbt).

Es idempotente: **sobrescribe las particiones `dt`** en lugar de añadir.

### Glue Data Catalog

**Qué es:** catálogo central de metadatos: qué tablas existen, sus columnas, tipos y ubicación de los datos.

**Rol en TrendAnalyzer:** es el **catálogo oficial** del proyecto (reemplaza a Dataplex). Define las bases `ta_raw`, `ta_trusted` y `ta_curated`, que Athena consulta. Es donde se documentan descripciones de tablas y columnas (entregable de gobernanza).

### Amazon Athena

**Qué es:** servicio para consultar archivos en S3 con **SQL estándar**, sin servidores ni cargas previas. Se paga por datos escaneados.

**Rol en TrendAnalyzer:** es la **capa de consulta** sobre el lakehouse. Usa un workgroup propio (`ta-analytics`) con límite de bytes escaneados. QuickSight se conecta a través de Athena.

### Partition projection

**Qué es:** una forma de decirle a Athena cómo se llaman y qué valores tienen las particiones, calculándolas en la consulta en lugar de registrarlas una a una (sin crawlers).

**Rol en TrendAnalyzer:** es más simple y barato que los crawlers (ADR-08). **Raw** se proyecta por origen, fuente, fecha y hora, con claves `part_origin`, `part_source`, `part_dt`, `part_hr` (el prefijo `part_` evita chocar con las columnas `origin` y `source`, que también viajan dentro del Parquet). **Trusted** se proyecta por `source` y `dt`, con `dt` igual a la fecha de `event_time` en UTC.

### Amazon EventBridge

**Qué es:** bus de eventos y programador de AWS. Reacciona a eventos o dispara acciones según un horario.

**Rol en TrendAnalyzer:** una regla `rate(1 hour)` dispara cada hora el workflow de Glue. Resuelve la "flecha huérfana" que tenía el diagrama anterior.

### Amazon QuickSight

**Qué es:** herramienta de inteligencia de negocios (BI) de AWS para crear dashboards interactivos.

**Rol en TrendAnalyzer:** es la **capa de visualización** para Marketing, Gerencia y Analistas. Sus datasets leen `ta_curated` vía Athena y usan **SPICE** para cumplir el KPI de respuesta < 3 segundos. Los dashboards se muestran en la zona horaria `America/Bogota`; el almacenamiento es siempre UTC.

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

**Rol en TrendAnalyzer:** produce el **dataset histórico** con el mismo generador que el simulador. Usa el mismo envelope, anonimiza con HMAC, corre el NLP en lote y sube Parquet a `s3://…/raw/origin=historical/…/dt=…/hr=…/`. Entra directo a AWS, sin pasar por Pub/Sub.

---

## 7. Procesamiento y NLP

### Apache Beam

**Qué es:** framework de código abierto para definir pipelines de datos en un modelo unificado (batch y streaming). Dataflow es uno de sus *runners*.

**Rol en TrendAnalyzer:** el pipeline de `src/streaming/` está escrito en Beam y se prueba localmente con `DirectRunner` antes de ejecutarlo en Dataflow.

### NLP preentrenado (`RunInference`) y RoBERTuito

**Qué es:** *Natural Language Processing* es la rama de la IA que interpreta texto. Un modelo **preentrenado** ya fue entrenado por otros y solo se aplica. `RunInference` es el transform de Beam para ejecutar un modelo dentro del pipeline. **RoBERTuito** es un modelo de lenguaje para español de redes sociales; la librería `pysentimiento` ofrece versiones ya entrenadas para sentimiento y emoción.

**Rol en TrendAnalyzer:** para cada texto calcula `sentiment_label` (`negative/neutral/positive`) y `emotion_label` (`joy, sadness, anger, fear, disgust, surprise, neutral`), con sus puntajes. Usa `robertuito-sentiment-analysis` y `robertuito-emotion-analysis` (ADR-05); el mapeo de las etiquetas del modelo a las del proyecto vive en `src/common/label_map.yaml`. Se registra la `nlp_model_version` (`robertuito-sentiment@<hash>+robertuito-emotion@<hash>`) en cada registro para trazabilidad. Su licencia es de uso académico y se cita en el informe. Es la pieza con más riesgo técnico (imagen pesada y memoria); el plan B es correr solo el modelo de sentimiento o mover el NLP a Cloud Run.

### Anonimización HMAC-SHA256

**Qué es:** HMAC es una función de hash con llave secreta. Convierte un dato en un código irreversible y consistente: el mismo usuario siempre produce el mismo hash, pero no se puede revertir sin la llave.

**Rol en TrendAnalyzer:** convierte `author.user_id` y `author.handle` en `user_hash`. Se hace en el **primer paso de cada plano** (Dataflow o loader), de modo que ningún dato personal cruza a AWS (ADR-06). Cumple el KPI de 100 % de identificadores anonimizados y mantiene el análisis de relaciones (el mismo usuario sigue siendo identificable como hash).

### Deduplicación

**Qué es:** eliminar registros repetidos.

**Rol en TrendAnalyzer:** se hace dos veces: en Dataflow (por `event_id`, usando el `id_label` de Pub/Sub) y de nuevo en Glue al construir Trusted. Necesaria porque Pub/Sub entrega *al menos una vez* y porque el caos del simulador inyecta duplicados. Desde v2.35 el job `ta-job-raw-to-trusted` (Lote 4.2) se queda con la copia de menor `process_time` (`ARCHITECTURE.md` §3.21 y §5.7).

### Dead-letter queue (DLQ)

**Qué es:** destino alterno para mensajes que no se pudieron procesar.

**Rol en TrendAnalyzer:** los mensajes malformados no se pierden ni rompen el pipeline: van al topic `ta-dlq`, a `gs://…/dlq/` y a la tabla `dlq_events`, con tipo y detalle del error.

### Normalización de engagement

**Qué es:** llevar métricas con nombres distintos en cada red a un vocabulario común.

**Rol en TrendAnalyzer:** `likes` = `score` (Reddit) | `reactions_total` (Facebook) | `like_count` (Instagram); `shares` = `shares_count` (Facebook) y `null` en Reddit e Instagram; `comments` = `num_comments` (Reddit) | `comments_count` (Facebook, Instagram).

### Ventanas deslizantes y `trend_windows`

**Qué es:** en streaming, una **ventana** agrupa eventos por tiempo. Una ventana *deslizante* se solapa: por ejemplo, 5 minutos de duración que avanza cada 1 minuto.

**Rol en TrendAnalyzer:** calcula por marca y fuente el volumen, la proporción positiva/negativa y el sentimiento promedio. Compara contra una línea base móvil mediante **z-scores**; cuando se desvía lo suficiente, marca `is_alert`. Sustenta el KPI de detección de tendencias en menos de 5 minutos.

### Parámetros operativos de las alertas

**Qué es:** reglas para evitar ruido en las alertas.

**Rol en TrendAnalyzer:** *allowed lateness* de 30 min (un evento más tardío se escribe pero no recalcula ventanas); **arranque en frío**: sin baseline no hay alertas (`is_alert = false` hasta tener 60 ventanas, 1 h); *cooldown* de 15 min por marca × fuente × tipo de alerta.

### Watermark

**Qué es:** en Dataflow, la noción de "hasta qué momento del tiempo creo que ya llegaron todos los datos". En el exportador, el término se usa para el marcador de lo último ya copiado.

**Rol en TrendAnalyzer:** permite manejar los eventos tardíos que genera el caos controlado. En `export-state/watermark.json` el exportador guarda su avance para no recopiar archivos.

### Parquet y esquema plano

**Qué es:** Parquet es un formato de archivo **columnar** y comprimido, muy eficiente para analítica.

**Rol en TrendAnalyzer:** es el formato de todo lo que vive en `enriched/` (GCS) y en S3. Los Parquet usan el **esquema plano** de BigQuery `posts_enriched` (`user_hash`, `followers`, `label_sentiment`, `brand_primary` aplanados y `payload` como JSON en texto), no el JSON anidado del envelope. Reduce costo y acelera Athena.

---

## 8. DataOps e infraestructura

### Terraform

**Qué es:** herramienta de **Infraestructura como Código (IaC)**: la infraestructura se describe en archivos y se crea o modifica con comandos.

**Rol en TrendAnalyzer:** aprovisiona todo en `infra/gcp/` e `infra/aws/` (un módulo por recurso lógico). Cada módulo expone `outputs` (nombres de topics, buckets, ARNs) que el código consume por variables de entorno en lugar de hardcodearlos. El estado es **remoto** desde el primer día (GCS con versionado y S3 con bloqueo, ADR-14). En desarrollo, Terraform se autentica en AWS con el perfil local de AWS CLI (`aws configure`), sin claves en el código.

### Bootstrap de Terraform

**Qué es:** la preparación inicial que debe existir antes del resto de la infraestructura.

**Rol en TrendAnalyzer:** el Lote 1.0 (HU-046) configura el proyecto GCP existente (región `us-east1`), habilita las APIs, crea los buckets de estado remoto, deja `terraform.tfvars.example` y un presupuesto mínimo. Como nada está aplicado todavía, es lo primero que se hace en la Fase 1.

### Entornos y regiones

**Qué es:** dónde vive cada cosa.

**Rol en TrendAnalyzer:** GCP en `us-east1` (cercana a AWS y a Colombia, ADR-12) y AWS en `us-east-1`. Hoy solo existe el entorno `dev`.

### GitHub y GitHub Actions

**Qué es:** GitHub aloja el repositorio. GitHub Actions ejecuta flujos automáticos (CI/CD) cuando hay un push o pull request.

**Rol en TrendAnalyzer:** versiona el código y los contratos; ejecuta tests, `terraform fmt/validate/plan`, y construye y publica las imágenes Docker. Los workflows viven en `.github/workflows/` (raíz) y usan `working-directory: project`. Un test en CI verifica que no existan columnas `handle`/`user_id` en los datos publicados.

### Docker y Artifact Registry

**Qué es:** Docker empaqueta una aplicación con todas sus dependencias en una *imagen*. Artifact Registry es donde Google guarda esas imágenes.

**Rol en TrendAnalyzer:** el simulador, el exportador y el contenedor de Dataflow (con el modelo NLP) se construyen como imágenes y se despliegan desde Artifact Registry.

### Great Expectations

**Qué es:** librería para definir y ejecutar pruebas de calidad sobre los datos ("este campo no puede ser nulo", "este id debe ser único").

**Rol en TrendAnalyzer:** valida Trusted y BigQuery contra los KPIs: completitud ≥ 95 %, unicidad ≥ 99 %, duplicados < 1 % y anonimización total. Escribe los resultados en `dq_results` y en `s3://…/quality/`.

### dbt

**Qué es:** herramienta para transformar datos con SQL de manera versionada, con tests y documentación automática.

**Rol en TrendAnalyzer:** construye el modelo Curated (`dbt-athena`) y sus pruebas. Sus *docs* generan el **linaje** de datos, que es el reemplazo de OpenLineage.

### Prueba de humo end-to-end (`smoke_e2e.py`)

**Qué es:** una prueba rápida que sigue un solo evento de punta a punta.

**Rol en TrendAnalyzer:** emite un evento **centinela** (con `event_id` conocido) y lo sigue desde el simulador hasta `ta_curated.fact_posts`, reportando la latencia por etapa. Es evidencia para el informe (HU-048).

### Prefijo `ta-` y etiquetas

**Qué es:** convención de nombres.

**Rol en TrendAnalyzer:** todo recurso lleva el prefijo `ta-` y las etiquetas `project=trend-analyzer`, `env=dev`, `owner=alejandro`, lo que facilita rastrear costos y responsables.

### `terraform.tfvars` y secretos

**Qué es:** el archivo de valores de Terraform, que puede contener datos sensibles.

**Rol en TrendAnalyzer:** está en `.gitignore`; se versiona `terraform.tfvars.example`. Nunca hay secretos en el repositorio (`gitleaks` lo vigila) y los IDs de cuenta y proyecto van en configuración, no en el código.

---

## 9. Calidad y gobernanza

### Profiling

**Qué es:** análisis exploratorio de un conjunto de datos para conocer su estructura, nulos, distribuciones y anomalías.

**Rol en TrendAnalyzer:** se aplica sobre Raw y Trusted para entender el estado real de los datos antes de definir reglas de calidad (HU-031). Incluye reportar la **diversidad léxica**, dado que el generador usa plantillas. Desde v2.33 lo hace `scripts/profile_data.py` (Lote 5.1); qué mide, cómo se usa y lo que encontró en Raw están en `ARCHITECTURE.md` §3.20, y las reglas que propone, en §5.10.

**Términos del informe:** *TTR* (tipos / tokens), *MATTR* (TTR medio en ventanas deslizantes de 500 tokens; no depende del tamaño del texto), *hapax* (palabras que aparecen una sola vez), *distinct-2* (bigramas distintos / bigramas) y *esqueleto* (el texto en minúsculas, sin tildes, hashtags ni signos y con la marca enmascarada, para medir cuánto se repiten las plantillas). Definiciones exactas en `ARCHITECTURE.md` §3.20.

### Calidad de datos y KPIs

**Qué es:** medición de si los datos son completos, únicos, consistentes y oportunos.

**Rol en TrendAnalyzer:** cada KPI del proyecto tiene una regla medible (completitud, unicidad, duplicados, anonimización, latencia, dashboard < 3 s). Además se mide la **precisión del NLP** comparando `sentiment_label` contra `label_sentiment` (extra; se excluyen los eventos con `null`).

### Catálogo y linaje

**Qué es:** el *catálogo* describe qué datos existen y qué significan. El *linaje* muestra de dónde vienen y cómo se transforman.

**Rol en TrendAnalyzer:** el catálogo vive en el Glue Data Catalog; el linaje se documenta con dbt docs y un diagrama.

### DAMA-DMBOK

**Qué es:** guía de referencia internacional de buenas prácticas en gestión de datos.

**Rol en TrendAnalyzer:** el marco con el que se estructura y documenta la gobernanza del proyecto.

### Ley 1581 (Colombia)

**Qué es:** ley colombiana de protección de datos personales (Habeas Data).

**Rol en TrendAnalyzer:** aunque los datos son sintéticos, se documenta el tratamiento como si fueran reales (anonimización, acceso restringido). Es el motivo por el que `raw/` en GCS tiene acceso restringido.

### Retención

**Qué es:** cuánto tiempo se conserva cada dato.

**Rol en TrendAnalyzer:** GCS `raw/` y `dlq/` 30 días; BigQuery `posts_enriched` 90 días (el histórico completo vive en S3); S3 `athena-results/` 7 días; Pub/Sub 7 días.

---

## 10. Conceptos transversales

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

- **Raw:** tal cual llega (ya anonimizado).
- **Trusted:** limpio, deduplicado y validado; une stream e histórico y añade `text_clean` (el texto en minúsculas, sin emojis, URLs, menciones ni hashtags: regla `text-clean@1` de `ARCHITECTURE.md` §5.7). Lo escribe el job `ta-job-raw-to-trusted` desde v2.35 (§3.21).
- **Curated:** modelado para análisis.

### Modelo estrella (`fact_posts`, `dim_*`)

**Qué es:** diseño analítico con una tabla central de hechos rodeada de tablas de dimensiones.

**Rol en TrendAnalyzer:** `fact_posts` guarda un registro por post con sus métricas; `dim_date`, `dim_source`, `dim_brand` y `dim_user` describen el contexto (`dim_user` solo con `user_hash`). Se complementa con agregados (`agg_emotion_by_brand_hour`, `agg_trend_daily`).

### Tablas de aristas (`edges_*`)

**Qué es:** representación de relaciones como pares origen-destino con un peso (la base de un grafo).

**Rol en TrendAnalyzer:** `edges_user_brand` y `edges_hashtag_cooccurrence` cubren el "análisis de relaciones y propagación" sin necesidad de una base de grafos.

### Idempotencia

**Qué es:** propiedad de una operación que, aplicada varias veces, da el mismo resultado que aplicada una.

**Rol en TrendAnalyzer:** el `event_id` determinista, el exportador que omite objetos ya copiados y Glue que sobrescribe particiones permiten reintentos seguros sin duplicar datos.

### Contrato

**Qué es:** el acuerdo explícito sobre la forma en que dos componentes se hablan (formato, nombres, campos obligatorios).

**Rol en TrendAnalyzer:** es la idea que organiza el proyecto: cada interfaz está escrita en `ARCHITECTURE.md` §5, tiene su copia ejecutable en `project/contracts/` y se valida en CI. Quien construye un componente solo necesita el contrato del vecino, no su código.

### JSON Schema

**Qué es:** un estándar para describir y validar la forma de un documento JSON (campos, tipos, valores permitidos).

**Rol en TrendAnalyzer:** los `*.schema.json` de `contracts/` (borrador 2020-12) definen qué es un evento válido; por ejemplo, el schema enriquecido prohíbe `handle` y `user_id`, con lo que la anonimización queda verificable.

---

## 11. Fuera del alcance (mencionados pero no usados en v2)

| Componente | Qué es | Por qué quedó fuera |
|---|---|---|
| **Mastodon / RSS** | Red social descentralizada y formato de titulares | Se reemplazaron por Facebook e Instagram simulados (v2.2). |
| **Datasets de emociones** (TEC, GoEmotions, SemEval-2007, DailyDialog, ISEAR) | Colecciones públicas de textos etiquetados con emoción | Eran en inglés, no hablaban de las marcas y obligaban a inyectarlas; los reemplaza el generador (v2.2). |
| **Modelo en inglés** (`emotion-english-distilroberta-base`) | Modelo de emoción para inglés | Sustituido por RoBERTuito en español (ADR-05). |
| **CocaCola y Colombiana** | Marcas que trae el script del generador | Fuera de v1; se pueden añadir después en `brands.yaml`. |
| **Firestore** | Base de datos documental de GCP | El estado del simulador vive en un objeto de GCS. |
| **Cloud SQL** | Base relacional administrada (PostgreSQL) | No hay datos transaccionales en v1. |
| **Amazon Neptune** | Base de datos de grafos de AWS | Se cubre con tablas de aristas consultables en Athena; queda como extensión. |
| **Dataplex / OpenLineage** | Gobernanza y linaje de GCP / estándar abierto de linaje | Se usa Glue Data Catalog y dbt docs. |
| **`src/collectors/`** | Conectores REST reales de Mastodon, Reddit y RSS | Quedan como código legado/opcional; no se despliegan. |

---

## 12. Tabla resumen: componente → rol

| Componente | Nube / ámbito | Rol en una frase |
|---|---|---|
| `ARCHITECTURE.md` | Repositorio | Fuente de verdad: contratos, plan, decisiones y estado. |
| `GLOSSARY.md` | Repositorio | Vocabulario común del proyecto. |
| `HU.md` | Repositorio | Historias de usuario y criterios de aceptación (qué es "terminado"). |
| `contracts/` | Repositorio | Contratos ejecutables: schemas, DDL, ejemplos y pruebas. |
| Lote / Fase | Plan | Unidad de entrega / agrupación hacia los hitos Sprint 2 y Final. |
| ADR | Plan | Decisión de arquitectura con su alternativa descartada. |
| Generador de comentarios | GCP (en el simulador) | Produce el texto y la marca de cada comentario en español. |
| `stream-simulator` | GCP (Cloud Run) | Reproduce Reddit, Facebook e Instagram como si estuvieran en vivo. |
| Cloud Storage | GCP | Guarda el estado del simulador y el lake de GCP (raw, enriched, dlq). |
| Cloud Run / Run Job | GCP | Ejecuta el simulador (siempre activo) y el exportador (por lotes). |
| Cloud Scheduler | GCP | Dispara el exportador cada 15 minutos. |
| Pub/Sub | GCP | Mensajería que desacopla el simulador de Dataflow. |
| Dataflow | GCP | Valida, deduplica, anonimiza, aplica NLP y calcula tendencias en tiempo casi real. |
| BigQuery | GCP | Analítica caliente: posts enriquecidos y ventanas de tendencia. |
| Secret Manager | GCP | Custodia la llave HMAC de anonimización. |
| Monitoring + Logging | GCP | Observabilidad del plano caliente y alertas por correo. |
| `gcs-to-s3` + OIDC | GCP → AWS | Único puente entre nubes; copia solo datos anonimizados. |
| Loader histórico | AWS | Carga masiva única del histórico, generado con el mismo generador. |
| Amazon S3 | AWS | Lakehouse por capas Raw → Trusted → Curated. |
| AWS Glue ETL | AWS | Transforma entre capas con Spark. |
| Glue Data Catalog | AWS | Catálogo oficial de tablas y metadatos. |
| Athena (+ partition projection) | AWS | Consultas SQL sobre S3 sin crawlers. |
| EventBridge | AWS | Dispara el workflow de Glue cada hora. |
| QuickSight (+ SPICE) | AWS | Dashboards para Marketing, Gerencia y Analistas. |
| CloudWatch | AWS | Observabilidad del plano AWS. |
| IAM + KMS | AWS | Permisos mínimos y cifrado. |
| Terraform | DataOps | Crea toda la infraestructura como código, con estado remoto. |
| GitHub + Actions | DataOps | Versionado y CI/CD. |
| Docker + Artifact Registry | DataOps | Empaqueta y distribuye los contenedores. |
| Great Expectations | DataOps | Pruebas de calidad contra los KPIs. |
| dbt | DataOps | Modelo Curated, tests y linaje. |
| Modelos RoBERTuito (pysentimiento) | GCP (Dataflow) | Clasifican sentimiento y emoción de cada texto en español. |
| HMAC-SHA256 | GCP / AWS | Anonimiza usuarios antes de que el dato salga de GCP. |
