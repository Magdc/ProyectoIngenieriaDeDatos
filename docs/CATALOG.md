# TrendAnalyzer · Catálogo de datos (diccionario)

> **Generado** desde `project/contracts/athena/*.sql` con `cd project/src && python -m lakehouse.catalog_spec --write`; no se edita a mano (una prueba exige que coincida con las DDL). Es lo mismo que el Glue Data Catalog muestra en `ta_raw`, `ta_trusted` y `ta_curated` (descripción de cada tabla y `Comment` de cada columna). Reglas, linaje y gobernanza: `ARCHITECTURE.md` §3.25, `LINEAGE.md` y `GOVERNANCE.md`. Datos sintéticos.

**11 tablas y 108 columnas** (con las claves de partición). Clases de privacidad: `anonimizado` 5, `cuasi_identificador` 8, `texto_libre` 3, `sin_dato_personal` 92.

| Clase | Qué significa | ¿Dato anonimizado? |
|---|---|---|
| `anonimizado` | El identificador del autor, reemplazado por un seudónimo HMAC-SHA256 (`user_hash`): la única columna que identifica a una persona; sin la llave `ta-hmac-key` no se revierte | **Sí** |
| `cuasi_identificador` | No identifica por sí sola, pero cruzada con otras fuentes ayudaría a reidentificar al autor (seguidores, actividad, ids y enlaces del post en `payload`) | No |
| `texto_libre` | Texto escrito por el autor: en datos reales podría traer nombres, menciones o datos sensibles | No |
| `sin_dato_personal` | Describe el comentario, la marca, el tiempo, la red o el modelo, no a la persona | No |

| Tabla | Columnas | Particiones | Anonimizadas | Ubicación |
|---|---|---|---|---|
| [`ta_raw.posts_raw`](#ta_rawposts_raw) | 24 | `part_origin`, `part_source`, `part_dt`, `part_hr` | `user_hash` | `s3://{lake}/raw/` |
| [`ta_trusted.posts`](#ta_trustedposts) | 24 | `source`, `dt` | `user_hash` | `s3://{lake}/trusted/posts/` |
| [`ta_curated.fact_posts`](#ta_curatedfact_posts) | 16 | `dt` | `user_hash` | `s3://{lake}/curated/fact_posts/` |
| [`ta_curated.dim_date`](#ta_curateddim_date) | 8 | — | — | `s3://{lake}/curated/dim_date/` |
| [`ta_curated.dim_source`](#ta_curateddim_source) | 3 | — | — | `s3://{lake}/curated/dim_source/` |
| [`ta_curated.dim_brand`](#ta_curateddim_brand) | 3 | — | — | `s3://{lake}/curated/dim_brand/` |
| [`ta_curated.dim_user`](#ta_curateddim_user) | 5 | — | `user_hash` | `s3://{lake}/curated/dim_user/` |
| [`ta_curated.agg_emotion_by_brand_hour`](#ta_curatedagg_emotion_by_brand_hour) | 5 | — | — | `s3://{lake}/curated/agg_emotion_by_brand_hour/` |
| [`ta_curated.agg_trend_daily`](#ta_curatedagg_trend_daily) | 7 | — | — | `s3://{lake}/curated/agg_trend_daily/` |
| [`ta_curated.edges_user_brand`](#ta_curatededges_user_brand) | 3 | — | `user_hash` | `s3://{lake}/curated/edges_user_brand/` |
| [`ta_curated.edges_hashtag_cooccurrence`](#ta_curatededges_hashtag_cooccurrence) | 3 | — | — | `s3://{lake}/curated/edges_hashtag_cooccurrence/` |

## ta_raw.posts_raw

Capa Raw (bronce): una fila por comentario simulado de redes sociales, tal como llega a S3 desde GCP (stream, exportador gcs-to-s3) y desde la carga histórica (loader). Ya viene anonimizada: el autor es user_hash, nunca user_id ni handle. Puede tener event_id repetidos, filas sin NLP y los centinelas ta-verify@* de los verificadores: Trusted (ta_trusted.posts) los resuelve. Particiones: origen, red social, día y hora (en el stream, de proceso; en el histórico, de event_time). Datos sintéticos.

Ubicación `s3://{lake}/raw/` · Parquet + Snappy · particiones `part_origin`, `part_source`, `part_dt`, `part_hr` (partition projection) · DDL `contracts/athena/posts_raw.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `event_id` | `string` | no | `sin_dato_personal` | Identificador del evento (UUID del simulador); clave de deduplicación |
| 2 | `origin` | `string` | no | `sin_dato_personal` | Origen del dato: stream (simulador en vivo) \| historical (carga histórica) |
| 3 | `source` | `string` | no | `sin_dato_personal` | Red social simulada: reddit \| facebook \| instagram |
| 4 | `generator` | `string` | no | `sin_dato_personal` | Nombre y versión del generador de comentarios (ta-verify@* = centinela de validación) |
| 5 | `event_time` | `timestamp` | no | `sin_dato_personal` | Instante en que se publicó el comentario (UTC) |
| 6 | `ingest_time` | `timestamp` | no | `sin_dato_personal` | Instante en que el simulador o el loader lo emitió (UTC) |
| 7 | `process_time` | `timestamp` | no | `sin_dato_personal` | Instante en que Dataflow (o el loader) lo enriqueció (UTC) |
| 8 | `user_hash` | `string` | **sí** | `anonimizado` | Autor seudonimizado: HMAC-SHA256 en hex (64) de user_id con la llave ta-hmac-key; mismo usuario, mismo hash |
| 9 | `followers` | `bigint` | no | `cuasi_identificador` | Seguidores del autor al publicar (puede ser nulo) |
| 10 | `text` | `string` | no | `texto_libre` | Texto del comentario tal como se publicó (sintético: plantillas, emojis y hashtags) |
| 11 | `lang` | `string` | no | `sin_dato_personal` | Idioma del texto (es) |
| 12 | `brands` | `array<string>` | no | `sin_dato_personal` | Marcas mencionadas, claves de brands.yaml (postobon, pepsi, sprite); puede ser vacío |
| 13 | `brand_primary` | `string` | no | `sin_dato_personal` | Marca principal: la primera de brands (nula si no hay) |
| 14 | `hashtags` | `array<string>` | no | `sin_dato_personal` | Hashtags del texto en forma canónica (minúsculas, sin #, [a-z0-9_]), en orden de aparición |
| 15 | `likes` | `bigint` | no | `sin_dato_personal` | Me gusta, normalizados desde payload según la red (§5.2) |
| 16 | `shares` | `bigint` | no | `sin_dato_personal` | Veces compartido, normalizado desde payload (solo Facebook lo tiene) |
| 17 | `comments` | `bigint` | no | `sin_dato_personal` | Respuestas o comentarios recibidos, normalizado desde payload |
| 18 | `sentiment_label` | `string` | no | `sin_dato_personal` | Sentimiento predicho por RoBERTuito: negative \| neutral \| positive (nulo si no hubo NLP) |
| 19 | `sentiment_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en sentiment_label, en [0, 1] |
| 20 | `emotion_label` | `string` | no | `sin_dato_personal` | Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP) |
| 21 | `emotion_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en emotion_label, en [0, 1] |
| 22 | `label_sentiment` | `string` | no | `sin_dato_personal` | Sentimiento de verdad del simulador (ground truth; nulo en categorías ambiguas). Solo para evaluar el NLP |
| 23 | `nlp_model_version` | `string` | no | `sin_dato_personal` | Modelos de NLP y commit con que se predijo (nulo sin NLP) |
| 24 | `payload` | `string` | no | `cuasi_identificador` | JSON con los campos propios de la red (ids del post, enlace, página, reacciones); sin identidad del autor |
| 25 | `part_origin` (partición) | `string` | no | `sin_dato_personal` | Partición de S3 origin=: stream \| historical |
| 26 | `part_source` (partición) | `string` | no | `sin_dato_personal` | Partición de S3 source=: reddit \| facebook \| instagram |
| 27 | `part_dt` (partición) | `string` | no | `sin_dato_personal` | Partición de S3 dt=: día UTC (AAAA-MM-DD); en el stream, el de proceso; en el histórico, el de event_time |
| 28 | `part_hr` (partición) | `string` | no | `sin_dato_personal` | Partición de S3 hr=: hora UTC (00-23), con el mismo criterio que part_dt |

## ta_trusted.posts

Capa Trusted (plata): una fila por comentario (event_id único), unión de stream e histórico. La reconstruye entera cada hora ta-job-raw-to-trusted desde Raw: deduplica por event_id (se queda con el menor process_time), descarta los centinelas ta-verify@* y las filas sin event_id o event_time, corrige tipos y añade text_clean. Las filas sin NLP se conservan con el NLP en nulo. Particiones: red social y día UTC de event_time. Fuente de Curated. Datos sintéticos, ya anonimizados.

Ubicación `s3://{lake}/trusted/posts/` · Parquet + Snappy · particiones `source`, `dt` (partition projection) · DDL `contracts/athena/posts.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `event_id` | `string` | no | `sin_dato_personal` | Identificador del evento (UUID del simulador); único en esta tabla |
| 2 | `origin` | `string` | no | `sin_dato_personal` | Origen del dato: stream (simulador en vivo) \| historical (carga histórica) |
| 3 | `generator` | `string` | no | `sin_dato_personal` | Nombre y versión del generador de comentarios |
| 4 | `event_time` | `timestamp` | no | `sin_dato_personal` | Instante en que se publicó el comentario (UTC); nunca nulo |
| 5 | `ingest_time` | `timestamp` | no | `sin_dato_personal` | Instante en que el simulador o el loader lo emitió (UTC) |
| 6 | `process_time` | `timestamp` | no | `sin_dato_personal` | Instante en que Dataflow (o el loader) lo enriqueció (UTC) |
| 7 | `user_hash` | `string` | **sí** | `anonimizado` | Autor seudonimizado: HMAC-SHA256 en hex (64) de user_id con la llave ta-hmac-key; mismo usuario, mismo hash |
| 8 | `followers` | `bigint` | no | `cuasi_identificador` | Seguidores del autor al publicar (puede ser nulo) |
| 9 | `text` | `string` | no | `texto_libre` | Texto original del comentario, sin cambios (sintético) |
| 10 | `text_clean` | `string` | no | `texto_libre` | Texto normalizado con la regla text-clean@1: sin emojis, URLs, menciones ni hashtags, en minúsculas |
| 11 | `lang` | `string` | no | `sin_dato_personal` | Idioma del texto (es) |
| 12 | `brands` | `array<string>` | no | `sin_dato_personal` | Marcas mencionadas, claves de brands.yaml (postobon, pepsi, sprite); puede ser vacío |
| 13 | `brand_primary` | `string` | no | `sin_dato_personal` | Marca principal: la primera de brands (nula si no hay) |
| 14 | `hashtags` | `array<string>` | no | `sin_dato_personal` | Hashtags del texto en forma canónica (minúsculas, sin #, [a-z0-9_]), en orden de aparición |
| 15 | `likes` | `bigint` | no | `sin_dato_personal` | Me gusta, normalizados desde payload según la red (§5.2) |
| 16 | `shares` | `bigint` | no | `sin_dato_personal` | Veces compartido, normalizado desde payload (solo Facebook lo tiene) |
| 17 | `comments` | `bigint` | no | `sin_dato_personal` | Respuestas o comentarios recibidos, normalizado desde payload |
| 18 | `sentiment_label` | `string` | no | `sin_dato_personal` | Sentimiento predicho por RoBERTuito: negative \| neutral \| positive (nulo si la fila no tiene NLP) |
| 19 | `sentiment_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en sentiment_label, en [0, 1] |
| 20 | `emotion_label` | `string` | no | `sin_dato_personal` | Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP) |
| 21 | `emotion_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en emotion_label, en [0, 1] |
| 22 | `label_sentiment` | `string` | no | `sin_dato_personal` | Sentimiento de verdad del simulador (ground truth; nulo en categorías ambiguas). Solo para evaluar el NLP |
| 23 | `nlp_model_version` | `string` | no | `sin_dato_personal` | Modelos de NLP y commit con que se predijo (nulo sin NLP) |
| 24 | `payload` | `string` | no | `cuasi_identificador` | JSON con los campos propios de la red (ids del post, enlace, página, reacciones); sin identidad del autor |
| 25 | `source` (partición) | `string` | no | `sin_dato_personal` | Red social simulada (partición source=): reddit \| facebook \| instagram |
| 26 | `dt` (partición) | `string` | no | `sin_dato_personal` | Día UTC de event_time (partición dt=, AAAA-MM-DD) |

## ta_curated.fact_posts

Hechos: un registro por comentario de Trusted (grano = event_id), con sus llaves a las dimensiones, el engagement y el NLP (nulo en las filas sin NLP). Partición dt = día UTC de event_time.

Ubicación `s3://{lake}/curated/fact_posts/` · Parquet + Snappy · particiones `dt` (partition projection) · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `event_id` | `string` | no | `sin_dato_personal` | Identificador del comentario (UUID del simulador); grano de la tabla |
| 2 | `date_key` | `int` | no | `sin_dato_personal` | Día UTC de event_time como entero yyyymmdd; llave a dim_date.date_key |
| 3 | `origin` | `string` | no | `sin_dato_personal` | Origen del dato: stream \| historical |
| 4 | `source` | `string` | no | `sin_dato_personal` | Red social simulada; llave a dim_source.source |
| 5 | `brand_primary` | `string` | no | `sin_dato_personal` | Marca principal (la primera de brands); llave a dim_brand.brand |
| 6 | `brands` | `array<string>` | no | `sin_dato_personal` | Todas las marcas mencionadas (claves de brands.yaml) |
| 7 | `user_hash` | `string` | **sí** | `anonimizado` | Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave a dim_user.user_hash |
| 8 | `event_time` | `timestamp` | no | `sin_dato_personal` | Instante en que se publicó el comentario (UTC) |
| 9 | `likes` | `bigint` | no | `sin_dato_personal` | Me gusta, normalizados según la red |
| 10 | `shares` | `bigint` | no | `sin_dato_personal` | Veces compartido (solo Facebook) |
| 11 | `comments` | `bigint` | no | `sin_dato_personal` | Respuestas o comentarios recibidos |
| 12 | `sentiment_label` | `string` | no | `sin_dato_personal` | Sentimiento predicho: negative \| neutral \| positive (nulo sin NLP) |
| 13 | `sentiment_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en sentiment_label, en [0, 1] |
| 14 | `emotion_label` | `string` | no | `sin_dato_personal` | Emoción predicha: joy, sadness, anger, fear, disgust, surprise, neutral (nula sin NLP) |
| 15 | `emotion_score` | `double` | no | `sin_dato_personal` | Confianza del modelo en emotion_label, en [0, 1] |
| 16 | `label_sentiment` | `string` | no | `sin_dato_personal` | Sentimiento de verdad del simulador (ground truth); solo para medir la precisión del NLP |
| 17 | `dt` (partición) | `string` | no | `sin_dato_personal` | Día UTC de event_time (partición dt=, AAAA-MM-DD) |

## ta_curated.dim_date

Dimensión calendario: un día por fila, continuo desde el primer hasta el último dt de fact_posts.

Ubicación `s3://{lake}/curated/dim_date/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `date_key` | `int` | no | `sin_dato_personal` | Día como entero yyyymmdd (llave) |
| 2 | `full_date` | `date` | no | `sin_dato_personal` | Fecha del día (UTC) |
| 3 | `year` | `int` | no | `sin_dato_personal` | Año |
| 4 | `month` | `int` | no | `sin_dato_personal` | Mes (1-12) |
| 5 | `day` | `int` | no | `sin_dato_personal` | Día del mes (1-31) |
| 6 | `iso_week` | `int` | no | `sin_dato_personal` | Semana ISO 8601 (1-53) |
| 7 | `day_of_week` | `int` | no | `sin_dato_personal` | Día de la semana: 1 = lunes ... 7 = domingo |
| 8 | `is_weekend` | `boolean` | no | `sin_dato_personal` | Verdadero en sábado y domingo |

## ta_curated.dim_source

Dimensión red social: las tres fuentes simuladas con su nombre visible y su mezcla de publicaciones y comentarios.

Ubicación `s3://{lake}/curated/dim_source/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `source` | `string` | no | `sin_dato_personal` | Red social simulada (llave): reddit \| facebook \| instagram |
| 2 | `display_name` | `string` | no | `sin_dato_personal` | Nombre para mostrar en los tableros |
| 3 | `kind_mix` | `string` | no | `sin_dato_personal` | Mezcla de kind de la fuente (§3.3), p. ej. post 30% · comment 70% |

## ta_curated.dim_brand

Dimensión marca: las marcas de brands.yaml (propia y competidores), más cualquier marca de los datos que no esté (con display y role nulos).

Ubicación `s3://{lake}/curated/dim_brand/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `brand` | `string` | no | `sin_dato_personal` | Clave canónica de brands.yaml (postobon, pepsi, sprite); llave |
| 2 | `display` | `string` | no | `sin_dato_personal` | Nombre para mostrar de la marca |
| 3 | `role` | `string` | no | `sin_dato_personal` | Papel de la marca para el análisis: propia \| competidor |

## ta_curated.dim_user

Dimensión autor seudonimizado: una fila por user_hash con su actividad. Solo user_hash: nunca handle ni user_id.

Ubicación `s3://{lake}/curated/dim_user/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `user_hash` | `string` | **sí** | `anonimizado` | Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave |
| 2 | `first_seen` | `timestamp` | no | `cuasi_identificador` | Primer comentario del autor (mínimo event_time, UTC) |
| 3 | `last_seen` | `timestamp` | no | `cuasi_identificador` | Último comentario del autor (máximo event_time, UTC) |
| 4 | `n_posts` | `bigint` | no | `cuasi_identificador` | Comentarios del autor en fact_posts |
| 5 | `max_followers` | `bigint` | no | `cuasi_identificador` | Máximo de seguidores observado para el autor |

## ta_curated.agg_emotion_by_brand_hour

Conteos de emoción por hora UTC, marca principal, red social y emoción (formato largo). Solo cuenta las filas con emoción predicha.

Ubicación `s3://{lake}/curated/agg_emotion_by_brand_hour/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `hour_start` | `timestamp` | no | `sin_dato_personal` | Inicio de la hora (UTC, truncado a la hora) |
| 2 | `brand` | `string` | no | `sin_dato_personal` | Marca principal (brand_primary) |
| 3 | `source` | `string` | no | `sin_dato_personal` | Red social simulada |
| 4 | `emotion_label` | `string` | no | `sin_dato_personal` | Emoción predicha |
| 5 | `n_posts` | `bigint` | no | `sin_dato_personal` | Comentarios de esa hora, marca, red y emoción |

## ta_curated.agg_trend_daily

Volumen y sentimiento diario por marca principal. n_posts cuenta todas las filas; los conteos por sentimiento y avg_sentiment, solo las que tienen sentimiento. El día es UTC; los tableros convierten a America/Bogota (§5.12).

Ubicación `s3://{lake}/curated/agg_trend_daily/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `full_date` | `date` | no | `sin_dato_personal` | Día UTC |
| 2 | `brand` | `string` | no | `sin_dato_personal` | Marca principal (brand_primary) |
| 3 | `n_posts` | `bigint` | no | `sin_dato_personal` | Comentarios del día y la marca (también los que no tienen NLP) |
| 4 | `n_negative` | `bigint` | no | `sin_dato_personal` | Comentarios con sentimiento negative |
| 5 | `n_neutral` | `bigint` | no | `sin_dato_personal` | Comentarios con sentimiento neutral |
| 6 | `n_positive` | `bigint` | no | `sin_dato_personal` | Comentarios con sentimiento positive |
| 7 | `avg_sentiment` | `double` | no | `sin_dato_personal` | Media de +score (positive), -score (negative) y 0 (neutral), en [-1, 1]; como trend_windows |

## ta_curated.edges_user_brand

Arista usuario -> marca para análisis de relaciones: una fila por par (user_hash, marca); weight = número de menciones (filas del usuario cuyo brands contiene la marca).

Ubicación `s3://{lake}/curated/edges_user_brand/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `user_hash` | `string` | **sí** | `anonimizado` | Autor seudonimizado (HMAC-SHA256 en hex de user_id); llave a dim_user |
| 2 | `brand` | `string` | no | `sin_dato_personal` | Marca mencionada; llave a dim_brand |
| 3 | `weight` | `bigint` | no | `sin_dato_personal` | Comentarios del autor que mencionan la marca (cada marca una vez por comentario) |

## ta_curated.edges_hashtag_cooccurrence

Coocurrencia de hashtags: arista no dirigida, siempre hashtag_a < hashtag_b; weight = comentarios con los dos.

Ubicación `s3://{lake}/curated/edges_hashtag_cooccurrence/` · Parquet + Snappy · sin particiones · DDL `contracts/athena/curated.sql`

| # | Columna | Tipo | Anonimizado | Clase | Descripción |
|---|---|---|---|---|---|
| 1 | `hashtag_a` | `string` | no | `sin_dato_personal` | Primer hashtag del par (forma canónica; el menor por punto de código) |
| 2 | `hashtag_b` | `string` | no | `sin_dato_personal` | Segundo hashtag del par (siempre mayor que hashtag_a) |
| 3 | `weight` | `bigint` | no | `sin_dato_personal` | Comentarios que contienen los dos hashtags |
