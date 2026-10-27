# Profiling de Trusted · `ta_trusted.posts` (Lote 5.1, HU-031)

> Generado por `ta-profiler@1.0` el 2026-10-26T22:54:28Z (referencia `as_of` 2026-10-26T22:45:34Z) desde `s3://ta-dev-648617469176-lake/trusted/posts/ (caché local)` en 534.0 s. Solo agregados: sin `user_hash`, `event_id` ni textos completos. Detalle en el JSON del mismo nombre; definiciones en `docs/ARCHITECTURE.md` §3.20.

## 1. Resumen

| Grupo (origen/fuente) | Filas | Archivos | MB |
|---|---:|---:|---:|
| */* | 221 629 | 543 | 67.4 |
| */facebook | 73 890 | 181 | 23.1 |
| */instagram | 73 946 | 181 | 21.9 |
| */reddit | 73 793 | 181 | 22.4 |
| historical/* | 50 000 | 0 | 0.0 |
| historical/facebook | 16 667 | 0 | 0.0 |
| historical/instagram | 16 666 | 0 | 0.0 |
| historical/reddit | 16 667 | 0 | 0.0 |
| stream/* | 171 629 | 0 | 0.0 |
| stream/facebook | 57 223 | 0 | 0.0 |
| stream/instagram | 57 280 | 0 | 0.0 |
| stream/reddit | 57 126 | 0 | 0.0 |

Anomalías: 0 error, 1 aviso, 14 informativas. Reglas propuestas evaluadas: 54 cumple.

## 2. KPI preliminares (§5.10)

| Origen | Completitud | Unicidad | Duplicados | Anonimización |
|---|---:|---:|---:|---:|
| * | 100.00 % | 100.000 % | 0.000 % | 100.00 % |
| historical | 100.00 % | 100.000 % | 0.000 % | 100.00 % |
| stream | 100.00 % | 100.000 % | 0.000 % | 100.00 % |

## 3. Columnas (nulos, cardinalidad, tipo y distribución)

### trusted/historical (50 000 filas)

| Columna | Tipo | Tipo físico | Nulos | Distintos | Resumen |
|---|---|---|---:|---:|---|
| `event_id` | string | string | 0.00 % | 50 000 | largo 36–36 (p50 36) · máx. repetición 1 |
| `origin` | string | string | 0.00 % | 1 | historical 100.0 % |
| `generator` | string | string | 0.00 % | 1 | comment-gen@1.0 100.0 % |
| `event_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 50 000 | 2026-04-27T00:01:32.345Z → 2026-10-23T23:52:30.630Z (179.99 días) |
| `ingest_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 50 000 | 2026-04-27T00:01:34.350Z → 2026-10-23T23:52:33.942Z (179.99 días) |
| `process_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 1 | 2026-10-24T14:57:48.401Z → 2026-10-24T14:57:48.401Z (0.00 días) |
| `user_hash` | string | string | 0.00 % | 4 547 | largo 64–64 (p50 64) · máx. repetición 2858 |
| `followers` | int | int64 | 0.00 % | 1 145 | min 1.00 · p50 136.00 · p95 1 660.00 · máx 19 446.00 |
| `text` | string | string | 0.00 % | 49 998 | largo 26–268 (p50 110) · máx. repetición 2 |
| `text_clean` | string | string | 0.00 % | 49 951 | largo 25–259 (p50 104) · máx. repetición 4 |
| `lang` | string | string | 0.00 % | 1 | es 100.0 % |
| `brands` | list | list<element: string> | 0.00 % | 9 | largo 0/1/2/3+: 0/47338/2662/0 · postobon 21981, pepsi 17658, sprite 13023 |
| `brand_primary` | string | string | 0.00 % | 3 | postobon 42.4 %, pepsi 33.6 %, sprite 24.1 % |
| `hashtags` | list | list<element: string> | 0.00 % | 617 | largo 0/1/2/3+: 29910/12557/6240/1293 · colombia 5231, gaseosa 5081, bebidas 3843, refresco 3763 |
| `likes` | int | int64 | 0.00 % | 442 | min -233.00 · p50 9.00 · p95 64.00 · máx 3 710.00 |
| `shares` | int | int64 | 66.67 % | 69 | min 0.00 · p50 0.00 · p95 8.00 · máx 127.00 |
| `comments` | int | int64 | 23.36 % | 149 | min 0.00 · p50 1.00 · p95 19.00 · máx 390.00 |
| `sentiment_label` | string | string | 0.00 % | 3 | negative 35.2 %, positive 32.7 %, neutral 32.0 % |
| `sentiment_score` | float | double | 0.00 % | 49 606 | min 0.34 · p50 0.87 · p95 0.98 · máx 0.99 |
| `emotion_label` | string | string | 0.00 % | 7 | neutral 49.1 %, joy 28.3 %, anger 11.5 %, sadness 9.1 % |
| `emotion_score` | float | double | 0.00 % | 49 075 | min 0.20 · p50 0.92 · p95 0.99 · máx 1.00 |
| `label_sentiment` | string | string | 29.91 % | 3 | positive 40.1 %, negative 34.5 %, neutral 25.4 % |
| `nlp_model_version` | string | string | 0.00 % | 1 | robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a 100.0 % |
| `payload` | json | string | 0.00 % | 50 000 | largo 215–400 (p50 252) · máx. repetición 1 |
| `source` | string | — | 0.00 % | 3 | facebook 33.3 %, reddit 33.3 %, instagram 33.3 % |

### trusted/stream (171 629 filas)

| Columna | Tipo | Tipo físico | Nulos | Distintos | Resumen |
|---|---|---|---:|---:|---|
| `event_id` | string | string | 0.00 % | 171 629 | largo 36–36 (p50 36) · máx. repetición 1 |
| `origin` | string | string | 0.00 % | 1 | stream 100.0 % |
| `generator` | string | string | 0.00 % | 1 | comment-gen@1.0 100.0 % |
| `event_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 171 504 | 2026-10-21T22:00:52.466Z → 2026-10-24T03:11:35.606Z (2.22 días) |
| `ingest_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 171 626 | 2026-10-21T22:00:54.844Z → 2026-10-24T03:11:37.736Z (2.22 días) |
| `process_time` | timestamp | timestamp[ms, tz=UTC] | 0.00 % | 166 513 | 2026-10-21T22:00:57.271Z → 2026-10-24T03:11:38.260Z (2.22 días) |
| `user_hash` | string | string | 0.00 % | 5 850 | largo 64–64 (p50 64) · máx. repetición 9592 |
| `followers` | int | int64 | 0.00 % | 1 290 | min 1.00 · p50 136.00 · p95 1 660.00 · máx 19 446.00 |
| `text` | string | string | 0.00 % | 171 595 | largo 24–283 (p50 110) · máx. repetición 2 |
| `text_clean` | string | string | 0.00 % | 171 167 | largo 21–279 (p50 104) · máx. repetición 5 |
| `lang` | string | string | 0.00 % | 1 | es 100.0 % |
| `brands` | list | list<element: string> | 0.00 % | 9 | largo 0/1/2/3+: 0/162525/9104/0 · postobon 81455, pepsi 51776, sprite 47502 |
| `brand_primary` | string | string | 0.00 % | 3 | postobon 46.0 %, pepsi 28.3 %, sprite 25.7 % |
| `hashtags` | list | list<element: string> | 0.00 % | 790 | largo 0/1/2/3+: 102668/43168/21460/4333 · colombia 18100, gaseosa 17526, bebidas 13201, refresco 13175 |
| `likes` | int | int64 | 0.00 % | 617 | min -250.00 · p50 9.00 · p95 65.00 · máx 2 165.00 |
| `shares` | int | int64 | 66.66 % | 87 | min 0.00 · p50 0.00 · p95 8.00 · máx 154.00 |
| `comments` | int | int64 | 23.33 % | 189 | min 0.00 · p50 1.00 · p95 18.00 · máx 379.00 |
| `sentiment_label` | string | string | 38.03 % | 3 | negative 34.9 %, positive 32.8 %, neutral 32.3 % |
| `sentiment_score` | float | double | 38.03 % | 104 651 | min 0.34 · p50 0.87 · p95 0.98 · máx 0.99 |
| `emotion_label` | string | string | 38.03 % | 7 | neutral 49.3 %, joy 28.4 %, anger 11.2 %, sadness 9.0 % |
| `emotion_score` | float | double | 38.03 % | 102 478 | min 0.20 · p50 0.92 · p95 0.99 · máx 1.00 |
| `label_sentiment` | string | string | 30.09 % | 3 | positive 39.8 %, negative 34.4 %, neutral 25.8 % |
| `nlp_model_version` | string | string | 38.03 % | 1 | robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a 100.0 % |
| `payload` | json | string | 0.00 % | 171 629 | largo 214–405 (p50 252) · máx. repetición 1 |
| `source` | string | — | 0.00 % | 3 | instagram 33.4 %, facebook 33.3 %, reddit 33.3 % |

## 4. Distribuciones por origen y fuente

| Grupo | Marcas (brand_primary) | Sentimiento (NLP) | Emoción (NLP) | label_sentiment nulo | kind |
|---|---|---|---|---:|---|
| historical/* | postobon 42.4 %, pepsi 33.6 %, sprite 24.1 % | negative 35.2 %, positive 32.7 %, neutral 32.0 % | neutral 49.1 %, joy 28.3 %, anger 11.5 %, sadness 9.1 %, surprise 0.7 %, fear 0.7 %, disgust 0.6 % | 29.9 % | comment 85.0 %, post 15.0 % |
| historical/facebook | postobon 42.4 %, pepsi 33.7 %, sprite 24.0 % | negative 35.6 %, positive 33.7 %, neutral 30.8 % | neutral 48.3 %, joy 28.2 %, anger 11.8 %, sadness 9.6 %, disgust 0.7 %, surprise 0.7 %, fear 0.7 % | 29.5 % | comment 84.9 %, post 15.1 % |
| historical/instagram | postobon 42.3 %, pepsi 33.3 %, sprite 24.4 % | neutral 35.4 %, negative 34.0 %, positive 30.6 % | neutral 49.2 %, joy 28.8 %, anger 11.8 %, sadness 8.6 %, fear 0.6 %, surprise 0.6 %, disgust 0.5 % | 30.3 % | comment 100.0 % |
| historical/reddit | postobon 42.4 %, pepsi 33.7 %, sprite 23.9 % | negative 36.1 %, positive 34.0 %, neutral 29.9 % | neutral 49.7 %, joy 28.0 %, anger 11.0 %, sadness 9.2 %, fear 0.8 %, surprise 0.7 %, disgust 0.6 % | 30.0 % | comment 70.1 %, post 29.9 % |
| stream/* | postobon 46.0 %, pepsi 28.3 %, sprite 25.7 % | negative 34.9 %, positive 32.8 %, neutral 32.3 % | neutral 49.3 %, joy 28.4 %, anger 11.2 %, sadness 9.0 %, fear 0.7 %, surprise 0.7 %, disgust 0.6 % | 30.1 % | comment 84.9 %, post 15.1 % |
| stream/facebook | postobon 45.7 %, pepsi 28.4 %, sprite 25.8 % | negative 35.1 %, positive 33.4 %, neutral 31.5 % | neutral 49.6 %, joy 28.2 %, anger 11.0 %, sadness 9.0 %, fear 0.7 %, surprise 0.7 %, disgust 0.6 % | 30.1 % | comment 84.7 %, post 15.3 % |
| stream/instagram | postobon 46.2 %, pepsi 28.4 %, sprite 25.4 % | neutral 35.8 %, negative 33.7 %, positive 30.5 % | neutral 49.6 %, joy 28.6 %, anger 11.4 %, sadness 8.8 %, fear 0.5 %, surprise 0.5 %, disgust 0.5 % | 29.9 % | comment 100.0 % |
| stream/reddit | postobon 46.0 %, pepsi 28.1 %, sprite 25.9 % | negative 35.9 %, positive 34.5 %, neutral 29.6 % | neutral 48.8 %, joy 28.5 %, anger 11.1 %, sadness 9.3 %, surprise 0.9 %, fear 0.8 %, disgust 0.7 % | 30.3 % | comment 70.1 %, post 29.9 % |

Seguidores, engagement y acuerdo del NLP con el *ground truth* (dato; la precisión formal es HU-033):

| Grupo | followers p50 / p99 / máx | likes p50 / máx | comments p50 | Acuerdo NLP | Usuarios | Posts por usuario (media / máx) |
|---|---|---|---|---:|---:|---|
| historical/* | 136.00 / 3 208.00 / 19 446.00 | 9.00 / 3 710.00 | 1.00 | 91.8 % | 4 547 | 11.00 / 2 858.00 |
| historical/facebook | 122.00 / 2 233.00 / 19 446.00 | 8.00 / 3 710.00 | 1.00 | 92.5 % | 1 511 | 11.03 / 2 800.00 |
| historical/instagram | 136.00 / 1 995.00 / 11 060.00 | 11.00 / 550.00 | 1.00 | 89.7 % | 1 493 | 11.16 / 2 788.00 |
| historical/reddit | 135.00 / 12 992.00 / 14 770.00 | 8.00 / 855.00 | 8.00 | 93.2 % | 1 543 | 10.80 / 2 858.00 |
| stream/* | 136.00 / 3 159.56 / 19 446.00 | 9.00 / 2 165.00 | 1.00 | 92.0 % | 5 850 | 29.34 / 9 592.00 |
| stream/facebook | 122.00 / 2 233.00 / 19 446.00 | 8.00 / 2 165.00 | 1.00 | 92.7 % | 1 943 | 29.45 / 9 563.00 |
| stream/instagram | 136.00 / 2 014.00 / 11 060.00 | 11.00 / 539.00 | 1.00 | 89.9 % | 1 944 | 29.46 / 9 592.00 |
| stream/reddit | 135.00 / 12 992.00 / 14 770.00 | 8.00 / 1 101.00 | 9.00 | 93.5 % | 1 963 | 29.10 / 9 470.00 |

## 5. Tiempo y particiones

| Grupo | event_time (rango) | Días | Lag ingest − event p95 (s) | Latencia process − event p50 / p95 (s) | Offset partición mín / máx (s) |
|---|---|---:|---:|---|---|
| */* | 2026-04-27T00:01:32.345Z → 2026-10-24T03:11:35.606Z | 181 | 4.75 | 3.70 / 11 545 741.81 | — / — |
| historical/* | 2026-04-27T00:01:32.345Z → 2026-10-23T23:52:30.630Z | 180 | 4.75 | 7 224 428.41 / 14 609 354.03 | — / — |
| stream/* | 2026-10-21T22:00:52.466Z → 2026-10-24T03:11:35.606Z | 4 | 4.75 | 2.94 / 5.38 | — / — |

En el histórico, `dt`/`hr` son la fecha y hora de `event_time` y el *offset* es `event_time − hora de la partición` (0–3.600 s). En el stream son la hora de proceso en GCS y el *offset* es `process_time − hora de la partición`. La latencia del histórico no es una latencia real: `process_time` es el momento de la carga (§3.17).

## 6. Texto: diversidad léxica y repetición del generador

| Grupo | Textos | Distintos | Repetidos (filas) | Máx. repetición | Esqueletos distintos | Tokens / texto | Tipos | TTR | MATTR(500) | Herdan C | distinct-2 | Hapax |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| */* text | 221 629 | 99.98 % | 98 | 2 | 98.71 % | 20.40 | 3 614 | 0.0008 | 0.4791 | 0.5346 | 0.0135 | 29.0 % |
| */* text_clean | 221 629 | 99.67 % | 1 410 | 6 | 98.71 % | 19.82 | 3 613 | 0.0008 | 0.4814 | 0.5356 | 0.0125 | 29.1 % |
| */facebook text | 73 890 | 99.99 % | 12 | 2 | 99.43 % | 20.28 | 2 617 | 0.0018 | 0.4821 | 0.5534 | 0.0300 | 28.0 % |
| */facebook text_clean | 73 890 | 99.89 % | 160 | 3 | 99.43 % | 19.85 | 2 616 | 0.0018 | 0.4813 | 0.5542 | 0.0279 | 28.0 % |
| */instagram text | 73 946 | 100.00 % | 4 | 2 | 99.42 % | 20.91 | 2 638 | 0.0017 | 0.4727 | 0.5528 | 0.0296 | 28.8 % |
| */instagram text_clean | 73 946 | 99.87 % | 189 | 3 | 99.42 % | 19.81 | 2 637 | 0.0018 | 0.4814 | 0.5548 | 0.0280 | 28.8 % |
| */reddit text | 73 793 | 99.99 % | 12 | 2 | 99.42 % | 20.00 | 2 635 | 0.0018 | 0.4829 | 0.5545 | 0.0298 | 28.0 % |
| */reddit text_clean | 73 793 | 99.87 % | 190 | 3 | 99.42 % | 19.80 | 2 634 | 0.0018 | 0.4815 | 0.5549 | 0.0280 | 28.0 % |
| historical/* text | 50 000 | 100.00 % | 4 | 2 | 99.54 % | 20.42 | 2 332 | 0.0023 | 0.4785 | 0.5605 | 0.0390 | 25.5 % |
| historical/* text_clean | 50 000 | 99.90 % | 96 | 4 | 99.54 % | 19.84 | 2 331 | 0.0024 | 0.4807 | 0.5616 | 0.0363 | 25.5 % |
| stream/* text | 171 629 | 99.98 % | 68 | 2 | 98.93 % | 20.39 | 3 363 | 0.0010 | 0.4793 | 0.5389 | 0.0164 | 29.5 % |
| stream/* text_clean | 171 629 | 99.73 % | 886 | 5 | 98.93 % | 19.82 | 3 362 | 0.0010 | 0.4816 | 0.5399 | 0.0152 | 29.5 % |

Textos más repetidos (primeros 60 caracteres; son plantillas del generador, no datos personales):

- 2 veces: «Bueno, un domingo sin Postobón no es domingo.…»
- 2 veces: «Cada vez que tomo Pepsi me acuerdo de los partidos de fútbol…»
- 2 veces: «Cambiaron la etiqueta de Postobón, ahora trae un diseño nuev…»
- 2 veces: «Cambiaron la etiqueta de Postobón, ahora trae un diseño nuev…»
- 2 veces: «Confieso que no tolero Sprite porque es pesada. #Sprite…»

Palabras más frecuentes: de (175515), la (174492), me (139687), en (130967), postobon (116378), que (115734), y (104065), mi (88628), con (85518), el (83848), a (83454), pepsi (77859).

Los duplicados de **evento** se miden por `event_id` (sección 7), no por texto: el mismo texto en dos eventos es esperable con plantillas (§3.5).

## 7. Duplicados por event_id

Filas 221 629, `event_id` distintos 221 629, copias 0 (mismo archivo 0, otro archivo del mismo origen 0, entre orígenes 0).

- */*: unicidad 100.000 %, copias 0, máx. copias 1
- historical/*: unicidad 100.000 %, copias 0, máx. copias 1
- stream/*: unicidad 100.000 %, copias 0, máx. copias 1

## 8. Archivos y esquema

Archivos listados 543, reconocidos 543, no reconocidos 0, ilegibles 0, auxiliares 0. Variantes de esquema: 1.

- 543 archivos: = contrato

| Grupo | Archivos | Filas por archivo p50 (mín–máx) | KB por archivo p50 |
|---|---:|---|---:|
| */* | 543 | 89 (55–27 766) | 41.5 |
| */facebook | 181 | 88 (55–27 693) | 42.1 |
| */instagram | 181 | 90 (59–27 766) | 40.3 |
| */reddit | 181 | 89 (60–27 676) | 41.3 |

## 9. Stream frente a histórico

| Variable | Valor | Stream | Histórico | Diferencia (pp) |
|---|---|---:|---:|---:|
| brand_primary | null | 0.0 % | 0.0 % | 0.00 |
| brand_primary | pepsi | 28.3 % | 33.6 % | -5.28 |
| brand_primary | postobon | 46.0 % | 42.4 % | 3.65 |
| brand_primary | sprite | 25.7 % | 24.1 % | 1.63 |
| sentiment_label | negative | 34.9 % | 35.2 % | -0.30 |
| sentiment_label | neutral | 32.3 % | 32.0 % | 0.26 |
| sentiment_label | positive | 32.8 % | 32.7 % | 0.04 |
| emotion_label | anger | 11.2 % | 11.5 % | -0.37 |
| emotion_label | disgust | 0.6 % | 0.6 % | 0.01 |
| emotion_label | fear | 0.7 % | 0.7 % | 0.04 |
| emotion_label | joy | 28.4 % | 28.3 % | 0.11 |
| emotion_label | neutral | 49.3 % | 49.1 % | 0.26 |
| emotion_label | sadness | 9.0 % | 9.1 % | -0.08 |
| emotion_label | surprise | 0.7 % | 0.7 % | 0.03 |
| label_sentiment | negative | 24.1 % | 24.2 % | -0.12 |
| label_sentiment | neutral | 18.0 % | 17.8 % | 0.22 |
| label_sentiment | null | 30.1 % | 29.9 % | 0.18 |
| label_sentiment | positive | 27.8 % | 28.1 % | -0.28 |
| lang | es | 100.0 % | 100.0 % | 0.00 |
| lang | null | 0.0 % | 0.0 % | 0.00 |
| source | facebook | 33.3 % | 33.3 % | 0.01 |
| source | instagram | 33.4 % | 33.3 % | 0.04 |
| source | reddit | 33.3 % | 33.3 % | -0.05 |
| payload_kind | comment | 84.9 % | 85.0 % | -0.04 |
| payload_kind | post | 15.1 % | 15.0 % | 0.04 |

| Métrica | Stream | Histórico |
|---|---:|---:|
| text_length_mean | 112.2813 | 112.2457 |
| text_tokens_mean | 20.3932 | 20.4163 |
| followers_p50 | 136.0000 | 136.0000 |
| followers_p99 | 3 159.5600 | 3 208.0000 |
| likes_p50 | 9.0000 | 9.0000 |
| comments_p50 | 1.0000 | 1.0000 |
| ingest_lag_s_p95 | 4.7520 | 4.7530 |
| latency_s_p50 | 2.9390 | 7 224 428.4080 |
| latency_s_p95 | 5.3810 | 14 609 354.0301 |
| distinct_users | 5 850 | 4 547 |
| posts_per_user_mean | 29.3383 | 10.9963 |
| posts_per_user_max | 9 592.0000 | 2 858.0000 |
| distinct_text_ratio | 0.9998 | 1.0000 |
| skeleton_distinct_ratio | 0.9893 | 0.9954 |
| mattr | 0.4793 | 0.4785 |
| ttr | 0.0010 | 0.0023 |
| hapax_ratio | 0.2950 | 0.2551 |
| nlp_agreement | 0.9205 | 0.9180 |
| rows_with_hashtags_ratio | 0.4018 | 0.4018 |

Solapes: 13 textos idénticos en los dos orígenes (0.01 % de los textos distintos del stream); 4 461 `user_hash` comunes (76.26 % de los 5 850 del stream; el histórico tiene 4 547).

## 10. Anomalías

| Gravedad | Tipo | Columna | Alcance | Filas | Detalle |
|---|---|---|---|---:|---|
| warning | nlp_ausente | sentiment_label | trusted/stream/* | 65 263 | 65263 filas sin NLP en 2 particiones dt (2026-10-21 → 2026-10-22; 1 sin ninguna fila con NLP); ¿las procesó un job con el NLP apagado? (§3.14; DQ-13 de §5.10) |
| info | clase_rara | emotion_label | trusted/historical/* | — | emociones con menos del 1 %: surprise 0.68 %, fear 0.68 %, disgust 0.59 % |
| info | clase_rara | emotion_label | trusted/stream/* | — | emociones con menos del 1 %: fear 0.71 %, surprise 0.71 %, disgust 0.60 % |
| info | cola_larga | followers | trusted/historical/* | 6 452 | followers con cola larga: p50 136, p99 3208, máx 19446 |
| info | cola_larga | followers | trusted/stream/* | 22 229 | followers con cola larga: p50 136, p99 3159.56, máx 19446 |
| info | constante | * | trusted/historical/* | — | columnas con un solo valor: generator=comment-gen@1.0, process_time, lang=es, nlp_model_version=robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a |
| info | constante | * | trusted/stream/* | — | columnas con un solo valor: generator=comment-gen@1.0, lang=es, nlp_model_version=robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a |
| info | ground_truth | label_sentiment | trusted/historical/* | — | label_sentiment nulo en el 29.9 % (nostalgia y adiccion, ADR-13; esperado ~30 %) |
| info | ground_truth | label_sentiment | trusted/stream/* | — | label_sentiment nulo en el 30.1 % (nostalgia y adiccion, ADR-13; esperado ~30 %) |
| info | solape | * | * | — | 13 textos idénticos en los dos orígenes; 4461 user_hash comunes (76.3 % de los del stream) |
| info | stream_vs_historico | brand_primary | * | — | brand_primary (stream vs histórico): pepsi: 28.3 vs 33.6 % |
| info | texto_repetido | text | trusted/historical/* | 4 | 100.00 % de textos distintos; 2 textos repetidos (máx 2 veces); esqueletos distintos 99.54 %; MATTR 0.4785, vocabulario 2332 tipos en 1020815 tokens |
| info | texto_repetido | text | trusted/stream/* | 68 | 99.98 % de textos distintos; 34 textos repetidos (máx 2 veces); esqueletos distintos 98.93 %; MATTR 0.4793, vocabulario 3363 tipos en 3500070 tokens |
| info | usuario_dominante | user_hash | trusted/historical/* | — | el usuario más activo de una fuente tiene el 17.1 % de sus filas (actividad Zipf s = 1,1 por fuente, §3.10) |
| info | usuario_dominante | user_hash | trusted/stream/* | — | el usuario más activo de una fuente tiene el 16.7 % de sus filas (actividad Zipf s = 1,1 por fuente, §3.10) |

## 11. Reglas propuestas para HU-032 (Lote 5.2)

Catálogo completo con expectativa, predicado SQL y motivo en `docs/ARCHITECTURE.md` §5.10. Estado con los datos de hoy:

| Regla | Columna | Comprobación | Umbral | KPI | Gravedad | Observado | Estado |
|---|---|---|---|---|---|---|---|
| DQ-01 | `*` | `metric:schema` | todos los archivos | otro | error | *: 1.0 | cumple |
| DQ-02 | `*` | `metric:anonimizacion` | 100 % | anonimizacion | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-03 | `user_hash` | `regex:user_hash` | 100 % | anonimizacion | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-04 | `payload` | `payload:no_identity` | 100 % | anonimizacion | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-05 | `*` | `metric:completitud` | ≥ 95 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-06 | `event_id` | `not_null:event_id` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-07 | `event_time` | `not_null:event_time` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-09 | `text` | `not_null:text` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-10 | `user_hash` | `not_null:user_hash` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-11 | `lang` | `not_null:lang` | ≥ 98 % | completitud | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-12 | `followers` | `not_null:followers` | ≥ 98 % | completitud | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-13 | `nlp_model_version` | `nlp:complete_since` | ≥ 99 % desde 2026-10-22T23:00:00Z | completitud | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-16 | `event_id` | `uuid:event_id` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-17 | `origin` | `enum:origin` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-19 | `source` | `enum:source` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-21 | `generator` | `regex:generator` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-22 | `lang` | `set:lang` | 100 % | otro | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-23 | `followers` | `range:followers` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-24 | `likes` | `engagement:likes` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-25 | `shares` | `engagement:shares` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-26 | `comments` | `engagement:comments` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-27 | `sentiment_label` | `enum:sentiment_label` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-28 | `emotion_label` | `enum:emotion_label` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-29 | `sentiment_score` | `range:sentiment_score` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-30 | `emotion_score` | `range:emotion_score` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-31 | `nlp_model_version` | `regex:nlp_model_version` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-32 | `nlp_model_version` | `metric:nlp_single_version` | 1 versión | otro | warning | historical: 1, stream: 1 | cumple |
| DQ-33 | `label_sentiment` | `enum:label_sentiment` | 100 % (nulos permitidos) | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-34 | `label_sentiment` | `metric:label_sentiment_null` | nulos entre 25% y 35% | otro | warning | historical: 0.2991, stream: 0.30094 | cumple |
| DQ-35 | `brands` | `regex:brands` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-36 | `brands` | `set:brands` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-37 | `brand_primary` | `pair:brand_primary` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-38 | `hashtags` | `regex:hashtags` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-39 | `hashtags` | `consistency:hashtags_text` | 100 % | otro | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-40 | `text` | `text:not_empty` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-41 | `text` | `metric:text_max_length` | ≤ 450 caracteres | otro | warning | historical: 268.0, stream: 283.0 | cumple |
| DQ-42 | `payload` | `payload:json` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-43 | `payload` | `payload:kind` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-44 | `ingest_time` | `time:ingest_ge_event` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-45 | `ingest_time` | `time:ingest_lag` | ≥ 98 % entre 0 y 5 s | otro | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-46 | `process_time` | `time:process_ge_ingest` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-47 | `event_time` | `time:not_future` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-48 | `event_id` | `metric:duplicados` | < 1 % | duplicados | error | historical: 0.0, stream: 0.0 | cumple |
| DQ-49 | `event_id` | `metric:unicidad` | 100 % | unicidad | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-50 | `dt` | `partition:projection_range` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-54 | `event_time` | `partition:trusted_event_date` | 100 % | otro | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-55 | `source` | `not_null:source_partition` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-56 | `text_clean` | `not_null:text_clean` | 100 % | completitud | error | historical: 1.0, stream: 1.0 | cumple |
| DQ-57 | `text_clean` | `text_clean:not_longer` | 100 % | otro | warning | historical: 1.0, stream: 1.0 | cumple |
| DQ-58 | `*` | `metric:rows_vs_raw` | entre 99 % y 100 % de los event_id distintos de Raw | duplicados | error | *: {'trusted_rows': 221629, 'raw_distinct': 221635} | cumple |
| DQ-60 | `source` | `metric:source_share` | cada fuente entre 30% y 37% | otro | warning | historical: {'facebook': 0.3333, 'instagram': 0.3333, 'reddit': 0.3333}, stream: {'facebook': 0.3334, 'instagram': 0.3337, 'reddit': 0.3328} | cumple |
| DQ-61 | `text` | `metric:text_distinct_ratio` | ≥ 99% por origen | otro | warning | historical: 0.99996, stream: 0.9998 | cumple |
| DQ-62 | `sentiment_label` | `metric:nlp_agreement` | ≥ 88% | precision_nlp | warning | historical: 0.91802, stream: 0.92048 | cumple |
| DQ-63 | `generator` | `set:generator` | 100 % | otro | warning | historical: 1.0, stream: 1.0 | cumple |

