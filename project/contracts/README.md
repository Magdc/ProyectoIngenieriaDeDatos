# contracts/ · copia legible por máquina de ARCHITECTURE.md §5

Lote 0.1 · HU-001. Si algo de aquí cambia, se cambia **primero** `docs/ARCHITECTURE.md` §5 y en el mismo commit este directorio.

## Contenido

| Archivo | Contrato | Sección |
|---|---|---|
| `envelope.v1.schema.json` | Envelope canónico (salida del simulador / loader) | §5.2 |
| `envelope.enriched.v1.schema.json` | Envelope enriquecido (salida de Dataflow / loader hacia BigQuery y S3). Sin `user_id`/`handle`. | §5.2 |
| `payload.{reddit,facebook,instagram}.schema.json` | `payload` por fuente (se enlazan desde los dos envelopes según `source`) | §5.2 |
| `pubsub.attributes.schema.json` | Atributos de mensaje de `ta-raw-{source}` | §5.3 |
| `bigquery/{posts_enriched,trend_windows,dlq_events,dq_results}.sql` | Tablas del dataset `ta_stream` | §5.5 |
| `athena/{posts_raw,posts,curated}.sql` | Capas Raw, Trusted y Curated (partition projection) | §5.7 |
| `examples/valid/`, `examples/invalid/` | Ejemplos; el prefijo (`envelope__`, `enriched__`, `pubsub__`) indica contra qué schema se valida | – |
| `tests/test_contracts.py` | 18 pruebas (schemas, ejemplos, coherencia BigQuery ↔ Athena ↔ envelope) | – |

## Cómo validar

```bash
pip install -r project/contracts/requirements.txt
pytest project/contracts/tests -q          # o: python project/contracts/tests/test_contracts.py
```

Para validar eventos reales en código: cargar los `*.schema.json` en un `referencing.Registry` (los `$ref` son relativos al `$id`) y usar `Draft202012Validator`. Los generadores deben validar ≥ 1.000 eventos por fuente con el caos apagado (§5.12).

## Aplicar las DDL

Los placeholders de despliegue son `{{GCP_PROJECT}}` (BigQuery) y `{{lake_bucket}}` (Athena); los `${part_dt}` etc. de Athena son variables de partition projection y **no** se sustituyen.

## Decisiones tomadas en este lote (contratos tocados)

§5 de ARCHITECTURE.md no las fijaba; están reflejadas en la copia v2.7 del documento. Revisar antes de que alguien construya sobre ellas:

1. **Parquet plano.** Lo que Dataflow escribe en `enriched/` y lo que llega a `raw/` en S3 usa las columnas de `posts_enriched` (`user_hash`, `followers`, `label_sentiment` y `brand_primary` aplanados; `payload` como JSON en string), no el JSON anidado. `posts_raw` coincide columna a columna con BigQuery (lo verifica un test).
2. **`hr` también en el histórico.** La proyección de Raw usa `origin/source/dt/hr`; el loader histórico debe escribir `hr={HH}` igual que el exportador.
3. **Claves de partición de Raw con prefijo `part_`** (`part_origin`, `part_source`, `part_dt`, `part_hr`): `origin` y `source` ya existen como columnas dentro del Parquet y Athena no admite ambas con el mismo nombre. La ruta física no cambia.
4. **Trusted:** `dt = DATE(event_time)` en UTC (Raw usa la fecha de proceso) y columna nueva `text_clean`. `source` es partición, no columna.
5. **`avg_sentiment`** = media con signo en [-1, 1] (positivo `+score`, negativo `-score`, neutro 0), igual en `trend_windows` y `agg_trend_daily`.
6. **`sim.chaos`** ∈ `duplicate | late | null_fields | malformed` (§3.9).
7. **Invariantes por tipo de payload** (se validan): en Reddit, `post` ⇒ `title` y `upvote_ratio`/`num_comments` presentes, `parent_id` nulo; `comment` ⇒ `title`, `upvote_ratio` y `num_comments` nulos. Facebook `post` ⇒ `parent_id` nulo. En el enriquecido: `shares` es `null` en Reddit/Instagram y entero en Facebook.
8. **`dq_results`** y las **dimensiones/agregados de Curated** no tenían columnas en §5; las defino aquí.
9. **`nlp_model_version`** debe tener la forma `robertuito-sentiment@<hash>+robertuito-emotion@<hash>`. Si se cambia de modelo (plan B de ADR-05), se cambia el patrón.
10. **Forma de `hashtags[]`** (decidida el 17 oct): forma canónica = minúsculas, **sin `#`**, sin tildes, solo `[a-z0-9_]` (1–100 caracteres), sin repetidos y en orden de aparición en el texto. El texto conserva el hashtag tal como se escribió (`#Postobón` en el texto ⇒ `"postobon"` en `hashtags[]`). Razón: las claves de `edges_hashtag_cooccurrence` y los joins quedan estables, igual que las claves de marca. Regla de referencia: `normalize_hashtag()` en `tests/test_contracts.py`; el simulador debe implementarla en `src/common/`.

## Por confirmar

- **`HU.md`** no estaba entre los archivos que recibí, así que no puedo confirmar criterio por criterio los de HU-001; la cobertura contra §5 está en las pruebas.
