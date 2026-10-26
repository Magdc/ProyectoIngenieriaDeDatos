# Rama de tendencias, `dlq_events` y plantilla Flex (Lote 2.4)

> HU-005 y HU-023. Contrato en `docs/ARCHITECTURE.md` §3.16, §5.3, §5.5, §5.8 y §5.12.
> Validación paso a paso en `docs/validation/validation-2-4.md`. Rutas relativas a `project/`.

## 1. Qué hace

```
filas con NLP (attach_enriched) ─► TrendFeatures ─► SlidingWindows(300 s, 60 s) ─► CombinePerKey(marca|fuente)
   ─► ScoreTrends (baseline de 60 ventanas en estado) ─┬─► ta_stream.trend_windows      (Dataflow)
                                                       └─► .local-lake/trend_windows/    (local)
ta-dlq-df ─► DlqEventsToBigQuery ─► ta_stream.dlq_events                                 (solo Dataflow)
```

| Archivo | Contenido |
|---|---|
| `streaming/trends_core.py` | Lógica pura: ventanas, acumulador, `avg_sentiment`, z-score, alertas, fila de salida y reglas. |
| `streaming/trends.py` | `TrendOptions`, `resolve_trends_config()`, `TrendWindows`, `ScoreTrendsFn`, sumidero local y `attach_trends()`. |
| `streaming/dlq_bq.py` | `ta-dlq-df` → `dlq_events` (fila desde los atributos `dlq_*` de §5.3). |
| `streaming/bq_sink.py` | `WriteRowsToBigQuery`: *streaming inserts* con registro de filas rechazadas. |
| `common/stream_tables.py` | Columnas de `trend_windows`, `dlq_events` y `dq_results`; genera los schemas de Terraform. |
| `streaming/trends_check.py`, `trends_bq.py`, `trends_cli.py` | Verificador (`scripts/verify_trends.py`). |
| `streaming/template_tools.py`, `flex_main.py`, `flex_template/` | Plantilla Flex (`scripts/dataflow_template.py`). |

## 2. Enganche (dos o tres líneas en archivos de lotes anteriores)

En `src/streaming/pipeline.py`, junto a la rama enriched del Lote 2.2:

```python
from streaming.trends import attach_trends            # Lote 2.4
...
enriched_rows = attach_enriched(unique)                # antes: attach_enriched(unique)
attach_trends(enriched_rows)                           # Lote 2.4
```

`attach_trends` necesita la `PCollection` de filas de `posts_enriched` **ya con NLP** (la que
reciben los sumideros de `enriched/` y `posts_enriched`). Acepta la `PCollection` o un objeto que
la tenga como atributo o clave `rows`, `nlp_rows`, `enriched_rows`, `rows_with_nlp`, `with_nlp`,
`enriched`, `output` o `result`.

Si `attach_enriched()` no devuelve nada, añada al final de esa función, en `src/streaming/enriched.py`:

```python
    return rows   # el nombre de la PCollection que va a WriteEnrichedParquet / posts_enriched
```

Las pruebas `test_trends_hook.py` y el paso A4 del validador comprueban las tres cosas. Si
`attach_enriched()` devuelve `None` porque la rama está apagada (local sin salida), la de
tendencias también se apaga; en Dataflow eso es un error.

## 3. Opciones

Todas tienen valor por defecto; `dataflow_job.sh start` no necesita cambios.

| Opción | Por defecto | Efecto |
|---|---|---|
| `--trends_enabled` | `auto` | Con `auto` manda `TA_TRENDS_ENABLED` (por defecto, encendida). |
| `--trend_windows_table` | `auto` | Dataflow: `{proyecto}:ta_stream.trend_windows`. `none` la apaga. |
| `--trends_root` | `auto` | Local: `{output_root}/trend_windows`. No admite `gs://` (§5.4). |
| `--trend_window_seconds`, `--trend_period_seconds` | 300, 60 | Ventana y paso. |
| `--trend_allowed_lateness_seconds` | 1800 | Atraso tolerado; más tarde, `late_events` y sin recalcular. |
| `--trend_late_delay_seconds` | `auto` | Dataflow 60 (un panel tardío por minuto); local 0 (un panel por dato tardío). |
| `--trend_baseline_windows` | 60 | Ventanas de baseline; sin ellas, `is_alert = false`. |
| `--trend_z_threshold`, `--trend_min_posts` | 3.0, 5 | Umbral de \|z\| y posts mínimos para alertar. |
| `--dlq_bq_enabled` | `auto` | Solo Dataflow; con `auto` manda `TA_DLQ_BQ_ENABLED`. Se apaga si se pasa `--dlq_table` del 2.1. |
| `--dlq_events_table` | `auto` | Dataflow: `{proyecto}:ta_stream.dlq_events`. |
| `--dlq_df_subscription` | `auto` | `projects/{proyecto}/subscriptions/ta-dlq-df`. |

Al construir el pipeline se imprime una línea `{"event": "trends_config", …}` en la terminal
del lanzador (o en los logs de `dataflow-local`), como `enriched_config` del Lote 2.2.

## 4. Salida y lectura

- **`trend_windows`**: una fila por **panel**. El panel a tiempo sale cuando la marca de agua
  pasa el fin de la ventana; los datos tardíos (hasta 30 min) generan paneles con los conteos
  acumulados. La versión vigente de cada ventana × marca × fuente es la de mayor `n_posts`:
  la vista **`trend_windows_latest`** hace esa selección. Para consultar, use la vista.
- **Métricas** (espacio `trends`): `trend_rows_in`, `late_events`, `trend_rows_without_brand`,
  `trend_rows_unrecognized`, `panes_on_time`, `panes_late`, `panes_incomplete`, `windows_emitted`, `alerts`,
  `window_emit_delay_ms`, `bq_failed_rows`. Espacio `dlq_bq`: `dlq_rows`, `received_at_fallback`,
  `rows_without_type`, `bq_failed_rows`.
- **Logs** (workers): `trend_alert` (cada alerta), `trend_row_unrecognized` (5 primeras),
  `trend_bq_insert_failed`, `dlq_bq_insert_failed`.

## 5. Plantilla Flex

```bash
cd project
python scripts/dataflow_template.py params            # parámetros tomados de terraform output
python scripts/dataflow_template.py build             # dataflow-launcher:{commit} + _dataflow/templates/ta-stream.json
bash scripts/dataflow_job.sh drain                    # si hay un job ta-stream-dev activo
python scripts/dataflow_template.py run               # job ta-stream-dev desde la plantilla
```

La imagen `dataflow-launcher` solo arma el grafo; los workers usan `dataflow-nlp` (Lote 2.3).
`flex_main.py` corre el pipeline desde `src/` (como `dataflow_job.sh`), pasa `nlp_container_image`
a `TA_NLP_IMAGE` y tolera el resultado sin job que devuelve Beam con `--template_location`.
Después de cambiar algo de `src/`, reconstruya la imagen (`build`): la plantilla lleva el código adentro.
Los parámetros salen de los outputs `lake_bucket`, `artifact_registry_url`, `service_accounts`,
`posts_enriched_table`, `trend_windows_table`, `dlq_events_table` y `dlq_df_subscription`.

## 6. Problemas frecuentes

| Síntoma | Causa y arreglo |
|---|---|
| `trends_config` con `"enabled": false, "reason": "sin_salida"` | Local sin `--output_root`, o Dataflow sin proyecto. |
| `trend_row_unrecognized` en los logs | `attach_trends` recibió otra cosa (p. ej. la salida del escritor de Parquet). Revise §2. |
| RuntimeError `attach_trends recibió None en Dataflow` | `attach_enriched()` no devuelve las filas: añada `return rows`. |
| Muchas filas por ventana | Normal: paneles tardíos. Consulte `trend_windows_latest`. |
| Alertas con caída de volumen justo al detener el job | Ventanas cerradas por el drenado con datos parciales. Desde la corrección de v2.26 no alertan (`panes_incomplete`); si aparecen, el job drenado tenía el código anterior. |
| Ventanas duplicadas alrededor de un cambio de job | El job viejo y el nuevo escriben cada uno su parte de las ventanas que cruzan el cambio; ninguna versión está completa. Para análisis, descarte los 5 min alrededor del cambio. |
| `zscore_*` nulos al arrancar | Normal: hacen falta 2 ventanas completas de baseline; las alertas, 60. |
| `dlq_events` vacía | ¿Existe `ta-dlq-df` (parte C)? ¿Se lanzó el job con el código del 2.4? `dlq_bq_attached` en la terminal. |
| `GETTING_PUBSUB_SUBSCRIPTION_FAILED` persiste | El permiso de la parte C no se aplicó; si menciona el topic, añada `roles/pubsub.viewer` en los topics. |
| Local: el pipeline gira sin avanzar | `--trend_late_delay_seconds` > 0 con el DirectRunner (no lo admite): use `auto`. |
