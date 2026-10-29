# TrendAnalyzer · Informe técnico

> Lote 8.1 (HU-044). Resume qué se construyó, cómo, con qué decisiones y qué se midió. El detalle de cada pieza está en
> [`ARCHITECTURE.md`](ARCHITECTURE.md) (contratos en §5, decisiones en §9). Cada cifra de este informe sale de la
> evidencia de una validación en `project/evidencia/` y tiene un comando para volver a medirla. La tabla de KPI la
> escribe `python project/scripts/kpi_report.py --write`.

## 1. Qué es

TrendAnalyzer detecta en casi tiempo real cambios en la conversación de redes sociales sobre marcas de gaseosas
colombianas (Postobón, Pepsi, Sprite). Mide volumen, sentimiento y emoción, y avisa cuando una marca se dispara. El
procesamiento en caliente corre en **GCP**: Pub/Sub, Dataflow con NLP en español, BigQuery y alertas. El análisis
histórico y el tablero corren en **AWS**: un lakehouse en S3 con Glue y Athena, y QuickSight. Las dos nubes se unen sin
llaves, con federación OIDC.

## 2. Por qué las fuentes son simuladas

Las APIs reales piden aprobaciones que no caben en un semestre:
- **Reddit:** OAuth y límites de uso.
- **Facebook e Instagram:** la revisión de la aplicación por parte de Meta.

Además, sus términos no permiten guardar ni redistribuir los comentarios. Por eso el streaming se **simula** (ADR-01):
un **generador de comentarios en Python** (plantillas por marca y categoría, ruido de redes, hashtags) alimenta tres
fuentes con el formato de cada red (Reddit, Facebook e Instagram, §3.2–§3.10).

La simulación no se salta nada del problema de ingeniería:
- los eventos tienen identidad determinista;
- hay defectos controlados: duplicados, tardíos, nulos y malformados (§3.9);
- los usuarios llevan identificadores que hay que anonimizar;
- el histórico (50.000 filas en 180 días) sale del mismo generador (§3.17).

Ver las limitaciones en §7.

## 3. Arquitectura

```
GCP (us-central1)                                                     AWS (us-east-1)
Cloud Run ta-stream-simulator ──► Pub/Sub ta-raw-{reddit,facebook,instagram}
        (POST /control/scenario: ola de demo, §3.29)
                │
                ▼
Dataflow ta-stream-dev (1 worker): valida (DLQ) · deduplica · HMAC de user_id · NLP RoBERTuito · ventanas de 5 min
   ├─► GCS raw/ (auditoría) y enriched/ (Parquet) ──► Cloud Run Job ta-gcs-to-s3 (cada 15 min, OIDC sin llaves) ──► S3 raw/
   ├─► BigQuery posts_enriched, trend_windows, dlq_events, dq_results                                        │
   └─► is_alert ─► Pub/Sub ta-alerts ─► Cloud Run ta-alert-notifier ─► Cloud Monitoring ─► correo           ▼
                                                                         Glue ta-wf-raw-to-curated (cada hora)
                                                                         raw → trusted → curated (estrella, agregados, aristas)
                                                                                    │
                                                                         Athena ta-analytics ─► QuickSight (SPICE)
```

| Capa | Pieza | Sección |
|---|---|---|
| Ingesta | Simulador en Cloud Run (3 workers asyncio), 1,5 mensajes/s, escenarios de demo | §3.1–§3.10, §3.29 |
| Streaming | Dataflow (Beam 2.77, Streaming Engine): validación contra el contrato, deduplicación por `event_id`, anonimización HMAC-SHA256, NLP (RoBERTuito), tendencias con z-score y alertas | §3.13–§3.16, §3.28 |
| Puente | Exportador GCS → S3 con federación OIDC GCP → AWS | §3.18 |
| Lakehouse | S3 cifrado con KMS; Glue (Raw → Trusted → Curated) cada hora; Athena con *partition projection* | §3.19–§3.23 |
| Calidad y gobierno | Profiling, Great Expectations (239 comprobaciones), catálogo, linaje y gobernanza | §3.20, §3.24, §3.25 |
| Consumo | QuickSight Enterprise con SPICE; alertas por correo | §3.27, §3.28 |
| Operación | CI en GitHub Actions, tableros y alertas en Cloud Monitoring y CloudWatch, alertas de gasto, `on.py`/`off.py`/`status.py` | §3.30–§3.32 |

Infraestructura como código: Terraform con estado remoto en las dos nubes (4 raíces, ~30 módulos). El diagrama de
arquitectura original está en `docs/trend_analyzer_multicloud.jpg` y el linaje por columna en
[`LINEAGE.md`](LINEAGE.md).

## 4. Decisiones principales

Las 37 decisiones están en `ARCHITECTURE.md` §9, cada una con su alternativa descartada. Las que más pesan:

| ADR | Decisión |
|---|---|
| ADR-01 | Fuentes simuladas con un generador de comentarios en español |
| ADR-02 | Simulador en Cloud Run (no en EC2): vive junto a Pub/Sub |
| ADR-05 | NLP en español con RoBERTuito (pysentimiento), horneado en la imagen de Dataflow |
| ADR-17 | Deduplicación en dos capas por `event_id`, que es determinista: `uuid5(origen, fuente, semilla, seq)`, §3.6 |
| ADR-21 | Histórico con el mismo `EventFactory` del simulador, aceptación según un plan de tendencias |
| ADR-26 | Curated con Glue Python (sin dbt), recalculado entero en cada corrida |
| ADR-28 | Calidad con Great Expectations 1.24, a demanda, en Raw, Trusted y Curated |
| ADR-31 | Tablero en QuickSight Enterprise con SPICE (respuesta < 3 s) |
| ADR-32 | Alertas detectadas en Dataflow, *cooldown* de 15 min y correo por una política basada en logs |
| ADR-33 | La ola de demo se suma al tráfico normal (no lo reemplaza) |
| ADR-34 | CI sin credenciales; CD con OIDC, apagado hasta que el dueño lo active |
| ADR-35/36 | Alertas de gasto por olvido; observabilidad solo con métricas del sistema |

## 5. Contratos

Los componentes se hablan solo por contratos versionados (§5): el envelope v1.0 y los payloads de cada red en
`project/contracts/*.schema.json`, el DDL de BigQuery y de Athena, el mensaje `ta-alert` 1.0 y los outputs de Terraform.
Las pruebas de contratos (`project/contracts/tests`, 18) y la del simulador contra el envelope (1.000 eventos por
fuente) corren en cada PR (HU-001, §3.30).

## 6. KPI medidos frente al objetivo

<!-- kpis:inicio · generado por `python project/scripts/kpi_report.py --write`; no editar a mano -->
| KPI | Objetivo | Medido | Cumple | Evidencia | Cómo volver a medirlo |
|---|---|---|---|---|---|
| Latencia de ingesta: Pub/Sub → BigQuery `posts_enriched` | < 60 s (§5.12) | 19,1 s | ✅ | `project/evidencia/` smoke_e2e_5_4.json (HU-048) | `python project/scripts/smoke_e2e.py run` |
| Latencia de detección de tendencias: fin de la ventana → notificación | < 5 min (300 s) | 159,8 s (6.2) y 106,9 s (6.3) | ✅ | `project/evidencia/` validacion_6_2.json, validacion_6_3.json (F7) | `python project/scripts/validate_6_3.py flow` |
| Punta a punta: Pub/Sub → `ta_curated.fact_posts` con el exportador y el workflow forzados | cada etapa en su objetivo (§5.12) | 465,2 s; bigquery 19,1 s, gcs_enriched 175,1 s, s3_raw 78,6 s, trusted 149,1 s, curated 62,4 s | ✅ | `project/evidencia/` smoke_e2e_5_4.json (HU-048) | `python project/scripts/smoke_e2e.py run` |
| Completitud (capa confiable) | ≥ 95 % | 100 % (peor de 2) | ✅ | `project/evidencia/` validacion_5_2.json (Great Expectations) | `python project/scripts/run_dq.py run` (venv `~/.venvs/ta-gx`) |
| Unicidad (capa confiable) | ≥ 99 % | 100 % (peor de 2) | ✅ | `project/evidencia/` validacion_5_2.json (Great Expectations) | `python project/scripts/run_dq.py run` (venv `~/.venvs/ta-gx`) |
| Duplicados (capa confiable) | < 1 % | 0 % (peor de 2) | ✅ | `project/evidencia/` validacion_5_2.json (Great Expectations) | `python project/scripts/run_dq.py run` (venv `~/.venvs/ta-gx`) |
| Respuesta del tablero de QuickSight (SPICE) | < 3 s | 2,83 s el peor clic; carga de visual máx. 1,86 s | ✅ | `project/evidencia/` validacion_6_1.json (E1, E5) | `python project/scripts/validate_6_1.py latency` |
| Identificadores de usuario anonimizados antes de la capa analítica | 100 % | 100 % en Curated, Raw, Trusted; 0 `user_id`/`handle` en las 5 etapas de la prueba de humo | ✅ | `project/evidencia/` validacion_5_2.json, smoke_e2e_5_4.json | `python project/scripts/run_dq.py run` y `smoke_e2e.py run` |
| Precisión del NLP frente a la verdad del simulador (HU-033) | informativo | accuracy 0,9197, F1 macro 0,9167 (109.365 filas) | — | `project/evidencia/` validacion_5_2.json, precision_nlp_5_2.md | `python project/scripts/run_dq.py run` |
<!-- kpis:fin -->

Notas de lectura:
- **Latencia de detección.** Se midió dos veces con alertas reales (lotes 6.2 y 6.3), no como un p95 sobre muchas.
  El tramo que más varía es la política de Cloud Monitoring: de ~30 s a ~3,7 min entre el log y la notificación.
- **Punta a punta.** Con el exportador y el workflow forzados por la prueba de humo. Con los horarios normales (cada 15
  min y cada hora), el dato llega a Curated en ≤ 1 h 30 min y al tablero en ≤ 2 h 30 min (`status.py`).
- **Calidad.** Es la de Trusted, la «capa confiable» de README. Raw del stream tiene 0,0012 % de duplicados, del caos
  de §3.9, y Trusted los quita. Las 239 comprobaciones de Great Expectations están en `quality/` y en
  `ta_stream.dq_results`.

## 7. Limitaciones

- **Datos sintéticos.** Todo comentario y todo usuario son inventados; ninguna cifra describe a personas ni a marcas
  reales.
- **Comentarios de plantillas con vocabulario acotado.** La diversidad léxica es baja (profiling del 5.1, §3.20) y los
  textos se repiten.
- **Asociación marca → sentimiento artificial.** La eligen el simulador, el plan del histórico y las olas de demo
  (marcadas en `sim.scenario`). Las conclusiones de negocio no son válidas.
- **Métricas de NLP optimistas.** Accuracy 0,9197 y F1 macro 0,9167 sobre la verdad del simulador. Las plantillas son
  claras y repetitivas; con comentarios reales la precisión sería menor.
- **Un solo worker de Dataflow con NLP en CPU.** Alcanza para 1,5 a 2,5 mensajes/s. Con más carga, la marca de agua se
  atrasa y las alertas llegan tarde (§3.29).
- **Partes que esperan una decisión del dueño del proyecto:**
  - el CD desde GitHub, apagado (§3.30);
  - las etiquetas de asignación de costos de AWS (§3.31);
  - revocar un token de Mastodon que quedó en el historial público del repositorio (§9, riesgos).

## 8. Modelos de NLP y licencias

- `pysentimiento/robertuito-sentiment-analysis` (`NEG`/`NEU`/`POS`) y `pysentimiento/robertuito-emotion-analysis`
  (7 emociones). Los dos se descargan al construir la imagen `dataflow-nlp`, con su commit fijo (§3.15).
- Los modelos se entrenaron con el corpus de **TASS 2020** (IberLEF 2020). El de emoción usa la tarea 2, basada en
  **EmoEvent**.
- Sus *model cards* no declaran licencia. pysentimiento advierte que quedan sujetos a las licencias de los datos de
  entrenamiento, muchas de uso no comercial. El proyecto es **académico y sin uso comercial** (ADR-05).
- Citas que piden los autores:
  - Pérez, J. M., Rajngewerc, M., Giudici, J. C., Furman, D. A., Luque, F., Alemany, L. A. y Martínez, M. V. (2021).
    *pysentimiento: A Python Toolkit for Opinion Mining and Social NLP tasks*. arXiv:2106.09462.
  - Pérez, J. M., Furman, D. A., Alonso Alemany, L. y Luque, F. (2022). *RoBERTuito: a pre-trained language model for
    social media text in Spanish*. LREC 2022.
  - García-Vega, M. et al. (2020). *Overview of TASS 2020: Introducing Emotion Detection*. IberLEF 2020.
  - Plaza del Arco, F. M., Strapparava, C., Ureña-López, L. A. y Martín-Valdivia, M. T. (2020). *EmoEvent: A
    Multilingual Emotion Corpus based on different Events*. LREC 2020.

## 9. Cómo reproducir

| Qué | Comando (WSL, desde la raíz del repo) |
|---|---|
| Todo sin nube: las 1.667 pruebas (más las 18 de contratos) | `python -m pytest -c project/pytest.ini -q` (o la CI de GitHub, §3.30) |
| Encender y apagar | `python project/scripts/on.py` · `off.py` · `status.py` (`docs/ACTIVE_SERVICES.md`) |
| Latencia por etapa (HU-048) | `python project/scripts/smoke_e2e.py run` |
| Calidad y precisión del NLP | `python project/scripts/run_dq.py run` (venv `~/.venvs/ta-gx`) |
| Una alerta real con la ola de demo | `python project/scripts/validate_6_3.py flow` |
| La tabla de KPI de este informe | `python project/scripts/kpi_report.py --write` |
| La demo y su plan B | [`DEMO.md`](DEMO.md) y [`SUSTENTACION.md`](SUSTENTACION.md) |
| La operación | [`RUNBOOK.md`](RUNBOOK.md) |
