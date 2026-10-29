# TrendAnalyzer · Guion de sustentación

> Lote 8.1 (HU-045). Demostración de punta a punta en ~20 min: simulador → Pub/Sub → Dataflow → BigQuery → S3 →
> Athena → QuickSight, con un pico y su alerta. Parte de [`DEMO.md`](DEMO.md) (el pico) y del informe
> [`INFORME.md`](INFORME.md) (las cifras). La operación está en [`RUNBOOK.md`](RUNBOOK.md). Al final está el registro
> del ensayo.

## Preparación

| Cuándo | Qué | Comando |
|---|---|---|
| El día antes | Todo verde sin nube y la CI de `main` en verde | `python -m pytest -c project/pytest.ini -q` · `python project/scripts/validate_7_1.py status` |
| T − 80 min | Encender todo: exportador, horario del lakehouse, simulador y Dataflow con NLP | `python project/scripts/on.py` |
| T − 70 min | Dataflow en Running. El baseline de las alertas está listo ≈ 66 min después de los primeros datos | `python project/scripts/status.py` |
| T − 10 min | El simulador responde y no hay escenario activo; el tablero de QuickSight abre | `python project/scripts/demo_scenario.py status` |
| T − 5 min | Pestañas abiertas: Cloud Monitoring («TrendAnalyzer · operación»), BigQuery (`trend_windows_latest`), S3 del lake, Athena (`ta-analytics`), QuickSight (`ta-tablero-tendencias`), el correo | — |

## Guion (T = 0)

| Min | Quién | Qué se muestra | Qué se dice |
|---|---|---|---|
| 0–2 | A | `INFORME.md` §1–§3: el diagrama | El problema, por qué las fuentes son simuladas (ADR-01) y las dos nubes |
| 2–4 | A | Logs del simulador en Cloud Run; mensajes por minuto en el tablero de Cloud Monitoring | Tres redes simuladas, ~1,5 mensajes/s, con identidad determinista y caos controlado |
| 4–6 | C | Dataflow: el grafo del job; `posts_enriched` en BigQuery con `user_hash`, sentimiento y emoción | Validación contra el contrato, DLQ, deduplicación, HMAC y NLP RoBERTuito. Ingesta en ~19 s (KPI) |
| 6–7 | C | `python project/scripts/demo_scenario.py start` | Forzamos una crisis: ola negativa de Postobón en Instagram, ×3 durante 5 min (§3.29) |
| 7–9 | M | GCS `enriched/` → S3 `raw/origin=stream/` (el exportador; mismos bytes) | El puente sin llaves: OIDC de GCP a AWS (§3.18) |
| 9–11 | M | Athena: `SELECT` sobre `ta_curated.fact_posts` y una consulta guardada | Lakehouse Raw → Trusted → Curated con Glue cada hora; calidad con Great Expectations (239 comprobaciones) |
| 11–13 | C | `trend_windows_latest`: la ventana de Postobón/Instagram con `is_alert = true` | El z-score contra la última hora detecta el pico |
| 13–14 | C | El correo de la alerta (o el incidente en Cloud Monitoring) | Del fin de la ventana a la notificación: 107–160 s medidos (< 5 min) |
| 14–17 | A | QuickSight: hojas «Tendencias» y «Relaciones» | Respuesta < 3 s con SPICE. El pico de hoy llega con el refresco de la hora (o con la ruta forzada, abajo) |
| 17–19 | M | `INFORME.md` §6 (KPI) y §7 (limitaciones); CI en GitHub; alertas de operación y de gasto | Qué se midió, qué no vale como conclusión de negocio y cómo se opera |
| 19–20 | Todos | Preguntas | — |

A, C y M son los responsables de las HU (`HU.md`). Se reparten como mejor les quede.

**Ruta forzada hasta el tablero** (opcional, si se quiere ver el pico en QuickSight durante la demo; ~10 min):

```bash
python project/scripts/smoke_e2e.py run
```

Corre el exportador y el workflow de Glue sin esperar los horarios. Después:

```bash
python project/scripts/quicksight.py refresh
```

Reimporta SPICE (~80 s). Luego se recarga el tablero.

## Plan B

| Si falla… | Entonces |
|---|---|
| La alerta no llega a tiempo (o Dataflow lleva menos de 66 min) | Alerta sintética, que llega en ≈ 30 s: `python project/scripts/alerts_check.py publish-test`. Mostrar el pico en `trend_windows` igual |
| QuickSight no abre | Capturas del tablero (las del ensayo) y las consultas de Athena equivalentes (`src/serving/sql/`) |
| La nube entera (red, cuotas) | **Ejecución grabada**: la evidencia de las validaciones (`project/evidencia/*.json` y `*.md`: la prueba de humo, la ola de demo y las alertas, con sus tiempos) y la grabación del ensayo, si se hizo. **Simulador local** sin nube (HU-047): `bash project/scripts/local_dev.sh up` y `verify` |
| El simulador no responde | `python project/scripts/on.py` (lo vuelve a crear; el cursor sigue en GCS) |

## Al terminar

```bash
python project/scripts/off.py
python project/scripts/status.py
```

Drena Dataflow, apaga el simulador y pausa el exportador. `status.py` debe decir «CORTADO en simulador».

## Registro del ensayo

<!-- ensayo:inicio · lo completa `python project/scripts/validate_8_1.py ensayo` -->
Pendiente: el ensayo completo en la nube necesita el OK del dueño del proyecto (encender ~1,5 h, ≈ 0,6 USD).
<!-- ensayo:fin -->
