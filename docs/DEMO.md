# TrendAnalyzer · Guion de la demo de picos

> Guion del Lote 6.3 (HU-040): forzar en vivo una ola de comentarios negativos sobre una marca y mostrar cómo el sistema la detecta y avisa. Interfaces en `ARCHITECTURE.md` §3.29 y §3.29.1; validación en `docs/validation/validation-6-3.md`. El guion completo de la sustentación (HU-045, Lote 8.1) parte de este.

## Qué se muestra

Durante 5 minutos el simulador triplica los comentarios de Instagram y dos de cada tres son **negativos sobre Postobón**. El resto del tráfico (Pepsi, Sprite y la mezcla normal de Postobón) sigue igual. En unos 3 o 4 minutos:

1. Dataflow ve el pico en `trend_windows`: el volumen de Postobón en Instagram sube a varias veces su media y el sentimiento cae (z de volumen ≫ 3, z de sentimiento ≪ −3).
2. La rama de alertas publica `volume_up` y `sentiment_down` en `ta-alerts`.
3. El notificador lo registra y la política de Cloud Monitoring manda **un correo por alerta**, con asunto «[ALERT - Warning] TrendAnalyzer: volume_up de postobon en instagram».
4. Cada comentario de la ola lleva `sim.scenario = demo/postobon-negative/<hora>`: se distinguen los eventos forzados de los normales.

## Antes de empezar (T − 75 min)

Con un job de Dataflow recién lanzado **no hay alertas durante ≈ 66 min**: el z-score necesita 60 ventanas de baseline (§5.12). Hay que encender todo con tiempo.

| Cuándo | Qué | Comando (en WSL, `~/.venvs/ta-streaming`, desde la raíz del repo) |
|---|---|---|
| Una vez | Imagen del simulador con el escenario en `terraform.tfvars` | `python project/scripts/validate_6_3.py image` |
| T − 75 min | Encender el exportador, el simulador y Dataflow (≈ 0,35 USD/h mientras estén encendidos) | `python project/scripts/on.py` |
| T − 10 min | Comprobar que todo corre y que el baseline está listo | `python project/scripts/status.py` y `python project/scripts/demo_scenario.py status` |
| T − 5 min | Abrir las pestañas: BigQuery (`trend_windows_latest`), el correo, la política `ta-alerta-tendencia` en Cloud Monitoring y el tablero de QuickSight | — |

`demo_scenario.py status` debe responder `scenario_status` con `"active": null`. Si responde `scenario_error`, el simulador está apagado. Si responde `scenario_rejected` con 403, la cuenta de `gcloud` no puede invocar el servicio: tiene que ser la dueña del proyecto.

## La demo (T = 0)

| Minuto | Qué se hace | Qué se dice |
|---|---|---|
| 0:00 | Mostrar en BigQuery las últimas ventanas de `postobon`/`instagram`: ~70 posts por ventana de 5 min y sentimiento cerca de 0 | «Esto es el tráfico normal: el simulador emite medio comentario por segundo por red.» |
| 0:30 | `python project/scripts/demo_scenario.py start` | «Forzamos una crisis: durante 5 minutos, triple de comentarios en Instagram, dos de cada tres negativos sobre Postobón.» |
| 0:31 | Mostrar la respuesta (`scenario_started`: nombre, marca, ×3, fin) | «El endpoint no es público: Cloud Run exige el ID token de una cuenta autorizada.» |
| 2:00 | Repetir la consulta de BigQuery: las ventanas nuevas traen cientos de posts y sentimiento negativo, con `is_alert = true` | «Dataflow calcula el z-score contra la última hora: el volumen se dispara y el sentimiento se hunde.» |
| 3:00–4:30 | Abrir el correo (o el incidente en Cloud Monitoring) | «Del fin de la ventana a la notificación, menos de 5 minutos.» En la validación del 6.3 (29-oct-2026): 106,9 s desde el fin de la ventana, ≈ 3 min 26 s desde el `start` |
| 5:30 | `python project/scripts/demo_scenario.py status`: `active` vacío, `last.reason = expired` y cuántos eventos se forzaron | «El escenario termina solo; los eventos quedaron marcados en `sim.scenario`.» |
| después | El tablero de QuickSight muestra la ola en la siguiente hora: horario del lakehouse y refresco de SPICE, ambos cada hora | «La misma ola llega al lakehouse de AWS y al tablero; la ruta lenta es por diseño.» |

Consulta de BigQuery para los pasos 0:00 y 2:00 (proyecto `project-0568d9ba-848f-413c-88b`):

```sql
SELECT window_end, n_posts, ROUND(avg_sentiment, 2) AS sentimiento, ROUND(zscore_volume, 1) AS z_volumen,
       ROUND(zscore_sentiment, 1) AS z_sentimiento, is_alert
FROM `ta_stream.trend_windows_latest`
WHERE brand = 'postobon' AND source = 'instagram' AND window_end >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE)
ORDER BY window_end DESC
```

## Variantes

```bash
python project/scripts/demo_scenario.py start --brand pepsi --sentiment positive --multiplier 3 --duration 300 --source facebook
```

```bash
python project/scripts/demo_scenario.py stop
```

- `--source all` lleva la ola a las tres redes: dispara alertas en las tres claves, y Dataflow recibe ×3 en todas. Con un solo worker, mejor una red.
- `--multiplier` admite de 2 a 10 y `--duration` de 60 a 3600 s. **Recomendado: ×3 y 300 s.** Con ×3 en una red el job pasa de 1,5 a 2,5 mensajes/s. Con multiplicadores altos el NLP del único worker (`e2-standard-2`) se atrasa, y la marca de agua con él: la alerta llega más tarde.
- Solo hay un escenario a la vez: otro `start` responde 409 hasta que el primero termine o se pare con `stop`.
- El *cooldown* es de 15 min por marca × red × tipo. Repetir la misma ola antes de 15 min no manda otro correo: para un segundo pico, cambiar de marca o de red.

## Plan B

| Si… | Entonces |
|---|---|
| Dataflow lleva menos de 66 min encendido (sin baseline) | Mostrar la alerta sintética: `python project/scripts/alerts_check.py publish-test` publica en `ta-alerts` y el correo llega en ≈ 30 s (§3.28.1). Lanzar el escenario igual para mostrar el pico en `trend_windows`. |
| El correo tarda (la política tarda de 30 s a ~3,7 min) | Mostrar el incidente abierto en Cloud Monitoring → Alerting, o el log del notificador (`jsonPayload.alert="trend"`). |
| El simulador no responde | `python project/scripts/on.py` lo vuelve a crear (el cursor sigue en GCS). Un reinicio termina el escenario: lanzarlo otra vez. |
| La nube falla entera | Capturas y la ejecución grabada de la validación (`project/evidencia/validacion_6_3.json`: tiempos de cada paso), y el simulador local: `SIM_PUBLISHER=stdout SIM_STATE_BACKEND=local python -m simulator.app` con `demo_scenario.py start --url http://localhost:8080` (ver `project/LOCAL_DEV.md`). |

## Al terminar

```bash
python project/scripts/off.py
```

Drena Dataflow, apaga el simulador y pausa el exportador tras una última corrida. El notificador queda (escala a cero). Con el simulador encendido hay alertas naturales (unas pocas por hora) y cada una manda su correo: por eso no se deja encendido.
