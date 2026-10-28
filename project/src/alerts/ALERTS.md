# Alertas de tendencia (Lote 6.2, HU-039)

Guía de uso. El diseño y los contratos están en `docs/ARCHITECTURE.md` §3.28 (y §3.28.1), §5.3 (mensaje `ta-alert`),
§5.8 (`sa-alerts`) y §5.12 (cooldown, canal de correo). Validación: `docs/validation/validation-6-2.md`.

## Flujo

```
Dataflow ta-stream-dev
  ScoreTrends ─(is_alert)─► un mensaje ta-alert por métrica con |z| ≥ 3 ─► cooldown 15 min (marca × fuente × tipo)
    ─► Pub/Sub ta-alerts ─(push, ID token de sa-alerts)─► Cloud Run ta-alert-notifier
    ─► log WARNING alert=trend (jsonPayload) ─► política basada en logs ta-alerta-tendencia
    ─► correo (alert_emails) + copia en el topic ta-alerts-incidents
```

- La rama vive en `src/streaming/alerts.py` y se engancha sola desde `attach_trends` (no cambia `pipeline.py` ni
  `dataflow_job.sh`). Solo corre en Dataflow (`--alerts_enabled auto`).
- El contrato del mensaje está en `src/common/alert_message.py` (lo comparten Dataflow y el notificador).
- El notificador (`src/alerts/notifier.py`) solo usa la biblioteca estándar y escala a cero: no cuesta en reposo.
- Como máximo **un correo cada 5 min** (lo exige Cloud Monitoring para las políticas basadas en logs). Todas las alertas
  quedan en el log del notificador y en `ta-alerts` aunque no generen correo.

## Mensaje (`ta-alert` 1.0)

Cuerpo JSON con `alert_id` (uuid5 determinista), `alert_type` (`volume_up`, `volume_down`, `sentiment_up`,
`sentiment_down`), `brand`, `source`, `window_start`, `window_end`, `value`, `baseline_mean`, `baseline_std`, `zscore`,
`change`, `change_pct` (solo volumen), `n_posts`, `avg_sentiment`, `detected_at`, `cooldown_seconds` y `origin`
(`dataflow` o `check`). Atributos: `alert_id`, `alert_type`, `brand`, `source`, `window_end`, `schema_version`.

Texto del correo, por ejemplo:

> Volumen al alza de postobon en instagram: 190 posts frente a una media de 47,0 (+304 %, z = 12,0) en la ventana
> 2026-10-27 22:56–23:01 UTC (17:56–18:01 en Bogotá).

## Operación

```bash
# desde la raíz del repo, en WSL con ~/.venvs/ta-streaming
python project/scripts/alerts_check.py plan                     # sin nube: alerta de ejemplo y ola de prueba
python project/scripts/alerts_check.py publish-test             # alerta sintética [prueba] → correo
python project/scripts/alerts_check.py burst --count 150        # ola de prueba (Dataflow encendido y con baseline)
bash project/scripts/build_alerts_image.sh cloudbuild           # imagen alert-notifier (o validate_6_2.py image)
```

- **Encender las alertas** = encender Dataflow (`python project/scripts/on.py` o `validate_6_2.py prepare`). La primera
  alerta posible llega ≈ 66 min después de los primeros datos (baseline de 60 ventanas, §5.12).
- **Apagar** = drenar Dataflow (`off.py` o `validate_6_2.py off`). El notificador y la política se quedan.
- **Cambiar destinatarios**: `alert_emails` en `project/infra/gcp/environments/dev/terraform.tfvars` (no se versiona) y
  `validate_6_2.py terraform --apply`.
- **Ver las alertas**: Cloud Logging, servicio `ta-alert-notifier`, `jsonPayload.alert="trend"`; o
  `gcloud logging read 'resource.labels.service_name="ta-alert-notifier" AND jsonPayload.alert="trend"' --freshness=1d`.
- La ola de prueba usa `generator = ta-verify@6.2`: Trusted la descarta (no llega a Curated ni al tablero).
