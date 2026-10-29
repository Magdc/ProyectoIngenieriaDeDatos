# Validación del Lote 7.2 · HU-042

> **Observabilidad** (ADR-36). Cloud Monitoring: el tablero «TrendAnalyzer · operación» tiene 8 gráficos. Muestran la
> tasa de mensajes de `ta-raw-*`, el atraso de `ta-raw-*-df`, la **latencia p95** de lectura de Dataflow (`ack_latencies`,
> una distribución), la edad de la marca de agua y el retraso de Dataflow, la **DLQ**, las alertas de tendencia y el
> exportador. Hay también **4 alertas de operación**: Dataflow fallido, latencia (marca de agua > 5 min), DLQ creciente
> (> 30 en 10 min) y exportador fallido, por los canales del 6.2. AWS: una regla de EventBridge guarda **todos los estados
> de los jobs de Glue**, el tablero `ta-lakehouse` los muestra y la alarma `ta-glue-job-fallido` avisa por SNS
> (`ta-observabilidad`). Los logs de cada componente son JSON. Contrato en `docs/ARCHITECTURE.md` §3.32 (interfaces en
> §3.32.1).
>
> **Automático:** `python project/scripts/validate_7_2.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming`. Evidencia en `project/evidencia/validacion_7_2.json`.
>
> **Cambia la nube** (con el OK del usuario):
> - `terraform --apply` crea el tablero, las 4 alertas y lo de AWS. El mismo plan trae lo del 7.3: GCP 7 a crear y
>   1 a cambiar; AWS 8 a crear y 29 a cambiar; 0 a borrar.
> - `prepare` enciende el simulador y Dataflow (≈ 0,35 USD/h hasta `off`).
> - `fire` publica 40 mensajes malformados en `ta-raw-reddit` y baja el umbral de latencia a 1 s con un `apply`
>   dirigido, que después se devuelve.
> - `fire` también corre `ta-job-trusted-to-curated` una vez con un bucket inexistente para que falle (≈ 0,0005 USD;
>   no escribe nada).
> - Cada alerta disparada manda un correo.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-042 · Cloud Monitoring muestra tasa de mensajes, latencia p95 y tamaño de la DLQ | L1 (en el módulo), K1 (el tablero en la nube), M1–M5 (cada gráfico con datos con Dataflow encendido) |
| HU-042 · CloudWatch muestra el estado de los jobs de Glue | L2, K3 (tablero `ta-lakehouse`), K5 (regla de estados), F5 (una corrida aparece) |
| HU-042 · Alertas para fallos del pipeline, latencia fuera del objetivo y DLQ creciente | L1, K2, K4; F2 (DLQ creciente, de verdad), F3 (latencia, con el umbral bajado), F6 (fallo de Glue, de verdad) |
| HU-042 · Los logs son JSON a stdout | L3 (cada componente en el código), K6 (stdout de los servicios de Cloud Run en Cloud Logging) |

## Cómo ejecutarlo

```bash
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
source ~/.venvs/ta-streaming/bin/activate
python project/scripts/validate_7_2.py preflight
python project/scripts/validate_7_2.py tests
python project/scripts/validate_7_2.py local
python project/scripts/validate_7_2.py terraform --apply   # con el OK del usuario
python project/scripts/validate_7_2.py cloud
python project/scripts/validate_7_2.py prepare --yes       # simulador y Dataflow (cobran hasta off)
python project/scripts/validate_7_2.py metrics             # tras ~15 min con Dataflow en Running
python project/scripts/validate_7_2.py fire --yes          # ~30 min
python project/scripts/validate_7_3.py fire --yes          # (7.3) las alertas de gasto, con todo encendido
python project/scripts/validate_7_2.py off --yes
python project/scripts/validate_7_2.py status
```

## Parte A · Preflight

A1 los 7 archivos del lote · A2 `gcloud`, `aws`, `terraform` y `google-cloud-monitoring` · A3 estado de Dataflow y del simulador.

## Parte B · Pruebas

B1 `test_ops_validate72.py`: **6** passed (los módulos reales cumplen HU-042; logs JSON en el código; la parte M con
la API de Monitoring falsa).

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | El módulo `observability` tiene la tasa de mensajes, `ack_latencies` con `REDUCE_PERCENTILE_95`, la marca de agua, la DLQ (`ta-dlq`) y las 4 alertas con canales |
| L2 | El módulo `lakehouse_observability`: regla de estados de Glue, tablero, alarma y suscripción de correo |
| L3 | Simulador, notificador, exportador, pipeline y jobs de Glue escriben JSON (en Dataflow, por `logging`) |
| L4 | `infra/validate.sh` sin errores ni avisos |

## Parte C · Terraform

C1 GCP: 7 a crear (4 de este lote, 2 del 7.3 y el tablero), 1 a cambiar; C2 AWS: 8 a crear, 29 a cambiar (etiquetas
del 7.3); **0 a borrar**. Con `--apply`, «Apply complete» y plan limpio después.

## Parte K · La nube

| Paso | Esperado |
|---|---|
| K1 | «TrendAnalyzer · operación» con 8 gráficos |
| K2 | Las 4 alertas `ta-op-*` habilitadas, con el correo y la copia en Pub/Sub |
| K3 | `ta-lakehouse` con 4 widgets |
| K4 | `ta-glue-job-fallido` con acción en `ta-observabilidad`; suscripciones de correo (si `alarm_emails` tiene direcciones, cada una queda «PendingConfirmation» hasta que se confirma en el correo de AWS) |
| K5 | Regla `ta-glue-job-estados` en ENABLED |
| K6 | El stdout de `ta-stream-simulator`, `ta-alert-notifier` y `ta-gcs-to-s3` en Cloud Logging es JSON (`jsonPayload`) |

## Parte M · Métricas con datos

Con Dataflow en Running unos 15 min: M1 tasa de `ta-raw-*`, M2 atraso de `ta-raw-*-df`, M3 latencia p95 de lectura,
M4 marca de agua y M5 retraso del sistema, todas con puntos en los últimos 30 min.

## Parte F · Alertas de verdad

| Paso | Esperado |
|---|---|
| F1–F2 | 40 mensajes malformados → `ta-dlq` → notificación de `ta-op-dlq-creciente` (en ≤ 15 min) |
| F3–F4 | Umbral de latencia a 1 s → notificación de `ta-op-latencia` → umbral de vuelta a 300 s |
| F5–F6 | `ta-job-trusted-to-curated` con un bucket inexistente → FAILED → `ta-glue-job-fallido` en ALARM |

## Parte O · Apagado

La del 6.2: Dataflow drenado, simulador apagado, sin `ta-verify-*`.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| M sin datos | Las métricas de Dataflow tardan unos minutos en aparecer después de Running; repetir `metrics` |
| K4 sin suscripciones | `alarm_emails` vacío en `infra/aws/environments/dev/terraform.tfvars`: agregar la dirección y aplicar; luego confirmar el correo de AWS |
| F6 no pasa a ALARM | El filtro de métricas cuenta los eventos de `/aws/events/ta-glue-job-fallos`: revisar que la regla `ta-glue-job-fallos` (4.1) siga ENABLED |
