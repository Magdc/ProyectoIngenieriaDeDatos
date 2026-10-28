# Validación del Lote 6.2 · HU-039

> **Alertas de tendencia** (ADR-32). Cuando una fila de `trend_windows` tiene `is_alert`, la rama nueva del job de
> Dataflow publica un mensaje `ta-alert` en `ta-alerts` (uno por métrica que pasa el umbral, con *cooldown* de 15 min
> por marca × fuente × tipo); la suscripción push `ta-alerts-notifier` lo entrega al servicio de Cloud Run
> `ta-alert-notifier`, que escribe un log `WARNING` con `alert = trend`, y la política basada en logs
> `ta-alerta-tendencia` manda el correo (y una copia a `ta-alerts-incidents`). Contrato en `docs/ARCHITECTURE.md`
> §3.28 (interfaces en §3.28.1), §5.3 y §5.12; guía de uso en `project/src/alerts/ALERTS.md`.
>
> **Automático:** `python project/scripts/validate_6_2.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_6_2.json` (sin
> direcciones de correo).
>
> **Cambia la nube** (con el OK del usuario del 27-oct-2026): `image` construye la imagen; `terraform --apply` crea 10
> recursos; `notify` publica una alerta de prueba (llega un correo «[prueba]»); `prepare` enciende el simulador y
> Dataflow (**cobran ≈ 0,35 USD/h hasta `off`**); `flow` publica una ola de 150 comentarios de prueba; `off` apaga.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-039 · Cuando `trend_windows.is_alert` es verdadero se publica un mensaje en `ta-alerts` | B2 (rama completa con `TestStream`), F4 (filas con `is_alert`) y F5 (mensajes de Dataflow en `ta-alerts`) |
| HU-039 · El mensaje dispara una notificación por correo (alerta basada en logs con canal email, §5.12) | C5 (política y canales), E3–E5 (log, notificación, asunto y cuerpo), F7, M1 (correo en el buzón) |
| HU-039 · La alerta incluye marca, fuente, ventana y magnitud del cambio | L2, E5, F6 |
| HU-039 · La detección se hace en < 5 min | F7 (fin de la ventana → notificación de la política) |
| HU-039 · *Cooldown* de 15 min por marca × fuente × tipo de alerta | B2, L3, F8 |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| `GCP_PROJECT` | `project-0568d9ba-848f-413c-88b` (`us-central1`) | Pub/Sub, Cloud Run, Monitoring, BigQuery, Dataflow y Terraform |

### Preparación

1. Terminal en WSL:
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   export GCP_PROJECT=project-0568d9ba-848f-413c-88b
   gcloud config get-value project
   ```
2. `git status --short` y `git log -1 --oneline`: el commit del lote en `main`, sin cambios.
3. Destinatarios: `alert_emails = ["…"]` en `project/infra/gcp/environments/dev/terraform.tfvars` (no se versiona). Si
   falta, la parte C pone el destinatario del presupuesto (canal `ta-budget-*`, el de §5.12).
4. Lo que se publicaría, sin nube:
   ```bash
   python project/scripts/alerts_check.py plan
   ```
   Esperado: una línea `alerts_plan` con `sample_problems: []` y la ola sin problemas de contrato.

### Ejecución

```bash
python project/scripts/validate_6_2.py preflight
python project/scripts/validate_6_2.py tests
python project/scripts/validate_6_2.py local
python project/scripts/validate_6_2.py image --yes                # Cloud Build, ≈ 40 s
python project/scripts/validate_6_2.py terraform --apply --yes    # 10 recursos
python project/scripts/validate_6_2.py notify --yes               # correo «[prueba]»
python project/scripts/validate_6_2.py prepare --yes              # simulador + Dataflow: cobran hasta el off
python project/scripts/validate_6_2.py flow --yes                 # espera ≈ 66 min el baseline, ola y 12 min de observación
python project/scripts/validate_6_2.py email --note "asunto … recibido a las …"   # lo que llegó, sin la dirección
python project/scripts/validate_6_2.py off --yes
python project/scripts/validate_6_2.py status
```

Salida esperada (resumen): cada parte termina con `-- Parte X: OK` (A con AVISO si falta `alert_emails`; E con E6 en
AVISO hasta registrar el correo con `email`) y código 0.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, B, L | Nada (A lee Dataflow, Cloud Run y Pub/Sub) | 0 |
| D | Cloud Build y Artifact Registry | Centavos por construcción |
| C | Terraform de `infra/gcp/environments/dev` con `backend.hcl` y `terraform.tfvars` | 0 (el notificador escala a cero) |
| E | Lo de C | 1 mensaje, 1 petición a Cloud Run, 1 correo |
| P, F | El simulador y Dataflow con NLP (P los enciende) | ≈ 0,35 USD/h hasta `off` |
| O | — | Apaga lo que cobra |

**Estado final:** notificador, push, canales y política creados (sin costo en reposo); Dataflow y el simulador
**apagados** (las alertas solo existen con Dataflow encendido); ninguna suscripción `ta-verify-*`.

---

## Parte A · Preflight

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 con Beam, Pub/Sub y jsonschema |
| A2 | Los 23 archivos del lote y los 7 que reutiliza |
| A3 | `gcloud`, `bq`, `terraform` y `bash` |
| A4 | Proyecto y ADC |
| A5 | Dataflow y simulador (informativo) y el topic `ta-alerts` (Lote 1.3) |
| A6 | `alert_emails` con al menos una dirección (AVISO si falta: C lo completa) |

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `pytest src/alerts`: 20 passed (notificador y contrato, comprobaciones y CLI, Terraform frente al código) |
| B2 | `pytest src/streaming/tests/test_alerts_pipeline.py`: 6 passed (configuración, candidatos, *cooldown*, rama completa con `TestStream`, enganche) |
| B3 | `pytest src/streaming/tests -k trends`: 151 passed (regresión del Lote 2.4) |
| B4 | `pytest src/ops/tests/test_ops_validate62.py`: 12 passed |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | `alerts_check.py plan`: alerta de ejemplo válida (`ta-alert` 1.0, 6 atributos) y ola de 150 comentarios `ta-verify@6.2` sin errores de envelope |
| L2 | El texto de la alerta lleva marca, fuente, ventana (UTC y Bogotá) y magnitud (valor, media, % y z) |
| L3 | Parámetros = §5.12: *cooldown* 900 s, \|z\| ≥ 3, `n_posts` ≥ 5, baseline 60, un correo cada 300 s como máximo |
| L4 | `build_alerts_image.sh --dry-run`: etiqueta `h-{huella}` y contexto de 8 archivos |

## Parte D · Imagen

| Paso | Esperado |
|---|---|
| D1 | `alert-notifier:h-{huella}` existe o se construye en Cloud Build con las pruebas del notificador dentro de la imagen (11 passed, 0 omitidas) |
| D2 | `alerts.auto.tfvars` con `alerts_notifier_image` (no se versiona) |

## Parte C · Terraform

| Paso | Esperado |
|---|---|
| C0 | `alert_emails` con N direcciones (sin mostrarlas) |
| C0b | Imagen presente (sin ella se crea todo menos el servicio, el invocador y la push) |
| C0c | Agente de notificaciones de Cloud Monitoring (`gcloud beta services identity create`, idempotente) |
| C1 | `infra/validate.sh` OK |
| C2 | Plan con `-target=module.sa_alerts -target=module.alerts -target=google_pubsub_topic_iam_member.dataflow_alerts_publisher`: hasta 10 a crear (`sa-alerts`, permiso de `sa-dataflow`, servicio, invocador, push, `ta-alerts-incidents`, canal de correo por dirección, canal de Pub/Sub, permiso del agente, política), 0 a destruir; o «ya aplicado» |
| C3–C4 | `--apply` y plan limpio después |
| C5 | Servicio listo y privado (solo `sa-alerts` invoca), push con el ID token de `sa-alerts`, `sa-dataflow` publica en `ta-alerts`, política habilitada con N + 1 canales y 1 notificación cada 300 s |

## Parte E · Alerta sintética

| Paso | Esperado |
|---|---|
| E1 | Suscripción temporal `ta-verify-incidents-{id}` a `ta-alerts-incidents` (se borra al final) |
| E2 | `alerts_check.py publish-test`: alerta `volume_up` con `origin = check` en `ta-alerts` |
| E3 | Log del notificador con esa alerta: severidad `WARNING` (en la entrada), `aviso.*` completo y el texto; segundos desde la publicación |
| E4 | Notificación de la política en `ta-alerts-incidents` con el `alert_id`; segundos del log a la notificación (< 7 min) |
| E5 | Asunto y cuerpo con marca, fuente, tipo y `alert_id`, sin variables `${…}` sin reemplazar |
| E6 | AVISO: comprobar el correo «[ALERT - Warning] TrendAnalyzer: volume_up de postobon en instagram» con «[prueba]» en el cuerpo; registrarlo con `email --note` |

## Parte P · Preparar

| Paso | Esperado |
|---|---|
| P1 | *Seek* de `ta-raw-*-df` a ahora (descarta el atraso) |
| P2 | Simulador en Cloud Run (Terraform vigilado de `ops/services.py`) y `ta-stream-dev` en *Running* con NLP; en la terminal, `alerts_config` con `"enabled": true` y `ta-alerts` |

## Parte F · Alerta real

| Paso | Esperado |
|---|---|
| F0 | Dataflow encendido, notificador desplegado y la parte P hecha |
| F1 | Baseline completo de `postobon`/`instagram`: espera hasta 66 min después de su primera ventana (`--max-wait-minutes`, 90 por defecto) |
| F2 | Suscripciones temporales a `ta-alerts` y `ta-alerts-incidents` |
| F3 | Ola de 150 comentarios negativos de `postobon` en `instagram` (`ta-verify@6.2`, escenario `alert-check-6.2/{id}`) en pocos segundos |
| F4 | `trend_windows_latest`: ventanas de la clave con `is_alert` y z de volumen ≥ 3 (varias: la ola cae en ventanas solapadas) |
| F5 | Mensajes de Dataflow (`origin = dataflow`) de la clave en `ta-alerts`, todos válidos; cuántos de otras claves |
| F6 | El primero lleva marca, fuente, ventana y magnitud (`alert_problems` vacío) |
| F7 | **Fin de la ventana → notificación < 300 s** (y cada tramo: Dataflow, `ta-alerts`, notificador); AVISO si la notificación no se vio (el límite de 1 cada 5 min pudo absorberla) |
| F8 | Un mensaje por tipo para la ola aunque alerten varias ventanas; ningún par de la misma clave y tipo a menos de 15 min entre todos los mensajes vistos |
| F9 | Informativo: alertas naturales de otras claves durante la observación y notificaciones de la política |

## Parte M · Correo

| Paso | Esperado |
|---|---|
| M1 | `email --note "…"` con lo que llegó (asunto, hora); FALLA si la nota trae una dirección (no se guarda) |

## Parte O · Apagado

| Paso | Esperado |
|---|---|
| O1 | Dataflow drenado y simulador apagado; horas y costo aproximado |
| O2 | Ninguna suscripción `ta-verify-*` |

---

## Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| C3: «Service account service-…@gcp-sa-monitoring-notification… does not exist» | El agente de Cloud Monitoring no existe (no lo crea el canal hecho por API) | C0c lo crea; repetir `terraform --apply` |
| C4: el plan propone quitar `scaling` del servicio | Bloque de escalado a nivel de servicio que devuelve la API | Ya ignorado (`ignore_changes`); si reaparece, revisar el proveedor |
| E3 FALLA con el log presente | `severity` se busca en `jsonPayload` | Cloud Logging la saca a la entrada (`_severity` en el validador) |
| E4 sin notificación | La política notifica como máximo una vez cada 5 min | Esperar 5 min y repetir `notify` |
| F1 se queda esperando | El simulador no corre o el job no procesa | `status`; `bash project/scripts/dataflow_job.sh status` |
| F5 sin mensajes y F4 con `is_alert` | Job lanzado con código anterior al 6.2 o sin permiso en `ta-alerts` | `off` y `prepare` otra vez; C5 |
| F7 AVISO | Otra alerta notificó en los 5 min anteriores | Ver F9; el log del notificador (F7, tramo «notificador») sigue midiendo la detección |

---

## Cierre

*(Se completa con la validación en la nube.)*
