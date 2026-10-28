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
| L3 | Parámetros = §5.12: *cooldown* 900 s, \|z\| ≥ 3, `n_posts` ≥ 5, baseline 60, límite de la política 300 s por incidente |
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
| C2 | Plan con `-target=module.sa_alerts -target=module.alerts -target=google_pubsub_topic_iam_member.dataflow_alerts_publisher`: hasta 10 a crear (`sa-alerts`, permiso de `sa-dataflow`, servicio, invocador, push, `ta-alerts-incidents`, canal de correo por dirección, canal de Pub/Sub, permiso del agente, política), 0 a destruir; solo se admite actualizar el servicio (imagen) y la política (texto); o «ya aplicado» |
| C3–C4 | `--apply` y plan limpio después |
| C5 | Servicio listo y privado (solo `sa-alerts` invoca), push con el ID token de `sa-alerts`, `sa-dataflow` publica en `ta-alerts`, política habilitada con N + 1 canales y límite de 300 s (por incidente) |

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
| F7 | **Fin de la ventana → notificación < 300 s** (y cada tramo: Dataflow, `ta-alerts`, notificador); AVISO si la notificación no se vio en la observación pero el notificador sí midió < 300 s |
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
| E4 sin notificación | La política recién creada tardó 138 s la primera vez; o el mensaje del canal no se pudo leer | Esperar y repetir `notify`; la métrica `send_message_operation_count` de `ta-alerts-incidents` cuenta las notificaciones |
| F1 se queda esperando | El simulador no corre o el job no procesa | `status`; `bash project/scripts/dataflow_job.sh status` |
| F5 sin mensajes y F4 con `is_alert` | Job lanzado con código anterior al 6.2 o sin permiso en `ta-alerts` | `off` y `prepare` otra vez; C5 |
| F9 cuenta menos notificaciones que alertas | Antes de v2.50: `data` de gcloud en base64 URL-safe | Corregido (`b64_any`); para contar todas, la métrica del topic de incidentes |
| La observación de F dura mucho más que `--watch-minutes` | Antes de v2.50: cada `pull` vacío de gcloud espera ~20 s | Corregido (la corta el reloj) |

---

## Cierre

**Lote 6.2 cerrado (v2.50).** Validado el 27/28-oct-2026 en WSL (`ta-streaming`) sobre `main` = `b27a1d6` más las
correcciones de abajo, con el OK del usuario para la nube (apply y validación con el simulador y Dataflow).

| Parte | Resultado |
|---|---|
| A | AVISO solo en A6 (`alert_emails` vacío; C lo completó con el destinatario del presupuesto) |
| B | OK: 20, 6, 151 y 12 passed, 0 omitidas |
| L | OK (4): alerta de 569 bytes, ola de 150 comentarios sin errores de envelope, parámetros = §5.12, contexto de la imagen de 8 archivos |
| D | OK: `alert-notifier:h-e9a8dab903df` en 39 s, 11 pruebas dentro de la imagen |
| C | OK tras dos correcciones: 10 recursos creados (8 + 2 después de crear el agente de Cloud Monitoring); plan limpio; recursos reales comprobados. Después, 1 cambio en el lugar aplicado con el OK del usuario (texto del correo, abajo) y plan limpio |
| E | OK en la segunda corrida: publicación → log del notificador 0,8 s → notificación 28,5 s (la primera vez, con la política recién creada, 139 s); asunto «[ALERT - Warning] TrendAnalyzer: volume_up de postobon en instagram». E6 (buzón): sin confirmar |
| P | OK: *seek* a 22:24:36 UTC; simulador y `ta-stream-dev` con NLP y `alerts_config` activa |
| F | OK (10), segunda corrida (la primera quedó cortada al terminar la sesión, sin dejar suscripciones). Ola a las 23:39:54 UTC; 5 ventanas con `is_alert` (z de volumen 9,28 → 3,63); 2 mensajes de la ola (`volume_up`, `sentiment_down`) y 3 naturales; **fin de la ventana → Dataflow 63,9 s → `ta-alerts` 64,6 s → notificador 69,0 s → notificación 159,8 s**; 1 mensaje por tipo aunque alertaron 5 ventanas (logs `trend_alert_suppressed` en el job) |
| O | OK: Dataflow drenado y simulador apagado; ≈ 1,9 h encendidos ≈ 0,65 USD (estimado) |
| status | OK (3) |

**Métrica del topic de incidentes** (`pubsub.googleapis.com/topic/send_message_operation_count`, 22:00–00:20 UTC):
7 notificaciones para los 7 mensajes de `ta-alerts`. El límite de 300 s de la política es por incidente: **un correo por
alerta**.

**Hallazgos (corregidos en v2.49 o v2.50):** el agente de notificaciones de Cloud Monitoring no se crea con el canal
(C0c lo crea); `scaling` a nivel de servicio (ignorado); `severity` fuera del `jsonPayload` (E3); `data` de gcloud en
base64 URL-safe (F9 vio 1 notificación de 7); `pull` vacío de ~20 s (la observación duró 34 min); y el texto del correo,
que prometía «como máximo un correo cada 5 min»: cambiado en el módulo a «un correo por alerta» y **aplicado**
con `validate_6_2.py terraform --apply` (1 recurso, la política, en el lugar; plan limpio y C5 OK después).

**Sin comprobar:** que el correo llegue al buzón (el validador no lo puede leer; sale en el mismo incidente que la copia
de Pub/Sub, que sí se midió); la plantilla Flex con la rama de alertas (no se reconstruyó).
