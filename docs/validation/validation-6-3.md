# Validación del Lote 6.3 · HU-040

> **Escenario de demo** (ADR-33). `POST /control/scenario` en el simulador sube la cadencia de una red ×`multiplier`
> durante `duration_s` y fuerza marca y sentimiento **en lo que se suma** (fracción `1 − 1/m` de los eventos). El resto
> del tráfico sigue igual. Cada evento forzado lleva `sim.scenario`. En Cloud Run el endpoint exige ID token. Cliente y
> guion: `project/scripts/demo_scenario.py` y `docs/DEMO.md`. Contrato en `docs/ARCHITECTURE.md` §3.29 (interfaces en
> §3.29.1).
>
> **Automático:** `python project/scripts/validate_6_3.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_6_3.json`.
>
> **Cambia la nube** (pedir el OK del usuario antes de `image`, `prepare` y `flow`):
> - `image` construye `stream-simulator:h-{huella}` en Cloud Build (corre las pruebas dentro de la imagen) y la pone
>   en `terraform.tfvars` (no se versiona).
> - `prepare` es la del 6.2: descarta con *seek* lo pendiente en `ta-raw-*-df` (irreversible) y enciende el simulador
>   con la imagen nueva (Terraform vigilado de `ops/services.py`: solo Cloud Run) y Dataflow con NLP. Cobra ≈ 0,35 USD/h
>   hasta `off`.
> - `flow` espera el baseline (≈ 66 min), lanza un escenario de 5 min (Postobón negativa en Instagram, ×3) y deja
>   suscripciones temporales `ta-verify-*` que borra al terminar. Las alertas mandan correo (una por alerta).
> - `off` drena Dataflow y apaga el simulador.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-040 · `POST /control/scenario` con `brand`, `sentiment`, `multiplier`, `duration_s` y `source` (opcional) fuerza marca y sentimiento al generador y sube la frecuencia durante ese tiempo | B1 (pruebas), L1 (ola en tiempo virtual con el bucle real), L2 (API), S5 (400 con los problemas), F3 (201 en Cloud Run), F11 (cadencia ×m y fracción marcada en `ta-raw-*`), F10 (termina solo) |
| HU-040 · Los eventos generados durante el escenario quedan marcados en `sim.scenario` | L1, F11 (todos los marcados con la marca, el sentimiento, el nombre y `comment-gen@1.0`; ninguno antes ni después), F12 (logs `scenario_start`/`scenario_end`) |
| HU-040 · El endpoint está protegido con ID token de Cloud Run (no es público) | B2 (Terraform sin `allUsers` ni `allAuthenticatedUsers`), S2 (IAM real), S3 (401/403 sin token), S4 (200 con token) |
| HU-040 · Existe un guion de demo documentado | `docs/DEMO.md`; F4–F9 (lo que el guion promete: pico en `trend_windows`, alerta, notificación < 5 min, *cooldown*) |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| ADC de GCP | `~/.config/gcloud/application_default_credentials.json` | Pub/Sub y BigQuery desde Python |
| `gcloud` con sesión de la dueña del proyecto | Proyecto `project-0568d9ba-848f-413c-88b` (o `--project`) | Cloud Build, Cloud Run, ID token (`gcloud auth print-identity-token`), Dataflow, *seek* |

### Preparación

1. Terminal en WSL:
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   ```
2. `git status --short` y `git log -1 --oneline` (con el git de Windows): el commit del lote en `main`, sin cambios.
   La etiqueta de la imagen es la huella de su contenido: mismo código ⇒ misma etiqueta, y D no reconstruye.
3. La ola sin nube:
   ```bash
   python project/scripts/demo_scenario.py check
   ```
   Esperado: una línea `scenario_check` con `"problems": []` y `"api_problems": []`.

### Ejecución

```bash
python project/scripts/validate_6_3.py preflight
python project/scripts/validate_6_3.py tests
python project/scripts/validate_6_3.py local
python project/scripts/validate_6_3.py image --yes     # Cloud Build, ~3–5 min
python project/scripts/validate_6_3.py prepare --yes   # seek + simulador con la imagen nueva + Dataflow (5–10 min hasta Running)
python project/scripts/validate_6_3.py auth
python project/scripts/validate_6_3.py flow --yes      # espera el baseline (≈ 66 min) + 2 min + 5 min de ola + 8 min
python project/scripts/validate_6_3.py off --yes       # drena Dataflow y apaga el simulador (hasta 15 min; --force cancela)
python project/scripts/validate_6_3.py status
```

Opciones de `flow` (y de `local`): `--brand`, `--sentiment`, `--multiplier`, `--duration`, `--source`,
`--max-wait-minutes` (90) y `--watch-minutes` (8). Si `flow` falla o se corta, **corra igual `off --yes`**.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, B, L | Nada encendido (A lee el estado de GCP) | 0 |
| D | `gcloud` | Una construcción de Cloud Build (centavos o dentro de la cuota gratuita) |
| P | La imagen de D en `terraform.tfvars`, Terraform inicializado en `infra/gcp/environments/dev`, Beam en `ta-streaming` | Simulador ≈ 0,10 USD/h + Dataflow ≈ 0,25 USD/h hasta O |
| S | El simulador encendido (P) | 0 (unas peticiones) |
| F | P, el notificador y la política del 6.2 | 3 suscripciones temporales; consultas de BigQuery (< 1 centavo); correos de las alertas |
| O | `gcloud`, Terraform | Deja de cobrar |

**Estado final:** Dataflow y simulador apagados, `simulator_image` con la etiqueta `h-{huella}` del código actual en `terraform.tfvars` (la
próxima vez que se encienda el simulador ya trae el escenario), notificador escalado a cero, sin `ta-verify-*`.

---

## Parte A · Preflight

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 con Beam, Pub/Sub, jsonschema y PyYAML |
| A2 | Los 12 archivos del lote y los 7 que reutiliza |
| A3 | `gcloud`, `bq`, `terraform`, `bash` |
| A4 | Proyecto de GCP y ADC presentes |
| A5 | Estado de Dataflow y del simulador; `simulator_image` presente en `terraform.tfvars` |
| A6 | El notificador y la política del 6.2 (sin ellos, F no mide la notificación: AVISO) |

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `src/common src/simulator`: **137** passed, 0 omitidas (lo mismo que corre la imagen; incluye `test_scenario.py` y `test_app.py` con la API) |
| B2 | `src/demo`: **9** passed (cliente, CLI con y sin token, Terraform sin acceso público, API con el servidor real, ola en tiempo virtual) |
| B3 | `test_ops_validate63.py`: **10** passed |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | `demo_scenario.py check`: 5 min normales, 5 de escenario (×3) y 5 normales en tiempo virtual, con el `SourceWorker` real. Cadencia durante/antes ≈ 3 (0,75–1,25 × m); fracción marcada ≈ 67 % (± 8 pp); tráfico normal igual (± 40 %); ningún marcado fuera del escenario; todos los marcados `postobon`/`negative`/nombre del escenario; 0 errores de contrato; el escenario termina por `expired` |
| L2 | API con el servidor HTTP del simulador en `127.0.0.1`: `/health` 200, `GET` 200 sin escenario, `POST` inválido 400 con 2 problemas, `POST` 201, otro `POST` 409, `GET` y `/health` con el escenario, `DELETE` 200 (`stopped`), `GET` después 200 (`last.reason = stopped`), ruta desconocida 404 |

## Parte D · Imagen

| Paso | Esperado |
|---|---|
| D1 | Etiqueta `h-{huella}`: sha256 de las rutas y el contenido (con LF) de los archivos que entran a la imagen (`pytest.ini`, `contracts/`, `src/conftest.py`, `src/common/`, `src/simulator/`, `scripts/generate_comments.py`), 12 hex. No sale de git: el git de WSL no lee el checkout de Windows |
| D2 | La imagen ya existe en `ta-images/stream-simulator` (no se reconstruye) o Cloud Build la construye: la etapa `test` corre las 137 pruebas y falla si alguna se omite |
| D3 | `simulator_image` en `terraform.tfvars` apunta a la etiqueta nueva (la anterior queda en la evidencia) |

## Parte P · Preparar la nube

La del 6.2 (`validate62.part_prepare`):

| Paso | Esperado |
|---|---|
| P1 | Con Dataflow apagado: *seek* de `ta-raw-{reddit,facebook,instagram}-df` a la hora actual. Con Dataflow ya encendido: OMITIDO |
| P2 | `cloud_on`: Terraform crea o actualiza **solo** el servicio de Cloud Run del simulador con la imagen de D; Dataflow `ta-stream-dev` con NLP en Running |

## Parte S · Acceso

| Paso | Esperado |
|---|---|
| S1 | La imagen desplegada en `ta-stream-simulator` = `simulator_image` de `terraform.tfvars` |
| S2 | La política IAM del servicio no tiene `allUsers` ni `allAuthenticatedUsers` |
| S3 | `demo_scenario.py status --no-token` → **401 o 403** (lo responde el *frontend* de Cloud Run; el contenedor no lo ve) |
| S4 | `demo_scenario.py status` (con el ID token de `gcloud auth print-identity-token`) → 200 con `active` y `last` |
| S5 | `POST` con `brand = cocacola` y `multiplier = 50` → 400 con los 2 problemas (no se emite nada) |

## Parte F · Escenario real

| Paso | Esperado |
|---|---|
| F0 | Dataflow y simulador encendidos y la parte P hecha |
| F1 | Baseline completo de `postobon`/`instagram`: 66 min desde su primera ventana |
| F2 | Suscripciones temporales a `ta-raw-instagram`, `ta-alerts` y `ta-alerts-incidents` (`ta-verify-*`) |
| F3 | Tras 2 min de cadencia normal, `demo_scenario.py start` → 201 con el nombre, ×3 y la hora de fin |
| F4–F9 | Los del 6.2 (`validate62.flow_report`): F4 ventanas con `is_alert` de volumen (y de sentimiento) después del inicio; F5 mensajes de Dataflow de esa clave (`volume_up`, `sentiment_down`); F6 contenido; **F7 fin de la ventana → notificación < 300 s**; F8 un mensaje por tipo (*cooldown*); F9 otras alertas |
| F10 | Después de `duration_s`, `GET` → `active` vacío y `last.reason = expired`, con los eventos forzados |
| F11 | En `ta-raw-instagram` (sin malformados ni repetidos): cadencia durante/antes ≈ m (0,7–1,3 × m); fracción marcada ≈ 1 − 1/m (± 10 pp); ningún marcado antes ni después; todos con la marca, el sentimiento, el nombre y `comment-gen@1.0`; tráfico normal igual (± 40 %) |
| F12 | Cloud Logging del simulador: un `scenario_start` y un `scenario_end` con el nombre (AVISO si el log tarda) |

## Parte O · Apagado

La del 6.2 (`validate62.part_off`): O1 Dataflow drenado y simulador apagado, con horas y costo estimado; O2 sin
`ta-verify-*`.

## Status

| Paso | Esperado |
|---|---|
| S1 | Dataflow y simulador apagados |
| S2 | `simulator_image` = la etiqueta del código actual |
| S3 | La última ola medida: ×m y segundos hasta la notificación |

## Cierre

**Lote 6.3 cerrado (v2.53).** Validado el 29-oct-2026 en WSL (`ta-streaming`) sobre `main` = `0511e44`, con el OK del
usuario para la nube.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (6): dependencias; 12 + 7 archivos; ADC; Dataflow y simulador apagados; notificador y política del 6.2 |
| B · Pruebas | OK (3): B1 137, B2 9, B3 10 |
| L · Local | OK (2): ×2,88 con m = 3, 67 % marcados, tráfico normal igual, 0 errores de contrato, fin por `expired`; API 10/10 |
| D · Imagen | OK (3): `stream-simulator:h-6fae173050de` (123 archivos) en Cloud Build en 100 s, con 137 pruebas dentro; `simulator_image` actualizada |
| P · Preparar | OK (2): *seek* a 00:17:37 UTC; Terraform creó solo el servicio del simulador; Dataflow `2026-10-28_17_19_15-14648965928001110752` con NLP |
| S · Acceso | OK (5): imagen desplegada = `terraform.tfvars`; IAM sin enlaces públicos; **403 sin token**; 200 con token; 400 con los 2 problemas |
| F · Escenario real | OK (13): baseline listo a las 01:20 UTC; `demo/postobon-negative/20261029T012221Z` (×3, 01:22:21–01:27:21); 5 ventanas con `is_alert` de volumen (z máx. 7,5) y 5 de sentimiento; `volume_up` y `sentiment_down`; «171 posts frente a una media de 67,2 (+154 %, z = 6,4)»; **fin de la ventana → Dataflow 40,8 s → `ta-alerts` 41,6 s → notificador 45,6 s → notificación 106,9 s**; 1 mensaje por tipo (*cooldown*); 258 eventos forzados y fin por `expired`; en `ta-raw-instagram` 29,0 → 83,4 → 30,3 eventos/min (×2,88), 62 % marcados, todos `postobon`/`negative`/`comment-gen@1.0`; logs `scenario_start` y `scenario_end` |
| O · Apagado | OK (2): Dataflow drenado y simulador apagado (≈ 1,35 h ≈ 0,47 USD); sin `ta-verify-*` |
| `status` | OK (3) |

Cifras también en `ARCHITECTURE.md` §3.29, `CHANGELOG.md` v2.53 y `project/evidencia/validacion_6_3.json`. Nada quedó
encendido; el correo en el buzón no se comprobó (el validador mide la copia de Pub/Sub de cada notificación).

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| S3 da 200 | Alguien dio `roles/run.invoker` a `allUsers`: `gcloud run services get-iam-policy ta-stream-simulator --region us-central1` y quitarlo (S2 lo muestra) |
| S4 da 403 | La cuenta de `gcloud` no tiene `run.routes.invoke` en el proyecto: usar la dueña del proyecto (`gcloud auth login`) |
| S1 FALLA (imagen vieja) | Repetir `prepare`: `cloud_on` aplica el cambio de imagen (solo Cloud Run) |
| F3 da 409 | Hay un escenario activo: `python project/scripts/demo_scenario.py stop` y repetir |
| F7 > 300 s | Con multiplicadores altos el NLP se atrasa (un solo worker): repetir con `--multiplier 3` o menos |
| F11 con pocos eventos | Un `pull` de `gcloud` trae hasta 1.000 mensajes: si el escenario fue muy largo o muy intenso, la suscripción pudo quedarse con mensajes (se borran con ella) |
| `off` «sigue drenando» | `python project/scripts/validate_6_3.py off --yes --force` |
