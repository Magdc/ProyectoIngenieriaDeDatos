# Validación del Lote 7.3 · HU-043

> **Control de costos** (ADR-35). Encender y apagar ya estaba en `on.py`, `off.py` y `status.py` (v2.51). Este lote
> suma **dos alertas de gasto por olvido** en Cloud Monitoring: Dataflow `ta-stream-*` con más de 4 h de vida y el
> simulador con instancias más de 8 h seguidas, con los canales de las alertas del 6.2 (correo y copia en
> `ta-alerts-incidents`). También pone la **etiqueta `component`** en todo lo de AWS, con un proveedor por
> componente y `default_tags`, y en Artifact Registry. Además comprueba en la nube los presupuestos (Lote 1.0), el
> Dataflow de 1 worker y los ciclos de vida de §5.12. Contrato en `docs/ARCHITECTURE.md` §3.31 (interfaces en
> §3.31.1).
>
> **Automático:** `python project/scripts/validate_7_3.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming`. Evidencia en `project/evidencia/validacion_7_3.json`.
>
> **Cambia la nube** (con el OK del usuario): `terraform --apply` crea 2 políticas de alerta y actualiza etiquetas
> en el lugar (GCP: 2 a crear y 1 a cambiar; AWS: 29 a cambiar, 0 a borrar). `fire` necesita Dataflow y el
> simulador encendidos (los del 7.2): baja los umbrales a 6 min con dos `apply` dirigidos a `module.cost_guard` y
> los devuelve a 4 h y 8 h.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-043 · Scripts de encendido y apagado de Dataflow y del simulador | L2 (`on.py`, `off.py`, `status.py`) y B2 (sus 36 pruebas) |
| HU-043 · Alertas de presupuesto en GCP y AWS | K1 y K2 (presupuestos del Lote 1.0 en la nube); K6 y F (las dos alertas de gasto por olvido de este lote) |
| HU-043 · Dataflow con `maxNumWorkers` de 1 a 2 | L1 (`--num_workers 1 --max_num_workers 1 --autoscaling_algorithm NONE`) |
| HU-043 · Las etiquetas permiten desglosar costos por componente | L3, L4 (en el código), K4 y K5 (en la nube) |
| HU-043 · Ciclos de vida: GCS `raw/` y `dlq/` a 30 días, partición de `posts_enriched` a 90, `athena-results/` a 7 | L5 (código) y K3 (lo aplicado) |

## Cómo ejecutarlo

```bash
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
source ~/.venvs/ta-streaming/bin/activate
python project/scripts/validate_7_3.py preflight
python project/scripts/validate_7_3.py tests
python project/scripts/validate_7_3.py local
python project/scripts/validate_7_3.py terraform            # solo el plan
python project/scripts/validate_7_3.py terraform --apply    # con el OK del usuario
python project/scripts/validate_7_3.py cloud
python project/scripts/validate_7_3.py fire --yes           # con Dataflow y el simulador encendidos (7.2)
python project/scripts/validate_7_3.py status
```

## Preflight y apagado

Nada que encender para A, B, L, C ni K. `fire` usa el Dataflow y el simulador que enciende `validate_7_2.py prepare`
y los apaga `validate_7_2.py off`. Las dos políticas nuevas no cobran (Cloud Monitoring no cobra las políticas de
alerta de métricas del sistema en este volumen).

## Parte A · Preflight

A1 los 6 archivos del lote · A2 `gcloud`, `aws`, `terraform`, `bq` · A3 estado de Dataflow y del simulador.

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `test_ops_validate73.py`: **9** passed |
| B2 | `test_ops_services.py`: **36** passed (`on.py`, `off.py`, `status.py`) |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | `dataflow_job.sh`: `num_workers 1`, `max_num_workers 1`, `autoscaling_algorithm NONE` |
| L2 | `on.py`, `off.py`, `status.py` presentes |
| L3 | Cada `module` de `infra/aws/environments/dev` con `providers = { aws = aws.<componente> }` (`lake`, `historical`, `exporter`, `lakehouse`, `serving`, `cicd`) |
| L4 | En `infra/gcp/environments/dev`, todas las `labels` con `component` (Pub/Sub la pone su módulo: `ingest`) |
| L5 | Ciclos de vida en el código = §5.12: `raw/` 30, `dlq/` 30, `posts_enriched` 90, `athena-results/` 7 |
| L6 | `infra/validate.sh` sin errores ni avisos |

## Parte C · Terraform

| Paso | Esperado |
|---|---|
| C1 | GCP: **2 a crear** (`ta-gasto-dataflow-encendido`, `ta-gasto-simulador-encendido`), **1 a cambiar** (etiqueta de `ta-images`), 0 a borrar |
| C2 | AWS: **29 a cambiar** (etiquetas en el lugar; la política del bucket y la de logs se recalculan con el mismo contenido porque sus *data sources* cambian de proveedor), 0 a crear, 0 a borrar |
| C1a/C2a | Con `--apply`: «Apply complete» |
| C1b/C2b | Plan sin cambios después |

## Parte K · La nube

| Paso | Esperado |
|---|---|
| K1 | Presupuesto de GCP del Lote 1.0 con sus umbrales (50 %, 90 %, 100 % y el de pronóstico) |
| K2 | Presupuesto de AWS `ta-*` de 50 USD |
| K3 | GCS `raw/` y `dlq/` a 30 días, `posts_enriched` a 90, S3 `athena-results/` a 7 |
| K4 | Recursos de AWS con `project = trend-analyzer` agrupados por `component` (lake, historical, exporter, lakehouse, serving, observability). Los roles `ta-*` se leen con IAM, porque la API de etiquetas no los devuelve; lo del bootstrap (estado, lock y presupuesto) cuenta como `(bootstrap)` |
| K5 | `component` en el bucket del lake (`lake`), `ta-images` (`images`) y `ta-raw-reddit` (`ingest`) |
| K6 | Las dos alertas de gasto habilitadas, con el correo y la copia en Pub/Sub |

## Parte F · Disparar las alertas de gasto

Con Dataflow y el simulador encendidos: F1 umbrales a 0,1 h (`-target=module.cost_guard`, de a una política) y los
que quedaron en la nube; F2 y F3 la apertura del incidente de cada política en `ta-alerts-incidents` (en ≤ 25 min);
F4 umbrales de vuelta a 4 h y 8 h (reintenta una vez). Cada notificación manda también un correo. El simulador se
prueba una vez por encendido: su incidente sigue abierto mientras tenga instancias.

## Cierre

**Lote 7.3 cerrado (v2.59).** 29-oct-2026, WSL (`ta-streaming`), con el OK del usuario.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (3) |
| B · Pruebas | OK (2): 9 y 36 |
| L · Local | OK (6) |
| C · Terraform | OK: apply con el 7.2, GCP 7 creados y 1 cambiado, AWS 8 creados y 27 cambiados, 0 borrados; planes después sin cambios |
| K · La nube | OK (6): presupuestos, ciclos de vida, `component` en AWS (con los roles de IAM) y en GCP, las dos alertas |
| F · Alertas de verdad | OK (4) a las 19:07 UTC: Dataflow a los 475 s, simulador a los 70 s, umbrales de vuelta |

La condición del simulador cambió (instancias activas por revisión, 60 s, `ALIGN_MAX`): la anterior no abrió
incidentes. Las corridas de `fire` anteriores fallaron por la red de WSL al guardar el estado, por el 409 de Cloud
Monitoring y porque el incidente del simulador sigue abierto mientras está encendido: corregido en el validador.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| K4 sin `component` en Cost Explorer | Las etiquetas están en los recursos (K4), pero Cost Explorer solo agrupa por las que se activan en Billing → Cost allocation tags. La cuenta es compartida: lo decide el usuario |
| K1 «sin presupuesto» | La cuenta de `gcloud` necesita `billing.budgets.list` en la cuenta de facturación |
| F2 o F3 no llegan | `fire` necesita el simulador con instancias y un job `ta-stream-*` corriendo; la métrica de Dataflow tarda unos minutos en aparecer |
| F3 no llega y el simulador sigue encendido desde un `fire` anterior | El incidente anterior sigue abierto y no se avisa dos veces: apagar (`off.py`), encender y repetir |
| F1 o F4 con `Failed to save state` | La red cortó la escritura del estado; el validador lo sube con `state push` y saca `errored.tfstate` del repo. Si el push también falló: `terraform -chdir=infra/gcp/environments/dev state push errored.tfstate` desde `project/` |
