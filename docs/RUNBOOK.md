# TrendAnalyzer · Runbook

> Lote 8.1 (HU-045). Qué hacer para operar el sistema: encender y apagar, reiniciar el simulador, reprocesar desde
> `raw/`, atender la DLQ y responder a cada alerta. Comandos en WSL, desde la raíz del repo, con el venv activo. El
> detalle de costos y de cada servicio está en [`ACTIVE_SERVICES.md`](ACTIVE_SERVICES.md); los contratos, en
> [`ARCHITECTURE.md`](ARCHITECTURE.md) §5.

```bash
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
source ~/.venvs/ta-streaming/bin/activate
```

Credenciales: `gcloud auth login` y `gcloud auth application-default login` (proyecto `project-0568d9ba-848f-413c-88b`),
perfil `default` de AWS (cuenta `648617469176`, `us-east-1`). Antes de cualquier cosa: `python project/scripts/status.py`.

## 1. Encender y apagar

| Qué | Comando | Qué hace | Costo |
|---|---|---|---|
| Ver qué está encendido | `python project/scripts/status.py` | Dataflow, simulador, exportador, horario del lakehouse, alertas, QuickSight y «Flujo de punta a punta» | 0 |
| Encender todo | `python project/scripts/on.py` | Reanuda el exportador, habilita el horario del lakehouse, crea el simulador (Terraform, solo Cloud Run) y lanza Dataflow con NLP | ≈ 8,6 USD/día con todo |
| Solo el stream | `python project/scripts/on.py --stream-only` | Simulador y Dataflow | ≈ 0,35 USD/h |
| Apagar | `python project/scripts/off.py` | Drena Dataflow, apaga el simulador y pausa el exportador después de una última corrida. El horario del lakehouse queda encendido | — |
| Apagar también el lakehouse | `python project/scripts/off.py --lakehouse` | Además deshabilita el horario de Glue | ahorra ≈ 0,47 USD/día |

Las alertas de gasto (§3.31) mandan un correo si Dataflow lleva más de 4 h o el simulador más de 8 h encendidos.

## 2. Reiniciar el simulador

| Situación | Qué hacer |
|---|---|
| Se cayó un worker (`/healthz` en 503; Cloud Run lo reinicia solo) | Esperar 1–2 min y `status.py`. El cursor sigue en `gs://…-ta-datasets/sim-state/`: retoma desde el último `seq` guardado (repite hasta 50 eventos; Dataflow los deduplica por `event_id`) |
| Hay que forzar un reinicio | `python project/scripts/off.py` y luego `on.py`: Terraform borra y crea el servicio, y el cursor se conserva. Para reiniciar solo Dataflow y dejar el simulador: `off.py --keep-simulator` y `on.py` |
| Código nuevo del simulador | `python project/scripts/validate_6_3.py image` (Cloud Build, etiqueta por huella, actualiza `simulator_image`) y `on.py` |
| Cambiar la semilla | `SIM_SEED` en el servicio (Terraform): con otra semilla el cursor vuelve a `seq = 0` y los `event_id` no chocan (§3.6) |

Un reinicio termina el escenario de demo que estuviera activo (§3.29): lanzarlo otra vez con `demo_scenario.py start`.

## 3. Reprocesar desde `raw/`

**Lakehouse (Trusted y Curated).** Se reconstruyen **enteros** desde `raw/` de S3 en cada corrida: reprocesar es correr
el workflow. Mismo Raw ⇒ mismos bytes (§5.12, «Idempotencia en Glue»).

```bash
aws glue start-workflow-run --name ta-wf-raw-to-curated
python project/scripts/validate_4_4.py status
```

Después, refrescar el tablero: `python project/scripts/quicksight.py refresh`.

**Stream (BigQuery `posts_enriched`, `trend_windows`).** `raw/` de GCS guarda cada mensaje tal como llegó (auditoría,
30 días). No hay reinyección automática a Pub/Sub: un evento viejo reinyectado entraría fuera de la tolerancia de
las ventanas (30 min) y duplicaría filas en BigQuery. Si Dataflow estuvo caído, lo que quedó en `ta-raw-*-df` (7 días)
se procesa solo al encenderlo. Para descartarlo hay un *seek* a la hora actual: lo hacen los `validate_*.py prepare`,
y es irreversible.

**Histórico.** `python project/scripts/load_historical.py` es reanudable por etapas (§3.17). La carga de 50.000 filas
ya está en S3 y no hace falta repetirla.

## 4. Atender la DLQ

1. **Avisa** la alerta `ta-op-dlq-creciente`: más de 30 mensajes en `ta-dlq` en 10 min (§3.32). Lo normal son ~2, por
   el caos de §3.9.
2. **Ver qué entró:**
   ```sql
   SELECT error_type, source, COUNT(*) AS n, ANY_VALUE(error_detail) AS ejemplo
   FROM `ta_stream.dlq_events`
   WHERE received_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
   GROUP BY 1, 2 ORDER BY n DESC
   ```
   Los tipos están en §5.5: JSON roto, esquema, atributos, codificación, enrutamiento. El mensaje original va en
   `raw_message`, y también en `dlq/` de GCS (30 días).
3. **Decidir según la causa:**
   - *Caos o pruebas* (`ta-verify@*`, mensajes malformados de los validadores): no hacer nada.
   - *Un productor roto* (el simulador emite algo que no cumple el contrato): apagarlo (`off.py`), corregir el código,
     las pruebas de contrato (`pytest project/contracts/tests` y `test_simulator_contract.py`) y desplegar
     (`validate_6_3.py image`, `on.py`).
   - *El contrato cambió*: se cambia primero §5 y `contracts/`, después el código (regla de oro, §0).
4. Los mensajes de la DLQ no se reinyectan: quedan como evidencia hasta su expiración (30 días).

## 5. Responder a cada alerta

| Alerta (correo) | Qué significa | Qué hacer |
|---|---|---|
| `ta-alerta-tendencia` (6.2) | Una marca se disparó en volumen o sentimiento (\|z\| ≥ 3) | Es el producto: mirar `trend_windows_latest` y el tablero. Con el simulador encendido hay algunas por hora |
| `ta-op-dataflow-fallido` (7.2) | El job `ta-stream-*` quedó en Failed | Logs del job en la consola de Dataflow. Relanzar con `on.py`: el atraso de `ta-raw-*-df` se procesa al arrancar |
| `ta-op-latencia` (7.2) | La marca de agua lleva > 5 min de atraso | Mirar el tablero «TrendAnalyzer · operación»: atraso de `ta-raw-*-df` y retraso del sistema. Suele ser el NLP saturado (una ola de demo con multiplicador alto). Esperar o bajar la carga |
| `ta-op-dlq-creciente` (7.2) | Entran mensajes inválidos | §4 de este runbook |
| `ta-op-exportador-fallido` (7.2) | Falló una ejecución de `ta-gcs-to-s3` | `gcloud run jobs executions list --job ta-gcs-to-s3 --region us-central1` y su log `export_failed` (§3.18.1). La siguiente (cada 15 min) retoma desde la marca de agua |
| `ta-glue-job-fallido` (7.2, AWS) | Un job `ta-job-*` terminó en FAILED/TIMEOUT/ERROR | `/aws/events/ta-glue-job-fallos` y `/aws-glue/python-jobs/error`. Corregir y `aws glue start-workflow-run --name ta-wf-raw-to-curated` |
| `ta-gasto-dataflow-encendido`, `ta-gasto-simulador-encendido` (7.3) | Algo lleva encendido más de lo normal | Si no hay demo ni validación en curso: `off.py` |
| Presupuesto de GCP o AWS (1.0) | El gasto del mes pasó un umbral | `status.py`; apagar lo que no se use |

## 6. Demo, ensayo y plan B

- Guion de la demo de picos: [`DEMO.md`](DEMO.md).
- Guion completo de la sustentación y plan B: [`SUSTENTACION.md`](SUSTENTACION.md).
- Simulador local sin nube (HU-047): `bash project/scripts/local_dev.sh up` (§3.12 y `project/LOCAL_DEV.md`).

## 7. Al terminar el proyecto

- `python project/scripts/off.py --lakehouse`: nada queda encendido.
- QuickSight cobra después de la prueba gratis (≈ 26-nov-2026): `python project/scripts/validate_6_1.py unsubscribe
  --confirm-unsubscribe`.
- Los datos y la infraestructura base cobran centavos (§0 de `ACTIVE_SERVICES.md`). Borrarlos (`terraform destroy`) es
  una decisión del dueño del proyecto.
