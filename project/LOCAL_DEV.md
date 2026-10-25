# Entorno local (Lote 1.5, HU-047)

Simulador + emulador de Pub/Sub + consumidor de validación, con `docker compose` y **sin credenciales** de GCP ni de AWS. No gasta créditos. Contrato y detalle en `docs/ARCHITECTURE.md` §3.12.

```
pubsub-emulator  (gcloud beta emulators pubsub, :8085)
      ▲   topics ta-raw-{reddit,facebook,instagram}, ta-dlq, ta-alerts · subs ta-raw-{source}-df
      │
pubsub-init  ── crea la topología (§5.3) y prepara el volumen del cursor; termina
      │
simulator    ── la misma imagen de Cloud Run (target runtime), cursor en el volumen sim-state
      │
validator    ── verify_ingest.py (Lote 1.4): schema, tasa, ids únicos, seq, cursor  [bajo demanda]
```

Todos los comandos se corren desde `project/`.

## 1. Preflight (qué debe estar activo)

| Necesario | Cómo comprobarlo | Si falla |
|---|---|---|
| Docker con `docker compose` v2 | `docker info` y `docker compose version` | En WSL: abra Docker Desktop con la integración WSL activada, o `sudo service docker start` si usa Docker Engine dentro de WSL |
| Puertos 8085 y 8080 libres | `bash scripts/local_dev.sh preflight` | Libérelos o use `PUBSUB_EMULATOR_PORT=…` / `SIM_PORT=…` |
| `contracts/` y `src/simulator/Dockerfile` en el repo | idem | Actualice el repo |

**No hace falta:** nada en GCP ni en AWS. El simulador de Cloud Run puede (y debería) seguir apagado (`simulator_running = false`).

```bash
bash scripts/local_dev.sh preflight     # todo en [OK] → "Preflight: OK"
```

## 2. Arranque

```bash
bash scripts/local_dev.sh up
```

La primera vez descarga las imágenes (el emulador pesa ~1 GB) y construye la del simulador, que corre sus pruebas en la etapa `test`; con el repo en `/mnt/c` puede tardar varios minutos. Termina con `simulator sano`. Equivale a `docker compose up -d --build simulator`.

Estado en cualquier momento:

```bash
bash scripts/local_dev.sh status        # contenedores, /healthz con seq por fuente, cursor del volumen
bash scripts/local_dev.sh logs          # simulator_start y un worker_start por fuente, sin publish_error
```

## 3. Prueba

```bash
bash scripts/local_dev.sh verify        # 60 s; o: verify 120
bash scripts/local_dev.sh restart-check # ~3 min: el seq continúa tras docker compose restart simulator
```

Esperado: `Resultado: OK`. `verify` comprueba cada mensaje contra `contracts/envelope.v1.schema.json` y reporta tasa (~0,5 msg/s por fuente) e ids únicos. `restart-check` reinicia el simulador con `SIGTERM`: debe reanudar desde el `seq` guardado al parar, con 0 o casi 0 repetidos (en local no hay solapamiento de instancias, a diferencia de Cloud Run, §3.11). Los JSON quedan en `evidencia/local_measure.json` y `evidencia/local_restart.json`.

Con caos (para ver los defectos de §3.9):

```bash
SIM_CHAOS_ENABLED=true docker compose up -d simulator
docker compose run --rm validator measure --duration 120 --validate yes --cursor local --expect-chaos
```

Otra semilla reinicia el `seq` en 0 (§3.6): `SIM_SEED=8 docker compose up -d simulator`.

## 4. Apagado

```bash
bash scripts/local_dev.sh down          # para y borra contenedores; conserva el cursor (volumen sim-state)
bash scripts/local_dev.sh reset         # además borra el cursor: el próximo up empieza en seq = 0
docker compose ps                        # vacío
```

El emulador guarda todo en memoria: al apagarlo se pierden topics y mensajes, y `pubsub-init` los vuelve a crear en el siguiente `up`.

## Problemas frecuentes

| Síntoma | Causa y arreglo |
|---|---|
| `pubsub-init` falla con permiso o `chown` | El volumen viene de otra corrida con otro dueño: `bash scripts/local_dev.sh reset`. |
| `simulator` no queda sano | `bash scripts/local_dev.sh logs`. Si hay `publish_error`, el emulador se reinició sin topics: `docker compose up -d pubsub-init simulator`. |
| `verify` da 0 mensajes | El simulador no está arriba: `status`. |
| La construcción del simulador falla en la etapa `test` | Es la misma imagen de Cloud Run: arregle la prueba que falla antes de seguir. |
| Puerto ocupado | Otra instancia o servicio: `PUBSUB_EMULATOR_PORT=18085 SIM_PORT=18080 bash scripts/local_dev.sh up`. |

## Lo que todavía no hay (Lote 2.1)

El pipeline de Dataflow con `DirectRunner` leyendo de `ta-raw-{source}-df` en este mismo emulador llega con el Lote 2.1, como un servicio más de este `docker-compose.yml`. Las suscripciones `-df` ya existen en el emulador para eso; mientras nadie las consuma, acumulan mensajes en memoria (unos 130 mil por día), así que conviene apagar el entorno cuando no se use.
