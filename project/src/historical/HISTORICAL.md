# Carga histórica (Lote 3.1, HU-024)

Guía del módulo `src/historical/`. Contrato en `docs/ARCHITECTURE.md` §3.17 (y §5.7, §5.8,
§5.9, §5.12); validación paso a paso en `docs/validation/validation-3-1.md`, automática con
`python project/scripts/validate_3_1.py <parte>`.

## 1. Qué hace

Genera ~50.000 comentarios históricos (configurable) con el **mismo simulador** del stream
(Lote 1.2, `EventFactory`) y **otra semilla** (`HIST_SEED`, 3101), repartidos entre
`reddit`, `facebook` e `instagram` en los últimos 180 días. Los anonimiza con el mismo
código y la misma llave que Dataflow, les corre el mismo NLP y los deja como Parquet en
`s3://ta-dev-{cuenta}-lake/raw/origin=historical/`.

```
EventFactory.make(source, seq, t)   ← mismo pool de usuarios, payloads, engagement y hashtags
   │  origin = historical · event_id = uuid5("historical|source|seed|seq")
   ▼
aceptación por tendencias (plan.py) ── estacionalidad, crecimiento, marcas, episodios
   ▼
enrich_envelope() + llave ta-hmac-key   → stage1/  (ya anonimizado)        [host]
   ▼
streaming.nlp (RunInference, RoBERTuito) → stage2/                         [imagen de NLP]
   ▼
rows_to_parquet_bytes()                  → .local-s3/raw/origin=historical/source=…/dt=…/hr=…/   [host]
   ▼
upload (rol ta-historical-loader, SSE-KMS) → s3://…/raw/origin=historical/…                     [host]
```

## 2. Requisitos

| Etapa | Dónde | Necesita |
|---|---|---|
| `plan` | cualquier Python 3.11 | PyYAML |
| `doctor`, `generate`, `write`, `verify` | `~/.venvs/ta-streaming` | simulador (`src/simulator/requirements.txt` con `constraints.txt`), pyarrow, jsonschema; con la llave del secreto, ADC de GCP (`gcloud auth application-default login`) |
| `nlp` | imagen `ta-dataflow-nlp-local:local` (Lote 2.3) vía `scripts/historical_nlp.sh` | Docker Desktop. Sin credenciales |
| `upload`, `verify --s3` | `~/.venvs/ta-streaming` | `boto3` y el perfil de AWS CLI (`AWS_PROFILE`) |

```bash
# desde project/
python -m pip install -c src/simulator/constraints.txt \
  -r src/simulator/requirements.txt -r src/historical/requirements.txt
bash scripts/local_nlp.sh build      # solo si no existe la imagen de NLP (10-15 min)
```

## 3. Comandos

Desde `project/` (o la raíz, con `project/scripts/…`). Todos aceptan `--root` (espejo local
del bucket; por defecto `project/.local-s3`), `--seed` y `--json-out`.

| Comando | Qué hace |
|---|---|
| `python scripts/load_historical.py plan` | Conteos por fuente, ventana y episodios. |
| `python scripts/load_historical.py doctor` | Comprueba cada adaptador contra el código real (§6). Sin nube. |
| `python scripts/load_historical.py generate` | Etapa 1 con la llave del secreto y cruce de `user_hash` con `posts_enriched`. Si el plan no cambió y la etapa está completa, no repite (`--force`). |
| `python scripts/load_historical.py generate --digest-only` | Recalcula el digest sin anonimizar ni escribir y lo compara con el guardado. |
| `bash scripts/historical_nlp.sh` | Etapa 2 en la imagen de NLP. Reanudable; `stop` la detiene. |
| `bash scripts/historical_nlp.sh doctor` | NLP real sobre 3 filas sintéticas dentro de la imagen. |
| `bash scripts/historical_nlp.sh doctor --nlp-bench 160` | Filas/s y tamaño medio de lote con 1 proceso (espera por defecto y 0 s) y con 2 y 4 procesos en paralelo (`--bench-workers 2,4`); avisa si faltan filas y recomienda opciones para `nlp` (`--batch-seconds`, `--workers`). |
| `bash scripts/historical_nlp.sh --workers 4` | Etapa 2 con 4 procesos en paralelo (cada uno con sus modelos y `núcleos / 4` hilos de torch). Siempre con el `BundleBasedDirectRunner`: el `FnApiRunner` pierde o atasca el último lote parcial con el NLP del stream (`--runner fnapi` se rechaza). |
| `python scripts/load_historical.py write` | Etapa 3: Parquet por `source/dt/hr`. |
| `python scripts/load_historical.py verify [--bq] [--s3 --bucket …]` | Reglas H1–H12 (+ H13 con `--bq`, + H14 con `--s3`). H7 usa el simulador para reconstruir los candidatos del primer y el último mes (su mezcla de marcas cambia con `seq`); por eso `verify` corre en `ta-streaming`. |
| `python scripts/load_historical.py upload --bucket … --kms-key-arn … --role-arn …` | Subida idempotente (`--dry-run`, `--probe`, `--no-role`). |
| `python scripts/load_historical.py status` | Estado de las etapas. |

Salida 0 = pasa (con o sin avisos), 1 = falla, 2 = error de uso o de infraestructura.

Prueba rápida con la llave local (sin nube): `generate --root .local-s3/_smoke --seed 3199
--n 600 --days 60 --key-source local_dev`, `bash scripts/historical_nlp.sh --root
.local-s3/_smoke --seed 3199`, `write` y `verify` con los mismos `--root` y `--seed`.

## 4. El plan (`config/historical.yaml`)

- **Reparto en el tiempo** (`plan.slot_times`): por cada fuente, N/3 instantes en las 4.320
  horas de la ventana con intensidad = semana × hora local (UTC−5) × crecimiento (+30 %) ×
  ruido diario (σ = 0,10) × factor medio de marcas. Reparto sistemático: suma exacta.
- **Aceptación** (`TrendModel.acceptance`): para cada instante se piden eventos al
  simulador con `seq = 0, 1, 2…` y se acepta cada uno con probabilidad
  `factor_marca × factor_sentimiento / máximo`. Así el `seq` crece con el tiempo y la mezcla
  sigue el plan sin tocar el simulador. El sorteo usa su propio
  `Random("{seed}|{source}|{seq}|hist-accept")`. Aceptación media esperada: ~0,7.
- **Tendencias**: Pepsi +35 % de punta a punta, Postobón −10 %, Sprite estable (multiplican la
  mezcla del `BrandSelector` del simulador, §3.10).
- **Episodios**: `campana_sprite` (hace 120 días, 10 días, volumen ×1,8, positivos ×1,5) y
  `crisis_postobon` (hace 40 días, 6 días, volumen ×1,5, negativos ×3, positivos ×0,6).
- **Reproducible**: misma semilla + configuración + `as_of` ⇒ mismo digest, mismos `event_id`,
  mismas claves de S3. `as_of` por defecto es hoy a las 00:00 UTC y queda guardado en
  `generate.json`: las corridas siguientes lo reutilizan (`--new-as-of` lo cambia).

## 5. Formatos

- **Parquet**: las 24 columnas de `posts_enriched` en orden (`rows_to_parquet_bytes`, mismo
  formato que `enriched/`, §5.4), filas ordenadas por `event_id`.
- **Claves** (§5.7): `raw/origin=historical/source={s}/dt={AAAA-MM-DD}/hr={HH}/part-historical-s{seed}-{AAAAMMDD}T{HH}.parquet`,
  con `dt`/`hr` de `event_time` en UTC (ADR-21). Un archivo por fuente y hora: unos 12.000
  archivos de ~9 KB (≈ 110 MB) para 50.000 filas.
- **Trabajo** (`{root}/_historical/s{seed}/`, no se sube): `generate.json` (plan, digest,
  estadísticas, `seq` aceptados, huella de la llave), `stage1/` y `stage2/`
  (`{source}-{k}.jsonl.gz`, envelopes **enriquecidos**, sin `user_id` ni `handle`),
  `nlp.json`, `output.json` (claves, filas, tamaño y sha256) y `upload.json`.

## 6. Adaptadores (puntos a confirmar)

`adapters.py` llama al código de los lotes anteriores con las firmas de ARCHITECTURE.md y
tolera variantes de nombres; `doctor` dice qué resolvió. Si una firma real no encaja:

| Pieza | Se espera | Si falla |
|---|---|---|
| `simulator.sim_config.load_config()` | Aplica `SIM_SEED` y `SIM_CHAOS_ENABLED` del entorno | D2 dice "no usa la semilla": pasar la semilla de otra forma |
| `simulator.factory.EventFactory(config)` o un `build_factory(cfg)` | Construye la fábrica sin más argumentos obligatorios | "no sé qué pasar a …": añadir ese argumento en `build_factory()` |
| `EventFactory.make(source, seq, now)` | `now` = `datetime` UTC (o epoch si la anotación dice `float`); devuelve `Emission` con el envelope | `extract_envelope()` busca `envelope`, `event`, `messages[].data` |
| `common.enriched_row.enrich_envelope(envelope, key, process_time=…)` | Anonimiza; acepta `datetime` o texto | Nombres de parámetro en `KEY_PARAM_NAMES` / `PROCESS_PARAM_NAMES` |
| `common.hmac_key.load_hmac_key(KeySource…)` | Devuelve la misma llave que usan los workers | Alternativas: `access_secret()` y `gcloud secrets versions access` (con la variante sin salto de línea si hace falta) |
| `streaming.nlp.nlp_from_options(options)` | PTransform que recibe y devuelve filas de `posts_enriched` | `nlp_transform()` acepta el PTransform o una tupla que lo contenga |

`test_hist_repo.py` corre estas piezas reales en las pruebas: si falla ahí, el ajuste es
pequeño y está en `adapters.py`.

## 7. Seguridad

- La llave HMAC sale de `ta-hmac-key` con las credenciales del desarrollador (§5.8) y nunca
  se escribe: `generate.json` guarda solo una huella (`HMAC(llave, constante)[:16]`).
- Antes de publicar la etapa 1, `generate` cruza los 500 `user_hash` más activos con
  `posts_enriched`: si el stream tiene filas y ninguno coincide, la llave no es la del stream y
  se aborta sin escribir nada.
- El contenedor de NLP no recibe credenciales; solo ve `project/` montado en `/work`.
- La subida usa el rol `ta-historical-loader`: `s3:PutObject` y `s3:ListBucket` solo bajo
  `raw/origin=historical/`, `kms:GenerateDataKey` en la llave del lake. `--probe` comprueba
  que no pueda escribir en `raw/origin=stream/` ni listar `trusted/`. No borra nada.

## 8. Tiempos y costos (50.000 filas)

| Etapa | Tiempo | Costo |
|---|---|---|
| `generate` | 1–3 min | BigQuery: unos MB |
| `nlp` | ~50 min con `--workers 4` (≈ 4 filas/s por proceso, ≈ 16 en total, en la máquina del equipo y **con el entorno local del stream apagado**) | 0 |
| `write` / `verify` | 2–6 min en `/mnt/c` | 0 |
| `upload` | 1–3 min (16 hilos) | ~0,07 USD de PUT; almacenamiento ~0,003 USD/mes |
| Llave KMS | — | 1 USD/mes mientras exista |

## 9. Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| `error (adaptador): … no sé qué pasar a X` | Ver §6; `doctor` muestra la firma real. |
| `la llave HMAC no coincide con la del stream` | La llave no es `ta-hmac-key` o se leyó distinta (p. ej. con salto de línea). Compare `key.fingerprint` de `generate.json` entre corridas y revise qué alternativa usó (`key.detail`); **no** cambie el secreto (cambiaría todos los `user_hash`). |
| `nlp_from_options() devolvió None` | Se corrió fuera de la imagen: use `bash scripts/historical_nlp.sh`. |
| `quedan N archivos` tras `nlp` | Se cortó: repita el comando, sigue donde iba. |
| NLP a < 10 filas/s con `batch_mean` cerca de 16 | Algo más usa la CPU: casi siempre el entorno local del stream (`trendanalyzer-local-*`, cuyo `dataflow-local` corre el mismo NLP sin parar). `docker ps`; apáguelo con `bash scripts/local_dataflow.sh down-all` y repita (lo hecho se conserva). |
| NLP a < 10 filas/s (`batch_mean` cerca de 1 en el log) | Los lotes no se llenan: `historical_nlp.sh stop`, `historical_nlp.sh doctor --nlp-bench 160` y siga con las opciones que recomiende. |
| `faltan … archivos de NLP` en `write` | Termine la etapa 2. |
| `H8` AVISO con pocas filas | Normal en la prueba de humo (600 filas): las reglas de estacionalidad y episodios solo fallan con ≥ 10.000. |
| `AccessDenied` al subir | ¿Aplicó la parte C? ¿El perfil puede asumir `ta-historical-loader` (`sts:AssumeRole`)? |
| `el perfil es la cuenta raíz de AWS` | AWS no deja que la raíz asuma roles: use un usuario IAM o SSO, o `--no-role` (sube con el perfil; el cifrado SSE-KMS es el mismo). |
