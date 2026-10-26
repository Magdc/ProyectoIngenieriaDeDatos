# Validación del Lote 3.1 · HU-024

> Carga histórica (*full load*): ~50.000 comentarios con el mismo simulador y generador del
> stream (otra semilla), anonimizados y con NLP con el mismo código, en Parquet en
> `s3://ta-dev-{cuenta}-lake/raw/origin=historical/`. Adelanta del Lote 4.1 el bucket del lake,
> su llave KMS y el rol `ta-historical-loader` (ADR-21). Contrato en `docs/ARCHITECTURE.md`
> §3.17; guía del módulo en `project/src/historical/HISTORICAL.md`.
>
> **Automático:** `python project/scripts/validate_3_1.py <parte>` desde la raíz del repo, con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_3_1.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.

## Qué se valida

| Criterio de HU-024 | Dónde |
|---|---|
| Usa el **mismo generador** en batch (vía `EventFactory` del simulador) con otra semilla | A4 (D2 del doctor), B1 (`test_hist_repo.py`), E3 |
| ~50k registros (configurable) repartidos entre `reddit`, `facebook` e `instagram` | E6 (H3) |
| Fechas en los últimos 180 días, con estacionalidad semanal y horaria y tendencias de marca | E6 (H5, H6, H7, H8) |
| Mismo envelope (`origin = historical`) y mismo pool de usuarios | B1 (`test_mismo_pool_de_usuarios_que_el_stream`), E3 y E6 (H13: `user_hash` compartidos con el stream) |
| Mismo código de anonimización y NLP; NLP en lote | A4 (D4–D5), D3, E4, E6 (H9, H11, H13: misma `nlp_model_version`) |
| Escribe Parquet a `s3://…/raw/origin=historical/` | F2, F4 (H14) |
| Una vez y reejecutable sin duplicar (`event_id` determinista por semilla y `seq`) | D8, D9, E6 (H4), E7, F5 |
| Recursos adelantados del 4.1: bucket SSE-KMS sin acceso público, llave KMS, rol con mínimo privilegio | C2–C5, F3 |

## Antes de empezar

1. Copie los archivos del lote en sus rutas (`project/…` y `docs/…`).
2. Agregue `.local-s3/` al `.gitignore` de la raíz (espejo local del bucket; A7 lo revisa).
3. Terminal en WSL, en la raíz del repo, con `source ~/.venvs/ta-streaming/bin/activate`
   (Python 3.11.17, Beam 2.77.0) y las dependencias del simulador y del loader. Si un comando
   falla con `FileNotFoundError` en `from pathlib import Path`, vuelva a entrar a la carpeta con `cd`:
   ```bash
   cd project
   python -m pip install -c src/simulator/constraints.txt \
     -r src/simulator/requirements.txt -r src/historical/requirements.txt
   cd ..
   ```
4. Para C y F: el perfil de AWS CLI de Terraform (`aws configure` o `export AWS_PROFILE=…`) y,
   si no existen, `backend.hcl` y `terraform.tfvars` del entorno AWS, con los valores del Lote 1.0
   (§5.9 y §5.14):
   ```bash
   cd project/infra/aws/environments/dev
   cp backend.hcl.example backend.hcl            # bucket = "ta-dev-648617469176-tfstate",
                                                 # dynamodb_table = "ta-dev-tflock", region = "us-east-1"
   cp terraform.tfvars.example terraform.tfvars  # aws_account_id = "648617469176" (resto como en el ejemplo)
   cd ../../../../..
   ```
   Para E: `export GCP_PROJECT=project-0568d9ba-848f-413c-88b` y ADC
   (`gcloud auth application-default print-access-token > /dev/null && echo ok`).

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada | Nube, Docker | 0 |
| C | Credenciales de AWS (perfil local) | GCP, Docker | 0 al aplicar; luego 1 USD/mes por la llave KMS |
| D | Docker Desktop e imagen `ta-dataflow-nlp-local:local` (si falta, `--build`) | Nube. **Apagado** el entorno local del stream (ver abajo) | 0 |
| E | Docker; ADC de GCP (lee `ta-hmac-key` y consulta `posts_enriched`) | **Simulador y Dataflow** (no hace falta encenderlos). **Apagado** el entorno local del stream (ver abajo) | BigQuery: unos MB |
| F | Credenciales de AWS; parte C aplicada | GCP, Docker | ~0,06 USD (unos 12.000 PUT) + ~0,003 USD/mes |

Compruebe el estado con `python project/scripts/status.py`. **Este lote no enciende nada que
cobre por hora**: si `status.py` muestra el simulador o un job de Dataflow encendidos de otra
sesión, apáguelos (`python project/scripts/off.py`); no se usan aquí. E3 cruza los `user_hash`
con lo que el stream ya dejó en `posts_enriched` (90 días); si la tabla está vacía, E2 y E3 dan
AVISO y la llave no se puede confirmar contra el stream.

**El entorno local del stream debe estar apagado en D y E.** Los contenedores
`trendanalyzer-local-*` de los lotes 1.5–2.4 (emulador, simulador y `dataflow-local` con NLP)
siguen encendidos aunque se cierren las aplicaciones de Windows, y `dataflow-local` corre los
mismos modelos sin parar. Con él encendido el NLP del histórico fue **varias veces más lento**
(benchmark: 2,6 frente a 35 filas/s; carga real: 2,3 filas/s con 1 proceso frente a ~16 con 4). Compruébelo con `docker ps` y apáguelo con
`bash project/scripts/local_dataflow.sh down-all` (conserva el cursor del simulador y
`.local-lake/`). A8 avisa, y D5/E4 no arrancan el NLP mientras esté encendido (D5a/E4a en
FALLA; `--allow-busy` corre igual).

---

## Parte A · Preflight del código

```bash
python project/scripts/validate_3_1.py preflight
```

| Paso | Esperado |
|---|---|
| A1 | Python 3.11; `yaml`, `faker`, `pyarrow`, `jsonschema`, `referencing` importan (si falta `boto3` o BigQuery, AVISO: hacen falta en E y F) |
| A2 | Los 27 archivos del lote presentes |
| A3 | Los archivos de los lotes 1.1 a 2.3 que el loader reutiliza |
| A4 | `load_historical.py doctor`: D1–D5 OK (importaciones, `EventFactory` con la semilla 3101 y caos apagado, llave local, anonimización y contratos, fila y Parquet con `POSTS_ENRICHED_COLUMNS`) |
| A5 | `docker`, `terraform`, `aws`, `gcloud`, `bash` |
| A6 | Docker responde e imagen `ta-dataflow-nlp-local:local` presente. AVISO si Docker no responde (abra Docker Desktop antes de D y E) o si falta la imagen (D la construye con `--build`) |
| A7 | `.local-s3/` en el `.gitignore` (AVISO si no) |
| A8 | Ningún contenedor del entorno local del stream (`trendanalyzer-local-*`) ni de NLP encendido (AVISO si lo hay, con el comando para apagarlo) |

Manual: `cd project && python scripts/load_historical.py doctor`. **Si A4 falla, pare aquí y
páseme la salida completa**: el detalle trae la firma real de la función que no encajó y el
ajuste va en `src/historical/adapters.py` (`HISTORICAL.md` §6).

## Parte B · Pruebas

```bash
python project/scripts/validate_3_1.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/historical -q -rs` | **142 passed, 0 skipped** (incluye `test_hist_repo.py` contra el código real y `test_hist_infra.py` con `terraform fmt`) |
| B2 | `python -m pytest src/ops/tests/test_ops_validate31.py -q` | **39 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/common -q` | sin fallas (regresión) |

B1 tarda 2–4 min (escribe y lee unos 10.000 Parquet de prueba en `/tmp`). Si tiene omitidas,
el detalle dice qué falta: `simulator.factory` o `faker` (dependencias del punto 3),
`terraform` en el PATH, o `contracts/`. Si falla `test_hist_repo.py`, el mensaje nombra la
función real y su firma: páseme ese bloque.

## Parte C · Terraform (AWS)

```bash
python project/scripts/validate_3_1.py terraform            # validate.sh + init + plan
python project/scripts/validate_3_1.py terraform --apply    # aplica (pide confirmación)
```

El plan va limitado con `-target` a los tres módulos del lote, para que nada más de
`infra/aws/environments/dev` se aplique por accidente.

| Paso | Esperado |
|---|---|
| C0 | `aws sts get-caller-identity` en la cuenta de `terraform.tfvars`. **AVISO** si es la cuenta raíz (`…:root`): AWS no le deja asumir roles, así que F sube con el perfil (`--no-role`) y prueba el rol simulando su política. Recomendado: un usuario IAM o SSO |
| C0b | `backend.hcl` y `terraform.tfvars` en `infra/aws/environments/dev` (no se versionan; ver "Antes de empezar", punto 4) |
| C1 | `bash infra/validate.sh` en verde (las cuatro raíces, incluido el entorno AWS con los archivos nuevos) |
| C2a | `terraform init` instala los módulos nuevos (`kms_key`, `s3_lake`, `historical_loader_role`) |
| C2 | **10 a crear, 0 a cambiar, 0 a destruir**: llave KMS y alias (2), bucket con propiedad, bloqueo público, cifrado, ciclo de vida y política TLS (6), rol y su política (2). `No changes` si ya se aplicó |
| C3 | `Apply complete! Resources: 10 added, 0 changed, 0 destroyed.` |
| C4 | Outputs `lake_bucket` (`ta-dev-648617469176-lake`), `kms_key_arn`, `historical_loader_role_arn` (y `kms_key_alias`, `lake_bucket_arn`) |
| C5 | `get-bucket-encryption`: `aws:kms` con la llave del lake; `get-public-access-block`: las 4 en `true` |

Manual (desde `project/`):

```bash
bash infra/validate.sh
terraform -chdir=infra/aws/environments/dev init -input=false -backend-config=backend.hcl
terraform -chdir=infra/aws/environments/dev plan \
  -target=module.lake_kms -target=module.lake_bucket -target=module.historical_loader \
  -out=../../../../evidencia/tfplan_3_1
terraform -chdir=infra/aws/environments/dev apply ../../../../evidencia/tfplan_3_1
terraform -chdir=infra/aws/environments/dev output
aws s3api get-bucket-encryption --bucket "$(terraform -chdir=infra/aws/environments/dev output -raw lake_bucket)"
```

`evidencia/tfplan_3_1` puede contener valores de `terraform.tfvars`: no lo versione.

Si `validate.sh` marca `Reference to undeclared input variable`, el entorno AWS del Lote 1.0
llama distinto a `env` o a `aws_account_id`: `test_hist_infra.py` le dice cuál, y se cambia en
`lake.tf`. Si `init` dice `Backend configuration changed`, repita con
`terraform -chdir=infra/aws/environments/dev init -backend-config=backend.hcl`.

## Parte D · Local (prueba de humo con NLP real, sin nube)

```bash
python project/scripts/validate_3_1.py local          # --build si falta la imagen de NLP
```

600 filas en 60 días con la llave **local** de desarrollo, en `project/.local-s3/_smoke/`
(semilla 3199). No toca la nube ni el dataset de E.

| Paso | Qué hace | Esperado |
|---|---|---|
| D1 | `docker info` | Docker responde |
| D2 | Imagen `ta-dataflow-nlp-local:local` | Presente (o construida con `--build`, 10–15 min) |
| D3 | `bash scripts/historical_nlp.sh doctor` | Inferencia real de 3 filas con `streaming.nlp` dentro de la imagen |
| D4 | `generate --root .local-s3/_smoke --seed 3199 --n 600 --days 60 --key-source local_dev --force` | Etapa 1 escrita (600 filas) |
| D5 | `bash scripts/historical_nlp.sh --root .local-s3/_smoke --seed 3199` | 600 filas con NLP, 0 sin NLP, una versión `robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a`; anota la velocidad (filas/s), que estima E4 |
| D6 | `write` | ~550 archivos |
| D7 | `verify` | H1–H12 sin FALLA (AVISO normal en H6–H8 con tan pocas filas) |
| D8 | `generate --digest-only` | Mismo digest |
| D9 | `write` otra vez | 0 escritos, todos sin cambios |

Manual (desde `project/`):

```bash
python scripts/load_historical.py generate --root .local-s3/_smoke --seed 3199 --n 600 --days 60 --key-source local_dev --force
bash scripts/historical_nlp.sh --root .local-s3/_smoke --seed 3199
python scripts/load_historical.py write  --root .local-s3/_smoke --seed 3199
python scripts/load_historical.py verify --root .local-s3/_smoke --seed 3199
```

## Parte E · Carga completa (50.000 filas)

```bash
python project/scripts/validate_3_1.py load --nlp-workers 4   # --n 20000 para una corrida más corta
```

| Paso | Qué hace | Esperado |
|---|---|---|
| E1 | Proyecto y ADC | `project-0568d9ba-848f-413c-88b · credenciales OK` |
| E2 | Conteo de `posts_enriched` (stream, 90 días) | ≥ 100 filas (si no, AVISO: no hay con qué cruzar la llave) |
| E3 | `generate --key-source secret` | 50.000 filas en 1–3 min; **llave: N de 500 `user_hash` aparecen en el stream** (si 0, aborta sin escribir). Si ya estaba generado con el mismo plan, no repite |
| E4 | `bash scripts/historical_nlp.sh` | Antes, E4a: si el entorno local del stream está encendido, FALLA sin arrancar. 50.000 filas con NLP, 0 sin NLP, una versión. Cada archivo registra `rows_per_s` y `batch_mean` (tamaño medio del lote; debe acercarse a 16). Medido en la máquina del equipo con `--nlp-workers 4`: ~4 filas/s por proceso, **~16 en total** (unos 8,5 min por archivo de 2.000 filas), es decir **~50 min** para 50.000; reanudable. Si va a menos de ~10 filas/s, vea "NLP lento" en Problemas frecuentes |
| E5 | `write` | ~12.000 archivos (~110 MB) en `.local-s3/raw/origin=historical/` |
| E6 | `verify --bq` | **H1–H13 OK** (abajo) |
| E7 | `generate --digest-only` | Mismo digest (reproducible) |

Comprobaciones de E6:

| Id | Qué | Esperado |
|---|---|---|
| H1 | Archivos = `output.json`, particiones = `event_time` UTC | 0 problemas |
| H2 | Columnas = `POSTS_ENRICHED_COLUMNS` en orden; timestamps `ms, UTC` | OK |
| H3 | Filas por fuente | 16.667 / 16.667 / 16.666; 100 % `event_id` únicos; `origin = historical` |
| H4 | `event_id` = uuid5(`historical|source|3101|seq`) | Todas |
| H5 | Fechas | Todas en los últimos 180 días; 180 días con datos por fuente |
| H6 | Estacionalidad | Semana r ≥ 0,8; hora r ≥ 0,9; final/inicio ≈ 1,3 |
| H7 | Tendencias | La participación de cada marca entre el primer y el último mes coincide con la esperada (±2 pp o más con pocas filas). Lo esperado parte de los **candidatos del simulador**, cuya mezcla de marcas ya cambia con `seq` (§3.10), multiplicada por el plan. El detalle muestra observado, esperado y simulador, y la **tendencia aplicada frente a Sprite**: Pepsi ≈ ×1,28 y Postobón ≈ ×0,92 entre el primer y el último mes. Si el simulador no carga, H7 supone una mezcla constante y lo dice en el detalle |
| H8 | Episodios | `campana_sprite` y `crisis_postobon`: volumen ≥ ×1,4 / ×1,25 y su ground truth sube ≥ 5 pp; AVISO si el NLP no ve el cambio |
| H9 | Anonimización | Sin `user_id`/`handle` (columnas ni `payload`); `user_hash` 1 a 1 |
| H10 | Engagement | Reglas de §5.2 por fuente y `kind` |
| H11 | NLP | 100 %, una versión, motor `beam`; acuerdo con `label_sentiment` como dato (~90 % en el stream) |
| H12 | Marcas y hashtags | Canónicos; `brand_primary = brands[0]` |
| H13 | Cruce con el stream | `user_hash` compartidos con `posts_enriched` y la misma `nlp_model_version` |

Manual (desde `project/`):

```bash
export GCP_PROJECT=project-0568d9ba-848f-413c-88b
python scripts/load_historical.py generate --json-out evidencia/historical_generate.json
bash scripts/historical_nlp.sh                     # repetir hasta que no queden archivos
python scripts/load_historical.py write
python scripts/load_historical.py verify --bq --json-out evidencia/historical_verify.json
python scripts/load_historical.py generate --digest-only
```

Consultas útiles (BigQuery, solo lectura):

```sql
-- usuarios del stream (para comparar con H13)
SELECT COUNT(DISTINCT user_hash) AS usuarios, COUNT(*) AS filas
FROM `project-0568d9ba-848f-413c-88b.ta_stream.posts_enriched`
WHERE origin = 'stream' AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY);
```

## Parte F · Subida a S3

```bash
python project/scripts/validate_3_1.py upload
```

| Paso | Qué hace | Esperado |
|---|---|---|
| F1 | `terraform output` del entorno AWS | Bucket, llave y rol |
| F2 | `upload --role-arn … --kms-key-arn … --probe` (con la cuenta raíz, `--no-role`: AVISO) | ~12.000 subidos (o "ya estaban" si se repite), 0 errores; 1–3 min |
| F3 | `aws iam simulate-principal-policy` sobre el rol (6 casos) y, si no es la cuenta raíz, la sonda real con el rol | Permitidos: `put` y `list` en `raw/origin=historical/`. Denegados: `put raw/origin=stream/`, `get` y `delete` en el histórico, `list trusted/`; sonda real: `DENEGADO` |
| F4 | `verify --s3` (H14) | Objetos = `output.json` (claves y tamaños), SSE-KMS con la llave del lake |
| F5 | Segunda subida | **0 subidos**, todos omitidos por mismo tamaño |

Manual (desde `project/`):

```bash
O="terraform -chdir=infra/aws/environments/dev output -raw"
B=$($O lake_bucket); K=$($O kms_key_arn); R=$($O historical_loader_role_arn)
python scripts/load_historical.py upload --bucket "$B" --kms-key-arn "$K" --role-arn "$R" --probe   # con la cuenta raíz: --no-role en vez de --role-arn/--probe
python scripts/load_historical.py verify --s3 --bucket "$B" --kms-key-arn "$K"
python scripts/load_historical.py upload --bucket "$B" --kms-key-arn "$K" --role-arn "$R"   # 0 subidos
aws s3 ls "s3://$B/raw/origin=historical/" --recursive --summarize | tail -2
```

## Apagado

Este lote no deja nada corriendo que cobre por hora:

```bash
bash project/scripts/historical_nlp.sh stop        # por si quedó un contenedor de NLP
docker ps --filter name=ta-hist-nlp                 # esperado: vacío
python project/scripts/status.py                    # confirmar que nada sigue cobrando
```

El entorno local del stream se apagó para D y E. Si lo necesita para otro lote, enciéndalo como
indica `docs/ACTIVE_SERVICES.md` §1.

Lo que queda creado: el bucket (centavos al mes por ~110 MB), la llave KMS (**1 USD/mes**) y el
rol (gratis). No se borran: el Lote 4.1 los amplía. El espejo local `project/.local-s3/`
ocupa ~250 MB con las etapas; se puede borrar `.local-s3/_smoke/` y, tras el cierre,
`.local-s3/_historical/s3101/stage1` y `stage2` (conserve `generate.json` y `output.json`).

## Cierre

**Resultado de la validación (v2.29): lote cerrado.**

| Parte | Resultado |
|---|---|
| A · Preflight | OK: A1–A8 (Python 3.11.17, 27 archivos del lote y 15 de lotes anteriores, `doctor` contra el código real, herramientas, imagen de NLP, `.gitignore`, ningún contenedor compitiendo por la CPU) |
| B · Pruebas | OK: B1 **142**, B2 **39**, B3 18, B4 28; sin fallas ni omitidas |
| C · Terraform | AVISO solo por la **cuenta raíz** (C0). C0b–C5 OK: `fmt` y `validate`, plan con `-target` de 10 a crear, **10 recursos aplicados**, outputs (`ta-dev-648617469176-lake`, llave del lake, `ta-historical-loader`), SSE-KMS y acceso público bloqueado |
| D · Local | OK: D1–D9. 600 filas con NLP real, H1–H12 OK, mismo digest y reescribir sin cambios. Corrió antes de las correcciones de E4 y con el entorno local del stream encendido (1,85 filas/s); lo corregido después lo cubren E y F con las 50.000 filas |
| E · Carga completa | OK salvo el H7 anterior (falso aviso, corregido; ver F4). E2: 176.715 filas de stream (5.861 usuarios). E3: 50.000 filas, digest `939248b3…`, 500 de 500 `user_hash` en el stream. E4: 0 filas sin NLP, una versión; con 4 procesos, ~4 filas/s cada uno (35,3 min para lo que faltaba). E5: 12.102 archivos. E6: H1–H6 y H8–H13 OK (semana r = 0,991, hora r = 1,000, crecimiento 1,36 frente a 1,34, `campana_sprite` ×4,69, `crisis_postobon` ×1,45, 4.547 `user_hash` 1 a 1, acuerdo del NLP con `label_sentiment` 91,8 %). E7: mismo digest |
| F · Subida a S3 | AVISO solo por la **cuenta raíz** (sube con el perfil, `--no-role`). F2: **12.102 objetos** en 164,7 s. F3: la política del rol cumple los 6 casos (simulada). F4: **H1–H12 y H14 OK** en S3 con el verificador corregido, **incluido H7**. F5: la segunda subida no sube nada (12.102 omitidos) |

**Abiertos, sin bloquear:** (1) se trabajó con la cuenta raíz de AWS, así que el rol
`ta-historical-loader` se probó por simulación y no asumiéndolo; con un usuario IAM o SSO, F3
hace además la prueba real. (2) Tras el cierre se pueden borrar `.local-s3/_smoke/` y
`.local-s3/_historical/s3101/stage1` y `stage2` (ver Apagado). (3) El entorno local del stream
quedó apagado; enciéndalo si otro lote lo necesita (`docs/ACTIVE_SERVICES.md` §1).

Hecho: Lote 3.1 en ✅ en `PLAN.md` §3 y HU-024 en `HU.md`; cifras en `CHANGELOG.md` (v2.29) y
`ARCHITECTURE.md` §3.17. La Fase 3 sigue en 🟨 hasta el Lote 3.2.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` en `from pathlib import Path` (o en `_path_importer_cache`) antes de imprimir nada | No es el código: la carpeta actual de la terminal quedó inválida en `/mnt/c` (pasa cuando se escriben archivos del repo desde Windows, como al copiar un lote). Vuelva a entrar: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita (igual que en v2.25). |
| A4/B1 `error (adaptador): … no sé qué pasar a X` | Una firma real no es la documentada. Pegue el bloque: el ajuste va en `src/historical/adapters.py` (`HISTORICAL.md` §6). |
| A1 falta `faker` | Instale las dependencias del simulador en `ta-streaming` (Antes de empezar, punto 3). |
| B1 omitidas en `test_hist_repo.py` | Falta una pieza del repo o una dependencia; el motivo sale en la línea `SKIPPED`. |
| C2a `Missing region value` | Falta `infra/aws/environments/dev/backend.hcl` (no se versiona): cópielo de `backend.hcl.example` y llénelo como en el Lote 1.0 (§5.14). El validador lo pasa con `-backend-config=backend.hcl`. |
| C2 propone cambiar o destruir | **No aplique.** Algún archivo del entorno AWS cambió; revise con `git status`. |
| C2a `Backend configuration changed` | `terraform -chdir=infra/aws/environments/dev init -backend-config=backend.hcl` |
| D3/E4 `No está la imagen` | `bash scripts/local_nlp.sh build` o la parte D con `--build`. |
| D3/E4 `nlp_from_options() devolvió None` | La imagen no trae `models.json` en `$TA_NLP_MODELS_DIR`: reconstruya la imagen de NLP (Lote 2.3). |
| D5/E4 NLP lento (< 10 filas/s) | **Primero:** `docker ps`. Si hay contenedores `trendanalyzer-local-*` (entorno local del stream) u otro de NLP, esa es la causa: `bash project/scripts/local_dataflow.sh down-all` y repita (lo hecho se conserva). Si no hay nada más encendido, detenga la corrida (`bash project/scripts/historical_nlp.sh stop`; lo ya procesado se conserva) y mida: `bash project/scripts/historical_nlp.sh doctor --nlp-bench 160` (5–10 min). D7 compara 1 proceso (espera por defecto y 0 s) con **2 y 4 procesos en paralelo** (`--bench-workers 2,4`), con el lote medio y la parte del tiempo que es inferencia, y termina con `mejor: … → use …`. Siga con esas opciones, p. ej. `python project/scripts/validate_3_1.py load --nlp-workers 4` (no regenera; el NLP sigue donde iba). Si el paralelo no mejora, revise CPU y memoria de Docker Desktop o de WSL (`.wslconfig`): cada proceso carga los dos modelos (~1–1,5 GB). Si D7 dice `terminó con código -9`, al sistema le faltó memoria: use menos procesos. |
| D7/E4 `time-pending bundles did not execute` o `--runner fnapi` rechazado | El `FnApiRunner` no sirve con el NLP del stream: con un último lote parcial falla así o pierde filas sin avisar (detalle en §3.17). Se quitó en v2.28: el NLP en lote usa siempre el `BundleBasedDirectRunner`; para ir más rápido, `--nlp-workers N`. |
| E3 `llave HMAC vacía` (versión anterior del loader) | El adaptador aceptaba una llave vacía o no encontraba el parámetro de la llave de `enrich_envelope()`. Corregido en v2.28: ahora prueba `load_hmac_key`, `access_secret` y `gcloud` en orden, rechaza resultados vacíos e imprime `llave: …` con el camino usado. Para diagnosticar: `python project/scripts/load_historical.py doctor --key-source secret --gcp-project project-0568d9ba-848f-413c-88b` (D3 dice qué devolvió cada intento, sin mostrar la llave). |
| E3 `la llave HMAC no coincide con la del stream` | La llave leída no es la del stream. No cambie el secreto. Pegue `key.detail` de `evidencia/historical_generate.json`. |
| E3 AVISO `no hay con qué comparar` | `posts_enriched` sin filas de stream recientes: H13 tampoco podrá confirmar. Es aceptable si en algún momento corrió el stream con la llave real. |
| E6 H7 AVISO `sprite: cambio +0.033 vs esperado -0.021` (versión anterior del verificador) | Falso aviso: H7 suponía que el simulador entrega las marcas en proporción fija, pero su `BrandSelector` las mueve con `seq` (§3.10) y el plan solo multiplica esa mezcla. Corregido en v2.28: H7 reconstruye los candidatos del simulador en el primer y el último mes. Repita E6 (`validate_3_1.py load --nlp-workers 4`: no regenera ni repite el NLP). |
| E4 `quedan N archivos` | Se cortó: repita `validate_3_1.py load` (no regenera; el NLP sigue donde iba). |
| E5/E6 lentos | Son ~12.000 archivos pequeños en `/mnt/c`: 2–6 min es lo esperado. El `--root` debe quedar dentro de `project/`, porque el contenedor de NLP solo ve esa carpeta. |
| F2 `AccessDenied` al asumir el rol | Con la cuenta raíz no se puede (AWS): el validador ya usa `--no-role`; a mano, añada `--no-role`. Con un usuario IAM, necesita `sts:AssumeRole` sobre `ta-historical-loader` (los administradores lo tienen). |
| F2 `AccessDenied` al subir | ¿Se aplicó C? ¿El rol tiene la política `ta-historical-loader-s3`? |
| F4 H14 con `faltan` | Repita F2: la subida es idempotente y completa lo que falte. |
