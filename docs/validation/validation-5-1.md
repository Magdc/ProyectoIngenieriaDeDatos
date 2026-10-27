# Validación del Lote 5.1 · HU-031

> *Profiling* (US-14) de las capas Raw y Trusted del lake de S3: nulos, cardinalidad, distribuciones,
> tipos y anomalías por columna, para `origin=stream` y `origin=historical`, con la diversidad léxica
> y el texto repetido del generador, y las reglas de calidad propuestas para HU-032 (Lote 5.2).
> Contrato en `docs/ARCHITECTURE.md` §3.20 (interfaces en §3.20.1) y reglas en §5.10.
>
> **Automático:** `python project/scripts/validate_5_1.py <parte>` desde la raíz del repo, con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_5_1.json`.
> Cada paso trae abajo su equivalente manual, por si el validador se detiene.
>
> **Este lote solo lee de la nube** (`sts get-caller-identity`, `ListObjectsV2` y `GetObject` del
> lake): no crea, cambia ni borra nada en AWS ni en GCP, y no enciende nada que cobre por hora.

## Qué se valida

| Criterio de HU-031 | Dónde |
|---|---|
| Informe de profiling con nulos, cardinalidad, distribuciones, tipos y anomalías por columna | C4–C6 (Raw real), D3–D4 (Trusted), B1 |
| Cubre las capas Raw y Trusted | C (Raw real); D (Trusted real si `trusted/posts/` tiene objetos; mientras no exista el Lote 4.2, Trusted simulado con el contrato de §5.7 y D1 en AVISO "pendiente") |
| Cubre `origin=stream` y `origin=historical` | C2, C5, C6 |
| Diversidad léxica y texto repetido del generador; duplicados por `event_id`, no por texto | C5 (`text.*` y `event_id`), C6; secciones 6 y 7 del informe |
| Las conclusiones alimentan las reglas de HU-032 | C7 (catálogo DQ-01…DQ-63 con umbral, gravedad y KPI) y C9 (§5.10 = código) |
| La evidencia no lleva datos personales ni volcados | C8, D6 |

## Antes de empezar

1. Traiga los archivos del lote (`project/src/quality/`, `project/scripts/{profile_data.py,validate_5_1.py}`,
   `project/src/ops/validate51.py`, sus pruebas y `docs/…`). No cambia ningún archivo de otros lotes.
2. Terminal en WSL, **entrando de nuevo a la carpeta** (tras copiar desde Windows la terminal puede
   quedar en una carpeta "fantasma" y Python falla con `FileNotFoundError`):
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   ```
   El venv ya trae lo que usa el lote (pyarrow, boto3, PyYAML; `project/src/quality/requirements.txt`).
   **No instale nada** en él: lo comparten otros lotes.
3. Credenciales de AWS con lectura del lake (`s3:ListBucket`, `s3:GetObject` y `kms:Decrypt` en
   `alias/ta-dev-lake`); el perfil `default` del equipo sirve:
   ```bash
   export AWS_PROFILE=default AWS_REGION=us-east-1
   aws sts get-caller-identity --query Account --output text      # 648617469176
   ```
   El bucket sale de `TA_LAKE_BUCKET` o, si no está, de la cuenta (`ta-dev-648617469176-lake`, §5.7).
4. Unos 0,5 GB libres en `~/.cache` (caché de Raw ~0,25 GB y Trusted simulado ~0,1 GB). Otra
   carpeta: `export TA_QUALITY_CACHE=/ruta/fuera/del/repo` (dentro del repo se rechaza).

## Preflight: qué debe estar encendido

| Parte | Necesita | No necesita | Costo |
|---|---|---|---|
| A, B | Nada | Nube, Docker | 0 |
| C | Lectura de AWS (perfil) | GCP, simulador, Dataflow, Docker | Primera vez ≈ 0,01 USD (13.300 GET, LIST y KMS) y ~0,18 GB de salida de AWS (dentro de los 100 GB gratuitos al mes); después solo LIST |
| D | Lo de C (la caché y `profiling_raw_5_1.json`); lectura de AWS para listar `trusted/posts/` | GCP, Docker | ~0 (un LIST); con Trusted real, sus GET |

Compruebe con `python project/scripts/status.py` que nada cobra. **Este lote no necesita el simulador
ni Dataflow**: si `status.py` los muestra encendidos de otra sesión, apáguelos (`python project/scripts/off.py`).
A7 avisa.

---

## Parte A · Preflight del código

```bash
python project/scripts/validate_5_1.py preflight
```

| Paso | Esperado |
|---|---|
| A0 | Solo aparece (en FALLA) si corre en Windows/PowerShell: repita desde WSL. Las demás partes no arrancan fuera de WSL |
| A1 | Python 3.11; `pyarrow` importa (FALLA si no); `yaml` y `boto3` (AVISO si faltan: C y D los usan) |
| A2 | Los 18 archivos del lote presentes |
| A3 | Los 10 archivos de lotes anteriores que usa (`common/{enriched_row,parquet_rows,hashtags,envelope}.py`, `brands.yaml`, `contracts/athena/{posts_raw,posts}.sql`, `envelope.enriched.v1.schema.json`, `ops/services.py`, `status.py`) |
| A4 | Las capas del profiler = `contracts/athena/`: 24 columnas de Raw y 24 de Trusted, en orden, y sus particiones (`part_origin, part_source, part_dt, part_hr`; `source, dt`) |
| A5 | `aws` presente (AVISO si no: C1 lo usa) |
| A6 | Caché fuera del repo (por defecto `/root/.cache/trendanalyzer/quality`) y espacio libre (AVISO con menos de 1 GB) |
| A7 | `ops.services.status()`: nada `ENCENDIDO` (AVISO si algo cobra, con el comando para apagarlo) |

Manual: `cd project && python -c "import pyarrow, boto3, yaml; print('ok')"` y `python scripts/status.py`.

## Parte B · Pruebas

```bash
python project/scripts/validate_5_1.py tests
```

| Paso | Comando (desde `project/`) | Esperado |
|---|---|---|
| B1 | `python -m pytest src/quality -q -rs` | **165 passed, 0 skipped** |
| B2 | `python -m pytest src/ops/tests/test_ops_validate51.py -q` | **24 passed** |
| B3 | `python -m pytest contracts/tests -q` | 18 passed |
| B4 | `python -m pytest src/ops/tests/test_ops_services.py -q` | sin fallas (regresión de `ops.services`, que el validador importa) |

B1 tarda unos 5 s. `test_architecture_5_10_lista_el_mismo_catalogo` lee `docs/ARCHITECTURE.md`: si falla,
§5.10 y `quality.rules.RULES` no dicen lo mismo (corregir primero el documento, §0).

## Parte C · Raw: caché de S3 y profiling real

```bash
python project/scripts/validate_5_1.py raw
```

La primera vez baja ~183 MB (13.257 objetos, unos 4 min); después solo lista. El profiling tarda ~3 min.

| Paso | Qué hace | Esperado |
|---|---|---|
| C1 | `aws sts get-caller-identity` | Cuenta `648617469176` (con la cuenta raíz dice "cuenta raíz": aquí no estorba, solo se lee) |
| C2 | `profile_data.py sync --layer raw` | **13.257 objetos** (182,7 MB): **12.102 del histórico** y 1.155 del stream (385 por fuente) o más si el exportador subió después; 0 fallidos. AVISO si el histórico no tiene 12.102 objetos o falta alguna fuente; FALLA si no hay stream o histórico |
| C3 | El mismo `sync` con `--dry-run` | `por bajar 0` |
| C4 | `profile_data.py profile --layer raw` | `profiling_raw_5_1.json` (~400 KB) y `profiling_raw_5_1.md`; **221.637 filas** (o más) en ~3 min |
| C5 | Cobertura de HU-031 | Las 24 columnas con nulos, cardinalidad, tipo y distribución en `historical/*` y `stream/*`; las 3 fuentes de cada origen; diversidad léxica (TTR, MATTR, repetidos, esqueletos) y duplicados por `event_id`; tipos físicos |
| C6 | Resultados | Histórico **50.000 filas**; completitud y **anonimización 100 %** en los dos orígenes (FALLA si no); 0 anomalías de error (AVISO si hay, con el detalle). Hoy: unicidad 100 % / 99,9988 %, 3 avisos y 16 informativas |
| C7 | Reglas propuestas | 63 reglas (Raw 56, Trusted 54) con umbral, gravedad y KPI, que cubren los 4 KPI de §5.10; hoy en Raw cumplen 55 y no cumple **DQ-63** (aviso, 6 centinelas). AVISO si no cumple alguna de gravedad `error` |
| C8 | Privacidad de la evidencia | Ni hex de 64 (`user_hash`) ni `u_NNNNNN` (`user_id`) en el JSON, el Markdown ni el resumen del `sync`; cada uno < 2 MB |
| C9 | `ARCHITECTURE.md` §5.10 frente a `quality.rules` | 63 reglas iguales |

Manual (desde la raíz):

```bash
python project/scripts/profile_data.py sync --layer raw --json-out project/evidencia/profiling_sync_raw_5_1.json
python project/scripts/profile_data.py sync --layer raw --dry-run
python project/scripts/profile_data.py profile --layer raw --json-out project/evidencia/profiling_raw_5_1.json --md-out project/evidencia/profiling_raw_5_1.md
python project/scripts/profile_data.py rules --catalog
```

Equivalente sin Python para la descarga (mismo layout de la caché):
`aws s3 sync s3://ta-dev-648617469176-lake/raw/ ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake/raw/`
(sin el manifiesto `_sync/raw.json`: el informe no podrá avisar si la caché quedó incompleta).

## Parte D · Trusted

```bash
python project/scripts/validate_5_1.py trusted
```

Necesita la parte C (usa la caché y `profiling_raw_5_1.json`). Mira si `s3://…/trusted/posts/` tiene objetos:

**Mientras no exista Trusted (hasta el Lote 4.2; desde v2.36 Trusted existe y D perfila el real):**

| Paso | Qué hace | Esperado |
|---|---|---|
| D0 | Cuenta de AWS | Como C1 (con `all` no se repite: usa la de C1) |
| D1 | `sync --layer trusted --dry-run` | **AVISO**: `trusted/posts/` vacío; el profiling real de Trusted queda **pendiente del Lote 4.2** |
| D2 | `synth-trusted` desde la caché de Raw | Trusted simulado en `~/.cache/trendanalyzer/quality/synthetic/trusted/`: **543 archivos** (uno por `source/dt`), **221.635** `event_id` distintos de 221.637 filas de Raw (las 2 copias se quitan) |
| D3 | `profile --layer trusted --root … --raw-profile …` | `profiling_trusted_synth_5_1.{json,md}` en ~4 min |
| D4 | Contrato de Trusted en el informe | Layout `trusted/posts/source=…/dt=…/` reconocido, esquema = `ta_trusted.posts` (sin `source`, con `text_clean`), `dt = DATE(event_time)` en todas, unicidad 1,0, DQ-58 `cumple` (filas = `event_id` distintos de Raw), `text_clean` perfilado. Con el simulado, cualquier problema es FALLA (sería del profiler) |
| D5 | `synth-trusted --defects --limit-files 400` y su profiling | Los 4 defectos inyectados **detectados**: `duplicate_event_id`, `dt_mismatch`, `null_text_clean`, `identity_column` |
| D6 | Privacidad de la evidencia de Trusted | Sin `user_hash` ni `user_id` |

Resultado de la parte: **AVISO** (por D1), código de salida 0.

**Cuando exista Trusted (Lote 4.2): cierre del Lote 5.1.** Después de que `ta-job-raw-to-trusted` haya
corrido al menos una vez (y sin que vuelva a correr ni el exportador suba objetos entre los pasos 1 y 2,
para que Raw y Trusted sean la misma foto):

1. `python project/scripts/validate_5_1.py raw` → C en OK, como arriba. Refresca la caché de Raw y
   `profiling_raw_5_1.json` (si el exportador subió objetos nuevos, C2 los baja y las cifras crecen).
2. `python project/scripts/validate_5_1.py trusted` → esperado:

   | Paso | Esperado con Trusted real |
   |---|---|
   | D0 | Cuenta de AWS |
   | D1 | **OK**: `N objetos: se perfila el Trusted real` (N > 0) |
   | D2 | OK: `sync --layer trusted` baja `trusted/posts/` a la caché, sin fallidos |
   | D3 | OK: `profiling_trusted_5_1.{json,md}`, perfilado con `--raw-profile project/evidencia/profiling_raw_5_1.json` |
   | D4 | **OK** si: layout `trusted/posts/source=…/dt=…/` reconocido; esquema = las 24 columnas de `ta_trusted.posts` en orden (sin `source`, con `text_clean`; se comparan tipos lógicos, no físicos: Glue puede escribir otros tipos de fecha o enteros); `dt = DATE(event_time)` en todas; unicidad 1,0; DQ-58 `cumple` (filas entre el 99 y el 100 % de los `event_id` distintos de Raw: hoy 221.635); `text_clean` perfilado. Si algo de eso falla, D4 da **AVISO** (es un hallazgo del dato para el 4.2, no del validador) y el detalle lo nombra |
   | D6 | OK: sin `user_hash` ni `user_id` en la evidencia de Trusted |

   Resultado de la parte: OK (o AVISO por D4). Qué esperar según cómo resuelva el 4.2 los hallazgos de
   Raw (`ARCHITECTURE.md` §3.20): si conserva las filas sin NLP y los centinelas, DQ-58 cumple (221.635) y
   el informe muestra los avisos `nlp_ausente` y `eventos_de_validacion` (DQ-63 no cumple); si excluye
   solo los 6 centinelas, DQ-58 sigue cumpliendo (221.629, 99,997 %); si excluyera las 65.264 filas sin
   NLP, DQ-58 no cumpliría (≈ 70 %) y D4 daría AVISO. Eso se decide con el 4.2; no se corrige aquí.
3. Revise en `profiling_trusted_5_1.md` las secciones 10 (anomalías) y 11 (las 54 reglas de Trusted con
   su estado) y anote las cifras.
4. Cierre en los documentos (con la versión que toque): `PLAN.md` §3, fila 5.1 → ✅ (la Fase 5 sigue 🟨
   hasta el 5.4); `HU.md`, HU-031 → ✅ (tabla resumen y su "Estado"); `ARCHITECTURE.md` §3.20, una nota de
   cierre con las cifras de Trusted y el estado de DQ-48, DQ-49 y DQ-54–DQ-58; la columna "Observado" de
   §5.10 para las reglas de Trusted; `CHANGELOG.md`; y la tabla de **Cierre** de esta guía.
5. `python project/scripts/validate_5_1.py all` sobre el commit de `main` → código 0, con A, B, C y D en
   OK (D ya sin el AVISO de D1; con AVISO solo si D4 encontró algo, ver el punto 2).

Manual (desde la raíz):

```bash
python project/scripts/profile_data.py sync --layer trusted --dry-run
python project/scripts/profile_data.py synth-trusted --json-out project/evidencia/profiling_trusted_synth_build_5_1.json
python project/scripts/profile_data.py profile --layer trusted --root ~/.cache/trendanalyzer/quality/synthetic/trusted --raw-profile project/evidencia/profiling_raw_5_1.json --json-out project/evidencia/profiling_trusted_synth_5_1.json --md-out project/evidencia/profiling_trusted_synth_5_1.md
# con Trusted real:
python project/scripts/profile_data.py sync --layer trusted
python project/scripts/profile_data.py profile --layer trusted --raw-profile project/evidencia/profiling_raw_5_1.json --json-out project/evidencia/profiling_trusted_5_1.json --md-out project/evidencia/profiling_trusted_5_1.md
```

## Todo seguido

```bash
python project/scripts/validate_5_1.py all          # A, B, C y D; código 0 si ningún paso dio FALLA
```

## Apagado

Este lote **no enciende nada**: no hay nada que apagar en AWS ni en GCP.

```bash
python project/scripts/status.py                       # lo que cobra de otros lotes (esperado: nada encendido)
python project/scripts/validate_5_1.py clean            # opcional: borra la caché local (~0,35 GB; pide confirmación)
```

| Paso | Esperado |
|---|---|
| L1 | Caché `~/.cache/trendanalyzer/quality` borrada (o OMITIDO si no confirma). No toca S3; la parte C la vuelve a bajar |

La publicación del informe en `s3://…/quality/` (`profile_data.py publish`) está **apagada por defecto**
y este lote no la usa (sin `--apply` solo muestra el plan).

## Cierre

**Resultado antes de entregar (v2.33):** corrido por el autor del lote en su copia del repo (WSL, sin
commit), 26-oct-2026. El cierre formal (🟨 → ✅) espera el profiling real de Trusted (Lote 4.2) y una
corrida sobre un commit de `main`.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (7/7): Python 3.11.17, pyarrow 25.0.1, capas = `contracts/athena`, caché en `/root/.cache/trendanalyzer/quality`, nada encendido |
| B · Pruebas | OK: B1 165 sin omitidas, B2 24, B3 18, B4 23 |
| C · Raw | OK (9/9): 13.257 objetos (12.102 + 1.155) ya en caché (la primera bajada, 254 s), 0 por bajar; 221.637 filas perfiladas en 182–188 s; completitud y anonimización 100 %; 0 anomalías de error, 3 avisos; 55 de 56 reglas de Raw cumplen (DQ-63 no); evidencia sin `user_hash`; §5.10 = código |
| D · Trusted | AVISO solo por D1 (Trusted no existe): Trusted simulado de 543 archivos y 221.635 filas, contrato y DQ-58 OK; los 4 defectos detectados |
| Apagado | Nada que apagar |

**Validación del maestro (v2.34):** `validate_5_1.py all` con código 0 en la rama del lote (A 7 OK, B 165 +
24 + 18 + 23, C 9 OK con 221.637 filas en 177 s, D en AVISO solo por D1) y, tras fusionar en `main`, A y B
de nuevo en OK. Sigue 🟨 hasta el profiling real de Trusted (pasos de arriba, tras el Lote 4.2).

**Cerrado en v2.36** (26-oct-2026, WSL, perfil `default`; `validate_5_1.py all` sobre `main` = `0bc0fbf`, código 0),
tras la validación del Lote 4.2. Lo corrió el maestro.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (7): Python 3.11.17, pyarrow 25.0.1, 18 archivos del lote, capas = `contracts/athena`, caché fuera del repo, nada encendido |
| B · Pruebas | OK: B1 165 sin omitidas, B2 24, B3 18, B4 23 |
| C · Raw | OK (9): 13.257 objetos ya en caché, 0 por bajar; 221.637 filas perfiladas en 388 s; completitud y anonimización 1,0; 55 de 56 reglas de Raw (DQ-63 no cumple en Raw por los 6 centinelas, esperado); evidencia sin datos personales; C9 §5.10 = código (63 reglas) |
| D · Trusted | **OK (5) con el Trusted real**: D1 543 objetos; D2 `sync` (67,4 MB); D3 profiling en 534 s → `profiling_trusted_5_1.{json,md}`; D4 221.629 filas, `dt = DATE(event_time)` en todas, unicidad 1,0, DQ-58 cumple (221.629 / 221.635), `text_clean` perfilado; D6 evidencia sin datos personales. En el informe: completitud, unicidad y anonimización 1,0, duplicados 0; anomalías 0 de error, 1 aviso (`nlp_ausente`, las 65.263 filas conservadas por decisión del usuario), 14 informativas; **54/54 reglas de Trusted** |
| Apagado | Nada que apagar |

Cifras y hallazgos en `ARCHITECTURE.md` §3.20 y `CHANGELOG.md` (v2.33, v2.34 y v2.36).

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| Cualquier parte: `FileNotFoundError` en `from pathlib import Path` antes de imprimir nada | La carpeta actual quedó inválida en `/mnt/c` tras copiar archivos desde Windows: `cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` y repita. |
| A0 / `error: está en Windows` | Está en PowerShell. Abra WSL, entre a la carpeta del repo y active `~/.venvs/ta-streaming`. |
| A1 FALLA `faltan: pyarrow` | No está en el venv del proyecto: `source ~/.venvs/ta-streaming/bin/activate`. |
| A6 / CLI `la caché … está dentro del repositorio` | `TA_QUALITY_CACHE` o `--cache` apuntan al repo: use una carpeta fuera (por defecto `~/.cache/trendanalyzer/quality`). |
| C1 FALLA `Unable to locate credentials` / `ProfileNotFound` | `export AWS_PROFILE=default` (o el perfil con lectura del lake) y `aws sts get-caller-identity`. |
| C2 fallidos con `AccessDenied` | Al perfil le falta `s3:GetObject` o `kms:Decrypt` en la llave del lake (los objetos van con SSE-KMS). |
| C2 AVISO: el histórico no tiene 12.102 objetos | ¿Se volvió a cargar el histórico con otra configuración (Lote 3.1)? El profiling vale igual; anote la cifra nueva. |
| C4 tarda mucho más de 3 min | ¿La caché está en `/mnt/c`? Use la de por defecto (`~/.cache`, ext4). ¿Corre otro contenedor de NLP? (`docker ps`). |
| C6 AVISO con anomalías de error | Es un hallazgo del dato, no del validador: pegue el detalle (sección 10 del Markdown). |
| C7 AVISO: no cumple una regla `error` | Ídem: el dato rompió un contrato de §5.2/§5.7. |
| C8 FALLA "hex de 64" | El informe tiene un valor que parece `user_hash`: no lo versione y páseme el detalle. |
| C9 / B1 `§5.10 no dice lo mismo` | Se cambió el catálogo en un lado: primero §5.10 (con su versión y CHANGELOG), luego `quality/rules.py`. |
| D2 FALLA "falta el profiling de Raw" | Corra antes la parte C. |
| D5 FALLA "NO detectado" | El profiler dejó de ver un defecto: páseme el detalle de D5 y `profiling_trusted_defects_5_1.json`. |
| `synth-trusted`: `ya tiene trusted/ y no lo creó synth-trusted` | La carpeta `--out` no es del simulador: use otra (el CLI no borra carpetas ajenas). |
| El informe avisa `cache` (la caché no coincide con S3) | El exportador subió objetos después del último `sync`: repita la parte C. |
