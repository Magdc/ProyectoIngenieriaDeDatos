# Validación del Lote 5.3 · HU-034, HU-035, HU-036

> Catálogo, linaje y gobernanza (ADR-29). **Catálogo (HU-034):** la descripción de cada tabla y el comentario de cada
> columna (con su clase de privacidad: `[anonimizado]`, `[cuasi_identificador]`, `[texto_libre]`, `[sin_dato_personal]`)
> viven en las DDL de `project/contracts/athena/`; `catalog_spec` los lleva a `tables.json` (Terraform → Glue) y al
> diccionario `docs/CATALOG.md`. **Linaje (HU-035):** `docs/LINEAGE.md`, documento y diagrama hechos a mano, sin
> *dbt docs* (decisión del usuario). **Gobernanza (HU-036):** `docs/GOVERNANCE.md` (DAMA-DMBOK, Ley 1581, datos
> sintéticos). Contrato en `docs/ARCHITECTURE.md` §3.25 (interfaces en §3.25.1).
>
> **Automático:** `python project/scripts/validate_5_3.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_5_3.json`.
>
> **No cambia datos, jobs ni el horario** (sigue encendido): el único cambio en la nube es el `apply` de la parte C, que
> actualiza la descripción y los comentarios de las 11 tablas de Glue (no choca con los jobs: no leen el catálogo).

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-034 · Todas las tablas de `ta_raw`, `ta_trusted` y `ta_curated` tienen descripción en el Glue Data Catalog | L1 (las DDL), C (Terraform), D2 (Glue real) |
| HU-034 · Cada columna tiene descripción, tipo y si contiene dato anonimizado | L1 (clase de privacidad en cada comentario; `[anonimizado]` = las 5 `user_hash`), D2 y D3 (Glue y Athena), `docs/CATALOG.md` |
| HU-035 · Linaje Raw → Trusted → Curated (sin *dbt docs*, decisión del usuario) | L3 (cada columna de las 11 tablas en `LINEAGE.md` §4) |
| HU-035 · Documento y diagrama de extremo a extremo (generador → simulador → Pub/Sub → Dataflow → BigQuery/S3 → Curated → tablero) | L3 (diagrama Mermaid), L4 (rutas citadas existen) |
| HU-036 · Mapeo de las áreas de DAMA-DMBOK a componentes | L5 (las 11 áreas) |
| HU-036 · Nota de la Ley 1581: anonimización, acceso restringido a `raw/`, minimización | L5 (texto) y G1–G7 (los controles que cita, en la nube) |
| HU-036 · Declaración de datos sintéticos y sus limitaciones | L5 |
| Los documentos no llevan datos personales ni secretos (repo público) | L6 |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | C, D, G y `status` |
| `gcloud` con sesión | El proyecto de `project/infra/gcp/environments/dev/terraform.tfvars` (o `--project`) | G5–G7 (solo lectura) |

### Preparación

1. Terminal en WSL:
   ```bash
   cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
   source ~/.venvs/ta-streaming/bin/activate
   export AWS_PROFILE=default
   aws sts get-caller-identity --query Account --output text
   ```
   Esperado: `648617469176`.
2. `git status --short` y `git log -1 --oneline`: el commit del lote en `main`, sin cambios.
3. El catálogo generado al día (sin nube):
   ```bash
   cd project/src && python -m lakehouse.catalog_spec --check; cd ../..
   ```
   Esperado: `tables.json y diccionario al día; catálogo completo`.

### Ejecución

```bash
python project/scripts/validate_5_3.py preflight
python project/scripts/validate_5_3.py tests
python project/scripts/validate_5_3.py local
python project/scripts/validate_5_3.py terraform              # plan: 0 a crear, 11 a cambiar, 0 a destruir
python project/scripts/validate_5_3.py terraform --apply      # pide confirmación; luego, plan sin deriva
python project/scripts/validate_5_3.py glue
python project/scripts/validate_5_3.py controls
python project/scripts/validate_5_3.py status
```

Salida esperada (resumen): cada parte termina con `-- Parte X: OK` y código 0, salvo C0 en AVISO por la cuenta raíz y
`terraform` sin `--apply` con C3 en AVISO. `glue` antes del `apply` da D2 en FALLA («descripción distinta de la DDL»):
es lo que el `apply` arregla.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, B, L | Nada en la nube | 0 |
| C | Perfil de AWS y el estado de Terraform | Lecturas de la API; `apply` = 11 `UpdateTable` de Glue (dentro del millón gratuito) |
| D | Perfil de AWS | `GetTables` y una consulta de Athena a `information_schema` (≈ 0,00005 USD, mínimo de 10 MB) |
| G | Perfil de AWS y `gcloud` | Solo lecturas (S3, IAM simulado, GCS, Secret Manager): 0 |

**Estado final:** nada nuevo encendido; el horario del lakehouse sigue `ENABLED` (estado normal) y el exportador
`PAUSED`. Glue guarda una versión nueva de cada tabla (historial de versiones de Glue).

---

## Parte A · Preflight

```bash
python project/scripts/validate_5_3.py preflight
```

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 (ta-streaming), boto3 y pytest |
| A2 | Los 14 archivos del lote: 10 de `project/` (las 3 DDL, `catalog_spec` y su prueba, `glue_catalog/{main.tf,tables.json}`, validador, pruebas y script) y 4 de `docs/` (`CATALOG.md`, `LINEAGE.md`, `GOVERNANCE.md`, esta guía) |
| A3 | `validate32`, `validate41` y `services` |
| A4 | `tables.json` y `docs/CATALOG.md` al día; catálogo completo |
| A5 | `terraform`, `aws`, `gcloud`, `bash` (sin `gcloud`: AVISO, G5–G7 en AVISO) |
| A6 | El horario: `ENABLED` es lo normal y no hace falta apagarlo |
| A7 | Lo que cobra (`services.status`) |

## Parte B · Pruebas

```bash
python project/scripts/validate_5_3.py tests
```

| Paso | Esperado |
|---|---|
| B1 | `src/lakehouse`: **132** passed (127 del 4.4 + 5 del catálogo), 0 omitidas |
| B2 | `test_ops_validate53.py`: **20** passed |
| B3 | `contracts/tests`: 18 (las DDL con comentarios siguen cumpliendo los contratos) |
| B4 | `src/dq` en `ta-streaming`: 3 (la prueba de identidad de HU-032 lee el catálogo) |
| B5 | `test_ops_validate51.py`: 24 (su parser de DDL lee las columnas con comentarios) |

## Parte L · Catálogo, linaje y gobernanza (sin nube)

```bash
python project/scripts/validate_5_3.py local
```

| Paso | Esperado |
|---|---|
| L1 | `11 tablas con descripción; 108 columnas (con particiones) con descripción y clase: anonimizado 5, cuasi_identificador 8, texto_libre 3, sin_dato_personal 92; anonimizadas: ta_raw.posts_raw.user_hash, …` (AVISO si las cifras cambian sin cambiar esta guía) |
| L2 | `tables.json` y `docs/CATALOG.md` = las DDL |
| L3 | Diagrama Mermaid y las 108 columnas en `LINEAGE.md` §4 |
| L4 | Las rutas `project/…` y `docs/…` citadas en los tres documentos existen |
| L5 | Las 11 áreas de DAMA-DMBOK, anonimización, acceso restringido a `raw/`, minimización y la declaración de datos sintéticos con sus tres limitaciones |
| L6 | Sin correos, llaves de AWS, URLs prefirmadas ni hashes de 64 hex en los documentos |

## Parte C · Terraform

```bash
python project/scripts/validate_5_3.py terraform
python project/scripts/validate_5_3.py terraform --apply
```

| Paso | Esperado |
|---|---|
| C0 | La cuenta `648617469176` (AVISO: cuenta raíz) |
| C0b | `backend.hcl` y `terraform.tfvars` presentes |
| C1 | `bash infra/validate.sh` OK |
| C2a | `terraform init` OK |
| C2 | Plan con `-target=module.glue_catalog`: **0 a crear, 11 a cambiar, 0 a destruir**, solo `aws_glue_catalog_table.this[…]` y **solo descripción y comentarios** (`plan_only_descriptions`; FALLA y no aplica si cambia un tipo, una ubicación o un parámetro) |
| C3 | Sin `--apply`: AVISO. Con `--apply`: pide confirmación y `Apply complete! Resources: 0 added, 11 changed, 0 destroyed.` |
| C3b | Plan otra vez: `No changes` |

## Parte D · Glue y Athena

```bash
python project/scripts/validate_5_3.py glue
```

| Paso | Esperado |
|---|---|
| D1 | Las 11 tablas en Glue con el esquema de las DDL (`glue_table_problems`, lo mismo que D2 del 4.1) |
| D2 | La descripción de las 11 tablas y los 108 comentarios iguales a las DDL (`glue_description_problems`) |
| D3 | `information_schema.columns` de Athena devuelve las 108 columnas con su comentario (lo que ve el analista); AVISO si la consulta falla (D2 ya mira Glue) |

## Parte G · Controles de `GOVERNANCE.md`

```bash
python project/scripts/validate_5_3.py controls
```

| Paso | Esperado |
|---|---|
| G1 | S3 del lake: los 4 bloqueos de acceso público activos |
| G2 | Cifrado por defecto `aws:kms` con llave y *bucket key* |
| G3 | La política del bucket niega todo con `aws:SecureTransport = false` |
| G4 | 16 casos de IAM simulados: `ta-gcp-exporter-role` solo escribe `raw/origin=stream/` y `ta-historical-loader` solo `raw/origin=historical/` (sin leer ni borrar); `ta-glue-role` lee `raw/` sin escribirlo; `ta-quicksight-role` solo lee `curated/` |
| G5 | GCS `…-ta-lake`: acceso uniforme, prevención de acceso público `enforced`, `raw/` y `dlq/` expiran a 30 días |
| G6 | GCS `…-ta-lake`: sin `allUsers` ni `allAuthenticatedUsers` (los miembros salen enmascarados: service accounts por nombre corto, usuarios como `user:***`) |
| G7 | `ta-hmac-key`: el único `secretAccessor` del secreto es `sa-dataflow` |

## Status

```bash
python project/scripts/validate_5_3.py status
```

| Paso | Esperado |
|---|---|
| S1 | `11 de 11` tablas de Glue con las descripciones de las DDL |
| S2 | Horario `ENABLED` |

## Cierre

*Pendiente de la validación en la nube (v2.44).*

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| A4/L2 FALLA «distinto de las DDL» | `cd project/src && python -m lakehouse.catalog_spec --write` y commit de `tables.json` y `docs/CATALOG.md` junto con la DDL |
| L1 FALLA «sin clase de privacidad» o «comentario de N caracteres» | Cada comentario de columna empieza por una de las 4 etiquetas y cabe en 255 caracteres (límite de Glue); la descripción de tabla son las líneas `--` pegadas al `CREATE` (≥ 40 caracteres) |
| L3 FALLA «faltan …» | Una columna nueva del catálogo no está en `LINEAGE.md` §4: agréguela a la tabla de su tabla |
| C2 FALLA «cambia algo más que descripción» | El plan trae otro cambio (un tipo, una ubicación): no es de este lote; revise la DDL o el estado antes de aplicar |
| D2 FALLA antes del `apply` | Es lo esperado: aplique la parte C |
| D3 AVISO | Athena no expuso `information_schema.columns.comment` o la consulta falló: D2 ya comprobó Glue; mire el `query_id` en la evidencia |
| G4 FALLA en un caso permitido | Revise la política del rol (§5.8) y si el simulador necesita contexto (p. ej. `s3:prefix` o cifrado) antes de cambiar nada |
| G5–G7 AVISO | `gcloud` sin sesión: `gcloud auth login` y repita `controls` |
