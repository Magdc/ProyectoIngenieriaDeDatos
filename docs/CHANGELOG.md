# TrendAnalyzer · CHANGELOG

> Historial de versiones del proyecto: una entrada por versión, la más reciente primero. Cada entrada indica el lote que motiva los cambios.
> Fuente de verdad: [`ARCHITECTURE.md`](ARCHITECTURE.md) (versión vigente **2.15**). Las referencias `§N` de este archivo apuntan a ese documento, salvo las marcadas `PLAN.md §N`.
> Plan, trazabilidad HU y estado por lote: [`PLAN.md`](PLAN.md).

---

## v2.15 · desde v2.14

**Lote que lo motiva:** Lote 1.2 (HU-010 a HU-015) · entrega y cierre

| Área | Cambio |
|---|---|
| Lote 1.2 (HU-010 a HU-015) | Entregado y cerrado: núcleo del simulador en `src/simulator/` (`app.py`, `publisher.py`, `state.py`, `factory.py`, `sim_config.py`, `config/sim.yaml`, `enrich/{users,engagement,brands,chaos}.py`) y 9 archivos de prueba. Detalle en **§3.10**. |
| Verificación | Desde la raíz: `python -m pytest -c project/pytest.ini -rs`: **134 passed, 0 omitidas** (incluye `test_simulator_contract.py`: 1.000 eventos por fuente contra los `contracts/` reales, caos apagado). `python -m pytest project/contracts/tests -q`: **18 passed**, sin cambios. Entorno: WSL (Ubuntu) con entorno virtual; unos 3 min con el repo en `/mnt/c`. |
| Simulador | Tres workers asyncio (uno por fuente), cadencia de 1–3 s, cursor en `sim-state/{source}.json` cada 50 mensajes y al parar, reanudación con los mismos `event_id`, semilla distinta ⇒ `seq = 0`, y reintento del mismo `seq` si falla la publicación. `GET /healthz`; `POST /control/scenario` responde 501 hasta el Lote 6.3. |
| Determinismo | Misma semilla + fuente + `seq` ⇒ mismo evento salvo `event_time` e `ingest_time`; independiente de `PYTHONHASHSEED`. Los `comment` enlazan con un post real reciente de la misma fuente (en Facebook, de la misma marca), sin guardar estado. |
| Usuarios | Pool de 2.000 por fuente con `Faker('es_CO')` y semilla fija `users.pool_seed = 1581`, independiente de `SIM_SEED`, para que el histórico (Lote 3.1) reutilice los mismos usuarios. `user_id` = `u_{n:06d}` con un rango por fuente. |
| Caos | Usa un `Random` propio por evento: encenderlo no cambia texto, marca ni ids. `malformed` excluye los demás defectos y conserva los atributos Pub/Sub para que Dataflow lo envíe a la DLQ. |
| Dependencias | `requirements.txt` del simulador agrega `Faker` y los clientes de Pub/Sub y GCS. Recomendado fijar la versión exacta de Faker en la imagen (Lote 1.3): otra versión cambia los nombres del pool. |
| HU.md | HU-010 a HU-015 en ✅. Se pone al día la tabla resumen con HU-008, HU-009 (Lote 1.1) y HU-046 (Lote 1.0), que ya estaban cerradas en PLAN.md §3. El criterio del CSV de HU-008 se alinea con la decisión de §3.2.1 #7 (`marca,comentario,sentimiento[,categoria]`). |
| Decisiones | §9 #11 (criterios de HU-008 y HU-009) **cerrado** tras revisarlos contra `HU.md`. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. §5.1 se amplía con variables propias del simulador (`PORT`, `SIM_PUBLISHER`, `SIM_STATE_*`, `SIM_SOURCES`, `SIM_TOPIC_TEMPLATE`, `SIM_CONFIG`, `SIM_MAX_EVENTS`). |
| Documento | Nueva §3.10; ampliadas §5.1, §5.11, §6, §9 y PLAN.md §3. Archivos que §6 no preveía: `factory.py`, `sim_config.py` y `tests/_sim_helpers.py`. |
| Pendiente (no bloquea) | Publicación real en Pub/Sub y cursor en GCS: se prueban con el Lote 1.3 (infraestructura) y el Lote 1.5 (emulador). Repetir las pruebas en Python 3.11 con el `Dockerfile`. |

## v2.14 · desde v2.13

**Lote que lo motiva:** Lote 1.1 (HU-008, HU-009) · cierre

| Área | Cambio |
|---|---|
| Verificación | `python -m pytest` en `project/`: **64 passed, 0 omitidas** (con `jsonschema` 4.26.0 y `contracts/` presentes, así que las pruebas contra los schemas sí corrieron). `python -m pytest contracts/tests -q`: **18 passed**, sin cambios. Entorno: Windows, Python 3.14.7, pytest 9.1.1. |
| Corrección de una prueba | En la primera ejecución falló `test_identico_entre_procesos_con_distinto_hashseed` (63 de 64). Causa: en Windows la salida de un subproceso por tubería usa cp1252 y no soporta los emojis de los comentarios (`UnicodeEncodeError`). Era un defecto de la prueba, no del generador: ahora el subproceso se lanza con `PYTHONIOENCODING=utf-8` y se lee con `encoding="utf-8"`. |
| CLI | `scripts/generate_comments.py --n 50000 --seed 7` generó 50.000 filas en 3,5 s (unas 14.000 por segundo) en la máquina de desarrollo. |
| Decisiones | Los tres parámetros del generador (§5.12) quedan **adoptados por defecto**, igual que ADR-02 en Fase 0: se cambian aquí primero si se quiere otro valor (§9 #12). |
| Lote 1.1 | Pasa a ✅ cerrado en PLAN.md §3. El siguiente es el **1.2**. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |
| Pendiente (no bloquea) | Confirmar los criterios de aceptación de HU-008 y HU-009 contra `HU.md` (§9 #11). Las pruebas pasaron en Python 3.14, pero el proyecto apunta a 3.11 (§5.1): se repiten en 3.11 cuando haya `Dockerfile` (Lote 1.3) y CI (Lote 7.1). |

## v2.13 · desde v2.12

**Lote que lo motiva:** Lote 1.1 (HU-008, HU-009) · entrega

| Área | Cambio |
|---|---|
| Lote 1.1 (HU-008, HU-009) | Entregado: `src/common/{envelope,hashing,hashtags,label_map}.py` + `label_map.yaml`, el generador (`src/simulator/generator/`), `config/brands.yaml`, `scripts/generate_comments.py`, `pytest.ini`, `src/conftest.py` y 64 pruebas. Detalle en **§3.2.2**. |
| Generador | Los 7 hallazgos de §3.2.1 pasan de decisión a **implementado** (columna nueva de la tabla). Categoría `informativo` con 40 plantillas y 18 comparativas de dos marcas (el mínimo de §3.2.1 #3 era 15 y 15). El ruido ya no puede quitar la marca. Semilla por evento con `random.Random(f"{seed}\|{fuente}\|{seq}")`. |
| CSV del CLI | Columnas `marca,comentario,sentimiento[,categoria]`. `marca` lleva la **clave canónica** (`postobon`), no el nombre con tilde. `-n` por defecto pasa a 1000. UTF-8 sin BOM (`--bom` para Excel). |
| Corrección de contenido | Una plantilla de nostalgia no nombraba la marca, así que el texto no coincidía con la columna `marca`. Ahora es `"{m} me recuerda a cuando todo era más sencillo."`. |
| Parámetros nuevos (propuestos) | Mezcla de categorías cuando no se fija sentimiento (28 / 24 / 18 / 15 / 15 %), 30 % de comparativas dentro de `informativo` y truncado de hashtags a 100 caracteres. Registrados en §5.12; **pendientes de aprobación** (§9 #12). |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. En §5.2 la nota de `normalize_hashtag()` pasa de futuro a implementado, y §5.13 anota las dos pruebas nuevas que usan los schemas. |
| Documento | Nueva §3.2.2; ampliadas §3.2, §3.2.1 (columna de resultado), §3.3, §3.6, §3.8, §5.2, §5.11, §5.12, §5.13, §6, PLAN.md §1, §9 y PLAN.md §3. |
| Pendiente | Correr `pytest` real (las 60 pruebas que se ejecutaron usaron un ejecutor mínimo y 4 se omitieron por falta de `jsonschema` y de `contracts/`), confirmar los criterios de aceptación de HU-008 y HU-009 contra `HU.md` y aprobar los parámetros nuevos. *(Resuelto en v2.14; los criterios de HU-008 y HU-009 siguen en §9 #11.)* |

## v2.12 · desde v2.11

**Lote que lo motiva:** Lote 1.0 (HU-046) · aplicado y verificado

| Área | Cambio |
|---|---|
| Verificación | `validate.sh` pasa (`fmt` y `validate`) en las 4 raíces. `gcloud billing projects describe` confirma `billingEnabled: true` con la cuenta `01CCA0-88151A-A99D15` (abierta). |
| Aplicación | `gcp/bootstrap` y `aws/bootstrap` aplicados; los valores reales están en §5.14. En GCP el primer `apply` falló porque `alert_emails` tenía un correo sin `@`; se corrigió el `terraform.tfvars` y se repitió sin recrear el bucket de estado. |
| Entornos `dev` | `init -backend-config=backend.hcl` correcto en ambos (backends `gcs` y `s3`). El `plan` solo propone *outputs* nuevos y ningún recurso, lo esperado en entornos vacíos. |
| Decisiones | §9 #1 (facturación) y #10 (montos y `billing_account_id`) cerrados. **ADR-15:** con Terraform ≥ 1.10 el backend S3 avisa de que `dynamodb_table` está deprecado; funciona igual y la migración a `use_lockfile` queda como mejora opcional. |
| Repositorio | Los `.terraform.lock.hcl` de las 4 raíces se versionan. `tfplan`, `terraform.tfvars`, `backend.hcl` y el estado local no se versionan. Recomendado: `.gitattributes` con `*.sh`, `*.tf` y `*.hcl` en `eol=lf` (el repo se trabaja en Windows y WSL). |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |

## v2.11 · desde v2.10

**Lote que lo motiva:** Lote 1.0 (HU-046) · entrega

| Área | Cambio |
|---|---|
| Lote 1.0 (HU-046) | `project/infra/` entregado: un *bootstrap* por nube (estado local, se aplica una vez), 5 módulos (3 de GCP, 2 de AWS), entornos `dev` vacíos ya conectados al estado remoto, `validate.sh` y `BOOTSTRAP.md`. Contrato completo en **§5.14**. |
| Estado remoto (ADR-14) | GCP: bucket `{GCP_PROJECT}-ta-tfstate` con versionado. AWS: bucket `ta-{ENV}-{AWS_ACCOUNT_ID}-tfstate` y tabla de bloqueo `ta-{ENV}-tflock` (ADR-15). |
| APIs de GCP | 19 APIs habilitadas desde Terraform (lista en §5.14). |
| Presupuestos | GCP (moneda de la cuenta, `COP` por defecto) y AWS (USD) con aviso por correo en 50 %, 80/90 % y 100 % del gasto real, y en 100 % del pronóstico. Los montos del ejemplo (400000 COP y 50 USD) son provisionales (§9 #10). |
| Decisiones | **ADR-15:** bloqueo con DynamoDB en AWS. **ADR-16:** bootstrap aislado, con estado local y protecciones contra borrado. |
| Documento | Nueva §5.14; ampliadas §5.1 (nombres, proveedores, `allowed_account_ids`), §5.8 (identidades aún inexistentes), §5.9 (outputs reales), §5.11 y §5.12 (retención del estado y presupuestos); §6 (árbol de `infra/`), PLAN.md §1 (Lote 1.0), §9 y PLAN.md §3. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |
| Corrección | §5.1 decía que cada módulo expone `outputs.tf`. En el código, los módulos declaran sus outputs en su `main.tf`; `outputs.tf` existe en cada raíz (bootstrap y entorno). |
| Pendiente | Ejecutar `validate.sh`, confirmar `billing_account_id` y montos, y aplicar el bootstrap (§5.14, PLAN.md §3). No se pudo comprobar `terraform validate` al preparar esta versión. *(Resuelto en v2.12.)* |

## v2.10 · desde v2.9

**Lote que lo motiva:** Fase 0 · cierre (HU-001, HU-002, HU-003)

| Área | Cambio |
|---|---|
| Fechas | Se eliminan todas las fechas y el calendario: encabezados de fase, hitos, aprobaciones y el riesgo de tiempo. Sprint 2 y Sprint Final quedan como entregas, sin día (PLAN.md §1, PLAN.md §3). Los timestamps de §5.2 son datos de ejemplo, no fechas del plan. |
| Proyecto GCP | Nuevo ID `project-0568d9ba-848f-413c-88b`, número `63567159332`, nombre visible `proyectoDatos`. Reemplaza a `project-2911bf21-1f97-41aa-ac6` (§5.1, §9 #1). |
| Región | **ADR-12:** la región de GCP pasa a `us-central1`. Las cuotas de vCPU (p. ej. 8 vCPU en la familia C2, uso 0) y de Dataflow (p. ej. 640 *shuffle slots*) se verificaron en esa región. |
| Autenticación de Terraform en GCP | Application Default Credentials (`gcloud auth application-default login`), sin llaves JSON (§5.1). |
| Accesos y alertas | Por ahora solo el correo de Alejandro: único destinatario de alertas y único usuario con acceso. Miguel y Camila se agregan cuando lo necesiten sus lotes (HU-046). |
| Decisiones | §9 #8 (`dq_results.layer`) y #9 (`fact_posts.dt`) **aprobados** tal como se propusieron. |
| `contracts/` | Revisión del equipo completada (HU-001). |
| Prueba de sincronización §5 ↔ `contracts/` | Recomendada, no bloquea: pasa a la Fase 5 (§5.13). |
| Fase 0 | **Cerrada** (PLAN.md §3). |

## v2.9 · desde v2.8

**Lote que lo motiva:** Lote 0.1 (HU-001) · auditoría de `contracts/` contra §5; prepara el Lote 1.1

| Área | Cambio |
|---|---|
| Auditoría | Se revisó `contracts/` contra §5 (18 pruebas en verde). Lo que solo vivía en `contracts/` y el documento delegaba pasa a este documento, que vuelve a ser suficiente para trabajar sin abrir código. |
| §5.2 | Reglas de validación que aplica el schema (formatos, patrones, campos extra), invariantes por fuente y `kind`, reglas del envelope enriquecido y referencia a `normalize_hashtag()`. |
| §5.5 | `emotion_label` con sus 7 clases, scores en [0, 1] y tabla `dq_results` (11 columnas, partición y clustering). |
| §5.7 | Columnas de las 9 tablas de Curated, llaves foráneas naturales, regla de regeneración y día UTC. |
| §5.10 | `dq_results.kpi` toma los valores de esta tabla. |
| §5.13 | **Nueva.** Qué contiene `contracts/`, regla de cambio, qué cubren (y qué no) sus pruebas, y la prueba de sincronización pendiente. |
| §6 | Árbol de `contracts/` ordenado (las cuatro DDL de BigQuery juntas). |
| Lote 1.1 | Incluye `normalize_hashtag()` en `src/common/`. |
| §9 | Dos puntos abiertos: valores de `dq_results.layer` y significado de `fact_posts.dt` (#8 y #9). |
| Calendario | Nota sobre fechas vencidas (retirada en v2.10: el plan ya no usa fechas). |

## v2.8 · desde v2.7

**Lote que lo motiva:** Fase 0 · decisiones cerradas (ADR-02, ADR-13)

| Área | Cambio |
|---|---|
| Decisiones | **ADR-02** (Cloud Run) y **ADR-13** (mapeo categoría → sentimiento) **aprobados**; dejan de ser "por defecto" (§9). |
| `hashtags[]` | Forma canónica: minúsculas, **sin `#`**, sin tildes, solo `[a-z0-9_]` (1–100), sin repetidos y en orden de aparición. El texto conserva la forma original (§3.8, §5.2). Schemas, ejemplos y pruebas actualizados (ahora 45 ejemplos y 18 pruebas). |
| Autenticación | Terraform se autentica en AWS con el perfil local de AWS CLI (`aws configure`), sin claves en el código (§5.1). La de GCP queda pendiente. |
| Facturación GCP | La consola muestra el proyecto vinculado a una cuenta de facturación activa, con cargos estimados de 0 COP. Faltan las cuotas (§9). |
| Alertas | Un solo destinatario por ahora. La dirección vive en `terraform.tfvars` (`alert_emails`), que no se versiona (§5.12). |

## v2.7 · desde v2.6

**Lote que lo motiva:** Lote 0.1 (HU-001) · entrega de `contracts/`

| Área | Cambio |
|---|---|
| Contratos | `project/contracts/` con 6 JSON Schemas (envelope, envelope enriquecido, 3 payloads, atributos Pub/Sub), DDL de BigQuery (4 tablas) y Athena (Raw, Trusted, 9 tablas Curated), 45 ejemplos y 18 pruebas (cifras vigentes desde v2.8). Detalle en `project/contracts/README.md`. |
| §5.4 / §5.7 | Los Parquet de `enriched/` y `raw/` usan el **esquema plano** de `posts_enriched` (§5.5), con `payload` como JSON en string. |
| §5.7 | Raw se proyecta por `origin/source/dt/hr` (claves `part_*`; el histórico también escribe `hr`). Trusted por `source/dt` con `dt = DATE(event_time)` UTC y columna `text_clean`. |
| §5.5 | `avg_sentiment` = media con signo en [-1, 1]. `dq_results` y Curated con columnas definidas en `contracts/`; desde v2.9 también en §5.5 y §5.7. |
| §5.2 | `sim.chaos` ∈ `duplicate\|late\|null_fields\|malformed`. La forma de `hashtags[]` se cerró en v2.8. |

## v2.6 · desde v2.5

**Lote que lo motiva:** Lote 0.1 (HU-001, HU-002) · README y diagrama; estructura `docs/` + `project/`

| Área | Cambio |
|---|---|
| Repositorio | Los documentos viven en `docs/` y el código en `project/` (§6). Las rutas de este documento (`src/`, `infra/`, `contracts/`, `scripts/`) son relativas a `project/`. |
| CI/CD | Los workflows de GitHub Actions deben estar en `.github/workflows/` en la **raíz** del repositorio y usar `working-directory: project`. Lo mismo aplica a `.pre-commit-config.yaml` y `.gitignore`. |
| Diagrama | `docs/trend_analyzer_multicloud.drawio` actualizado a la v2.5 (HU-002) y exportado a `docs/trend_analyzer_multicloud.jpg`. |
| README | Reescrito con entornos, marcas, decisiones del generador, estado por fases y estructura. |

## v2.5 · desde v2.4

**Lote que lo motiva:** Fase 0 · cuentas y alertas (prepara el Lote 1.0, HU-046)

| Área | Cambio |
|---|---|
| Cuentas | GCP `project-2911bf21-1f97-41aa-ac6` (región `us-east1`, facturación con crédito a cargo de Alejandro Sepulveda; proyecto y región reemplazados en v2.10) y AWS `648617469176` (`us-east-1`). |
| Alertas | Canal por **correo**: suscriptor de `ta-alerts` → log estructurado → alerta basada en logs de Cloud Monitoring con canal de correo (sin SMTP ni secretos). |
| Etiquetas | `owner=alejandro` en todos los recursos. |
| Riesgo nuevo | Cuotas de la cuenta de crédito de GCP pueden limitar Dataflow. |

## v2.4 · desde v2.3

**Lote que lo motiva:** Fase 0 · prepara el Lote 1.0 (HU-046); crea HU-047 (Lote 1.5) y HU-048 (Lote 5.4)

| Área | Cambio |
|---|---|
| Decisiones | **ADR-12:** región de GCP `us-east1` (reemplazada por `us-central1` en v2.10). **ADR-13:** mapeo `categoria → sentimiento` (incluye la categoría nueva `informativo`). **ADR-14:** estado remoto de Terraform. ADR-02 (Cloud Run) queda adoptado por defecto. |
| Generador | Los 7 hallazgos de §3.2.1 pasan de propuesta a decisión y se incorporan a HU-008. |
| HU nuevas | **HU-046** Bootstrap de Terraform (Lote 1.0) · **HU-047** Entorno local con emulador de Pub/Sub (Lote 1.5) · **HU-048** Prueba de humo end-to-end (Lote 5.4). |
| Parámetros operativos | Nueva §5.12: ventanas tardías, arranque en frío del z-score, cooldown de alertas, zona horaria, retención, idempotencia de Glue, contratos en CI. |
| Riesgos | Dos nuevos: infraestructura desde cero y falsas alertas al arrancar. |

## v2.3 · desde v2.2

**Lote que lo motiva:** Fase 0 · respuestas previas al Lote 1.1 (HU-008, HU-009)

| Área | Cambio |
|---|---|
| Marcas v1 | **Postobón (propia), Pepsi y Sprite (competidores)**. El script real también trae CocaCola y Colombiana: quedan fuera de v1 (§3.2.1). |
| Región AWS | `us-east-1` confirmada. Proyecto y región de GCP siguen sin definir; `infra/` está vacío (partimos de cero). |
| Fuentes y NLP | Reddit, Facebook e Instagram confirmadas; ADR-05 (RoBERTuito) y su licencia académica aceptados. |
| Generador | Analizado el script real (§3.2.1): hay 5 puntos a resolver antes del Lote 1.1, entre ellos el mapeo `categoria → sentimiento` y los hashtags que el script ya inserta en el texto. |
| Contratos | Sin cambios en §5; `schema_version` sigue en `"1.0"`. |

## v2.2 · desde v2.1

**Lote que lo motiva:** Fase 0 · cambio de fuentes (afecta HU-008 a HU-014, HU-022, HU-024, HU-033 y HU-040)

| Área | v2.1 | v2.2 |
|---|---|---|
| Fuentes simuladas | Mastodon, Reddit, RSS | **Reddit, Facebook, Instagram** (`source` ∈ `reddit`, `facebook`, `instagram`) |
| Origen del texto | Datasets de emociones (TEC, GoEmotions, SemEval, DailyDialog) en `prepared/*.jsonl` | **Generador de comentarios** en Python (`marca,comentario`), en proceso (§3.2) |
| Idioma | Inglés | **Español (Colombia)**, `lang = "es"` |
| NLP | `emotion-english-distilroberta-base` | **RoBERTuito (pysentimiento)**: sentimiento y emoción en español (ADR-05) |
| Marca | Asignada al azar al ~35% de los mensajes | **Viene del generador**; el simulador solo controla los pesos en el tiempo (§3.8) |
| Envelope | `dataset`, `cycle`, `row_index`, `sim.label_raw`, `sim.label_unified` | `generator`, `seq`, `sim.label_sentiment` (opcional), `sim.scenario` |
| Pub/Sub | `ta-raw-mastodon / reddit / rss` | `ta-raw-reddit / facebook / instagram` |
| Preparación offline | `scripts/prepare_datasets.py` | Eliminada. Se agrega `scripts/generate_comments.py` (snapshots / histórico) |
| ADR | — | ADR-01 y ADR-05 actualizados; **ADR-11** nuevo |

`schema_version` se mantiene en `"1.0"` porque ningún componente consume aún el contrato (todo está ⬜ en PLAN.md §3). Si ya existe código que lo use, subir a `"1.1"` y migrar.

---

## Versiones anteriores a v2.2

- **v2.1:** cada fase y lote queda asociado a sus historias de usuario (`HU.md`, formato HU-000). Ver `PLAN.md` §1 y la matriz de trazabilidad `PLAN.md` §2. Las fuentes simuladas eran Mastodon, Reddit y RSS con datasets de emociones en inglés (evolución descrita en §1.1).

> Nota sobre `README.md`: se actualizó en v2.6 (tabla de fuentes, generador y decisiones, entornos, stack, estructura del repo, estado por fases y *quick start*); el detalle está en la entrada v2.6.