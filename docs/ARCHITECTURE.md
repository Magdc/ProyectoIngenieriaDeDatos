# TrendAnalyzer · ARCHITECTURE

> Documento maestro de arquitectura, contratos y plan de trabajo.
> **Versión 2.11** · **Lote 1.0 entregado** (bootstrap de Terraform, HU-046): nueva §5.14 con el contrato de la infraestructura base. Desde la v2.2 las fuentes en streaming son **Reddit, Facebook e Instagram** y el texto lo produce un **generador de comentarios en Python** (`marca,comentario`), en lugar de los datasets de emociones en inglés de la v2.1.
> Fuentes de verdad combinadas: `README.md` + `diagramaFINAL.drawio` + decisiones de este documento.
> **v2.1:** cada fase y lote queda asociado a sus historias de usuario (`HU.md`, formato HU-000). Ver §7 y la matriz de trazabilidad §7.1. **v2.2:** resumen de cambios en §1.5; las HU afectadas por el cambio de fuentes se señalan en §7.1.
> **v2.3:** respuestas de Fase 0 registradas en §9 (marcas, región AWS, NLP, fuentes), y análisis del script real del generador en §3.2.1.
> **v2.4:** decisiones adoptadas (ADR-12 a ADR-14), parámetros operativos (§5.12) y tres HU nuevas: HU-046 (bootstrap), HU-047 (entorno local) y HU-048 (prueba de humo E2E). Ahora son 48 HU.
> **v2.5:** cuentas e IDs reales registrados (§5.1), canal de alertas por correo (§5.12) y responsable `owner=alejandro`.
> **v2.6:** estructura real del repositorio (`docs/` + `project/`, §6 y §1.9), diagrama `.drawio` actualizado y README alineado.
> **v2.11:** Lote 1.0 entregado (`project/infra/`, HU-046): estado remoto en GCS y S3, bloqueo en DynamoDB, 19 APIs de GCP, presupuestos con aviso por correo y `validate.sh`. Nueva §5.14; ADR-15 y ADR-16; §9 #10 abierto. Sin cambios en `contracts/` (§1.14).
> **v2.10:** sin fechas ni calendario; proyecto GCP `project-0568d9ba-848f-413c-88b` (`proyectoDatos`) y región `us-central1` (ADR-12); autenticación de Terraform en GCP decidida; §9 #8 y #9 aprobados; Fase 0 cerrada (§1.13).
> **v2.9:** auditoría de `contracts/` contra §5: las columnas de `dq_results` y de Curated, las reglas de validación del envelope y las 7 clases de emoción pasan a este documento; nueva §5.13 (relación con `contracts/` y cómo mantenerlos alineados); dos puntos abiertos en §9 y nota de calendario (§1.12).
> **v2.8:** decisiones de Fase 0 cerradas: ADR-02 y ADR-13 aprobados, forma canónica de `hashtags[]`, autenticación de Terraform en AWS y facturación de GCP (§1.11).
> **v2.7:** `project/contracts/` entregado (Lote 0.1, HU-001) y decisiones que §5 no fijaba: Parquet plano, particiones de Raw/Trusted, `avg_sentiment` con signo (§1.10).

---

## 0. Cómo usar este documento

Este archivo está pensado para que **baste con leerlo (o pegármelo) para construir cualquier pieza del proyecto sin tener que enviar el código ya hecho**.

| Si quieres… | Lee |
|---|---|
| Entender qué cambió y por qué | §1 |
| Ver el panorama completo | §2 |
| Entender cómo funciona el simulador | §3 |
| Saber **cómo hablarle a cada componente** (formatos, nombres, topics, tablas, IAM) | **§5 (contratos)** |
| Saber **cómo se valida cada contrato** y cómo mantener §5 y `contracts/` alineados | **§5.13** |
| Saber **qué infraestructura base existe** y cómo aplicarla (estado remoto, APIs, presupuestos) | **§5.14** |
| Saber qué construir y en qué orden | §7 (fases y lotes) |
| Saber qué historias de usuario cubre cada fase o lote | **§7 y §7.1** (detalle en `HU.md`) |
| Pedirme el siguiente lote | §8 (plantilla de petición) |

**Regla de oro:** si dos componentes se comunican, su interfaz vive en §5. Si algo no está en §5, no es una interfaz y no debe depender de él ningún otro componente. Cuando una interfaz cambie, se cambia **primero aquí** y luego en el código.

---

## 1. Qué cambia respecto al README y al diagrama FINAL

### 1.1 Decisión central

Reddit, Facebook e Instagram solo se pueden consumir por APIs REST/OAuth (polling), lo que no es streaming real, y además dependen de aprobaciones y límites de acceso: Reddit exige aprobación OAuth, y las APIs de Meta (Facebook/Instagram) exigen revisión de la app y permisos, y no dan acceso libre a la conversación pública de terceros. **Decisión:** las tres fuentes pasan a ser **simuladas**. Cada una pide al **generador de comentarios** (§3.2) un par `marca,comentario` y publica un mensaje a Pub/Sub cada **1–3 s**. El dataset histórico es un **full load** batch directo a S3, generado con el **mismo** generador.

*Evolución:* la v2.0/v2.1 simulaba Mastodon, Reddit y RSS reproduciendo datasets públicos de emociones (TEC, GoEmotions, SemEval, DailyDialog). Se reemplazan porque estaban en inglés, no hablaban de las marcas del negocio y obligaban a inyectarlas artificialmente; el generador ya produce comentarios en español colombiano sobre marcas concretas.

Todo lo que viene después de Pub/Sub (Dataflow, GCS, BigQuery, S3, Glue, Athena, QuickSight) **no cambia de naturaleza**: el pipeline real es idéntico al que tendría con fuentes reales. Lo único falso es el origen del dato.

### 1.2 Delta contra el diagrama FINAL

| Elemento del diagrama FINAL | Cambio en v2 |
|---|---|
| Mastodon / Reddit / RSS → Cloud Run (flechas "Streaming HTTPS", "REST/OAuth2", "HTTPS") | Se eliminan las flechas externas. Los tres orígenes se sustituyen por **Reddit, Facebook e Instagram simulados**. Dentro de GCP aparece **`stream-simulator`** (Cloud Run), que invoca al generador de comentarios y publica a Pub/Sub. |
| Cloud Run (collectors) | Pasa a ser un único servicio always-on `stream-simulator` (§3.1). Los collectors reales (`src/collectors/`) quedan como código legado/opcional. |
| Cloud Scheduler → Cloud Run | Cambia de rol: ya no dispara polling. Dispara el **exportador GCS→S3** cada 15 min (y opcionalmente escenarios de demo, §3.7). |
| Secret Manager (credenciales de APIs) | Ya no guarda tokens de Mastodon/Reddit. Guarda la **llave HMAC de anonimización**. |
| Datasets históricos → S3 | Se mantiene. Se precisa: `scripts/generate_comments.py` / `historical/loader` generan registros con el **mismo generador** y el mismo envelope (§5.2) y los suben a `s3://…/raw/origin=historical/`. |
| *(no existe)* Datasets de emociones en GCS | Ya no se usan (v2.2). El bucket `…-ta-datasets` solo guarda el estado del simulador y snapshots opcionales del generador (§5.4). |
| *(no existe)* GCS → S3 | **Falta en el diagrama.** Se agrega el exportador `gcs-to-s3` (§5.6). Hoy el único camino GCP→AWS dibujado es BigQuery→QuickSight. |
| Flecha huérfana `? → EventBridge` | Sin origen. Se resuelve: EventBridge es un **schedule** que dispara el workflow de Glue (§5.7). |
| BigQuery → QuickSight "Carga batch" | **Verificar** si el conector de QuickSight a BigQuery está disponible para la cuenta. Camino principal recomendado: Parquet en S3 → Athena → QuickSight. BigQuery queda para analítica caliente en GCP. |

### 1.3 Discrepancias README ↔ diagrama FINAL (resueltas)

El README menciona **Firestore, Cloud SQL, Neptune y Dataplex**, que **no están** en el diagrama FINAL. El diagrama manda:

| Servicio del README | Decisión v2 |
|---|---|
| Firestore, Cloud SQL | **Fuera del alcance v1.** El estado del simulador vive en un objeto GCS (§3.6). |
| Neptune | **Fuera del alcance v1.** "Análisis de relaciones" se cubre con tablas de aristas en Curated consultables por Athena (§5.7). Neptune queda como extensión si sobra tiempo. |
| Dataplex / OpenLineage | Catálogo = **Glue Data Catalog**. Linaje = **dbt docs + documento de linaje** (§7, Fase 5). |

### 1.4 Estado de `README.md`

Actualizado en v2.6: tabla de fuentes, generador de comentarios y decisiones adoptadas, entornos, stack, estructura del repo (`docs/` + `project/`), estado por fases y *quick start*. El diagrama `docs/trend_analyzer_multicloud.drawio` ya refleja la v2.5 (§1.2 aplicado).

### 1.5 Resumen de cambios v2.1 → v2.2

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

`schema_version` se mantiene en `"1.0"` porque ningún componente consume aún el contrato (todo está ⬜ en §10). Si ya existe código que lo use, subir a `"1.1"` y migrar.

### 1.6 Cambios v2.2 → v2.3

| Área | Cambio |
|---|---|
| Marcas v1 | **Postobón (propia), Pepsi y Sprite (competidores)**. El script real también trae CocaCola y Colombiana: quedan fuera de v1 (§3.2.1). |
| Región AWS | `us-east-1` confirmada. Proyecto y región de GCP siguen sin definir; `infra/` está vacío (partimos de cero). |
| Fuentes y NLP | Reddit, Facebook e Instagram confirmadas; ADR-05 (RoBERTuito) y su licencia académica aceptados. |
| Generador | Analizado el script real (§3.2.1): hay 5 puntos a resolver antes del Lote 1.1, entre ellos el mapeo `categoria → sentimiento` y los hashtags que el script ya inserta en el texto. |
| Contratos | Sin cambios en §5; `schema_version` sigue en `"1.0"`. |

### 1.7 Cambios v2.3 → v2.4

| Área | Cambio |
|---|---|
| Decisiones | **ADR-12:** región de GCP `us-east1` (reemplazada por `us-central1` en v2.10, §1.13). **ADR-13:** mapeo `categoria → sentimiento` (incluye la categoría nueva `informativo`). **ADR-14:** estado remoto de Terraform. ADR-02 (Cloud Run) queda adoptado por defecto. |
| Generador | Los 7 hallazgos de §3.2.1 pasan de propuesta a decisión y se incorporan a HU-008. |
| HU nuevas | **HU-046** Bootstrap de Terraform (Lote 1.0) · **HU-047** Entorno local con emulador de Pub/Sub (Lote 1.5) · **HU-048** Prueba de humo end-to-end (Lote 5.4). |
| Parámetros operativos | Nueva §5.12: ventanas tardías, arranque en frío del z-score, cooldown de alertas, zona horaria, retención, idempotencia de Glue, contratos en CI. |
| Riesgos | Dos nuevos: infraestructura desde cero y falsas alertas al arrancar. |

### 1.8 Cambios v2.4 → v2.5

| Área | Cambio |
|---|---|
| Cuentas | GCP `project-2911bf21-1f97-41aa-ac6` (región `us-east1`, facturación con crédito a cargo de Alejandro Sepulveda; proyecto y región reemplazados en v2.10, §1.13) y AWS `648617469176` (`us-east-1`). |
| Alertas | Canal por **correo**: suscriptor de `ta-alerts` → log estructurado → alerta basada en logs de Cloud Monitoring con canal de correo (sin SMTP ni secretos). |
| Etiquetas | `owner=alejandro` en todos los recursos. |
| Riesgo nuevo | Cuotas de la cuenta de crédito de GCP pueden limitar Dataflow. |

### 1.9 Cambios v2.5 → v2.6

| Área | Cambio |
|---|---|
| Repositorio | Los documentos viven en `docs/` y el código en `project/` (§6). Las rutas de este documento (`src/`, `infra/`, `contracts/`, `scripts/`) son relativas a `project/`. |
| CI/CD | Los workflows de GitHub Actions deben estar en `.github/workflows/` en la **raíz** del repositorio y usar `working-directory: project`. Lo mismo aplica a `.pre-commit-config.yaml` y `.gitignore`. |
| Diagrama | `docs/trend_analyzer_multicloud.drawio` actualizado a la v2.5 (HU-002) y exportado a `docs/trend_analyzer_multicloud.jpg`. |
| README | Reescrito con entornos, marcas, decisiones del generador, estado por fases y estructura. |

### 1.10 Cambios v2.6 → v2.7

| Área | Cambio |
|---|---|
| Contratos | `project/contracts/` con 6 JSON Schemas (envelope, envelope enriquecido, 3 payloads, atributos Pub/Sub), DDL de BigQuery (4 tablas) y Athena (Raw, Trusted, 9 tablas Curated), 45 ejemplos y 18 pruebas (cifras vigentes desde v2.8). Detalle en `project/contracts/README.md`. |
| §5.4 / §5.7 | Los Parquet de `enriched/` y `raw/` usan el **esquema plano** de `posts_enriched` (§5.5), con `payload` como JSON en string. |
| §5.7 | Raw se proyecta por `origin/source/dt/hr` (claves `part_*`; el histórico también escribe `hr`). Trusted por `source/dt` con `dt = DATE(event_time)` UTC y columna `text_clean`. |
| §5.5 | `avg_sentiment` = media con signo en [-1, 1]. `dq_results` y Curated con columnas definidas en `contracts/`; desde v2.9 también en §5.5 y §5.7. |
| §5.2 | `sim.chaos` ∈ `duplicate\|late\|null_fields\|malformed`. La forma de `hashtags[]` se cerró en v2.8. |

### 1.11 Cambios v2.7 → v2.8

| Área | Cambio |
|---|---|
| Decisiones | **ADR-02** (Cloud Run) y **ADR-13** (mapeo categoría → sentimiento) **aprobados**; dejan de ser "por defecto" (§9). |
| `hashtags[]` | Forma canónica: minúsculas, **sin `#`**, sin tildes, solo `[a-z0-9_]` (1–100), sin repetidos y en orden de aparición. El texto conserva la forma original (§3.8, §5.2). Schemas, ejemplos y pruebas actualizados (ahora 45 ejemplos y 18 pruebas). |
| Autenticación | Terraform se autentica en AWS con el perfil local de AWS CLI (`aws configure`), sin claves en el código (§5.1). La de GCP queda pendiente. |
| Facturación GCP | La consola muestra el proyecto vinculado a una cuenta de facturación activa, con cargos estimados de 0 COP. Faltan las cuotas (§9). |
| Alertas | Un solo destinatario por ahora. La dirección vive en `terraform.tfvars` (`alert_emails`), que no se versiona (§5.12). |

### 1.12 Cambios v2.8 → v2.9

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

### 1.13 Cambios v2.9 → v2.10

| Área | Cambio |
|---|---|
| Fechas | Se eliminan todas las fechas y el calendario: encabezados de fase, hitos, aprobaciones y el riesgo de tiempo. Sprint 2 y Sprint Final quedan como entregas, sin día (§7, §10). Los timestamps de §5.2 son datos de ejemplo, no fechas del plan. |
| Proyecto GCP | Nuevo ID `project-0568d9ba-848f-413c-88b`, número `63567159332`, nombre visible `proyectoDatos`. Reemplaza a `project-2911bf21-1f97-41aa-ac6` (§5.1, §9 #1). |
| Región | **ADR-12:** la región de GCP pasa a `us-central1`. Las cuotas de vCPU (p. ej. 8 vCPU en la familia C2, uso 0) y de Dataflow (p. ej. 640 *shuffle slots*) se verificaron en esa región. |
| Autenticación de Terraform en GCP | Application Default Credentials (`gcloud auth application-default login`), sin llaves JSON (§5.1). |
| Accesos y alertas | Por ahora solo el correo de Alejandro: único destinatario de alertas y único usuario con acceso. Miguel y Camila se agregan cuando lo necesiten sus lotes (HU-046). |
| Decisiones | §9 #8 (`dq_results.layer`) y #9 (`fact_posts.dt`) **aprobados** tal como se propusieron. |
| `contracts/` | Revisión del equipo completada (HU-001). |
| Prueba de sincronización §5 ↔ `contracts/` | Recomendada, no bloquea: pasa a la Fase 5 (§5.13). |
| Fase 0 | **Cerrada** (§10). |

### 1.14 Cambios v2.10 → v2.11

| Área | Cambio |
|---|---|
| Lote 1.0 (HU-046) | `project/infra/` entregado: un *bootstrap* por nube (estado local, se aplica una vez), 5 módulos (3 de GCP, 2 de AWS), entornos `dev` vacíos ya conectados al estado remoto, `validate.sh` y `BOOTSTRAP.md`. Contrato completo en **§5.14**. |
| Estado remoto (ADR-14) | GCP: bucket `{GCP_PROJECT}-ta-tfstate` con versionado. AWS: bucket `ta-{ENV}-{AWS_ACCOUNT_ID}-tfstate` y tabla de bloqueo `ta-{ENV}-tflock` (ADR-15). |
| APIs de GCP | 19 APIs habilitadas desde Terraform (lista en §5.14). |
| Presupuestos | GCP (moneda de la cuenta, `COP` por defecto) y AWS (USD) con aviso por correo en 50 %, 80/90 % y 100 % del gasto real, y en 100 % del pronóstico. Los montos del ejemplo (400000 COP y 50 USD) son provisionales (§9 #10). |
| Decisiones | **ADR-15:** bloqueo con DynamoDB en AWS. **ADR-16:** bootstrap aislado, con estado local y protecciones contra borrado. |
| Documento | Nueva §5.14; ampliadas §5.1 (nombres, proveedores, `allowed_account_ids`), §5.8 (identidades aún inexistentes), §5.9 (outputs reales), §5.11 y §5.12 (retención del estado y presupuestos); §6 (árbol de `infra/`), §7 (Lote 1.0), §9 y §10. |
| Contratos | **Sin cambios** en §5.2–§5.8 ni en `contracts/`. `schema_version` sigue en `"1.0"`. |
| Corrección | §5.1 decía que cada módulo expone `outputs.tf`. En el código, los módulos declaran sus outputs en su `main.tf`; `outputs.tf` existe en cada raíz (bootstrap y entorno). |
| Pendiente | Ejecutar `validate.sh`, confirmar `billing_account_id` y montos, y aplicar el bootstrap (§5.14, §10). No se pudo comprobar `terraform validate` al preparar esta versión. |

---

## 2. Arquitectura objetivo

```mermaid
flowchart LR
  subgraph GCP["Google Cloud Platform · ingesta y procesamiento near-real-time"]
    DSB[("GCS datasets<br/>sim-state · snapshots CSV")]
    SIMS["Cloud Run<br/>stream-simulator<br/>+ generador de comentarios"]
    PS["Pub/Sub<br/>ta-raw-reddit / facebook / instagram"]
    DF["Dataflow streaming<br/>validar · dedup · anonimizar · NLP"]
    BQ[("BigQuery<br/>ta_stream")]
    GCS[("GCS lake<br/>raw / enriched / dlq")]
    EXP["Cloud Run Job<br/>gcs-to-s3"]
    SCH["Cloud Scheduler"]
    SEC["Secret Manager<br/>llave HMAC"]
    MON["Monitoring + Logging"]
    SIMS <-->|cursor| DSB
    SIMS --> PS
    PS --> DF
    DF --> BQ
    DF --> GCS
    GCS --> EXP
    SCH -.-> EXP
    SEC -.-> DF
    SIMS -.-> MON
    DF -.-> MON
  end
  subgraph AWS["Amazon Web Services · lakehouse y analítica"]
    HIST["Loader histórico<br/>full load"]
    S3[("S3 lake<br/>raw → trusted → curated")]
    EB["EventBridge"]
    GLUE["Glue ETL + Data Catalog"]
    ATH["Athena"]
    QS["QuickSight"]
    CW["CloudWatch"]
    IAM["IAM + KMS"]
    HIST --> S3
    EB -.-> GLUE
    S3 <--> GLUE
    GLUE --> ATH
    S3 --> ATH
    ATH --> QS
    GLUE -.-> CW
    IAM -.-> S3
  end
  EXP ==>|"Parquet · HTTPS/TLS"| S3
  QS --> CONS["Marketing · Gerencia · Analistas · Relaciones"]
  subgraph OPS["DataOps"]
    GH["GitHub → Actions → Terraform"]
    GE["Great Expectations / dbt"]
  end
  GH -.-> GCP
  GH -.-> AWS
  GE -.-> BQ
  GE -.-> S3
```

**Dos planos, un contrato.** GCP = plano caliente (segundos/minutos). AWS = plano de lakehouse (horas). Ambos hablan el mismo **envelope** (§5.2), por eso el dato histórico y el streaming terminan en la misma tabla Trusted.

---

## 3. El simulador de streaming

### 3.1 Dónde corre (y por qué no EC2)

**Decisión:** **un solo servicio Cloud Run `ta-stream-simulator`**, con `min-instances=1`, `max-instances=1` y CPU siempre asignada (`--no-cpu-throttling`).

- *Por qué no EC2:* publicar a Pub/Sub desde AWS obliga a llevar credenciales de GCP a otra nube y añade latencia/egreso entre nubes. El simulador debe vivir **donde está Pub/Sub**.
- *Por qué Cloud Run y no una VM:* el diagrama ya lo dibuja, ya existe módulo Terraform de Cloud Run y Dockerfiles en el repo, y se despliega/actualiza con una imagen.
- *Por qué `max=1`:* con más de una instancia habría emisiones duplicadas y el cursor se pisaría.
- *Servicio único con 3 workers asyncio* (uno por fuente) en vez de 3 servicios: una sola instancia siempre encendida, un solo costo fijo.
- *Plan B si el costo molesta:* una VM `e2-small` en GCE con el mismo contenedor (ADR-02, §9).

El contenedor expone `GET /healthz` (requisito de Cloud Run) y, opcionalmente, `POST /control/scenario` (§3.7). El resto es un loop de fondo.

### 3.2 El generador de comentarios

El script de Python que ya existe se integra como módulo del simulador (`src/simulator/generator/comment_generator.py`). **No es un servicio aparte**: corre en el mismo proceso del simulador y también se invoca en batch para el histórico (Lote 3.1).

**Contrato mínimo de salida** (CSV UTF-8 o su equivalente en objeto): `marca,comentario`.

```csv
marca,comentario
Postobón,Opinión personal: soy fan d Postobón y no pienso cambiar. Volvería mil veces. 🙌
Pepsi,Mi aporte del día: no tolero Pepsi Cola en el trancón. Mejor me hubiera tomado un agua. Es mi opinión.
Sprite,Vamos al punto: me chifla Sprite sobre todo con el frío de Bogotá. Eso sí es sabor. 🥤
```

Lo que se observa en los ejemplos: español coloquial colombiano, jerga, abreviaturas (`d`), emojis, y la marca presente tanto en la columna como en el texto. **No trae** red social, usuario, fecha, engagement ni etiqueta: eso lo añade el simulador (§3.8). Como un comentario puede contener comas, el CSV se escribe siempre con el módulo `csv` (con comillas).

**Cambios propuestos al script** (se pueden ajustar):

| # | Cambio | Por qué | Prioridad |
|---|---|---|---|
| 1 | Exponer `generate(rng, marca=None, sentimiento=None) -> Comentario` como función pura, con un `random.Random` inyectado (sin `random.choice` global) | Determinismo e idempotencia (§3.6) | Requerido |
| 2 | Aceptar `marca` y `sentimiento` opcionales | El simulador controla los pesos de marca en el tiempo (§3.8) y los escenarios de demo (§3.7) | Requerido |
| 3 | Devolver una tercera columna opcional `sentimiento` ∈ `positivo\|negativo\|neutro` (la plantilla ya lo conoce) | *Ground truth* para medir la precisión del NLP (§5.10). Sin ella esa métrica se omite | Recomendado |
| 4 | Normalizar la marca a una clave canónica (`Postobón` → `postobon`) con los alias de `brands.yaml` | Joins y dimensiones estables (`dim_brand`) | Requerido |
| 5 | Ampliar la variedad: más plantillas por marca y sentimiento, aperturas y cierres intercambiables, comentarios neutros/informativos y algunos con dos marcas ("Pepsi vs Postobón") | Menos repetición léxica y relaciones marca–marca para el análisis de grafos | Recomendado |
| 6 | Quitar (si los tiene): `sleep`, bucles infinitos, `print` o escritura directa a archivo, pesos de marca fijos, semilla global | El ritmo, el estado y la selección de marca los controla el simulador | Requerido |
| 7 | Mezclar longitudes (frase corta y 2–3 frases) y conservar el ruido (typos, jerga, emojis) | Los posts de Reddit usan la primera frase como título (§3.8); el ruido ejercita la limpieza en Trusted | Opcional |

#### 3.2.1 Estado del script real (`generar_comentarios.py`, v3 con plantillas ampliadas)

Ya cumple buena parte del cambio #5 (4 categorías, plantillas generativas, ruido de redes) y su función `construir(rng, marca, cat, …)` ya recibe `rng`, marca y categoría, por lo que se parece mucho a la función pura de los cambios #1 y #2. Puntos que resuelve el Lote 1.1 (HU-008), con la decisión adoptada:

| # | Hallazgo | Decisión (adoptada, ADR-13) |
|---|---|---|
| 1 | `MARCAS` trae 5 marcas (Postobon, CocaCola, Sprite, Pepsi, Colombiana); v1 usa 3. | Leer la lista de `brands.yaml` (`postobon`, `pepsi`, `sprite`). CocaCola y Colombiana quedan fuera de v1 y se pueden añadir después. Clave canónica sin tilde (`postobon`), `display` con tilde. |
| 2 | El script no tiene *sentimiento*, solo `categoria` ∈ `gusto\|asco\|nostalgia\|adiccion`. | `gusto → positive`, `asco → negative`, `informativo → neutral` (categoría nueva, fila 3), `nostalgia` y `adiccion → null` (ambiguas, sin ground truth). El parámetro `sentimiento` de `generate()` restringe la categoría (`positive → gusto`, `negative → asco`, `neutral → informativo`). |
| 3 | No hay comentarios neutros/informativos ni con dos marcas. Sin neutral la precisión del NLP se mide en solo 2 clases. | Añadir una categoría `informativo` (→ `neutral`) y plantillas comparativas ("Pepsi vs Postobón"). Cambio #5, incluido en HU-008 (mínimo 15 plantillas informativas y 15 comparativas). |
| 4 | `ruido_texto` pasa a minúsculas/mayúsculas, quita tildes y mete *typos* en palabras de más de 4 letras: puede dañar el nombre de la marca ("Postobón" → "Pstoobón"). | `brands[]` = marca elegida (se conoce de antemano) + otras detectadas por alias **normalizados** (sin tildes, sin mayúsculas). Proteger los tokens de marca del *typo*. |
| 5 | Usa un `random.Random(seed)` secuencial (el resultado depende del orden) y `--unico` usa `hash()` de Python (varía entre procesos). | Un `Random(f"{seed}\|{fuente}\|{seq}")` por evento (§3.6). Para unicidad en batch usar `hashlib`. El modo en línea no exige unicidad (§3.5). |
| 6 | Los cierres ya incluyen hashtags (`#Colombia`, `#<marca>`, `#Nostalgia`…), contrario a lo que asume §3.8. | Extraer `hashtags[]` del texto con regex; el simulador solo añade los que falten según la probabilidad por red (§3.8). |
| 7 | CSV en `utf-8-sig`, `-n` por defecto 20 (el docstring dice 2.000.000) y salida solo `marca,comentario[,categoria]`. | `scripts/generate_comments.py` conserva el CLI; la salida pasa a `Comentario(marca, texto, categoria, sentimiento)` y el CSV agrega `sentimiento`. |

**Marcas** (`src/simulator/config/brands.yaml`). La clave canónica es la que viaja en `brands[]`; hay que registrar cada marca que produzca el generador (v1: `postobon`, `pepsi`, `sprite`):

```yaml
brands:
  postobon:
    display: "Postobón"
    aliases: ["Postobón", "Postobon"]
    role: propia            # confirmado en Fase 0
    facebook_page: "Postobón (sim)"
    instagram_account: "postobon.sim"
  pepsi:
    display: "Pepsi"
    aliases: ["Pepsi", "Pepsi Cola"]
    role: competidor
    facebook_page: "Pepsi (sim)"
    instagram_account: "pepsi.sim"
  sprite:
    display: "Sprite"
    aliases: ["Sprite"]
    role: competidor
    facebook_page: "Sprite (sim)"
    instagram_account: "sprite.sim"
```

**Modos de uso:**
- *En línea:* el bucle de emisión (§3.5) llama a `generate(rng, marca, sentimiento)`.
- *Batch:* `python scripts/generate_comments.py --n 50000 --seed 7 --out comentarios.csv` (histórico y snapshots).

### 3.3 Reparto por fuente y etiquetas

Cada comentario se publica por **una** red simulada. Las tres comparten el texto del generador y difieren en el `payload` y en la mezcla de tipos:

| Fuente simulada | Naturaleza | `kind` | Contexto simulado |
|---|---|---|---|
| **Reddit** | Foro, hilos | `post` 30% · `comment` 70% | Subreddit de una lista (`r/colombia`, `r/medellin`, …) |
| **Facebook** | Red social, páginas de marca | `post` 15% · `comment` 85% | Página de la marca comentada (`facebook_page` en `brands.yaml`) |
| **Instagram** | Red visual, cuentas de marca | `comment` 100% | Publicación (`media_id`, `media_type`) de la cuenta de la marca (`instagram_account`) |

**Etiquetas de salida del NLP** (ADR-05): `sentiment_label` ∈ `negative|neutral|positive` (el modelo devuelve `NEG/NEU/POS`; se mapea) y `emotion_label` ∈ 7 clases `joy, sadness, anger, fear, disgust, surprise, neutral` (la clase neutra del modelo de emoción se mapea a `neutral`; verificar el nombre exacto de la etiqueta al integrar). El mapeo vive en `src/common/label_map.yaml`.

**Ground truth:** `sim.label_sentiment` ∈ `positive|negative|neutral|null`, tomado de la columna opcional `sentimiento` del generador (cambio #3). Mapeo desde la categoría del script en §3.2.1 #2 (ADR-13): `nostalgia` y `adiccion` → `null`. No hay ground truth de emoción. **No se usa** como feature ni como respuesta del pipeline: solo sirve para medir la precisión del NLP (insumo para la sustentación). El sentimiento y la emoción oficiales los produce el modelo.

### 3.4 Reproducibilidad y snapshots (opcional)

Ya no hay preparación offline obligatoria. Todo el aleatorio sale de una semilla (`SIM_SEED`), de modo que misma semilla + fuente + `seq` ⇒ mismo comentario (con los mismos pesos de marca; los escenarios de §3.7 son la excepción y quedan marcados en `sim.scenario`). Para auditar o repetir una demo se puede guardar un snapshot con `scripts/generate_comments.py` en `gs://{GCP_PROJECT}-ta-datasets/snapshots/` (no se versiona en git).

### 3.5 Bucle de emisión

```
para cada fuente (tarea asyncio independiente):
  seq = leer cursor de sim-state (o 0)
  loop infinito:
     rng = Random(f"{SIM_SEED}|{fuente}|{seq}")                    # §3.6
     marca = elegir_marca(pesos_brands(seq), escenario_activo)     # §3.8
     c = generador.generate(rng, marca=marca, sentimiento=…)       # §3.2
     evento = construir_envelope(c, fuente, seq, rng)              # §3.8
     publicar(topic ta-raw-{fuente}, evento)
     seq += 1
     cada 50 mensajes: guardar cursor
     dormir uniform(1, 3) segundos
```

- Cadencia: ~0.5 msg/s por fuente → ~1.5 msg/s en total (~130k mensajes/día). Trivial para Pub/Sub y Dataflow.
- El generador es inagotable: ya no hay "vueltas" de dataset. El **texto exacto puede repetirse** entre eventos (plantillas); eso **no** es un duplicado, porque `event_id` depende de `seq`. Los duplicados se miden por `event_id`, no por texto.

### 3.6 Identidad, idempotencia y estado

- `event_id = uuid5(NAMESPACE_URL, "{origin}|{source}|{seed}|{seq}")`. Determinista → reenviar tras un reinicio produce el **mismo** id (Dataflow lo deduplica). La semilla forma parte del id: si se cambia, los ids nuevos no chocan con los anteriores.
- Entrega **al menos una vez**. Un reinicio puede repetir hasta 50 mensajes; es aceptable y deduplicable.
- **Cursor** persistido en `gs://…-ta-datasets/sim-state/{source}.json` → `{"seed": 7, "seq": 1207, "updated_at": "…"}`. Si la semilla configurada no coincide con la del cursor, se reinicia en `seq = 0`.
- Todo el aleatorio sale de `random.Random(f"{seed}|{source}|{seq}")`. **No usar `hash()` de Python** para sembrar: varía entre procesos (aleatorización de hash de `str`) y rompería la reproducibilidad.

### 3.7 Escenarios de demo (opcional, Fase 6)

Para mostrar alertas en la sustentación sin esperar una tendencia real:

`POST /control/scenario` `{"brand": "postobon", "sentiment": "negative", "multiplier": 5, "duration_s": 600, "source": "instagram"}` (`source` opcional) → durante ese tiempo el simulador fuerza esa marca y ese sentimiento al generador (p. ej. una ola de comentarios negativos sobre `postobon`), sube la frecuencia y marca los eventos con `sim.scenario`. Cloud Scheduler puede dispararlo. Protegido con ID token de Cloud Run (no público). Requiere el cambio #2 del generador (§3.2).

### 3.8 Qué se inventa (campos sintéticos)

El **texto, la marca y (opcionalmente) el sentimiento** vienen del generador. Todo lo demás se simula en `src/simulator/enrich/`:

| Campo | Cómo se genera |
|---|---|
| **Selección de marca** | El simulador elige la marca con **pesos de `brands.yaml` que varían con `seq`** (ciclos suaves con deriva, para crear tendencias reproducibles) y se la pasa al generador. `brands[]` = marca elegida + otras marcas de `brands.yaml` detectadas por alias en el texto (p. ej. comentarios que comparan dos marcas). Es `[]` solo si el texto no menciona ninguna. |
| **Usuarios** | Pool estable por fuente (~2.000 usuarios, semilla fija, `Faker('es_CO')`). Actividad tipo Zipf (pocos publican mucho). Mismo pool en stream e histórico → los usuarios se repiten y se pueden unir/analizar relaciones. |
| **Handle** | Estilo por red: Reddit `u/usuario`; Facebook nombre visible (`María Gómez`); Instagram `@usuario_89`. |
| **Seguidores** | Lognormal por usuario (estable). Facebook: amigos/seguidores; Instagram: seguidores; Reddit: aproximación (karma). |
| **Engagement** | Reddit: `score` / `upvote_ratio` / `num_comments`. Facebook: reacciones (total y desglose `like/love/haha/wow/sad/angry`), `shares_count`, `comments_count`. Instagram: `like_count`, `comments_count` (sin shares). Escalado por seguidores, con leve impulso para sentimiento extremo y, en Facebook, un sesgo leve del desglose de reacciones según `label_sentiment` cuando existe. Se interpreta como *snapshot* al momento de la recolección. |
| **Fechas** | `event_time` = instante de emisión − lag aleatorio (0–5 s). `ingest_time` = instante de publicación. Siempre UTC. |
| **Hashtags** | El generador ya inserta algunos en los cierres (`#Colombia`, `#<marca>`…): se extraen del texto con regex a `hashtags[]` (§3.2.1 #6). El simulador añade 1–2 más derivados de la marca y del sector (`#Postobon`, `#Colombia`) con probabilidad por red (por defecto Instagram ~60%, Facebook ~15%, Reddit ~0%) solo si faltan, los agrega al final del texto y los registra en `hashtags[]`, en su **forma canónica**: minúsculas, sin `#`, sin tildes, solo `[a-z0-9_]`, sin repetidos y en orden de aparición (el texto conserva el hashtag tal como se escribió: `#Postobón` ⇒ `"postobon"`). Configurable en `sim.yaml`. |
| **Subreddit / página / cuenta** | Reddit: lista de subreddits. Facebook: `facebook_page` de la marca. Instagram: `instagram_account` de la marca y `media_type` aleatorio. URLs ficticias (`sim.local`). |
| **`kind` y `parent_id`** | Según §3.3. Un `comment` de Reddit o Facebook lleva el `parent_id` de un post reciente simulado. |

> **Limitación honesta (para el informe):** los comentarios salen de plantillas y la marca/sentimiento están controlados por el simulador, así que la asociación marca→sentimiento es artificial y el vocabulario es acotado (las métricas de NLP serán optimistas). El objetivo es **ingeniería de datos** (ingesta, calidad, linaje, latencia), no conclusiones de negocio válidas.

### 3.9 Caos controlado (para que Calidad tenga qué medir)

Activable con `SIM_CHAOS_ENABLED=true`. Tasas configurables en `sim.yaml`:

| Defecto | Tasa por defecto | Qué ejercita |
|---|---|---|
| Duplicado (re-publica mismo `event_id`) | 0.5% | KPI Unicidad / Duplicados |
| Evento tardío (`event_time` −2 a −30 min) | 1% | Ventanas, watermarks, latencia |
| Campos opcionales nulos (`lang`, `followers`) | 1% | KPI Completitud |
| Mensaje malformado (JSON roto o falta campo requerido) | 0.2% | Validación + DLQ |

Hay que poder apagarlo para medir la línea base de los KPIs.

---

## 4. Flujo extremo a extremo

| # | Etapa | Componente | Entrada | Salida | Latencia objetivo |
|---|---|---|---|---|---|
| 1 | Emisión | `stream-simulator` (+ generador de comentarios) | Comentario `marca,comentario` generado en proceso | Mensaje Pub/Sub `ta-raw-{source}` | 1–3 s entre mensajes |
| 2 | Validación + dedup | Dataflow | Pub/Sub | Válidos continúan; inválidos → `ta-dlq` + `gs://…/dlq/` | segundos |
| 3 | Anonimización | Dataflow | Envelope con `author.user_id/handle` | `author.user_hash` (HMAC); sin handle ni id en claro | segundos |
| 4 | Normalización | Dataflow | `payload` específico de red | `likes/shares/comments` normalizados | segundos |
| 5 | NLP | Dataflow (`RunInference`) | `text` | `sentiment_*`, `emotion_*` | segundos |
| 6 | Escritura | Dataflow | Evento enriquecido | BigQuery `posts_enriched` + GCS `raw/` y `enriched/` | < 1 min |
| 7 | Tendencias | Dataflow (ventanas) | Eventos enriquecidos | BigQuery `trend_windows` | **< 5 min** (KPI) |
| 8 | Exportación | `gcs-to-s3` | GCS `enriched/` | S3 `raw/origin=stream/` | cada 15 min |
| 9 | Carga histórica | `historical/loader` (mismo generador, en batch) | Comentarios generados con semilla propia | S3 `raw/origin=historical/` | one-shot |
| 10 | Raw → Trusted | Glue | S3 `raw/` | S3 `trusted/posts/` | horario |
| 11 | Trusted → Curated | Glue/dbt | `trusted/` | S3 `curated/*` | horario |
| 12 | Consulta | Athena → QuickSight | `curated/` | Dashboards | **< 3 s** (KPI) |

---

## 5. Contratos de comunicación

> Esta es la sección que hace innecesario enviarme código. Cada subsección = una interfaz.

### 5.1 Convenciones globales

**Variables de entorno comunes** (todo código las lee del entorno; nada hardcodeado):

| Variable | Valor por defecto / ejemplo | Usada por |
|---|---|---|
| `ENV` | `dev` | todos |
| `GCP_PROJECT` | `project-0568d9ba-848f-413c-88b` *(confirmado; nombre `proyectoDatos`, número `63567159332`)* | simulador, Dataflow, exportador |
| `GCP_REGION` | `us-central1` *(ADR-12)* | idem |
| `AWS_REGION` | `us-east-1` *(confirmada)* | exportador, loader, Glue |
| `AWS_ACCOUNT_ID` | `648617469176` *(confirmado; va en `terraform.tfvars`, no en el código)* | Terraform, exportador, loader |
| `TA_HMAC_SECRET_NAME` | `ta-hmac-key` | Dataflow, loader |
| `SIM_SEED` | `7` | simulador, loader histórico |
| `SIM_CHAOS_ENABLED` | `false` | simulador (§3.9) |

**Nombres:** prefijo `ta-` en todos los recursos. Labels/tags: `project=trend-analyzer`, `env=dev`, `owner=alejandro`. En AWS se aplican con `default_tags` del proveedor; en GCP, con `labels` en los recursos que los admiten (el bucket de estado lo hace; `google_project_service`, el presupuesto y los canales de notificación no). El bucket de estado de GCP es una excepción de nombre: `{GCP_PROJECT}-ta-tfstate` (§5.14).
**Código:** Python 3.11, type hints, `pytest`, logs JSON a stdout, configuración por env vars + YAML, cero secretos en el repo.
**Terraform:** ≥1.6, proveedores `hashicorp/google ~> 6.0` y `hashicorp/aws ~> 5.0`. Un módulo por recurso lógico en `infra/{gcp,aws}/modules/`, entornos en `infra/{gcp,aws}/environments/dev/` y un directorio `bootstrap/` por nube (estado local, se aplica una vez; §5.14). Cada raíz (bootstrap y entorno) expone `outputs.tf` (§5.9); los módulos declaran sus outputs junto a su `main.tf`.
**Tiempo:** todo en **UTC**, ISO-8601 con milisegundos y `Z`.
**Autenticación de Terraform (dev):** en AWS, el proveedor `hashicorp/aws` usa las credenciales del perfil local de AWS CLI (`aws configure`, o `AWS_PROFILE`), sin claves en el código ni en el repositorio; además el proveedor lleva `allowed_account_ids` y se detiene si el perfil apunta a otra cuenta. En GCP usa Application Default Credentials, sin llaves JSON: `gcloud auth login`, `gcloud config set project project-0568d9ba-848f-413c-88b`, `gcloud auth application-default login` y `gcloud auth application-default set-quota-project project-0568d9ba-848f-413c-88b`. La cuenta de Alejandro debe ser Owner o Editor del proyecto. La autenticación en CI se define en el Lote 7.1.

**Contratos:** §5 es la fuente de verdad; `project/contracts/` es su copia ejecutable y verificada por pruebas (§5.13).

### 5.2 Envelope canónico (v1.0)

Todo evento —de stream o histórico, de cualquier fuente— tiene esta forma al salir del simulador/loader. Es el **contrato más importante del proyecto**.

```json
{
  "schema_version": "1.0",
  "event_id": "7d1c3f9e-…",
  "origin": "stream",
  "source": "reddit",
  "generator": "comment-gen@1.0",
  "seq": 1207,
  "event_time": "2026-10-17T14:03:11.482Z",
  "ingest_time": "2026-10-17T14:03:13.020Z",
  "author": {
    "user_id": "u_000481",
    "handle": "u/mariag_89",
    "followers": 412
  },
  "text": "Opinión personal: soy fan d Postobón y no pienso cambiar. Volvería mil veces. 🙌",
  "lang": "es",
  "brands": ["postobon"],
  "hashtags": [],
  "payload": { },
  "sim": {
    "label_sentiment": "positive",
    "scenario": null,
    "chaos": []
  }
}
```

| Campo | Tipo | Req. | Notas |
|---|---|---|---|
| `schema_version` | string | ✔ | `"1.0"`. Cambia solo con cambio incompatible. |
| `event_id` | string (UUID) | ✔ | Ver §3.6. Clave de deduplicación. |
| `origin` | enum `stream`\|`historical` | ✔ | |
| `source` | enum `reddit`\|`facebook`\|`instagram` | ✔ | **Red social simulada**, también en histórico. |
| `generator` | string | ✔ | Nombre y versión del generador de comentarios (`comment-gen@1.0`). |
| `seq` | int | ✔ | Posición del evento en la secuencia de su fuente (§3.5). |
| `event_time` | timestamp | ✔ | Cuándo "se publicó". |
| `ingest_time` | timestamp | ✔ | Cuándo se emitió. |
| `author.user_id` | string | ✔ | Estable. **Solo existe hasta Dataflow/loader.** |
| `author.handle` | string | ✔ | **Solo existe hasta Dataflow/loader.** |
| `author.followers` | int | – | Puede ser nulo (caos §3.9). |
| `text` | string | ✔ | Texto plano (comentario del generador + hashtags añadidos, §3.8). |
| `lang` | string | – | `es` en v2.2. |
| `brands` | string[] | ✔ | Puede ser `[]`. Claves de `brands.yaml`. |
| `hashtags` | string[] | ✔ | Puede ser `[]`. Forma canónica: minúsculas, sin `#`, sin tildes, solo `[a-z0-9_]` (1–100), sin repetidos y en orden de aparición en el texto (§3.8). |
| `payload` | objeto | ✔ | Específico de red (abajo). |
| `sim.*` | objeto | ✔ | `label_sentiment` (`positive`\|`negative`\|`neutral`\|`null`), `scenario` (string o `null`) y `chaos` (lista). Ground truth y trazas. **Nunca se usa como feature.** |

**`payload` por fuente:**

```json
// source = "reddit"
{ "reddit_id": "t1_k3x9a1", "kind": "comment", "subreddit": "r/colombia",
  "title": null, "parent_id": "t3_j8f2ds", "score": 27, "upvote_ratio": null,
  "num_comments": null, "permalink": "/r/colombia/comments/j8f2ds/_/k3x9a1/", "flair": null }

// source = "facebook"
{ "comment_id": "1029384756_5647382910", "kind": "comment", "page_id": "sim-page-postobon",
  "page_name": "Postobón (sim)", "post_id": "sim-post-000812", "parent_id": "sim-post-000812",
  "reactions_total": 14,
  "reactions": { "like": 9, "love": 3, "haha": 1, "wow": 0, "sad": 0, "angry": 1 },
  "shares_count": 2, "comments_count": 3,
  "permalink": "https://sim.facebook.local/postobon/posts/000812" }

// source = "instagram"
{ "comment_id": "17912345678901234", "kind": "comment", "media_id": "17855544433322211",
  "media_type": "IMAGE", "account_username": "postobon.sim", "parent_id": null,
  "like_count": 31, "comments_count": 4,
  "permalink": "https://sim.instagram.local/p/000812/" }
```

Los campos imitan el estilo de cada red (Reddit API, Graph API de Meta) pero son simulados. En Facebook, cuando `kind = "post"`, `parent_id` es `null`. `media_type` ∈ `IMAGE`\|`VIDEO`\|`CAROUSEL_ALBUM`.

**Envelope enriquecido** (lo que sale de Dataflow / loader hacia BigQuery y S3) = envelope **menos** `author.user_id`, `author.handle`, **más**:

```json
{
  "author": { "user_hash": "9a4c…(HMAC-SHA256 hex)", "followers": 412 },
  "likes": 27, "shares": null, "comments": null,
  "sentiment_label": "positive", "sentiment_score": 0.93,
  "emotion_label": "joy", "emotion_score": 0.88,
  "nlp_model_version": "robertuito-sentiment@<hash>+robertuito-emotion@<hash>",
  "process_time": "2026-10-17T14:03:13.900Z"
}
```

Normalización de engagement: `likes` = `score` (Reddit) | `reactions_total` (Facebook) | `like_count` (Instagram); `shares` = `shares_count` (Facebook) | `null` (Reddit, Instagram); `comments` = `num_comments` (Reddit) | `comments_count` (Facebook, Instagram).

**Reglas de validación.** Las aplica `contracts/` además de los tipos de arriba (§5.13):

| Campo | Regla |
|---|---|
| `event_time`, `ingest_time`, `process_time` | UTC, ISO-8601 con **milisegundos** y `Z` (`2026-10-17T14:03:11.482Z`). Sin milisegundos o con otra zona se rechazan. |
| `event_id` | UUID en minúsculas, calculado con la fórmula de §3.6. |
| `generator` | `nombre@versión`, p. ej. `comment-gen@1.0`. |
| `brands[]` | Claves canónicas `[a-z0-9_]+`, sin repetidos. |
| `hashtags[]` | Forma canónica descrita arriba. La función de referencia es `normalize_hashtag()` (hoy en `contracts/tests/test_contracts.py`); el simulador la implementa en `src/common/` (Lote 1.1). |
| `text` | No vacío. |
| `lang` | Dos letras minúsculas (`es`) o `null`. |
| `author.followers` | Entero ≥ 0 o `null`. |
| `sim.chaos` | Lista sin repetidos; valores `duplicate`, `late`, `null_fields`, `malformed` (§3.9). |
| Campos no definidos | El envelope, `author` y `sim` los rechazan. |

**Invariantes por fuente y `kind`:**

| Fuente · `kind` | Regla |
|---|---|
| Reddit · `post` | `reddit_id` empieza por `t3_`; `title` no vacío; `parent_id` `null`; `upvote_ratio` (número) y `num_comments` (entero) presentes. |
| Reddit · `comment` | `reddit_id` empieza por `t1_`; `parent_id` es texto; `title`, `upvote_ratio` y `num_comments` son `null`. |
| Facebook · `post` | `parent_id` `null`. |
| Facebook · `comment` | `parent_id` es texto. |
| Instagram | `kind` es siempre `comment`; `media_type` ∈ `IMAGE`\|`VIDEO`\|`CAROUSEL_ALBUM`. |

**Reglas del envelope enriquecido:**
- `author` solo contiene `user_hash` (hex de 64 caracteres) y `followers`. Si aparece `handle` o `user_id`, se rechaza.
- `sentiment_label` ∈ `negative`\|`neutral`\|`positive` (no se aceptan las etiquetas crudas `NEG/NEU/POS` del modelo); `emotion_label` ∈ las 7 clases de §3.3.
- `sentiment_score` y `emotion_score` ∈ [0, 1].
- `nlp_model_version` tiene la forma `robertuito-sentiment@<hash>+robertuito-emotion@<hash>`. Si se adopta el plan B de ADR-05, se cambia el patrón en el schema y aquí.
- `process_time` es obligatorio.
- `likes` es entero (en Reddit puede ser negativo). `shares` es `null` en Reddit e Instagram y entero en Facebook. `comments` es entero en Facebook e Instagram; en Reddit es entero en posts y `null` en comentarios.

### 5.3 Pub/Sub

| Recurso | Nombre | Notas |
|---|---|---|
| Topic | `ta-raw-reddit`, `ta-raw-facebook`, `ta-raw-instagram` | Un topic por fuente. |
| Suscripción | `ta-raw-{source}-df` | Pull, consumida por Dataflow. Ack deadline 60 s. |
| Topic DLQ | `ta-dlq` | Mensajes que no pasan validación. |
| Topic alertas | `ta-alerts` | Fase 6. |
| Retención | 7 días | |

- **Cuerpo:** JSON UTF-8 del envelope (§5.2).
- **Atributos del mensaje:** `event_id`, `source`, `origin`, `schema_version`. Dataflow usa `event_id` como `id_label` para deduplicar.
- **Publica:** `sa-simulator`. **Consume:** `sa-dataflow`.
- Sin ordenamiento garantizado (no se necesita).

### 5.4 Google Cloud Storage

| Bucket | Contenido |
|---|---|
| `{GCP_PROJECT}-ta-datasets` | Estado del simulador (cursor) y snapshots opcionales del generador |
| `{GCP_PROJECT}-ta-lake` | Salida de Dataflow |

```
gs://{GCP_PROJECT}-ta-datasets/
  snapshots/comentarios-{seed}-{n}.csv      # opcional: salida de scripts/generate_comments.py (no se versiona en git)
  sim-state/{source}.json                   # cursor del simulador
gs://{GCP_PROJECT}-ta-lake/
  raw/source={s}/dt={YYYY-MM-DD}/hr={HH}/part-*.jsonl.gz     # envelope tal cual llegó (auditoría/replay)
  enriched/source={s}/dt={YYYY-MM-DD}/hr={HH}/part-*.parquet # envelope enriquecido, anonimizado
  dlq/dt={YYYY-MM-DD}/part-*.jsonl.gz
  export-state/watermark.json
```

Particiones por **fecha/hora de proceso**. El Parquet de `enriched/` usa el **esquema plano** de `posts_enriched` (§5.5): `author.user_hash` → `user_hash`, `author.followers` → `followers`, `sim.label_sentiment` → `label_sentiment`, `brands[0]` → `brand_primary`, `payload` como JSON en string. `raw/` conserva el handle simulado (dato sintético, solo para auditoría y replay; acceso restringido). **Solo `enriched/` sale hacia AWS.**

### 5.5 BigQuery

Dataset: `ta_stream` (ubicación = región de GCP).

**`posts_enriched`** — partición por `DATE(event_time)`, clustering `source, brand_primary`.

| Columna | Tipo |
|---|---|
| `event_id` | STRING (único) |
| `origin`, `source`, `generator` | STRING |
| `event_time`, `ingest_time`, `process_time` | TIMESTAMP |
| `user_hash` | STRING |
| `followers` | INT64 |
| `text`, `lang` | STRING |
| `brands` | ARRAY&lt;STRING&gt; |
| `brand_primary` | STRING |
| `hashtags` | ARRAY&lt;STRING&gt; |
| `likes`, `shares`, `comments` | INT64 |
| `sentiment_label` | STRING (`negative`/`neutral`/`positive`) |
| `sentiment_score`, `emotion_score` | FLOAT64 (en [0, 1]) |
| `emotion_label` | STRING (`joy`, `sadness`, `anger`, `fear`, `disgust`, `surprise`, `neutral`) |
| `label_sentiment` | STRING (ground truth, nullable, solo evaluación) |
| `nlp_model_version` | STRING |
| `payload` | JSON |

**`trend_windows`** — ventanas deslizantes de 5 min (paso 1 min) por `brand` × `source`.

| Columna | Tipo |
|---|---|
| `window_start`, `window_end` | TIMESTAMP |
| `brand`, `source` | STRING |
| `n_posts`, `n_negative`, `n_positive` | INT64 |
| `avg_sentiment` | FLOAT64 (media con signo en [-1, 1]: positivo +score, negativo −score, neutro 0) |
| `emotion_counts` | JSON |
| `zscore_volume`, `zscore_sentiment` | FLOAT64 (vs. baseline móvil) |
| `is_alert` | BOOL |

**`dlq_events`** — `received_at`, `source`, `error_type`, `error_detail`, `raw_message` (STRING).
**`dq_results`** — una fila por verificación de calidad ejecutada (§5.10). Partición por `DATE(run_ts)`, clustering `layer, kpi`. Mismo reporte, además, en `s3://…/quality/{run_date}/`.

| Columna | Tipo | Notas |
|---|---|---|
| `run_id` | STRING, obligatorio | Identifica la corrida. |
| `run_ts` | TIMESTAMP, obligatorio | Partición. |
| `layer` | STRING, obligatorio | Capa verificada. Valores propuestos: `stream`, `raw`, `trusted`, `curated` (§9, #8). |
| `table_name` | STRING, obligatorio | |
| `suite` | STRING | Suite de Great Expectations o test de dbt. |
| `check_name` | STRING, obligatorio | |
| `kpi` | STRING | `completitud`, `unicidad`, `duplicados`, `anonimizacion`, `latencia`, `precision_nlp` u `otro` (§5.10). |
| `observed_value`, `threshold` | FLOAT64 | |
| `passed` | BOOL, obligatorio | |
| `details` | JSON | |

### 5.6 Puente GCP → AWS (exportador)

- **Componente:** Cloud Run Job `ta-gcs-to-s3`, disparado por Cloud Scheduler cada 15 min.
- **Lee:** `gs://…-ta-lake/enriched/` con watermark en `export-state/watermark.json`.
- **Escribe:** `s3://{lake_bucket}/raw/origin=stream/source={s}/dt={YYYY-MM-DD}/hr={HH}/{mismo nombre}.parquet`.
- **Idempotente:** si el objeto ya existe en S3 con el mismo tamaño, lo omite. Actualiza el watermark solo tras subir con éxito.
- **Autenticación recomendada:** el ID token OIDC de `sa-exporter` asume el rol AWS `ta-gcp-exporter-role` vía `AssumeRoleWithWebIdentity` (requiere un OIDC identity provider de Google en IAM). Evita llaves estáticas. *Plan B:* usuario IAM con llaves guardadas en Secret Manager (menos seguro).
- **Transporte:** HTTPS/TLS; objetos cifrados con SSE-KMS en destino.

### 5.7 AWS: S3, Glue, Athena, QuickSight

**Bucket:** `ta-{ENV}-{aws_account_id}-lake`

```
s3://…-lake/
  raw/origin={stream|historical}/source={s}/dt={YYYY-MM-DD}/…parquet
  trusted/posts/source={s}/dt={YYYY-MM-DD}/…parquet
  curated/{tabla}/…parquet
  quality/{run_date}/…                      # reportes de Great Expectations
  athena-results/
```

Formato: **Parquet + Snappy**. Particiones registradas con **partition projection** en Athena (sin crawlers, más simple y barato).

- **Raw** (`ta_raw.posts_raw`): rutas `raw/origin=…/source=…/dt=…/hr=…/`; también el histórico escribe `hr`. Claves de partición `part_origin`, `part_source`, `part_dt`, `part_hr` (las columnas `origin` y `source` ya viajan dentro del Parquet). Columnas = las de BigQuery `posts_enriched`.
- **Trusted** (`ta_trusted.posts`): `trusted/posts/source=…/dt=…/`, con `dt = DATE(event_time)` UTC; añade `text_clean`; `source` es partición, no columna.
- DDL en `project/contracts/athena/`, copia verificada de esta sección (§5.13). Las columnas de Curated están definidas abajo.

| Capa | Glue DB | Contenido |
|---|---|---|
| Raw | `ta_raw` | Tal como llega (ya anonimizado). Tabla `posts_raw`. |
| Trusted | `ta_trusted` | Unión stream + histórico, deduplicado por `event_id`, tipos correctos, texto limpio, reglas de calidad aplicadas. Tabla `posts`. |
| Curated | `ta_curated` | Modelo analítico (abajo). |

**Curated (esquema estrella + agregados + aristas):**

Parquet + Snappy en `curated/{tabla}/`. Las llaves foráneas son **naturales** (sin surrogate keys):
- `fact_posts.source` → `dim_source.source`
- `fact_posts.brand_primary` → `dim_brand.brand`
- `fact_posts.user_hash` → `dim_user.user_hash`
- `fact_posts.date_key` → `dim_date.date_key`

Solo `fact_posts` está particionada (por `dt`, con partition projection). Las demás tablas se **regeneran completas** en cada corrida.

| Tabla | Columnas |
|---|---|
| `fact_posts` | Grano = `event_id`. `event_id`, `date_key` (entero `yyyymmdd`), `origin`, `source`, `brand_primary`, `brands[]`, `user_hash`, `event_time`, `likes`, `shares`, `comments`, `sentiment_label`, `sentiment_score`, `emotion_label`, `emotion_score`, `label_sentiment`. Partición `dt` (ver §9, #9). |
| `dim_date` | `date_key`, `full_date`, `year`, `month`, `day`, `iso_week`, `day_of_week` (1 = lunes … 7 = domingo), `is_weekend`. |
| `dim_source` | `source` (`reddit`\|`facebook`\|`instagram`), `display_name`, `kind_mix` (descripción de la mezcla de `kind`, §3.3). |
| `dim_brand` | `brand` (clave de `brands.yaml`: `postobon`, `pepsi`, `sprite`), `display`, `role` (`propia`\|`competidor`). |
| `dim_user` | `user_hash`, `first_seen`, `last_seen`, `n_posts`, `max_followers`. **Nunca** `handle` ni `user_id`. |
| `agg_emotion_by_brand_hour` | Formato largo: `hour_start` (UTC, truncado a la hora), `brand`, `source`, `emotion_label`, `n_posts`. |
| `agg_trend_daily` | `full_date`, `brand`, `n_posts`, `n_negative`, `n_neutral`, `n_positive`, `avg_sentiment` (misma definición que `trend_windows`: con signo, en [-1, 1]). El día es UTC; los dashboards convierten a `America/Bogota` (§5.12). |
| `edges_user_brand` | Arista usuario → marca: `user_hash`, `brand`, `weight` (nº de menciones). |
| `edges_hashtag_cooccurrence` | Arista no dirigida: `hashtag_a`, `hashtag_b`, `weight`; siempre `hashtag_a < hashtag_b`. *(Cubre "análisis de relaciones" sin Neptune.)* |

**Orquestación:** EventBridge `rate(1 hour)` → Glue Workflow `ta-wf-raw-to-curated` (jobs `ta-job-raw-to-trusted`, `ta-job-trusted-to-curated`).
**Athena:** workgroup `ta-analytics`, resultados en `athena-results/`, límite de bytes escaneados por consulta.
**QuickSight:** datasets sobre `ta_curated` vía Athena, **SPICE** para cumplir < 3 s. Acceso por roles (IAM).

### 5.8 Identidades y permisos (mínimo privilegio)

| Identidad | Nube | Permisos |
|---|---|---|
| `sa-simulator` | GCP | `pubsub.publisher` en `ta-raw-*`; `storage.objectUser` en `…-ta-datasets` |
| `sa-dataflow` | GCP | `pubsub.subscriber`; `bigquery.dataEditor` en `ta_stream`; `storage.objectAdmin` en `…-ta-lake`; `secretmanager.secretAccessor` en `ta-hmac-key`; `dataflow.worker` |
| `sa-exporter` | GCP | `storage.objectViewer` en `…-ta-lake`; objeto escritura en `export-state/`; puede emitir OIDC |
| `ta-gcp-exporter-role` | AWS | `s3:PutObject`, `s3:HeadObject`, `kms:GenerateDataKey` solo sobre `raw/origin=stream/` |
| `ta-glue-role` | AWS | R/W en `raw/ trusted/ curated/ quality/`; Glue Catalog; logs |
| `ta-historical-loader` | AWS | `s3:PutObject` en `raw/origin=historical/` (perfil/SSO del desarrollador) |
| `ta-quicksight-role` | AWS | `athena:*` en workgroup `ta-analytics`; lectura `curated/` |

**Estado tras el Lote 1.0:** ninguna de estas identidades existe todavía. El bootstrap no crea service accounts ni roles; las de GCP llegan con el Lote 1.3 y las de AWS con las Fases 3 y 4. Mientras tanto, Terraform opera con las credenciales de Alejandro (§5.1).

### 5.9 Salidas de Terraform (outputs que consumen los demás)

| Módulo | Outputs |
|---|---|
| `infra/gcp` | `topic_names` (map source→topic), `subscription_names`, `datasets_bucket`, `lake_bucket`, `bq_dataset`, `simulator_url`, `service_accounts` (map) |
| `infra/aws` | `lake_bucket`, `glue_db_names`, `athena_workgroup`, `exporter_role_arn`, `glue_role_arn`, `kms_key_arn` |

**Outputs reales tras el Lote 1.0** (el resto de la tabla anterior llega con el Lote 1.3 y la Fase 4):

| Raíz | Outputs |
|---|---|
| `infra/gcp/bootstrap` | `tfstate_bucket`, `enabled_services`, `budget_name` |
| `infra/gcp/environments/dev` | `project_id`, `region`, `labels` |
| `infra/aws/bootstrap` | `tfstate_bucket`, `tflock_table`, `budget_name` |
| `infra/aws/environments/dev` | `aws_region` |

El código **nunca** hardcodea estos valores: los recibe como variables de entorno o de plantilla de Dataflow. Para pedirme algo nuevo, basta darme los `outputs` reales.

### 5.10 Reglas de calidad ↔ KPIs

| KPI | Regla | Dónde se mide |
|---|---|---|
| Completitud ≥ 95% | `event_id, event_time, source, text, user_hash` no nulos; ratio global de no-nulos | GE sobre `trusted/posts` y BigQuery |
| Unicidad ≥ 99% | `event_id` único | idem |
| Duplicados < 1% | `COUNT(*) − COUNT(DISTINCT event_id)` / `COUNT(*)` | idem (medir **en Trusted**, tras dedup) |
| 100% anonimización | No existen columnas `handle`/`user_id`; `user_hash` con regex hex de 64 | GE + test en CI |
| Latencia < 5 min (p95) | `process_time − event_time` en eventos no tardíos | BigQuery + Cloud Monitoring |
| Dashboard < 3 s | Tiempo de carga con SPICE | Prueba manual documentada |
| Precisión NLP *(extra)* | `sentiment_label` vs `label_sentiment` (solo eventos con etiqueta; se excluyen los `null`, §3.2.1 #2) | notebook / dbt model |

Resultados en BigQuery `dq_results` (columnas en §5.5) y `s3://…/quality/`. El valor de `dq_results.kpi` sigue la primera columna de esta tabla (`completitud`, `unicidad`, `duplicados`, `anonimizacion`, `latencia`, `precision_nlp`); el resto, `otro`.

### 5.11 Matriz "si construyes X, necesitas…"

| Si construyes… | Necesitas leer | Me tienes que dar |
|---|---|---|
| Simulador | §3, §5.2, §5.3, §5.4 | `outputs` de `infra/gcp` y el **script del generador** (o su lista de plantillas) |
| Pipeline Dataflow | §5.2, §5.3, §5.4, §5.5, §5.8 | Nombres reales de topics/buckets, región |
| Exportador | §5.4, §5.6, §5.8 | ARN del rol AWS, bucket S3 |
| Loader histórico | §3.2, §3.8, §5.2, §5.7 | Perfil AWS, bucket S3 |
| Jobs de Glue / dbt | §5.2, §5.7, §5.10 | Esquema real de Athena (`SHOW CREATE TABLE`) si ya existe algo |
| Calidad | §5.5, §5.7, §5.10 | — |
| Terraform | §5.1, §5.8, §5.9, §5.14 | Estado actual de `infra/` y `terraform.tfvars` (sin secretos) |
| Aplicar o ampliar la infraestructura base | §5.14 e `infra/BOOTSTRAP.md` | `billing_account_id`, montos de presupuesto y salida de `terraform plan` |
| Dashboard | §5.7 (Curated) | Región de QuickSight |
| Cambiar o validar un contrato | §5.2, §5.13 | — |

### 5.12 Parámetros operativos acordados

Valores por defecto que evitan decidir en cada lote. Se ajustan aquí primero.

| Tema | Valor acordado | HU |
|---|---|---|
| Eventos tardíos en ventanas | *Allowed lateness* de 30 min (cubre el caos de §3.9). Un evento más tardío se escribe en `posts_enriched` pero no recalcula ventanas; contador `late_events`. | HU-023 |
| Arranque en frío del z-score | Sin baseline no hay alertas: `is_alert = false` hasta tener 60 ventanas (1 h). Opcional: sembrar el baseline con `agg_trend_daily` del histórico. | HU-023 |
| Alertas repetidas | *Cooldown* de 15 min por marca × fuente × tipo de alerta. | HU-039 |
| Zona horaria | Almacenamiento en UTC; dashboards en `America/Bogota`. | HU-037 |
| Retención | GCS `raw/` y `dlq/`: 30 días · BigQuery `posts_enriched`: expiración de partición a 90 días (el histórico completo vive en S3) · S3 `athena-results/`: 7 días · Pub/Sub: 7 días. | HU-043 |
| Idempotencia en Glue | Se sobrescriben las particiones `dt` en lugar de añadir: mismo input ⇒ mismo output. | HU-026 |
| Contratos en CI | La salida del generador y del simulador (≥ 1.000 eventos por fuente, caos apagado) se valida contra `contracts/*.schema.json`. | HU-001, HU-041 |
| Secretos | *Pre-commit* con `gitleaks`; `terraform.tfvars` en `.gitignore` y `terraform.tfvars.example` versionado. | HU-041, HU-046 |
| Estado de Terraform | Remoto: GCS con versionado (GCP) y S3 con bloqueo (AWS). | HU-046 |
| Desarrollo local | `docker-compose` con emulador de Pub/Sub + simulador + `DirectRunner`, sin credenciales. | HU-047 |
| Canal de alertas | Correo. Un suscriptor de `ta-alerts` escribe un log estructurado (`severity=WARNING`, `alert=trend`) y una alerta basada en logs de Cloud Monitoring envía el correo mediante un canal de notificación de tipo email. Las direcciones van en `terraform.tfvars` (`alert_emails`, que no se versiona); por ahora el único destinatario es el correo de Alejandro. Sin SMTP ni secretos. | HU-039 |
| Prueba de humo E2E | `scripts/smoke_e2e.py` sigue un evento centinela (`event_id` conocido) desde el simulador hasta `ta_curated.fact_posts` y reporta latencia por etapa. | HU-048 |
| Retención del estado de Terraform | GCS: se conservan las 10 versiones no vigentes más recientes. S3: las versiones no vigentes expiran a los 90 días y los *uploads* multiparte incompletos a los 7. | HU-046 |
| Presupuestos | Solo avisan por correo; no cortan el gasto. GCP: 50 %, 90 % y 100 % del gasto real y 100 % del pronóstico. AWS: 50 %, 80 % y 100 % del gasto real y 100 % del pronóstico. Montos provisionales (§9 #10). | HU-046, HU-043 |

### 5.13 Relación con `contracts/` y sincronización

**Regla:** §5 es la fuente de verdad. `project/contracts/` es su copia ejecutable: JSON Schemas, DDL, ejemplos y pruebas. Un cambio de interfaz se hace **primero en §5** y, en el **mismo commit**, en `contracts/` (con `pytest project/contracts/tests -q` en verde).

| Archivo de `contracts/` | Qué fija | Sección |
|---|---|---|
| `envelope.v1.schema.json`, `envelope.enriched.v1.schema.json` | Envelope antes y después de Dataflow/loader, con las reglas de §5.2 | §5.2 |
| `payload.{reddit,facebook,instagram}.schema.json` | `payload` por fuente e invariantes por `kind` | §5.2 |
| `pubsub.attributes.schema.json` | Atributos de `ta-raw-{source}` | §5.3 |
| `bigquery/{posts_enriched,trend_windows,dlq_events,dq_results}.sql` | Las 4 tablas de `ta_stream` | §5.5 |
| `athena/{posts_raw,posts,curated}.sql` | Raw, Trusted y las 9 tablas de Curated | §5.7 |
| `examples/valid/` (14), `examples/invalid/` (31) | Casos que deben pasar y casos que deben fallar | – |
| `tests/test_contracts.py` | 18 pruebas | – |

**Placeholders de despliegue** en las DDL: `{{GCP_PROJECT}}` (BigQuery) y `{{lake_bucket}}` (Athena). Los `${part_dt}` y similares de Athena son variables de partition projection y **no** se sustituyen.

**Qué verifican las 18 pruebas:**
- Los schemas son JSON Schema Draft 2020-12 válidos.
- Todos los ejemplos válidos pasan y todos los inválidos fallan. Los inválidos cubren, entre otros: identidad filtrada en el enriquecido (`handle`, `user_id`), hashtags no canónicos, marca no canónica, timestamps sin milisegundos o no UTC, payload de otra fuente, violaciones de `kind`, score fuera de rango y atributos de Pub/Sub mal tipados.
- Cada combinación fuente × `kind` tiene un ejemplo válido.
- `event_id` sigue la fórmula uuid5 de §3.6.
- El enriquecido es el envelope menos la identidad más el NLP; los atributos de Pub/Sub coinciden con el envelope; la normalización de engagement coincide con §5.2; los hashtags de los ejemplos son canónicos y coinciden con el texto.
- Coherencia entre capas: las 4 tablas de BigQuery existen; `posts_raw` coincide columna a columna con `posts_enriched`; `posts` es Raw más `text_clean` con `source` como partición; la partition projection cubre cada clave de partición y los valores de Raw coinciden con los enums del envelope; las 9 tablas de Curated y sus llaves foráneas existen; las columnas de BigQuery se derivan del envelope enriquecido.

**Qué no verifican:** que `contracts/` y este documento digan lo mismo. Hoy esa alineación depende de la revisión en el PR.

**Pendiente (recomendado, Fase 5; no bloquea la Fase 0):** una prueba de sincronización que lea los nombres de columna de cada `.sql` y compruebe que aparecen en las tablas de §5.5 y §5.7, para que el CI falle si cambia un lado sin el otro.

### 5.14 Infraestructura base: bootstrap de Terraform (Lote 1.0)

El Lote 1.0 (HU-046) deja lo mínimo para que todo el Terraform posterior use **estado remoto** (ADR-14). Las rutas son relativas a `project/infra/`; la guía de ejecución paso a paso es `BOOTSTRAP.md`.

**Estructura**

```
infra/
├── BOOTSTRAP.md  validate.sh  .gitignore
├── gcp/
│   ├── bootstrap/                 # estado LOCAL; se aplica una vez
│   ├── modules/{project_services,tfstate_bucket,budget}/
│   └── environments/dev/          # estado REMOTO (GCS, prefix env/dev); vacío hasta el Lote 1.3
└── aws/
    ├── bootstrap/                 # estado LOCAL; se aplica una vez
    ├── modules/{tfstate_backend,budget}/
    └── environments/dev/          # estado REMOTO (S3, key env/dev/terraform.tfstate); vacío hasta las Fases 3 y 4
```

**Qué crea cada bootstrap**

| Nube | Recurso | Nombre | Detalle |
|---|---|---|---|
| GCP | APIs del proyecto | (19, abajo) | `disable_on_destroy = false`: destruir Terraform no apaga APIs. |
| GCP | Bucket de estado | `{GCP_PROJECT}-ta-tfstate` | Ubicación = `GCP_REGION`; acceso uniforme; `public_access_prevention = enforced`; versionado; borra versiones archivadas cuando hay 10 más nuevas; `force_destroy = false`; `prevent_destroy`; con labels. |
| GCP | Canal de notificación (uno por correo) | `ta-budget-{correo}` | Tipo `email`, en Cloud Monitoring. |
| GCP | Presupuesto | `ta-budget-{GCP_PROJECT}` | Filtrado por el **número** del proyecto (`63567159332`). |
| AWS | Bucket de estado | `ta-{ENV}-{AWS_ACCOUNT_ID}-tfstate` | Versionado; cifrado SSE `AES256` (no KMS: la llave del lake llega en la Fase 4); bloqueo total de acceso público; política que deniega todo tráfico sin TLS; versiones no vigentes a 90 días; `prevent_destroy`. |
| AWS | Tabla de bloqueo | `ta-{ENV}-tflock` | DynamoDB, `PAY_PER_REQUEST`, clave `LockID` (S), recuperación a un punto en el tiempo, `prevent_destroy` (ADR-15). |
| AWS | Presupuesto | `ta-{ENV}-monthly` | `COST`, mensual, en USD, a nivel de cuenta. |

**APIs de GCP habilitadas** (`gcp/bootstrap`): `serviceusage`, `cloudresourcemanager`, `cloudbilling`, `billingbudgets`, `iam`, `iamcredentials`, `sts`, `compute`, `storage`, `pubsub`, `run`, `cloudscheduler`, `dataflow`, `bigquery`, `secretmanager`, `artifactregistry`, `cloudbuild`, `monitoring` y `logging` (todas `*.googleapis.com`). Quien necesite otra API la agrega a esa lista, no a mano.

**Presupuestos**

| | GCP | AWS |
|---|---|---|
| Moneda | La de la cuenta de facturación (`COP` por defecto; debe coincidir con la de la cuenta) | USD |
| Alcance | Proyecto | Cuenta |
| Umbrales sobre gasto real | 50 %, 90 %, 100 % | 50 %, 80 %, 100 % |
| Pronóstico | 100 % | 100 % |
| Aviso | Correo mediante canales de Cloud Monitoring; además, con `disable_default_iam_recipients = false`, los administradores de la cuenta de facturación | Correo directo a `alert_emails` |
| Monto del ejemplo | `400000` COP | `50` USD |

**Interfaz bootstrap → entornos.** Cada entorno declara un backend parcial y recibe el resto con `terraform init -backend-config=backend.hcl`:

| Nube | Output del bootstrap | Clave de `backend.hcl` | Fijo en el `main.tf` del entorno |
|---|---|---|---|
| GCP | `tfstate_bucket` | `bucket` | backend `gcs`, `prefix = "env/dev"` |
| AWS | `tfstate_bucket` | `bucket` | backend `s3`, `key = "env/dev/terraform.tfstate"`, `encrypt = true` |
| AWS | `tflock_table` | `dynamodb_table` | (además, `region` en `backend.hcl`) |

**Variables de entrada** (los valores reales van en `terraform.tfvars`, que no se versiona; cada raíz trae un `terraform.tfvars.example`):

| Raíz | Variables (valor por defecto entre paréntesis) |
|---|---|
| `gcp/bootstrap` | `project_id`, `project_number`, `region` (`us-central1`), `env` (`dev`), `owner` (`alejandro`), `billing_account_id`, `budget_amount`, `budget_currency` (`COP`), `alert_emails` |
| `gcp/environments/dev` | `project_id`, `region` (`us-central1`), `env`, `owner` |
| `aws/bootstrap` | `aws_account_id`, `aws_region` (`us-east-1`), `env`, `owner`, `budget_usd`, `alert_emails` |
| `aws/environments/dev` | `aws_account_id`, `aws_region` (`us-east-1`), `env`, `owner` |

`billing_account_id`, `budget_amount`, `budget_usd` y `alert_emails` no tienen valor por defecto. `infra/.gitignore` excluye `.terraform/`, el estado, los planes, `terraform.tfvars`, `*.auto.tfvars` y `backend.hcl`, y **sí** versiona los `*.example` y `.terraform.lock.hcl` (hay que hacer *commit* del lock tras el primer `init`).

**Módulos reutilizables**

| Módulo | Variables (por defecto) | Outputs |
|---|---|---|
| `gcp/modules/project_services` | `project_id`, `services` | `enabled_services` |
| `gcp/modules/tfstate_bucket` | `project_id`, `name`, `location`, `noncurrent_versions_to_keep` (10), `labels` | `name`, `url` |
| `gcp/modules/budget` | `project_id`, `project_number`, `billing_account_id`, `amount`, `currency_code`, `alert_emails`, `thresholds` (`[0.5, 0.9, 1.0]`) | `budget_name` |
| `aws/modules/tfstate_backend` | `bucket_name`, `lock_table_name`, `noncurrent_expiration_days` (90) | `bucket_name`, `bucket_arn`, `lock_table_name` |
| `aws/modules/budget` | `name`, `limit_usd`, `alert_emails`, `actual_thresholds` (`[50, 80, 100]`) | `budget_name` |

**Protecciones.** Los buckets de estado y la tabla de bloqueo llevan `prevent_destroy`. El proveedor de AWS usa `allowed_account_ids` (cuenta `648617469176`). El proveedor de GCP usa `user_project_override` y `billing_project` para que Billing Budgets y Monitoring cuenten contra el proyecto. El estado del bootstrap es local y no se versiona: si se pierde, se recupera con `terraform import` de estos recursos.

**Orden de ejecución.** (1) `bash project/infra/validate.sh` (sin credenciales; necesita Terraform ≥ 1.6; corre `terraform fmt -check -recursive` y, en las cuatro raíces, `init -backend=false` + `validate`). (2) `gcp/bootstrap`: `init`, `plan`, `apply`; si falla porque `serviceusage` o `cloudresourcemanager` no están activos, se habilitan con `gcloud services enable` y se repite. (3) `aws/bootstrap`: igual. (4) En cada `environments/dev`: copiar `backend.hcl` y `terraform.tfvars` desde los `*.example`, `init -backend-config=backend.hcl` y `plan` (debe dar **sin cambios**).

**Qué no existe todavía:**
- Service accounts y roles (§5.8): Lote 1.3 en GCP; Fases 3 y 4 en AWS.
- Topics, suscripciones, buckets `…-ta-datasets` y `…-ta-lake`, Cloud Run y BigQuery: Lote 1.3 y Fase 2.
- Secreto `ta-hmac-key`: Lote 2.4.
- Bucket S3 del lake, KMS, Glue, Athena y EventBridge: Fase 4. Federación OIDC GCP → AWS: Lote 3.2.
- Acceso de Miguel y Camila al proyecto GCP: diferido (§9, Fase 0).

**Observaciones para lotes futuros:**
- `alert_emails` ya se usa en los bootstraps para los presupuestos. Cuando el Lote 6.2 cree el canal de alertas de tendencias (§5.12), debe declarar su propia variable en `environments/dev` y reutilizar o referenciar los canales para no duplicar correos.
- El bloqueo con DynamoDB se mantiene para respetar Terraform ≥ 1.6. Con ≥ 1.10 se podría usar `use_lockfile = true` y eliminar la tabla (ADR-15).

---

## 6. Estructura del repositorio (v2)

```
.                                          <- raíz del repositorio
├── README.md
├── .gitignore                             <- incluye terraform.tfvars
├── .pre-commit-config.yaml                <- gitleaks, formato (HU-041)
├── .github/workflows/                     <- CI/CD; debe estar en la raíz y usar working-directory: project
│
├── docs/
│   ├── ARCHITECTURE.md                    <- este archivo
│   ├── HU.md                              <- historias de usuario (HU-001 a HU-048)
│   ├── GLOSSARY.md                        <- glosario del proyecto
│   ├── NOTAS.md                           <- notas de trabajo
│   ├── trend_analyzer_multicloud.drawio   <- diagrama editable (v2.5)
│   └── trend_analyzer_multicloud.jpg      <- exportación del diagrama
│
└── project/                               <- código, contratos e infraestructura (las rutas de este documento son relativas a aquí)
    ├── docker-compose.yml                 <- entorno local: emulador Pub/Sub + simulador (HU-047)
    ├── contracts/                         <- copia legible por máquina de §5 (Fase 0, entregado en v2.7)
    │   ├── README.md  requirements.txt
    │   ├── envelope.v1.schema.json
    │   ├── envelope.enriched.v1.schema.json
    │   ├── pubsub.attributes.schema.json
    │   ├── payload.{reddit,facebook,instagram}.schema.json
    │   ├── bigquery/{posts_enriched,trend_windows,dlq_events,dq_results}.sql
    │   ├── athena/{posts_raw,posts,curated}.sql
    │   ├── examples/{valid,invalid}/
    │   └── tests/test_contracts.py
    ├── infra/                             <- Lote 1.0 entregado (§5.14)
    │   ├── BOOTSTRAP.md                   <- guía de ejecución del bootstrap
    │   ├── validate.sh                    <- fmt + validate sin credenciales
    │   ├── .gitignore                     <- excluye estado, tfvars y backend.hcl
    │   ├── gcp/
    │   │   ├── bootstrap/                 <- estado local: APIs, bucket de estado, presupuesto
    │   │   ├── modules/{project_services,tfstate_bucket,budget}/
    │   │   └── environments/dev/          <- estado remoto (GCS)
    │   └── aws/
    │       ├── bootstrap/                 <- estado local: bucket de estado, tabla de bloqueo, presupuesto
    │       ├── modules/{tfstate_backend,budget}/
    │       └── environments/dev/          <- estado remoto (S3)
    ├── scripts/
    │   ├── generate_comments.py           <- CLI del generador: snapshots CSV / histórico (§3.2)
    │   └── smoke_e2e.py                   <- prueba de humo extremo a extremo (HU-048)
    └── src/
        ├── common/                        <- librería compartida: envelope, anonimización, NLP, label_map
        ├── simulator/                     <- NUEVO (reemplaza a collectors)
        │   ├── app.py  publisher.py  state.py
        │   ├── generator/comment_generator.py   <- script de comentarios (marca, comentario) adaptado
        │   ├── enrich/{users,engagement,brands,chaos}.py
        │   ├── config/{sim,brands}.yaml
        │   └── tests/  Dockerfile  requirements.txt
        ├── streaming/                     <- Dataflow (Beam)
        ├── historical/                    <- full load (usa el mismo generador)
        ├── exporter/                      <- gcs-to-s3
        ├── lakehouse/{glue_jobs,dbt}/
        ├── quality/                       <- Great Expectations
        └── collectors/                    <- LEGADO: conectores reales Mastodon/Reddit/RSS (no se despliega)
```

**Nota de rutas:** en `docs/` se documenta; en `project/` se construye. Un lote entrega archivos bajo `project/` (p. ej. `project/src/simulator/app.py`); los workflows de CI viven en `.github/workflows/` de la raíz.

`src/common/` evita duplicar lógica: el **mismo** código de anonimización y NLP lo usan Dataflow y el loader histórico (así stream e histórico son comparables).

---

## 7. Plan por fases (lotes)

> El plan se organiza en fases y lotes, sin fechas: lo que importa es el orden y las dependencias. Hay dos entregas: **Sprint 2** y **Sprint Final** (ver los marcadores ★ y §7.1). Responsables sugeridos según el README: **M** = Miguel (infra/IAM/CI-CD), **C** = Camila (pipelines), **A** = Alejandro (simulador/calidad). Las fases 1, 2 y 3 se pueden trabajar en paralelo una vez cerrada la Fase 0.

**Trazabilidad:** cada fase y cada lote indica las **HU** que cubre (definidas en `HU.md`, con criterios de aceptación). Una fase se considera cerrada cuando todas sus HU cumplen sus criterios de aceptación y su estado se marca en §10. La matriz completa está en §7.1.

Cada **lote** es una petición mía de tamaño razonable: yo entrego un conjunto cerrado de archivos con pruebas, ustedes los integran y validan, y pasamos al siguiente.

### Fase 0 · Alineación (todos)
**HU cubiertas:** HU-001, HU-002, HU-003

**Objetivo:** congelar decisiones y contratos.
- **Lote 0.1** → `contracts/*`, README actualizado (hecho en v2.6), `docs/trend_analyzer_multicloud.drawio` actualizado a la v2.5 (hecho en v2.6). · **HU:** HU-001, HU-002
- **Ustedes me dan:** respuestas a §9 (preguntas abiertas), estado real de `infra/` (qué ya está aplicado), proyecto GCP y región.
- **Listo cuando:** ADRs aprobados y contratos en el repo.

### Fase 1 · Simulador (A, con M para infra)
**HU cubiertas:** HU-004, HU-008, HU-009, HU-010, HU-011, HU-012, HU-013, HU-014, HU-015, HU-016, HU-017, HU-046, HU-047

- **Lote 1.0** → *bootstrap* de Terraform: configuración del proyecto GCP existente (región `us-central1`), APIs habilitadas, estado remoto en GCS y S3, `terraform.tfvars.example`, presupuesto mínimo. · **HU:** HU-046
  - **Estado: entregado en v2.11** (código y guía en `project/infra/`; contrato en §5.14). Se cierra cuando el equipo corra `validate.sh`, confirme `billing_account_id` y montos (§9 #10) y aplique ambos bootstraps según `BOOTSTRAP.md`.
  - **Contratos tocados:** ninguno de §5.2–§5.8; se añadió §5.14 y se ampliaron §5.1, §5.8, §5.9, §5.11 y §5.12.
- **Lote 1.1** → `src/common/` (envelope, hashing, label_map, `normalize_hashtag()`) + adaptación del generador de comentarios (§3.2: función pura con `rng`, parámetros `marca`/`sentimiento`, marca canónica) y `scripts/generate_comments.py`. · **HU:** HU-008, HU-009
- **Lote 1.2** → núcleo del simulador (cursor, selección de marca con pesos, usuarios, engagement por red, hashtags, caos, publisher asyncio) + tests unitarios (incluye test de determinismo: misma semilla + `seq` ⇒ mismo `event_id` y mismo texto; y test de reanudación del cursor). · **HU:** HU-010, HU-011, HU-012, HU-013, HU-014, HU-015
- **Lote 1.3** → `Dockerfile` + Terraform GCP: Cloud Run `min=max=1` sin throttling, topics, suscripciones, DLQ, buckets, service accounts. · **HU:** HU-004, HU-016
- **Lote 1.4** → script de verificación: se suscribe y reporta tasa de mensajes, ids únicos y `seq`. · **HU:** HU-017
- **Lote 1.5** → entorno local: `docker-compose` con emulador de Pub/Sub, simulador y consumidor de validación contra los schemas. · **HU:** HU-047
- **Listo cuando:** los 3 topics reciben ~0.5 msg/s cada uno, el cursor sobrevive a un reinicio, y el test de determinismo pasa.

### Fase 2 · Streaming en GCP (C)
**HU cubiertas:** HU-005, HU-018, HU-019, HU-020, HU-021, HU-022, HU-023

- **Lote 2.1** → esqueleto Dataflow: lectura, validación, dedup por `event_id`, DLQ, escritura a `raw/`. · **HU:** HU-018, HU-019
- **Lote 2.2** → anonimización HMAC + normalización de engagement + sink a BigQuery `posts_enriched` + `enriched/` Parquet. · **HU:** HU-020, HU-021
- **Lote 2.3** → NLP con `RunInference` (contenedor personalizado) + `nlp_model_version`. · **HU:** HU-022
- **Lote 2.4** → ventanas `trend_windows` + Terraform (BigQuery, plantilla de Dataflow, secret HMAC). · **HU:** HU-005, HU-023
- **Listo cuando:** un evento emitido aparece en BigQuery con `emotion_label` en < 1 min, y p95 de latencia < 5 min.

### Fase 3 · Histórico y puente a AWS (A + M)
**HU cubiertas:** HU-006, HU-024, HU-025

- **Lote 3.1** → `historical/`: usa el generador en batch (el mismo código del simulador, con otra semilla) para producir N registros (por defecto ~50k, configurable) repartidos entre `reddit`, `facebook` e `instagram`, con fechas en los últimos 180 días (estacionalidad semanal/horaria y tendencias de marca, para que haya historia que comparar), mismo envelope, anonimiza, corre el NLP en lote y escribe Parquet a `raw/origin=historical/`. · **HU:** HU-024
- **Lote 3.2** → exportador `gcs-to-s3` + federación OIDC GCP→AWS + Terraform en ambas nubes. · **HU:** HU-006, HU-025
- **Listo cuando:** datos de ambos orígenes aparecen en `s3://…/raw/`.

### Fase 4 · Lakehouse AWS (C + M)
**HU cubiertas:** HU-007, HU-026, HU-027, HU-028, HU-029, HU-030

- **Lote 4.1** → Terraform: Glue DBs, tablas con partition projection, workgroup Athena, EventBridge, KMS/IAM. · **HU:** HU-007, HU-029
- **Lote 4.2** → Glue job `raw→trusted` (unión, dedup, tipos, limpieza). · **HU:** HU-026
- **Lote 4.3** → `trusted→curated` (dbt-athena o Glue): hechos, dimensiones, agregados. · **HU:** HU-027, HU-030
- **Lote 4.4** → tablas de aristas para análisis de relaciones. · **HU:** HU-028
- **Listo cuando:** `SELECT` en Athena sobre `ta_curated.fact_posts` devuelve datos de stream e histórico.

### Fase 5 · Calidad y gobernanza (A, en paralelo)
**HU cubiertas:** HU-031, HU-032, HU-033, HU-034, HU-035, HU-036, HU-048

- **Lote 5.1** → *profiling* (US-14) sobre Raw/Trusted. · **HU:** HU-031
- **Lote 5.2** → suites de Great Expectations + tests dbt según §5.10; escritura en `dq_results`. · **HU:** HU-032, HU-033
- **Lote 5.3** → catálogo (descripciones de tablas/columnas en Glue), documento de linaje (dbt docs + diagrama), mapeo DAMA-DMBOK y nota Ley 1581 (los datos son sintéticos; igualmente se documenta el tratamiento). · **HU:** HU-034, HU-035, HU-036
- **Lote 5.4** → `scripts/smoke_e2e.py`: prueba de humo del flujo completo con evento centinela, latencia por etapa y evidencia para el informe. · **HU:** HU-048

> ### ★ Entrega Sprint 2
> Debe poder demostrarse: simulador → Pub/Sub → Dataflow → BigQuery; exportación a S3; Glue Raw→Trusted→Curated; consulta en Athena; reportes de calidad; catálogo y linaje documentados.

### Fase 6 · Serving, alertas y escenarios (C + A)
**HU cubiertas:** HU-037, HU-038, HU-039, HU-040

- **Lote 6.1** → datasets y dashboard de QuickSight (guía + definiciones). · **HU:** HU-037, HU-038
- **Lote 6.2** → alertas: `trend_windows.is_alert` → `ta-alerts` → notificación. · **HU:** HU-039
- **Lote 6.3** → endpoint `/control/scenario` y guion de demo de picos. · **HU:** HU-040

### Fase 7 · CI/CD, observabilidad y control de costos (M)
**HU cubiertas:** HU-041, HU-042, HU-043

- **Lote 7.1** → GitHub Actions: tests, `terraform fmt/validate/plan`, build y push de imágenes. · **HU:** HU-041
- **Lote 7.2** → dashboards/alertas de Cloud Monitoring y CloudWatch; métricas de latencia y DLQ. · **HU:** HU-042
- **Lote 7.3** → scripts de encendido/apagado (Dataflow y simulador) y alertas de presupuesto. · **HU:** HU-043

### Fase 8 · Informe y sustentación (todos)
**HU cubiertas:** HU-044, HU-045

- **Lote 8.1** → estructura del informe técnico, tabla de KPIs medidos, guion de sustentación, runbook. · **HU:** HU-044, HU-045
> ### ★ Sprint Final

### 7.1 Matriz de trazabilidad Fase · Lote → HU

> Fuente de verdad de las HU: `HU.md`. Si se agrega o reasigna una HU, se actualiza `HU.md` primero y esta matriz en el mismo commit.

| Fase | Lote | HU | Resp. |
|---|---|---|---|
| 0 | 0.1 | HU-001, HU-002 | Todos |
| 0 | — | HU-003 | Todos |
| 1 | 1.0 | HU-046 | M |
| 1 | 1.1 | HU-008, HU-009 | A |
| 1 | 1.2 | HU-010, HU-011, HU-012, HU-013, HU-014, HU-015 | A |
| 1 | 1.3 | HU-004, HU-016 | M + A |
| 1 | 1.4 | HU-017 | A |
| 1 | 1.5 | HU-047 | A |
| 2 | 2.1 | HU-018, HU-019 | C |
| 2 | 2.2 | HU-020, HU-021 | C |
| 2 | 2.3 | HU-022 | C |
| 2 | 2.4 | HU-005, HU-023 | C + M |
| 3 | 3.1 | HU-024 | A |
| 3 | 3.2 | HU-006, HU-025 | M |
| 4 | 4.1 | HU-007, HU-029 | M |
| 4 | 4.2 | HU-026 | C |
| 4 | 4.3 | HU-027, HU-030 | C |
| 4 | 4.4 | HU-028 | C |
| 5 | 5.1 | HU-031 | A |
| 5 | 5.2 | HU-032, HU-033 | A |
| 5 | 5.3 | HU-034, HU-035, HU-036 | A |
| 5 | 5.4 | HU-048 | A |
| 6 | 6.1 | HU-037, HU-038 | C |
| 6 | 6.2 | HU-039 | C |
| 6 | 6.3 | HU-040 | A |
| 7 | 7.1 | HU-041 | M |
| 7 | 7.2 | HU-042 | M |
| 7 | 7.3 | HU-043 | M |
| 8 | 8.1 | HU-044, HU-045 | Todos |

**Cobertura:** 48 HU (HU-001 a HU-048), cada una asignada a exactamente un lote o a la Fase 0. HU de Sprint 2: HU-001 a HU-036 y HU-046 a HU-048. HU de Sprint Final: HU-037 a HU-045.

**Equivalencia con las US del README:** US-10 → HU-010 · US-11 → HU-011 · US-12 → HU-012 · US-13 → HU-032 · US-14 → HU-031. *(El README v2.2 ya no presenta US-10/11/12 como conectores externos.)*

> ✅ **`HU.md` alineado con la v2.2** (HU-008 a HU-014, HU-022, HU-024, HU-033 y HU-040 reescritas; HU-010 = Facebook, HU-011 = Reddit, HU-012 = Instagram).

---

## 8. Protocolo de trabajo conmigo

**Para pedirme un lote, envíame:**

```text
Lote: <fase.lote>  (ej. 2.2)
HU: <HU-xxx cubiertas por el lote, según §7.1>  (ej. HU-020, HU-021)
Contexto: ARCHITECTURE.md adjunto (usa §<secciones de la matriz 5.11>).
Entorno real: <outputs de Terraform / proyecto / región / cualquier valor que cambie>
Restricciones: <opcional: lo que ya existe y no hay que tocar>
Entrégame: archivos completos en las rutas de §6, con tests y comandos para validarlos.
```

**Lo que yo entrego:**
1. Archivos completos en la ruta exacta de §6 (no fragmentos).
2. Pruebas y los comandos para correrlas.
3. La confirmación de **qué criterios de aceptación de cada HU** quedan cubiertos (y cuáles no).
4. Una lista de **"contratos tocados"**: si un lote requiere cambiar §5, lo digo explícitamente y propongo el cambio al documento antes de que el código lo asuma.

**Reglas que me impongo:**
- Si un archivo depende de otro ya existente, uso §5 y **no necesito verlo**.
- Nunca invento nombres de recursos: uso los de §5 o los `outputs` que me pasen.
- Nunca hardcodeo secretos ni IDs; todo va por variable de entorno.

**Reglas para ustedes:**
- Si cambian una interfaz en el código, actualicen §5 en el mismo commit.
- Marquen el estado en §10 al cerrar cada lote y en la tabla resumen de `HU.md` al cumplir cada HU.

---

## 9. Decisiones (ADR), riesgos y preguntas abiertas

### Decisiones

| ID | Decisión | Alternativa descartada |
|---|---|---|
| ADR-01 | Fuentes **Reddit, Facebook e Instagram simuladas** con un generador de comentarios en Python | APIs reales (OAuth de Reddit, revisión de app de Meta); datasets de emociones en inglés (v2.1); polling REST |
| ADR-02 | Simulador en Cloud Run (`min=max=1`, CPU always-on). ✅ **Aprobado** | EC2 (cross-cloud); VM en GCE (plan B por costo) |
| ADR-03 | Un servicio con 3 workers, no 3 servicios | 3 servicios always-on (3× costo fijo) |
| ADR-04 | Envelope único para stream e histórico; `origin` distingue | Esquemas separados |
| ADR-05 | NLP en **español** con modelos preentrenados RoBERTuito de pysentimiento: `robertuito-sentiment-analysis` (POS/NEG/NEU) y `robertuito-emotion-analysis` (emoción) | Modelo en inglés (v2.1); traducir comentarios; modelo multilingüe (plan B por evaluar: `cardiffnlp/twitter-xlm-roberta-base-sentiment`) |
| ADR-06 | Anonimización HMAC-SHA256 en el **primer** paso de cada plano (Dataflow / loader) | Anonimizar en AWS (el dato en claro cruzaría de nube) |
| ADR-07 | GCS→S3 con Cloud Run Job + OIDC | Storage Transfer Service / llaves estáticas |
| ADR-08 | Partition projection en Athena | Glue Crawlers |
| ADR-09 | Aristas en Curated en lugar de Neptune | Neptune |
| ADR-10 | Catálogo = Glue Data Catalog; linaje = dbt docs | Dataplex + OpenLineage |
| ADR-11 | Generador de comentarios **integrado en el proceso del simulador** y reutilizado en batch para el histórico; la selección de marca/sentimiento la hace el simulador | Generar un CSV fijo y reproducirlo por vueltas; servicio de generación separado |
| ADR-12 | Región de GCP **`us-central1`** (cuotas de vCPU y Dataflow verificadas en esa región; decidido en v2.10) | `us-east1` (primera elección en v2.4: más cerca de AWS `us-east-1` y de Colombia) |
| ADR-13 | Mapeo `categoria → sentimiento`: `gusto → positive`, `asco → negative`, `informativo → neutral`, `nostalgia` y `adiccion → null` ✅ **Aprobado** | Forzar una etiqueta a las categorías ambiguas (inflaría la precisión del NLP) |
| ADR-14 | Estado de Terraform **remoto** (GCS y S3) desde el primer día | Estado local (se pierde y no permite CI) |
| ADR-15 | Bloqueo del estado en AWS con tabla DynamoDB `ta-{env}-tflock`, compatible con Terraform ≥ 1.6 (adoptado en el Lote 1.0) | `use_lockfile = true` de S3 (exige Terraform ≥ 1.10; se puede migrar y eliminar la tabla) |
| ADR-16 | *Bootstrap* en un directorio aparte por nube, con estado local, `prevent_destroy` en buckets y tabla, y `allowed_account_ids` en AWS (adoptado en el Lote 1.0) | Bootstrap con estado remoto (dependencia circular); recursos de estado sin protección contra borrado |

### Riesgos

| Riesgo | Mitigación |
|---|---|
| **Tiempo** (dos entregas: Sprint 2 y Sprint Final) | Paralelizar fases 1–3; NLP en Dataflow es lo más arriesgado → fallback: modelo ligero o NLP en Cloud Run con Pub/Sub intermedio |
| **Costo** (Dataflow 24/7 + Cloud Run always-on) | `maxNumWorkers=1–2`, apagar fuera de demos, alertas de presupuesto (Lote 7.3) |
| Dataflow + modelos HF (imagen pesada, memoria; RoBERTuito requiere `pysentimiento` y su `preprocess_tweet`) | Contenedor personalizado, probar local con `DirectRunner` primero; si la memoria no alcanza, correr solo el modelo de sentimiento y dejar la emoción como mejora |
| Federación OIDC GCP→AWS no funciona a tiempo | Plan B con llaves en Secret Manager (documentar como deuda) |
| Licencias de modelos NLP | Uso académico; verificar la licencia de cada modelo en su *model card* (los de pysentimiento se entrenaron con corpus TASS) y citarlos en el informe |
| Modelos entrenados en tweets vs. comentarios con jerga y emojis de Reddit/Facebook/Instagram | Medir contra `label_sentiment`; declarar la limitación en el informe |
| Generador por plantillas: baja variedad léxica y texto repetido | Ampliar plantillas (§3.2 cambio #5); medir duplicados por `event_id` y **no** por texto; reportar la diversidad léxica en el profiling |
| Infraestructura desde cero (nada aplicado): retrasa la Fase 1 | Lote 1.0 (HU-046) primero; el simulador se desarrolla en local con el emulador (HU-047) mientras se crea el proyecto. **v2.11:** Lote 1.0 entregado; falta aplicarlo (§5.14) |
| Falsas alertas al arrancar (z-score sin baseline) | `is_alert = false` hasta 60 ventanas y *cooldown* de 15 min (§5.12) |
| Cuotas de la cuenta de crédito de GCP (p. ej. límite de vCPU) pueden limitar Dataflow con modelos pesados | Verificar cuotas en el Lote 1.0; `maxNumWorkers=1–2`; plan B: NLP en Cloud Run |

### Preguntas abiertas y respuestas (Fase 0)

| # | Pregunta | Respuesta | Estado |
|---|---|---|---|
| 1 | Proyecto/región de GCP y cuenta/región de AWS; estado de `infra/` | AWS `us-east-1`, cuenta `648617469176`. GCP `project-0568d9ba-848f-413c-88b` (nombre `proyectoDatos`, número `63567159332`), región `us-central1` (ADR-12), facturación con crédito a cargo de Alejandro Sepulveda. `infra/` sin nada aplicado (partimos de cero). | ✅ Proyecto, región y cuotas de vCPU y Dataflow verificados en `us-central1`. La facturación del proyecto nuevo se confirma en HU-046 con `gcloud billing projects describe` (`BOOTSTRAP.md` §0); el acceso de Miguel y Camila se otorga cuando lo necesiten sus lotes |
| 2 | ADR-02: ¿Cloud Run o VM en GCE? | Sin respuesta explícita: se adopta Cloud Run por defecto. | ✅ Aprobado (plan B en GCE si el costo molesta) |
| 3 | Empresa y marcas simuladas | Postobón (propia), Pepsi y Sprite (competidores). | ✅ El script trae además CocaCola y Colombiana: fuera de v1 (§3.2.1) |
| 4 | Fuentes y cambios al generador; script | Tres fuentes confirmadas. Script compartido. | ✅ Fuentes · ✅ Cambios al generador adoptados (§3.2.1, ADR-13) |
| 5 | Presupuesto para Dataflow en demos | No es preocupación por ahora; se asume disponible. | ✅ (supuesto). Se mantiene el Lote 7.3 como buena práctica |
| 6 | QuickSight y conector a BigQuery | Se asume acceso a QuickSight; conector sin verificar. | 🟨 El camino principal (Athena → QuickSight) no depende del conector |
| 7 | NLP en español (ADR-05) y licencia académica | Aceptados. | ✅ |
| 8 | Valores permitidos de `dq_results.layer` (el DDL no los fija) | Propuesta: `stream`, `raw`, `trusted`, `curated`. | ✅ Aprobado |
| 9 | `fact_posts.dt` (y `date_key`): ¿`DATE(event_time)` UTC o fecha de proceso? | Propuesta: `DATE(event_time)` UTC, igual que Trusted (§5.7). | ✅ Aprobado |
| 10 | Montos de los presupuestos y `billing_account_id` (Lote 1.0) | Valores de ejemplo en `terraform.tfvars.example`: GCP 400000 COP, AWS 50 USD; `billing_account_id` se obtiene con `gcloud billing accounts list`. Destinatario: solo el correo de Alejandro. | 🟨 Confirmar antes del `apply`; no bloquea el código (§5.14) |

**Fase 0 cerrada.** Resuelto: proyecto GCP (`project-0568d9ba-848f-413c-88b`), región `us-central1` y cuotas; autenticación de Terraform en GCP (§5.1); revisión de `contracts/` (HU-001); ADR-02, ADR-12 y ADR-13; §9 #8 y #9; solo el correo de Alejandro por ahora. **Diferido (no bloquea):** acceso de Miguel y Camila (HU-046), verificación del conector QuickSight → BigQuery (§9 #6, Lote 6.1) y prueba de sincronización §5 ↔ `contracts/` (Fase 5, §5.13).
---

## 10. Estado del proyecto

| Fase | Estado | Responsable | HU | Notas |
|---|---|---|---|---|
| 0 · Alineación | ✅ | Todos | HU-001 a HU-003 | Proyecto GCP `project-0568d9ba-848f-413c-88b` y región `us-central1` confirmados con cuotas verificadas; Terraform se autentica en GCP con Application Default Credentials; `contracts/` revisado (HU-001); HU-002 hecha; ADR-02, ADR-12 y ADR-13 aprobados; §9 #8 y #9 aprobados. Diferido: accesos de Miguel y Camila, conector QuickSight → BigQuery y prueba de sincronización (Fase 5) |
| 1 · Simulador | 🟨 | A (+M) | HU-004, HU-008 a HU-017, HU-046, HU-047 | **Lote 1.0 (HU-046) entregado** (§5.14): falta validar con `validate.sh` y aplicar los bootstraps. Lotes 1.1 a 1.5 ⬜. `HU.md` ya alineado con v2.2; el Lote 1.1 parte de los hallazgos de §3.2.1 |
| 2 · Streaming GCP | ⬜ | C | HU-005, HU-018 a HU-023 | |
| 3 · Histórico + puente | ⬜ | A + M | HU-006, HU-024, HU-025 | |
| 4 · Lakehouse AWS | ⬜ | C + M | HU-007, HU-026 a HU-030 | |
| 5 · Calidad y gobernanza | ⬜ | A | HU-031 a HU-036, HU-048 | |
| ★ Sprint 2 | ⬜ | | HU-001 a HU-036, HU-046 a HU-048 | |
| 6 · Serving y alertas | ⬜ | C + A | HU-037 a HU-040 | |
| 7 · CI/CD y costos | ⬜ | M | HU-041 a HU-043 | |
| 8 · Informe | ⬜ | Todos | HU-044, HU-045 | |
| ★ Sprint Final | ⬜ | | HU-037 a HU-045 | |

Leyenda: ⬜ pendiente · 🟨 en curso · ✅ completada