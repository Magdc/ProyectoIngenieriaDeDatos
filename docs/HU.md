# TrendAnalyzer · Historias de Usuario (HU)

> Backlog completo del proyecto, alineado con `ARCHITECTURE.md` **v2.17** (en `docs/`) (fuentes simuladas **Reddit, Facebook e Instagram** alimentadas por un **generador de comentarios** en Python; GCP + AWS).
> Cada HU tiene un ID **HU-000**, su fase/lote de `ARCHITECTURE.md` §7, responsable, sprint y criterios de aceptación verificables.

---

## Cómo leer este documento

**Formato de cada HU**

```
### HU-000 · Título
| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
Como <rol> quiero <capacidad> para <beneficio>.
Criterios de aceptación: lista verificable.
```

**Roles de usuario usados**

| Rol | Quién es |
|---|---|
| **Marketing** | Equipo de investigación de mercado de la empresa simulada |
| **Gerencia** | Quien toma decisiones y consume indicadores |
| **Analista** | Consulta datos y explora relaciones |
| **Ingeniero de datos** | Construye y opera los pipelines (Camila, Alejandro) |
| **Líder técnico / DataOps** | Infraestructura, seguridad, CI/CD (Miguel) |
| **Responsable de gobernanza** | Calidad, catálogo, linaje, cumplimiento (Alejandro) |

**Responsables:** **M** = Miguel · **C** = Camila · **A** = Alejandro. **Prioridad (MoSCoW):** Must / Should / Could.
**Sprints:** Sprint 2 y Sprint Final son entregas, sin fechas. El contenido de cada una está en `ARCHITECTURE.md` §7 y §7.1.

**Estado:** ⬜ pendiente · 🟨 en curso · ✅ completada (se marca en la tabla resumen).

### Equivalencia con las US anteriores del README

| US anterior | HU actual |
|---|---|
| US-10 Conector Mastodon | **HU-010** (ahora **Facebook** simulado con el generador de comentarios) |
| US-11 Conector Reddit | **HU-011** (**Reddit** se conserva, ahora simulado con el generador) |
| US-12 Conector RSS / News | **HU-012** (ahora **Instagram** simulado con el generador) |
| US-13 Calidad | **HU-032** |
| US-14 Profiling | **HU-031** |

---

## Tabla resumen

| HU | Título | Fase · Lote | Resp. | Prio. | Sprint | Estado |
|---|---|---|---|---|---|---|
| **E0 · Alineación** | | | | | | |
| HU-001 | Contratos de datos versionados | 0 · 0.1 | Todos | Must | 2 | ✅ |
| HU-002 | README y diagrama alineados con la v2 | 0 · 0.1 | Todos | Must | 2 | ✅ |
| HU-003 | Decisiones de arquitectura cerradas | 0 | Todos | Must | 2 | ✅ |
| **E1 · Infraestructura como código** | | | | | | |
| HU-004 | Infraestructura GCP base | 1 · 1.3 | M | Must | 2 | ✅ |
| HU-005 | Recursos de streaming (BigQuery, secreto HMAC) | 2 · 2.4 | M + C | Must | 2 | ⬜ |
| HU-006 | Federación OIDC GCP → AWS | 3 · 3.2 | M | Must | 2 | ⬜ |
| HU-007 | Infraestructura AWS del lakehouse | 4 · 4.1 | M | Must | 2 | ⬜ |
| HU-046 | Bootstrap de Terraform y entornos | 1 · 1.0 | M | Must | 2 | ✅ |
| **E2 · Simulación de fuentes** | | | | | | |
| HU-008 | Adaptación del generador de comentarios | 1 · 1.1 | A | Must | 2 | ✅ |
| HU-009 | Librería común | 1 · 1.1 | A | Must | 2 | ✅ |
| HU-010 | Fuente Facebook simulada | 1 · 1.2 | A | Must | 2 | ✅ |
| HU-011 | Fuente Reddit simulada | 1 · 1.2 | A | Must | 2 | ✅ |
| HU-012 | Fuente Instagram simulada | 1 · 1.2 | A | Must | 2 | ✅ |
| HU-013 | Cursor persistente y emisión determinista | 1 · 1.2 | A | Must | 2 | ✅ |
| HU-014 | Enriquecimiento sintético | 1 · 1.2 | A | Must | 2 | ✅ |
| HU-015 | Caos controlado | 1 · 1.2 | A | Should | 2 | ✅ |
| HU-016 | Despliegue del simulador | 1 · 1.3 | A + M | Must | 2 | ✅ |
| HU-017 | Verificación de la ingesta | 1 · 1.4 | A | Should | 2 | ⬜ |
| HU-047 | Entorno local de desarrollo | 1 · 1.5 | A | Should | 2 | ⬜ |
| **E3 · Streaming en GCP** | | | | | | |
| HU-018 | Validación, deduplicación y DLQ | 2 · 2.1 | C | Must | 2 | ⬜ |
| HU-019 | Persistencia Raw en GCS | 2 · 2.1 | C | Must | 2 | ⬜ |
| HU-020 | Anonimización de usuarios | 2 · 2.2 | C | Must | 2 | ⬜ |
| HU-021 | Normalización y carga a BigQuery | 2 · 2.2 | C | Must | 2 | ⬜ |
| HU-022 | Análisis de emoción y sentimiento | 2 · 2.3 | C | Must | 2 | ⬜ |
| HU-023 | Detección de tendencias en ventanas | 2 · 2.4 | C | Must | 2 | ⬜ |
| **E4 · Histórico y puente a AWS** | | | | | | |
| HU-024 | Carga histórica (full load) | 3 · 3.1 | A | Must | 2 | ⬜ |
| HU-025 | Exportación GCS → S3 | 3 · 3.2 | M | Must | 2 | ⬜ |
| **E5 · Lakehouse en AWS** | | | | | | |
| HU-026 | Capa Trusted | 4 · 4.2 | C | Must | 2 | ⬜ |
| HU-027 | Capa Curated (modelo estrella) | 4 · 4.3 | C | Must | 2 | ⬜ |
| HU-028 | Tablas de aristas | 4 · 4.4 | C | Should | 2 | ⬜ |
| HU-029 | Orquestación horaria | 4 · 4.1 | M | Must | 2 | ⬜ |
| HU-030 | Consulta SQL para analistas | 4 · 4.3 | C | Must | 2 | ⬜ |
| **E6 · Calidad y gobernanza** | | | | | | |
| HU-031 | Profiling de datos | 5 · 5.1 | A | Must | 2 | ⬜ |
| HU-032 | Reglas de calidad y KPIs | 5 · 5.2 | A | Must | 2 | ⬜ |
| HU-033 | Precisión del NLP | 5 · 5.2 | A | Could | 2 | ⬜ |
| HU-034 | Catálogo de datos | 5 · 5.3 | A | Must | 2 | ⬜ |
| HU-035 | Linaje de datos | 5 · 5.3 | A | Must | 2 | ⬜ |
| HU-036 | Cumplimiento normativo y DAMA-DMBOK | 5 · 5.3 | A | Must | 2 | ⬜ |
| HU-048 | Prueba de humo end-to-end | 5 · 5.4 | A | Must | 2 | ⬜ |
| **E7 · Serving y alertas** | | | | | | |
| HU-037 | Dashboard de tendencias y sentimiento | 6 · 6.1 | C | Must | Final | ⬜ |
| HU-038 | Vista de relaciones | 6 · 6.1 | C | Should | Final | ⬜ |
| HU-039 | Alertas de cambios en la conversación | 6 · 6.2 | C | Should | Final | ⬜ |
| HU-040 | Escenarios de demo | 6 · 6.3 | A | Could | Final | ⬜ |
| **E8 · DataOps** | | | | | | |
| HU-041 | CI/CD | 7 · 7.1 | M | Must | Final | ⬜ |
| HU-042 | Observabilidad | 7 · 7.2 | M | Must | Final | ⬜ |
| HU-043 | Control de costos | 7 · 7.3 | M | Should | Final | ⬜ |
| **E9 · Entrega final** | | | | | | |
| HU-044 | Informe técnico y KPIs medidos | 8 · 8.1 | Todos | Must | Final | ⬜ |
| HU-045 | Sustentación y runbook | 8 · 8.1 | Todos | Must | Final | ⬜ |

---

# E0 · Alineación

### HU-001 · Contratos de datos versionados

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E0 | 0 · 0.1 | Todos | Must | 2 | — |

**Como** ingeniero de datos **quiero** contratos legibles por máquina (envelope, payloads, DDL de BigQuery y Athena) **para** que dos componentes puedan integrarse sin leer el código del otro.

**Criterios de aceptación**
- Existe `contracts/envelope.v1.schema.json` (con `generator`, `seq` y `sim.label_sentiment`) y un schema por cada payload (`reddit`, `facebook`, `instagram`).
- Existen los DDL de `posts_enriched`, `trend_windows`, `dlq_events` (BigQuery) y `posts_raw`, `posts`, tablas Curated (Athena).
- Los contratos coinciden exactamente con `ARCHITECTURE.md` §5.
- Todo cambio de interfaz modifica primero §5 y luego el código.
- Los cambios compatibles (campos opcionales nuevos) mantienen `schema_version = "1.0"`; los incompatibles la suben y se migran.
- Un test valida la salida del generador y del simulador (≥ 1.000 eventos por fuente) contra los schemas (§5.12).

### HU-002 · README y diagrama alineados con la v2

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E0 | 0 · 0.1 | Todos | Must | 2 | — |

**Como** integrante del equipo **quiero** que `README.md` y el `.drawio` reflejen las fuentes simuladas y el generador de comentarios **para** no trabajar con documentación contradictoria.

**Criterios de aceptación**
- README actualizado (v2.2): tabla de fuentes (Reddit, Facebook, Instagram), generador de comentarios, bloque de arquitectura, estructura del repo y notas de seguridad.
- Se eliminan del README la nota de Reddit/OAuth, el aviso del token de Mastodon y las referencias a los datasets de emociones (TEC, GoEmotions, SemEval, DailyDialog).
- Se retiran del README las referencias a Firestore, Cloud SQL, Neptune y Dataplex (ADR-09, ADR-10).
- El diagrama (`docs/trend_analyzer_multicloud.drawio`) incluye `stream-simulator` con el generador de comentarios, las fuentes Reddit/Facebook/Instagram simuladas, el exportador `gcs-to-s3` y EventBridge como schedule del workflow de Glue.
### HU-003 · Decisiones de arquitectura cerradas

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E0 | 0 | Todos | Must | 2 | — |

**Como** líder técnico **quiero** respuestas a las preguntas abiertas de `ARCHITECTURE.md` §9 y ADRs aprobados **para** desbloquear la construcción sin retrabajo.

**Criterios de aceptación**
- Confirmados proyecto y región de GCP (`project-0568d9ba-848f-413c-88b`, nombre `proyectoDatos`, número `63567159332`, `us-central1`) y cuenta y región de AWS (`648617469176`, `us-east-1`).
- Decidido ADR-02 (Cloud Run vs. VM en GCE).
- Confirmadas las tres fuentes (Reddit, Facebook, Instagram) y los cambios propuestos al generador (§3.2), en especial `marca`/`sentimiento` como parámetros y la columna opcional `sentimiento`; compartido el script del generador o su lista de plantillas.
- Definida la empresa simulada y sus marcas (Postobón, Pepsi, Sprite y las demás que produzca el generador), con su rol propia/competidor (insumo de `brands.yaml`).
- Aceptado el NLP en español con RoBERTuito/pysentimiento (ADR-05) y su licencia académica.
- Verificado el estado real de `infra/` (sin nada aplicado) y las cuotas de vCPU y Dataflow en `us-central1`. El acceso a QuickSight se asume; su verificación se hace en HU-037.
- Definido el canal de alertas (correo; por ahora el único destinatario es el de Alejandro) y el responsable de las etiquetas (`owner=alejandro`).
---

# E1 · Infraestructura como código

### HU-004 · Infraestructura GCP base

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E1 | 1 · 1.3 | M | Must | 2 | HU-046 |

**Como** líder técnico **quiero** provisionar con Terraform la base de GCP **para** que simulador y pipeline tengan dónde correr de forma reproducible.

**Criterios de aceptación**
- Existen los buckets `…-ta-datasets` y `…-ta-lake`.
- Existen los topics `ta-raw-{reddit,facebook,instagram}`, sus suscripciones `ta-raw-{source}-df`, `ta-dlq` y `ta-alerts`, con retención de 7 días.
- Existen `sa-simulator`, `sa-dataflow` y `sa-exporter` con los permisos mínimos de §5.8.
- Todos los recursos usan prefijo `ta-` y etiquetas `project`, `env`, `owner`.
- `terraform validate` pasa y los `outputs` de §5.9 están definidos.

### HU-005 · Recursos de streaming (BigQuery, secreto HMAC)

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E1 | 2 · 2.4 | M + C | Must | 2 | HU-004 |

**Como** ingeniero de datos **quiero** BigQuery, la llave HMAC y la plantilla de Dataflow provisionadas **para** desplegar el pipeline de streaming.

**Criterios de aceptación**
- Dataset `ta_stream` con `posts_enriched`, `trend_windows`, `dlq_events` y `dq_results`.
- `posts_enriched` particionada por `DATE(event_time)` y con clustering por `source, brand_primary`.
- Secreto `ta-hmac-key` creado y accesible solo para `sa-dataflow` (y el loader).
- Plantilla de Dataflow parametrizada con los `outputs` de Terraform, sin valores hardcodeados.

### HU-006 · Federación OIDC GCP → AWS

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E1 | 3 · 3.2 | M | Must | 2 | HU-004, HU-007 |

**Como** líder técnico **quiero** que GCP asuma un rol de AWS sin llaves estáticas **para** transferir datos entre nubes de forma segura.

**Criterios de aceptación**
- Existe un OIDC identity provider de Google en IAM de AWS.
- `sa-exporter` asume `ta-gcp-exporter-role` mediante `AssumeRoleWithWebIdentity`.
- El rol solo permite `s3:PutObject`, `s3:HeadObject` y `kms:GenerateDataKey` sobre `raw/origin=stream/`.
- Si la federación no funciona a tiempo, se activa el plan B (llaves en Secret Manager) y se registra como deuda técnica.

### HU-007 · Infraestructura AWS del lakehouse

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E1 | 4 · 4.1 | M | Must | 2 | HU-003 |

**Como** líder técnico **quiero** provisionar S3, Glue, Athena, KMS e IAM **para** tener la base del lakehouse.

**Criterios de aceptación**
- Bucket `ta-{ENV}-{account_id}-lake` con carpetas `raw/`, `trusted/`, `curated/`, `quality/`, `athena-results/`, cifrado con KMS.
- Bases Glue `ta_raw`, `ta_trusted`, `ta_curated` con tablas que usan partition projection.
- Workgroup `ta-analytics` con límite de bytes escaneados por consulta.
- Roles `ta-glue-role`, `ta-historical-loader`, `ta-quicksight-role` con mínimo privilegio.
- Outputs definidos: `lake_bucket`, `glue_db_names`, `athena_workgroup`, `exporter_role_arn`, `glue_role_arn`, `kms_key_arn`.

### HU-046 · Bootstrap de Terraform y entornos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E1 | 1 · 1.0 | M | Must | 2 | HU-003 |

**Como** líder técnico **quiero** crear el proyecto de GCP, el estado remoto de Terraform y habilitar las APIs **para** provisionar toda la infraestructura desde cero de forma reproducible.

**Criterios de aceptación**
- Proyecto GCP `project-0568d9ba-848f-413c-88b` (`proyectoDatos`) con facturación por crédito (a cargo de Alejandro Sepulveda) verificada, región de trabajo `us-central1` (ADR-12) y cuotas de Dataflow/Compute revisadas en esa región.
- Alejandro tiene acceso con rol Owner o Editor a GCP y AWS (por ahora es el único usuario); el acceso de Miguel y Camila, con roles mínimos, se otorga cuando lo necesiten sus lotes. Terraform se autentica en AWS con un perfil o SSO documentado y en GCP con Application Default Credentials (`gcloud auth application-default login`), sin llaves en el repositorio.
- APIs habilitadas: Pub/Sub, Dataflow, BigQuery, Cloud Run, Artifact Registry, Secret Manager, Cloud Scheduler, IAM, Monitoring y Logging.
- Estado remoto de Terraform: bucket GCS con versionado (GCP) y bucket S3 con bloqueo (AWS), ADR-14.
- Existe la estructura `infra/{gcp,aws}/{modules,environments/dev}` y `terraform init` y `terraform validate` pasan en ambas nubes.
- `terraform.tfvars` está en `.gitignore` y se versiona un `terraform.tfvars.example` sin secretos.
- Presupuesto mínimo con alerta activo en GCP y AWS desde el primer día (se amplía en HU-043).
- Etiquetas comunes definidas: `project=trend-analyzer`, `env=dev`, `owner`.

---

# E2 · Simulación de fuentes

### HU-008 · Adaptación del generador de comentarios

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.1 | A | Must | 2 | HU-001 |

**Como** ingeniero de datos **quiero** integrar el generador de comentarios (`marca,comentario`) como módulo puro y reproducible **para** que el simulador y el loader histórico produzcan texto en español colombiano sobre las marcas sin lógica específica por fuente.

**Criterios de aceptación**
- El generador vive en `src/simulator/generator/comment_generator.py` y expone `generate(rng, marca=None, sentimiento=None) -> Comentario` como función pura, con un `random.Random` inyectado (sin `random.choice` global ni semilla global).
- Las marcas se leen de `brands.yaml`: `postobon` (propia), `pepsi` y `sprite` (competidores); CocaCola y Colombiana quedan fuera de v1. Clave canónica sin tilde.
- `Comentario` incluye `marca`, `texto`, `categoria` y `sentimiento`. Mapeo (ADR-13): `gusto → positive`, `asco → negative`, `informativo → neutral`, `nostalgia` y `adiccion → null`. El parámetro `sentimiento` restringe la categoría.
- Se añade la categoría `informativo` (mínimo 15 plantillas) y al menos 15 plantillas comparativas con dos marcas (p. ej. "Pepsi vs Postobón").
- Los nombres de marca se protegen del *typo* del ruido; con el ruido por defecto, ≥ 95% de los comentarios conservan la marca detectable por alias normalizados (sin tildes ni mayúsculas).
- Se expone una función que extrae `hashtags[]` del texto con regex, porque los cierres ya incluyen hashtags (`#Colombia`, `#<marca>`).
- Se elimina todo `sleep`, bucle infinito, `print` o escritura directa a archivo; la unicidad en batch usa `hashlib`, no `hash()` de Python.
- `scripts/generate_comments.py --n 50000 --seed 7 --out comentarios.csv` genera un CSV con las columnas `marca,comentario,sentimiento` (más `categoria` con `--categoria`; §3.2.1 #7), escrito con el módulo `csv`; los snapshots opcionales se suben a `gs://…-ta-datasets/snapshots/` y no se versionan.
- Tests con `pytest`: misma semilla ⇒ mismo comentario; la marca pedida aparece en la columna y en el texto; el mapeo categoría → sentimiento se cumple.
### HU-009 · Librería común

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.1 | A | Must | 2 | HU-001 |

**Como** ingeniero de datos **quiero** una librería compartida (`src/common/`) con envelope, hashing y mapa de etiquetas **para** que Dataflow y el loader histórico usen exactamente el mismo código.

**Criterios de aceptación**
- Construye y valida el envelope según el schema de HU-001 (incluye `generator` y `seq`).
- Incluye la anonimización HMAC-SHA256 y la generación de `event_id` con `uuid5(NAMESPACE_URL, "{origin}|{source}|{seed}|{seq}")`.
- Incluye `label_map.yaml` con el mapeo `NEG/NEU/POS` → `negative/neutral/positive` y las 7 clases de emoción (la neutra del modelo → `neutral`).
- Tests unitarios con `pytest` que cubren envelope, hashing, `event_id` determinista y mapeo de etiquetas.
- Código en Python 3.11 con type hints, sin secretos; no usa `hash()` de Python para sembrar.
### HU-010 · Fuente Facebook simulada

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Must | 2 | HU-008, HU-009 |

**Como** equipo de Marketing **quiero** ver publicaciones y comentarios en las páginas de las marcas **para** seguir la conversación en una red social abierta al público general. *(Reemplaza US-10.)*

**Criterios de aceptación**
- Un worker pide comentarios al generador y publica en `ta-raw-facebook` cada 1–3 s.
- Cerca del 15% de los eventos son `post` (`parent_id = null`) y el 85% son `comment` con `parent_id` de un post reciente simulado.
- El `payload` incluye `comment_id`, `kind`, `page_id`, `page_name` (`facebook_page` de la marca en `brands.yaml`), `post_id`, `parent_id`, `reactions_total`, `reactions` (`like/love/haha/wow/sad/angry`), `shares_count`, `comments_count`, `permalink`.
- `source = "facebook"` y `generator = "comment-gen@1.0"` en cada evento.
- Los mensajes cumplen el envelope v1.0.
### HU-011 · Fuente Reddit simulada

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Must | 2 | HU-008, HU-009 |

**Como** equipo de Marketing **quiero** ver discusiones tipo foro sobre las marcas **para** entender opiniones más extensas. *(Reemplaza US-11.)*

**Criterios de aceptación**
- Un worker pide comentarios al generador y publica en `ta-raw-reddit`.
- Cerca del 30% de los eventos son `post` (título = primera frase) y el 70% son `comment` con `parent_id` de un post reciente.
- El `payload` incluye `reddit_id`, `kind`, `subreddit` (de una lista simulada, p. ej. `r/colombia`, `r/medellin`), `title`, `parent_id`, `score`, `upvote_ratio`, `num_comments`, `permalink`, `flair`.
- No depende de OAuth ni de aprobaciones externas.
### HU-012 · Fuente Instagram simulada

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Must | 2 | HU-008, HU-009 |

**Como** equipo de Marketing **quiero** ver comentarios sobre las publicaciones de las cuentas de marca **para** conocer cómo reacciona la audiencia en una red visual. *(Reemplaza US-12.)*

**Criterios de aceptación**
- Un worker pide comentarios al generador y publica en `ta-raw-instagram`.
- El 100% de los eventos son `comment`, asociados a una publicación (`media_id`) de la cuenta de la marca (`instagram_account`).
- El `payload` incluye `comment_id`, `kind`, `media_id`, `media_type` (`IMAGE|VIDEO|CAROUSEL_ALBUM`), `account_username`, `parent_id`, `like_count`, `comments_count`, `permalink` (dominio ficticio `sim.local`).
- No hay `shares`: la normalización lo deja en `null`.
- El handle sigue el estilo `@usuario_89` y los hashtags se añaden con mayor probabilidad que en las otras redes (~60% por defecto).
### HU-013 · Cursor persistente y emisión determinista

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Must | 2 | HU-010 a HU-012 |

**Como** ingeniero de datos **quiero** que el simulador recuerde su posición y que cada evento sea reproducible **para** emitir de forma continua, sobrevivir a reinicios y deduplicar sin perder el hilo.

**Criterios de aceptación**
- El cursor `{seed, seq, updated_at}` se guarda en `sim-state/{source}.json` cada 50 mensajes.
- Al reiniciar, el simulador continúa desde el último `seq`; si `SIM_SEED` no coincide con la semilla del cursor, reinicia en `seq = 0`.
- Todo el aleatorio sale de `random.Random(f"{seed}|{source}|{seq}")`: misma semilla + fuente + `seq` ⇒ mismo texto y mismo `event_id`.
- `event_id` es determinista: el mismo `(origin, source, seed, seq)` produce el mismo id; el texto exacto puede repetirse entre eventos (plantillas) y eso no cuenta como duplicado.
- Entrega al menos una vez: un reinicio puede repetir hasta 50 mensajes con los mismos ids.
- Tests unitarios de determinismo y de reanudación del cursor.
### HU-014 · Enriquecimiento sintético

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Must | 2 | HU-008, HU-009 |

**Como** analista **quiero** usuarios, engagement, marcas y hashtags verosímiles **para** poder analizar relaciones y tendencias aunque el generador solo entregue marca y texto.

**Criterios de aceptación**
- Pool estable de ~2.000 usuarios por fuente (`Faker('es_CO')`, semilla fija) con actividad tipo Zipf y seguidores lognormales; el mismo pool se usa en stream e histórico.
- Handle por red: Reddit `u/usuario`, Facebook nombre visible, Instagram `@usuario_89`.
- Engagement por red: Reddit (`score`/`upvote_ratio`/`num_comments`), Facebook (reacciones con desglose, `shares_count`, `comments_count`), Instagram (`like_count`, `comments_count`).
- La marca viene del generador; el simulador elige la marca con pesos de `brands.yaml` que varían con `seq` para crear tendencias reproducibles. `brands[]` = marca elegida + otras marcas detectadas por alias en el texto (`[]` solo si no hay ninguna).
- Hashtags: 1–2 derivados de la marca y del sector, añadidos al texto y a `hashtags[]` con probabilidad por red configurable en `sim.yaml` (por defecto Instagram ~60%, Facebook ~15%, Reddit ~0%).
- Fechas en UTC con ISO-8601: `event_time` = emisión menos un lag de 0–5 s; `ingest_time` = instante de publicación.
- Toda la aleatoriedad es reproducible con semilla.
### HU-015 · Caos controlado

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.2 | A | Should | 2 | HU-010 a HU-012 |

**Como** responsable de gobernanza **quiero** inyectar defectos controlados **para** tener datos reales que medir con las reglas de calidad.

**Criterios de aceptación**
- Con `SIM_CHAOS_ENABLED=true` se generan duplicados (0.5%, re-publican el mismo `event_id`), tardíos (1%), nulos en campos opcionales como `lang` y `followers` (1%) y malformados (0.2%), configurables en `sim.yaml`.
- Cada defecto queda registrado en `sim.chaos`.
- Con el caos apagado no se genera ningún defecto (línea base de KPIs).

### HU-016 · Despliegue del simulador

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.3 | A + M | Must | 2 | HU-004, HU-013 |

**Como** líder técnico **quiero** el simulador desplegado en Cloud Run **para** que emita datos de forma continua sin intervención.

**Criterios de aceptación**
- `Dockerfile` construye la imagen y se publica en Artifact Registry.
- Servicio `ta-stream-simulator` con `min-instances=1`, `max-instances=1` y CPU siempre asignada.
- `GET /healthz` responde 200.
- Los 3 topics reciben mensajes tras el despliegue.

### HU-017 · Verificación de la ingesta

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.4 | A | Should | 2 | HU-016 |

**Como** ingeniero de datos **quiero** un script que se suscriba y reporte métricas de emisión **para** comprobar que la ingesta cumple lo esperado antes de construir encima.

**Criterios de aceptación**
- Reporta tasa de mensajes por fuente (~0.5 msg/s), cantidad de `event_id` únicos y `seq` actual.
- Verifica que el cursor sobrevive a un reinicio del servicio (el `seq` continúa, sin reiniciar en 0).

### HU-047 · Entorno local de desarrollo

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E2 | 1 · 1.5 | A | Should | 2 | HU-010 a HU-013 |

**Como** ingeniero de datos **quiero** correr el simulador y el pipeline en local contra el emulador de Pub/Sub **para** desarrollar y probar sin gastar créditos ni depender de la nube.

**Criterios de aceptación**
- `docker-compose up` levanta el emulador de Pub/Sub y el simulador con `SIM_SEED` y las variables de entorno de §5.1.
- Un consumidor local valida cada mensaje contra el schema del envelope y reporta tasa y ids únicos.
- El pipeline de Dataflow se ejecuta con `DirectRunner` leyendo del emulador.
- No requiere credenciales de GCP ni de AWS.
- El README documenta los comandos de arranque, prueba y apagado.

---

# E3 · Streaming en GCP

### HU-018 · Validación, deduplicación y DLQ

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.1 | C | Must | 2 | HU-016 |

**Como** ingeniero de datos **quiero** que Dataflow valide y deduplique cada evento **para** que solo datos íntegros y únicos avancen.

**Criterios de aceptación**
- Dataflow lee de las suscripciones `ta-raw-{source}-df` y valida contra el schema del envelope.
- Los eventos inválidos van a `ta-dlq`, a `gs://…/dlq/` y a `dlq_events` con `error_type` y `error_detail`.
- Los duplicados se eliminan por `event_id` (usando `id_label`).
- El pipeline se prueba primero con `DirectRunner`.

### HU-019 · Persistencia Raw en GCS

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.1 | C | Must | 2 | HU-018 |

**Como** responsable de gobernanza **quiero** conservar el evento tal como llegó **para** auditar y reprocesar si es necesario.

**Criterios de aceptación**
- Se escriben `raw/source={s}/dt={YYYY-MM-DD}/hr={HH}/part-*.jsonl.gz` en `…-ta-lake`.
- Las particiones usan fecha/hora de proceso.
- El acceso a `raw/` está restringido (contiene el handle simulado).

### HU-020 · Anonimización de usuarios

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.2 | C | Must | 2 | HU-018, HU-005 |

**Como** responsable de gobernanza **quiero** reemplazar identificadores por un hash antes de salir de GCP **para** garantizar privacidad y cumplir el KPI de 100% de anonimización.

**Criterios de aceptación**
- `author.user_id` y `author.handle` se reemplazan por `author.user_hash` (HMAC-SHA256 hex de 64 caracteres) con la llave de `ta-hmac-key`.
- Ningún evento enriquecido contiene `user_id` ni `handle`.
- El mismo usuario produce siempre el mismo hash.
- Un test automático verifica la ausencia de los campos en claro.

### HU-021 · Normalización y carga a BigQuery

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.2 | C | Must | 2 | HU-020 |

**Como** analista **quiero** métricas de engagement comparables entre fuentes y los datos en BigQuery **para** analizarlas con SQL casi en tiempo real.

**Criterios de aceptación**
- Se calculan `likes`, `shares`, `comments` según la regla de §5.2 (`shares = null` en Reddit e Instagram).
- Los eventos se escriben en `posts_enriched` y como Parquet en `enriched/source={s}/dt=…/hr=…`.
- El registro conserva `label_sentiment` (ground truth, nullable) solo como columna de evaluación y calcula `brand_primary`.

### HU-022 · Análisis de emoción y sentimiento

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.3 | C | Must | 2 | HU-021 |

**Como** equipo de Marketing **quiero** saber qué emoción y sentimiento expresa cada comentario en español **para** entender cómo se siente la audiencia respecto a la marca.

**Criterios de aceptación**
- Cada evento recibe `emotion_label` (7 clases), `emotion_score`, `sentiment_label` (`negative/neutral/positive`) y `sentiment_score`.
- Usa los modelos preentrenados RoBERTuito de pysentimiento (`robertuito-sentiment-analysis` y `robertuito-emotion-analysis`, ADR-05) con `RunInference` en un contenedor personalizado, aplicando su `preprocess_tweet`.
- El mapeo de etiquetas (`NEG/NEU/POS` → `negative/neutral/positive`; emoción neutra → `neutral`) vive en `src/common/label_map.yaml`; se verifica el nombre exacto de la etiqueta al integrar.
- Se registra `nlp_model_version` (`robertuito-sentiment@<hash>+robertuito-emotion@<hash>`).
- Un evento emitido aparece en BigQuery con `emotion_label` en menos de 1 minuto.
- Si la memoria no alcanza, se corre solo el modelo de sentimiento y la emoción queda como mejora; si es inviable en Dataflow, se activa el fallback documentado (modelo ligero, modelo multilingüe o NLP en Cloud Run).
- Se verifica la licencia de cada modelo en su *model card*.
### HU-023 · Detección de tendencias en ventanas

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E3 | 2 · 2.4 | C | Must | 2 | HU-022, HU-005 |

**Como** Gerencia **quiero** detectar cambios de volumen y sentimiento por marca **para** reaccionar rápido a una tendencia emergente.

**Criterios de aceptación**
- `trend_windows` contiene ventanas deslizantes de 5 min (paso 1 min) por `brand` × `source`.
- Cada ventana incluye `n_posts`, `n_negative`, `n_positive`, `avg_sentiment`, `emotion_counts`, `zscore_volume`, `zscore_sentiment` e `is_alert`.
- La latencia `process_time − event_time` cumple p95 < 5 min en eventos no tardíos.
- Las ventanas toleran eventos tardíos hasta 30 min; los más tardíos se escriben en `posts_enriched` sin recalcular ventanas (contador `late_events`).
- Arranque en frío: `is_alert = false` hasta tener 60 ventanas de baseline (§5.12).

---

# E4 · Histórico y puente a AWS

### HU-024 · Carga histórica (full load)

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E4 | 3 · 3.1 | A | Must | 2 | HU-008, HU-009, HU-007 |

**Como** analista **quiero** un volumen histórico de comentarios **para** analizar estacionalidad y comparar con el streaming.

**Criterios de aceptación**
- `historical/loader` usa el **mismo generador** de comentarios en batch (con otra semilla) para producir ~50k registros (configurable) repartidos entre `reddit`, `facebook` e `instagram`.
- Fechas repartidas en los últimos 180 días, con estacionalidad semanal y horaria y tendencias de marca para que haya historia que comparar.
- Usa el mismo envelope (`origin = "historical"`), el mismo pool de usuarios y el mismo código de anonimización y NLP (`src/common/`); el NLP corre en lote.
- Escribe Parquet a `s3://…/raw/origin=historical/`.
- Se ejecuta una vez y es reejecutable sin duplicar datos (`event_id` determinista por semilla y `seq`).
### HU-025 · Exportación GCS → S3

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E4 | 3 · 3.2 | M | Must | 2 | HU-021, HU-006 |

**Como** ingeniero de datos **quiero** copiar periódicamente los datos enriquecidos de GCP a AWS **para** alimentar el lakehouse sin mover datos personales.

**Criterios de aceptación**
- Cloud Run Job `ta-gcs-to-s3` lanzado por Cloud Scheduler cada 15 min.
- Lee solo `enriched/` y usa `export-state/watermark.json`; actualiza el watermark únicamente tras subir con éxito.
- Escribe en `raw/origin=stream/source={s}/dt=…/hr=…/*.parquet` con SSE-KMS.
- Es idempotente: omite objetos ya existentes con el mismo tamaño.
- Los datos de stream e histórico aparecen en `s3://…/raw/`.

---

# E5 · Lakehouse en AWS

### HU-026 · Capa Trusted

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E5 | 4 · 4.2 | C | Must | 2 | HU-024, HU-025, HU-007 |

**Como** analista **quiero** una tabla única, limpia y deduplicada **para** consultar stream e histórico sin preocuparme de su origen.

**Criterios de aceptación**
- Job `ta-job-raw-to-trusted` une `origin=stream` e `origin=historical` en `ta_trusted.posts`.
- Deduplica por `event_id`, corrige tipos y limpia el texto.
- Aplica las reglas de calidad definidas en HU-032.
- Duplicados < 1% medidos en Trusted.
- El job es idempotente: sobrescribe las particiones `dt`, de modo que reejecutarlo no duplica datos.

### HU-027 · Capa Curated (modelo estrella)

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E5 | 4 · 4.3 | C | Must | 2 | HU-026 |

**Como** Gerencia **quiero** un modelo analítico optimizado **para** obtener indicadores rápidos y consistentes.

**Criterios de aceptación**
- Existen `fact_posts`, `dim_date`, `dim_source`, `dim_brand`, `dim_user` (solo con `user_hash`).
- Existen los agregados `agg_emotion_by_brand_hour` y `agg_trend_daily`.
- Construido con dbt-athena o Glue, con tests.
- `SELECT` sobre `ta_curated.fact_posts` devuelve datos de ambos orígenes.

### HU-028 · Tablas de aristas

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E5 | 4 · 4.4 | C | Should | 2 | HU-027 |

**Como** analista **quiero** relaciones usuario→marca y co-ocurrencia de hashtags **para** analizar propagación sin una base de grafos (ADR-09).

**Criterios de aceptación**
- `edges_user_brand` con peso igual al número de menciones.
- `edges_hashtag_cooccurrence` con pares de hashtags y frecuencia.
- Ambas son consultables desde Athena.

### HU-029 · Orquestación horaria

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E5 | 4 · 4.1 | M | Must | 2 | HU-007 |

**Como** líder técnico **quiero** que el pipeline de Glue corra solo cada hora **para** no depender de ejecuciones manuales.

**Criterios de aceptación**
- EventBridge `rate(1 hour)` dispara el workflow `ta-wf-raw-to-curated`.
- El workflow ejecuta en orden `ta-job-raw-to-trusted` y `ta-job-trusted-to-curated`.
- Los fallos quedan registrados en CloudWatch.

### HU-030 · Consulta SQL para analistas

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E5 | 4 · 4.3 | C | Must | 2 | HU-007, HU-027 |

**Como** analista **quiero** consultar los datos con SQL en Athena **para** explorar libremente sin depender de un tercero.

**Criterios de aceptación**
- Workgroup `ta-analytics` operativo con límite de bytes escaneados.
- Las particiones se resuelven con partition projection (sin crawlers).
- Existe un conjunto de consultas de ejemplo documentado.

---

# E6 · Calidad y gobernanza

### HU-031 · Profiling de datos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.1 | A | Must | 2 | HU-025 |

**Como** responsable de gobernanza **quiero** perfilar los datos de Raw y Trusted **para** conocer su estado real antes de fijar reglas de calidad. *(Equivale a US-14.)*

**Criterios de aceptación**
- Informe de profiling con nulos, cardinalidad, distribuciones, tipos y anomalías por columna.
- Cubre las capas Raw y Trusted, y tanto `origin=stream` como `origin=historical`.
- Incluye la diversidad léxica y el texto repetido del generador (se mide duplicados por `event_id`, no por texto).
- Las conclusiones alimentan las reglas de HU-032.

### HU-032 · Reglas de calidad y KPIs

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.2 | A | Must | 2 | HU-026, HU-031 |

**Como** Gerencia **quiero** medir la calidad de los datos contra KPIs **para** confiar en lo que muestra el dashboard. *(Equivale a US-13.)*

**Criterios de aceptación**
- Suites de Great Expectations y tests dbt según §5.10.
- Completitud ≥ 95%, unicidad ≥ 99%, duplicados < 1% (medido en Trusted tras dedup), anonimización 100%.
- Resultados escritos en BigQuery `dq_results` y en `s3://…/quality/`.
- Un test en CI falla si existen columnas `handle` o `user_id`.

### HU-033 · Precisión del NLP

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.2 | A | Could | 2 | HU-008, HU-022, HU-026 |

**Como** responsable de gobernanza **quiero** comparar `sentiment_label` con `sim.label_sentiment` **para** cuantificar qué tan bien funciona el modelo en español.

**Criterios de aceptación**
- Reporte con accuracy, precisión y recall por clase (matriz de confusión) del sentimiento, sobre los eventos con `label_sentiment` distinto de `null` (`nostalgia` y `adiccion` se excluyen, ADR-13).
- No hay ground truth de emoción: para `emotion_label` solo se reporta su distribución.
- `label_sentiment` se usa solo para evaluación, nunca como feature ni como respuesta del pipeline.
- Se declara la limitación: comentarios de plantillas y modelos entrenados en tweets hacen que las métricas sean optimistas.
- Resultados incluidos como insumo del informe.
### HU-034 · Catálogo de datos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.3 | A | Must | 2 | HU-026, HU-027 |

**Como** analista **quiero** descripciones claras de tablas y columnas **para** entender qué significa cada dato.

**Criterios de aceptación**
- Todas las tablas de `ta_raw`, `ta_trusted` y `ta_curated` tienen descripción en el Glue Data Catalog.
- Cada columna tiene descripción, tipo y si contiene dato anonimizado.

### HU-035 · Linaje de datos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.3 | A | Must | 2 | HU-027 |

**Como** responsable de gobernanza **quiero** trazar de dónde viene cada dato **para** poder auditarlo y analizar el impacto de un cambio.

**Criterios de aceptación**
- Linaje generado con dbt docs que cubre Raw → Trusted → Curated.
- Documento y diagrama de linaje de extremo a extremo (generador de comentarios → simulador → Pub/Sub → Dataflow → BigQuery/S3 → Curated → dashboard).

### HU-036 · Cumplimiento normativo y DAMA-DMBOK

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.3 | A | Must | 2 | HU-020 |

**Como** responsable de gobernanza **quiero** documentar el tratamiento de datos según la Ley 1581 y DAMA-DMBOK **para** demostrar un manejo responsable aunque los datos sean sintéticos.

**Criterios de aceptación**
- Mapeo de las áreas de conocimiento de DAMA-DMBOK a componentes del proyecto.
- Nota de cumplimiento de la Ley 1581: anonimización, acceso restringido a `raw/`, minimización.
- Se declara explícitamente que los datos son sintéticos y las limitaciones (comentarios de plantillas con vocabulario acotado, asociación marca→sentimiento artificial, métricas de NLP optimistas).

### HU-048 · Prueba de humo end-to-end

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E6 | 5 · 5.4 | A | Must | 2 | HU-025, HU-026, HU-027 |

**Como** equipo **quiero** una prueba automatizada que siga un evento desde el simulador hasta Athena **para** demostrar el flujo completo antes del Sprint 2 y medir las latencias reales.

**Criterios de aceptación**
- `scripts/smoke_e2e.py` emite (o localiza) un evento centinela con `event_id` conocido.
- Verifica su aparición en BigQuery `posts_enriched` (< 1 min), en `enriched/` de GCS, en `raw/origin=stream/` de S3 y en `ta_curated.fact_posts` consultado desde Athena.
- Reporta la latencia de cada etapa y falla si algún paso excede su objetivo.
- Comprueba que el evento no contiene `handle` ni `user_id` en ningún punto posterior a Dataflow.
- Los resultados se guardan como evidencia para el informe (HU-044) y el ensayo (HU-045).

---

# E7 · Serving y alertas

### HU-037 · Dashboard de tendencias y sentimiento

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E7 | 6 · 6.1 | C | Must | Final | HU-027 |

**Como** equipo de Marketing y Gerencia **quiero** un dashboard de tendencias, emociones y sentimiento por marca y fuente **para** tomar decisiones sin escribir SQL y reducir el tiempo de elaboración de informes.

**Criterios de aceptación**
- Datasets de QuickSight sobre `ta_curated` vía Athena, importados a SPICE.
- Muestra evolución de volumen, sentimiento y emociones por marca, fuente y periodo, comparando marca propia contra competidores.
- Tiempo de respuesta < 3 s (prueba manual documentada).
- Acceso por roles IAM.
- Se documenta la reducción ≥ 50% del tiempo de elaboración de informes.
- Los datos se almacenan en UTC y el dashboard muestra la hora en `America/Bogota`.

### HU-038 · Vista de relaciones

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E7 | 6 · 6.1 | C | Should | Final | HU-028, HU-037 |

**Como** analista **quiero** visualizar relaciones entre usuarios, marcas y hashtags **para** entender cómo se propaga la conversación.

**Criterios de aceptación**
- Una vista del dashboard usa `edges_user_brand` y `edges_hashtag_cooccurrence`.
- Solo se muestra `user_hash`, nunca identificadores en claro.

### HU-039 · Alertas de cambios en la conversación

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E7 | 6 · 6.2 | C | Should | Final | HU-023 |

**Como** Gerencia **quiero** recibir una alerta cuando cambie bruscamente el volumen o el sentimiento de una marca **para** actuar a tiempo.

**Criterios de aceptación**
- Cuando `trend_windows.is_alert` es verdadero se publica un mensaje en `ta-alerts`.
- El mensaje dispara una notificación por correo (alerta basada en logs de Cloud Monitoring con canal email, §5.12).
- La alerta incluye marca, fuente, ventana y magnitud del cambio.
- La detección se hace en < 5 min.
- Hay un *cooldown* de 15 min por marca × fuente × tipo de alerta para evitar alertas repetidas.

### HU-040 · Escenarios de demo

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E7 | 6 · 6.3 | A | Could | Final | HU-008, HU-016, HU-039 |

**Como** presentador del proyecto **quiero** forzar una ola controlada de comentarios con un sentimiento sobre una marca **para** mostrar las alertas en la sustentación sin esperar una tendencia real.

**Criterios de aceptación**
- `POST /control/scenario` con `brand`, `sentiment`, `multiplier`, `duration_s` y `source` (opcional) fuerza esa marca y ese sentimiento al generador y sube la frecuencia durante ese tiempo.
- Los eventos generados durante el escenario quedan marcados en `sim.scenario`.
- El endpoint está protegido con ID token de Cloud Run (no es público).
- Existe un guion de demo documentado.
---

# E8 · DataOps

### HU-041 · CI/CD

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E8 | 7 · 7.1 | M | Must | Final | HU-004, HU-007 |

**Como** líder técnico **quiero** pipelines automáticos en GitHub Actions **para** validar y desplegar cambios de forma segura y repetible.

**Criterios de aceptación**
- En cada PR corren `pytest`, `terraform fmt/validate/plan` y el test de ausencia de `handle`/`user_id`.
- Se construyen y publican las imágenes Docker en Artifact Registry.
- Ningún secreto está en el repositorio y `terraform.tfvars` permanece en `.gitignore`.
- Hay *pre-commit* con `gitleaks` y el test de contratos de HU-001 corre en cada PR.

### HU-042 · Observabilidad

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E8 | 7 · 7.2 | M | Must | Final | HU-016, HU-029 |

**Como** líder técnico **quiero** dashboards y alertas operativas en ambas nubes **para** detectar fallas antes de que lo haga el usuario.

**Criterios de aceptación**
- Cloud Monitoring muestra tasa de mensajes, latencia p95 y tamaño de la DLQ.
- CloudWatch muestra el estado de los jobs de Glue.
- Hay alertas para fallos del pipeline, latencia fuera del objetivo y DLQ creciente.
- Los logs son JSON a stdout.

### HU-043 · Control de costos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E8 | 7 · 7.3 | M | Should | Final | HU-016, HU-018 |

**Como** líder técnico **quiero** poder apagar y encender los recursos costosos y recibir alertas de presupuesto **para** no agotar los créditos fuera de las demos.

**Criterios de aceptación**
- Scripts de encendido/apagado de Dataflow y del simulador.
- Alertas de presupuesto en GCP y AWS.
- Dataflow configurado con `maxNumWorkers` de 1 a 2.
- Las etiquetas permiten desglosar costos por componente.
- Políticas de ciclo de vida aplicadas: GCS `raw/` y `dlq/` a 30 días, partición de `posts_enriched` a 90 días, `athena-results/` a 7 días (§5.12).

---

# E9 · Entrega final

### HU-044 · Informe técnico y KPIs medidos

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E9 | 8 · 8.1 | Todos | Must | Final | Todas las anteriores |

**Como** profesor evaluador **quiero** un informe técnico con los KPIs efectivamente medidos **para** valorar el diseño, la implementación y sus limitaciones.

**Criterios de aceptación**
- Incluye arquitectura, decisiones (ADR), contratos y justificación del cambio a fuentes simuladas (Reddit, Facebook, Instagram) con un generador de comentarios.
- Tabla de KPIs medidos vs. objetivo: latencia < 5 min, completitud ≥ 95%, unicidad ≥ 99%, duplicados < 1%, dashboard < 3 s, anonimización 100%.
- Limitaciones declaradas: datos sintéticos, comentarios de plantillas con vocabulario acotado, asociación marca→sentimiento artificial, métricas de NLP optimistas.
- Cita los modelos NLP usados (RoBERTuito/pysentimiento, entrenados con corpus TASS) y su licencia.
- Cada KPI tiene una consulta o notebook reproducible, y la tabla del informe se alimenta de los resultados de HU-048.

### HU-045 · Sustentación y runbook

| Épica | Fase · Lote | Responsable | Prioridad | Sprint | Depende de |
|---|---|---|---|---|---|
| E9 | 8 · 8.1 | Todos | Must | Final | HU-044, HU-040 |

**Como** equipo **quiero** un guion de sustentación y un runbook operativo **para** demostrar el sistema de extremo a extremo y poder operarlo.

**Criterios de aceptación**
- Guion de demo: simulador → Pub/Sub → Dataflow → BigQuery → S3 → Athena → QuickSight, incluyendo un pico y su alerta.
- Runbook con encendido/apagado, reinicio del simulador, reproceso desde `raw/` y atención de la DLQ.
- Plan B de la demo si falla algo en vivo: capturas del dashboard, ejecución grabada y simulador local (HU-047).
- Ensayo completo realizado antes del Sprint Final.