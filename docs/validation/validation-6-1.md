# Validación del Lote 6.1 · HU-037 y HU-038

> **Tablero de QuickSight sobre `ta_curated`** (ADR-31). QuickSight está suscrito en la cuenta (Enterprise,
> `us-east-1`, autenticación `IAM_AND_QUICKSIGHT`; decisión del usuario del 27-oct-2026, prueba gratis hasta
> ≈ 26-nov-2026). `project/scripts/quicksight.py deploy` crea o actualiza la fuente Athena `ta-athena-curated` (con el
> rol de servicio `ta-quicksight-role`), cuatro datasets SPICE con refresco cada hora, los grupos `ta-autores` y
> `ta-lectores` con los usuarios de los roles IAM `ta-quicksight-autor` y `ta-quicksight-lector`, y el tablero
> `ta-tablero-tendencias` con las hojas «Tendencias» (HU-037) y «Relaciones» (HU-038). Contrato en
> `docs/ARCHITECTURE.md` §3.27 (interfaces en §3.27.1); guía de uso en `project/src/serving/QUICKSIGHT.md`.
>
> **Automático:** `python project/scripts/validate_6_1.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_6_1.json`.
>
> **Cambia la nube** (con el OK del usuario del 27-oct-2026): `terraform --apply` crea los 2 roles IAM; `deploy`
> crea o actualiza lo de QuickSight e importa a SPICE (4 consultas de Athena de pocos MB). `unsubscribe` borra la
> suscripción (irreversible) y **no** es parte de la validación: se corre al terminar el uso del proyecto.

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-037 · Datasets de QuickSight sobre `ta_curated` vía Athena, importados a SPICE | L2, D4–D6, Q1, Q5, Q10 |
| HU-037 · Evolución de volumen, sentimiento y emociones por marca, fuente y periodo, propia frente a competidores | L3, D9, Q4 (hoja «Tendencias»: 11 visuales y filtros marca, red, origen y periodo) |
| HU-037 · Tiempo de respuesta < 3 s (prueba manual documentada) | E1 (mediciones en el navegador), E2 (referencia en Athena) |
| HU-037 · Acceso por roles IAM | C (roles), D3, Q6, Q7, Q8, Q9 |
| HU-037 · Reducción ≥ 50 % del tiempo de elaboración de informes | E3 (estimación por pasos, abajo) |
| HU-037 · Datos en UTC, tablero en `America/Bogota` | L4, Q3 |
| HU-038 · Una vista usa `edges_user_brand` y `edges_hashtag_cooccurrence` | L3, Q4 (hoja «Relaciones») |
| HU-038 · Solo `user_hash`, nunca identificadores en claro (y ni siquiera `user_hash` en los visuales) | L2, Q2, Q5, E4 |

## Cómo ejecutarlo

### Variables

| Variable | Valor | Para qué |
|---|---|---|
| `REPO` | `/mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal` | Raíz del repo en WSL |
| `AWS_PROFILE` | `default` (cuenta `648617469176`, `us-east-1`) | QuickSight, Athena, IAM y Terraform |

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
3. **Suscripción de QuickSight (una vez por cuenta; ya hecha el 27-oct-2026 18:46 UTC).** Si `preflight` da A4 FALLA:
   ```bash
   aws quicksight create-account-subscription --region us-east-1 --aws-account-id 648617469176 \
     --edition ENTERPRISE --authentication-method IAM_AND_QUICKSIGHT \
     --account-name ta-trend-analyzer-648617469176 --notification-email <correo del equipo>
   aws quicksight describe-account-subscription --region us-east-1 --aws-account-id 648617469176
   ```
   Esperado: `ACCOUNT_CREATED` en ~30 s. El correo no se guarda en el repositorio.
4. Lo que se desplegaría, sin nube:
   ```bash
   python project/scripts/quicksight.py plan
   ```
   Esperado: una línea `plan` con `"problems": []`, 4 datasets y las hojas `tendencias` (11 visuales) y `relaciones` (5).

### Ejecución

```bash
python project/scripts/validate_6_1.py preflight
python project/scripts/validate_6_1.py tests
python project/scripts/validate_6_1.py local
python project/scripts/validate_6_1.py terraform --apply --yes   # 2 roles IAM (4 recursos)
python project/scripts/validate_6_1.py deploy --yes              # ≈ 5 min (importación a SPICE)
python project/scripts/validate_6_1.py checks
python project/scripts/quicksight.py embed-url --user 648617469176   # abrir y medir (parte E)
python project/scripts/validate_6_1.py latency --samples "restablecer=2.10,origen_historico=2.83,hoja_relaciones=1.65"
python project/scripts/validate_6_1.py status
```

Salida esperada (resumen): cada parte termina con `-- Parte X: OK` (C0 en AVISO si el perfil es la cuenta raíz) y
código 0.

---

## Preflight: qué debe estar encendido

| Parte | Necesita | Costo |
|---|---|---|
| A, B, L | Nada (A lee QuickSight, IAM y el horario) | 0 |
| C | Terraform con `backend.hcl` y `terraform.tfvars` de `infra/aws` | 0 (roles IAM) |
| D | La suscripción de QuickSight | Prueba gratis; 4 consultas de Athena (~13 MB) |
| Q | Lo de D | ~10 consultas de Athena (< 1 centavo) |
| E | Lo de D y un navegador | 4 consultas de Athena |

**Estado final:** QuickSight suscrito (prueba gratis), 3 usuarios (administrador `ADMIN`, autor `AUTHOR`, lector
`READER`), 4 datasets SPICE con refresco cada hora y el tablero publicado. El horario del lakehouse sigue `ENABLED`.
**Al terminar el uso del proyecto** (no en esta validación): `validate_6_1.py unsubscribe --confirm-unsubscribe`.

---

## Parte A · Preflight

| Paso | Esperado |
|---|---|
| A1 | Python 3.11 con boto3 |
| A2 | Los 15 archivos del lote y los 4 que reutiliza |
| A3 | `aws` y `terraform` |
| A4 | Suscripción `ACCOUNT_CREATED`, `ENTERPRISE`, fin de la prueba (≈ 2026-11-26) |
| A5 | Existe `ta-quicksight-role` (Lote 4.1) |
| A6 | Horario del lakehouse `ENABLED` |

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `pytest src/serving`: 10 passed (definiciones y QuickSight falso) |
| B2 | `pytest src/ops/tests/test_ops_validate61.py`: 12 passed |
| B3 | `pytest src/lakehouse`: 132 passed (regresión: catálogo e infraestructura) |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | `quicksight.py plan` sin problemas |
| L2 | Las tablas leídas son solo de `ta_curated` y ninguna columna de salida es `user_hash`, `user_id` ni `handle` |
| L3 | «Tendencias»: 11 visuales (volumen, sentimiento, propia frente a competidores, red, emociones…) y 4 filtros; «Relaciones»: 5 visuales (coocurrencia, sankey de marcas, combinaciones…) |
| L4 | Solo `ta-ds-publicaciones` tiene horas y las convierte con `AT TIME ZONE 'America/Bogota'` |

## Parte C · Terraform

| Paso | Esperado |
|---|---|
| C0 | Identidad de AWS de la cuenta de `terraform.tfvars` (AVISO si es la raíz) |
| C1 | `bash infra/validate.sh` OK |
| C2 | `plan -target=module.quicksight_access`: **4 a crear** (2 roles y 2 políticas), nada más; o «ya aplicado» |
| C3 | Con `--apply`: `Apply complete! Resources: 4 added, 0 changed, 0 destroyed.` |
| C4 | Plan después del apply sin cambios |

## Parte D · Despliegue

| Paso | Esperado |
|---|---|
| D1 | Suscripción `ACCOUNT_CREATED` |
| D2 | Ningún usuario Pro (la primera vez, el administrador pasa de `ADMIN_PRO` a `ADMIN`) y SPICE con compra automática (`AUTO_PURCHASE`) |
| D3 | `ta-quicksight-autor/demo` (AUTHOR) en `ta-autores` y `ta-quicksight-lector/demo` (READER) en `ta-lectores` |
| D4 | Fuente `ta-athena-curated` `CREATION_SUCCESSFUL` o `UPDATE_SUCCESSFUL` |
| D5 | Los 4 datasets creados o actualizados |
| D6 | Importación a SPICE `COMPLETED` sin filas descartadas (hoy ~221.630, 136, 3 y 7 filas; 45–105 s cada una) |
| D7 | Refresco `ta-cada-hora` (`HOURLY`, `America/Bogota`) en los 4 |
| D8 | Análisis sin errores, hojas «Tendencias» y «Relaciones» |
| D9 | Nueva versión del tablero publicada, sin errores |

## Parte Q · Comprobaciones

| Paso | Esperado |
|---|---|
| Q1 | Filas de la última importación = `count(*)` de la misma consulta en Athena (AVISO si el horario corrió entre medio: `quicksight.py refresh`) |
| Q2 | Columnas que devuelve Athena = las declaradas (nombre, orden, tipo); ninguna de autor |
| Q3 | `event_time − hora_bogota = 5 h` y `fecha_bogota` = día de `hora_bogota` en todas las filas |
| Q4 | El tablero publicado tiene la definición del repositorio (16 visuales, `definition_problems` vacío) |
| Q5 | Los 4 datasets en SPICE con las columnas declaradas; la fuente usa `ta-analytics` y `ta-quicksight-role` |
| Q6 | Permisos del tablero: `ta-autores` y administradores dueños; `ta-lectores` solo `DescribeDashboard`, `ListDashboardVersions` y `QueryDashboard`; nadie más |
| Q7 | Papeles y grupos de los 3 usuarios, sin Pro; costo después de la prueba (≈ 51 USD/mes) |
| Q8 | Los 2 roles confían solo en la cuenta, tienen solo la política de auto-registro y la simulación del lector niega Athena, S3, Glue y `UpdateDashboard` |
| Q9 | QuickSight emite una sesión del lector sobre el tablero (la URL no se guarda) |
| Q10 | Refresco cada hora en los 4 datasets |

## Parte E · Tiempo de respuesta y reducción del tiempo de informes

**Medición (< 3 s con SPICE).** `quicksight.py embed-url --user 648617469176` (o `ta-quicksight-lector/demo`) imprime
una URL de un solo uso (válida 5 min para abrirla; la sesión dura `--minutes`): ábrala en un navegador. No la pegue
en ningún lado. Con el tablero cargado, mida desde el clic hasta que todos los visuales de la hoja muestran el
resultado nuevo:

| Acción | Medido el 27-oct-2026 (navegador del panel de Claude, sesión del administrador) |
|---|---|
| «Restablecer» (vuelve a pedir los 11 visuales de «Tendencias») | 2,10 s y 2,03 s |
| «Origen del dato» = solo `historical` (KPI pasa a 50.000) | 2,83 s |
| Cambiar a la hoja «Relaciones» (5 visuales) | 1,65 s |

Método: un `MutationObserver` sobre la hoja y un `pointerdown` registran el clic y la última mutación del DOM; el
peor caso queda por debajo de 3 s. A mano sirve un cronómetro. Registro: `validate_6_1.py latency --samples
"nombre=segundos,…" [--method "…"]` (E1 exige todas < 3 s; sin `--samples` conserva las de la corrida anterior). E2
corre las 4 consultas en Athena directo como referencia (sin SPICE). E5 lee de CloudWatch `VisualLoadTime` del tablero
(24 h; exige máximo < 3 s) y E6 `DashboardViewLoadTime`, la carga inicial completa (sesión, aplicación y hoja): da
AVISO si pasa de 3 s, porque el KPI se mide por interacción y por visual.

**Reducción del tiempo de elaboración de un informe (E3, estimación por pasos).** Un informe semanal de volumen,
sentimiento y emociones por marca y red:

| Antes: SQL en la consola de Athena + hoja de cálculo | min | Con el tablero | min |
|---|---|---|---|
| Escribir o adaptar 5 consultas (volumen, sentimiento, emociones, redes, hora del día) | 25 | Abrir el tablero y elegir periodo, marca, red y origen | 2 |
| Correrlas, revisar errores y descargar 5 CSV | 10 | Leer los 4 indicadores, los 7 gráficos y la hoja «Relaciones» | 8 |
| Pasar UTC a hora de Bogotá y unir los CSV | 15 | Exportar a CSV lo necesario y redactar el resumen | 15 |
| Armar 6 gráficos | 30 | | |
| Comparar Postobón con Pepsi y Sprite y redactar | 15 | | |
| **Total** | **95** | **Total** | **25** |

**−73,7 %** (≥ 50 %). Las consultas en Athena tardan 1–3 s (medido); los minutos de trabajo humano son una
estimación del equipo, no una medición con usuarios. E4 revisa que la evidencia no tenga correos, tokens ni URLs de
sesión.

## Status

`validate_6_1.py status`: suscripción y días de prueba que quedan, usuarios con su papel y el costo que tendrían al
terminar la prueba, última importación de cada dataset y versión del tablero. Solo lectura.

## Borrar la suscripción (al terminar el uso del proyecto)

```bash
python project/scripts/validate_6_1.py unsubscribe --confirm-unsubscribe
```

Quita la protección contra borrado y borra la suscripción: se van los usuarios, los datasets, el análisis y el
tablero (irreversible). El código queda: para volver, se crea la suscripción (Preparación, paso 3) y se corre
`quicksight.py deploy --yes`. Los roles IAM y `ta-quicksight-role` quedan (no cobran).

## Cierre

**Cerrado en v2.48** (27-oct-2026, WSL; `main` = `6960793` más las dos correcciones de abajo; `ta-streaming`, perfil
`default` = cuenta raíz; con el OK del usuario para la suscripción y el apply).

| Parte | Resultado |
|---|---|
| A · Preflight | OK (6): boto3; 15 + 4 archivos; `aws` y `terraform`; suscripción `ACCOUNT_CREATED` Enterprise (prueba hasta ≈ 2026-11-26); `ta-quicksight-role`; horario `ENABLED` |
| B · Pruebas | OK (3): B1 10, B2 12, B3 132 |
| L · Local | OK (4) |
| C · Terraform | AVISO (6; solo C0 por la cuenta raíz): 4 a crear → `Apply complete! Resources: 4 added, 0 changed, 0 destroyed.` → plan limpio |
| D · Despliegue | OK (9) en la segunda corrida: SPICE `AUTO_PURCHASE`; `ta-quicksight-autor/demo` en `ta-autores`, `ta-quicksight-lector/demo` en `ta-lectores`; 221.630, 136, 3 y 7 filas en 79, 45, 45 y 46 s, 0 descartadas; tablero versión 3 |
| Q · Comprobaciones | OK (10): SPICE = Athena; columnas; **Q3: 0 de 221.630 filas mal**; 16 visuales = repositorio; lectores solo ven; 3 usuarios sin Pro (≈ 51 USD/mes tras la prueba); roles sin acceso a datos; sesión del lector; refresco cada hora |
| E · Respuesta e informes | AVISO (6): E1 2,10 / 2,03 / 2,83 / 1,65 s; E2 Athena 0,6–3,1 s; E3 −73,7 %; **E5 `VisualLoadTime` media 719 ms, máximo 1.861 ms (80 cargas)**; E6 AVISO `DashboardViewLoadTime` 6,8 s (carga inicial completa, 2 vistas); E4 limpia |
| `status` | OK (4): 29 días de prueba |

**Lo que encontró la primera corrida (corregido en v2.48):** Q3 FALLA (`hora_bogota` en UTC por el `CAST` de Athena →
`format_datetime`) y D FALLA («Insufficient SPICE capacity» al actualizar un dataset: sin Pro el límite quedó en 98 MB →
compra automática de SPICE). En el navegador, el lector ve las dos hojas con datos y sin opciones de edición. Sin
comprobar: entrar por la consola asumiendo los roles (la cuenta raíz no puede; el equipo no tiene identidades IAM).
Cifras también en `ARCHITECTURE.md` §3.27 y `CHANGELOG.md` v2.48. QuickSight queda suscrito hasta que el equipo corra
`unsubscribe`.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| A4 / D1 «The account does not exist» | Falta la suscripción (Preparación, paso 3) |
| C2 FALLA «toca …» | El plan cambia algo fuera de `module.quicksight_access`: no aplique; revise el diff |
| D3 FALLA al registrar un rol | Los roles no existen: `terraform --apply` (parte C) antes de `deploy` |
| D6 FALLA (`FAILED`) | `ErrorInfo` de la ingesta: casi siempre permisos de `ta-quicksight-role` (Athena, `curated/`, KMS) o una columna que cambió en `ta_curated` (Q2) |
| D8/D9 FALLA con `errors` | Un campo del tablero no existe en su dataset: `quicksight.py plan` lo detecta antes (`definition_problems`) |
| `ConflictException` en `update_*_permissions` | Una actualización seguía en curso; `deploy` ya espera, repítalo |
| Q1 AVISO | El horario de Glue corrió después de la importación: `python project/scripts/quicksight.py refresh` y repita `checks` |
| La URL de `embed-url` no abre | Vence a los 5 min sin usar: genere otra |
| D FALLA «Insufficient SPICE capacity» | La compra automática de SPICE no quedó activa: `aws quicksight update-spice-capacity-configuration --aws-account-id 648617469176 --purchase-mode AUTO_PURCHASE --region us-east-1` y repita `deploy` |
| Q3 FALLA | Una hora local hecha con `CAST(… AT TIME ZONE … AS timestamp)` (en Athena da UTC): use `format_datetime` como `sql/publicaciones.sql` |
