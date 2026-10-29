# Validación del Lote 7.1 · HU-041

> **CI/CD con GitHub Actions** (ADR-34). `.github/workflows/ci.yml` corre en cada PR, en cada push a `main` y a mano,
> **sin credenciales de nube**: pytest (contratos de HU-001, la prueba de identidad y la suite completa), `terraform
> fmt`/`validate` de las 4 raíces, las tres imágenes en su etapa `test` (pruebas dentro, sin publicar) y gitleaks
> sobre todo el historial, más la comprobación de que ningún `terraform.tfvars` está versionado.
> `.github/workflows/cd.yml` publica las imágenes en Artifact Registry y corre `terraform plan` con OIDC (sin
> llaves). Está **apagado** hasta que el usuario decida activarlo: hay que confiar en el repositorio público y
> subir los `tfvars` como secretos (ver «Activar el CD»). Pre-commit con gitleaks en `.pre-commit-config.yaml`.
> Contrato en `docs/ARCHITECTURE.md` §3.30 (interfaces en §3.30.1).
>
> **Automático:** `python project/scripts/validate_7_1.py <parte>` desde la raíz del repo, en WSL con
> `~/.venvs/ta-streaming` activado. Cada parte deja su resumen en `project/evidencia/validacion_7_1.json`.
>
> **No cambia la nube.** `plan` solo lee (con el CD apagado no debe proponer cambios). `github` lee la API pública de
> GitHub (sin token; 60 consultas por hora).

## Qué se valida

| Criterio | Dónde |
|---|---|
| HU-041 · En cada PR corren `pytest`, `terraform fmt/validate/plan` y el test de ausencia de `handle`/`user_id` | L1 (ci.yml), E1–E3 (la CI en un contenedor limpio), G1 (la corrida real en GitHub). El `plan` contra la nube va en cd.yml (L2, C1–C2 a mano): pendiente de activar |
| HU-041 · Se construyen y publican las imágenes Docker en Artifact Registry | L7 (se construyen y prueban en CI); la publicación, en cd.yml (L2), apagada hasta que el usuario la active. Mientras tanto se publican con los `build_*_image.sh` (como en los lotes 1.3, 3.2, 6.2 y 6.3) |
| HU-041 · Ningún secreto en el repositorio y `terraform.tfvars` en `.gitignore` | L5 (gitleaks sobre todo el historial), L6 (tfvars ignorados y sin versionar), G1 (trabajo `secretos`) |
| HU-041 · Pre-commit con gitleaks y el test de contratos de HU-001 en cada PR | L3 (`.pre-commit-config.yaml`), L1 y E1 (contratos) |

## Cómo ejecutarlo

```bash
cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal
source ~/.venvs/ta-streaming/bin/activate
python project/scripts/validate_7_1.py preflight
python project/scripts/validate_7_1.py tests
python project/scripts/validate_7_1.py local       # ~3 min (las tres imágenes; --skip-images las omite)
python project/scripts/validate_7_1.py emulate     # ~15 min: la CI en python:3.11 limpio
python project/scripts/validate_7_1.py plan        # terraform plan de las dos raíces (solo lectura)
# commit y push a main (git de Windows), luego:
python project/scripts/validate_7_1.py github      # espera la corrida de CI del commit (hasta 45 min)
python project/scripts/validate_7_1.py status
```

## Preflight y apagado

Nada que encender ni apagar: Docker local para L y E, credenciales de GCP y AWS solo para `plan`. Ningún recurso
nuevo en la nube (el CD queda apagado).

## Parte A · Preflight

| Paso | Esperado |
|---|---|
| A1 | Los 13 archivos del lote |
| A2 | `docker`, `terraform`, `git` |
| A3 | Docker en marcha |

## Parte B · Pruebas

| Paso | Esperado |
|---|---|
| B1 | `test_ops_validate71.py`: **9** passed (los workflows reales cumplen HU-041; el CD está apagado y sin llaves; pre-commit, gitleaks, tfvars; la parte G con GitHub falso) |

## Parte L · Local

| Paso | Esperado |
|---|---|
| L1 | `ci.yml` corre en `pull_request` y en push a `main`, solo con `contents: read`, sin secretos, y tiene los 7 pasos de HU-041 |
| L2 | `cd.yml`: trabajos `images` y `plan`, los dos con `if: vars.TA_CD_ENABLED == 'true'`, `id-token: write` y sin llaves |
| L3 | `.pre-commit-config.yaml` con los hooks `gitleaks` y `terraform-fmt` |
| L4 | `bash project/infra/validate.sh`: `fmt -check` y `validate` de las 4 raíces, con los módulos `github_cd` y `github_cd_role` |
| L5 | gitleaks v8.28.0 sobre todo el historial con `.gitleaks.toml` y `.gitleaksignore`: 0 hallazgos nuevos |
| L6 | Ningún `terraform.tfvars` ni `*.auto.tfvars` versionado; los dos `terraform.tfvars` ignorados |
| L7s/L7e/L7a | `build_{simulator,exporter,alerts}_image.sh test`: la etapa `test` de cada imagen (137, 96 y 11 pruebas dentro) |

## Parte E · La CI en un contenedor limpio

`docker run python:3.11` sobre una copia de los archivos versionados (más los nuevos), con `pip install -r
project/requirements-ci.txt` y los mismos tres pasos de pytest de `ci.yml`: E1 contratos, E2 identidad, E3 suite
completa (0 fallas), E4 el tiempo.

## Parte C · Plan

`terraform plan` de `infra/gcp/environments/dev` y de `infra/aws/environments/dev` con el CD apagado: **0 a crear, 0 a
cambiar, 0 a destruir** (los módulos nuevos tienen `count = 0`).

## Parte G · GitHub Actions

| Paso | Esperado |
|---|---|
| G1 | La corrida **CI** del commit (push a `main`) termina en `success`, con los trabajos `pytest`, `terraform fmt y validate`, `imagen simulator/exporter/alerts` y `secretos` en `success` |
| G2 | La corrida **CD** del commit (si tocó rutas del CD) termina con sus trabajos `skipped` |

## Activar el CD (decisión del usuario)

1. En `infra/gcp/environments/dev/terraform.tfvars` y en `infra/aws/environments/dev/terraform.tfvars`:
   `github_cd_enabled = true`. Si la cuenta de AWS ya tiene el proveedor OIDC de GitHub (es uno por cuenta; la
   comparte Campus Locator), poner también `github_oidc_provider_arn = "<su ARN>"`. Luego `terraform apply` en las
   dos raíces: crea el pool `ta-github` con el proveedor `github` (solo este repositorio en `main`), `sa-github-cd`
   (Artifact Registry `ta-images`, Cloud Build, el bucket `{proyecto}_cloudbuild` y lectura del estado) y el rol
   `ta-github-cd` (`ViewOnlyAccess` y lectura del estado).
2. En GitHub, Settings → Secrets and variables → Actions:
   - **Variables:** `TA_CD_ENABLED = true`, `TA_GCP_PROJECT`, `TA_GCP_WIF_PROVIDER` y `TA_GCP_CD_SA` (outputs
     `github_cd_*` de `infra/gcp`), `TA_AWS_CD_ROLE` (output `github_cd_role_arn` de `infra/aws`).
   - **Secretos:** `TA_GCP_TFVARS` y `TA_AWS_TFVARS`, con el contenido de `terraform.tfvars` y de los `*.auto.tfvars`
     de cada raíz (llevan el correo de las alertas; por eso no van al repositorio).
3. Correr «CD» a mano (Actions → CD → Run workflow) y revisar que `images` publique las tres imágenes y que los dos
   `plan` digan «No changes».

## Cierre

**Lote 7.1 cerrado (v2.55).** 29-oct-2026, WSL (`ta-streaming`), Docker 29.7, sobre `main` = `b3b5ac3`.

| Parte | Resultado |
|---|---|
| A · Preflight | OK (3) |
| B · Pruebas | OK (1): 9 |
| L · Local | OK (9): workflows, pre-commit, Terraform de las 4 raíces, gitleaks sin hallazgos nuevos (2 revisados a mano), tfvars, las tres imágenes (137, 96 y 11 pruebas dentro) |
| E · CI en un contenedor | OK (4): contratos 18, identidad 12, suite 1.644 passed y 4 omitidas, 8,7 min |
| C · Plan | OK (2): GCP y AWS sin cambios con el CD apagado |
| G · GitHub Actions | OK (2): **CI #3 `success`** con los 6 trabajos, 3,7 min; CD sin corrida (sus trabajos `skipped` en la entrega) |

Las corridas #1 y #2 fallaron en `pytest` por dos supuestos del entorno local (correr como root; el marcador del
validador): corregidos. Pendiente, decisión del usuario: activar el CD y revocar el token de Mastodon del historial.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| L5 o el trabajo `secretos` encuentran algo | Si es real: sacarlo del código, **revocarlo** donde se emitió y decidir si reescribir el historial. Si es falso: agregarlo a `.gitleaks.toml` (por ruta) o su huella a `.gitleaksignore` |
| El push de `.github/workflows/*` se rechaza («refusing to allow an OAuth App to create or update workflow») | La credencial de git no tiene el permiso `workflow`: volver a iniciar sesión en el administrador de credenciales de Git |
| G1 «no hay corrida» | Actions desactivado en el repositorio, o el push no llegó: revisar la pestaña Actions |
| E3 o `pytest` fallan solo en la suite completa | Contaminación entre pruebas (una prueba deja una variable de entorno puesta): correr el archivo solo y luego con sus vecinos, como se hizo con `test_flex_template.py` en este lote |
