# Trend Analyzer Infrastructure

Infraestructura como codigo para el MVP academico de Trend Analyzer.

## Historias cubiertas por esta carpeta

- `US-02`: provisionamiento base de infraestructura GCP con Terraform.
- `US-03`: provisionamiento base de infraestructura AWS con Terraform.
- `US-04`: ingesta programada con Cloud Scheduler para fuentes near-real-time.
- `US-05`: manejo seguro de credenciales con Secret Manager.

No se implementan aqui los conectores de aplicacion, el job real de Dataflow, reglas de calidad ni perfilamiento de datos, porque corresponden a otras historias del sprint.

## Estructura

```text
infra/
  gcp/environments/dev/
  aws/environments/dev/
```

## Validacion local

```bash
cd infra/gcp/environments/dev
terraform init -backend=false
terraform fmt
terraform validate

cd ../../../../aws/environments/dev
terraform init -backend=false
terraform fmt
terraform validate
```

## Secretos

Terraform crea los contenedores de secretos, pero no guarda valores reales. Los valores se tienen que cargar fuera de Terraform cuando existan credenciales de AWS o de GCP.
