# Lakehouse de AWS (Lote 4.1, HU-007 y HU-029; ARCHITECTURE.md §3.19, ADR-23).
#
# Se suma a main.tf / variables.tf (Lote 1.0), lake.tf (Lote 3.1) y exporter.tf (Lote 3.2) sin
# modificarlos: usa sus variables (aws_account_id, aws_region, env) y los módulos lake_bucket y
# lake_kms. Crea el catálogo de Glue (ta_raw, ta_trusted, ta_curated con partition projection),
# el workgroup de Athena ta-analytics, los roles ta-glue-role y ta-quicksight-role, el workflow
# ta-wf-raw-to-curated con sus dos jobs (marcadores de posición hasta los lotes 4.2 y 4.3), el
# horario de EventBridge Scheduler (DESHABILITADO: estado de ahorro) y la regla que guarda los
# fallos de los jobs en CloudWatch Logs. Lote 4.2 (§3.21): ta-job-raw-to-trusted es el job real y
# corre con 1 DPU. Lote 4.3 (§3.22): ta-job-trusted-to-curated es el job real (0,0625 DPU) y hay
# cinco consultas guardadas sobre Curated (HU-030).

locals {
  lakehouse_databases = {
    raw     = "ta_raw"
    trusted = "ta_trusted"
    curated = "ta_curated"
  }
  lakehouse_workflow      = "ta-wf-raw-to-curated"
  lakehouse_src           = "${path.module}/../../../../src/lakehouse"
  lakehouse_results_path  = "athena-results/"
  lakehouse_layer_folders = ["trusted/", "curated/", "quality/"]

  # Jobs en orden de ejecución. Los ajustes de var.glue_job_settings reemplazan los valores de aquí
  # y los por defecto del módulo (pythonshell 3.9, 0.0625 DPU, 30 min); los nulos no cuentan.
  lakehouse_job_base = [
    {
      key         = "raw_to_trusted"
      name        = "ta-job-raw-to-trusted"
      description = "Raw → Trusted (HU-026, Lote 4.2): unión, deduplicación por event_id, tipos y text_clean."
      script_path = "${local.lakehouse_src}/glue_jobs/raw_to_trusted.py"
      # Lote 4.2 (§3.21, ADR-25): 1 DPU (16 GB). Pico medido en local con Python 3.9 y pyarrow 7:
      # ~0,6 GB con el Raw de hoy; 1 GB (0,0625 DPU) no deja margen. glue_job_settings lo cambia.
      max_capacity = 1
      arguments = {
        "--ta_input_prefix"  = "raw/"
        "--ta_output_prefix" = "trusted/"
      }
    },
    {
      key         = "trusted_to_curated"
      name        = "ta-job-trusted-to-curated"
      description = "Trusted → Curated (HU-027, Lote 4.3): fact_posts, dimensiones y agregados."
      script_path = "${local.lakehouse_src}/glue_jobs/trusted_to_curated.py"
      # Lote 4.3 (§3.22, ADR-26): la capacidad por defecto del módulo, 0,0625 DPU (1 GB). Pico medido
      # en local con Python 3.9 y pyarrow 7: ~0,43 GB con el Trusted de hoy. glue_job_settings la cambia.
      arguments = {
        "--ta_input_prefix"  = "trusted/"
        "--ta_output_prefix" = "curated/"
      }
    },
  ]
  lakehouse_job_extra_args = {
    for j in local.lakehouse_job_base : j.key => (
      try(var.glue_job_settings[j.key].arguments, null) != null ? var.glue_job_settings[j.key].arguments : {}
    )
  }
  lakehouse_jobs = [
    for j in local.lakehouse_job_base : merge(
      j,
      { for k, v in try(var.glue_job_settings[j.key], {}) : k => v if v != null && k != "arguments" },
      { arguments = merge(j.arguments, local.lakehouse_job_extra_args[j.key]) },
    )
  ]

  lakehouse_named_queries = {
    "ta-raw-conteo-por-origen" = {
      database    = "ta_raw"
      description = "Filas de ta_raw.posts_raw por origen y fuente (demostración del Lote 4.1)"
      query       = replace(file("${local.lakehouse_src}/queries/raw_conteo_por_origen.sql"), "\r\n", "\n")
    }
    "ta-raw-sentimiento-7-dias" = {
      database    = "ta_raw"
      description = "Sentimiento por marca y origen en los últimos 7 días (poda de particiones)"
      query       = replace(file("${local.lakehouse_src}/queries/raw_sentimiento_7_dias.sql"), "\r\n", "\n")
    }
    # Lote 4.3 (HU-030, §3.22.1): consultas de ejemplo sobre Curated.
    "ta-curated-posts-por-origen" = {
      database    = "ta_curated"
      description = "Filas de fact_posts por origen y fuente (Listo cuando de la Fase 4)"
      query       = replace(file("${local.lakehouse_src}/queries/curated_posts_por_origen.sql"), "\r\n", "\n")
    }
    "ta-curated-tendencia-diaria" = {
      database    = "ta_curated"
      description = "Volumen y sentimiento diario por marca, últimos 30 días con datos"
      query       = replace(file("${local.lakehouse_src}/queries/curated_tendencia_diaria.sql"), "\r\n", "\n")
    }
    "ta-curated-emociones-por-hora-bogota" = {
      database    = "ta_curated"
      description = "Emociones por marca y hora del día en America/Bogota, últimos 7 días con datos"
      query       = replace(file("${local.lakehouse_src}/queries/curated_emociones_por_hora_bogota.sql"), "\r\n", "\n")
    }
    "ta-curated-semana-vs-fin-de-semana" = {
      database    = "ta_curated"
      description = "Volumen y sentimiento por marca entre semana y en fin de semana (poda por dt)"
      query       = replace(file("${local.lakehouse_src}/queries/curated_semana_vs_fin_de_semana.sql"), "\r\n", "\n")
    }
    "ta-curated-precision-nlp" = {
      database    = "ta_curated"
      description = "Matriz de confusión del NLP frente a label_sentiment (HU-033)"
      query       = replace(file("${local.lakehouse_src}/queries/curated_precision_nlp.sql"), "\r\n", "\n")
    }
  }
}

module "glue_catalog" {
  source = "../../modules/glue_catalog"

  bucket_name = module.lake_bucket.bucket
  databases   = local.lakehouse_databases
}

# "Carpetas" de las capas que todavía no tienen datos (HU-007). raw/ ya existe (3.1 y 3.2) y
# athena-results/ aparece con la primera consulta (y expira a los 7 días, §5.12).
resource "aws_s3_object" "lake_layer" {
  for_each = toset(local.lakehouse_layer_folders)

  bucket                 = module.lake_bucket.bucket
  key                    = each.value
  content                = ""
  content_type           = "application/x-directory"
  server_side_encryption = "aws:kms"
  kms_key_id             = module.lake_kms.key_arn
  bucket_key_enabled     = true
}

module "athena" {
  source = "../../modules/athena_workgroup"

  name                           = "ta-analytics"
  bucket_name                    = module.lake_bucket.bucket
  results_prefix                 = local.lakehouse_results_path
  kms_key_arn                    = module.lake_kms.key_arn
  bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff_per_query
  named_queries                  = local.lakehouse_named_queries
}

module "glue_role" {
  source = "../../modules/glue_role"

  role_name      = "ta-glue-role"
  bucket_name    = module.lake_bucket.bucket
  kms_key_arn    = module.lake_kms.key_arn
  aws_region     = var.aws_region
  aws_account_id = var.aws_account_id
  database_names = values(local.lakehouse_databases)
  workflow_name  = local.lakehouse_workflow
}

module "quicksight_role" {
  source = "../../modules/quicksight_role"

  role_name      = "ta-quicksight-role"
  bucket_name    = module.lake_bucket.bucket
  kms_key_arn    = module.lake_kms.key_arn
  aws_region     = var.aws_region
  aws_account_id = var.aws_account_id
  workgroup_name = module.athena.name
  results_prefix = local.lakehouse_results_path
  database_names = [local.lakehouse_databases.curated]
}

module "glue_workflow" {
  source = "../../modules/glue_workflow"

  workflow_name      = local.lakehouse_workflow
  description        = "Raw → Trusted → Curated cada hora (HU-029). Lo arranca ${var.lakehouse_schedule_name} (deshabilitado por defecto)."
  role_arn           = module.glue_role.role_arn
  bucket_name        = module.lake_bucket.bucket
  kms_key_arn        = module.lake_kms.key_arn
  jobs               = local.lakehouse_jobs
  log_retention_days = var.lakehouse_log_retention_days

  common_arguments = {
    "--lake_bucket"   = module.lake_bucket.bucket
    "--ta_region"     = var.aws_region
    "--ta_env"        = var.env
    "--ta_db_raw"     = local.lakehouse_databases.raw
    "--ta_db_trusted" = local.lakehouse_databases.trusted
    "--ta_db_curated" = local.lakehouse_databases.curated
  }
}

module "lakehouse_schedule" {
  source = "../../modules/lakehouse_schedule"

  schedule_name       = var.lakehouse_schedule_name
  role_name           = "ta-lakehouse-scheduler-role"
  aws_account_id      = var.aws_account_id
  workflow_name       = module.glue_workflow.workflow_name
  workflow_arn        = module.glue_workflow.workflow_arn
  schedule_expression = var.lakehouse_schedule_expression
  initial_state       = "DISABLED"
}

module "glue_failure_events" {
  source = "../../modules/glue_failure_events"

  rule_name       = "ta-glue-job-fallos"
  log_group_name  = "/aws/events/ta-glue-job-fallos"
  job_name_prefix = "ta-job-"
  retention_days  = var.lakehouse_log_retention_days
}
