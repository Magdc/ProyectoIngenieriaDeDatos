# Glue Workflow ta-wf-raw-to-curated y sus jobs (Lote 4.1, HU-029; ARCHITECTURE.md §3.19, ADR-23).
#
# Los jobs corren en el orden de var.jobs: el primero con un disparador ON_DEMAND (lo arranca
# StartWorkflowRun, que llama el horario de EventBridge Scheduler o una persona) y cada siguiente
# con un disparador CONDITIONAL que espera el SUCCEEDED del anterior. Los scripts se suben desde
# project/src/lakehouse/glue_jobs/ a s3://{lake}/_glue/scripts/{job}.py: reemplazar el archivo y
# aplicar cambia el job sin tocar Terraform (el 4.2 y el 4.3 solo reemplazan el script).
#
# Nada de esto cobra si no corre: Glue cobra por DPU-segundo mientras un job ejecuta.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "workflow_name" {
  description = "Nombre del workflow (§5.7)."
  type        = string
  default     = "ta-wf-raw-to-curated"
}

variable "description" {
  description = "Descripción del workflow."
  type        = string
  default     = ""
}

variable "role_arn" {
  description = "Rol de los jobs (ta-glue-role)."
  type        = string
}

variable "bucket_name" {
  description = "Bucket del lake (scripts y temporales)."
  type        = string
}

variable "kms_key_arn" {
  description = "Llave del lake (cifra los scripts subidos)."
  type        = string
}

variable "scripts_prefix" {
  description = "Prefijo de los scripts en el lake."
  type        = string
  default     = "_glue/scripts/"
}

variable "temp_prefix" {
  description = "Prefijo temporal de Glue (--TempDir)."
  type        = string
  default     = "_glue/temp/"
}

variable "jobs" {
  description = "Jobs en el orden en que corren. command: pythonshell (por defecto) o glueetl."
  type = list(object({
    key               = string
    name              = string
    description       = string
    script_path       = string
    command           = optional(string, "pythonshell")
    python_version    = optional(string, "3.9")
    max_capacity      = optional(number, 0.0625)
    glue_version      = optional(string, "5.0")
    worker_type       = optional(string, "G.1X")
    number_of_workers = optional(number, 2)
    timeout_minutes   = optional(number, 30)
    arguments         = optional(map(string), {})
  }))

  validation {
    condition     = length(var.jobs) >= 1 && alltrue([for j in var.jobs : contains(["pythonshell", "glueetl"], j.command)])
    error_message = "Hace falta al menos un job, y command debe ser pythonshell o glueetl."
  }

  validation {
    condition     = length(distinct([for j in var.jobs : j.key])) == length(var.jobs)
    error_message = "Las claves de los jobs deben ser únicas."
  }
}

variable "common_arguments" {
  description = "Argumentos que reciben todos los jobs (se suman a los propios)."
  type        = map(string)
  default     = {}
}

variable "max_concurrent_runs" {
  description = "Corridas del workflow a la vez."
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "Retención de los grupos de logs de Glue que crea el módulo."
  type        = number
  default     = 30
}

locals {
  jobs_by_key = { for j in var.jobs : j.key => j }
  first_key   = var.jobs[0].key
  next        = { for i, j in var.jobs : j.key => var.jobs[i - 1].key if i > 0 }

  # Glue escribe en grupos fijos según el tipo de job; el módulo los crea con retención.
  log_groups = toset(concat(
    anytrue([for j in var.jobs : j.command == "pythonshell"]) ? ["/aws-glue/python-jobs/output", "/aws-glue/python-jobs/error"] : [],
    anytrue([for j in var.jobs : j.command == "glueetl"]) ? ["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"] : [],
  ))
}

resource "aws_cloudwatch_log_group" "glue" {
  for_each = local.log_groups

  name              = each.value
  retention_in_days = var.log_retention_days
}

resource "aws_s3_object" "script" {
  for_each = local.jobs_by_key

  bucket                 = var.bucket_name
  key                    = "${var.scripts_prefix}${each.value.name}.py"
  source                 = each.value.script_path
  source_hash            = filemd5(each.value.script_path)
  content_type           = "text/x-python"
  server_side_encryption = "aws:kms"
  kms_key_id             = var.kms_key_arn
  bucket_key_enabled     = true
}

resource "aws_glue_job" "this" {
  for_each = local.jobs_by_key

  name              = each.value.name
  description       = each.value.description
  role_arn          = var.role_arn
  glue_version      = each.value.command == "glueetl" ? each.value.glue_version : null
  max_capacity      = each.value.command == "pythonshell" ? each.value.max_capacity : null
  worker_type       = each.value.command == "glueetl" ? each.value.worker_type : null
  number_of_workers = each.value.command == "glueetl" ? each.value.number_of_workers : null
  timeout           = each.value.timeout_minutes
  max_retries       = 0

  command {
    name            = each.value.command
    script_location = "s3://${var.bucket_name}/${aws_s3_object.script[each.key].key}"
    python_version  = each.value.command == "pythonshell" ? each.value.python_version : "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = merge(
    {
      "--job-language" = "python"
      "--TempDir"      = "s3://${var.bucket_name}/${var.temp_prefix}"
      "--ta_job"       = each.value.name
      "--ta_job_key"   = each.key
    },
    each.value.command == "pythonshell" ? { "library-set" = "analytics" } : { "--enable-continuous-cloudwatch-log" = "true" },
    var.common_arguments,
    each.value.arguments,
  )

  depends_on = [aws_cloudwatch_log_group.glue]
}

resource "aws_glue_workflow" "this" {
  name                = var.workflow_name
  description         = var.description
  max_concurrent_runs = var.max_concurrent_runs
}

# enabled = false: con un disparador ON_DEMAND, enabled = true hace que el proveedor llame
# StartTrigger al crearlo, y eso EJECUTA el workflow (pasó en el apply del 4.1: CloudTrail muestra
# StartTrigger 1 s después de CreateTrigger y la corrida wr_0ab6…). StartWorkflowRun no necesita
# que esté "activado" (un ON_DEMAND queda en CREATED igual). ignore_changes evita que el cambio de
# true a false en el recurso ya creado llame StopTrigger: el plan no muestra cambios.
resource "aws_glue_trigger" "start" {
  name          = "${var.workflow_name}-inicio"
  description   = "Arranca ${aws_glue_job.this[local.first_key].name} con StartWorkflowRun (Scheduler o a mano)"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.this.name
  enabled       = false

  actions {
    job_name = aws_glue_job.this[local.first_key].name
  }

  lifecycle {
    ignore_changes = [enabled]
  }
}

resource "aws_glue_trigger" "next" {
  for_each = local.next

  name          = "${var.workflow_name}-${replace(each.key, "_", "-")}"
  description   = "Corre ${aws_glue_job.this[each.key].name} si ${aws_glue_job.this[each.value].name} terminó en SUCCEEDED"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.this.name

  predicate {
    conditions {
      job_name = aws_glue_job.this[each.value].name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.this[each.key].name
  }
}

output "workflow_name" {
  description = "Nombre del workflow."
  value       = aws_glue_workflow.this.name
}

output "workflow_arn" {
  description = "ARN del workflow (lo usa el rol del Scheduler)."
  value       = aws_glue_workflow.this.arn
}

output "job_names" {
  description = "Clave → nombre del job."
  value       = { for k, j in aws_glue_job.this : k => j.name }
}

output "job_arns" {
  description = "Clave → ARN del job."
  value       = { for k, j in aws_glue_job.this : k => j.arn }
}

output "script_locations" {
  description = "Clave → s3:// del script."
  value       = { for k, j in aws_glue_job.this : k => j.command[0].script_location }
}

output "trigger_names" {
  description = "Disparadores del workflow (inicio y condicionales)."
  value       = concat([aws_glue_trigger.start.name], [for k, t in aws_glue_trigger.next : t.name])
}

output "log_group_names" {
  description = "Grupos de logs de Glue con retención."
  value       = sort([for g in aws_cloudwatch_log_group.glue : g.name])
}
