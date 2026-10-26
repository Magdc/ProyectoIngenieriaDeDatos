# Variables del Lote 4.1 (lakehouse.tf). Todas con valor por defecto: terraform.tfvars no cambia
# y los planes de otros lotes tampoco.

variable "athena_bytes_scanned_cutoff_per_query" {
  description = "Límite de bytes escaneados por consulta en ta-analytics (§5.12). 1 GiB ≈ 0,005 USD."
  type        = number
  default     = 1073741824
}

variable "lakehouse_schedule_name" {
  description = "Horario de EventBridge Scheduler que arranca ta-wf-raw-to-curated."
  type        = string
  default     = "ta-wf-raw-to-curated-cada-hora"
}

variable "lakehouse_schedule_expression" {
  description = "Expresión del horario (§5.7: rate(1 hour)). Se crea deshabilitado."
  type        = string
  default     = "rate(1 hour)"
}

variable "lakehouse_log_retention_days" {
  description = "Retención de los grupos de logs de Glue y de /aws/events/ta-glue-job-fallos."
  type        = number
  default     = 30
}

variable "glue_job_settings" {
  description = <<-EOT
    Ajustes por job (claves raw_to_trusted y trusted_to_curated); lo omitido usa el valor por
    defecto (pythonshell, Python 3.9, 0.0625 DPU, 30 min). Para Spark: command = "glueetl" con
    glue_version, worker_type y number_of_workers. arguments se suma a los del job.
  EOT
  type = map(object({
    command           = optional(string)
    python_version    = optional(string)
    max_capacity      = optional(number)
    glue_version      = optional(string)
    worker_type       = optional(string)
    number_of_workers = optional(number)
    timeout_minutes   = optional(number)
    arguments         = optional(map(string))
  }))
  default = {}
}
