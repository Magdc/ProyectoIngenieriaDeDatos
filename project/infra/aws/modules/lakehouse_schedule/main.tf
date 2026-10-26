# Horario del lakehouse (Lote 4.1, HU-029; ARCHITECTURE.md §3.19, ADR-23): EventBridge Scheduler
# rate(1 hour) → glue:StartWorkflowRun de ta-wf-raw-to-curated (destino universal del SDK).
#
# Se crea DESHABILITADO (estado de ahorro, como el Scheduler del 3.2 en ADR-22) y Terraform
# ignora el estado (ignore_changes): validate_4_1.py on/off lo cambia con update-schedule sin dejar
# deriva. Sin reintentos: si una corrida sigue en curso (máximo 1 a la vez), la de esa hora se
# pierde y la siguiente corre igual.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "schedule_name" {
  description = "Nombre del horario (grupo default)."
  type        = string
  default     = "ta-wf-raw-to-curated-cada-hora"
}

variable "role_name" {
  description = "Rol que asume EventBridge Scheduler para arrancar el workflow."
  type        = string
  default     = "ta-lakehouse-scheduler-role"
}

variable "aws_account_id" {
  description = "Cuenta (condición aws:SourceAccount de la confianza)."
  type        = string
}

variable "workflow_name" {
  description = "Workflow a arrancar."
  type        = string
}

variable "workflow_arn" {
  description = "ARN del workflow (único recurso que el rol puede arrancar)."
  type        = string
}

variable "schedule_expression" {
  description = "Expresión del horario (§5.7: rate(1 hour))."
  type        = string
  default     = "rate(1 hour)"
}

variable "schedule_timezone" {
  description = "Zona horaria del horario."
  type        = string
  default     = "UTC"
}

variable "initial_state" {
  description = "Estado al crearlo. DISABLED = ahorro; luego lo cambian validate_4_1.py on/off."
  type        = string
  default     = "DISABLED"

  validation {
    condition     = contains(["ENABLED", "DISABLED"], var.initial_state)
    error_message = "initial_state debe ser ENABLED o DISABLED."
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "EventBridgeScheduler"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.aws_account_id]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  description          = "Lote 4.1: EventBridge Scheduler arranca ${var.workflow_name}"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "start" {
  statement {
    sid       = "StartWorkflow"
    effect    = "Allow"
    actions   = ["glue:StartWorkflowRun"]
    resources = [var.workflow_arn]
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.role_name}-glue"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.start.json
}

resource "aws_scheduler_schedule" "this" {
  name        = var.schedule_name
  group_name  = "default"
  description = "TrendAnalyzer: arranca ${var.workflow_name} (${var.schedule_expression}); creado deshabilitado"

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = var.initial_state

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun"
    role_arn = aws_iam_role.this.arn
    input    = jsonencode({ Name = var.workflow_name })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 0
    }
  }

  lifecycle {
    ignore_changes = [state]
  }

  depends_on = [aws_iam_role_policy.this]
}

output "schedule_name" {
  description = "Nombre del horario."
  value       = aws_scheduler_schedule.this.name
}

output "schedule_arn" {
  description = "ARN del horario."
  value       = aws_scheduler_schedule.this.arn
}

output "role_arn" {
  description = "Rol del Scheduler (también lo usa el horario de prueba de validate_4_1.py)."
  value       = aws_iam_role.this.arn
}

output "target_arn" {
  description = "Destino universal que llama el horario."
  value       = "arn:aws:scheduler:::aws-sdk:glue:startWorkflowRun"
  depends_on  = [aws_scheduler_schedule.this]
}
