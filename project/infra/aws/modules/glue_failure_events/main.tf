# Fallos de los jobs de Glue en CloudWatch Logs (Lote 4.1, HU-029; ARCHITECTURE.md §3.19).
#
# Una regla de EventBridge (bus default) toma los eventos "Glue Job State Change" de los jobs
# ta-job-* que terminan en FAILED, TIMEOUT, STOPPED o ERROR y los guarda, tal cual, en el grupo
# /aws/events/ta-glue-job-fallos. Así hay un solo lugar donde ver los fallos (y del que el Lote 7.2
# puede colgar una alarma), además del traceback que el job deja en /aws-glue/python-jobs/error.
# Los eventos de servicios de AWS en el bus default no cobran; CloudWatch Logs, centavos.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "rule_name" {
  description = "Nombre de la regla de EventBridge."
  type        = string
  default     = "ta-glue-job-fallos"
}

variable "log_group_name" {
  description = "Grupo de logs de destino (debe empezar por /aws/events/)."
  type        = string
  default     = "/aws/events/ta-glue-job-fallos"

  validation {
    condition     = startswith(var.log_group_name, "/aws/events/")
    error_message = "EventBridge solo escribe en grupos /aws/events/…"
  }
}

variable "job_name_prefix" {
  description = "Prefijo de los jobs vigilados."
  type        = string
  default     = "ta-job-"
}

variable "states" {
  description = "Estados de un job run que cuentan como fallo."
  type        = list(string)
  default     = ["FAILED", "TIMEOUT", "STOPPED", "ERROR"]
}

variable "retention_days" {
  description = "Retención del grupo de logs."
  type        = number
  default     = 30
}

resource "aws_cloudwatch_log_group" "this" {
  name              = var.log_group_name
  retention_in_days = var.retention_days
}

resource "aws_cloudwatch_event_rule" "this" {
  name        = var.rule_name
  description = "Jobs ${var.job_name_prefix}* que terminan en ${join("/", var.states)} → ${var.log_group_name}"
  state       = "ENABLED"

  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [{ prefix = var.job_name_prefix }]
      state   = var.states
    }
  })
}

resource "aws_cloudwatch_event_target" "this" {
  rule      = aws_cloudwatch_event_rule.this.name
  target_id = "cloudwatch-logs"
  arn       = aws_cloudwatch_log_group.this.arn
}

# EventBridge necesita una política de recursos de CloudWatch Logs para escribir en el grupo
# (la consola la crea sola; con la API hay que declararla).
data "aws_iam_policy_document" "logs" {
  statement {
    sid       = "EventBridgeCreateStream"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "delivery.logs.amazonaws.com"]
    }
  }

  statement {
    sid       = "EventBridgePutEvents"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*:*"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "delivery.logs.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.this.arn]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "this" {
  policy_name     = "${var.rule_name}-events"
  policy_document = data.aws_iam_policy_document.logs.json
}

output "rule_name" {
  description = "Regla de EventBridge."
  value       = aws_cloudwatch_event_rule.this.name
}

output "rule_arn" {
  description = "ARN de la regla."
  value       = aws_cloudwatch_event_rule.this.arn
}

output "log_group_name" {
  description = "Grupo de logs con los eventos de fallo."
  value       = aws_cloudwatch_log_group.this.name
  depends_on  = [aws_cloudwatch_event_target.this, aws_cloudwatch_log_resource_policy.this]
}
