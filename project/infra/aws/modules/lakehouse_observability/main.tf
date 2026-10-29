# Observabilidad del lakehouse en AWS (Lote 7.2, HU-042; ARCHITECTURE.md §3.32, ADR-36).
#
# - Regla ta-glue-job-estados: todos los "Glue Job State Change" de ta-job-* → /aws/events/ta-glue-job-estados.
# - Filtro de métricas sobre /aws/events/ta-glue-job-fallos (Lote 4.1) → TrendAnalyzer/GlueJobFailures y la alarma
#   ta-glue-job-fallido (≥ 1 en 5 min) → SNS ta-observabilidad → correo (la suscripción hay que confirmarla en el
#   correo que manda AWS).
# - Tablero de CloudWatch ta-lakehouse: estado de cada job (Logs Insights), fallos, corridas del workflow y bytes de
#   Athena por workgroup.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

variable "region" {
  type = string
}

variable "job_name_prefix" {
  type    = string
  default = "ta-job-"
}

variable "failures_log_group" {
  description = "Grupo de logs de los fallos de Glue del Lote 4.1."
  type        = string
  default     = "/aws/events/ta-glue-job-fallos"
}

variable "athena_workgroup" {
  type    = string
  default = "ta-analytics"
}

variable "alarm_emails" {
  description = "Destinatarios de la alarma (cada uno confirma la suscripción). Vacío = solo el tema SNS."
  type        = list(string)
  default     = []
  sensitive   = true
}

variable "retention_days" {
  type    = number
  default = 30
}

locals {
  states_group = "/aws/events/ta-glue-job-estados"
}

resource "aws_cloudwatch_log_group" "states" {
  name              = local.states_group
  retention_in_days = var.retention_days
}

resource "aws_cloudwatch_event_rule" "states" {
  name        = "ta-glue-job-estados"
  description = "Todos los cambios de estado de los jobs ${var.job_name_prefix}* → ${local.states_group} (tablero ta-lakehouse)"
  state       = "ENABLED"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [{ prefix = var.job_name_prefix }]
    }
  })
}

resource "aws_cloudwatch_event_target" "states" {
  rule      = aws_cloudwatch_event_rule.states.name
  target_id = "cloudwatch-logs"
  arn       = aws_cloudwatch_log_group.states.arn
}

data "aws_iam_policy_document" "states" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.states.arn}:*"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "delivery.logs.amazonaws.com"]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "states" {
  policy_name     = "ta-glue-job-estados-events"
  policy_document = data.aws_iam_policy_document.states.json
}

resource "aws_cloudwatch_log_metric_filter" "failures" {
  name           = "ta-glue-job-fallos"
  log_group_name = var.failures_log_group
  pattern        = "{ $.source = \"aws.glue\" }"
  metric_transformation {
    name          = "GlueJobFailures"
    namespace     = "TrendAnalyzer"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_sns_topic" "ops" {
  name = "ta-observabilidad"
}

resource "aws_sns_topic_subscription" "email" {
  # Clave por posición: las direcciones no aparecen en las direcciones de recurso (como en el 6.2).
  for_each  = toset([for i in range(nonsensitive(length(var.alarm_emails))) : tostring(i)])
  topic_arn = aws_sns_topic.ops.arn
  protocol  = "email"
  endpoint  = var.alarm_emails[tonumber(each.key)]
}

resource "aws_cloudwatch_metric_alarm" "glue_failed" {
  alarm_name          = "ta-glue-job-fallido"
  alarm_description   = "Un job ta-job-* terminó en FAILED, TIMEOUT, STOPPED o ERROR (Lote 7.2). Ver /aws/events/ta-glue-job-fallos y /aws-glue/python-jobs/error; runbook: docs/RUNBOOK.md."
  namespace           = "TrendAnalyzer"
  metric_name         = "GlueJobFailures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.ops.arn]
  ok_actions          = [aws_sns_topic.ops.arn]
}

resource "aws_cloudwatch_dashboard" "lakehouse" {
  dashboard_name = "ta-lakehouse"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "log", x = 0, y = 0, width = 12, height = 6
        properties = {
          title  = "Estado de los jobs de Glue (últimas corridas)"
          region = var.region
          query  = "SOURCE '${local.states_group}' | fields @timestamp, detail.jobName as job, detail.state as estado, detail.jobRunId as corrida | sort @timestamp desc | limit 30"
          view   = "table"
        }
      },
      {
        type = "log", x = 12, y = 0, width = 12, height = 6
        properties = {
          title  = "Corridas por job y estado"
          region = var.region
          query  = "SOURCE '${local.states_group}' | stats count(*) as corridas by detail.jobName as job, detail.state as estado"
          view   = "table"
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 12, height = 6
        properties = {
          title       = "Fallos de jobs de Glue (alarma ta-glue-job-fallido)"
          region      = var.region
          stat        = "Sum"
          period      = 300
          metrics     = [["TrendAnalyzer", "GlueJobFailures"]]
          annotations = { horizontal = [{ value = 1, label = "alarma" }] }
        }
      },
      {
        type = "metric", x = 12, y = 6, width = 12, height = 6
        properties = {
          title   = "Athena: bytes escaneados por consulta (${var.athena_workgroup})"
          region  = var.region
          stat    = "Sum"
          period  = 3600
          metrics = [["AWS/Athena", "ProcessedBytes", "WorkGroup", var.athena_workgroup]]
        }
      }
    ]
  })
}

output "sns_topic_arn" {
  value = aws_sns_topic.ops.arn
}

output "alarm_name" {
  value = aws_cloudwatch_metric_alarm.glue_failed.alarm_name
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.lakehouse.dashboard_name
}

output "email_subscriptions" {
  value = length(aws_sns_topic_subscription.email)
}
