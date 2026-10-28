# Roles de acceso al tablero de QuickSight (Lote 6.1, HU-037; ARCHITECTURE.md §5.8 y §3.27).
#
# QuickSight está suscrito con autenticación IAM_AND_QUICKSIGHT: una identidad IAM que asume uno de estos roles
# entra a QuickSight como el usuario «<rol>/<sesión>», que scripts/quicksight.py deploy registra (sesión «demo») y
# pone en su grupo: ta-quicksight-autor → AUTHOR en ta-autores (edita el análisis y los datasets) y
# ta-quicksight-lector → READER en ta-lectores (solo ve el tablero). Los asume cualquier identidad de la cuenta a la
# que su propia política le permita sts:AssumeRole sobre ellos. La única política en línea es la que AWS documenta
# para que la sesión se registre sola la primera vez (CreateReader / CreateUser sobre su propio usuario): no da
# acceso a datos. Los datos los lee solo ta-quicksight-role, el rol de servicio de la fuente Athena.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "aws_account_id" {
  description = "Cuenta."
  type        = string
}

variable "roles" {
  description = "Rol IAM → acción de QuickSight con la que la sesión puede registrarse sola."
  type        = map(string)
  default = {
    "ta-quicksight-autor"  = "quicksight:CreateUser"
    "ta-quicksight-lector" = "quicksight:CreateReader"
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AccountPrincipals"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.aws_account_id}:root"]
    }
  }
}

resource "aws_iam_role" "this" {
  for_each = var.roles

  name                 = each.key
  description          = "Lote 6.1: acceso al tablero de QuickSight (${each.value == "quicksight:CreateReader" ? "lector" : "autor"})"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "self_register" {
  for_each = var.roles

  statement {
    sid       = "SelfRegister"
    effect    = "Allow"
    actions   = [each.value]
    resources = ["arn:aws:quicksight::${var.aws_account_id}:user/$${aws:userid}"]
  }
}

resource "aws_iam_role_policy" "this" {
  for_each = var.roles

  name   = "${each.key}-quicksight"
  role   = aws_iam_role.this[each.key].id
  policy = data.aws_iam_policy_document.self_register[each.key].json
}

output "role_arns" {
  description = "ARN de cada rol de acceso."
  value       = { for k, r in aws_iam_role.this : k => r.arn }
  depends_on  = [aws_iam_role_policy.this]
}

output "policy_names" {
  description = "Política en línea de cada rol."
  value       = { for k, p in aws_iam_role_policy.this : k => p.name }
}
