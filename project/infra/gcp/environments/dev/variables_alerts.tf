# Variables del Lote 6.2 (alertas de tendencia). Todas tienen valor por defecto; la imagen la escribe
# validate_6_2.py en alerts.auto.tfvars y los correos van en terraform.tfvars (ninguno se versiona).

variable "alert_emails" {
  description = "Destinatarios del correo de las alertas (§5.12). Vacío = solo el canal de Pub/Sub."
  type        = list(string)
  default     = []
  sensitive   = true
}

variable "alerts_notifier_image" {
  description = "Imagen del notificador (…/ta-images/alert-notifier:{etiqueta}). Vacía = sin servicio ni push."
  type        = string
  default     = ""
}

variable "alert_rate_limit_seconds" {
  description = "Límite de notificaciones por incidente de la política (mínimo y valor de §5.12: 300); cada alerta es un incidente."
  type        = number
  default     = 300
}

variable "alert_auto_close_seconds" {
  description = "Cierre automático de los incidentes de la política (mínimo 1800)."
  type        = number
  default     = 1800
}
