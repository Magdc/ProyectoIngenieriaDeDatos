# Variables del Lote 1.3. Todas tienen valor por defecto: terraform.tfvars solo necesita
# simulator_image para desplegar el simulador (ver infra/DEPLOY_SIMULATOR.md).

variable "artifact_repository" {
  description = "Repositorio Docker de Artifact Registry."
  type        = string
  default     = "ta-images"
}

variable "simulator_image" {
  description = "Imagen del simulador (p. ej. us-central1-docker.pkg.dev/<proyecto>/ta-images/stream-simulator:<tag>). Vacía = no se despliega."
  type        = string
  default     = ""
}

variable "simulator_running" {
  description = "false borra el servicio Cloud Run para no pagar la instancia siempre encendida; el cursor se conserva."
  type        = bool
  default     = true
}

variable "sim_seed" {
  description = "SIM_SEED (§5.1). Cambiarla reinicia el cursor en seq = 0."
  type        = number
  default     = 7
}

variable "sim_chaos_enabled" {
  description = "SIM_CHAOS_ENABLED (§3.9)."
  type        = bool
  default     = false
}

variable "simulator_cpu" {
  type    = string
  default = "1"
}

variable "simulator_memory" {
  type    = string
  default = "512Mi"
}
