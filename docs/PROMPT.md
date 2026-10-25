Precondiciones de ejecución (obligatorio):
- No asumas que ningún recurso está encendido o desplegado. El estado por defecto es "apagado para ahorrar"
  (simulator_running = false, sin Dataflow corriendo).
- Antes de cualquier comando de validación, entrégame una sección "Preflight" con:
  1. La lista de servicios/recursos que deben estar activos para validar este lote, y cuáles NO hacen falta.
  2. Los comandos para comprobar el estado de cada uno, con la salida esperada.
  3. Los comandos para encenderlos si están apagados (con el plan esperado: qué se crea y qué no se toca).
  4. El tiempo de espera hasta que estén listos y cómo saberlo.
- Después de la validación, incluye una sección "Apagado" con los comandos para volver al estado de ahorro
  y cómo confirmar que no queda nada cobrando (incluidas suscripciones o recursos temporales).
- Si un comando de validación falla, primero descarta que sea un recurso apagado antes de cambiar código.
Estado actual (salida de preflight.sh): <pegar aquí>