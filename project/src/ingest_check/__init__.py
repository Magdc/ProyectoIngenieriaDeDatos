"""Verificación de la ingesta del simulador (Lote 1.4, HU-017).

Se suscribe a los topics ``ta-raw-{source}`` y mide, por fuente, la tasa de
mensajes, los ``event_id`` únicos y el ``seq`` actual. También comprueba que el
cursor del simulador sobrevive a un reinicio del servicio.

El núcleo (``stats``, ``cursor``, ``schema``) no depende de Google Cloud: las
librerías de Pub/Sub y GCS se importan solo al usarlas (``pubsub_io`` y
``cursor.GcsCursorReader``). Así el mismo código sirve contra el emulador de
Pub/Sub (Lote 1.5) y en las pruebas sin credenciales.

Descripción completa en ``ARCHITECTURE.md`` §3.11.
"""

__all__ = ["__version__"]

__version__ = "1.0"
