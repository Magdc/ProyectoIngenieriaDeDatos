"""Notificador de alertas (Lote 6.2, HU-039): ``ta-alerts`` (push) → log estructurado → correo.

Servicio de Cloud Run ``ta-alert-notifier``. Pub/Sub le entrega cada mensaje de ``ta-alerts`` con una
petición push autenticada con el ID token de ``sa-alerts`` (Cloud Run la verifica: el servicio no es
público). Por cada alerta válida escribe en stdout una línea JSON con ``severity = WARNING`` y
``alert = trend`` (``common.alert_message.log_record``); Cloud Logging la guarda como ``jsonPayload`` y la
política basada en logs ``ta-alerta-tendencia`` la convierte en un correo (§5.12). Sin SMTP ni secretos.

Respuestas:

* ``204``: alerta registrada, o mensaje inválido registrado con ``alert = invalid`` y severidad ``ERROR``
  (reintentarlo no lo arregla, así que se confirma).
* ``400``: el cuerpo no es un push de Pub/Sub (no se registra ninguna alerta).
* ``500``: error inesperado (Pub/Sub reintenta con espera de 10 a 300 s).

``GET /healthz`` responde ``ok`` (sonda de arranque). Solo biblioteca estándar.

    PORT=8080 python -m alerts.notifier
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from common.alert_message import decode, format_ts, log_record

MAX_BODY_BYTES = 1_000_000


def _emit(record: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def handle_push(body: bytes, now_ms: int, emit: Callable[[dict[str, Any]], None] = _emit) -> tuple[int, dict[str, Any] | None]:
    """Procesa el cuerpo de una petición push. Devuelve ``(código HTTP, registro escrito o None)``."""
    try:
        envelope = json.loads(body.decode("utf-8"))
        message = envelope["message"]
        if not isinstance(message, dict):
            raise TypeError("message no es un objeto")
    except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        record = {"severity": "WARNING", "alert": "bad_request", "message": f"la petición no es un push de Pub/Sub: {exc}"}
        emit(record)
        return 400, record
    message_id = message.get("messageId") or message.get("message_id")
    publish_time = message.get("publishTime") or message.get("publish_time")
    try:
        data = base64.b64decode(message.get("data") or "", validate=True)
        alert = decode(data)
    except (binascii.Error, ValueError) as exc:
        record = {"severity": "ERROR", "alert": "invalid", "message": f"mensaje de ta-alerts inválido: {exc}",
                  "message_id": message_id, "publish_time": publish_time,
                  "attributes": message.get("attributes") or {}, "received_at": format_ts(now_ms)}
        emit(record)
        return 204, record
    record = log_record(alert, received_ms=now_ms, publish_time=publish_time, message_id=message_id)
    emit(record)
    return 204, record


class Handler(BaseHTTPRequestHandler):
    server_version = "ta-alert-notifier/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # sin el log de acceso por defecto (Cloud Run ya lo tiene)
        return

    def _reply(self, code: int, text: str = "") -> None:
        payload = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?")[0] == "/healthz":
            self._reply(200, "ok")
        else:
            self._reply(404, "no encontrado")

    def do_POST(self) -> None:  # noqa: N802
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY_BYTES:
                self._reply(400, "cuerpo vacío o demasiado grande")
                return
            code, _record = handle_push(self.rfile.read(length), int(time.time() * 1000))
            self._reply(code)
        except Exception as exc:  # noqa: BLE001 - Pub/Sub reintenta
            _emit({"severity": "ERROR", "alert": "error", "message": f"error inesperado: {exc!r}"})
            self._reply(500, "error")


def main() -> None:  # pragma: no cover - lo prueba la imagen
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    _emit({"severity": "INFO", "event": "notifier_started", "port": port})
    server.serve_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
