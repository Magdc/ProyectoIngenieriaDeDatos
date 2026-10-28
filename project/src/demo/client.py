"""Cliente de ``/control/scenario`` del simulador (Lote 6.3, §3.29.1). Solo biblioteca estándar.

En la nube el servicio ``ta-stream-simulator`` no es público: cada petición lleva el ID token de quien
la hace (``gcloud auth print-identity-token``; hace falta ``run.routes.invoke``, que tiene el dueño del
proyecto). En local (``--url http://localhost:8080``) va sin token.
"""

from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Optional

SERVICE = "ta-stream-simulator"
REGION = "us-central1"
PATH = "/control/scenario"
#: Escenario por defecto del guion de demo (docs/DEMO.md): una ola negativa de Postobón en Instagram.
DEFAULTS: dict[str, Any] = {"brand": "postobon", "sentiment": "negative", "multiplier": 3, "duration_s": 300,
                            "source": "instagram"}


class ClientError(RuntimeError):
    """No se pudo hablar con el simulador (red, gcloud o respuesta que no es JSON)."""


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


def call(url: str, method: str, path: str = PATH, body: Optional[dict] = None, token: Optional[str] = None,
         timeout: float = 30) -> Reply:
    """Una petición HTTP; los 4xx/5xx vuelven como ``Reply`` (no como excepción)."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url.rstrip("/") + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - URL del simulador
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    except (urllib.error.URLError, OSError) as exc:
        raise ClientError(f"{method} {url}{path}: {exc}") from exc
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw.strip() else {}
    except (ValueError, UnicodeDecodeError):
        # El frontend de Cloud Run responde 401/403 en HTML, antes de llegar al contenedor.
        parsed = {"error": raw.decode("utf-8", "replace")[:200].strip()}
    return Reply(status, parsed if isinstance(parsed, dict) else {"body": parsed})


Gcloud = Callable[[list[str]], str]


def gcloud(args: list[str]) -> str:  # pragma: no cover - red
    proc = subprocess.run(["gcloud", *args], capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL)
    if proc.returncode != 0:
        raise ClientError(f"gcloud {' '.join(args[:3])}…: {(proc.stderr or proc.stdout).strip()[-400:]}")
    return proc.stdout.strip()


def service_url(project: str, region: str = REGION, run: Gcloud = gcloud) -> str:
    url = run(["run", "services", "describe", SERVICE, f"--region={region}", f"--project={project}",
               "--format=value(status.url)"])
    if not url.startswith("https://"):
        raise ClientError(f"{SERVICE} no está desplegado en {project}/{region} (¿simulador apagado? python project/scripts/on.py)")
    return url


def identity_token(run: Gcloud = gcloud) -> str:
    token = run(["auth", "print-identity-token"])
    if not token:
        raise ClientError("gcloud auth print-identity-token no devolvió un token (gcloud auth login)")
    return token


def scenario_body(brand: str, sentiment: str, multiplier: float, duration_s: int, source: Optional[str] = None,
                  name: Optional[str] = None) -> dict[str, Any]:
    body: dict[str, Any] = {"brand": brand, "sentiment": sentiment, "multiplier": multiplier, "duration_s": duration_s}
    if source:
        body["source"] = source
    if name:
        body["name"] = name
    return body
