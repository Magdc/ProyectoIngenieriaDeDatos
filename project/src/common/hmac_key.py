"""Llave HMAC de anonimización (``ta-hmac-key``, §5.1, ADR-06).

Reglas:

* En la nube la llave **siempre** sale de Secret Manager y se lee **en el worker**
  (``DoFn.setup``) con la identidad del job (``sa-dataflow``). Nunca viaja como opción
  del pipeline ni queda en el grafo del job.
* Se lee por REST con ``google-auth`` (ya viene con ``apache-beam[gcp]``), sin añadir
  ``google-cloud-secret-manager`` a las dependencias.
* La llave es el **texto** del secreto (UTF-8, sin espacios al final). Cambiarla cambia
  todos los ``user_hash``: no se rota sin reprocesar (ver ``scripts/hmac_key.sh``).
* En local (emulador, datos sintéticos) se admite una llave de desarrollo fija y pública
  o la de una variable de entorno. Nunca con ``DataflowRunner``.
"""

from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

MIN_KEY_LENGTH = 16

#: Llave **pública** solo para el entorno local con datos sintéticos. No es un secreto.
LOCAL_DEV_KEY = "ta-local-dev-hmac-key--solo-datos-sinteticos--no-usar-en-la-nube"

SECRET_MANAGER_URL = "https://secretmanager.googleapis.com/v1/projects/{project}/secrets/{name}/versions/{version}:access"
_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


class HmacKeyError(RuntimeError):
    """No se pudo obtener una llave HMAC válida."""


@dataclass(frozen=True)
class KeySource:
    """De dónde sale la llave. ``kind`` ∈ ``secret`` | ``env`` | ``local_dev``."""

    kind: str
    project: str | None = None
    secret_name: str = "ta-hmac-key"
    version: str = "latest"
    env_var: str | None = None

    def describe(self) -> str:
        if self.kind == "secret":
            return f"secret projects/{self.project}/secrets/{self.secret_name}/versions/{self.version}"
        if self.kind == "env":
            return f"variable de entorno {self.env_var}"
        return "llave local de desarrollo (pública)"

    def __post_init__(self) -> None:
        if self.kind not in ("secret", "env", "local_dev"):
            raise ValueError(f"KeySource.kind inválido: {self.kind!r}")
        if self.kind == "secret" and not self.project:
            raise ValueError("KeySource 'secret' necesita project")
        if self.kind == "env" and not self.env_var:
            raise ValueError("KeySource 'env' necesita env_var")


def _check(key: str, origin: str) -> str:
    key = key.strip()
    if len(key) < MIN_KEY_LENGTH:
        raise HmacKeyError(f"la llave HMAC de {origin} tiene menos de {MIN_KEY_LENGTH} caracteres")
    return key


def _crc32c_ok(data: bytes, expected: Any) -> bool:
    if expected in (None, ""):
        return True
    try:
        import google_crc32c  # viene con google-cloud-storage
    except ImportError:  # pragma: no cover - depende del entorno
        return True
    return int(google_crc32c.value(data)) == int(expected)


def access_secret(
    project: str,
    name: str,
    version: str = "latest",
    *,
    session: Any | None = None,
    retries: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Lee la versión de un secreto por REST y devuelve sus bytes.

    ``session`` es un ``google.auth.transport.requests.AuthorizedSession`` (o un doble de
    prueba con ``get(url, timeout=…)``). Reintenta errores 429/5xx.
    """
    if session is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        creds, _ = google.auth.default(scopes=list(_SCOPES))
        session = AuthorizedSession(creds)
    url = SECRET_MANAGER_URL.format(project=project, name=name, version=version)
    last = ""
    for attempt in range(retries):
        resp = session.get(url, timeout=30)
        status = getattr(resp, "status_code", 0)
        if status == 200:
            body = resp.json()
            payload = (body or {}).get("payload") or {}
            data = base64.b64decode(payload.get("data") or b"")
            if not _crc32c_ok(data, payload.get("dataCrc32c")):
                raise HmacKeyError("el checksum CRC32C del secreto no coincide")
            return data
        last = f"HTTP {status}"
        if status in (429, 500, 502, 503, 504) and attempt + 1 < retries:
            sleep(2 ** attempt)
            continue
        break
    hint = ""
    if last == "HTTP 403":
        hint = " (¿falta roles/secretmanager.secretAccessor para la cuenta del job?)"
    elif last == "HTTP 404":
        hint = " (¿el secreto no tiene versiones? ver scripts/hmac_key.sh init)"
    raise HmacKeyError(f"no se pudo leer projects/{project}/secrets/{name}: {last}{hint}")


def load_hmac_key(source: KeySource, *, session: Any | None = None, environ: dict | None = None) -> str:
    """Devuelve la llave como texto según ``source``. No la escribe en ningún log."""
    env = os.environ if environ is None else environ
    if source.kind == "secret":
        data = access_secret(source.project or "", source.secret_name, source.version, session=session)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HmacKeyError("el secreto no es texto UTF-8") from exc
        return _check(text, source.describe())
    if source.kind == "env":
        value = env.get(source.env_var or "")
        if not value:
            raise HmacKeyError(f"la variable {source.env_var} está vacía o no existe")
        return _check(value, source.describe())
    return LOCAL_DEV_KEY
