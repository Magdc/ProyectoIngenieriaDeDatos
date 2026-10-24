"""Anonimización HMAC-SHA256 (ADR-06, ARCHITECTURE.md §5.2).

``author.user_hash`` = HMAC-SHA256(llave, user_id) en hex (64 caracteres). La llave vive en
Secret Manager (``ta-hmac-key``); este módulo NO la obtiene: la recibe como argumento.
"""
from __future__ import annotations

import hashlib
import hmac
import re
from typing import Union

USER_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def user_hash(user_id: str, secret: Union[str, bytes]) -> str:
    if not user_id:
        raise ValueError("user_id vacío")
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not key:
        raise ValueError("la llave HMAC no puede ser vacía")
    return hmac.new(key, user_id.encode("utf-8"), hashlib.sha256).hexdigest()


def is_user_hash(value: object) -> bool:
    return isinstance(value, str) and USER_HASH_RE.match(value) is not None
