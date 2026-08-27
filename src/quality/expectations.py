"""
src/quality/expectations.py

Definicion de las reglas de calidad del esquema comun de ingesta.
=================================================================
Cada regla es una funcion pura que recibe un registro (dict) y
retorna (bool, str | None) — (es_valido, mensaje_de_error).

Las funciones son independientes de Great Expectations para que
puedan usarse en pruebas unitarias sin instalar GE, y tambien
pueden envolverse en expectativas GE si se usa el framework completo.

Reglas definidas (segun US-13 del TODO.md):
  R1  id        — no nulo, no vacio
  R2  source    — pertenece a {"mastodon", "reddit", "news"}
  R3  created_at — ISO8601 valido, en rango [2020, futuro]
  R4  text      — no nulo, longitud > 0
  R5  url       — formato URL valido
  R6  engagement — campos enteros >= 0 cuando no son nulos
  R7  privacidad — author_name / author_id no exponen PII directamente
                   (verificacion de presencia de anonimizacion)
"""

import re
from datetime import datetime, timezone
from typing import Optional

VALID_SOURCES = {"mastodon", "reddit", "news"}
_DATE_MIN = datetime(2020, 1, 1, tzinfo=timezone.utc)
# Regex minimo para URL (esquema + host)
_URL_RE = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE)
# Regex ISO8601 basico (acepta Z y +HH:MM)
_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)


# ---------------------------------------------------------------------------
# Reglas individuales
# ---------------------------------------------------------------------------

def rule_id(record: dict) -> tuple[bool, Optional[str]]:
    """R1: id no nulo y no vacio."""
    v = record.get("id")
    if v is None or str(v).strip() == "":
        return False, "R1: 'id' es nulo o vacio"
    return True, None


def rule_source(record: dict) -> tuple[bool, Optional[str]]:
    """R2: source pertenece al conjunto de fuentes validas."""
    v = record.get("source")
    if v not in VALID_SOURCES:
        return False, f"R2: 'source' invalido — valor: {v!r}"
    return True, None


def rule_created_at(record: dict) -> tuple[bool, Optional[str]]:
    """R3: created_at es ISO8601 valido, >= 2020-01-01 y no futuro."""
    v = record.get("created_at")
    if not v:
        return False, "R3: 'created_at' es nulo o vacio"
    if not _ISO8601_RE.match(str(v)):
        return False, f"R3: 'created_at' no es ISO8601 valido — valor: {v!r}"
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if dt_utc < _DATE_MIN:
            return False, f"R3: 'created_at' anterior a 2020 — valor: {v!r}"
        if dt_utc > now_utc:
            return False, f"R3: 'created_at' en el futuro — valor: {v!r}"
    except (ValueError, OSError):
        return False, f"R3: 'created_at' no parseable — valor: {v!r}"
    return True, None


def rule_text(record: dict) -> tuple[bool, Optional[str]]:
    """R4: text no nulo y con al menos 1 caracter."""
    v = record.get("text")
    if not v or str(v).strip() == "":
        return False, "R4: 'text' es nulo o vacio"
    return True, None


def rule_url(record: dict) -> tuple[bool, Optional[str]]:
    """R5: url tiene formato valido (http/https + host)."""
    v = record.get("url")
    if not v:
        return False, "R5: 'url' es nulo o vacio"
    if not _URL_RE.match(str(v)):
        return False, f"R5: 'url' no tiene formato valido — valor: {v!r}"
    return True, None


def rule_engagement(record: dict) -> tuple[bool, Optional[str]]:
    """R6: engagement.replies/reposts/likes son enteros >= 0 cuando no son nulos."""
    eng = record.get("engagement") or {}
    errors = []
    for field in ("replies", "reposts", "likes"):
        val = eng.get(field)
        if val is None:
            continue   # null permitido (ej. RSS no tiene engagement)
        if not isinstance(val, (int, float)):
            errors.append(f"engagement.{field} no es numerico: {val!r}")
        elif val < 0:
            errors.append(f"engagement.{field} es negativo: {val!r}")
    if errors:
        return False, "R6: " + " | ".join(errors)
    return True, None


def rule_privacy(record: dict) -> tuple[bool, Optional[str]]:
    """R7: los campos de autor no deben contener emails o numeros de telefono
    en texto plano (Ley 1581 / GDPR — verificacion basica pre-analitica).
    """
    _email_re = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    _phone_re = re.compile(r"\b(\+57|57)?[0-9]{10}\b")
    for field in ("author_name", "author_id"):
        val = str(record.get(field) or "")
        if _email_re.search(val):
            return False, f"R7: '{field}' parece contener un email — PII detectada"
        if _phone_re.search(val):
            return False, f"R7: '{field}' parece contener un telefono — PII detectada"
    return True, None


# ---------------------------------------------------------------------------
# Conjunto completo de reglas
# ---------------------------------------------------------------------------
ALL_RULES = [
    rule_id,
    rule_source,
    rule_created_at,
    rule_text,
    rule_url,
    rule_engagement,
    rule_privacy,
]
