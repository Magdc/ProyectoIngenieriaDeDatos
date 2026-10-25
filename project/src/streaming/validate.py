"""Validación de cada mensaje de `ta-raw-{source}-df` (HU-018).

`check_message()` es pura (sin Beam) y decide, en este orden:

1. ``encoding``   – el cuerpo no es UTF-8.
2. ``json``       – no es JSON o no es un objeto.
3. ``schema``     – no cumple ``envelope.v1.schema.json`` (incluye el payload).
4. ``attributes`` – los atributos no cumplen ``pubsub.attributes.schema.json``
   o no coinciden con el cuerpo (``event_id``, ``source``, ``origin``,
   ``schema_version``; §5.3).
5. ``routing``    – el evento es de otra fuente que la de la suscripción.

Lo que pasa los cinco controles sigue como `ValidEvent`; lo demás va a la DLQ.
"""

from __future__ import annotations

import json
import time
from typing import Any, Mapping

from streaming.records import (
    ERROR_ATTRIBUTES,
    ERROR_DETAIL_MAX,
    ERROR_ENCODING,
    ERROR_JSON,
    ERROR_ROUTING,
    ERROR_SCHEMA,
    DlqRecord,
    ValidEvent,
    format_ts,
)
from streaming.schemas import ATTRIBUTES_SCHEMA, ENVELOPE_SCHEMA, build_validator, describe_error

# Atributos de §5.3 que deben coincidir con el cuerpo.
COHERENT_ATTRIBUTES = ("event_id", "source", "origin", "schema_version")


def _dlq(source: str, error_type: str, detail: str, data: bytes, attributes: Mapping[str, str],
         now: float) -> DlqRecord:
    if len(detail) > ERROR_DETAIL_MAX:
        detail = detail[: ERROR_DETAIL_MAX - 1] + "…"
    return DlqRecord(
        received_at=format_ts(now),
        source=source,
        error_type=error_type,
        error_detail=detail,
        raw_message=data.decode("utf-8", errors="backslashreplace"),
        process_ts=now,
        data=bytes(data),
        attributes=dict(attributes or {}),
    )


def _one_line(text: str, obj: Any) -> str:
    if "\n" in text or "\r" in text:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return text


def check_message(source: str, data: bytes, attributes: Mapping[str, str] | None,
                  envelope_validator, attributes_validator=None,
                  now: float | None = None) -> ValidEvent | DlqRecord:
    """Valida un mensaje leído de la suscripción de `source`."""
    now = time.time() if now is None else now
    attributes = dict(attributes or {})
    data = bytes(data or b"")

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        return _dlq(source, ERROR_ENCODING, f"cuerpo no UTF-8: {exc}", data, attributes, now)

    try:
        obj = json.loads(text)
    except ValueError as exc:
        return _dlq(source, ERROR_JSON, f"JSON inválido: {exc}", data, attributes, now)
    if not isinstance(obj, dict):
        return _dlq(source, ERROR_JSON, f"el cuerpo es {type(obj).__name__}, no un objeto JSON",
                    data, attributes, now)

    problem = describe_error(envelope_validator, obj)
    if problem is not None:
        return _dlq(source, ERROR_SCHEMA, problem, data, attributes, now)

    if attributes_validator is not None:
        problem = describe_error(attributes_validator, attributes)
        if problem is not None:
            return _dlq(source, ERROR_ATTRIBUTES, problem, data, attributes, now)
    mismatches = []
    for key in COHERENT_ATTRIBUTES:
        if key not in attributes:
            mismatches.append(f"falta el atributo {key}")
        elif str(attributes[key]) != str(obj.get(key)):
            mismatches.append(f"{key}: atributo={attributes[key]!r} cuerpo={obj.get(key)!r}")
    if mismatches:
        return _dlq(source, ERROR_ATTRIBUTES, "; ".join(mismatches), data, attributes, now)

    if obj["source"] != source:
        return _dlq(source, ERROR_ROUTING,
                    f"evento de {obj['source']!r} en la suscripción de {source!r}",
                    data, attributes, now)

    return ValidEvent(source=source, event_id=obj["event_id"], body=_one_line(text, obj),
                      process_ts=now)


class Validators:
    """Validadores construidos una vez por worker a partir del bundle."""

    def __init__(self, bundle: dict):
        self.envelope = build_validator(bundle, ENVELOPE_SCHEMA)
        self.attributes = (build_validator(bundle, ATTRIBUTES_SCHEMA)
                           if ATTRIBUTES_SCHEMA in bundle else None)

    def check(self, source: str, data: bytes, attributes, now: float | None = None):
        return check_message(source, data, attributes, self.envelope, self.attributes, now)
