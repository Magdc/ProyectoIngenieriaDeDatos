"""``export-state/watermark.json`` (§5.4 y §5.6).

El watermark guarda hasta qué ``timeCreated`` de GCS está todo exportado (``cursor``).
Cada corrida vuelve a mirar los objetos creados en la última ``overlap`` antes del cursor
(por defecto 1 h): así un objeto que se hizo visible un poco tarde no se pierde, y la
comparación con S3 (mismo nombre y tamaño) evita subirlo dos veces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from exporter.jsonlog import format_ts, parse_ts

WATERMARK_SCHEMA = "ta-export-watermark@1"
ONE_MS = timedelta(milliseconds=1)


class WatermarkError(ValueError):
    """El archivo existe pero no tiene el formato esperado."""


@dataclass(frozen=True)
class Watermark:
    cursor: datetime | None = None
    updated_at: datetime | None = None
    run_id: str | None = None
    exporter: str | None = None
    last_run: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "Watermark":
        return cls()

    @classmethod
    def from_json(cls, data: dict[str, Any] | None) -> "Watermark":
        if not data:
            return cls.empty()
        if not isinstance(data, dict):
            raise WatermarkError("el watermark no es un objeto JSON")
        schema = data.get("schema")
        if schema != WATERMARK_SCHEMA:
            raise WatermarkError(f"schema desconocido: {schema!r} (se esperaba {WATERMARK_SCHEMA})")
        try:
            cursor = parse_ts(data.get("cursor_time_created"))
            updated = parse_ts(data.get("updated_at"))
        except ValueError as exc:
            raise WatermarkError(f"fecha inválida en el watermark: {exc}") from exc
        return cls(
            cursor=cursor,
            updated_at=updated,
            run_id=data.get("run_id"),
            exporter=data.get("exporter"),
            last_run=dict(data.get("last_run") or {}),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": WATERMARK_SCHEMA,
            "cursor_time_created": format_ts(self.cursor),
            "updated_at": format_ts(self.updated_at),
            "run_id": self.run_id,
            "exporter": self.exporter,
            "last_run": self.last_run,
        }


def floor_ms(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def candidate_threshold(wm: Watermark, overlap_seconds: int) -> datetime | None:
    """Los objetos con ``timeCreated`` > umbral son candidatos (``None`` = todos)."""
    if wm.cursor is None:
        return None
    return wm.cursor - timedelta(seconds=overlap_seconds)


def listing_since(wm: Watermark, overlap_seconds: int, margin_seconds: int) -> datetime | None:
    """Desde qué partición listar. El margen cubre los paneles tardíos, que se escriben en la
    partición de su ventana (hasta ~1 h 10 min antes de su ``timeCreated``)."""
    if wm.cursor is None:
        return None
    return wm.cursor - timedelta(seconds=overlap_seconds + margin_seconds)


def next_cursor(
    previous: datetime | None,
    candidate_times: list[datetime],
    pending_times: list[datetime],
) -> datetime | None:
    """Cursor tras una corrida sin fallas.

    - Sin pendientes: el mayor ``timeCreated`` de los candidatos.
    - Con pendientes (corte por ``max_objects``): justo antes del pendiente más antiguo,
      para que todo lo que queda antes del cursor ya esté en S3.
    - Nunca retrocede: un objeto que aparece con un ``timeCreated`` anterior al cursor
      vuelve a ser candidato por el solape.
    """
    proposal: datetime | None
    if pending_times:
        proposal = floor_ms(min(pending_times)) - ONE_MS
    elif candidate_times:
        proposal = floor_ms(max(candidate_times))
    else:
        proposal = None
    values = [v for v in (previous, proposal) if v is not None]
    return max(values) if values else None
