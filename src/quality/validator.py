"""
src/quality/validator.py

Validador de registros del esquema comun de ingesta.
====================================================
Expone dos interfaces:

  validate_record(record) -> ValidationResult
      Valida un registro individual antes de publicar a Pub/Sub.
      Usado directamente por cada collector (mastodon, reddit, news).

  validate_dataframe(df) -> DataFrameValidationResult
      Valida un DataFrame completo (usado por el profiler y el
      Cloud Run Job de calidad sobre la capa Raw).

Politica de fallos (segun US-13):
  - Registro invalido → el caller debe publicarlo al topic Dead Letter
    o escribirlo en GCS bajo el prefijo 'dead-letter/'.
  - Si el % de invalidos supera ALERT_THRESHOLD → loguear WARNING
    (el monitoreo real se hace en GCP Monitoring via los logs de Cloud Run).

Variables de entorno (cuando se usa como servicio Cloud Run):
  PUBSUB_TOPIC_DL   — Nombre del topic Dead Letter (ej. trend-dev-dead-letter)
  GCP_PROJECT_ID    — ID del proyecto GCP
"""

import json
import logging
import os
from dataclasses import dataclass, field

import pandas as pd

from expectations import ALL_RULES

log = logging.getLogger(__name__)

# Si el % de registros invalidos supera este umbral → WARNING en logs
ALERT_THRESHOLD = 0.05   # 5%


# ---------------------------------------------------------------------------
# Tipos de resultado
# ---------------------------------------------------------------------------
@dataclass
class ValidationResult:
    """Resultado de la validacion de un registro individual."""
    record: dict
    is_valid: bool
    errors: list[str] = field(default_factory=list)

    def to_dead_letter_payload(self) -> dict:
        """Serializa el registro invalido con sus errores para el Dead Letter."""
        return {
            "original_record": self.record,
            "validation_errors": self.errors,
        }


@dataclass
class DataFrameValidationResult:
    """Resultado de la validacion de un DataFrame completo."""
    total: int
    valid: int
    invalid: int
    invalid_pct: float
    alert_triggered: bool
    invalid_records: list[dict] = field(default_factory=list)
    errors_summary: dict[str, int] = field(default_factory=dict)   # regla → count

    def passed_kpi(self) -> bool:
        """Retorna True si se cumplen los KPIs del proyecto."""
        completeness = self.valid / self.total if self.total > 0 else 0
        return completeness >= 0.95 and self.invalid_pct < 0.01


# ---------------------------------------------------------------------------
# Validacion de un registro individual
# ---------------------------------------------------------------------------
def validate_record(record: dict) -> ValidationResult:
    """Aplica todas las reglas al registro y retorna el resultado.

    Uso tipico en los collectors:

        result = validate_record(mapped_event)
        if result.is_valid:
            publish_to_pubsub(mapped_event)
        else:
            publish_to_dead_letter(result.to_dead_letter_payload())
    """
    errors: list[str] = []
    for rule in ALL_RULES:
        ok, msg = rule(record)
        if not ok:
            errors.append(msg)
    return ValidationResult(
        record=record,
        is_valid=len(errors) == 0,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Validacion de un DataFrame completo
# ---------------------------------------------------------------------------
def validate_dataframe(df: pd.DataFrame) -> DataFrameValidationResult:
    """Valida cada fila del DataFrame y agrega estadisticas.

    Convierte cada fila a dict antes de aplicar las reglas para que
    los tipos pandas no interfieran con las comparaciones.
    """
    total = len(df)
    if total == 0:
        return DataFrameValidationResult(
            total=0, valid=0, invalid=0,
            invalid_pct=0.0, alert_triggered=False,
        )

    invalid_records: list[dict] = []
    errors_summary: dict[str, int] = {}
    valid_count = 0

    records = df.where(df.notna(), other=None).to_dict(orient="records")

    for record in records:
        result = validate_record(record)
        if result.is_valid:
            valid_count += 1
        else:
            invalid_records.append(result.to_dead_letter_payload())
            for err in result.errors:
                # La clave es el codigo de regla (R1, R2, ...) 
                rule_code = err.split(":")[0] if ":" in err else "UNKNOWN"
                errors_summary[rule_code] = errors_summary.get(rule_code, 0) + 1

    invalid_count = total - valid_count
    invalid_pct = invalid_count / total
    alert = invalid_pct > ALERT_THRESHOLD

    if alert:
        log.warning(
            "CALIDAD: %.1f%% de registros invalidos (umbral: %.0f%%) — "
            "revisar Dead Letter topic",
            invalid_pct * 100, ALERT_THRESHOLD * 100,
        )

    return DataFrameValidationResult(
        total=total,
        valid=valid_count,
        invalid=invalid_count,
        invalid_pct=invalid_pct,
        alert_triggered=alert,
        invalid_records=invalid_records,
        errors_summary=errors_summary,
    )


# ---------------------------------------------------------------------------
# Anonimizacion — Ley 1581 / GDPR
# ---------------------------------------------------------------------------
def anonymize_record(record: dict) -> dict:
    """Elimina o enmascara los campos de autor antes de la capa analitica.

    Segun Ley 1581 de 2012 (Colombia) y GDPR: los identificadores de
    usuario no deben propagarse a la capa de analisis sin consentimiento
    explicito. Se aplica antes de escribir al bucket curated o a BigQuery.
    """
    anonymized = record.copy()
    anonymized["author_name"] = None
    anonymized["author_id"] = None
    # El author_id en raw_metadata tambien se limpia
    if isinstance(anonymized.get("raw_metadata"), dict):
        anonymized["raw_metadata"] = anonymized["raw_metadata"].copy()
        anonymized["raw_metadata"].pop("author_id", None)
    return anonymized


# ---------------------------------------------------------------------------
# CLI minimo para pruebas manuales
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Registro valido de ejemplo
    sample_valid = {
        "source": "mastodon",
        "id": "117156446756584402",
        "created_at": "2026-08-25T13:45:08.445Z",
        "text": "Cafe colombiano conquista el mercado europeo",
        "url": "https://mastodon.social/@user/117156446756584402",
        "language": "es",
        "tags": ["colombia", "cafe"],
        "author_name": "usuario_prueba",
        "engagement": {"replies": 2, "reposts": 5, "likes": 10},
        "raw_metadata": {},
    }

    # Registro invalido de ejemplo
    sample_invalid = {
        "source": "twitter",          # R2: fuente invalida
        "id": "",                     # R1: id vacio
        "created_at": "no-es-fecha",  # R3: fecha invalida
        "text": "",                   # R4: texto vacio
        "url": "no-es-url",           # R5: URL invalida
        "engagement": {"likes": -5},  # R6: negativo
        "raw_metadata": {},
    }

    print("\n--- Registro VALIDO ---")
    r = validate_record(sample_valid)
    print(f"  is_valid: {r.is_valid}")
    print(f"  errors:   {r.errors}")

    print("\n--- Registro INVALIDO ---")
    r2 = validate_record(sample_invalid)
    print(f"  is_valid: {r2.is_valid}")
    for e in r2.errors:
        print(f"  * {e}")

    print("\n--- Anonimizacion ---")
    anon = anonymize_record(sample_valid)
    print(f"  author_name: {anon['author_name']}")
    print(f"  author_id:   {anon.get('author_id')}")

    sys.exit(0 if r.is_valid and not r2.is_valid else 1)
