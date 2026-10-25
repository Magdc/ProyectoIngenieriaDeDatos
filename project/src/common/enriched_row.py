"""Envelope enriquecido y fila plana de ``posts_enriched`` (Lote 2.2, §5.2, §5.4, §5.5).

Lo usan Dataflow (``streaming.enriched``) y, desde el Lote 3.1, el loader histórico,
para que stream e histórico se anonimicen y normalicen con **el mismo código**.

Flujo:

    envelope (§5.2) ──enrich_envelope()──► envelope enriquecido (sin identidad,
                                             con likes/shares/comments y process_time;
                                             el NLP lo añade el Lote 2.3)
                     ──to_posts_enriched_row()──► fila plana (columnas de §5.5)

El módulo solo depende de la librería estándar y de ``common.envelope`` /
``common.hashing`` (Lote 1.1). No importa Beam ni pyarrow.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from common import envelope as _envelope
from common import hashing as _hashing

# --------------------------------------------------------------------------- #
# Columnas de posts_enriched (§5.5). Mismo orden en BigQuery, Parquet y filas.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Column:
    name: str
    bq_type: str  # STRING, INT64, FLOAT64, TIMESTAMP, JSON
    repeated: bool = False
    required: bool = False
    description: str = ""


POSTS_ENRICHED_COLUMNS: tuple[Column, ...] = (
    Column("event_id", "STRING", required=True, description="UUID del evento (uuid5, §3.6). Clave de deduplicación."),
    Column("origin", "STRING", description="stream | historical"),
    Column("source", "STRING", description="Red social simulada: reddit | facebook | instagram"),
    Column("generator", "STRING", description="Generador de comentarios y versión (comment-gen@1.0)"),
    Column("event_time", "TIMESTAMP", required=True, description="Cuándo se publicó el comentario (UTC). Partición."),
    Column("ingest_time", "TIMESTAMP", description="Cuándo lo emitió el simulador o el loader (UTC)"),
    Column("process_time", "TIMESTAMP", description="Cuándo lo enriqueció Dataflow o el loader (UTC)"),
    Column("user_hash", "STRING", description="HMAC-SHA256 hex del user_id (dato anonimizado)"),
    Column("followers", "INT64", description="Seguidores del autor; puede ser nulo"),
    Column("text", "STRING", description="Texto del comentario"),
    Column("lang", "STRING", description="Idioma (es) o nulo"),
    Column("brands", "STRING", repeated=True, description="Marcas canónicas mencionadas"),
    Column("brand_primary", "STRING", description="brands[0] o nulo"),
    Column("hashtags", "STRING", repeated=True, description="Hashtags en forma canónica"),
    Column("likes", "INT64", description="score | reactions_total | like_count"),
    Column("shares", "INT64", description="shares_count (Facebook); nulo en Reddit e Instagram"),
    Column("comments", "INT64", description="num_comments | comments_count"),
    Column("sentiment_label", "STRING", description="negative | neutral | positive (Lote 2.3)"),
    Column("sentiment_score", "FLOAT64", description="[0, 1] (Lote 2.3)"),
    Column("emotion_label", "STRING", description="joy, sadness, anger, fear, disgust, surprise, neutral (Lote 2.3)"),
    Column("emotion_score", "FLOAT64", description="[0, 1] (Lote 2.3)"),
    Column("label_sentiment", "STRING", description="Ground truth del simulador; solo evaluación, nunca feature"),
    Column("nlp_model_version", "STRING", description="robertuito-sentiment@<hash>+robertuito-emotion@<hash> (Lote 2.3)"),
    Column("payload", "JSON", description="payload específico de la red, como JSON"),
)

COLUMN_NAMES: tuple[str, ...] = tuple(c.name for c in POSTS_ENRICHED_COLUMNS)

#: Columnas que rellena el NLP (Lote 2.3). En el Lote 2.2 viajan en nulo.
NLP_FIELDS: tuple[str, ...] = (
    "sentiment_label",
    "sentiment_score",
    "emotion_label",
    "emotion_score",
    "nlp_model_version",
)

#: Claves de identidad que no pueden salir de Dataflow ni del loader (§5.2, ADR-06).
IDENTITY_KEYS: frozenset[str] = frozenset({"user_id", "handle"})

USER_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #


def format_process_time(when: datetime | None = None) -> str:
    """UTC, ISO-8601 con milisegundos y ``Z`` (§5.1)."""
    when = when or datetime.now(timezone.utc)
    if when.tzinfo is None:
        raise ValueError("process_time debe tener zona horaria")
    when = when.astimezone(timezone.utc)
    return f"{when.strftime(_TS_FORMAT)}.{when.microsecond // 1000:03d}Z"


def hash_user(user_id: str, key: str) -> str:
    """``author.user_hash`` = HMAC-SHA256 hex de ``user_id`` con la llave ``key``.

    Punto de entrada único para Dataflow y el loader: llama a
    ``common.hashing.user_hash`` (Lote 1.1). La llave es el **texto** guardado en
    el secreto ``ta-hmac-key`` (ver ``common.hmac_key``).
    """
    if not isinstance(user_id, str) or not user_id:
        raise ValueError("user_id vacío o no es texto")
    if not isinstance(key, str) or not key:
        raise ValueError("llave HMAC vacía")
    try:
        result = _hashing.user_hash(user_id, key)
    except TypeError:
        # Por si la firma de Lote 1.1 recibe bytes.
        result = _hashing.user_hash(user_id, key.encode("utf-8"))
    if not isinstance(result, str) or not USER_HASH_RE.match(result):
        raise ValueError("common.hashing.user_hash no devolvió un hex de 64 caracteres")
    return result


def engagement(source: str, payload: Mapping[str, Any]) -> tuple[int | None, int | None, int | None]:
    """``(likes, shares, comments)`` con la regla de §5.2, vía ``common.envelope``."""
    result = _envelope.normalize_engagement(source, payload)
    if isinstance(result, Mapping):
        return result.get("likes"), result.get("shares"), result.get("comments")
    if isinstance(result, (tuple, list)) and len(result) == 3:
        likes, shares, comments = result
        return likes, shares, comments
    raise TypeError(f"normalize_engagement devolvió un tipo inesperado: {type(result).__name__}")


def find_identity_paths(obj: Any, _path: str = "") -> list[str]:
    """Rutas donde aparece una clave de identidad (``user_id`` o ``handle``)."""
    found: list[str] = []
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            here = f"{_path}.{k}" if _path else str(k)
            if k in IDENTITY_KEYS:
                found.append(here)
            found.extend(find_identity_paths(v, here))
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            found.extend(find_identity_paths(v, f"{_path}[{i}]"))
    return found


# --------------------------------------------------------------------------- #
# Envelope enriquecido y fila plana
# --------------------------------------------------------------------------- #


def enrich_envelope(envelope: Mapping[str, Any], key: str, process_time: str) -> dict[str, Any]:
    """Envelope enriquecido sin NLP: quita la identidad, añade engagement y ``process_time``.

    No modifica el envelope de entrada. Lanza ``ValueError`` si falta el autor o el
    ``user_id`` (no debería pasar: el envelope ya se validó contra el schema).
    """
    author = envelope.get("author")
    if not isinstance(author, Mapping) or not author.get("user_id"):
        raise ValueError("envelope sin author.user_id")
    source = envelope.get("source")
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("envelope sin payload")

    out: dict[str, Any] = {}
    for k, v in envelope.items():
        if k == "author":
            out["author"] = {
                "user_hash": hash_user(str(author["user_id"]), key),
                "followers": author.get("followers"),
            }
        else:
            out[k] = copy.deepcopy(v)
    likes, shares, comments = engagement(str(source), payload)
    out["likes"] = likes
    out["shares"] = shares
    out["comments"] = comments
    out["process_time"] = process_time

    leaked = find_identity_paths(out)
    if leaked:  # defensa: nunca debe salir identidad en claro
        raise ValueError(f"identidad en el envelope enriquecido: {leaked}")
    return out


def payload_json(payload: Any) -> str:
    """``payload`` como JSON compacto (UTF-8, conserva el orden de las claves)."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def to_posts_enriched_row(enriched: Mapping[str, Any]) -> dict[str, Any]:
    """Fila plana de ``posts_enriched`` desde un envelope enriquecido (§5.4).

    Los timestamps quedan como texto ISO (``…Z``); ``payload`` como JSON en texto.
    Las columnas de NLP se toman si existen (Lote 2.3) y si no, quedan en nulo.
    """
    author = enriched.get("author") or {}
    brands = list(enriched.get("brands") or [])
    sim = enriched.get("sim") or {}
    row: dict[str, Any] = {
        "event_id": enriched["event_id"],
        "origin": enriched.get("origin"),
        "source": enriched.get("source"),
        "generator": enriched.get("generator"),
        "event_time": enriched["event_time"],
        "ingest_time": enriched.get("ingest_time"),
        "process_time": enriched.get("process_time"),
        "user_hash": author.get("user_hash"),
        "followers": author.get("followers"),
        "text": enriched.get("text"),
        "lang": enriched.get("lang"),
        "brands": brands,
        "brand_primary": brands[0] if brands else None,
        "hashtags": list(enriched.get("hashtags") or []),
        "likes": enriched.get("likes"),
        "shares": enriched.get("shares"),
        "comments": enriched.get("comments"),
        "sentiment_label": enriched.get("sentiment_label"),
        "sentiment_score": enriched.get("sentiment_score"),
        "emotion_label": enriched.get("emotion_label"),
        "emotion_score": enriched.get("emotion_score"),
        "label_sentiment": sim.get("label_sentiment"),
        "nlp_model_version": enriched.get("nlp_model_version"),
        "payload": payload_json(enriched.get("payload")),
    }
    assert tuple(row) == COLUMN_NAMES  # el orden es parte del contrato interno
    return row


def envelope_to_row(envelope: Mapping[str, Any], key: str, process_time: str) -> dict[str, Any]:
    """Atajo: envelope de §5.2 → fila de ``posts_enriched``."""
    return to_posts_enriched_row(enrich_envelope(envelope, key, process_time))


def row_problems(row: Mapping[str, Any]) -> list[str]:
    """Reglas que se pueden comprobar sobre una fila (las usa el verificador)."""
    problems: list[str] = []
    if tuple(row.keys()) != COLUMN_NAMES and set(row.keys()) != set(COLUMN_NAMES):
        problems.append("columnas distintas de posts_enriched")
    for k in IDENTITY_KEYS:
        if k in row:
            problems.append(f"columna de identidad '{k}'")
    uh = row.get("user_hash")
    if not isinstance(uh, str) or not USER_HASH_RE.match(uh):
        problems.append("user_hash no es hex de 64")
    raw_payload = row.get("payload")
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except ValueError:
            problems.append("payload no es JSON")
        else:
            if find_identity_paths(parsed):
                problems.append("identidad dentro de payload")
    brands = row.get("brands") or []
    if row.get("brand_primary") != (brands[0] if brands else None):
        problems.append("brand_primary distinto de brands[0]")
    source = row.get("source")
    shares, likes, comments = row.get("shares"), row.get("likes"), row.get("comments")
    if source in ("reddit", "instagram") and shares is not None:
        problems.append(f"shares debe ser nulo en {source}")
    if source == "facebook" and not isinstance(shares, int):
        problems.append("shares debe ser entero en facebook")
    if not isinstance(likes, int):
        problems.append("likes debe ser entero")
    if source in ("facebook", "instagram") and not isinstance(comments, int):
        problems.append(f"comments debe ser entero en {source}")
    if source == "reddit" and comments is not None and not isinstance(comments, int):
        problems.append("comments debe ser entero o nulo en reddit")
    return problems


def bigquery_schema_fields(columns: Iterable[Column] = POSTS_ENRICHED_COLUMNS) -> list[dict[str, str]]:
    """Schema en el formato JSON de BigQuery (el mismo de ``infra/.../posts_enriched.json``)."""
    fields = []
    for c in columns:
        mode = "REPEATED" if c.repeated else ("REQUIRED" if c.required else "NULLABLE")
        fields.append({"name": c.name, "type": c.bq_type, "mode": mode, "description": c.description})
    return fields
