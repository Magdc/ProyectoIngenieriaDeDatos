"""Comprobaciones por fila que el *profiling* mide en cada grupo (§3.20.1).

Cada comprobación devuelve ``True`` (cumple), ``False`` (no cumple) o ``None`` (no aplica, p. ej.
un valor nulo en una comprobación de formato: igual que Great Expectations, los nulos se miden
aparte con ``not_null:*``). El profiler cuenta ``evaluated`` (True + False) y ``failed`` por grupo
y la propuesta de reglas (``rules.py``) convierte cada una en una regla con umbral para el Lote 5.2.

``severity`` es la gravedad de un incumplimiento al detectar anomalías: ``error`` = contrato de
§5.2/§5.7 roto; ``warning`` = posible por diseño (caos de §3.9, retrasos) pero hay que vigilarlo.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from common.hashtags import extract_hashtags

from quality import layers as L

ERROR, WARNING, INFO = "error", "warning", "info"
INGEST_LAG_MAX_S = 5.0
# process_time se fija al enriquecer, antes de la ventana de escritura: puede quedar unos segundos
# antes de la hora de la partición (medido −19,6 s) y, con un panel tardío de la última ventana de la
# hora, hasta una ventana (300 s) después de su fin (medido 3.600 s).
STREAM_OFFSET_MIN_S = -120.0
STREAM_OFFSET_MAX_S = 3900.0
FUTURE_TOLERANCE = timedelta(minutes=5)
#: Primera hora de proceso con NLP en Raw stream (medido el 26-oct-2026): antes, los jobs de los
#: lotes 2.1–2.2 corrían sin NLP y esas filas tienen las 5 columnas de NLP nulas (§3.14).
NLP_SINCE = datetime(2026, 10, 22, 23, 0, tzinfo=timezone.utc)
#: Columnas que son nulas por diseño en parte de las filas: su ``not_null`` es solo informativo
#: (las reglas por fuente DQ-25/26 y la de DQ-34 dicen cuándo deben ser nulas).
NULLABLE_BY_DESIGN = frozenset({"shares", "comments", "label_sentiment"})
GENERATORS = ("comment-gen@1.0",)


@dataclass
class RowMeta:
    """Lo que no está en la fila: capa, particiones de la clave y el instante de referencia."""

    layer: str
    origin: str | None  # Raw: partición; Trusted: columna origin
    source: str | None  # partición
    dt: str | None
    hr: str | None
    file: str
    as_of: datetime
    window_start: datetime | None = None
    brand_keys: frozenset[str] = field(default_factory=lambda: frozenset(L.BRAND_KEYS_V1))


def as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:  # Parquet sin zona (p. ej. el de Glue): se interpreta como UTC
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _payload(row: dict) -> Any:
    raw = row.get("payload")
    if not isinstance(raw, str):
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def _has_identity(obj: Any) -> bool:
    if isinstance(obj, dict):
        return any(k in L.IDENTITY_COLUMNS or _has_identity(v) for k, v in obj.items())
    if isinstance(obj, list):
        return any(_has_identity(v) for v in obj)
    return False


def _source(row: dict, meta: RowMeta) -> str | None:
    return meta.source if meta.layer == "trusted" else row.get("source")


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


# --- funciones de comprobación ---------------------------------------------------------------


def _regex(col: str, rx) -> Callable[[dict, RowMeta], bool | None]:
    def fn(row, meta):
        v = row.get(col)
        return None if v is None else bool(isinstance(v, str) and rx.match(v))
    return fn


def _in_set(col: str, values) -> Callable[[dict, RowMeta], bool | None]:
    allowed = frozenset(values)

    def fn(row, meta):
        v = row.get(col)
        return None if v is None else v in allowed
    return fn


def _between(col: str, lo: float | None, hi: float | None) -> Callable[[dict, RowMeta], bool | None]:
    def fn(row, meta):
        v = row.get(col)
        if v is None:
            return None
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            return False
        return (lo is None or v >= lo) and (hi is None or v <= hi)
    return fn


def _list_all(col: str, pred) -> Callable[[dict, RowMeta], bool | None]:
    def fn(row, meta):
        v = row.get(col)
        if v is None:
            return None
        return all(isinstance(x, str) and pred(x, meta) for x in v)
    return fn


def _list_unique(col: str):
    def fn(row, meta):
        v = row.get(col)
        return None if v is None else len(v) == len(set(v))
    return fn


def _source_in_set(row, meta):
    s = _source(row, meta)
    return None if s is None else s in L.SOURCES


def _origin_in_set(row, meta):
    v = row.get("origin")
    return None if v is None else v in L.ORIGINS


def _origin_matches_partition(row, meta):
    v = row.get("origin")
    return None if v is None or meta.layer != "raw" else v == meta.origin


def _source_matches_partition(row, meta):
    v = row.get("source")
    return None if v is None or meta.layer != "raw" else v == meta.source


def _brand_primary_first(row, meta):
    brands = row.get("brands")
    if brands is None:
        return None
    return row.get("brand_primary") == (brands[0] if brands else None)


def _hashtags_match_text(row, meta):
    text, tags = row.get("text"), row.get("hashtags")
    if text is None or tags is None:
        return None
    return list(tags) == extract_hashtags(text)


def _likes(row, meta):
    v, s = row.get("likes"), _source(row, meta)
    if v is None:
        return None
    if not _is_int(v):
        return False
    return True if s == "reddit" else v >= 0  # en Reddit el score puede ser negativo (§5.2)


def _shares(row, meta):
    v, s = row.get("shares"), _source(row, meta)
    if s in ("reddit", "instagram"):
        return v is None
    if s == "facebook":
        return _is_int(v) and v >= 0
    return None


def _comments(row, meta):
    v, s = row.get("comments"), _source(row, meta)
    if s in ("facebook", "instagram"):
        return _is_int(v) and v >= 0
    if s == "reddit":
        return v is None or (_is_int(v) and v >= 0)
    return None


def _text_not_empty(row, meta):
    v = row.get("text")
    return None if v is None else bool(isinstance(v, str) and v.strip())


def _payload_json(row, meta):
    raw = row.get("payload")
    if raw is None:
        return None
    return isinstance(_payload(row), dict)


def _payload_no_identity(row, meta):
    raw = row.get("payload")
    if raw is None:
        return None
    obj = _payload(row)
    return obj is not None and not _has_identity(obj)


def _payload_kind(row, meta):
    obj = _payload(row)
    if not isinstance(obj, dict):
        return None
    kind = obj.get("kind")
    if _source(row, meta) == "instagram":
        return kind == "comment"
    return kind in L.KINDS


def _ingest_ge_event(row, meta):
    e, i = as_utc(row.get("event_time")), as_utc(row.get("ingest_time"))
    return None if e is None or i is None else i >= e


def _ingest_lag(row, meta):
    e, i = as_utc(row.get("event_time")), as_utc(row.get("ingest_time"))
    if e is None or i is None:
        return None
    return 0 <= (i - e).total_seconds() <= INGEST_LAG_MAX_S


def _process_ge_ingest(row, meta):
    i, p = as_utc(row.get("ingest_time")), as_utc(row.get("process_time"))
    return None if i is None or p is None else p >= i


def _not_future(row, meta):
    e = as_utc(row.get("event_time"))
    return None if e is None else e <= meta.as_of + FUTURE_TOLERANCE


def _projection_range(row, meta):
    return None if meta.dt is None else meta.dt >= L.PROJECTION_START


def _hist_partition_event_hour(row, meta):
    if meta.layer != "raw" or meta.origin != "historical":
        return None
    e = as_utc(row.get("event_time"))
    if e is None or meta.dt is None or meta.hr is None:
        return None
    return e.strftime("%Y-%m-%d") == meta.dt and e.strftime("%H") == meta.hr


def _file_name_partition(row, meta):
    if meta.layer != "raw":
        return None
    return L.file_name_matches_partition(meta.origin, {"file": meta.file, "dt": meta.dt or "", "hr": meta.hr})


def _stream_process_window(row, meta):
    if meta.layer != "raw" or meta.origin != "stream" or meta.dt is None or meta.hr is None:
        return None
    p = as_utc(row.get("process_time"))
    if p is None:
        return None
    hour_start = datetime.strptime(f"{meta.dt} {meta.hr}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
    offset = (p - hour_start).total_seconds()
    return STREAM_OFFSET_MIN_S <= offset <= STREAM_OFFSET_MAX_S


def _trusted_dt_event_date(row, meta):
    if meta.layer != "trusted":
        return None
    e = as_utc(row.get("event_time"))
    return None if e is None or meta.dt is None else e.strftime("%Y-%m-%d") == meta.dt


def _text_clean_not_longer(row, meta):
    t, c = row.get("text"), row.get("text_clean")
    if t is None or c is None:
        return None
    return len(c) <= len(t)


def _nlp_complete(row, meta):
    return all(row.get(c) is not None for c in L.NLP_COLUMNS)


def _nlp_complete_since(row, meta):
    p = as_utc(row.get("process_time"))
    if p is None or p < NLP_SINCE:
        return None
    return _nlp_complete(row, meta)


def _brand_in_keys(x, meta):
    return x in meta.brand_keys


def _brand_primary_in_keys(row, meta):
    v = row.get("brand_primary")
    return None if v is None else v in meta.brand_keys


# --- registro ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class Check:
    id: str
    column: str
    description: str
    fn: Callable[[dict, RowMeta], bool | None]
    severity: str = ERROR
    layers: tuple[str, ...] = ("raw", "trusted")


def _not_null_checks() -> list[Check]:
    out = []
    soft = {"lang", "followers", "brand_primary", *L.NLP_COLUMNS}
    names = sorted(set(L.RAW_COLUMNS) | set(L.TRUSTED_COLUMNS))
    for name in names:
        layers = tuple(lay for lay in ("raw", "trusted") if name in L.layer_spec(lay).columns)
        severity = INFO if name in NULLABLE_BY_DESIGN else (WARNING if name in soft else ERROR)
        out.append(Check(f"not_null:{name}", name, f"{name} no nulo", (lambda n: lambda r, m: r.get(n) is not None)(name),
                         severity, layers))
    out.append(Check("not_null:source_partition", "source", "source (partición) no nula", lambda r, m: m.source is not None,
                     ERROR, ("trusted",)))
    return out


CHECKS: tuple[Check, ...] = tuple(_not_null_checks()) + (
    Check("uuid:event_id", "event_id", "event_id es UUID en minúsculas (§3.6)", _regex("event_id", L.UUID_RE)),
    Check("enum:origin", "origin", "origin ∈ {stream, historical}", _origin_in_set),
    Check("enum:source", "source", "source ∈ {reddit, facebook, instagram}", _source_in_set),
    Check("partition:origin", "origin", "origin = partición origin= (Raw)", _origin_matches_partition, ERROR, ("raw",)),
    Check("partition:source", "source", "source = partición source= (Raw)", _source_matches_partition, ERROR, ("raw",)),
    Check("regex:generator", "generator", "generator = nombre@versión", _regex("generator", L.GENERATOR_RE)),
    Check("set:generator", "generator", "generator = comment-gen@1.0 (otro valor = evento de validación, p. ej. ta-verify@2.1)",
          _in_set("generator", GENERATORS), WARNING),
    Check("regex:lang", "lang", "lang = dos letras minúsculas", _regex("lang", L.LANG_RE)),
    Check("set:lang", "lang", "lang = es (v2.2)", _in_set("lang", L.LANGS), WARNING),
    Check("regex:user_hash", "user_hash", "user_hash = hex de 64 (HMAC-SHA256)", _regex("user_hash", L.HEX64_RE)),
    Check("payload:no_identity", "payload", "payload sin claves user_id/handle", _payload_no_identity),
    Check("payload:json", "payload", "payload es un objeto JSON", _payload_json),
    Check("payload:kind", "payload", "payload.kind ∈ {post, comment}; Instagram solo comment", _payload_kind),
    Check("enum:sentiment_label", "sentiment_label", "sentiment_label ∈ {negative, neutral, positive}", _in_set("sentiment_label", L.SENTIMENTS)),
    Check("enum:emotion_label", "emotion_label", "emotion_label ∈ las 7 clases de §3.3", _in_set("emotion_label", L.EMOTIONS)),
    Check("enum:label_sentiment", "label_sentiment", "label_sentiment ∈ {negative, neutral, positive} o nulo", _in_set("label_sentiment", L.SENTIMENTS)),
    Check("range:sentiment_score", "sentiment_score", "sentiment_score ∈ [0, 1]", _between("sentiment_score", 0.0, 1.0)),
    Check("range:emotion_score", "emotion_score", "emotion_score ∈ [0, 1]", _between("emotion_score", 0.0, 1.0)),
    Check("regex:nlp_model_version", "nlp_model_version", "nlp_model_version = robertuito-sentiment@…+robertuito-emotion@…",
          _regex("nlp_model_version", L.NLP_VERSION_RE)),
    Check("nlp:complete", "nlp_model_version", "las 5 columnas de NLP presentes", _nlp_complete, WARNING),
    Check("nlp:complete_since", "nlp_model_version",
          f"las 5 columnas de NLP presentes con process_time ≥ {NLP_SINCE:%Y-%m-%dT%H:%MZ}", _nlp_complete_since, WARNING),
    Check("regex:brands", "brands", "brands[] con claves [a-z0-9_]+", _list_all("brands", lambda x, m: bool(L.BRAND_KEY_RE.match(x)))),
    Check("set:brands", "brands", "brands[] ⊆ claves de brands.yaml", _list_all("brands", _brand_in_keys)),
    Check("unique:brands", "brands", "brands[] sin repetidos", _list_unique("brands")),
    Check("set:brand_primary", "brand_primary", "brand_primary ∈ claves de brands.yaml", _brand_primary_in_keys),
    Check("pair:brand_primary", "brand_primary", "brand_primary = brands[0] (o nulo si brands = [])", _brand_primary_first),
    Check("regex:hashtags", "hashtags", "hashtags[] canónicos [a-z0-9_]{1,100}", _list_all("hashtags", lambda x, m: bool(L.HASHTAG_RE.match(x)))),
    Check("unique:hashtags", "hashtags", "hashtags[] sin repetidos", _list_unique("hashtags")),
    Check("consistency:hashtags_text", "hashtags", "hashtags[] = extract_hashtags(text)", _hashtags_match_text, WARNING),
    Check("range:followers", "followers", "followers ≥ 0", _between("followers", 0, None)),
    Check("engagement:likes", "likes", "likes entero (≥ 0 salvo Reddit)", _likes),
    Check("engagement:shares", "shares", "shares nulo en Reddit e Instagram; entero ≥ 0 en Facebook", _shares),
    Check("engagement:comments", "comments", "comments entero ≥ 0 en Facebook e Instagram; entero o nulo en Reddit", _comments),
    Check("text:not_empty", "text", "text no vacío", _text_not_empty),
    Check("time:ingest_ge_event", "ingest_time", "ingest_time ≥ event_time", _ingest_ge_event),
    Check("time:ingest_lag", "ingest_time", f"0 ≤ ingest_time − event_time ≤ {INGEST_LAG_MAX_S:g} s (lag de §3.8)", _ingest_lag, WARNING),
    Check("time:process_ge_ingest", "process_time", "process_time ≥ ingest_time", _process_ge_ingest),
    Check("time:not_future", "event_time", "event_time ≤ instante del profiling (+5 min)", _not_future),
    Check("partition:projection_range", "dt", f"dt ≥ {L.PROJECTION_START} (rango de la partition projection)", _projection_range),
    Check("partition:hist_event_hour", "event_time", "histórico: dt/hr = fecha y hora UTC de event_time", _hist_partition_event_hour, ERROR, ("raw",)),
    Check("partition:file_name", "dt", "nombre del archivo coherente con dt/hr (Raw)", _file_name_partition, WARNING, ("raw",)),
    Check("partition:stream_process_hour", "process_time",
          f"stream: process_time − inicio de la hora de la partición ∈ [{STREAM_OFFSET_MIN_S:g}, {STREAM_OFFSET_MAX_S:g}] s",
          _stream_process_window, WARNING, ("raw",)),
    Check("partition:trusted_event_date", "event_time", "Trusted: dt = DATE(event_time) UTC", _trusted_dt_event_date, ERROR, ("trusted",)),
    Check("text_clean:not_longer", "text_clean", "len(text_clean) ≤ len(text)", _text_clean_not_longer, WARNING, ("trusted",)),
)

CHECKS_BY_ID: dict[str, Check] = {c.id: c for c in CHECKS}


def checks_for(layer: str) -> tuple[Check, ...]:
    return tuple(c for c in CHECKS if layer in c.layers)
