"""Acumulación de mensajes y cálculo de métricas por fuente (Lote 1.4).

Este módulo es puro: no habla con Pub/Sub ni con GCS. Recibe cada mensaje
(cuerpo, atributos, ``message_id`` y tiempos) y, al final, calcula un resumen
por fuente y por fase (ventana de tiempo).

Tiempos:
- ``published_at``: ``publish_time`` que asigna el servidor de Pub/Sub (si no
  viene, se usa ``received_at``). Con él se asigna cada mensaje a una ventana,
  de modo que la latencia de entrega no altera la tasa medida.
- ``received_at``: instante en que lo recibió este proceso.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Sequence

# Campos mínimos del envelope (§5.2) que el verificador necesita para medir.
REQUIRED_FIELDS: tuple[str, ...] = ("event_id", "source", "seq")

# Validador opcional: recibe el envelope y devuelve la lista de errores.
Validator = Callable[[dict], list[str]]


@dataclass(frozen=True)
class Observation:
    """Un mensaje recibido, ya interpretado."""

    topic_source: str
    message_id: str
    published_at: float
    received_at: float
    event_id: str | None
    seq: int | None
    body_source: str | None
    attr_event_id: str | None
    attr_source: str | None
    kind: str | None
    chaos: tuple[str, ...]
    malformed: bool
    missing: tuple[str, ...]
    schema_errors: tuple[str, ...] | None  # None = no se validó

    @property
    def effective_event_id(self) -> str | None:
        """``event_id`` del cuerpo o, si el cuerpo está roto, el del atributo."""
        return self.event_id or self.attr_event_id

    @property
    def body_ok(self) -> bool:
        return not self.malformed and not self.missing


def parse_message(
    topic_source: str,
    data: bytes,
    attributes: Mapping[str, str] | None,
    message_id: str,
    received_at: float,
    published_at: float | None = None,
    validator: Validator | None = None,
) -> Observation:
    """Interpreta un mensaje de ``ta-raw-{topic_source}``. Nunca lanza."""
    attrs = dict(attributes or {})
    body: object = None
    malformed = False
    try:
        body = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        malformed = True
    if not isinstance(body, dict):
        malformed = True
        body = {}

    missing = tuple(f for f in REQUIRED_FIELDS if f not in body) if not malformed else ()

    seq = body.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        seq = None
    event_id = body.get("event_id") if isinstance(body.get("event_id"), str) else None
    body_source = body.get("source") if isinstance(body.get("source"), str) else None

    sim = body.get("sim") if isinstance(body.get("sim"), dict) else {}
    chaos_raw = sim.get("chaos") if isinstance(sim.get("chaos"), list) else []
    chaos = tuple(str(c) for c in chaos_raw)

    payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
    kind = payload.get("kind") if isinstance(payload.get("kind"), str) else None

    schema_errors: tuple[str, ...] | None = None
    if validator is not None:
        if malformed:
            schema_errors = ("cuerpo no es un objeto JSON",)
        else:
            try:
                schema_errors = tuple(validator(body))
            except Exception as exc:  # el validador nunca debe tumbar la medición
                schema_errors = (f"error del validador: {exc}",)

    return Observation(
        topic_source=topic_source,
        message_id=message_id,
        published_at=published_at if published_at is not None else received_at,
        received_at=received_at,
        event_id=event_id,
        seq=seq,
        body_source=body_source,
        attr_event_id=attrs.get("event_id"),
        attr_source=attrs.get("source"),
        kind=kind,
        chaos=chaos,
        malformed=malformed,
        missing=missing,
        schema_errors=schema_errors,
    )


class IngestStats:
    """Recolector seguro para hilos (los callbacks de Pub/Sub corren en varios)."""

    def __init__(self, validator: Validator | None = None) -> None:
        self._validator = validator
        self._lock = threading.Lock()
        self._obs: list[Observation] = []
        self.callback_errors = 0

    def add(
        self,
        topic_source: str,
        data: bytes,
        attributes: Mapping[str, str] | None,
        message_id: str,
        received_at: float,
        published_at: float | None = None,
    ) -> Observation:
        obs = parse_message(
            topic_source, data, attributes, message_id, received_at, published_at, self._validator
        )
        with self._lock:
            self._obs.append(obs)
        return obs

    @property
    def validating(self) -> bool:
        return self._validator is not None

    def observations(self) -> list[Observation]:
        with self._lock:
            return list(self._obs)


# ---------------------------------------------------------------------------
# Resumen


@dataclass
class SourceSummary:
    """Métricas de una fuente dentro de una ventana."""

    source: str
    window_s: float
    deliveries: int = 0          # entregas recibidas en la ventana (incluye reentregas)
    messages: int = 0            # mensajes distintos (por message_id)
    redeliveries: int = 0        # mismo message_id entregado más de una vez
    unique_event_ids: int = 0
    duplicate_copies: int = 0    # copias extra de un event_id ya visto
    chaos_duplicates: int = 0    # de esas, las marcadas con sim.chaos = ["duplicate"]
    malformed: int = 0
    incomplete: int = 0
    schema_checked: bool = False
    schema_invalid: int = 0
    attr_mismatch: int = 0
    source_mismatch: int = 0
    seq_min: int | None = None
    seq_max: int | None = None
    seq_gaps: int = 0
    first_published: float | None = None
    last_published: float | None = None
    kinds: dict[str, int] = field(default_factory=dict)
    chaos: dict[str, int] = field(default_factory=dict)
    sample_errors: list[str] = field(default_factory=list)

    @property
    def unexpected_duplicates(self) -> int:
        return self.duplicate_copies - self.chaos_duplicates

    @property
    def rate(self) -> float:
        """Mensajes distintos por segundo en la ventana."""
        return self.messages / self.window_s if self.window_s > 0 else 0.0

    @property
    def unique_ratio(self) -> float:
        return self.unique_event_ids / self.messages if self.messages else 0.0

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "window_s": round(self.window_s, 3),
            "deliveries": self.deliveries,
            "messages": self.messages,
            "redeliveries": self.redeliveries,
            "rate_msg_s": round(self.rate, 4),
            "unique_event_ids": self.unique_event_ids,
            "unique_ratio": round(self.unique_ratio, 6),
            "duplicate_copies": self.duplicate_copies,
            "chaos_duplicates": self.chaos_duplicates,
            "unexpected_duplicates": self.unexpected_duplicates,
            "malformed": self.malformed,
            "incomplete": self.incomplete,
            "schema_checked": self.schema_checked,
            "schema_invalid": self.schema_invalid,
            "attr_mismatch": self.attr_mismatch,
            "source_mismatch": self.source_mismatch,
            "seq_min": self.seq_min,
            "seq_max": self.seq_max,
            "seq_gaps": self.seq_gaps,
            "kinds": dict(sorted(self.kinds.items())),
            "chaos": dict(sorted(self.chaos.items())),
            "sample_errors": list(self.sample_errors),
        }


def in_window(obs: Observation, start: float, end: float) -> bool:
    """Ventana semiabierta ``[start, end)`` sobre ``published_at``."""
    return start <= obs.published_at < end


def summarize(
    observations: Iterable[Observation],
    source: str,
    start: float,
    end: float,
    max_samples: int = 5,
) -> SourceSummary:
    """Resume los mensajes de ``source`` publicados en ``[start, end)``."""
    s = SourceSummary(source=source, window_s=max(0.0, end - start))
    obs = [o for o in observations if o.topic_source == source and in_window(o, start, end)]
    s.deliveries = len(obs)

    by_msg: dict[str, Observation] = {}
    for o in obs:
        if o.message_id in by_msg:
            s.redeliveries += 1
        else:
            by_msg[o.message_id] = o
    distinct = sorted(by_msg.values(), key=lambda o: (o.published_at, o.message_id))
    s.messages = len(distinct)

    kinds: Counter[str] = Counter()
    chaos: Counter[str] = Counter()
    seen_ids: set[str] = set()
    seqs: set[int] = set()

    def sample(msg: str) -> None:
        if len(s.sample_errors) < max_samples:
            s.sample_errors.append(msg)

    for o in distinct:
        if s.first_published is None:
            s.first_published = o.published_at
        s.last_published = o.published_at

        eid = o.effective_event_id
        if eid is not None:
            if eid in seen_ids:
                s.duplicate_copies += 1
                if "duplicate" in o.chaos:
                    s.chaos_duplicates += 1
            else:
                seen_ids.add(eid)

        if o.malformed:
            s.malformed += 1
            sample(f"{o.message_id}: cuerpo no es JSON válido")
        elif o.missing:
            s.incomplete += 1
            sample(f"{o.message_id}: faltan {', '.join(o.missing)}")

        if o.schema_errors is not None:
            s.schema_checked = True
            if o.schema_errors:
                s.schema_invalid += 1
                sample(f"{o.message_id}: schema: {o.schema_errors[0]}")

        if o.body_ok:
            if o.attr_event_id is not None and o.attr_event_id != o.event_id:
                s.attr_mismatch += 1
                sample(f"{o.message_id}: atributo event_id distinto del cuerpo")
            if o.attr_source is not None and o.attr_source != o.body_source:
                s.attr_mismatch += 1
                sample(f"{o.message_id}: atributo source distinto del cuerpo")
            if o.body_source != source:
                s.source_mismatch += 1
                sample(f"{o.message_id}: source={o.body_source!r} en el topic de {source}")

        if o.seq is not None:
            seqs.add(o.seq)
        if o.kind:
            kinds[o.kind] += 1
        for c in o.chaos:
            chaos[c] += 1

    s.unique_event_ids = len(seen_ids)
    if seqs:
        s.seq_min, s.seq_max = min(seqs), max(seqs)
        s.seq_gaps = (s.seq_max - s.seq_min + 1) - len(seqs)
    s.kinds = dict(kinds)
    s.chaos = dict(chaos)
    return s


# ---------------------------------------------------------------------------
# Veredictos

OK, WARN, FAIL = "ok", "warn", "fail"


@dataclass(frozen=True)
class Check:
    source: str
    name: str
    status: str
    detail: str

    def to_dict(self) -> dict:
        return {"source": self.source, "check": self.name, "status": self.status, "detail": self.detail}


@dataclass(frozen=True)
class Thresholds:
    rate_min: float = 0.35
    rate_max: float = 0.65
    min_messages: int = 10
    min_unique_ratio: float = 0.99
    checkpoint_every: int = 50


def measure_checks(
    s: SourceSummary,
    th: Thresholds,
    expect_chaos: bool = False,
) -> list[Check]:
    """Criterios de HU-017 (tasa, ids únicos, ``seq``) más integridad básica."""
    out: list[Check] = []
    src = s.source

    if s.messages < th.min_messages:
        out.append(Check(src, "mensajes", FAIL,
                         f"{s.messages} mensajes en {s.window_s:.0f} s (mínimo {th.min_messages})"))
        return out  # sin volumen, el resto no es concluyente
    out.append(Check(src, "mensajes", OK, f"{s.messages} mensajes en {s.window_s:.0f} s"))

    rate_ok = th.rate_min <= s.rate <= th.rate_max
    out.append(Check(src, "tasa", OK if rate_ok else FAIL,
                     f"{s.rate:.3f} msg/s (esperado {th.rate_min}–{th.rate_max}, ~0.5)"))

    if s.unique_ratio < th.min_unique_ratio:
        status = FAIL
    elif s.unexpected_duplicates > 0:
        status = WARN
    else:
        status = OK
    detail = (f"{s.unique_event_ids} únicos de {s.messages} ({s.unique_ratio:.2%}); "
              f"copias repetidas {s.duplicate_copies} (de caos {s.chaos_duplicates})")
    out.append(Check(src, "event_id únicos", status, detail))

    bad_body = s.malformed + s.incomplete
    if bad_body:
        out.append(Check(src, "cuerpo", WARN if expect_chaos else FAIL,
                         f"{s.malformed} malformados, {s.incomplete} incompletos"
                         + (" (esperable con caos)" if expect_chaos else "")))
    else:
        out.append(Check(src, "cuerpo", OK, "todos los cuerpos son JSON con event_id, source y seq"))

    if s.schema_checked:
        if s.schema_invalid:
            out.append(Check(src, "schema", WARN if expect_chaos else FAIL,
                             f"{s.schema_invalid} no cumplen envelope.v1.schema.json"))
        else:
            out.append(Check(src, "schema", OK, "todos cumplen envelope.v1.schema.json"))

    if s.attr_mismatch or s.source_mismatch:
        out.append(Check(src, "atributos", FAIL,
                         f"{s.attr_mismatch} atributos incoherentes, {s.source_mismatch} con otra fuente"))
    else:
        out.append(Check(src, "atributos", OK, "atributos y source coherentes con el cuerpo"))

    if s.seq_gaps:
        out.append(Check(src, "seq", WARN,
                         f"seq {s.seq_min}–{s.seq_max} con {s.seq_gaps} huecos "
                         "(mensajes en vuelo al cortar, malformados o perdidos)"))
    else:
        out.append(Check(src, "seq", OK, f"seq {s.seq_min}–{s.seq_max} sin huecos"))
    return out


@dataclass(frozen=True)
class CursorState:
    seed: int
    seq: int
    updated_at: str | None = None

    def to_dict(self) -> dict:
        return {"seed": self.seed, "seq": self.seq, "updated_at": self.updated_at}


def cursor_checks(
    source: str,
    cursor: CursorState | None,
    seq_max: int | None,
    th: Thresholds,
    expected_seed: int | None = None,
) -> list[Check]:
    """El cursor guardado debe ir con lo emitido: a lo sumo ``checkpoint_every`` detrás."""
    if cursor is None:
        return [Check(source, "cursor", WARN, "no se encontró sim-state/{}.json".format(source))]
    out: list[Check] = []
    if expected_seed is not None and cursor.seed != expected_seed:
        out.append(Check(source, "cursor semilla", FAIL,
                         f"cursor con seed={cursor.seed}, se esperaba {expected_seed}"))
    if seq_max is None:
        out.append(Check(source, "cursor", WARN, f"cursor seq={cursor.seq}; sin seq observado para comparar"))
        return out
    # Se guarda cada N mensajes (y al parar); puede ir algo por delante si se leyó tras la ventana.
    lo, hi = seq_max - th.checkpoint_every - 5, seq_max + th.checkpoint_every + 5
    status = OK if lo <= cursor.seq <= hi else WARN
    out.append(Check(source, "cursor", status,
                     f"cursor seq={cursor.seq} (seed={cursor.seed}); último seq observado {seq_max}"))
    return out


def restart_checks(
    source: str,
    before: SourceSummary,
    resumed: SourceSummary,
    after: SourceSummary,
    th: Thresholds,
    observations: Sequence[Observation] = (),
    before_window: tuple[float, float] | None = None,
    resumed_window: tuple[float, float] | None = None,
    cursor_before: CursorState | None = None,
    cursor_after: CursorState | None = None,
) -> list[Check]:
    """El ``seq`` continúa tras el reinicio en lugar de volver a 0 (HU-017, criterio 2).

    ``before``: ventana antes de reiniciar. ``resumed``: transición + después
    (desde que se pidió el reinicio). ``after``: solo la ventana posterior.
    """
    out: list[Check] = []
    tol = th.checkpoint_every
    if before.seq_max is None:
        return [Check(source, "reinicio", FAIL, "sin mensajes antes del reinicio")]
    if after.messages == 0 or resumed.seq_min is None:
        return [Check(source, "reinicio", FAIL, "el servicio no volvió a emitir tras el reinicio")]

    b, a = before.seq_max, resumed.seq_min
    if a < b - tol:
        out.append(Check(source, "reinicio", FAIL,
                         f"seq volvió atrás: antes llegó a {b}, después empezó en {a} (tolerancia {tol})"))
    elif b < tol:
        out.append(Check(source, "reinicio", WARN,
                         f"no concluyente: antes solo llegó a seq {b} (< {tol}); deje correr más el simulador"))
    else:
        out.append(Check(source, "reinicio", OK,
                         f"antes llegó a seq {b}; después reanudó en {a} y siguió hasta {after.seq_max}"))

    if after.seq_max is not None and after.seq_max <= b:
        out.append(Check(source, "progreso", FAIL,
                         f"después del reinicio el seq no superó {b} (máximo {after.seq_max})"))

    if observations and before_window and resumed_window:
        mine = [o for o in observations if o.topic_source == source]
        ids_before = {o.effective_event_id for o in mine if in_window(o, *before_window)}
        ids_before.discard(None)
        repeated = {o.effective_event_id for o in mine
                    if in_window(o, *resumed_window) and o.effective_event_id in ids_before}
        n = len(repeated)
        out.append(Check(source, "repetidos", OK if n <= tol else FAIL,
                         f"{n} event_id repetidos tras el reinicio (máximo esperado {tol}, mismos ids)"))

    if cursor_before is not None and cursor_after is not None:
        if cursor_after.seed != cursor_before.seed:
            out.append(Check(source, "cursor", FAIL,
                             f"la semilla del cursor cambió ({cursor_before.seed} → {cursor_after.seed})"))
        elif cursor_after.seq < cursor_before.seq:
            out.append(Check(source, "cursor", FAIL,
                             f"el cursor retrocedió ({cursor_before.seq} → {cursor_after.seq})"))
        else:
            out.append(Check(source, "cursor", OK,
                             f"cursor {cursor_before.seq} → {cursor_after.seq} (seed {cursor_after.seed})"))
    return out


def verdict(checks: Iterable[Check]) -> str:
    statuses = {c.status for c in checks}
    if FAIL in statuses:
        return FAIL
    if WARN in statuses:
        return WARN
    return OK
