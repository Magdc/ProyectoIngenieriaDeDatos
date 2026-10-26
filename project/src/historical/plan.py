"""Plan del histórico: configuración, reparto en el tiempo y aceptación por tendencias.

Lógica pura (sin simulador, nube ni pyarrow). Todo el azar sale de
``random.Random(f"{seed}|{fuente}|…")`` (§3.6: nunca ``hash()`` de Python), así que el
plan es idéntico entre procesos, máquinas y corridas.

Cómo se arma el dataset de una fuente:

1. ``slot_times()`` reparte sus N registros en las horas de la ventana con una
   intensidad = semana × hora local × crecimiento × ruido diario × marcas (tendencias y
   episodios), y les da un instante dentro de la hora. Salen en orden cronológico.
2. El generador recorre ``seq = 0, 1, 2…`` del simulador. Para cada hueco pide el evento
   del ``seq`` siguiente y lo **acepta** con probabilidad ``acceptance()`` (según su marca
   y su ground truth en ese instante); si no, prueba el ``seq`` siguiente. Así el
   ``seq`` crece con el tiempo, como en el stream, y la mezcla de marcas y sentimiento
   sigue las tendencias del plan sin tocar el simulador.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import math
import random
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

UTC = dt.timezone.utc
SOURCES = ("reddit", "facebook", "instagram")
LABEL_KEYS = ("positive", "negative", "neutral", "null")
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "historical.yaml"
HOUR_MS = 3_600_000


class PlanError(ValueError):
    """Configuración o parámetros del plan inválidos."""


@dataclasses.dataclass(frozen=True)
class EventSpec:
    name: str
    brand: str
    start_days_ago: float
    duration_days: float
    volume: float = 1.0
    sentiment: Mapping[str, float] = dataclasses.field(default_factory=dict)

    def window(self, as_of: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
        start = as_of - dt.timedelta(days=self.start_days_ago)
        return start, start + dt.timedelta(days=self.duration_days)


@dataclasses.dataclass(frozen=True)
class HistoricalConfig:
    seed: int = 3101
    n: int = 50_000
    days: int = 180
    as_of: str | None = None
    chunk_size: int = 2000
    sources: Mapping[str, float] = dataclasses.field(
        default_factory=lambda: {s: 1.0 for s in SOURCES})
    local_utc_offset_hours: float = -5
    weekly: tuple[float, ...] = (1.0,) * 7
    hourly: tuple[float, ...] = (1.0,) * 24
    growth: float = 0.0
    daily_noise: float = 0.0
    brands: Mapping[str, float] = dataclasses.field(default_factory=dict)
    events: tuple[EventSpec, ...] = ()
    max_candidates_per_slot: int = 200

    # ------------------------------------------------------------------ identidad
    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["sources"] = dict(self.sources)
        d["brands"] = dict(self.brands)
        d["weekly"] = list(self.weekly)
        d["hourly"] = list(self.hourly)
        d["events"] = [dict(dataclasses.asdict(e), sentiment=dict(e.sentiment))
                       for e in self.events]
        return d

    def sha256(self) -> str:
        """Huella de todo lo que cambia el dataset (incluye seed, n, days y as_of)."""
        blob = json.dumps(self.to_dict(), sort_keys=True, ensure_ascii=True,
                          separators=(",", ":"))
        return hashlib.sha256(blob.encode("ascii")).hexdigest()

    def replace(self, **changes: Any) -> "HistoricalConfig":
        changes = {k: v for k, v in changes.items() if v is not None}
        cfg = dataclasses.replace(self, **changes)
        validate_config(cfg)
        return cfg


# ---------------------------------------------------------------------- carga

def _num(value: Any, name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise PlanError(f"{name}: se esperaba un número, llegó {value!r}") from exc
    if math.isnan(out) or math.isinf(out):
        raise PlanError(f"{name}: valor no finito")
    return out


def config_from_dict(data: Mapping[str, Any]) -> HistoricalConfig:
    if not isinstance(data, Mapping):
        raise PlanError("la configuración debe ser un objeto")
    known = {f.name for f in dataclasses.fields(HistoricalConfig)}
    unknown = sorted(set(data) - known)
    if unknown:
        raise PlanError(f"claves desconocidas en la configuración: {unknown}")
    kw: dict[str, Any] = {}
    for key in ("seed", "n", "days", "chunk_size", "max_candidates_per_slot"):
        if key in data:
            kw[key] = int(_num(data[key], key))
    for key in ("local_utc_offset_hours", "growth", "daily_noise"):
        if key in data:
            kw[key] = _num(data[key], key)
    if "as_of" in data:
        kw["as_of"] = None if data["as_of"] in (None, "") else str(data["as_of"])
    if "sources" in data:
        kw["sources"] = {str(k): _num(v, f"sources.{k}") for k, v in dict(data["sources"]).items()}
    if "weekly" in data:
        kw["weekly"] = tuple(_num(v, "weekly") for v in data["weekly"])
    if "hourly" in data:
        kw["hourly"] = tuple(_num(v, "hourly") for v in data["hourly"])
    if "brands" in data:
        brands = {}
        for key, spec in dict(data["brands"] or {}).items():
            if isinstance(spec, Mapping):
                extra = set(spec) - {"trend"}
                if extra:
                    raise PlanError(f"brands.{key}: claves desconocidas {sorted(extra)}")
                brands[str(key)] = _num(spec.get("trend", 0.0), f"brands.{key}.trend")
            else:
                brands[str(key)] = _num(spec, f"brands.{key}")
        kw["brands"] = brands
    if "events" in data:
        events = []
        for i, raw in enumerate(data["events"] or []):
            if not isinstance(raw, Mapping):
                raise PlanError(f"events[{i}] debe ser un objeto")
            extra = set(raw) - {"name", "brand", "start_days_ago", "duration_days",
                                "volume", "sentiment"}
            if extra:
                raise PlanError(f"events[{i}]: claves desconocidas {sorted(extra)}")
            sentiment = {str(k): _num(v, f"events[{i}].sentiment.{k}")
                         for k, v in dict(raw.get("sentiment") or {}).items()}
            events.append(EventSpec(
                name=str(raw.get("name") or f"evento_{i}"),
                brand=str(raw["brand"]),
                start_days_ago=_num(raw["start_days_ago"], f"events[{i}].start_days_ago"),
                duration_days=_num(raw["duration_days"], f"events[{i}].duration_days"),
                volume=_num(raw.get("volume", 1.0), f"events[{i}].volume"),
                sentiment=sentiment,
            ))
        kw["events"] = tuple(events)
    cfg = HistoricalConfig(**kw)
    validate_config(cfg)
    return cfg


def load_config(path: str | Path | None = None) -> HistoricalConfig:
    path = Path(path) if path else DEFAULT_CONFIG_PATH
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(text)
    else:
        import yaml  # PyYAML: está en el simulador y en la imagen de NLP

        data = yaml.safe_load(text)
    return config_from_dict(data or {})


def validate_config(cfg: HistoricalConfig) -> None:
    if cfg.n < 1:
        raise PlanError("n debe ser ≥ 1")
    if not 1 <= cfg.days <= 3660:
        raise PlanError("days debe estar entre 1 y 3660")
    if cfg.chunk_size < 1:
        raise PlanError("chunk_size debe ser ≥ 1")
    if cfg.max_candidates_per_slot < 1:
        raise PlanError("max_candidates_per_slot debe ser ≥ 1")
    if not cfg.sources:
        raise PlanError("sources no puede estar vacío")
    for src, w in cfg.sources.items():
        if src not in SOURCES:
            raise PlanError(f"fuente desconocida {src!r} (válidas: {', '.join(SOURCES)})")
        if w <= 0:
            raise PlanError(f"sources.{src} debe ser > 0")
    if len(cfg.weekly) != 7 or any(v <= 0 for v in cfg.weekly):
        raise PlanError("weekly necesita 7 valores > 0 (lunes a domingo)")
    if len(cfg.hourly) != 24 or any(v <= 0 for v in cfg.hourly):
        raise PlanError("hourly necesita 24 valores > 0")
    if not -14 <= cfg.local_utc_offset_hours <= 14:
        raise PlanError("local_utc_offset_hours fuera de rango")
    if cfg.growth <= -1:
        raise PlanError("growth debe ser > -1")
    if not 0 <= cfg.daily_noise <= 1:
        raise PlanError("daily_noise debe estar entre 0 y 1")
    for brand, trend in cfg.brands.items():
        if trend <= -1:
            raise PlanError(f"brands.{brand}.trend debe ser > -1")
    names = set()
    for ev in cfg.events:
        if ev.name in names:
            raise PlanError(f"evento repetido: {ev.name}")
        names.add(ev.name)
        if ev.duration_days <= 0:
            raise PlanError(f"{ev.name}: duration_days debe ser > 0")
        if ev.start_days_ago <= 0:
            raise PlanError(f"{ev.name}: start_days_ago debe ser > 0")
        if ev.volume <= 0:
            raise PlanError(f"{ev.name}: volume debe ser > 0")
        for label, factor in ev.sentiment.items():
            if label not in LABEL_KEYS:
                raise PlanError(f"{ev.name}: sentimiento desconocido {label!r} "
                                f"(válidos: {', '.join(LABEL_KEYS)})")
            if factor <= 0:
                raise PlanError(f"{ev.name}: sentiment.{label} debe ser > 0")
        if cfg.brands and ev.brand not in cfg.brands:
            raise PlanError(f"{ev.name}: la marca {ev.brand!r} no está en brands")


# ---------------------------------------------------------------------- tiempo

def format_ts(value: dt.datetime) -> str:
    """UTC ISO-8601 con milisegundos y ``Z`` (§5.1)."""
    value = value.astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def parse_ts(text: str) -> dt.datetime:
    if not isinstance(text, str) or not text.endswith("Z"):
        raise PlanError(f"timestamp sin Z: {text!r}")
    return dt.datetime.fromisoformat(text[:-1]).replace(tzinfo=UTC)


def resolve_as_of(value: str | dt.datetime | None, now: dt.datetime | None = None) -> dt.datetime:
    """Instante final (exclusivo) de la ventana. ``None`` = hoy a las 00:00 UTC."""
    now = (now or dt.datetime.now(UTC)).astimezone(UTC)
    if value is None or value == "":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if isinstance(value, dt.datetime):
        out = value
    else:
        text = str(value).strip()
        try:
            if len(text) == 10:
                out = dt.datetime.fromisoformat(text)
            else:
                out = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PlanError(f"as_of inválido: {value!r} (use AAAA-MM-DD o ISO-8601)") from exc
    if out.tzinfo is None:
        out = out.replace(tzinfo=UTC)
    out = out.astimezone(UTC).replace(microsecond=0)
    if out > now + dt.timedelta(minutes=1):
        raise PlanError(f"as_of {format_ts(out)} está en el futuro")
    return out


@dataclasses.dataclass(frozen=True)
class Window:
    as_of: dt.datetime
    days: int

    @property
    def start(self) -> dt.datetime:
        return self.as_of - dt.timedelta(days=self.days)

    @property
    def hours(self) -> int:
        return self.days * 24

    def frac(self, when: dt.datetime) -> float:
        total = (self.as_of - self.start).total_seconds()
        return min(1.0, max(0.0, (when - self.start).total_seconds() / total))


def split_counts(n: int, weights: Mapping[str, float]) -> dict[str, int]:
    """Reparte ``n`` según ``weights`` (mayor resto; empates por orden de SOURCES)."""
    keys = [s for s in SOURCES if s in weights]
    total = sum(weights[k] for k in keys)
    raw = {k: n * weights[k] / total for k in keys}
    out = {k: int(math.floor(v)) for k, v in raw.items()}
    rest = n - sum(out.values())
    order = sorted(keys, key=lambda k: (-(raw[k] - out[k]), keys.index(k)))
    for k in order[:rest]:
        out[k] += 1
    return out


# ------------------------------------------------------------- factores del plan

class TrendModel:
    """Factores de marca y sentimiento en cada instante, según la configuración."""

    def __init__(self, cfg: HistoricalConfig, as_of: dt.datetime):
        self.cfg = cfg
        self.window = Window(as_of, cfg.days)
        self._events = [(ev, *ev.window(as_of)) for ev in cfg.events]

    def active_events(self, when: dt.datetime) -> list[EventSpec]:
        return [ev for ev, start, end in self._events if start <= when < end]

    def brand_factor(self, brand: str | None, when: dt.datetime) -> float:
        if brand is None:
            return 1.0
        f = max(0.05, 1.0 + self.cfg.brands.get(brand, 0.0) * self.window.frac(when))
        for ev in self.active_events(when):
            if ev.brand == brand:
                f *= ev.volume
        return f

    def sentiment_factor(self, brand: str | None, label: str | None, when: dt.datetime) -> float:
        key = "null" if label is None else str(label)
        f = 1.0
        for ev in self.active_events(when):
            if ev.brand == brand:
                f *= ev.sentiment.get(key, 1.0)
        return f

    def _max_score(self, when: dt.datetime) -> float:
        best = 1.0
        for brand in self.cfg.brands or {}:
            bf = self.brand_factor(brand, when)
            best = max(best, max(bf * self.sentiment_factor(brand, lab, when)
                                 for lab in (*LABEL_KEYS[:3], None)))
        return best

    def acceptance(self, brand: str | None, label: str | None, when: dt.datetime) -> float:
        """Probabilidad de aceptar un candidato con esa marca y ground truth."""
        score = self.brand_factor(brand, when) * self.sentiment_factor(brand, label, when)
        return min(1.0, score / self._max_score(when))

    def volume_factor(self, when: dt.datetime) -> float:
        """Media de los factores de marca (mezcla uniforme aproximada del simulador)."""
        brands = list(self.cfg.brands or {})
        if not brands:
            return 1.0
        return sum(self.brand_factor(b, when) for b in brands) / len(brands)

    def intensity(self, source: str, hour_index: int) -> float:
        cfg = self.cfg
        start = self.window.start + dt.timedelta(hours=hour_index)
        mid = start + dt.timedelta(minutes=30)
        local = start + dt.timedelta(hours=cfg.local_utc_offset_hours)
        value = cfg.weekly[local.weekday()] * cfg.hourly[local.hour]
        value *= 1.0 + cfg.growth * self.window.frac(mid)
        value *= self.volume_factor(mid)
        if cfg.daily_noise > 0:
            day = hour_index // 24
            rng = random.Random(f"{cfg.seed}|{source}|hist-day|{day}")
            sigma = cfg.daily_noise
            value *= math.exp(rng.gauss(0.0, sigma) - sigma * sigma / 2)
        return value


def accept_draw(seed: int, source: str, seq: int) -> float:
    """Número uniforme del candidato ``seq`` (independiente del Random del evento)."""
    return random.Random(f"{seed}|{source}|{seq}|hist-accept").random()


def slot_times(model: TrendModel, source: str, count: int) -> list[dt.datetime]:
    """``count`` instantes en orden cronológico, repartidos según ``intensity``.

    Reparto sistemático (suma exacta, poca varianza) por hora y, dentro de cada hora,
    instantes uniformes con milisegundos.
    """
    cfg = model.cfg
    hours = model.window.hours
    if count <= 0:
        return []
    weights = [model.intensity(source, h) for h in range(hours)]
    total = sum(weights)
    u = random.Random(f"{cfg.seed}|{source}|hist-alloc").random()
    out: list[dt.datetime] = []
    cum = 0.0
    prev = math.floor(u)
    start = model.window.start
    for h, w in enumerate(weights):
        cum += count * w / total
        cur = math.floor(cum + u) if h < hours - 1 else count + math.floor(u)
        k = int(cur - prev)
        prev = cur
        if k <= 0:
            continue
        rng = random.Random(f"{cfg.seed}|{source}|hist-hour|{h}")
        offsets = sorted(rng.randrange(HOUR_MS) for _ in range(k))
        base = start + dt.timedelta(hours=h)
        out.extend(base + dt.timedelta(milliseconds=ms) for ms in offsets)
    if len(out) != count:  # no debería pasar; protege el contrato de N exacto
        raise PlanError(f"reparto inconsistente: {len(out)} != {count}")
    return out


# ------------------------------------------------------------------- resumen

def plan_summary(cfg: HistoricalConfig, as_of: dt.datetime) -> dict[str, Any]:
    model = TrendModel(cfg, as_of)
    counts = split_counts(cfg.n, cfg.sources)
    events = []
    for ev in cfg.events:
        start, end = ev.window(as_of)
        events.append({"name": ev.name, "brand": ev.brand, "start": format_ts(start),
                       "end": format_ts(end), "volume": ev.volume,
                       "sentiment": dict(ev.sentiment)})
    return {
        "seed": cfg.seed,
        "n": cfg.n,
        "days": cfg.days,
        "as_of": format_ts(as_of),
        "start": format_ts(model.window.start),
        "sources": counts,
        "chunk_size": cfg.chunk_size,
        "config_sha256": cfg.sha256(),
        "events": events,
        "brands": dict(cfg.brands),
    }


def expected_profiles(cfg: HistoricalConfig) -> dict[str, list[float]]:
    """Perfiles semanal y horario configurados (para comparar con lo generado)."""
    return {"weekly": list(cfg.weekly), "hourly": list(cfg.hourly)}


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(sxx * syy)


def iter_hours(window: Window) -> Iterable[dt.datetime]:
    for h in range(window.hours):
        yield window.start + dt.timedelta(hours=h)
