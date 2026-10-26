"""Lógica pura de las ventanas de tendencias (Lote 2.4, HU-023).

Sin Beam: la usan la rama de Dataflow (``streaming.trends``) y los verificadores
(``streaming.trends_check``), que recalculan las ventanas desde ``enriched/`` o
``posts_enriched`` con este mismo código.

Contrato de salida: ``trend_windows`` (§5.5). Reglas:

* Ventanas deslizantes de ``window_seconds`` (300) con paso ``period_seconds`` (60),
  alineadas a múltiplos del paso, sobre ``event_time`` (UTC).
* Una fila por ventana × ``brand`` × ``source``; ``brand`` = ``brand_primary``.
* ``avg_sentiment``: media con signo en [-1, 1] (positivo +score, negativo -score,
  neutro 0) sobre los posts con sentimiento.
* z-score frente a las ``baseline_windows`` ventanas anteriores de la misma clave.
  Las ventanas sin posts cuentan como volumen 0. Las primeras ventanas de la clave
  (incompletas porque el job acaba de arrancar) no entran en el baseline.
* ``is_alert`` solo con baseline completo (60 ventanas, §5.12), al menos
  ``min_posts`` posts y ``|z| >= z_threshold`` en volumen o sentimiento, y nunca en una
  ventana emitida antes de terminar en tiempo real (drenado del job, v2.26).
"""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

EMOTION_LABELS: tuple[str, ...] = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
SENTIMENT_LABELS: tuple[str, ...] = ("negative", "neutral", "positive")
SOURCES: tuple[str, ...] = ("reddit", "facebook", "instagram")

TREND_COLUMNS: tuple[str, ...] = (
    "window_start", "window_end", "brand", "source", "n_posts", "n_negative", "n_positive",
    "avg_sentiment", "emotion_counts", "zscore_volume", "zscore_sentiment", "is_alert",
)

# Etiquetas crudas que pudieran colarse (el contrato ya las mapea en el Lote 2.3).
_RAW_EMOTION = {"others": "neutral"}
_RAW_SENTIMENT = {"neg": "negative", "neu": "neutral", "pos": "positive"}

KEY_SEPARATOR = "|"


# --------------------------------------------------------------------------- configuración

@dataclass(frozen=True)
class TrendConfig:
    """Parámetros de las ventanas (§5.12). Todos tienen opción del pipeline."""

    window_seconds: int = 300
    period_seconds: int = 60
    allowed_lateness_seconds: int = 1800
    late_delay_seconds: int = 60
    baseline_windows: int = 60
    z_threshold: float = 3.0
    min_posts: int = 5
    volume_std_floor: float = 1.0
    sentiment_std_floor: float = 0.05

    def __post_init__(self) -> None:
        problems = self.problems()
        if problems:
            raise ValueError("configuración de tendencias inválida: " + "; ".join(problems))

    def problems(self) -> list[str]:
        out = []
        if self.window_seconds <= 0 or self.period_seconds <= 0:
            out.append("window_seconds y period_seconds deben ser > 0")
        elif self.window_seconds % self.period_seconds:
            out.append("window_seconds debe ser múltiplo de period_seconds")
        if self.allowed_lateness_seconds < 0:
            out.append("allowed_lateness_seconds debe ser >= 0")
        if self.late_delay_seconds < 0:
            out.append("late_delay_seconds debe ser >= 0")
        if self.baseline_windows < 2:
            out.append("baseline_windows debe ser >= 2")
        if not self.z_threshold > 0:
            out.append("z_threshold debe ser > 0")
        if self.min_posts < 0:
            out.append("min_posts debe ser >= 0")
        if self.volume_std_floor <= 0 or self.sentiment_std_floor <= 0:
            out.append("los pisos de desviación deben ser > 0")
        return out

    @property
    def window_ms(self) -> int:
        return self.window_seconds * 1000

    @property
    def period_ms(self) -> int:
        return self.period_seconds * 1000

    @property
    def lateness_ms(self) -> int:
        return self.allowed_lateness_seconds * 1000


# --------------------------------------------------------------------------- tiempo

def to_epoch_ms(value: Any) -> int | None:
    """Convierte un timestamp (ISO-8601, ``datetime`` o número) a milisegundos UTC.

    Números: segundos si son menores que 1e11, milisegundos si no. Un ``datetime``
    sin zona se toma como UTC. Devuelve ``None`` si no se puede interpretar.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(round(dt.timestamp() * 1000))
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            return None
        return int(round(value * 1000)) if abs(value) < 1e11 else int(round(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        return to_epoch_ms(dt)
    # Timestamp de Beam u otros con micros
    micros = getattr(value, "micros", None)
    if isinstance(micros, int):
        return micros // 1000
    return None


def format_ts(epoch_ms: int) -> str:
    """UTC con milisegundos y ``Z`` (§5.1): ``2026-10-22T19:05:00.000Z``."""
    epoch_ms = int(round(epoch_ms))
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{epoch_ms % 1000:03d}Z"


def window_starts(event_ms: int, cfg: TrendConfig) -> list[int]:
    """Inicios de las ventanas deslizantes que contienen ``event_ms`` (orden ascendente).

    Igual que ``SlidingWindows(size, period)`` de Beam con offset 0.
    """
    last = (event_ms // cfg.period_ms) * cfg.period_ms
    count = cfg.window_ms // cfg.period_ms
    return [last - k * cfg.period_ms for k in range(count - 1, -1, -1)]


# --------------------------------------------------------------------------- filas de entrada

def make_key(brand: str, source: str) -> str:
    return f"{brand}{KEY_SEPARATOR}{source}"


def split_key(key: str) -> tuple[str, str]:
    brand, _, source = key.partition(KEY_SEPARATOR)
    return brand, source


@dataclass(frozen=True)
class TrendFeature:
    """Lo que la ventana necesita de un post enriquecido."""

    brand: str
    source: str
    event_ms: int
    sentiment_label: str | None = None
    sentiment_score: float | None = None
    emotion_label: str | None = None

    @property
    def key(self) -> str:
        return make_key(self.brand, self.source)

    def as_tuple(self) -> tuple:
        return (self.event_ms, self.sentiment_label, self.sentiment_score, self.emotion_label)


def normalize_sentiment(label: Any) -> str | None:
    if not isinstance(label, str):
        return None
    text = label.strip().lower()
    text = _RAW_SENTIMENT.get(text, text)
    return text if text in SENTIMENT_LABELS else None


def normalize_emotion(label: Any) -> str | None:
    if not isinstance(label, str):
        return None
    text = label.strip().lower()
    text = _RAW_EMOTION.get(text, text)
    return text if text in EMOTION_LABELS else None


def normalize_score(score: Any) -> float | None:
    if score is None or isinstance(score, bool):
        return None
    try:
        value = float(score)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0.0 or value > 1.0:
        return None
    return value


def signed_sentiment(label: str | None, score: float | None) -> float | None:
    """Valor con signo de un post: +score, -score o 0 (neutro). ``None`` si no hay dato."""
    if label == "neutral":
        return 0.0
    if label == "positive" and score is not None:
        return score
    if label == "negative" and score is not None:
        return -score
    return None


def feature_from_row(row: Any) -> tuple[TrendFeature | None, str | None]:
    """Extrae un ``TrendFeature`` de una fila de ``posts_enriched`` (dict plano, §5.5).

    Devuelve ``(feature, None)`` o ``(None, motivo)`` con motivo en
    ``unrecognized`` · ``no_event_time`` · ``no_brand`` · ``no_source``.
    """
    if not isinstance(row, Mapping):
        return None, "unrecognized"
    if "event_time" not in row and "event_id" not in row:
        return None, "unrecognized"
    event_ms = to_epoch_ms(row.get("event_time"))
    if event_ms is None:
        return None, "no_event_time"
    brand = row.get("brand_primary")
    if brand is None and "brand_primary" not in row:
        brands = row.get("brands") or []
        brand = brands[0] if isinstance(brands, (list, tuple)) and brands else None
    if not isinstance(brand, str) or not brand:
        return None, "no_brand"
    source = row.get("source")
    if not isinstance(source, str) or not source:
        return None, "no_source"
    return (
        TrendFeature(
            brand=brand,
            source=source,
            event_ms=event_ms,
            sentiment_label=normalize_sentiment(row.get("sentiment_label")),
            sentiment_score=normalize_score(row.get("sentiment_score")),
            emotion_label=normalize_emotion(row.get("emotion_label")),
        ),
        None,
    )


# --------------------------------------------------------------------------- acumulador

def new_accumulator() -> dict:
    return {"n": 0, "neg": 0, "pos": 0, "neu": 0, "ssum": 0.0, "sn": 0,
            "emo": {label: 0 for label in EMOTION_LABELS}}


def add_feature(acc: dict, feature: tuple | TrendFeature) -> dict:
    """Suma un post (``TrendFeature`` o su ``as_tuple()``) al acumulador."""
    if isinstance(feature, TrendFeature):
        feature = feature.as_tuple()
    _event_ms, label, score, emotion = feature
    acc["n"] += 1
    if label == "negative":
        acc["neg"] += 1
    elif label == "positive":
        acc["pos"] += 1
    elif label == "neutral":
        acc["neu"] += 1
    signed = signed_sentiment(label, score)
    if signed is not None:
        acc["ssum"] += signed
        acc["sn"] += 1
    if emotion in acc["emo"]:
        acc["emo"][emotion] += 1
    return acc


def merge_accumulators(accs: Iterable[dict]) -> dict:
    out = new_accumulator()
    for acc in accs:
        out["n"] += acc["n"]
        out["neg"] += acc["neg"]
        out["pos"] += acc["pos"]
        out["neu"] += acc["neu"]
        out["ssum"] += acc["ssum"]
        out["sn"] += acc["sn"]
        for label, count in acc["emo"].items():
            out["emo"][label] = out["emo"].get(label, 0) + count
    return out


@dataclass(frozen=True)
class WindowAggregate:
    """Agregado de una ventana para una marca y una fuente."""

    brand: str
    source: str
    window_start_ms: int
    window_end_ms: int
    n_posts: int
    n_negative: int
    n_positive: int
    n_neutral: int
    sentiment_sum: float
    sentiment_n: int
    emotion_counts: Mapping[str, int] = field(default_factory=dict)
    # False si se emitió antes de que la ventana terminara en tiempo real: pasa cuando se drena
    # el job (la marca de agua salta al infinito y cierra las ventanas con datos parciales).
    complete: bool = True

    @property
    def key(self) -> str:
        return make_key(self.brand, self.source)

    @property
    def avg_sentiment(self) -> float | None:
        if self.sentiment_n <= 0:
            return None
        value = self.sentiment_sum / self.sentiment_n
        return round(max(-1.0, min(1.0, value)), 6)

    @classmethod
    def from_accumulator(cls, key: str, start_ms: int, end_ms: int, acc: Mapping,
                         complete: bool = True) -> "WindowAggregate":
        brand, source = split_key(key)
        return cls(
            brand=brand, source=source, window_start_ms=start_ms, window_end_ms=end_ms,
            n_posts=int(acc["n"]), n_negative=int(acc["neg"]), n_positive=int(acc["pos"]),
            n_neutral=int(acc.get("neu", 0)), sentiment_sum=float(acc["ssum"]),
            sentiment_n=int(acc["sn"]),
            emotion_counts={label: int(acc["emo"].get(label, 0)) for label in EMOTION_LABELS},
            complete=complete,
        )


def expected_windows(features: Iterable[TrendFeature], cfg: TrendConfig) -> dict[tuple[int, str, str], WindowAggregate]:
    """Recalcula todas las ventanas de un conjunto de posts (lo usan los verificadores)."""
    accs: dict[tuple[int, str], dict] = {}
    for feature in features:
        for start in window_starts(feature.event_ms, cfg):
            acc = accs.setdefault((start, feature.key), new_accumulator())
            add_feature(acc, feature)
    out = {}
    for (start, key), acc in accs.items():
        agg = WindowAggregate.from_accumulator(key, start, start + cfg.window_ms, acc)
        out[(start, agg.brand, agg.source)] = agg
    return out


# --------------------------------------------------------------------------- baseline y alertas

@dataclass(frozen=True)
class TrendScore:
    zscore_volume: float | None
    zscore_sentiment: float | None
    is_alert: bool
    baseline_available: int
    warm: bool


def _zscore(value: float, sample: list[float], floor: float) -> float | None:
    if len(sample) < 2:
        return None
    mean = statistics.fmean(sample)
    std = statistics.stdev(sample)
    z = (value - mean) / max(std, floor)
    return round(z, 6)


def score_window(state: Mapping | None, agg: WindowAggregate, cfg: TrendConfig) -> tuple[dict, TrendScore]:
    """Actualiza el baseline de una clave con ``agg`` y calcula sus z-scores.

    ``state`` es un dict plano (se guarda en el estado de Beam):
    ``{"hist": {inicio_ms: [n_posts, avg_sentiment]}, "min_start": ..., "max_start": ...}``.
    Una ventana que vuelve a llegar (panel tardío) reemplaza su versión anterior.
    """
    hist = {int(k): list(v) for k, v in (state or {}).get("hist", {}).items()}
    min_start = (state or {}).get("min_start")
    max_start = (state or {}).get("max_start")

    start = agg.window_start_ms
    hist[start] = [agg.n_posts, agg.avg_sentiment]
    min_start = start if min_start is None else min(min_start, start)
    max_start = start if max_start is None else max(max_start, start)

    # La primera ventana completa empieza una ventana después de la primera vista:
    # las anteriores solo tienen los posts procesados desde que arrancó el job.
    valid_from = min_start + cfg.window_ms
    baseline = [start - k * cfg.period_ms for k in range(1, cfg.baseline_windows + 1)]
    baseline = [b for b in baseline if b >= valid_from]
    volume_sample = [float(hist.get(b, [0, None])[0]) for b in baseline]
    sentiment_sample = [float(hist[b][1]) for b in baseline if b in hist and hist[b][1] is not None]

    z_volume = _zscore(float(agg.n_posts), volume_sample, cfg.volume_std_floor)
    avg = agg.avg_sentiment
    z_sentiment = _zscore(avg, sentiment_sample, cfg.sentiment_std_floor) if avg is not None else None

    warm = start >= valid_from and len(baseline) >= cfg.baseline_windows
    exceeded = any(z is not None and abs(z) >= cfg.z_threshold for z in (z_volume, z_sentiment))
    # Una ventana incompleta (drenado) tiene menos posts de los reales: su caída de volumen es falsa.
    is_alert = bool(warm and agg.complete and agg.n_posts >= cfg.min_posts and exceeded)

    keep_from = max_start - (cfg.baseline_windows * cfg.period_ms + cfg.lateness_ms + cfg.window_ms)
    hist = {k: v for k, v in hist.items() if k >= keep_from}
    new_state = {"hist": hist, "min_start": min_start, "max_start": max_start}
    return new_state, TrendScore(z_volume, z_sentiment, is_alert, len(baseline), warm)


# --------------------------------------------------------------------------- fila de salida

def emotion_counts_json(counts: Mapping[str, int]) -> str:
    """JSON compacto con las 7 emociones en el orden de §3.3."""
    ordered = {label: int(counts.get(label, 0)) for label in EMOTION_LABELS}
    return json.dumps(ordered, separators=(",", ":"))


def trend_row(agg: WindowAggregate, score: TrendScore) -> dict:
    """Fila de ``trend_windows`` con exactamente las columnas de §5.5, en orden."""
    return {
        "window_start": format_ts(agg.window_start_ms),
        "window_end": format_ts(agg.window_end_ms),
        "brand": agg.brand,
        "source": agg.source,
        "n_posts": agg.n_posts,
        "n_negative": agg.n_negative,
        "n_positive": agg.n_positive,
        "avg_sentiment": agg.avg_sentiment,
        "emotion_counts": emotion_counts_json(agg.emotion_counts),
        "zscore_volume": score.zscore_volume,
        "zscore_sentiment": score.zscore_sentiment,
        "is_alert": score.is_alert,
    }


def parse_emotion_counts(value: Any) -> dict | None:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def row_problems(row: Mapping, cfg: TrendConfig | None = None) -> list[str]:
    """Reglas de una fila de ``trend_windows`` (las aplican los verificadores)."""
    cfg = cfg or TrendConfig()
    problems: list[str] = []
    keys = set(row.keys())
    if keys != set(TREND_COLUMNS):
        missing = sorted(set(TREND_COLUMNS) - keys)
        extra = sorted(keys - set(TREND_COLUMNS))
        problems.append(f"columnas: faltan {missing}, sobran {extra}")
    start = to_epoch_ms(row.get("window_start"))
    end = to_epoch_ms(row.get("window_end"))
    if start is None or end is None:
        problems.append("window_start/window_end ilegibles")
    else:
        if end - start != cfg.window_ms:
            problems.append(f"duración {(end - start) / 1000:.0f} s (esperado {cfg.window_seconds})")
        if start % cfg.period_ms:
            problems.append("window_start no alineado al paso")
    brand = row.get("brand")
    if not isinstance(brand, str) or not brand or not all(c.islower() or c.isdigit() or c == "_" for c in brand):
        problems.append(f"brand no canónica: {brand!r}")
    if row.get("source") not in SOURCES:
        problems.append(f"source inválida: {row.get('source')!r}")
    counts = {}
    for name in ("n_posts", "n_negative", "n_positive"):
        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            problems.append(f"{name} inválido: {value!r}")
        else:
            counts[name] = value
    if len(counts) == 3:
        if counts["n_posts"] < 1:
            problems.append("n_posts = 0")
        if counts["n_negative"] + counts["n_positive"] > counts["n_posts"]:
            problems.append("n_negative + n_positive > n_posts")
    avg = row.get("avg_sentiment")
    if avg is not None and (isinstance(avg, bool) or not isinstance(avg, (int, float)) or not -1.0 <= avg <= 1.0):
        problems.append(f"avg_sentiment fuera de [-1, 1]: {avg!r}")
    emotions = parse_emotion_counts(row.get("emotion_counts"))
    if emotions is None:
        problems.append("emotion_counts no es un objeto JSON")
    else:
        if set(emotions) != set(EMOTION_LABELS):
            problems.append(f"emotion_counts con claves {sorted(emotions)}")
        values = list(emotions.values())
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in values):
            problems.append("emotion_counts con valores no enteros o negativos")
        elif "n_posts" in counts and sum(values) > counts["n_posts"]:
            problems.append("emotion_counts suma más que n_posts")
    for name in ("zscore_volume", "zscore_sentiment"):
        value = row.get(name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)):
            problems.append(f"{name} inválido: {value!r}")
    if not isinstance(row.get("is_alert"), bool):
        problems.append(f"is_alert no es booleano: {row.get('is_alert')!r}")
    return problems


def latest_versions(rows: Iterable[Mapping]) -> dict[tuple[int, str, str], Mapping]:
    """Una fila por ventana × marca × fuente: la de mayor ``n_posts`` (paneles acumulativos).

    Es la misma regla de la vista ``trend_windows_latest`` (§5.5).
    """
    out: dict[tuple[int, str, str], Mapping] = {}
    for row in rows:
        start = to_epoch_ms(row.get("window_start"))
        if start is None:
            continue
        key = (start, row.get("brand"), row.get("source"))
        current = out.get(key)
        if current is None or _version_rank(row) > _version_rank(current):
            out[key] = row
    return out


def _version_rank(row: Mapping) -> tuple:
    def as_int(v):
        return v if isinstance(v, int) and not isinstance(v, bool) else -1
    return (as_int(row.get("n_posts")), as_int(row.get("n_negative")), as_int(row.get("n_positive")))
