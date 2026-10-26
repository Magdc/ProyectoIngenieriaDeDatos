"""Acumuladores del *profiling* por columna y por grupo (lógica pura, sin pyarrow).

Un **grupo** es ``(capa, origin, source)``; ``"*"`` significa "todos". El profiler alimenta cada
fila a tres grupos: el de la capa entera, el del origen y el de origen × fuente (§3.20.1).

Qué mide cada columna (``ColumnAcc.result()``): filas, nulos, distintos (exacto), tipo lógico y,
según el tipo, valores más frecuentes, cuantiles, longitudes, rango de fechas e histogramas por
hora y día de la semana (UTC), o largo de las listas y sus elementos. Las columnas sensibles
(``layers.SENSITIVE_COLUMNS``) nunca escriben valores: solo cardinalidad y un histograma de
frecuencias (cuántos valores aparecen 1, 2, 3–5… veces).
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

from quality import layers as L
from quality.checks import RowMeta, as_utc, checks_for
from quality.text_stats import Skeletonizer, TextDiversity, _frequency_histogram, digest, tokens

TOP_N = 5
ALL_VALUES_UP_TO = 12  # con hasta 12 valores distintos se escriben todos (enums, marcas, emociones)
TOP_LIST_ELEMENTS = 10
QUANTILES = (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)
ALL = "*"


def quantile(sorted_values: list[float], q: float) -> float | None:
    """Cuantil con interpolación lineal (la misma de ``numpy.quantile`` por defecto)."""
    if not sorted_values:
        return None
    pos = (len(sorted_values) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def _r(x: float | None, nd: int = 4) -> float | None:
    return None if x is None else round(float(x), nd)


def numeric_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    vals = sorted(values)
    n = len(vals)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / n if n > 1 else 0.0
    q1, q3 = quantile(vals, 0.25), quantile(vals, 0.75)
    iqr = (q3 - q1) if q1 is not None and q3 is not None else 0
    out = {"count": n, "min": _r(vals[0]), "max": _r(vals[-1]), "mean": _r(mean), "std": _r(math.sqrt(var))}
    for q in QUANTILES:
        out[f"p{int(q * 100):02d}"] = _r(quantile(vals, q))
    out["zeros"] = sum(1 for v in vals if v == 0)
    out["negatives"] = sum(1 for v in vals if v < 0)
    out["outliers_iqr"] = sum(1 for v in vals if v < q1 - 1.5 * iqr or v > q3 + 1.5 * iqr)
    return out


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class ColumnAcc:
    def __init__(self, name: str, kind: str, *, sensitive: bool = False, daily: bool = False):
        self.name, self.kind, self.sensitive, self.daily = name, kind, sensitive, daily
        self.rows = 0
        self.nulls = 0
        self.counter: Counter = Counter()
        self.values: list[float] = []
        self.lengths: list[int] = []
        self.empty = 0
        self.by_hour = [0] * 24
        self.by_weekday = [0] * 7
        self.days: Counter[str] = Counter()
        self.ts_min: datetime | None = None
        self.ts_max: datetime | None = None
        self.list_len: Counter[str] = Counter()
        self.elements: Counter[str] = Counter()
        self.combos: Counter[tuple] = Counter()
        self.types: Counter[str] = Counter()

    def add(self, v: Any) -> None:
        self.rows += 1
        if v is None:
            self.nulls += 1
            return
        self.types[type(v).__name__] += 1
        k = self.kind
        if k in ("string", "json"):
            s = v if isinstance(v, str) else str(v)
            self.counter[digest(s) if self.sensitive else s] += 1
            self.lengths.append(len(s))
            if not s.strip():
                self.empty += 1
        elif k in ("int", "float"):
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                return
            self.values.append(float(v))
            self.counter[v] += 1
        elif k == "timestamp":
            ts = as_utc(v)
            if ts is None:
                return
            self.counter[int(ts.timestamp() * 1000)] += 1
            self.by_hour[ts.hour] += 1
            self.by_weekday[ts.weekday()] += 1
            if self.daily:
                self.days[ts.strftime("%Y-%m-%d")] += 1
            if self.ts_min is None or ts < self.ts_min:
                self.ts_min = ts
            if self.ts_max is None or ts > self.ts_max:
                self.ts_max = ts
        elif k == "list":
            items = list(v)
            n = len(items)
            self.list_len["3+" if n >= 3 else str(n)] += 1
            self.elements.update(str(x) for x in items)
            self.combos[tuple(str(x) for x in items)] += 1
            self.counter[tuple(items)] += 1

    def result(self) -> dict[str, Any]:
        non_null = self.rows - self.nulls
        distinct = len(self.counter)
        out: dict[str, Any] = {
            "kind": self.kind,
            "rows": self.rows,
            "nulls": self.nulls,
            "null_ratio": _r(self.nulls / self.rows, 6) if self.rows else None,
            "non_null": non_null,
            "distinct": distinct,
            "distinct_ratio": _r(distinct / non_null, 6) if non_null else None,
            "python_types": dict(self.types),
        }
        counts = self.counter.values()
        out["max_frequency"] = max(counts, default=0)
        if self.sensitive:
            out["frequency_histogram"] = _frequency_histogram(counts)
        elif self.kind in ("string", "int"):
            limit = distinct if distinct <= ALL_VALUES_UP_TO else TOP_N
            out["top"] = [
                {"value": val, "count": c, "ratio": _r(c / non_null, 5)}
                for val, c in sorted(self.counter.items(), key=lambda kv: (-kv[1], str(kv[0])))[:limit]
            ]
            out["top_complete"] = distinct <= ALL_VALUES_UP_TO
        if self.kind in ("string", "json"):
            out["length"] = numeric_summary([float(x) for x in self.lengths])
            out["length"]["empty"] = self.empty
        elif self.kind in ("int", "float"):
            out["numeric"] = numeric_summary(self.values)
        elif self.kind == "timestamp":
            out["timestamp"] = {
                "min": _iso(self.ts_min) if self.ts_min else None,
                "max": _iso(self.ts_max) if self.ts_max else None,
                "span_days": _r((self.ts_max - self.ts_min).total_seconds() / 86400, 3) if self.ts_min and self.ts_max else None,
                "by_hour_utc": self.by_hour,
                "by_weekday_utc": self.by_weekday,  # 0 = lunes
            }
            if self.daily:
                out["timestamp"]["days"] = len(self.days)
                out["timestamp"]["daily"] = dict(sorted(self.days.items()))
        elif self.kind == "list":
            out["list"] = {
                "length_histogram": {k: self.list_len.get(k, 0) for k in ("0", "1", "2", "3+")},
                "elements_distinct": len(self.elements),
                "elements_top": [{"value": e, "count": c} for e, c in
                                 sorted(self.elements.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_LIST_ELEMENTS]],
                "combos_top": [{"value": list(t), "count": c} for t, c in
                               sorted(self.combos.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_N]],
            }
        return out


class GroupAcc:
    """Perfil de un grupo (capa, origin, source)."""

    def __init__(self, layer: str, origin: str, source: str, *, skeletonizer: Skeletonizer | None = None):
        self.layer, self.origin, self.source = layer, origin, source
        spec = L.layer_spec(layer)
        self.columns_order = spec.columns
        cols = list(spec.columns)
        if layer == "trusted":
            cols.append("source")  # partición: se perfila igual que una columna
        self.cols: dict[str, ColumnAcc] = {
            c: ColumnAcc(c, L.COLUMN_KINDS[c], sensitive=c in L.SENSITIVE_COLUMNS, daily=(c == "event_time" and source == ALL))
            for c in cols
        }
        self.derived: dict[str, ColumnAcc] = {
            "text_length": ColumnAcc("text_length", "int"),
            "text_tokens": ColumnAcc("text_tokens", "int"),
            "ingest_lag_s": ColumnAcc("ingest_lag_s", "float"),
            "latency_s": ColumnAcc("latency_s", "float"),
            "partition_offset_s": ColumnAcc("partition_offset_s", "float"),
            "payload_kind": ColumnAcc("payload_kind", "string"),
            "payload_keys": ColumnAcc("payload_keys", "string"),
        }
        self.checks = {c.id: [0, 0] for c in checks_for(layer)}  # id → [evaluated, failed]
        self.check_list = checks_for(layer)
        self.text = TextDiversity(skeletonizer)
        self.text_clean = TextDiversity(skeletonizer) if layer == "trusted" else None
        self.event_ids: Counter[str] = Counter()
        self.users: Counter[int] = Counter()
        self.rows = 0
        self.files = 0
        self.bytes = 0
        self.rows_per_file: list[float] = []
        self.bytes_per_file: list[float] = []
        self.agree = [0, 0]  # [con label_sentiment, coincide]
        self.confusion: Counter[tuple[str, str]] = Counter()
        self.key_non_null = [0, 0]  # [celdas evaluadas, no nulas] de KEY_FIELDS
        self.anon_ok = 0
        # Por partición dt (solo en los grupos de origen): filas y filas sin NLP.
        self.track_dt = origin != ALL and source == ALL
        self.by_dt: dict[str, list[int]] = {}

    @property
    def key(self) -> dict[str, str]:
        return {"layer": self.layer, "origin": self.origin, "source": self.source}

    def add_file(self, rows: int, size: int) -> None:
        self.files += 1
        self.bytes += size
        self.rows_per_file.append(float(rows))
        self.bytes_per_file.append(float(size))

    def add_row(self, row: dict, meta: RowMeta, payload_obj: Any, check_results: dict[str, bool | None]) -> None:
        self.rows += 1
        for name, acc in self.cols.items():
            acc.add(meta.source if (name == "source" and self.layer == "trusted") else row.get(name))
        text = row.get("text")
        if isinstance(text, str):
            self.derived["text_length"].add(len(text))
            self.derived["text_tokens"].add(len(tokens(text)))
        self.text.add(text if isinstance(text, str) else None)
        if self.text_clean is not None:
            tc = row.get("text_clean")
            self.text_clean.add(tc if isinstance(tc, str) else None)
        e, i, p = as_utc(row.get("event_time")), as_utc(row.get("ingest_time")), as_utc(row.get("process_time"))
        if e and i:
            self.derived["ingest_lag_s"].add((i - e).total_seconds())
        if e and p:
            self.derived["latency_s"].add((p - e).total_seconds())
        if meta.dt and meta.hr and self.layer == "raw":
            hour_start = datetime.strptime(f"{meta.dt} {meta.hr}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
            ref = p if meta.origin == "stream" else e
            if ref:
                self.derived["partition_offset_s"].add((ref - hour_start).total_seconds())
        if isinstance(payload_obj, dict):
            self.derived["payload_kind"].add(str(payload_obj.get("kind")))
            self.derived["payload_keys"].add(",".join(sorted(payload_obj)))
        for cid, res in check_results.items():
            if res is None or cid not in self.checks:
                continue
            slot = self.checks[cid]
            slot[0] += 1
            if not res:
                slot[1] += 1
        eid = row.get("event_id")
        if isinstance(eid, str):
            self.event_ids[eid] += 1
        uh = row.get("user_hash")
        if isinstance(uh, str):
            self.users[digest(uh)] += 1
        for f in L.KEY_FIELDS:
            value = meta.source if (f == "source" and self.layer == "trusted") else row.get(f)
            self.key_non_null[0] += 1
            self.key_non_null[1] += value is not None
        if check_results.get("regex:user_hash") is True and check_results.get("payload:no_identity") is True:
            self.anon_ok += 1
        if self.track_dt and meta.dt:
            slot = self.by_dt.setdefault(meta.dt, [0, 0])
            slot[0] += 1
            slot[1] += check_results.get("nlp:complete") is False
        lbl, pred = row.get("label_sentiment"), row.get("sentiment_label")
        if lbl is not None and pred is not None:
            self.agree[0] += 1
            self.agree[1] += lbl == pred
            self.confusion[(lbl, pred)] += 1

    def result(self) -> dict[str, Any]:
        dup_rows = sum(c - 1 for c in self.event_ids.values() if c > 1)
        distinct_ids = len(self.event_ids)
        checks = {cid: {"evaluated": ev, "failed": fa, "pass_ratio": _r(1 - fa / ev, 6) if ev else None}
                  for cid, (ev, fa) in self.checks.items()}
        users = self.users.values()
        out = {
            "key": self.key,
            "rows": self.rows,
            "files": {
                "count": self.files,
                "bytes": self.bytes,
                "rows_per_file": numeric_summary(self.rows_per_file),
                "bytes_per_file": numeric_summary(self.bytes_per_file),
            },
            "columns": {name: acc.result() for name, acc in self.cols.items()},
            "derived": {name: acc.result() for name, acc in self.derived.items()},
            "checks": checks,
            "text": self.text.result(),
            "event_id": {
                "rows_with_id": sum(self.event_ids.values()),
                "distinct": distinct_ids,
                "duplicate_rows": dup_rows,
                "duplicate_ids": sum(1 for c in self.event_ids.values() if c > 1),
                "max_copies": max(self.event_ids.values(), default=0),
                "unique_ratio": _r(distinct_ids / self.rows, 6) if self.rows else None,
                "duplicate_ratio": _r(dup_rows / self.rows, 6) if self.rows else None,
            },
            "users": {
                "distinct": len(self.users),
                "posts_per_user": numeric_summary([float(c) for c in users]),
                "top_user_share": _r(max(users, default=0) / self.rows, 5) if self.rows else None,
            },
            "kpi": {
                "completitud": _r(self.key_non_null[1] / self.key_non_null[0], 6) if self.key_non_null[0] else None,
                "unicidad": _r(distinct_ids / self.rows, 6) if self.rows else None,
                "duplicados": _r(dup_rows / self.rows, 6) if self.rows else None,
                "anonimizacion": _r(self.anon_ok / self.rows, 6) if self.rows else None,
            },
            "nlp_agreement": {
                "labeled": self.agree[0],
                "agree": self.agree[1],
                "ratio": _r(self.agree[1] / self.agree[0], 5) if self.agree[0] else None,
                "confusion": {f"{a}->{b}": c for (a, b), c in sorted(self.confusion.items())},
            },
        }
        if self.text_clean is not None:
            out["text_clean"] = self.text_clean.result()
        if self.track_dt:
            out["by_dt"] = {dt: {"rows": n, "nlp_missing": m} for dt, (n, m) in sorted(self.by_dt.items())}
        return out


def group_keys(layer: str, origin: str, source: str) -> Iterable[tuple[str, str, str]]:
    yield (layer, ALL, ALL)
    yield (layer, origin, ALL)
    yield (layer, origin, source)
