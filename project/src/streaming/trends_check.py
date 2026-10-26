"""Comprobaciones del verificador del Lote 2.4 (``scripts/verify_trends.py``). Sin red.

Las usan ``trends_cli`` (local y nube) y las pruebas. Cada comprobación devuelve un
``Check`` con estado ``OK``, ``AVISO``, ``FALLA`` u ``OMITIDO``.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from common.stream_tables import TABLES, canonical_type
from streaming.trends_core import (
    SOURCES,
    TrendConfig,
    TrendFeature,
    expected_windows,
    feature_from_row,
    format_ts,
    latest_versions,
    parse_emotion_counts,
    row_problems,
    to_epoch_ms,
)

OK, AVISO, FALLA, OMITIDO = "OK", "AVISO", "FALLA", "OMITIDO"


@dataclass
class Check:
    id: str
    title: str
    status: str
    detail: str = ""
    data: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


def verdict(checks: Iterable[Check]) -> str:
    statuses = {c.status for c in checks}
    if FALLA in statuses:
        return FALLA
    if AVISO in statuses:
        return AVISO
    return OK


def percentiles(values: Sequence[float]) -> dict:
    """p50, p95 y máximo (método del rango más cercano)."""
    data = sorted(v for v in values if v is not None and math.isfinite(v))
    if not data:
        return {"n": 0, "p50": None, "p95": None, "max": None}

    def rank(q: float) -> float:
        index = max(0, min(len(data) - 1, math.ceil(q * len(data)) - 1))
        return data[index]

    return {"n": len(data), "p50": rank(0.50), "p95": rank(0.95), "max": data[-1]}


# --------------------------------------------------------------------------- tablas

def check_table_schemas(schemas: Mapping[str, Sequence[tuple[str, str]] | None]) -> Check:
    """``schemas``: tabla → ``[(nombre, tipo)]`` leído de BigQuery (``None`` si no existe)."""
    problems, data = [], {}
    for table, columns in TABLES.items():
        actual = schemas.get(table)
        if actual is None:
            problems.append(f"{table}: no existe")
            continue
        expected = [(c.name, canonical_type(c.type)) for c in columns]
        got = [(n, canonical_type(t)) for n, t in actual]
        data[table] = [n for n, _ in got]
        if got != expected:
            problems.append(f"{table}: columnas {got} ≠ {expected}")
    if schemas.get("trend_windows_latest") is None:
        problems.append("trend_windows_latest: no existe")
    status = FALLA if problems else OK
    return Check("T1", "Tablas y vista de ta_stream con las columnas de §5.5", status,
                 "; ".join(problems) or "trend_windows, dlq_events, dq_results y trend_windows_latest", data)


# --------------------------------------------------------------------------- filas

def check_rows(rows: Sequence[Mapping], cfg: TrendConfig) -> Check:
    if not rows:
        return Check("T3", "Reglas de cada fila de trend_windows", FALLA, "no hay filas en el periodo")
    bad = []
    for row in rows:
        problems = row_problems(row, cfg)
        if problems:
            bad.append({"window_start": str(row.get("window_start")), "brand": row.get("brand"),
                        "source": row.get("source"), "problems": problems})
    status = FALLA if bad else OK
    return Check("T3", "Reglas de cada fila de trend_windows", status,
                 f"{len(rows)} filas, {len(bad)} con problemas", {"rows": len(rows), "bad": bad[:20]})


def check_keys(rows: Sequence[Mapping]) -> Check:
    keys: dict[str, set] = {}
    for row in rows:
        keys.setdefault(row.get("source"), set()).add(row.get("brand"))
    missing = [s for s in SOURCES if not keys.get(s)]
    summary = {s: sorted(keys.get(s, set())) for s in SOURCES}
    if missing:
        return Check("T2", "Hay ventanas de cada fuente", FALLA, f"sin ventanas de {missing}", summary)
    thin = [s for s in SOURCES if len(keys[s]) < 2]
    status = AVISO if thin else OK
    detail = ", ".join(f"{s}: {len(keys[s])} marcas" for s in SOURCES)
    return Check("T2", "Hay ventanas de cada fuente", status, detail, summary)


def versions_summary(rows: Sequence[Mapping]) -> dict:
    counts: dict[tuple, int] = {}
    for row in rows:
        key = (str(row.get("window_start")), row.get("brand"), row.get("source"))
        counts[key] = counts.get(key, 0) + 1
    multi = sum(1 for c in counts.values() if c > 1)
    return {"keys": len(counts), "rows": len(rows), "keys_with_late_panes": multi,
            "max_versions": max(counts.values(), default=0)}


# --------------------------------------------------------------------------- conciliación

def _features(posts: Iterable[Mapping]) -> list[TrendFeature]:
    seen, out = set(), []
    for post in posts:
        event_id = post.get("event_id")
        if event_id in seen:
            continue
        seen.add(event_id)
        feature, _ = feature_from_row(post)
        if feature is not None:
            out.append(feature)
    return out


def _same(a: Any, b: Any, tolerance: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= tolerance


def compare_windows(trend_rows: Sequence[Mapping], posts: Iterable[Mapping], cfg: TrendConfig,
                    lo_ms: int, hi_ms: int, tolerance: float = 1e-5) -> dict:
    """Compara la versión vigente de cada ventana con el recálculo desde los posts.

    Solo ventanas con ``lo_ms <= window_start`` y ``window_end <= hi_ms``.
    """
    latest = {k: v for k, v in latest_versions(trend_rows).items()
              if k[0] >= lo_ms and k[0] + cfg.window_ms <= hi_ms}
    expected = {k: v for k, v in expected_windows(_features(posts), cfg).items()
                if k[0] >= lo_ms and k[0] + cfg.window_ms <= hi_ms}
    keys = sorted(set(latest) | set(expected))
    matched, mismatches, missing, extra = 0, [], [], []
    for key in keys:
        got, want = latest.get(key), expected.get(key)
        label = {"window_start": format_ts(key[0]), "brand": key[1], "source": key[2]}
        if got is None:
            missing.append({**label, "expected_n_posts": want.n_posts})
            continue
        if want is None:
            extra.append({**label, "n_posts": got.get("n_posts")})
            continue
        diffs = {}
        for name, want_value in (("n_posts", want.n_posts), ("n_negative", want.n_negative),
                                 ("n_positive", want.n_positive)):
            if got.get(name) != want_value:
                diffs[name] = [got.get(name), want_value]
        if not _same(got.get("avg_sentiment"), want.avg_sentiment, tolerance):
            diffs["avg_sentiment"] = [got.get("avg_sentiment"), want.avg_sentiment]
        emotions = parse_emotion_counts(got.get("emotion_counts")) or {}
        if {k: int(v) for k, v in emotions.items()} != dict(want.emotion_counts):
            diffs["emotion_counts"] = [emotions, dict(want.emotion_counts)]
        if diffs:
            mismatches.append({**label, "diffs": diffs})
        else:
            matched += 1
    compared = len(keys)
    return {"compared": compared, "matched": matched, "ratio": (matched / compared) if compared else None,
            "mismatches": mismatches[:20], "missing": missing[:20], "extra": extra[:20],
            "n_mismatches": len(mismatches), "n_missing": len(missing), "n_extra": len(extra),
            "lo": format_ts(lo_ms), "hi": format_ts(hi_ms)}


def check_reconciliation(result: Mapping, min_ok: float = 0.99, min_warn: float = 0.95) -> Check:
    title = "Conteos de cada ventana = recálculo desde los posts"
    if not result["compared"]:
        return Check("T4", title, AVISO, "sin ventanas cerradas para comparar (espere unos minutos)", dict(result))
    ratio = result["ratio"]
    status = OK if ratio >= min_ok else (AVISO if ratio >= min_warn else FALLA)
    detail = (f"{result['matched']}/{result['compared']} ventanas iguales ({ratio:.1%}); "
              f"distintas {result['n_mismatches']}, faltan {result['n_missing']}, sobran {result['n_extra']}")
    return Check("T4", title, status, detail, dict(result))


# --------------------------------------------------------------------------- arranque en frío y alertas

def check_cold_start(rows: Sequence[Mapping], cfg: TrendConfig, since_ms: int) -> Check:
    """Ninguna alerta en ``[since, since + baseline × paso)``: el job recién arrancado no tiene baseline."""
    cold_until = since_ms + cfg.baseline_windows * cfg.period_ms
    alerts = [r for r in rows if r.get("is_alert") is True]
    cold = [r for r in alerts if since_ms <= (to_epoch_ms(r.get("window_start")) or 0) < cold_until]
    data = {"since": format_ts(since_ms), "cold_until": format_ts(cold_until), "alerts": len(alerts),
            "alerts_in_cold_start": [{"window_start": str(r.get("window_start")), "brand": r.get("brand"),
                                      "source": r.get("source")} for r in cold[:20]]}
    if cold:
        return Check("T5", "Arranque en frío: sin alertas en la primera hora", FALLA,
                     f"{len(cold)} alertas antes de {format_ts(cold_until)}", data)
    return Check("T5", "Arranque en frío: sin alertas en la primera hora", OK,
                 f"0 alertas antes de {format_ts(cold_until)}; {len(alerts)} en total", data)


def check_switch_alerts(rows: Sequence[Mapping], cfg: TrendConfig, since_ms: int) -> Check:
    """Alertas en las ventanas que se cerraron al cambiar de job (``[since − 3 ventanas, since)``).

    Al drenar, la marca de agua salta al infinito y las ventanas abiertas salen con datos
    parciales: una caída de volumen falsa. Desde v2.26 esas ventanas no alertan; si aparecen,
    las escribió un job anterior sin la corrección. Es un aviso: no depende del job validado.
    """
    lo = since_ms - 3 * cfg.window_ms
    found = [r for r in rows if r.get("is_alert") is True
             and lo <= (to_epoch_ms(r.get("window_start")) or 0) < since_ms]
    title = "Sin alertas en las ventanas del cambio de job (drenado)"
    data = {"from": format_ts(lo), "to": format_ts(since_ms),
            "alerts": [{"window_start": str(r.get("window_start")), "brand": r.get("brand"),
                        "source": r.get("source"), "n_posts": r.get("n_posts"),
                        "zscore_volume": r.get("zscore_volume")} for r in found[:20]]}
    if found:
        return Check("T5b", title, AVISO,
                     f"{len(found)} alertas en ventanas que empiezan entre {format_ts(lo)} y {format_ts(since_ms)}: "
                     "probablemente del drenado de un job sin la corrección de v2.26", data)
    return Check("T5b", title, OK, "0 alertas", data)


def check_zscores(rows: Sequence[Mapping]) -> Check:
    with_z = [r for r in rows if r.get("zscore_volume") is not None]
    if not rows:
        return Check("T6", "z-scores calculados", OMITIDO, "sin filas")
    status = OK if with_z else AVISO
    return Check("T6", "z-scores calculados", status,
                 f"{len(with_z)}/{len(rows)} filas con zscore_volume (nulo con menos de 2 ventanas de baseline)")


# --------------------------------------------------------------------------- latencias

def check_latency(stats: Mapping, threshold_s: float = 300.0, check_id: str = "T7",
                  title: str = "Latencia event_time → process_time (p95 < 5 min)") -> Check:
    if not stats or not stats.get("n"):
        return Check(check_id, title, AVISO, "sin filas para medir", dict(stats or {}))
    p95 = stats["p95"]
    status = OK if p95 < threshold_s else FALLA
    return Check(check_id, title, status, f"p50 {stats['p50']:.1f} s · p95 {p95:.1f} s · máx {stats['max']:.1f} s "
                                          f"(n = {stats['n']})", dict(stats))


def detection_latencies(first_seen_ms: Mapping[tuple, int], window_ms: int = 300_000,
                        censored_ms: Mapping[tuple, int] | None = None) -> dict:
    """``(window_start, brand, source)`` → instante en que la fila apareció; latencia = aparición − fin.

    ``censored_ms``: claves que no aparecieron antes de terminar la observación, con ese instante;
    entran con su cota inferior (fin de la observación − fin de la ventana), para que una ventana
    muy atrasada no desaparezca del p95. ``windows`` da el detalle por ventana, para distinguir una
    ventana atrasada de un atraso general.
    """
    censored_ms = censored_ms or {}
    values = []
    by_window: dict[int, dict] = {}
    for source, is_censored in ((first_seen_ms, False), (censored_ms, True)):
        for (start_ms, _brand, _source), seen_ms in source.items():
            values.append((seen_ms - (start_ms + window_ms)) / 1000)
            entry = by_window.setdefault(start_ms, {"seen": [], "censored": 0})
            entry["seen"].append(seen_ms)
            entry["censored"] += int(is_censored)
    stats = percentiles(values)
    stats["censored"] = len(censored_ms)
    stats["windows"] = [
        {"window_start": format_ts(start), "window_end": format_ts(start + window_ms), "keys": len(e["seen"]),
         "not_seen": e["censored"], "first_seen": format_ts(min(e["seen"])),
         "latency_first_s": round((min(e["seen"]) - start - window_ms) / 1000, 1),
         "latency_last_s": round((max(e["seen"]) - start - window_ms) / 1000, 1)}
        for start, e in sorted(by_window.items())
    ][:60]
    return stats


# --------------------------------------------------------------------------- lake local

def load_local_trends(lake: Path) -> list[dict]:
    root = lake / "trend_windows"
    rows = []
    for path in sorted(root.rglob("*.jsonl")) if root.exists() else []:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    rows.append({"_invalid_line": line[:200]})
    return rows


def _partition_hour_ms(path: Path) -> int | None:
    dt = hr = None
    for part in path.parts:
        if part.startswith("dt="):
            dt = part[3:]
        elif part.startswith("hr="):
            hr = part[3:]
    if dt is None:
        return None
    return to_epoch_ms(f"{dt}T{hr or '00'}:00:00Z")


def load_local_posts(lake: Path, from_ms: int) -> list[dict]:
    """Filas de ``enriched/`` procesadas desde ``from_ms`` − 1 h (particiones por hora de proceso)."""
    import pyarrow.parquet as pq

    root = lake / "enriched"
    columns = ["event_id", "event_time", "brand_primary", "source", "sentiment_label",
               "sentiment_score", "emotion_label", "process_time", "ingest_time"]
    rows: list[dict] = []
    for path in sorted(root.rglob("*.parquet")) if root.exists() else []:
        hour = _partition_hour_ms(path)
        if hour is not None and hour < from_ms - 3_600_000:
            continue
        table = pq.read_table(path)
        present = [c for c in columns if c in table.column_names]
        rows.extend(table.select(present).to_pylist())
    return rows


def local_latency(posts: Iterable[Mapping], since_ms: int) -> dict:
    values = []
    for post in posts:
        event = to_epoch_ms(post.get("event_time"))
        processed = to_epoch_ms(post.get("process_time"))
        ingest = to_epoch_ms(post.get("ingest_time"))
        if event is None or processed is None or processed < since_ms:
            continue
        if ingest is not None and ingest - event > 60_000:  # tardío del caos (§3.9)
            continue
        values.append((processed - event) / 1000)
    return percentiles(values)
