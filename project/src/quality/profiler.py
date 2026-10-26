"""*Profiling* de una capa del lake leída desde una carpeta local con el layout de S3 (§3.20).

    report = profile_layer(Path("~/.cache/trendanalyzer/quality/s3/<bucket>"), "raw")

La carpeta (``root``) tiene las mismas claves que el bucket: ``{root}/raw/origin=…/…/*.parquet`` o
``{root}/trusted/posts/source=…/dt=…/*.parquet``. La llena ``s3sync.sync_prefix`` (solo lectura de
S3) o, en pruebas, ``testing.write_raw_layer`` / ``testing.write_trusted_layer``. pyarrow se importa
al usarse. El resultado es un ``dict`` serializable a JSON con solo agregados (§3.20.1).
"""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from quality import PROFILE_SCHEMA, PROFILER_ID
from quality import layers as L
from quality.checks import RowMeta, as_utc, checks_for
from quality.profile_core import ALL, GroupAcc, _r
from quality.text_stats import Skeletonizer

MAX_EXAMPLES = 5


def arrow_kind(type_str: str) -> str:
    """Tipo físico de pyarrow (texto) → tipo lógico de ``layers.COLUMN_KINDS``."""
    t = type_str.lower()
    if t.startswith("list") or t.startswith("large_list"):
        return "list"
    if t.startswith("timestamp"):
        return "timestamp"
    if t in ("string", "large_string", "utf8"):
        return "string"
    if t in ("int64", "int32", "int16", "int8", "uint64", "uint32"):
        return "int"
    if t in ("double", "float", "float64", "float32"):
        return "float"
    return t


def _physical_kind(kind: str) -> str:
    """``json`` viaja como texto en el Parquet (§5.4)."""
    return "string" if kind == "json" else kind


def expected_types(layer: str) -> dict[str, str]:
    """Tipos físicos esperados (texto de pyarrow). Raw: los de ``common.parquet_rows.arrow_schema()``."""
    if layer == "raw":
        from common.parquet_rows import arrow_schema

        return {f.name: str(f.type) for f in arrow_schema()}
    return {}


def iter_layer_files(root: Path, layer: str) -> list[tuple[str, Path, int]]:
    spec = L.layer_spec(layer)
    base = root / spec.prefix
    if not base.exists():
        return []
    out = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            out.append((p.relative_to(root).as_posix(), p, p.stat().st_size))
    return out


def _schema_report(variants: Counter, layer: str) -> list[dict[str, Any]]:
    spec = L.layer_spec(layer)
    exp_types = expected_types(layer)
    out = []
    for sig, files in variants.most_common():
        names = [n for n, _ in sig]
        types = dict(sig)
        missing = [c for c in spec.columns if c not in types]
        extra = [n for n in names if n not in spec.columns]
        kind_mismatch = [f"{n}: {types[n]}" for n in spec.columns
                         if n in types and arrow_kind(types[n]) != _physical_kind(L.COLUMN_KINDS[n])]
        type_mismatch = [f"{n}: {types[n]} (esperado {exp_types[n]})" for n in spec.columns
                         if n in types and n in exp_types and types[n] != exp_types[n]]
        out.append({
            "files": files,
            "columns": len(names),
            "order_matches": names == list(spec.columns),
            "missing": missing,
            "extra": extra,
            "kind_mismatch": kind_mismatch,
            "type_mismatch": type_mismatch,
            "matches_contract": names == list(spec.columns) and not kind_mismatch and not type_mismatch,
            "types": types if len(out) < 3 else None,
        })
    return out


def _share_table(col: dict[str, Any], *, include_null: bool = False) -> dict[str, float]:
    non_null = col.get("non_null") or 0
    rows = col.get("rows") or 0
    base = rows if include_null else non_null
    shares = {}
    for item in col.get("top", []) or []:
        shares[str(item["value"])] = _r(item["count"] / base, 5) if base else None
    if include_null:
        shares["null"] = _r(col.get("nulls", 0) / rows, 5) if rows else None
    return shares


COMPARE_CATEGORICAL = ("brand_primary", "sentiment_label", "emotion_label", "label_sentiment", "lang")


def compare_origins(groups: dict[tuple, GroupAcc], results: dict[tuple, dict], layer: str) -> dict[str, Any]:
    """Diferencias stream vs histórico (en puntos porcentuales) y solapes de texto y usuarios."""
    s_key, h_key = (layer, "stream", ALL), (layer, "historical", ALL)
    if s_key not in results or h_key not in results:
        return {"available": False}
    s, h = results[s_key], results[h_key]
    cat: dict[str, Any] = {}
    for col in COMPARE_CATEGORICAL + ("source",):
        sc, hc = s["columns"].get(col), h["columns"].get(col)
        if not sc or not hc:
            continue
        ss = _share_table(sc, include_null=col in ("label_sentiment", "brand_primary", "lang"))
        hs = _share_table(hc, include_null=col in ("label_sentiment", "brand_primary", "lang"))
        cat[col] = {v: {"stream": ss.get(v, 0.0), "historical": hs.get(v, 0.0),
                        "diff_pp": _r(((ss.get(v) or 0.0) - (hs.get(v) or 0.0)) * 100, 2)}
                    for v in sorted(set(ss) | set(hs))}
    for name in ("payload_kind",):
        sc, hc = s["derived"][name], h["derived"][name]
        ss, hs = _share_table(sc), _share_table(hc)
        cat[name] = {v: {"stream": ss.get(v, 0.0), "historical": hs.get(v, 0.0),
                         "diff_pp": _r(((ss.get(v) or 0.0) - (hs.get(v) or 0.0)) * 100, 2)} for v in sorted(set(ss) | set(hs))}

    def num(res: dict, path: tuple) -> Any:
        cur: Any = res
        for p in path:
            cur = (cur or {}).get(p) if isinstance(cur, dict) else None
        return cur

    numeric_paths = {
        "text_length_mean": ("derived", "text_length", "numeric", "mean"),
        "text_tokens_mean": ("derived", "text_tokens", "numeric", "mean"),
        "followers_p50": ("columns", "followers", "numeric", "p50"),
        "followers_p99": ("columns", "followers", "numeric", "p99"),
        "likes_p50": ("columns", "likes", "numeric", "p50"),
        "comments_p50": ("columns", "comments", "numeric", "p50"),
        "ingest_lag_s_p95": ("derived", "ingest_lag_s", "numeric", "p95"),
        "latency_s_p50": ("derived", "latency_s", "numeric", "p50"),
        "latency_s_p95": ("derived", "latency_s", "numeric", "p95"),
        "distinct_users": ("users", "distinct"),
        "posts_per_user_mean": ("users", "posts_per_user", "mean"),
        "posts_per_user_max": ("users", "posts_per_user", "max"),
        "distinct_text_ratio": ("text", "repeated", "distinct_ratio"),
        "skeleton_distinct_ratio": ("text", "skeletons", "distinct_ratio"),
        "mattr": ("text", "mattr"),
        "ttr": ("text", "ttr"),
        "hapax_ratio": ("text", "hapax_ratio"),
        "nlp_agreement": ("nlp_agreement", "ratio"),
    }
    numeric = {k: {"stream": num(s, p), "historical": num(h, p)} for k, p in numeric_paths.items()}
    hashtags = {}
    for name, res in (("stream", s), ("historical", h)):
        hist = (res["columns"].get("hashtags") or {}).get("list", {}).get("length_histogram", {})
        total = sum(hist.values()) or 1
        hashtags[name] = _r(1 - hist.get("0", 0) / total, 4)
    numeric["rows_with_hashtags_ratio"] = hashtags

    st, ht = groups[s_key].text.text_digests(), groups[h_key].text.text_digests()
    su, hu = set(groups[s_key].users), set(groups[h_key].users)
    overlap = {
        "texts_shared": len(st & ht),
        "texts_shared_ratio_of_stream": _r(len(st & ht) / len(st), 5) if st else None,
        "users_shared": len(su & hu),
        "users_shared_ratio_of_stream": _r(len(su & hu) / len(su), 5) if su else None,
        "users_stream": len(su),
        "users_historical": len(hu),
    }
    return {"available": True, "categorical": cat, "numeric": numeric, "overlap": overlap}


def profile_layer(
    root: Path,
    layer: str,
    *,
    as_of: datetime | None = None,
    source_uri: str | None = None,
    brand_aliases: dict[str, list[str]] | None = None,
    max_files: int | None = None,
    progress: Callable[[str], None] | None = None,
    sync_manifest: dict[str, Any] | None = None,
    raw_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Perfila ``layer`` (``raw`` | ``trusted``) bajo ``root`` y devuelve el informe con anomalías y reglas.

    ``raw_report`` (solo Trusted): informe de Raw de la misma foto, para la regla DQ-58.
    """
    import pyarrow.parquet as pq

    from quality import anomalies, rules

    started = time.monotonic()
    spec = L.layer_spec(layer)
    as_of = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    aliases = brand_aliases if brand_aliases is not None else L.load_brand_aliases()
    brand_keys = frozenset(aliases)
    skeleton = Skeletonizer(aliases)
    checks = checks_for(layer)
    files = iter_layer_files(Path(root), layer)
    if max_files is not None:
        files = files[:max_files]

    groups: dict[tuple, GroupAcc] = {}

    def group(key: tuple) -> GroupAcc:
        if key not in groups:
            groups[key] = GroupAcc(*key, skeletonizer=skeleton)
        return groups[key]

    group((layer, ALL, ALL))
    variants: Counter = Counter()
    physical: dict[str, Counter] = {}
    unrecognized: list[str] = []
    auxiliary = 0
    read_errors: list[dict[str, str]] = []
    first_seen: dict[str, tuple[str, str]] = {}
    dup = {"same_file": 0, "other_file_same_origin": 0, "cross_origin": 0, "examples": []}
    seeds: Counter = Counter()
    total_bytes = 0
    recognized = 0

    for idx, (key, path, size) in enumerate(files):
        if progress and idx and idx % 2000 == 0:
            progress(f"{idx} de {len(files)} archivos")
        name = key.rsplit("/", 1)[-1]
        if name.startswith(("_", ".")):
            auxiliary += 1
            continue
        part = L.parse_key(layer, key)
        if part is None:
            if len(unrecognized) < MAX_EXAMPLES:
                unrecognized.append(key)
            dup.setdefault("unrecognized_count", 0)
            dup["unrecognized_count"] += 1
            continue
        try:
            table = pq.read_table(path)
        except Exception as exc:  # noqa: BLE001 - un archivo ilegible es un hallazgo, no un error del profiler
            read_errors.append({"key": key, "error": f"{type(exc).__name__}: {str(exc)[:160]}"})
            continue
        recognized += 1
        total_bytes += size
        sig = tuple((f.name, str(f.type)) for f in table.schema)
        variants[sig] += 1
        for f in table.schema:
            physical.setdefault(f.name, Counter())[str(f.type)] += 1
        rows = table.to_pylist()
        origin_part = part.get("origin")
        source_part = part.get("source")
        if layer == "raw":
            seed = L.historical_seed(part["file"]) if origin_part == "historical" else None
            if seed:
                seeds[seed] += 1
            file_keys = list(dict.fromkeys([(layer, ALL, ALL), (layer, origin_part, ALL), (layer, origin_part, source_part)]))
        else:
            file_keys = [(layer, ALL, ALL), (layer, ALL, source_part)]
        for gk in file_keys:
            group(gk).add_file(len(rows), size)
        window = L.stream_window_start(part["file"]) if origin_part == "stream" else None
        for row in rows:
            origin = origin_part if layer == "raw" else (row.get("origin") if row.get("origin") in L.ORIGINS else "?")
            meta = RowMeta(layer, origin, source_part, part.get("dt"), part.get("hr"), part["file"], as_of, window, brand_keys)
            results = {c.id: c.fn(row, meta) for c in checks}
            payload_obj = None
            raw_payload = row.get("payload")
            if isinstance(raw_payload, str):
                try:
                    payload_obj = json.loads(raw_payload)
                except ValueError:
                    payload_obj = None
            keys = [(layer, ALL, ALL), (layer, origin, ALL), (layer, origin, source_part)]
            if layer == "trusted":
                keys.insert(1, (layer, ALL, source_part))
            for gk in keys:
                group(gk).add_row(row, meta, payload_obj, results)
            eid = row.get("event_id")
            if isinstance(eid, str):
                prev = first_seen.get(eid)
                if prev is None:
                    first_seen[eid] = (origin, key)
                else:
                    if prev[1] == key:
                        dup["same_file"] += 1
                    elif prev[0] == origin:
                        dup["other_file_same_origin"] += 1
                    else:
                        dup["cross_origin"] += 1
                    if len(dup["examples"]) < MAX_EXAMPLES:
                        dup["examples"].append({"first": prev[1], "again": key})

    results = {k: g.result() for k, g in groups.items()}
    order = sorted(results, key=lambda k: (k[1] != ALL, k[1], k[2] != ALL, k[2]))
    total_rows = groups[(layer, ALL, ALL)].rows
    distinct_ids = len(first_seen)
    report: dict[str, Any] = {
        "schema": PROFILE_SCHEMA,
        "profiler": PROFILER_ID,
        "layer": layer,
        "table": spec.table,
        "source_uri": source_uri or "local",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": as_of.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "contract": {"columns": list(spec.columns), "partition_keys": list(spec.partition_keys)},
        "files": {
            "listed": len(files),
            "recognized": recognized,
            "auxiliary": auxiliary,
            "unrecognized": dup.pop("unrecognized_count", 0),
            "unrecognized_examples": unrecognized,
            "read_errors": read_errors[:20],
            "read_error_count": len(read_errors),
            "bytes": total_bytes,
            "schema_variants": _schema_report(variants, layer),
            "physical_types": {c: dict(v) for c, v in physical.items()},
            "historical_seeds": dict(seeds),
        },
        "groups": [results[k] for k in order],
        "duplicates": {
            "rows": total_rows,
            "distinct_event_ids": distinct_ids,
            "duplicate_rows": dup["same_file"] + dup["other_file_same_origin"] + dup["cross_origin"],
            **dup,
        },
        "comparison": compare_origins(groups, results, layer),
        "sync": sync_manifest or None,
    }
    report["anomalies"] = anomalies.detect(report)
    report["rules"] = rules.evaluate(report, raw_report)
    report["dq_results_preview"] = rules.dq_preview(report)
    report["summary"] = summarize(report)
    report["duration_s"] = round(time.monotonic() - started, 1)
    return report


def find_group(report: dict[str, Any], origin: str = ALL, source: str = ALL) -> dict[str, Any] | None:
    for g in report.get("groups", []):
        if g["key"]["origin"] == origin and g["key"]["source"] == source:
            return g
    return None


def summarize(report: dict[str, Any]) -> dict[str, Any]:
    rows = {}
    kpi = {}
    for g in report["groups"]:
        k = g["key"]
        label = f"{k['origin']}/{k['source']}"
        rows[label] = g["rows"]
        if k["source"] == ALL:
            kpi[k["origin"]] = g["kpi"]
    sev = Counter(a["severity"] for a in report.get("anomalies", []))
    rule_status = Counter(r.get("status") for r in report.get("rules", []))
    return {
        "rows": rows,
        "files": report["files"]["recognized"],
        "bytes": report["files"]["bytes"],
        "kpi": kpi,
        "anomalies": {s: sev.get(s, 0) for s in ("error", "warning", "info")},
        "rules": dict(rule_status),
    }


def as_of_from_text(text: str | None) -> datetime | None:
    if not text:
        return None
    value = text.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


__all__ = ["profile_layer", "iter_layer_files", "find_group", "summarize", "arrow_kind", "as_utc", "as_of_from_text"]
