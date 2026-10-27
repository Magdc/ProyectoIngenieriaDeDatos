"""Lo que valida Great Expectations: DataFrames de Raw, Trusted y Curated, banderas por fila y métricas (Lote 5.2).

Lee la copia local del lake (el layout del bucket bajo ``{caché}/s3/{bucket}/``) con pyarrow y la pasa a pandas.
Las banderas ``dq__{id}`` son las comprobaciones por fila del 5.1 (``quality.checks``) para las reglas
``custom`` de §5.10; las métricas salen en una tabla larga (``rule``, ``key``, ``value``). En Curated, los
agregados, ``dim_user`` y las aristas se recalculan aquí (pandas) y se cuentan las filas distintas.
Sin dependencias de Great Expectations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dq.catalog import FLAG_PREFIX, layer_specs
from quality import layers as L
from quality.checks import CHECKS_BY_ID, RowMeta
from quality.profiler import iter_layer_files
from quality.rules import RULES_BY_ID

IDENTITY_COLUMNS = ("user_id", "handle")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
COMPLETENESS_COLUMNS = ("event_id", "event_time", "source", "text", "user_hash")
CURATED_TABLES = ("fact_posts", "dim_date", "dim_source", "dim_brand", "dim_user", "agg_emotion_by_brand_hour",
                  "agg_trend_daily", "edges_user_brand", "edges_hashtag_cooccurrence")
SENTIMENT_SIGN = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}


@dataclass
class LayerData:
    """Una capa (raw o trusted) por origen: filas con banderas y estadísticas de archivos."""

    layer: str
    frames: dict[str, pd.DataFrame]
    files: dict[str, dict]
    skipped: list[str] = field(default_factory=list)
    read_errors: list[dict] = field(default_factory=list)
    rows_without_origin: int = 0


def _flag_checks(layer: str) -> list[tuple[str, object]]:
    return [(s.rule, CHECKS_BY_ID[RULES_BY_ID[s.rule].check]) for s in layer_specs(layer) if s.kind == "flag"]


def load_layer(root: Path, layer: str, *, as_of: datetime | None = None, max_files: int | None = None) -> LayerData:
    """Lee ``raw/`` o ``trusted/posts/`` de ``root`` y arma un DataFrame por origen (columnas de la capa, ``source`` de
    la partición en Trusted, ``_origin``, ``_dt`` y las banderas ``dq__{id}``)."""
    as_of = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expected = list(L.RAW_COLUMNS if layer == "raw" else L.TRUSTED_COLUMNS)
    flags = _flag_checks(layer)
    pieces, files, skipped, errors = [], {}, [], []
    without_origin = 0
    for i, (key, path, _size) in enumerate(iter_layer_files(root, layer)):
        if max_files is not None and i >= max_files:
            break
        part = L.parse_key(layer, key)
        if part is None:
            if not key.endswith("/"):
                skipped.append(key)
            continue
        try:
            table = pq.read_table(path)
        except Exception as exc:  # noqa: BLE001 - un archivo ilegible es un hallazgo
            errors.append({"key": key, "error": f"{type(exc).__name__}: {str(exc)[:160]}"})
            continue
        names = table.schema.names
        rows = table.to_pylist()
        origin_part, source_part = part.get("origin"), part.get("source")
        window = L.stream_window_start(part["file"]) if origin_part == "stream" else None
        origins, values = [], {rid: [] for rid, _ in flags}
        for row in rows:
            origin = origin_part if layer == "raw" else (row.get("origin") if row.get("origin") in L.ORIGINS else "?")
            origins.append(origin)
            meta = RowMeta(layer, origin, source_part, part.get("dt"), part.get("hr"), part["file"], as_of, window)
            for rid, check in flags:
                values[rid].append(check.fn(row, meta))
        for origin in set(origins) or {origin_part or "?"}:
            stats = files.setdefault(origin, {"files": 0, "schema_ok": 0, "identity_columns": []})
            stats["files"] += 1
            stats["schema_ok"] += 1 if names == expected else 0
            stats["identity_columns"] += [c for c in names if c in IDENTITY_COLUMNS and c not in stats["identity_columns"]]
        n = table.num_rows
        cols = {c: (table.column(c) if c in names else pa.nulls(n)) for c in expected}
        if layer == "trusted":
            cols["source"] = pa.array([source_part] * n, pa.string())
        cols["_origin"] = pa.array(origins, pa.string())
        cols["_dt"] = pa.array([part.get("dt")] * n, pa.string())
        for rid, _ in flags:
            cols[FLAG_PREFIX + rid] = pa.array(values[rid], pa.bool_())
        pieces.append(pa.table(cols))
    frames = {}
    if pieces:
        df = pa.concat_tables(pieces, promote_options="permissive").to_pandas()
        without_origin = int((df["_origin"] == "?").sum())
        for origin in L.ORIGINS:
            sub = df[df["_origin"] == origin].reset_index(drop=True)
            if len(sub):
                frames[origin] = sub
    return LayerData(layer, frames, files, skipped, errors, without_origin)


# --- métricas de Raw y Trusted --------------------------------------------------------------------


def _ratio(num: float, den: float) -> float | None:
    return None if not den else num / den


def layer_metrics(data: LayerData, origin: str, *, raw_distinct: dict[str, int] | None = None) -> pd.DataFrame:
    """Métricas agregadas de un origen (tabla larga ``rule``, ``key``, ``value``), las de ``catalog.METRIC_BOUNDS``."""
    df, stats = data.frames[origin], data.files.get(origin, {})
    n = len(df)
    rows: list[tuple[str, str, float | None]] = []
    rows.append(("DQ-01", "*", _ratio(stats.get("schema_ok", 0), stats.get("files", 0))))
    if stats.get("identity_columns"):
        anon = 0.0
    else:
        hex_ok = df["user_hash"].map(lambda v: isinstance(v, str) and bool(HEX64.match(v)))
        payload_ok = df[FLAG_PREFIX + "DQ-04"].map(lambda v: v is not False and v == v)  # None o NaN → no es falla
        anon = _ratio(float((hex_ok & payload_ok).sum()), n)
    rows.append(("DQ-02", "*", anon))
    nulls = sum(int(df[c].isna().sum()) for c in COMPLETENESS_COLUMNS)
    rows.append(("DQ-05", "*", None if not n else 1 - nulls / (5.0 * n)))
    dup = _ratio(n - df["event_id"].nunique(dropna=True), n)
    rows.append(("DQ-15" if data.layer == "raw" else "DQ-48", "*", dup))
    if data.layer == "trusted" and raw_distinct and raw_distinct.get(origin):
        rows.append(("DQ-58", "*", n / raw_distinct[origin]))
    if data.layer == "raw" and origin == "stream":
        lag = (df["process_time"] - df["event_time"]).dt.total_seconds().dropna()
        rows.append(("DQ-59", "*", float(lag.quantile(0.95)) if len(lag) else None))
    for source, count in df["source"].value_counts(dropna=True).sort_index().items():
        rows.append(("DQ-60", str(source), count / n))
    both = df["sentiment_label"].notna() & df["label_sentiment"].notna()
    agree = (df["sentiment_label"] == df["label_sentiment"]) & both
    rows.append(("DQ-62", "*", _ratio(float(agree.sum()), float(both.sum()))))
    return pd.DataFrame([r for r in rows if r[2] is not None], columns=["rule", "key", "value"])


def distinct_event_ids(data: LayerData) -> dict[str, int]:
    return {o: int(df["event_id"].nunique(dropna=True)) for o, df in data.frames.items()}


def all_rows(data: LayerData) -> pd.DataFrame:
    frames = [df for df in data.frames.values()]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --- Curated ---------------------------------------------------------------------------------------


@dataclass
class CuratedData:
    tables: dict[str, pd.DataFrame]
    files: dict[str, dict]  # tabla → {files, schema_ok}
    identity_columns: list[str]


def _contract_columns() -> dict[str, list[str]]:
    from lakehouse import catalog_spec

    return {t: list(catalog_spec.table(f"ta_curated.{t}").column_names) for t in CURATED_TABLES}


def load_curated(root: Path) -> CuratedData:
    """Las nueve tablas de ``curated/`` de ``root`` (``fact_posts`` con ``dt`` de la partición y la bandera DQ-69)."""
    contract = _contract_columns()
    tables, files, identity = {}, {}, []
    for name in CURATED_TABLES:
        base = root / "curated" / name
        paths = sorted(p for p in base.rglob("*.parquet")) if base.exists() else []
        stats = files.setdefault(name, {"files": 0, "schema_ok": 0})
        pieces = []
        for p in paths:
            t = pq.read_table(p)
            stats["files"] += 1
            stats["schema_ok"] += 1 if t.schema.names == contract[name] else 0
            identity += [c for c in t.schema.names if c in IDENTITY_COLUMNS and c not in identity]
            if name == "fact_posts":
                m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", p.as_posix())
                t = t.append_column("dt", pa.array([m.group(1) if m else None] * t.num_rows, pa.string()))
            pieces.append(t)
        df = pa.concat_tables(pieces, promote_options="permissive").to_pandas() if pieces else pd.DataFrame(columns=contract[name])
        tables[name] = df
    fact = tables["fact_posts"]
    if len(fact):
        day = fact["event_time"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d")
        key = fact["dt"].str.replace("-", "", regex=False).astype("Int64")
        fact[FLAG_PREFIX + "DQ-69"] = (fact["date_key"].astype("Int64") == key) & (day == fact["dt"])
    return CuratedData(tables, files, identity)


def _mismatches(expected: pd.DataFrame, got: pd.DataFrame, keys: list[str], values: list[str],
                tol: dict[str, float] | None = None) -> int:
    """Filas que no coinciden: llaves de un solo lado más llaves comunes con algún valor distinto (``tol``: por columna)."""
    m = expected.merge(got[keys + values], on=keys, how="outer", suffixes=("_e", "_g"), indicator=True)
    bad = int((m["_merge"] != "both").sum())
    both = m[m["_merge"] == "both"]
    diff = pd.Series(False, index=both.index)
    for v in values:
        a, b = both[f"{v}_e"], both[f"{v}_g"]
        nulls = a.isna() & b.isna()
        if tol and v in tol:
            a, b = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
            same = (a - b).abs() <= tol[v]
        else:
            same = a == b
        diff |= ~(same.fillna(False).astype(bool) | nulls)
    return bad + int(diff.sum())


def _signed(label, score):
    if label not in SENTIMENT_SIGN or score is None or score != score:
        return None
    return SENTIMENT_SIGN[label] * float(score)


def recompute_trend(fact: pd.DataFrame) -> pd.DataFrame:
    f = fact[fact["brand_primary"].notna()].copy()
    f["signed"] = [_signed(lab, sc) for lab, sc in zip(f["sentiment_label"], f["sentiment_score"])]
    g = f.groupby(["dt", "brand_primary"])
    out = pd.DataFrame({
        "n_posts": g.size(),
        "n_negative": g["sentiment_label"].apply(lambda s: int((s == "negative").sum())),
        "n_neutral": g["sentiment_label"].apply(lambda s: int((s == "neutral").sum())),
        "n_positive": g["sentiment_label"].apply(lambda s: int((s == "positive").sum())),
        "avg_sentiment": g["signed"].apply(lambda s: None if s.dropna().empty else round(max(-1.0, min(1.0, float(s.dropna().mean()))), 6)),
    }).reset_index().rename(columns={"dt": "full_date", "brand_primary": "brand"})
    return out


def recompute_emotion(fact: pd.DataFrame) -> pd.DataFrame:
    f = fact[fact["emotion_label"].notna() & fact["brand_primary"].notna()].copy()
    f["hour_start"] = f["event_time"].dt.floor("h")
    out = f.groupby(["hour_start", "brand_primary", "source", "emotion_label"]).size().rename("n_posts").reset_index()
    return out.rename(columns={"brand_primary": "brand"})


def recompute_dim_user(trusted: pd.DataFrame) -> pd.DataFrame:
    t = trusted[trusted["user_hash"].notna()]
    g = t.groupby("user_hash")
    return pd.DataFrame({"first_seen": g["event_time"].min(), "last_seen": g["event_time"].max(), "n_posts": g.size(),
                         "max_followers": g["followers"].max()}).reset_index()


def recompute_edges_user_brand(fact: pd.DataFrame) -> pd.DataFrame:
    f = fact[fact["user_hash"].notna()][["user_hash", "brands"]].copy()
    f["brand"] = f["brands"].map(lambda b: sorted({x for x in (list(b) if b is not None else []) if x}))
    e = f.explode("brand").dropna(subset=["brand"])
    return e.groupby(["user_hash", "brand"]).size().rename("weight").reset_index()


def recompute_edges_hashtags(trusted: pd.DataFrame) -> pd.DataFrame:
    t = trusted[["event_id", "hashtags"]].copy()
    t["tag"] = t["hashtags"].map(lambda h: sorted({x for x in (list(h) if h is not None else []) if x}))
    e = t[["event_id", "tag"]].explode("tag").dropna(subset=["tag"])
    pairs = e.merge(e, on="event_id", suffixes=("_a", "_b"))
    pairs = pairs[pairs["tag_a"] < pairs["tag_b"]]
    out = pairs.groupby(["tag_a", "tag_b"]).size().rename("weight").reset_index()
    return out.rename(columns={"tag_a": "hashtag_a", "tag_b": "hashtag_b"})


def curated_metrics(cur: CuratedData, trusted: pd.DataFrame) -> pd.DataFrame:
    """Métricas de Curated (DQ-64, 65, 67, 68, 71–76) en tabla larga ``rule``, ``key``, ``value``."""
    t = cur.tables
    fact = t["fact_posts"]
    n = len(fact)
    rows: list[tuple[str, str, float | None]] = []
    total = sum(s["files"] for s in cur.files.values())
    rows.append(("DQ-64", "*", _ratio(sum(s["schema_ok"] for s in cur.files.values()), total)))
    hashes = pd.concat([t["fact_posts"]["user_hash"], t["dim_user"]["user_hash"], t["edges_user_brand"]["user_hash"]]).dropna()
    rows.append(("DQ-65", "*", 0.0 if cur.identity_columns else
                 _ratio(float(hashes.map(lambda v: bool(HEX64.match(str(v)))).sum()), float(len(hashes)))))
    cols = ("event_id", "event_time", "source", "user_hash", "date_key")
    rows.append(("DQ-67", "fact_posts", None if not n else 1 - sum(int(fact[c].isna().sum()) for c in cols) / (5.0 * n)))
    rows.append(("DQ-68", "fact_posts", _ratio(n, len(trusted))))
    dd = t["dim_date"]
    if len(dd):
        days = pd.date_range(pd.Timestamp(str(dd["date_key"].min())), pd.Timestamp(str(dd["date_key"].max())), freq="D")
        want = {int(d.strftime("%Y%m%d")) for d in days}
        got = list(dd["date_key"].astype(int))
        rows.append(("DQ-71", "dim_date", float(len(want ^ set(got)) + (len(got) - len(set(got))))))
    else:
        rows.append(("DQ-71", "dim_date", 0.0 if not n else 1.0))
    trend = t["agg_trend_daily"].copy()
    trend["full_date"] = trend["full_date"].astype(str)
    rows.append(("DQ-72", "agg_trend_daily", float(_mismatches(
        recompute_trend(fact), trend, ["full_date", "brand"], ["n_posts", "n_negative", "n_neutral", "n_positive", "avg_sentiment"],
        tol={"avg_sentiment": 1e-6}))))
    rows.append(("DQ-73", "agg_emotion_by_brand_hour", float(_mismatches(
        recompute_emotion(fact), t["agg_emotion_by_brand_hour"], ["hour_start", "brand", "source", "emotion_label"], ["n_posts"]))))
    rows.append(("DQ-74", "dim_user", float(_mismatches(
        recompute_dim_user(trusted), t["dim_user"], ["user_hash"], ["first_seen", "last_seen", "n_posts", "max_followers"]))))
    rows.append(("DQ-75", "edges_user_brand", float(_mismatches(
        recompute_edges_user_brand(fact), t["edges_user_brand"], ["user_hash", "brand"], ["weight"]))))
    rows.append(("DQ-76", "edges_hashtag_cooccurrence", float(_mismatches(
        recompute_edges_hashtags(trusted), t["edges_hashtag_cooccurrence"], ["hashtag_a", "hashtag_b"], ["weight"]))))
    return pd.DataFrame([r for r in rows if r[2] is not None], columns=["rule", "key", "value"])


def dimension_values(cur: CuratedData) -> dict[str, list]:
    """``{"dim.llave": valores}`` para las ``value_set`` de DQ-70."""
    out = {}
    for dim, key in (("dim_source", "source"), ("dim_brand", "brand"), ("dim_user", "user_hash"), ("dim_date", "date_key")):
        vals = cur.tables[dim][key].dropna().tolist() if dim in cur.tables else []
        out[f"{dim}.{key}"] = [int(v) if key == "date_key" else v for v in vals]
    return out
