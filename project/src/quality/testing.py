"""Datos sintéticos con la forma de Raw y de Trusted para probar el *profiling* (sin nube).

* ``make_row()``: una fila válida de ``posts_enriched`` (§5.5) con ``event_id`` de la fórmula de §3.6.
* ``write_raw_layer()``: escribe filas en el layout de Raw (§5.7) con el escritor real del proyecto
  (``common.parquet_rows.rows_to_parquet_bytes``).
* ``build_trusted()``: arma una capa Trusted **simulada** con el contrato de §5.7 (unión de orígenes,
  deduplicada por ``event_id``, sin la columna ``source``, con ``text_clean`` y ``dt = DATE(event_time)``)
  a partir de una carpeta Raw. Sirve para probar el profiler mientras no exista el Lote 4.2; el
  ``text_clean`` de aquí es un marcador (minúsculas, sin hashtags), **no** la limpieza del 4.2.
  ``defects=True`` añade cuatro defectos conocidos para comprobar que el profiler los detecta.
"""

from __future__ import annotations

import hashlib
import io
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from common.envelope import event_id as make_event_id
from common.hashtags import extract_hashtags

from quality import layers as L
from quality.text_stats import _HASHTAG_RE, _SPACES_RE

TEMPLATES = (
    "Me encanta {b} cuando está bien fría. #{b}",
    "Dato: en el domicilio pedí una {b} con carimañolas.",
    "Mira, a mi jefe le gusta {b} y yo no entiendo cómo. Cero.",
    "Opinión personal: soy fan de {b} y no pienso cambiar. 🙌",
    "Vamos al punto: {b} sobre todo con el frío de Bogotá. #Colombia",
)
DISPLAY = {"postobon": "Postobón", "pepsi": "Pepsi", "sprite": "Sprite"}
NLP_VERSION = "robertuito-sentiment@a2cc0f67ebd7+robertuito-emotion@bcd6835f4d1a"
SYNTH_SEED = 990051


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_row(origin: str, source: str, seq: int, event_time: datetime, *, seed: int = SYNTH_SEED,
             process_time: datetime | None = None) -> dict[str, Any]:
    rng = random.Random(f"{seed}|{origin}|{source}|{seq}")
    brand = rng.choice(L.BRAND_KEYS_V1)
    text = rng.choice(TEMPLATES).format(b=DISPLAY[brand]) + f" ({source[:2]}{seq % 997})"
    kind = "comment" if source == "instagram" or rng.random() < 0.8 else "post"
    likes = rng.randint(-3 if source == "reddit" else 0, 400)
    payload: dict[str, Any] = {"kind": kind, "parent_id": None if kind == "post" else "p1"}
    shares = rng.randint(0, 20) if source == "facebook" else None
    comments = rng.randint(0, 30) if source != "reddit" or kind == "post" else None
    ingest = event_time + timedelta(milliseconds=rng.randint(0, 5000))
    process = process_time or ingest + timedelta(seconds=rng.randint(1, 20))
    sentiment = rng.choice(L.SENTIMENTS)
    label = rng.choice(L.SENTIMENTS + (None,))
    return {
        "event_id": make_event_id(origin, source, seed, seq),
        "origin": origin,
        "source": source,
        "generator": "comment-gen@1.0",
        "event_time": _iso(event_time),
        "ingest_time": _iso(ingest),
        "process_time": _iso(process),
        "user_hash": hashlib.sha256(f"user-{source}-{rng.randint(0, 50)}".encode()).hexdigest(),
        "followers": rng.randint(0, 5000),
        "text": text,
        "lang": "es",
        "brands": [brand],
        "brand_primary": brand,
        "hashtags": extract_hashtags(text),
        "likes": likes,
        "shares": shares,
        "comments": comments,
        "sentiment_label": sentiment,
        "sentiment_score": round(rng.uniform(0.34, 1.0), 4),
        "emotion_label": rng.choice(L.EMOTIONS),
        "emotion_score": round(rng.uniform(0.2, 1.0), 4),
        "label_sentiment": label,
        "nlp_model_version": NLP_VERSION,
        "payload": json.dumps(payload, separators=(",", ":")),
    }


def make_rows(origin: str, source: str, n: int, start: datetime, *, step: timedelta = timedelta(minutes=7),
              seed: int = SYNTH_SEED) -> list[dict[str, Any]]:
    return [make_row(origin, source, i, start + i * step, seed=seed) for i in range(n)]


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def raw_key(row: dict[str, Any], *, seed: int = SYNTH_SEED, window_s: int = 300) -> str:
    """Clave de Raw para una fila: histórico por ``event_time``; stream por la ventana de ``process_time``."""
    origin, source = row["origin"], row["source"]
    if origin == "historical":
        e = _parse(row["event_time"])
        return (f"raw/origin=historical/source={source}/dt={e:%Y-%m-%d}/hr={e:%H}/"
                f"part-historical-s{seed}-{e:%Y%m%d}T{e:%H}.parquet")
    p = _parse(row["process_time"])
    start = p - timedelta(seconds=(p.minute * 60 + p.second) % window_s, microseconds=p.microsecond)
    return f"raw/origin=stream/source={source}/dt={start:%Y-%m-%d}/hr={start:%H}/part-{start:%Y%m%dT%H%M%S}Z-p000.parquet"


def write_raw_layer(root: Path, rows: Iterable[dict[str, Any]], *, seed: int = SYNTH_SEED) -> dict[str, int]:
    """Escribe ``rows`` bajo ``{root}/raw/…`` con el escritor real; devuelve {clave: filas}."""
    from common.parquet_rows import rows_to_parquet_bytes

    by_key: dict[str, list] = defaultdict(list)
    for r in rows:
        by_key[raw_key(r, seed=seed)].append(r)
    for key, items in by_key.items():
        path = Path(root) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(rows_to_parquet_bytes(items))
    return {k: len(v) for k, v in by_key.items()}


def trusted_arrow_schema():
    import pyarrow as pa

    from common.parquet_rows import arrow_schema

    fields = []
    for f in arrow_schema():
        if f.name == "source":
            continue
        fields.append(f)
        if f.name == "text":
            fields.append(pa.field("text_clean", pa.string(), nullable=True))
    return pa.schema(fields)


def placeholder_clean(text: str | None) -> str | None:
    """Marcador de ``text_clean`` para el Trusted simulado (no es la limpieza del Lote 4.2)."""
    if text is None:
        return None
    return _SPACES_RE.sub(" ", _HASHTAG_RE.sub(" ", text.lower())).strip()


def _write_trusted_file(path: Path, rows: list[dict[str, Any]], schema=None) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = schema or trusted_arrow_schema()
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    table = pa.Table.from_pydict(cols, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    path.write_bytes(buf.getvalue())


def build_trusted(raw_root: Path, out_root: Path, *, defects: bool = False, limit_files: int | None = None) -> dict[str, Any]:
    """Raw en ``raw_root`` → Trusted simulado en ``{out_root}/trusted/posts/source=…/dt=…/``.

    Con ``defects``: (1) una fila repetida en otra partición, (2) una fila en un ``dt`` que no es el
    de su ``event_time``, (3) un ``text_clean`` nulo y (4) un archivo extra con una columna ``user_id``.
    """
    import pyarrow.parquet as pq

    from quality.profiler import iter_layer_files

    seen: set[str] = set()
    parts: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    raw_rows = 0
    files = [f for f in iter_layer_files(Path(raw_root), "raw") if L.parse_key("raw", f[0])]
    if limit_files is not None:
        files = files[:limit_files]
    for key, path, _ in files:
        for row in pq.read_table(path).to_pylist():
            raw_rows += 1
            eid = row.get("event_id")
            if eid in seen:
                continue
            seen.add(eid)
            e = row["event_time"]
            if e.tzinfo is None:
                e = e.replace(tzinfo=timezone.utc)
            row = dict(row)
            source = row.pop("source")
            row["text_clean"] = placeholder_clean(row.get("text"))
            parts[(source, e.astimezone(timezone.utc).strftime("%Y-%m-%d"))].append(row)
    injected: list[str] = []
    if defects and parts:
        keys = sorted(parts)
        first = keys[0]
        victim = dict(parts[first][0])
        other = keys[-1] if len(keys) > 1 else first
        parts[other].append(dict(victim))
        injected.append("duplicate_event_id")
        moved = parts[first].pop(-1) if len(parts[first]) > 1 else None
        if moved is not None:
            wrong = (first[0], "2026-01-02" if first[1] != "2026-01-02" else "2026-01-03")
            parts[wrong].append(moved)
            injected.append("dt_mismatch")
        parts[other][0] = dict(parts[other][0], text_clean=None)
        injected.append("null_text_clean")
    written = 0
    for (source, dt), rows in sorted(parts.items()):
        rows.sort(key=lambda r: str(r.get("event_id")))
        _write_trusted_file(Path(out_root) / f"trusted/posts/source={source}/dt={dt}/part-00000-synthetic.snappy.parquet", rows)
        written += 1
    if defects and parts:
        import pyarrow as pa

        schema = trusted_arrow_schema().append(pa.field("user_id", pa.string()))
        (source, dt) = sorted(parts)[0]
        extra = [dict(make_row("stream", source, 999_999, datetime(2026, 10, 1, tzinfo=timezone.utc)), user_id="u_000001")]
        for r in extra:
            r.pop("source", None)
            r["text_clean"] = placeholder_clean(r["text"])
            r["event_time"] = _parse(r["event_time"])
            r["ingest_time"] = _parse(r["ingest_time"])
            r["process_time"] = _parse(r["process_time"])
        _write_trusted_file(Path(out_root) / f"trusted/posts/source={source}/dt=2026-10-01/part-00001-synthetic-identity.snappy.parquet",
                            extra, schema)
        written += 1
        injected.append("identity_column")
    return {"raw_files": len(files), "raw_rows": raw_rows, "distinct_event_ids": len(seen), "files": written,
            "partitions": len(parts), "defects": injected}
