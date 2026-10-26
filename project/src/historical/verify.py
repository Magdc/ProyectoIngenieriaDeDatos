"""``verify``: reglas del Lote 3.1 sobre el Parquet escrito (y, opcionalmente, BigQuery y S3).

| Id  | Qué comprueba |
|-----|---------------|
| H1  | Archivos = ``output.json`` (claves, tamaño, sha256), nombres y particiones = ``event_time`` |
| H2  | Columnas de ``posts_enriched`` en orden y tipos de Parquet (timestamps ms UTC) |
| H3  | Filas por fuente = plan, ``event_id`` únicos, ``origin = historical``, ``generator`` |
| H4  | ``event_id`` = uuid5 de ``historical|source|seed|seq`` para los ``seq`` aceptados (§3.6) |
| H5  | Fechas dentro de la ventana de 180 días, todos los días cubiertos, orden de tiempos |
| H6  | Estacionalidad semanal y horaria (correlación con el plan) y crecimiento |
| H7  | Tendencias de marca: participación al inicio frente al final, contra la mezcla de candidatos del simulador |
| H8  | Episodios: volumen y ground truth de la marca durante el episodio frente a antes |
| H9  | Anonimización: sin ``user_id``/``handle`` (columnas ni ``payload``), ``user_hash`` hex 64, 1 a 1 |
| H10 | Engagement por fuente y ``kind`` (§5.2) |
| H11 | NLP en el 100 %, etiquetas y scores válidos, una sola ``nlp_model_version``, motor real |
| H12 | Marcas, ``brand_primary``, hashtags canónicos, ``label_sentiment`` y ``lang`` |
| H13 | (``--bq``) ``user_hash`` compartidos con ``posts_enriched`` y misma versión de NLP |
| H14 | (``--s3``) Objetos en S3 = ``output.json`` (claves y tamaños) y SSE-KMS |
"""

from __future__ import annotations

import collections
import dataclasses
import datetime as dt
import json
import re
import uuid
from typing import Any, Callable, Iterable, Mapping

from historical.plan import (UTC, TrendModel, config_from_dict, format_ts, parse_ts, pearson,
                             split_counts)
from historical.work import HEX64_CHECK, PART_RE, WorkDir, find_identity_keys

OK, AVISO, FALLA = "OK", "AVISO", "FALLA"
SENTIMENTS = {"negative", "neutral", "positive"}
EMOTIONS = {"joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral"}
VERSION_RE = re.compile(r"^robertuito-sentiment@[^+@\s]+\+robertuito-emotion@[^+@\s]+$")
BRAND_RE = re.compile(r"^[a-z0-9_]+$")
HASHTAG_RE = re.compile(r"^[a-z0-9_]{1,100}$")
GENERATOR_RE = re.compile(r"^[A-Za-z0-9_.-]+@[0-9A-Za-z_.-]+$")
FALLBACK_COLUMNS = (
    "event_id", "origin", "source", "generator", "event_time", "ingest_time", "process_time",
    "user_hash", "followers", "text", "lang", "brands", "brand_primary", "hashtags", "likes",
    "shares", "comments", "sentiment_label", "sentiment_score", "emotion_label",
    "emotion_score", "label_sentiment", "nlp_model_version", "payload")
TIMESTAMP_COLUMNS = ("event_time", "ingest_time", "process_time")


@dataclasses.dataclass
class Check:
    id: str
    name: str
    status: str
    detail: str
    data: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def event_id_formula(origin: str, source: str, seed: int, seq: int) -> str:
    """§3.6: ``uuid5(NAMESPACE_URL, "{origin}|{source}|{seed}|{seq}")``."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}|{source}|{seed}|{seq}"))


def _local(ts: dt.datetime, offset_h: float) -> dt.datetime:
    return ts.astimezone(UTC) + dt.timedelta(hours=offset_h)


def _as_dt(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return parse_ts(str(value))


def load_parquet_rows(work: WorkDir, files: Iterable[Mapping[str, Any]], workers: int = 8):
    """Lee los Parquet en paralelo (pyarrow suelta el GIL): filas, schemas y sha256 por clave."""
    import concurrent.futures
    import hashlib
    import io

    import pyarrow.parquet as pq

    def read(key: str):
        path = work.local_path(key)
        if not path.exists():
            return key, None, None, None
        data = path.read_bytes()
        table = pq.read_table(io.BytesIO(data))
        return key, table.schema, table.to_pylist(), (len(data), hashlib.sha256(data).hexdigest())

    schemas: dict[str, Any] = {}
    digests: dict[str, tuple[int, str]] = {}
    rows: list[dict[str, Any]] = []
    keys = [f["key"] for f in files]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for key, schema, items, digest in pool.map(read, keys):
            if schema is None:
                continue
            schemas[key] = schema
            digests[key] = digest
            for r in items:
                r["_key"] = key
                r["_et"] = _as_dt(r["event_time"])
                rows.append(r)
    return rows, schemas, digests


CandidateFn = Callable[[str, int, dt.datetime], "tuple[str | None, str | None]"]


def run_checks(work: WorkDir, *, columns: list[str] | None = None,
               allow_fake_nlp: bool = False,
               candidate: CandidateFn | None = None,
               bq_check: Callable[[list[str], set[str]], Check] | None = None,
               s3_check: Callable[[list[dict[str, Any]]], Check] | None = None) -> dict[str, Any]:
    gen = work.read_json(work.generate_json)
    out = work.read_json(work.output_json)
    checks: list[Check] = []
    if not gen or not out:
        checks.append(Check("H0", "Etapas previas", FALLA,
                            "faltan generate.json u output.json: corra generate, nlp y write"))
        return _report(work, checks, {})
    cfg = config_from_dict(gen["config"])
    plan = gen["plan"]
    as_of = parse_ts(plan["as_of"])
    model = TrendModel(cfg, as_of)
    files = out["files"]
    rows, schemas, digests = load_parquet_rows(work, files)
    by_source: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for r in rows:
        by_source[r["source"]].append(r)
    big = cfg.n >= 10_000
    soft = FALLA if big else AVISO
    checks.append(_h1_files(work, files, rows, digests))
    checks.append(_h2_columns(schemas, columns))
    checks.append(_h3_counts(rows, by_source, plan))
    checks.append(_h4_event_ids(by_source, gen, cfg.seed))
    checks.append(_h5_dates(rows, by_source, model, cfg))
    checks.append(_h6_season(rows, model, cfg, soft))
    checks.append(_h7_brands(rows, by_source, model, cfg, gen, candidate))
    checks.append(_h8_events(rows, model, cfg, soft))
    checks.append(_h9_anonymization(rows, schemas, gen))
    checks.append(_h10_engagement(rows))
    checks.append(_h11_nlp(rows, work, allow_fake_nlp))
    checks.append(_h12_brands_tags(rows))
    if bq_check is not None:
        hashes = [h for h, _ in collections.Counter(r["user_hash"] for r in rows).most_common(500)]
        versions = {r.get("nlp_model_version") for r in rows if r.get("nlp_model_version")}
        checks.append(bq_check(hashes, versions))
    if s3_check is not None:
        checks.append(s3_check(files))
    summary = {"rows": len(rows), "files": len(files),
               "rows_per_source": {s: len(v) for s, v in sorted(by_source.items())},
               "digest": gen.get("digest"), "as_of": plan["as_of"], "start": plan["start"]}
    return _report(work, checks, summary)


def _report(work: WorkDir, checks: list[Check], summary: dict[str, Any]) -> dict[str, Any]:
    status = OK
    if any(c.status == FALLA for c in checks):
        status = FALLA
    elif any(c.status == AVISO for c in checks):
        status = AVISO
    return {"status": status, "seed": work.seed, "root": str(work.root),
            "checked_at": format_ts(dt.datetime.now(UTC)), "summary": summary,
            "checks": [c.to_dict() for c in checks]}


# --------------------------------------------------------------------- reglas

def _h1_files(work: WorkDir, files, rows, digests) -> Check:
    problems = []
    on_disk = {str(p.relative_to(work.root)).replace("\\", "/") for p in work.own_parts()}
    listed = {f["key"] for f in files}
    extra = sorted(on_disk - listed)
    missing = sorted(listed - on_disk)
    if extra:
        problems.append(f"{len(extra)} archivos de la semilla fuera de output.json ({extra[:2]})")
    if missing:
        problems.append(f"faltan {len(missing)} archivos ({missing[:2]})")
    for f in files:
        m = PART_RE.match(f["key"])
        if not m or int(m["seed"]) != work.seed:
            problems.append(f"nombre inesperado: {f['key']}")
            continue
        if m["day"] != m["dt"].replace("-", "") or m["hour"] != m["hr"]:
            problems.append(f"nombre y partición no coinciden: {f['key']}")
        got = digests.get(f["key"])
        if got is not None and got != (int(f["size"]), f["sha256"]):
            problems.append(f"contenido distinto de output.json: {f['key']}")
    bad_part = 0
    for r in rows:
        m = PART_RE.match(r["_key"])
        et = r["_et"]
        if not m or m["source"] != r["source"] or et.strftime("%Y-%m-%d") != m["dt"] \
                or et.strftime("%H") != m["hr"]:
            bad_part += 1
    if bad_part:
        problems.append(f"{bad_part} filas en una partición que no es la de su event_time/source")
    return Check("H1", "Archivos y particiones", FALLA if problems else OK,
                 "; ".join(problems[:6]) or f"{len(files)} archivos, particiones = event_time UTC",
                 {"files": len(files), "extra": extra[:20], "missing": missing[:20]})


def _h2_columns(schemas, columns: list[str] | None) -> Check:
    import pyarrow as pa

    expected = list(columns) if columns else list(FALLBACK_COLUMNS)
    problems = []
    names = None
    for key, schema in schemas.items():
        names = list(schema.names)
        if columns and names != expected:
            problems.append(f"{key}: columnas {names} != POSTS_ENRICHED_COLUMNS")
            break
        if not columns and sorted(names) != sorted(expected):
            problems.append(f"{key}: columnas {sorted(set(names) ^ set(expected))} difieren de §5.5")
            break
        for col in TIMESTAMP_COLUMNS:
            t = schema.field(col).type
            if not (pa.types.is_timestamp(t) and t.unit == "ms" and t.tz in ("UTC", "+00:00")):
                problems.append(f"{key}: {col} es {t}, se esperaba timestamp[ms, tz=UTC]")
                break
        for col in ("brands", "hashtags"):
            t = schema.field(col).type
            if not (pa.types.is_list(t) and pa.types.is_string(t.value_type)):
                problems.append(f"{key}: {col} es {t}, se esperaba list<string>")
        if problems:
            break
    distinct = {str(s) for s in schemas.values()}
    if len(distinct) > 1:
        problems.append(f"{len(distinct)} schemas distintos entre archivos")
    detail = "; ".join(problems) or (f"{len(expected)} columnas en orden"
                                     + ("" if columns else " (lista de §5.5; sin common/)"))
    return Check("H2", "Columnas y tipos", FALLA if problems else OK, detail,
                 {"columns": names, "reference": "POSTS_ENRICHED_COLUMNS" if columns else "§5.5"})


def _h3_counts(rows, by_source, plan) -> Check:
    problems = []
    expected = plan["sources"]
    got = {s: len(v) for s, v in by_source.items()}
    if got != expected:
        problems.append(f"filas por fuente {got} != plan {expected}")
    ids = [r["event_id"] for r in rows]
    dup = len(ids) - len(set(ids))
    if dup:
        problems.append(f"{dup} event_id repetidos")
    origins = collections.Counter(r["origin"] for r in rows)
    if set(origins) != {"historical"}:
        problems.append(f"origin {dict(origins)}")
    gens = collections.Counter(r["generator"] for r in rows)
    if any(not GENERATOR_RE.match(str(g)) for g in gens):
        problems.append(f"generator inválido {dict(gens)}")
    return Check("H3", "Conteos y unicidad", FALLA if problems else OK,
                 "; ".join(problems) or f"{len(rows)} filas = plan, 100 % event_id únicos, "
                                        f"generator {', '.join(gens)}",
                 {"rows_per_source": got, "generators": dict(gens)})


def _h4_event_ids(by_source, gen, seed: int) -> Check:
    problems = []
    seqs = gen.get("seqs") or {}
    for source, items in by_source.items():
        expected = {event_id_formula("historical", source, seed, s) for s in seqs.get(source, [])}
        got = {r["event_id"] for r in items}
        if expected != got:
            problems.append(f"{source}: {len(got - expected)} ids fuera de la fórmula, "
                            f"{len(expected - got)} faltan")
    return Check("H4", "event_id determinista", FALLA if problems else OK,
                 "; ".join(problems) or "event_id = uuid5(historical|source|seed|seq) en todas "
                                        "las filas (reejecutar no crea ids nuevos)",
                 {"seed": seed})


def _h5_dates(rows, by_source, model: TrendModel, cfg) -> Check:
    problems = []
    start = model.window.start - dt.timedelta(seconds=6)
    end = model.window.as_of
    out_of_range = 0
    bad_order = 0
    days_per_source = {}
    for source, items in by_source.items():
        days = set()
        for r in items:
            et = r["_et"]
            it = _as_dt(r["ingest_time"])
            pt = _as_dt(r["process_time"])
            if not start <= et < end:
                out_of_range += 1
            if not (et <= it <= pt):
                bad_order += 1
            days.add(et.date())
        days_per_source[source] = len(days)
    if out_of_range:
        problems.append(f"{out_of_range} filas fuera de [{format_ts(model.window.start)}, "
                        f"{format_ts(end)})")
    if bad_order:
        problems.append(f"{bad_order} filas sin event_time ≤ ingest_time ≤ process_time")
    status = FALLA if problems else OK
    min_days = min(days_per_source.values()) if days_per_source else 0
    per_day = cfg.n / max(1, len(by_source)) / cfg.days
    if per_day >= 20 and min_days < cfg.days - 1:
        problems.append(f"solo {min_days} de {cfg.days} días con datos en alguna fuente")
        status = FALLA
    return Check("H5", "Ventana de fechas", status,
                 "; ".join(problems) or f"todas en los últimos {cfg.days} días; días con datos "
                                        f"por fuente: {days_per_source}",
                 {"days_per_source": days_per_source})


def _expected_window_counts(model: TrendModel, first: dt.datetime, last: dt.datetime) -> float:
    total = 0.0
    h0 = int((first - model.window.start).total_seconds() // 3600)
    h1 = int((last - model.window.start).total_seconds() // 3600)
    cfg = model.cfg
    for h in range(max(0, h0), min(model.window.hours, h1)):
        start = model.window.start + dt.timedelta(hours=h)
        mid = start + dt.timedelta(minutes=30)
        local = start + dt.timedelta(hours=cfg.local_utc_offset_hours)
        total += (cfg.weekly[local.weekday()] * cfg.hourly[local.hour]
                  * (1 + cfg.growth * model.window.frac(mid)) * model.volume_factor(mid))
    return total


def _h6_season(rows, model: TrendModel, cfg, soft: str) -> Check:
    off = cfg.local_utc_offset_hours
    dow = [0] * 7
    hour = [0] * 24
    occurrences = [0] * 7
    d = (model.window.start + dt.timedelta(hours=off)).date()
    last = (model.window.as_of + dt.timedelta(hours=off)).date()
    while d < last:
        occurrences[d.weekday()] += 1
        d += dt.timedelta(days=1)
    for r in rows:
        lt = _local(r["_et"], off)
        dow[lt.weekday()] += 1
        hour[lt.hour] += 1
    per_dow = [dow[i] / occurrences[i] if occurrences[i] else 0 for i in range(7)]
    r_week = pearson(per_dow, list(cfg.weekly))
    r_hour = pearson(hour, list(cfg.hourly))
    span = _span(cfg)
    first_end = model.window.start + span
    last_start = model.window.as_of - span
    n_first = sum(1 for r in rows if r["_et"] < first_end)
    n_last = sum(1 for r in rows if r["_et"] >= last_start)
    ratio = n_last / n_first if n_first else None
    exp_ratio = (_expected_window_counts(model, last_start, model.window.as_of)
                 / max(1e-9, _expected_window_counts(model, model.window.start, first_end)))
    problems = []
    if r_week is None or r_week < 0.8:
        problems.append(f"semana r={r_week if r_week is None else round(r_week, 3)} < 0,8")
    if r_hour is None or r_hour < 0.9:
        problems.append(f"hora r={r_hour if r_hour is None else round(r_hour, 3)} < 0,9")
    warn = []
    if ratio is not None and exp_ratio > 0 and abs(ratio / exp_ratio - 1) > 0.25:
        warn.append(f"crecimiento {ratio:.2f} vs esperado {exp_ratio:.2f}")
    status = soft if problems else (AVISO if warn else OK)
    detail = "; ".join(problems + warn) or (
        f"semana r={r_week:.3f}, hora r={r_hour:.3f}, final/inicio "
        f"{ratio if ratio is None else round(ratio, 2)} (esperado {exp_ratio:.2f})")
    return Check("H6", "Estacionalidad y crecimiento", status, detail,
                 {"weekly_per_day": [round(x, 2) for x in per_dow], "hourly": hour,
                  "r_weekly": r_week, "r_hourly": r_hour, "growth_ratio": ratio,
                  "growth_expected": exp_ratio})


def _span(cfg) -> dt.timedelta:
    """Tramo inicial y final que se comparan: 30 días, o un cuarto de la ventana si es corta."""
    return dt.timedelta(days=min(30.0, cfg.days / 4))


def _brand_shares(rows) -> dict[str, float]:
    c = collections.Counter(r.get("brand_primary") for r in rows if r.get("brand_primary"))
    total = sum(c.values())
    return {b: n / total for b, n in c.items()} if total else {}


def _expected_shares(model: TrendModel, when: dt.datetime) -> dict[str, float]:
    brands = list(model.cfg.brands)
    f = {b: model.brand_factor(b, when) for b in brands}
    total = sum(f.values())
    return {b: v / total for b, v in f.items()} if total else {}


def _row_brand(row: Mapping[str, Any]) -> str | None:
    if row.get("brand_primary"):
        return row["brand_primary"]
    brands = row.get("brands") or []
    return brands[0] if len(brands) else None


def _h7_brands(rows, by_source, model: TrendModel, cfg, gen: Mapping[str, Any],
               candidate: CandidateFn | None) -> Check:
    """Tendencias de marca. El simulador ya mueve su mezcla de marcas con ``seq`` (§3.10:
    base × ciclo × deriva) y el plan la **multiplica** (§3.17), así que lo esperado sale de
    los candidatos reales: se reconstruyen los ``seq`` rechazados entre cada aceptado (comparten
    su instante) en el tramo inicial y el final, se pide su marca al simulador (``candidate``)
    y se suma la probabilidad de aceptación del plan. Sin ``candidate`` supone una mezcla del
    simulador uniforme y constante, que con la deriva real da falsos avisos (v2.28)."""
    if candidate is None or not gen.get("seqs"):
        return _h7_uniform(rows, model, cfg)
    span = _span(cfg)
    first_end = model.window.start + span
    last_start = model.window.as_of - span
    acc = {"first": collections.Counter(), "last": collections.Counter()}
    cand = {"first": collections.Counter(), "last": collections.Counter()}
    exp = {"first": collections.defaultdict(float), "last": collections.defaultdict(float)}
    generated = 0
    for source, items in by_source.items():
        seq_of = {event_id_formula("historical", source, cfg.seed, q): q
                  for q in (gen["seqs"].get(source) or [])}
        accepted = sorted((seq_of[r["event_id"]], i) for i, r in enumerate(items)
                          if r["event_id"] in seq_of)
        prev = -1
        for q, i in accepted:
            r = items[i]
            slot = _as_dt(r["ingest_time"])
            key = "first" if slot < first_end else ("last" if slot >= last_start else None)
            if key:
                for c in range(prev + 1, q + 1):
                    if c == q:
                        brand, label = _row_brand(r), r.get("label_sentiment")
                    else:
                        brand, label = candidate(source, c, slot)
                        generated += 1
                    cand[key][brand] += 1
                    exp[key][brand] += model.acceptance(brand, label, slot)
                acc[key][_row_brand(r)] += 1
            prev = q

    def shares(counter) -> dict[str, float]:
        total = sum(v for b, v in counter.items() if b)
        return {b: v / total for b, v in counter.items() if b} if total else {}

    obs = {k: shares(acc[k]) for k in acc}
    want = {k: shares(exp[k]) for k in exp}
    sim = {k: shares(cand[k]) for k in cand}
    n_first = sum(v for b, v in acc["first"].items() if b)
    n_last = sum(v for b, v in acc["last"].items() if b)
    problems, data, parts, lifts = [], {}, [], {}
    ref = min(cfg.brands, key=lambda x: abs(cfg.brands[x])) if cfg.brands else None
    for b in cfg.brands:
        got = obs["last"].get(b, 0) - obs["first"].get(b, 0)
        expected = want["last"].get(b, 0) - want["first"].get(b, 0)
        p = (obs["last"].get(b, 0) + obs["first"].get(b, 0)) / 2
        tol = max(0.02, 4 * (p * (1 - p) * (1 / max(1, n_first) + 1 / max(1, n_last))) ** 0.5)

        def lift(src):
            a0, a1 = src["first"].get(b, 0), src["last"].get(b, 0)
            c0, c1 = cand["first"].get(b, 0), cand["last"].get(b, 0)
            return (a1 / c1) / (a0 / c0) if a0 and c0 and c1 else None

        lift_got, lift_exp = lift(acc), lift(exp)
        data[b] = {"first": round(obs["first"].get(b, 0), 4), "last": round(obs["last"].get(b, 0), 4),
                   "change": round(got, 4), "expected_change": round(expected, 4),
                   "tolerance": round(tol, 4),
                   "simulator_first": round(sim["first"].get(b, 0), 4),
                   "simulator_last": round(sim["last"].get(b, 0), 4),
                   "acceptance_lift": round(lift_got, 3) if lift_got else None,
                   "acceptance_lift_expected": round(lift_exp, 3) if lift_exp else None}
        lifts[b] = (lift_got, lift_exp)
        if abs(got - expected) > tol:
            problems.append(f"{b}: cambio {got:+.3f} vs esperado {expected:+.3f} (±{tol:.3f})")
        parts.append(f"{b} {obs['first'].get(b, 0):.1%}→{obs['last'].get(b, 0):.1%} "
                     f"(esperado →{want['last'].get(b, 0):.1%}; simulador "
                     f"{sim['first'].get(b, 0):.1%}→{sim['last'].get(b, 0):.1%})")
    applied = []
    ref_got, ref_exp = lifts.get(ref, (None, None))
    for b, (lg, le) in lifts.items():
        if b == ref or not (lg and le and ref_got and ref_exp):
            continue
        data[b]["trend_ratio"] = round(lg / ref_got, 3)
        data[b]["trend_ratio_expected"] = round(le / ref_exp, 3)
        applied.append(f"{b} ×{lg / ref_got:.2f} (esperado ×{le / ref_exp:.2f})")
    if applied:
        parts.append(f"tendencia aplicada frente a {ref}: " + ", ".join(applied))
    data["_method"] = {"kind": "candidatos del simulador", "generated": generated,
                       "rows_first": n_first, "rows_last": n_last}
    return Check("H7", "Tendencias de marca", AVISO if problems else OK,
                 "; ".join(problems) or "; ".join(parts), data)


def _h7_uniform(rows, model: TrendModel, cfg) -> Check:
    span = _span(cfg)
    first_end = model.window.start + span
    last_start = model.window.as_of - span
    first = _brand_shares([r for r in rows if r["_et"] < first_end])
    last = _brand_shares([r for r in rows if r["_et"] >= last_start])
    exp_first = _expected_shares(model, model.window.start + span / 2)
    exp_last = _expected_shares(model, model.window.as_of - span / 2)
    problems = []
    data = {}
    for b in cfg.brands:
        exp = exp_last.get(b, 0) - exp_first.get(b, 0)
        got = last.get(b, 0) - first.get(b, 0)
        data[b] = {"first": round(first.get(b, 0), 4), "last": round(last.get(b, 0), 4),
                   "change": round(got, 4), "expected_change": round(exp, 4)}
        if abs(exp) >= 0.02 and (got * exp <= 0 or abs(got) < abs(exp) / 3):
            problems.append(f"{b}: cambio {got:+.3f} vs esperado {exp:+.3f}")
    detail = "; ".join(problems) or ", ".join(
        f"{b} {v['first']:.1%}→{v['last']:.1%}" for b, v in data.items())
    if problems:
        detail += (" (sin el simulador: supone su mezcla de marcas uniforme y constante, y el "
                   "simulador la mueve con seq; corra verify donde importe el simulador)")
    data["_method"] = {"kind": "mezcla uniforme"}
    return Check("H7", "Tendencias de marca", AVISO if problems else OK, detail, data)


def _h8_events(rows, model: TrendModel, cfg, soft: str) -> Check:
    problems = []
    warn = []
    data = {}
    for ev in cfg.events:
        start, end = ev.window(model.window.as_of)
        if start < model.window.start or end > model.window.as_of:
            data[ev.name] = {"skipped": "fuera de la ventana"}
            continue
        before_start = max(model.window.start, start - dt.timedelta(days=14))
        in_ev = [r for r in rows if r.get("brand_primary") == ev.brand
                 and start <= r["_et"] < end]
        before = [r for r in rows if r.get("brand_primary") == ev.brand
                  and before_start <= r["_et"] < start]
        days_ev = (end - start).total_seconds() / 86400
        days_before = (start - before_start).total_seconds() / 86400
        if not before or days_before <= 0:
            warn.append(f"{ev.name}: sin datos antes del episodio")
            continue
        rate_ev = len(in_ev) / days_ev
        rate_before = len(before) / days_before
        ratio = rate_ev / rate_before if rate_before else None
        need = 1 + (ev.volume - 1) * 0.5
        info: dict[str, Any] = {"rows": len(in_ev), "rows_before": len(before),
                                "volume_ratio": round(ratio, 3) if ratio else None,
                                "volume_needed": round(need, 3)}
        if ev.volume > 1 and (ratio is None or ratio < need):
            problems.append(f"{ev.name}: volumen ×{ratio:.2f} < ×{need:.2f}")
        for label, factor in ev.sentiment.items():
            key = None if label == "null" else label
            gt_ev = _share(in_ev, "label_sentiment", key)
            gt_before = _share(before, "label_sentiment", key)
            nlp_ev = _share(in_ev, "sentiment_label", key) if key else None
            nlp_before = _share(before, "sentiment_label", key) if key else None
            info[f"gt_{label}"] = [round(gt_before, 3), round(gt_ev, 3)]
            if key:
                info[f"nlp_{label}"] = [round(nlp_before, 3), round(nlp_ev, 3)]
            if factor > 1 and gt_ev - gt_before < 0.05:
                problems.append(f"{ev.name}: {label} {gt_before:.1%}→{gt_ev:.1%} (< +5 pp)")
            if factor > 1 and key and nlp_ev - nlp_before <= 0:
                warn.append(f"{ev.name}: el NLP no ve subir {label} ({nlp_before:.1%}→{nlp_ev:.1%})")
        data[ev.name] = info
    status = soft if problems else (AVISO if warn else OK)
    detail = "; ".join(problems + warn) or ", ".join(
        f"{k}: volumen ×{v['volume_ratio']}" if "volume_ratio" in v else f"{k}: {v['skipped']}"
        for k, v in data.items()) or "sin episodios"
    return Check("H8", "Episodios", status, detail, data)


def _share(rows, field: str, value: Any) -> float:
    if not rows:
        return 0.0
    return sum(1 for r in rows if r.get(field) == value) / len(rows)


def _h9_anonymization(rows, schemas, gen) -> Check:
    problems = []
    for key, schema in schemas.items():
        bad = [n for n in schema.names if n in ("user_id", "handle")]
        if bad:
            problems.append(f"{key}: columnas {bad}")
            break
    leaks = 0
    bad_hash = 0
    for r in rows:
        if not isinstance(r.get("user_hash"), str) or not HEX64_CHECK.match(r["user_hash"]):
            bad_hash += 1
        payload = r.get("payload")
        try:
            obj = json.loads(payload) if isinstance(payload, str) else payload
        except ValueError:
            obj = None
        if find_identity_keys(obj or {}):
            leaks += 1
    if leaks:
        problems.append(f"{leaks} filas con user_id/handle dentro de payload")
    if bad_hash:
        problems.append(f"{bad_hash} user_hash que no son hex de 64")
    users = gen.get("users") or {}
    if users.get("user_ids_with_several_hashes") or users.get("hashes_with_several_user_ids"):
        problems.append(f"user_hash no es 1 a 1: {users}")
    n_hash = len({r["user_hash"] for r in rows})
    return Check("H9", "Anonimización", FALLA if problems else OK,
                 "; ".join(problems) or f"sin identidad en claro; {n_hash} user_hash; 1 a 1 con "
                                        f"{users.get('distinct_user_ids', '?')} user_id (generate)",
                 {"user_hashes": n_hash, "generate_users": users})


def _h10_engagement(rows) -> Check:
    bad = collections.Counter()
    for r in rows:
        src = r["source"]
        try:
            kind = json.loads(r["payload"]).get("kind") if isinstance(r["payload"], str) else None
        except ValueError:
            kind = None
        if not isinstance(r.get("likes"), int):
            bad[f"{src}: likes no entero"] += 1
        if src == "facebook":
            if not isinstance(r.get("shares"), int):
                bad["facebook: shares no entero"] += 1
            if not isinstance(r.get("comments"), int):
                bad["facebook: comments no entero"] += 1
        elif src == "instagram":
            if r.get("shares") is not None:
                bad["instagram: shares no nulo"] += 1
            if not isinstance(r.get("comments"), int):
                bad["instagram: comments no entero"] += 1
        elif src == "reddit":
            if r.get("shares") is not None:
                bad["reddit: shares no nulo"] += 1
            if kind == "post" and not isinstance(r.get("comments"), int):
                bad["reddit post: comments no entero"] += 1
            if kind == "comment" and r.get("comments") is not None:
                bad["reddit comment: comments no nulo"] += 1
    return Check("H10", "Engagement por fuente", FALLA if bad else OK,
                 "; ".join(f"{k} ({v})" for k, v in bad.most_common(5))
                 or "likes/shares/comments según §5.2 en las tres fuentes", dict(bad))


def _h11_nlp(rows, work: WorkDir, allow_fake: bool) -> Check:
    problems = []
    missing = sum(1 for r in rows if r.get("sentiment_label") is None or r.get("emotion_label") is None)
    if missing:
        problems.append(f"{missing} filas sin NLP")
    bad_label = sum(1 for r in rows if r.get("sentiment_label") not in SENTIMENTS | {None}
                    or r.get("emotion_label") not in EMOTIONS | {None})
    if bad_label:
        problems.append(f"{bad_label} etiquetas fuera de los enums")
    bad_score = sum(1 for r in rows for f in ("sentiment_score", "emotion_score")
                    if r.get(f) is not None and not 0 <= r[f] <= 1)
    if bad_score:
        problems.append(f"{bad_score} scores fuera de [0, 1]")
    versions = collections.Counter(r.get("nlp_model_version") for r in rows)
    if len(versions) != 1:
        problems.append(f"{len(versions)} versiones de modelos: {dict(versions)}")
    bad_version = [v for v in versions if not (isinstance(v, str) and VERSION_RE.match(v))]
    if bad_version:
        problems.append(f"versión con formato inválido: {bad_version[:2]}")
    engines = set()
    for side in work.stage2.glob("*.jsonl.gz.json"):
        try:
            engines.add(str((json.loads(side.read_text(encoding="utf-8")).get("engine") or {}).get("engine")))
        except ValueError:
            engines.add("?")
    if engines - {"beam"} and not allow_fake:
        problems.append(f"motor de NLP {sorted(engines)}: solo 'beam' (streaming.nlp) vale para el lote")
    labelled = [r for r in rows if r.get("label_sentiment") in SENTIMENTS]
    agree = (sum(1 for r in labelled if r.get("sentiment_label") == r["label_sentiment"])
             / len(labelled)) if labelled else None
    sent = collections.Counter(r.get("sentiment_label") for r in rows)
    emo = collections.Counter(r.get("emotion_label") for r in rows)
    detail = "; ".join(problems) or (
        f"NLP en el 100 %; versión {next(iter(versions))}; acuerdo con label_sentiment "
        f"{agree:.1%}" if agree is not None else "NLP en el 100 %")
    return Check("H11", "NLP", FALLA if problems else OK, detail,
                 {"versions": dict(versions), "engines": sorted(engines), "agreement": agree,
                  "sentiment": dict(sent), "emotion": dict(emo)})


def _h12_brands_tags(rows) -> Check:
    bad = collections.Counter()
    for r in rows:
        brands = list(r.get("brands") or [])
        if any(not BRAND_RE.match(str(b)) for b in brands) or len(set(brands)) != len(brands):
            bad["brands no canónicas o repetidas"] += 1
        if r.get("brand_primary") != (brands[0] if brands else None):
            bad["brand_primary != brands[0]"] += 1
        tags = list(r.get("hashtags") or [])
        if any(not HASHTAG_RE.match(str(t)) for t in tags) or len(set(tags)) != len(tags):
            bad["hashtags no canónicos"] += 1
        if r.get("label_sentiment") not in SENTIMENTS | {None}:
            bad["label_sentiment inválido"] += 1
        if r.get("lang") not in ("es", None):
            bad["lang inesperado"] += 1
        if not r.get("text"):
            bad["text vacío"] += 1
    without_brand = sum(1 for r in rows if not r.get("brand_primary"))
    return Check("H12", "Marcas, hashtags y etiquetas", FALLA if bad else OK,
                 "; ".join(f"{k} ({v})" for k, v in bad.items())
                 or f"canónicos; {without_brand} filas sin marca",
                 {"without_brand": without_brand, **dict(bad)})


def check_counts_by_day(rows) -> dict[str, int]:
    return dict(sorted(collections.Counter(
        _as_dt(r["event_time"]).strftime("%Y-%m-%d") for r in rows).items()))


def expected_counts(cfg) -> dict[str, int]:
    return split_counts(cfg.n, cfg.sources)
