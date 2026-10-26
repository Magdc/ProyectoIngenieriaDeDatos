"""Anomalías del *profiling* (§3.20.1): qué se aparta del contrato (§5.2, §5.7) o llama la atención.

Gravedad:

* ``error``: contrato roto (formato, enum, rango, partición, identidad, esquema, duplicado
  dentro de un archivo o entre orígenes, duplicado en Trusted). No debería pasar nunca.
* ``warning``: posible por diseño pero hay que vigilarlo (caos de §3.9, copias de un reinicio,
  varias versiones de modelos, archivos que no encajan, latencia alta).
* ``info``: rasgo del dato que conviene conocer al fijar reglas (texto repetido, archivos
  pequeños, cola larga de seguidores, clases raras, diferencias stream vs histórico).
"""

from __future__ import annotations

from typing import Any

from quality.checks import CHECKS_BY_ID, ERROR, INFO, WARNING
from quality.layers import NLP_COLUMNS
from quality.profile_core import ALL

REDUNDANT = frozenset(f"not_null:{c}" for c in NLP_COLUMNS) | {"set:generator", "nlp:complete"}

LATENCY_P95_MAX_S = 300.0
SMALL_FILE_BYTES = 128 * 1024
RARE_CLASS_SHARE = 0.01
DIFF_PP_NOTABLE = 5.0
CONSTANT_IGNORED = {"origin", "source"}


def _scope(key: dict[str, str]) -> str:
    return f"{key['layer']}/{key['origin']}/{key['source']}"


def _add(out: list, severity: str, kind: str, detail: str, *, column: str = "*", scope: str = "*",
         count: int | None = None, ratio: float | None = None, **extra: Any) -> None:
    item = {"severity": severity, "kind": kind, "column": column, "scope": scope, "count": count,
            "ratio": ratio, "detail": detail}
    item.update(extra)
    out.append(item)


def _leaf_groups(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [g for g in report["groups"] if g["key"]["origin"] != ALL and g["key"]["source"] != ALL]


def _origin_groups(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [g for g in report["groups"] if g["key"]["origin"] != ALL and g["key"]["source"] == ALL]


def detect(report: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    layer = report["layer"]
    files = report["files"]

    # --- archivos y esquema ---
    if files["listed"] == 0:
        _add(out, WARNING, "sin_datos", f"no hay archivos bajo {layer}/ en la carpeta perfilada")
    for v in files.get("schema_variants", []):
        if not v["matches_contract"]:
            _add(out, ERROR, "esquema", "esquema distinto del contrato"
                 + (f"; faltan {v['missing']}" if v["missing"] else "") + (f"; sobran {v['extra']}" if v["extra"] else "")
                 + (f"; tipos {v['kind_mismatch'] + v['type_mismatch']}" if v["kind_mismatch"] or v["type_mismatch"] else "")
                 + ("" if v["order_matches"] or v["missing"] or v["extra"] else "; orden distinto"),
                 count=v["files"], identity=[c for c in v["extra"] if c in ("user_id", "handle")])
    if files.get("read_error_count"):
        _add(out, ERROR, "lectura", "archivos Parquet ilegibles", count=files["read_error_count"],
             examples=[e["key"] for e in files["read_errors"][:3]])
    if files.get("unrecognized"):
        _add(out, WARNING, "clave_no_reconocida", "objetos que no siguen el layout de §5.7", count=files["unrecognized"],
             examples=files.get("unrecognized_examples", [])[:3])
    if files.get("auxiliary"):
        _add(out, INFO, "auxiliares", "archivos auxiliares ignorados (nombre con _ o .)", count=files["auxiliary"])

    # --- comprobaciones por fila (en el grupo más fino) ---
    for g in _leaf_groups(report):
        for cid, c in g["checks"].items():
            check = CHECKS_BY_ID.get(cid)
            if not c["failed"] or cid in REDUNDANT or (check is not None and check.severity == INFO):
                continue  # nulos por diseño (shares, comments, label_sentiment) y nulos de NLP ya contados en nlp:complete
            _add(out, check.severity if check else WARNING, "regla", check.description if check else cid,
                 column=check.column if check else "*", scope=_scope(g["key"]), count=c["failed"],
                 ratio=round(c["failed"] / c["evaluated"], 6) if c["evaluated"] else None, check=cid)

    # --- NLP ausente por periodo (particiones dt) ---
    for g in _origin_groups(report):
        by_dt = g.get("by_dt") or {}
        missing = {dt: v for dt, v in by_dt.items() if v["nlp_missing"]}
        if missing:
            total = sum(v["nlp_missing"] for v in missing.values())
            full = [dt for dt, v in missing.items() if v["nlp_missing"] == v["rows"]]
            _add(out, WARNING, "nlp_ausente",
                 f"{total} filas sin NLP en {len(missing)} particiones dt ({min(missing)} → {max(missing)}; "
                 f"{len(full)} sin ninguna fila con NLP); ¿las procesó un job con el NLP apagado? (§3.14; DQ-13 de §5.10)",
                 column="sentiment_label", scope=_scope(g["key"]), count=total,
                 by_dt={dt: v for dt, v in list(missing.items())[:10]})
        gen = g["columns"].get("generator", {})
        other = [t for t in gen.get("top", []) if t["value"] != "comment-gen@1.0"]
        if other:
            _add(out, WARNING, "eventos_de_validacion",
                 "filas de otro generador (centinelas de los verificadores, no comentarios): "
                 + ", ".join(f"{t['value']} × {t['count']}" for t in other), column="generator", scope=_scope(g["key"]),
                 count=sum(t["count"] for t in other))
        leaves = [x for x in _leaf_groups(report) if x["key"]["origin"] == g["key"]["origin"]]
        share = max(((x.get("users") or {}).get("top_user_share") or 0 for x in leaves), default=0)
        if share > 0.05:
            _add(out, INFO, "usuario_dominante",
                 f"el usuario más activo de una fuente tiene el {share * 100:.1f} % de sus filas (actividad Zipf s = 1,1 por fuente, §3.10)",
                 column="user_hash", scope=_scope(g["key"]), ratio=round(share, 5))

    # --- duplicados por event_id ---
    d = report["duplicates"]
    if d.get("same_file"):
        _add(out, ERROR, "duplicado", "event_id repetido dentro de un mismo archivo", column="event_id", count=d["same_file"])
    if d.get("cross_origin"):
        _add(out, ERROR, "duplicado", "el mismo event_id en stream e histórico (uuid5 incluye origin, §3.6)", column="event_id",
             count=d["cross_origin"])
    if d.get("other_file_same_origin"):
        sev = ERROR if layer == "trusted" else WARNING
        _add(out, sev, "duplicado", "event_id repetido en otro archivo del mismo origen"
             + (" (Trusted debe deduplicar, §5.7)" if layer == "trusted" else " (copias de un reinicio o de horas distintas; Trusted deduplica)"),
             column="event_id", count=d["other_file_same_origin"], examples=d.get("examples", [])[:3])

    # --- columnas: siempre nulas, constantes, versiones de NLP, colas, clases raras ---
    for g in _origin_groups(report):
        scope = _scope(g["key"])
        rows = g["rows"]
        consts = []
        for name, col in g["columns"].items():
            if rows and col["nulls"] == rows:
                _add(out, WARNING, "columna_nula", f"{name} siempre nula", column=name, scope=scope, count=rows, ratio=1.0)
            elif rows > 1 and col["distinct"] == 1 and name not in CONSTANT_IGNORED:
                top = (col.get("top") or [{}])[0].get("value")
                consts.append(f"{name}={top}" if top is not None else name)
        if consts:
            _add(out, INFO, "constante", "columnas con un solo valor: " + ", ".join(consts), scope=scope)
        nlp = g["columns"].get("nlp_model_version", {})
        if nlp.get("distinct", 0) > 1:
            _add(out, WARNING, "nlp_version", f"{nlp['distinct']} versiones de modelos NLP", column="nlp_model_version", scope=scope,
                 count=nlp["distinct"])
        fol = (g["columns"].get("followers") or {}).get("numeric", {})
        if fol.get("p50") and fol.get("p99") and fol["p99"] / max(fol["p50"], 1) > 10:
            _add(out, INFO, "cola_larga", f"followers con cola larga: p50 {fol['p50']:g}, p99 {fol['p99']:g}, máx {fol['max']:g}",
                 column="followers", scope=scope, count=fol.get("outliers_iqr"))
        emo = g["columns"].get("emotion_label", {})
        rare = [f"{t['value']} {t['ratio'] * 100:.2f} %" for t in emo.get("top", []) if t.get("ratio") is not None and t["ratio"] < RARE_CLASS_SHARE]
        if rare:
            _add(out, INFO, "clase_rara", "emociones con menos del 1 %: " + ", ".join(rare), column="emotion_label", scope=scope)
        lbl = g["columns"].get("label_sentiment", {})
        if lbl.get("rows"):
            _add(out, INFO, "ground_truth", f"label_sentiment nulo en el {lbl['null_ratio'] * 100:.1f} % (nostalgia y adiccion, ADR-13; esperado ~30 %)",
                 column="label_sentiment", scope=scope, ratio=lbl["null_ratio"])
        t = g.get("text", {})
        rep = t.get("repeated", {})
        if t.get("texts"):
            _add(out, INFO, "texto_repetido",
                 f"{rep.get('distinct_ratio', 0) * 100:.2f} % de textos distintos; {rep.get('texts_repeated', 0)} textos repetidos "
                 f"(máx {rep.get('max_repetition')} veces); esqueletos distintos {t.get('skeletons', {}).get('distinct_ratio', 0) * 100:.2f} %; "
                 f"MATTR {t.get('mattr')}, vocabulario {t.get('types')} tipos en {t.get('tokens')} tokens",
                 column="text", scope=scope, count=rep.get("rows_in_repeated"), ratio=rep.get("duplicate_text_ratio"))
        bpf = (g.get("files") or {}).get("bytes_per_file", {})
        if g["files"]["count"] > 100 and bpf.get("p50") is not None and bpf["p50"] < SMALL_FILE_BYTES:
            _add(out, INFO, "archivos_pequenos",
                 f"{g['files']['count']} archivos con mediana de {bpf['p50'] / 1024:.1f} KB y {g['files']['rows_per_file'].get('p50')} filas",
                 scope=scope, count=g["files"]["count"])
        lat = (g.get("derived", {}).get("latency_s") or {}).get("numeric", {})
        if g["key"]["origin"] == "stream" and lat.get("p95") is not None and lat["p95"] > LATENCY_P95_MAX_S:
            _add(out, WARNING, "latencia", f"p95 de process_time − event_time = {lat['p95']:.1f} s (> {LATENCY_P95_MAX_S:g} s)",
                 column="process_time", scope=scope, ratio=None)

    # --- stream vs histórico ---
    comp = report.get("comparison") or {}
    if comp.get("available"):
        for col, values in comp["categorical"].items():
            big = [f"{v}: {x['stream'] * 100:.1f} vs {x['historical'] * 100:.1f} %" for v, x in values.items()
                   if x.get("diff_pp") is not None and abs(x["diff_pp"]) >= DIFF_PP_NOTABLE]
            if big:
                _add(out, INFO, "stream_vs_historico", f"{col} (stream vs histórico): " + "; ".join(big), column=col)
        ov = comp.get("overlap", {})
        _add(out, INFO, "solape", f"{ov.get('texts_shared')} textos idénticos en los dos orígenes; "
             f"{ov.get('users_shared')} user_hash comunes ({(ov.get('users_shared_ratio_of_stream') or 0) * 100:.1f} % de los del stream)")

    # --- caché incompleta ---
    sync = report.get("sync") or {}
    listed = sync.get("objects")
    if listed is not None and listed != files["listed"]:
        _add(out, WARNING, "cache", f"la caché tiene {files['listed']} archivos y S3 listó {listed}: sincronice antes de perfilar",
             count=abs(listed - files["listed"]))
    return out
