"""Informe de *profiling* en Markdown (HU-031) a partir del JSON de ``profiler.profile_layer``."""

from __future__ import annotations

from typing import Any

from quality.profile_core import ALL
from quality.profiler import find_group

SECTIONS = ("Resumen", "KPI preliminares", "Columnas", "Distribuciones", "Tiempo y particiones", "Texto",
            "Duplicados por event_id", "Archivos y esquema", "Stream frente a histórico", "Anomalías", "Reglas propuestas")


def _pct(x: Any, nd: int = 2) -> str:
    return "—" if x is None else f"{x * 100:.{nd}f} %"


def _num(x: Any, nd: int = 2) -> str:
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:,.{nd}f}".replace(",", " ")
    if isinstance(x, int):
        return f"{x:,}".replace(",", " ")
    return str(x)


def _cell(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def _column_summary(col: dict[str, Any]) -> str:
    kind = col["kind"]
    if "numeric" in col and col["numeric"].get("count"):
        n = col["numeric"]
        return f"min {_num(n['min'])} · p50 {_num(n['p50'])} · p95 {_num(n['p95'])} · máx {_num(n['max'])}"
    if kind == "timestamp" and col.get("timestamp", {}).get("min"):
        t = col["timestamp"]
        return f"{t['min']} → {t['max']} ({_num(t['span_days'])} días)"
    if kind == "list":
        lst = col.get("list", {})
        h = lst.get("length_histogram", {})
        top = ", ".join(f"{e['value']} {e['count']}" for e in lst.get("elements_top", [])[:4])
        return f"largo 0/1/2/3+: {h.get('0', 0)}/{h.get('1', 0)}/{h.get('2', 0)}/{h.get('3+', 0)} · {top}"
    if col.get("top"):
        return ", ".join(f"{t['value']} {_pct(t['ratio'], 1)}" for t in col["top"][:4])
    if "length" in col and col["length"].get("count"):
        ln = col["length"]
        extra = f" · máx. repetición {col.get('max_frequency')}" if col.get("frequency_histogram") else ""
        return f"largo {_num(ln['min'], 0)}–{_num(ln['max'], 0)} (p50 {_num(ln['p50'], 0)}){extra}"
    return ""


def render(report: dict[str, Any]) -> str:
    out: list[str] = []
    layer = report["layer"]
    s = report["summary"]
    out.append(f"# Profiling de {layer.capitalize()} · `{report['table']}` (Lote 5.1, HU-031)\n")
    out.append(f"> Generado por `{report['profiler']}` el {report['generated_at']} (referencia `as_of` {report['as_of']}) "
               f"desde `{report['source_uri']}` en {report.get('duration_s', '—')} s. Solo agregados: sin `user_hash`, "
               "`event_id` ni textos completos. Detalle en el JSON del mismo nombre; definiciones en `docs/ARCHITECTURE.md` §3.20.\n")

    out.append("## 1. Resumen\n")
    out.append("| Grupo (origen/fuente) | Filas | Archivos | MB |")
    out.append("|---|---:|---:|---:|")
    for g in report["groups"]:
        k = g["key"]
        out.append(f"| {k['origin']}/{k['source']} | {_num(g['rows'])} | {_num(g['files']['count'])} | {g['files']['bytes'] / 1e6:.1f} |")
    out.append("")
    out.append(f"Anomalías: {s['anomalies']['error']} error, {s['anomalies']['warning']} aviso, {s['anomalies']['info']} informativas. "
               f"Reglas propuestas evaluadas: " + ", ".join(f"{v} {k}" for k, v in sorted(s["rules"].items())) + ".\n")

    out.append("## 2. KPI preliminares (§5.10)\n")
    out.append("| Origen | Completitud | Unicidad | Duplicados | Anonimización |")
    out.append("|---|---:|---:|---:|---:|")
    for origin, kpi in s["kpi"].items():
        out.append(f"| {origin} | {_pct(kpi['completitud'])} | {_pct(kpi['unicidad'], 3)} | {_pct(kpi['duplicados'], 3)} | {_pct(kpi['anonimizacion'])} |")
    out.append("")

    out.append("## 3. Columnas (nulos, cardinalidad, tipo y distribución)\n")
    for g in report["groups"]:
        if g["key"]["source"] != ALL or g["key"]["origin"] == ALL:
            continue
        out.append(f"### {layer}/{g['key']['origin']} ({_num(g['rows'])} filas)\n")
        out.append("| Columna | Tipo | Tipo físico | Nulos | Distintos | Resumen |")
        out.append("|---|---|---|---:|---:|---|")
        phys = report["files"].get("physical_types", {})
        for name, col in g["columns"].items():
            ptype = ", ".join(phys.get(name, {}).keys()) or "—"
            out.append(f"| `{name}` | {col['kind']} | {_cell(ptype)} | {_pct(col['null_ratio'])} | {_num(col['distinct'])} | {_cell(_column_summary(col))} |")
        out.append("")

    out.append("## 4. Distribuciones por origen y fuente\n")
    out.append("| Grupo | Marcas (brand_primary) | Sentimiento (NLP) | Emoción (NLP) | label_sentiment nulo | kind |")
    out.append("|---|---|---|---|---:|---|")
    for g in report["groups"]:
        if g["key"]["origin"] == ALL:
            continue
        c = g["columns"]

        def tops(col: dict[str, Any], n: int = 7) -> str:
            return ", ".join(f"{t['value']} {_pct(t['ratio'], 1)}" for t in (col or {}).get("top", [])[:n])

        kind = tops(g["derived"].get("payload_kind", {}), 2)
        out.append(f"| {g['key']['origin']}/{g['key']['source']} | {tops(c.get('brand_primary'))} | {tops(c.get('sentiment_label'))} | "
                   f"{tops(c.get('emotion_label'))} | {_pct((c.get('label_sentiment') or {}).get('null_ratio'), 1)} | {kind} |")
    out.append("")
    out.append("Seguidores, engagement y acuerdo del NLP con el *ground truth* (dato; la precisión formal es HU-033):\n")
    out.append("| Grupo | followers p50 / p99 / máx | likes p50 / máx | comments p50 | Acuerdo NLP | Usuarios | Posts por usuario (media / máx) |")
    out.append("|---|---|---|---|---:|---:|---|")
    for g in report["groups"]:
        if g["key"]["origin"] == ALL:
            continue
        f = (g["columns"].get("followers") or {}).get("numeric", {})
        lk = (g["columns"].get("likes") or {}).get("numeric", {})
        cm = (g["columns"].get("comments") or {}).get("numeric", {})
        ppu = g["users"]["posts_per_user"]
        out.append(f"| {g['key']['origin']}/{g['key']['source']} | {_num(f.get('p50'))} / {_num(f.get('p99'))} / {_num(f.get('max'))} | "
                   f"{_num(lk.get('p50'))} / {_num(lk.get('max'))} | {_num(cm.get('p50'))} | {_pct(g['nlp_agreement']['ratio'], 1)} | "
                   f"{_num(g['users']['distinct'])} | {_num(ppu.get('mean'))} / {_num(ppu.get('max'))} |")
    out.append("")

    out.append("## 5. Tiempo y particiones\n")
    out.append("| Grupo | event_time (rango) | Días | Lag ingest − event p95 (s) | Latencia process − event p50 / p95 (s) | Offset partición mín / máx (s) |")
    out.append("|---|---|---:|---:|---|---|")
    for g in report["groups"]:
        if g["key"]["source"] != ALL:
            continue
        t = g["columns"]["event_time"].get("timestamp", {})
        lag = (g["derived"]["ingest_lag_s"].get("numeric") or {})
        lat = (g["derived"]["latency_s"].get("numeric") or {})
        off = (g["derived"]["partition_offset_s"].get("numeric") or {})
        out.append(f"| {g['key']['origin']}/* | {t.get('min')} → {t.get('max')} | {_num(t.get('days'))} | {_num(lag.get('p95'))} | "
                   f"{_num(lat.get('p50'))} / {_num(lat.get('p95'))} | {_num(off.get('min'))} / {_num(off.get('max'))} |")
    out.append("")
    out.append("En el histórico, `dt`/`hr` son la fecha y hora de `event_time` y el *offset* es `event_time − hora de la partición` "
               "(0–3.600 s). En el stream son la hora de proceso en GCS y el *offset* es `process_time − hora de la partición`. "
               "La latencia del histórico no es una latencia real: `process_time` es el momento de la carga (§3.17).\n")

    out.append("## 6. Texto: diversidad léxica y repetición del generador\n")
    out.append("| Grupo | Textos | Distintos | Repetidos (filas) | Máx. repetición | Esqueletos distintos | Tokens / texto | Tipos | TTR | MATTR(500) | Herdan C | distinct-2 | Hapax |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for g in report["groups"]:
        if g["key"]["source"] != ALL and g["key"]["origin"] != ALL:
            continue
        for label, t in (("text", g["text"]), ("text_clean", g.get("text_clean"))):
            if not t:
                continue
            r = t["repeated"]
            out.append(f"| {g['key']['origin']}/{g['key']['source']} {label} | {_num(t['texts'])} | {_pct(r['distinct_ratio'])} | "
                       f"{_num(r['rows_in_repeated'])} | {_num(r['max_repetition'])} | {_pct(t['skeletons']['distinct_ratio'])} | "
                       f"{_num(t['tokens_per_text'])} | {_num(t['types'])} | {_num(t['ttr'], 4)} | {_num(t['mattr'], 4)} | "
                       f"{_num(t['herdan_c'], 4)} | {_num(t['distinct_2'], 4)} | {_pct(t['hapax_ratio'], 1)} |")
    out.append("")
    g_all = find_group(report)
    if g_all and g_all["text"]["repeated"]["top"]:
        out.append("Textos más repetidos (primeros 60 caracteres; son plantillas del generador, no datos personales):\n")
        for item in g_all["text"]["repeated"]["top"]:
            out.append(f"- {item['count']} veces: «{_cell(item['preview'])}…»")
        out.append("")
    if g_all:
        out.append("Palabras más frecuentes: " + ", ".join(f"{t['token']} ({t['count']})" for t in g_all["text"]["top_tokens"][:12]) + ".\n")
    out.append("Los duplicados de **evento** se miden por `event_id` (sección 7), no por texto: el mismo texto en dos eventos es "
               "esperable con plantillas (§3.5).\n")

    out.append("## 7. Duplicados por event_id\n")
    d = report["duplicates"]
    out.append(f"Filas {_num(d['rows'])}, `event_id` distintos {_num(d['distinct_event_ids'])}, copias {_num(d['duplicate_rows'])} "
               f"(mismo archivo {d['same_file']}, otro archivo del mismo origen {d['other_file_same_origin']}, entre orígenes {d['cross_origin']}).\n")
    for g in report["groups"]:
        if g["key"]["source"] == ALL:
            e = g["event_id"]
            out.append(f"- {g['key']['origin']}/*: unicidad {_pct(e['unique_ratio'], 3)}, copias {e['duplicate_rows']}, máx. copias {e['max_copies']}")
    out.append("")

    out.append("## 8. Archivos y esquema\n")
    f = report["files"]
    out.append(f"Archivos listados {_num(f['listed'])}, reconocidos {_num(f['recognized'])}, no reconocidos {f['unrecognized']}, "
               f"ilegibles {f['read_error_count']}, auxiliares {f['auxiliary']}. Variantes de esquema: {len(f['schema_variants'])}.\n")
    for v in f["schema_variants"]:
        out.append(f"- {v['files']} archivos: {'= contrato' if v['matches_contract'] else 'distinto del contrato'}"
                   + (f"; faltan {v['missing']}" if v["missing"] else "") + (f"; sobran {v['extra']}" if v["extra"] else "")
                   + (f"; tipos {v['kind_mismatch'] + v['type_mismatch']}" if v["kind_mismatch"] or v["type_mismatch"] else ""))
    out.append("")
    out.append("| Grupo | Archivos | Filas por archivo p50 (mín–máx) | KB por archivo p50 |")
    out.append("|---|---:|---|---:|")
    for g in report["groups"]:
        if g["files"]["count"]:
            rpf, bpf = g["files"]["rows_per_file"], g["files"]["bytes_per_file"]
            out.append(f"| {g['key']['origin']}/{g['key']['source']} | {_num(g['files']['count'])} | {_num(rpf.get('p50'), 0)} "
                       f"({_num(rpf.get('min'), 0)}–{_num(rpf.get('max'), 0)}) | {_num((bpf.get('p50') or 0) / 1024, 1)} |")
    out.append("")

    out.append("## 9. Stream frente a histórico\n")
    comp = report.get("comparison") or {}
    if comp.get("available"):
        out.append("| Variable | Valor | Stream | Histórico | Diferencia (pp) |")
        out.append("|---|---|---:|---:|---:|")
        for col, values in comp["categorical"].items():
            for v, x in values.items():
                out.append(f"| {col} | {v} | {_pct(x['stream'], 1)} | {_pct(x['historical'], 1)} | {_num(x['diff_pp'])} |")
        out.append("")
        out.append("| Métrica | Stream | Histórico |")
        out.append("|---|---:|---:|")
        for k, x in comp["numeric"].items():
            out.append(f"| {k} | {_num(x['stream'], 4)} | {_num(x['historical'], 4)} |")
        ov = comp["overlap"]
        out.append("")
        out.append(f"Solapes: {_num(ov['texts_shared'])} textos idénticos en los dos orígenes ({_pct(ov['texts_shared_ratio_of_stream'])} de los "
                   f"textos distintos del stream); {_num(ov['users_shared'])} `user_hash` comunes ({_pct(ov['users_shared_ratio_of_stream'])} "
                   f"de los {_num(ov['users_stream'])} del stream; el histórico tiene {_num(ov['users_historical'])}).\n")
    else:
        out.append("Solo hay un origen en esta capa.\n")

    out.append("## 10. Anomalías\n")
    out.append("| Gravedad | Tipo | Columna | Alcance | Filas | Detalle |")
    out.append("|---|---|---|---|---:|---|")
    order = {"error": 0, "warning": 1, "info": 2}
    for a in sorted(report["anomalies"], key=lambda a: (order.get(a["severity"], 3), a["kind"], a["scope"])):
        out.append(f"| {a['severity']} | {a['kind']} | {a['column']} | {a['scope']} | {_num(a.get('count'))} | {_cell(a['detail'])} |")
    out.append("")

    out.append("## 11. Reglas propuestas para HU-032 (Lote 5.2)\n")
    out.append("Catálogo completo con expectativa, predicado SQL y motivo en `docs/ARCHITECTURE.md` §5.10. Estado con los datos de hoy:\n")
    out.append("| Regla | Columna | Comprobación | Umbral | KPI | Gravedad | Observado | Estado |")
    out.append("|---|---|---|---|---|---|---|---|")
    for r in report["rules"]:
        obs = ", ".join(f"{k}: {_cell(v)}" for k, v in r["observed"].items()) or "—"
        out.append(f"| {r['id']} | `{r['column']}` | `{r['check']}` | {_cell(r['threshold'])} | {r['kpi']} | {r['severity']} | {obs} | {r['status']} |")
    out.append("")
    return "\n".join(out)
