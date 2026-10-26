"""CLI del verificador del Lote 2.4 (``scripts/verify_trends.py``).

Comandos:

``tables``     T1: tablas ``trend_windows``, ``dlq_events``, ``dq_results`` y la vista.
``report``     Ventanas de tendencias desde ``--since``: reglas de cada fila, una ventana por
               fuente, conciliación con los posts (``posts_enriched`` o ``enriched/`` local),
               arranque en frío, z-scores, latencia p95 y, con ``--watch-minutes``, la
               latencia de detección. ``--local`` lee el lake local. ``--write-dq`` guarda
               los KPI en ``dq_results`` (capa ``stream``). ``--wait-caught-up-minutes`` espera a que
               el job se ponga al día antes de medir (tras un cambio de job hay backlog).
``dlq-check``  Publica tres mensajes defectuosos en ``ta-raw-reddit`` y espera verlos en
               ``dlq_events`` con su ``error_type``. Exige ``--allow-cloud``.

Salida: 0 pasa (con o sin avisos), 1 falla, 2 error de uso o de infraestructura.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from streaming.trends_check import (
    AVISO,
    FALLA,
    OK,
    Check,
    check_cold_start,
    check_keys,
    check_switch_alerts,
    check_latency,
    check_reconciliation,
    check_rows,
    check_table_schemas,
    check_zscores,
    compare_windows,
    detection_latencies,
    load_local_posts,
    load_local_trends,
    local_latency,
    verdict,
    versions_summary,
)
from streaming.trends_core import TrendConfig, format_ts, to_epoch_ms

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_LAKE = PROJECT_DIR / ".local-lake"
DEFECTS = (
    ("truncated_json", "json"),
    ("missing_fields", "schema"),
    ("not_utf8", "encoding"),
)


MIN_WATCH_MINUTES = 6.0


class UsageError(RuntimeError):
    pass


def _since_ms(args, now_ms: int) -> int:
    if args.since:
        value = to_epoch_ms(args.since)
        if value is None:
            raise UsageError(f"--since no es una fecha ISO-8601: {args.since!r}")
        return value
    return now_ms - int(args.hours * 3_600_000)


def _ceil(ms: int, step: int) -> int:
    return -(-ms // step) * step


def _project(args) -> str:
    project = args.project or os.environ.get("GCP_PROJECT")
    if not project:
        raise UsageError("falta el proyecto: --project o GCP_PROJECT")
    return project


def _print_checks(title: str, checks: Sequence[Check]) -> None:
    print(f"\n== {title} ==")
    for check in checks:
        print(f"[{check.status:5}] {check.id} {check.title}: {check.detail}")
    print(f"Resultado: {verdict(checks)}")


def _write_json(path: Path | None, payload: dict) -> None:
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _exit_code(checks: Sequence[Check]) -> int:
    return 1 if verdict(checks) == FALLA else 0


# --------------------------------------------------------------------------- report

def _report_checks(rows, posts, cfg, since_ms, now_ms, settle_ms, min_match) -> tuple[list[Check], dict]:
    in_period = [r for r in rows if (to_epoch_ms(r.get("window_start")) or 0) >= since_ms - cfg.window_ms]
    lo = _ceil(since_ms, cfg.period_ms)
    hi = now_ms - settle_ms
    result = compare_windows(in_period, posts, cfg, lo, hi)
    checks = [
        check_keys([r for r in in_period if (to_epoch_ms(r.get("window_start")) or 0) >= lo]),
        check_rows(in_period, cfg),
        check_reconciliation(result, min_ok=min_match),
        check_cold_start(in_period, cfg, since_ms),
        check_switch_alerts(in_period, cfg, since_ms),
        check_zscores(in_period),
    ]
    return checks, {"versions": versions_summary(in_period)}


def cmd_report_local(args, cfg: TrendConfig, clock) -> tuple[list[Check], dict]:
    now_ms = int(clock() * 1000)
    since_ms = _since_ms(args, now_ms)
    lake = Path(args.lake)
    rows = load_local_trends(lake)
    invalid = [r for r in rows if "_invalid_line" in r]
    rows = [r for r in rows if "_invalid_line" not in r]
    posts = load_local_posts(lake, since_ms - cfg.window_ms)
    checks, extra = _report_checks(rows, posts, cfg, since_ms, now_ms, int(args.settle_minutes * 60_000),
                                   args.min_match)
    if invalid:
        checks.append(Check("T3b", "Líneas JSON válidas en trend_windows/", FALLA, f"{len(invalid)} líneas inválidas"))
    checks.append(check_latency(local_latency(posts, since_ms + int(args.warmup_minutes * 60_000))))
    return checks, {"mode": "local", "lake": str(lake), "since": format_ts(since_ms), **extra}


def _window_lag_s(backend, cfg: TrendConfig, now_ms: int) -> float | None:
    """Segundos entre ahora y el fin de la ventana más reciente en trend_windows."""
    keys = backend.window_keys(now_ms - 30 * 60_000)
    if not keys:
        return None
    return (now_ms - (max(k[0] for k in keys) + cfg.window_ms)) / 1000


def _wait_caught_up(backend, cfg: TrendConfig, minutes: float, threshold_s: float, interval: float,
                    clock, sleep) -> dict:
    """Espera a que el job se ponga al día: la ventana más reciente cerró hace ≤ ``threshold_s``.

    Tras un cambio de job, el nuevo procesa primero el backlog y la marca de agua va atrasada;
    medir la detección en ese rato mide el arranque, no la operación normal.
    """
    start_ms = int(clock() * 1000)
    deadline = start_ms + int(minutes * 60_000)
    history = []
    while True:
        now_ms = int(clock() * 1000)
        lag = _window_lag_s(backend, cfg, now_ms)
        history.append({"at": format_ts(now_ms), "lag_s": lag})
        if lag is not None and lag <= threshold_s:
            return {"caught_up": True, "at_ms": now_ms, "waited_s": round((now_ms - start_ms) / 1000, 1),
                    "lag_s": lag, "threshold_s": threshold_s, "history": history[-30:]}
        if now_ms >= deadline:
            return {"caught_up": False, "at_ms": None, "waited_s": round((now_ms - start_ms) / 1000, 1),
                    "lag_s": lag, "threshold_s": threshold_s, "history": history[-30:]}
        print(f"  … ventana más reciente cerró hace {lag if lag is None else round(lag)} s; esperando "
              f"≤ {threshold_s:.0f} s", flush=True)
        sleep(interval)


def _watch(backend, cfg: TrendConfig, minutes: float, interval: float, clock, sleep,
           horizon_s: float = 300.0) -> dict:
    """Observa cuándo aparece cada ventana × marca × fuente que cierra durante la observación.

    Solo cuentan las ventanas que cerraron al menos ``horizon_s`` (= el umbral de 5 min) antes del
    final: para ellas se sabe si aparecieron a tiempo. Las que no aparecieron entran con su cota
    inferior (``censored``, ≥ 300 s), así un job atrasado o detenido falla en vez de quedarse sin
    datos. Por eso la observación debe durar más de 5 min (``MIN_WATCH_MINUTES``).
    """
    start_ms = int(clock() * 1000)
    lookback = start_ms - 30 * 60_000
    known = set(backend.window_keys(lookback))
    first_seen: dict[tuple, int] = {}
    deadline = start_ms + int(minutes * 60_000)
    polls = 0
    while int(clock() * 1000) < deadline:
        sleep(interval)
        polls += 1
        now_ms = int(clock() * 1000)
        for key in backend.window_keys(lookback):
            if key in known or key in first_seen:
                continue
            if key[0] + cfg.window_ms <= start_ms:
                continue  # la ventana ya había cerrado al empezar: no vimos cuándo apareció
            first_seen[key] = now_ms
    end_ms = int(clock() * 1000)
    horizon_ms = int(horizon_s * 1000)
    cutoff = end_ms - horizon_ms
    pairs = {(brand, source) for (_start, brand, source) in known | set(first_seen)}
    observed = {k: v for k, v in first_seen.items() if k[0] + cfg.window_ms <= cutoff}
    censored = {}
    for window_end in range(_ceil(start_ms + 1, cfg.period_ms), cutoff + 1, cfg.period_ms):
        for brand, source in pairs:
            key = (window_end - cfg.window_ms, brand, source)
            if key not in first_seen and key not in known:
                censored[key] = end_ms
    stats = detection_latencies(observed, cfg.window_ms, censored)
    stats.update({"polls": polls, "interval_s": interval, "horizon_s": horizon_ms / 1000,
                  "keys_seen_total": len(first_seen), "pairs": len(pairs)})
    return stats


def _dq_rows(run_id: str, now_ms: int, checks: Sequence[Check], latency: dict, detection: dict | None) -> list[dict]:
    run_ts = format_ts(now_ms)
    rows = []

    def add(table, check_name, kpi, observed, threshold, passed, details):
        rows.append({"run_id": run_id, "run_ts": run_ts, "layer": "stream", "table_name": table,
                     "suite": "verify_trends", "check_name": check_name, "kpi": kpi,
                     "observed_value": observed, "threshold": threshold, "passed": bool(passed),
                     "details": json.dumps(details, ensure_ascii=False, default=str)})

    if latency.get("n"):
        add("posts_enriched", "latency_event_to_process_p95", "latencia", latency["p95"], 300.0,
            latency["p95"] < 300, latency)
    if detection and detection.get("n"):
        add("trend_windows", "trend_detection_latency_p95", "latencia", detection["p95"], 300.0,
            detection["p95"] < 300, detection)
    by_id = {c.id: c for c in checks}
    if "T4" in by_id and by_id["T4"].data.get("compared"):
        data = by_id["T4"].data
        add("trend_windows", "trend_windows_match_posts", "otro", data["ratio"], 0.99,
            by_id["T4"].status == OK, {k: data[k] for k in ("compared", "matched", "lo", "hi")})
    if "T5" in by_id:
        add("trend_windows", "trend_cold_start_no_alerts", "otro",
            float(len(by_id["T5"].data.get("alerts_in_cold_start", []))), 0.0,
            by_id["T5"].status == OK, by_id["T5"].data)
    return rows


def cmd_report_cloud(args, cfg: TrendConfig, backend, clock, sleep) -> tuple[list[Check], dict]:
    schemas = {t: backend.table_columns(t) for t in ("trend_windows", "dlq_events", "dq_results",
                                                     "trend_windows_latest")}
    checks = [check_table_schemas(schemas)]
    if checks[0].status == FALLA:
        return checks, {"mode": "cloud"}
    detection = None
    caught = None
    if args.watch_minutes > 0 and args.wait_caught_up_minutes > 0:
        print(f"Esperando a que el job se ponga al día (hasta {args.wait_caught_up_minutes} min)…", flush=True)
        caught = _wait_caught_up(backend, cfg, args.wait_caught_up_minutes, args.caught_up_seconds,
                                 args.poll_seconds, clock, sleep)
    if args.watch_minutes > 0 and (caught is None or caught["caught_up"]):
        print(f"Observando trend_windows durante {args.watch_minutes} min…", flush=True)
        detection = _watch(backend, cfg, args.watch_minutes, args.poll_seconds, clock, sleep)
    now_ms = int(clock() * 1000)
    since_ms = _since_ms(args, now_ms)
    rows = backend.trend_rows(since_ms - cfg.window_ms, now_ms + cfg.window_ms)
    posts = backend.posts(_ceil(since_ms, cfg.period_ms) - cfg.window_ms, now_ms)
    report_checks, extra = _report_checks(rows, posts, cfg, since_ms, now_ms,
                                          int(args.settle_minutes * 60_000), args.min_match)
    checks.extend(report_checks)
    steady_from = since_ms + int(args.warmup_minutes * 60_000)
    if caught and caught["caught_up"]:
        steady_from = max(steady_from, caught["at_ms"])
    latency = backend.latency(steady_from)
    latency_all = backend.latency(since_ms)
    checks.append(check_latency(latency))
    detection_title = "Detección: fin de ventana → fila en trend_windows (p95 < 5 min)"
    if caught is not None and not caught["caught_up"]:
        checks.append(Check("T8", detection_title, FALLA,
                            f"el job no se puso al día en {args.wait_caught_up_minutes} min: la ventana más "
                            f"reciente cerró hace {caught['lag_s']} s (umbral {caught['threshold_s']:.0f} s)",
                            dict(caught)))
    elif detection is not None and not detection.get("n"):
        checks.append(Check("T8", detection_title, FALLA,
                            f"ninguna ventana cerró y apareció durante los {args.watch_minutes} min de observación "
                            "(¿simulador apagado o job detenido?)", dict(detection)))
    elif detection is not None:
        check = check_latency(detection, check_id="T8", title=detection_title)
        if detection.get("censored"):
            check.detail += f" · {detection['censored']} sin aparecer al terminar (contadas con su cota inferior)"
        checks.append(check)
    extra.update({"mode": "cloud", "project": backend.project, "since": format_ts(since_ms),
                  "steady_from": format_ts(steady_from), "caught_up": caught,
                  "latency_steady": latency, "latency_all": latency_all, "detection": detection})
    if args.write_dq:
        run_id = f"verify-trends-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{secrets.token_hex(2)}"
        dq_rows = _dq_rows(run_id, now_ms, checks, latency, detection)
        errors = backend.insert_dq(dq_rows)
        status = FALLA if errors else OK
        checks.append(Check("T9", "KPI guardados en dq_results", status,
                            f"{len(dq_rows)} filas, run_id {run_id}" + (f"; errores: {errors}" if errors else ""),
                            {"run_id": run_id, "rows": len(dq_rows)}))
    return checks, extra


# --------------------------------------------------------------------------- dlq-check

def _defect_body(kind: str, marker: str) -> bytes:
    if kind == "truncated_json":
        return f'{{"ta_verify": "{marker}", "truncado": '.encode()
    if kind == "missing_fields":
        return json.dumps({"schema_version": "1.0", "ta_verify": marker}).encode()
    if kind == "not_utf8":
        return b"\xff\xfe " + marker.encode() + b" \xff"
    raise ValueError(kind)


def cmd_dlq_check(args, backend, clock, sleep) -> tuple[list[Check], dict]:
    run = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"
    marker = f"ta-verify-dlqbq-{run}"
    since_ms = int(clock() * 1000) - 60_000
    published = {}
    for kind, _expected in DEFECTS:
        attributes = {"event_id": str(uuid.uuid4()), "source": "reddit", "origin": "stream", "schema_version": "1.0"}
        published[kind] = backend.publish("ta-raw-reddit", _defect_body(kind, marker), attributes)
    checks = [Check("D1", "Defectos publicados en ta-raw-reddit", OK, f"{len(published)} mensajes, marca {marker}",
                    {"message_ids": published})]
    deadline = clock() + args.timeout
    found: list[dict] = []
    while True:
        found = backend.dlq_rows(since_ms, marker)
        kinds = {_kind_of(r) for r in found}
        if all(k in kinds for k, _ in DEFECTS) or clock() >= deadline:
            break
        sleep(args.poll_seconds)
    elapsed = round(clock() - (since_ms / 1000 + 60), 1)
    problems, copies = [], {}
    for kind, expected in DEFECTS:
        rows = [r for r in found if _kind_of(r) == kind]
        copies[kind] = len(rows)
        if not rows:
            problems.append(f"{kind}: no llegó a dlq_events")
            continue
        if any(r.get("error_type") != expected for r in rows):
            problems.append(f"{kind}: error_type {[r.get('error_type') for r in rows]} (esperado {expected})")
        if any(r.get("source") != "reddit" for r in rows):
            problems.append(f"{kind}: source {[r.get('source') for r in rows]}")
    status = FALLA if problems else OK
    checks.append(Check("D2", "Cada defecto en dlq_events con su error_type", status,
                        "; ".join(problems) or f"3/3 en {elapsed} s", {"rows": found, "elapsed_s": elapsed}))
    repeated = {k: v for k, v in copies.items() if v > 1}
    checks.append(Check("D3", "Sin copias repetidas en dlq_events", AVISO if repeated else OK,
                        f"copias {copies}", copies))
    return checks, {"marker": marker, "elapsed_s": elapsed}


def _kind_of(row: dict) -> str | None:
    raw = row.get("raw_message") or ""
    if raw.startswith("\\xff"):
        return "not_utf8"
    if raw.lstrip().startswith("{") and not raw.rstrip().endswith("}"):
        return "truncated_json"
    if raw.lstrip().startswith("{"):
        return "missing_fields"
    return None


# --------------------------------------------------------------------------- main

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="verify_trends.py", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", help="Proyecto de GCP (por defecto GCP_PROJECT).")
    parser.add_argument("--dataset", default="ta_stream")
    parser.add_argument("--json-out", type=Path, help="Guarda el reporte en JSON (evidencia).")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("tables", help="Comprueba las tablas y la vista (T1).")

    r = sub.add_parser("report", help="Revisa trend_windows desde --since.")
    r.add_argument("--local", action="store_true", help="Lee el lake local en vez de BigQuery.")
    r.add_argument("--lake", default=str(DEFAULT_LAKE), help="Lake local (por defecto project/.local-lake).")
    r.add_argument("--since", help="Desde cuándo (ISO-8601 UTC): normalmente el arranque del job.")
    r.add_argument("--hours", type=float, default=1.0, help="Sin --since: las últimas N horas.")
    r.add_argument("--settle-minutes", type=float, default=3.0,
                   help="No concilia las ventanas que cerraron hace menos de N minutos.")
    r.add_argument("--warmup-minutes", type=float, default=10.0,
                   help="La latencia p95 se mide desde --since + N minutos (sin el backlog del arranque).")
    r.add_argument("--watch-minutes", type=float, default=0.0,
                   help="Nube: observa N minutos cuándo aparecen las ventanas (latencia de detección).")
    r.add_argument("--wait-caught-up-minutes", type=float, default=0.0,
                   help="Nube: antes de observar, espera hasta N min a que el job se ponga al día.")
    r.add_argument("--caught-up-seconds", type=float, default=150.0,
                   help="Al día = la ventana más reciente cerró hace ≤ N s.")
    r.add_argument("--poll-seconds", type=float, default=20.0)
    r.add_argument("--min-match", type=float, default=0.99)
    r.add_argument("--write-dq", action="store_true", help="Nube: guarda los KPI en dq_results.")
    r.add_argument("--window-seconds", type=int, default=300)
    r.add_argument("--period-seconds", type=int, default=60)
    r.add_argument("--baseline-windows", type=int, default=60)

    d = sub.add_parser("dlq-check", help="Defectos → ta-dlq → dlq_events.")
    d.add_argument("--allow-cloud", action="store_true", help="Necesario: publica en Pub/Sub de verdad.")
    d.add_argument("--timeout", type=float, default=480.0)
    d.add_argument("--poll-seconds", type=float, default=15.0)
    return parser


def main(argv: Sequence[str] | None = None, backend_factory: Callable | None = None,
         clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep) -> int:
    args = build_parser().parse_args(argv)
    try:
        if getattr(args, "watch_minutes", 0) and args.watch_minutes < MIN_WATCH_MINUTES:
            raise UsageError(f"--watch-minutes debe ser ≥ {MIN_WATCH_MINUTES:g}: solo cuentan las ventanas que "
                             "cerraron al menos 5 min antes del final de la observación")
        if args.command == "report" and args.local:
            cfg = TrendConfig(window_seconds=args.window_seconds, period_seconds=args.period_seconds,
                              baseline_windows=args.baseline_windows)
            checks, extra = cmd_report_local(args, cfg, clock)
        else:
            if args.command == "dlq-check" and not args.allow_cloud:
                raise UsageError("dlq-check publica en Pub/Sub de verdad: añada --allow-cloud")
            project = _project(args)
            factory = backend_factory or (lambda p, ds: __import__(
                "streaming.trends_bq", fromlist=["GoogleCloudBackend"]).GoogleCloudBackend(p, ds))
            backend = factory(project, args.dataset)
            if args.command == "tables":
                schemas = {t: backend.table_columns(t) for t in ("trend_windows", "dlq_events", "dq_results",
                                                                 "trend_windows_latest")}
                checks, extra = [check_table_schemas(schemas)], {"project": project}
            elif args.command == "report":
                cfg = TrendConfig(window_seconds=args.window_seconds, period_seconds=args.period_seconds,
                                  baseline_windows=args.baseline_windows)
                checks, extra = cmd_report_cloud(args, cfg, backend, clock, sleep)
            else:
                checks, extra = cmd_dlq_check(args, backend, clock, sleep)
    except UsageError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except ImportError as exc:
        print(f"ERROR: falta una dependencia ({exc}); use el entorno ~/.venvs/ta-streaming", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - credenciales, red, tabla inexistente…
        if type(exc).__module__.startswith(("google.", "grpc")):
            print(f"ERROR de GCP: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 2
        raise
    _print_checks(f"verify_trends {args.command}", checks)
    payload = {"command": args.command, "verdict": verdict(checks), "checks": [c.as_dict() for c in checks], **extra}
    _write_json(args.json_out, payload)
    return _exit_code(checks)
