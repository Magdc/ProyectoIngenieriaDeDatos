"""Verificador del pipeline de streaming (Lote 2.1) · `scripts/verify_streaming.py`.

Comandos:

* ``report`` – resume `raw/` y `dlq/` de las últimas horas: archivos, líneas,
  `event_id` únicos, copias repetidas y DLQ por tipo de error. Solo lee.
* ``check``  – publica un centinela (dos veces) y un mensaje por tipo de error,
  espera a que el pipeline los procese y comprueba que el centinela aparece
  **una sola vez** en `raw/`, que cada defecto llega a `dlq/` con su
  `error_type` y, si se pide, que también llega al topic `ta-dlq`.

Salida: 0 = pasa · 1 = falla · 2 = error de uso o de infraestructura.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from streaming.config import KNOWN_SOURCES, lake_bucket_name
from streaming.inject import Plan, build_plan, publish
from streaming.lake_report import open_lake, scan

PASS, FAIL, ERROR = 0, 1, 2


# --------------------------------------------------------------------------- evaluación pura

def evaluate(plan: Plan, lake_scan, dlq_messages: list[dict] | None) -> list[dict]:
    """Compara lo publicado con lo encontrado. Cada control: name, status (ok/fail), detail."""
    checks = []
    copies = lake_scan.raw_lines_with(plan.sentinel_id)
    checks.append({
        "name": "sentinel_in_raw_once", "status": "ok" if copies == 1 else "fail",
        "detail": f"{copies} copia(s) del centinela {plan.sentinel_id} en raw/ (esperada 1; se publicó 2 veces)"})

    expected = {m.event_id: (m.name, m.expect) for m in plan.messages if m.expect != "raw"}
    found: dict[str, list[dict]] = {eid: [] for eid in expected}
    for row in lake_scan.dlq_rows:
        raw = str(row.get("raw_message", ""))
        for eid in expected:
            if eid in raw:
                found[eid].append(row)
    for eid, (name, err) in expected.items():
        rows = found[eid]
        types = sorted({r.get("error_type") for r in rows})
        ok = len(rows) == 1 and types == [err]
        checks.append({"name": f"dlq_file:{name}", "status": "ok" if ok else "fail",
                       "detail": f"esperado 1 registro '{err}'; encontrados {len(rows)} {types}"})

    for eid, (name, _) in expected.items():
        if lake_scan.raw_lines_with(eid):
            checks.append({"name": f"not_in_raw:{name}", "status": "fail",
                           "detail": f"{name} ({eid}) apareció en raw/"})

    if dlq_messages is not None:
        for eid, (name, err) in expected.items():
            got = [m for m in dlq_messages if m.get("attributes", {}).get("event_id") == eid]
            types = sorted({m["attributes"].get("dlq_error_type") for m in got})
            ok = len(got) >= 1 and types == [err]
            checks.append({"name": f"dlq_topic:{name}", "status": "ok" if ok else "fail",
                           "detail": f"esperado '{err}' en ta-dlq; recibidos {len(got)} {types}"})
    return checks


def traffic_checks(lake_scan, max_extra_copies: int) -> list[dict]:
    summary = lake_scan.summary()
    extra = summary["raw_total_extra_copies"]
    bad = sum(v["bad_lines"] for v in summary["raw"].values())
    return [
        {"name": "raw_no_duplicates", "status": "ok" if extra <= max_extra_copies else "fail",
         "detail": f"{extra} copia(s) repetida(s) por event_id en raw/ (máximo {max_extra_copies})"},
        {"name": "raw_lines_valid", "status": "ok" if bad == 0 else "fail",
         "detail": f"{bad} línea(s) de raw/ que no son JSON o no coinciden con su partición"},
    ]


# --------------------------------------------------------------------------- Pub/Sub (ta-dlq)

class DlqWatcher:
    """Suscripción temporal `ta-verify-dlq-{run}` sobre `ta-dlq` (§5.3: nadie depende de ella)."""

    def __init__(self, project: str, topic: str, run_id: str, subscriber=None):
        from google.cloud import pubsub_v1

        self._client = subscriber or pubsub_v1.SubscriberClient()
        self.topic = f"projects/{project}/topics/{topic}"
        self.name = f"projects/{project}/subscriptions/ta-verify-dlq-{run_id}"
        self.messages: list[dict] = []

    def create(self) -> None:
        request = {"name": self.name, "topic": self.topic, "ack_deadline_seconds": 60}
        if not os.environ.get("PUBSUB_EMULATOR_HOST"):
            request.update({
                "expiration_policy": {"ttl": {"seconds": 86400}},
                "message_retention_duration": {"seconds": 600},
                "labels": {"project": "trend-analyzer", "env": os.environ.get("ENV", "dev"),
                           "owner": "alejandro", "component": "streaming-verify"},
            })
        self._client.create_subscription(request=request)

    def drain(self) -> None:
        while True:
            resp = self._client.pull(request={"subscription": self.name, "max_messages": 100},
                                     timeout=30)
            if not resp.received_messages:
                return
            for rm in resp.received_messages:
                self.messages.append({"attributes": dict(rm.message.attributes),
                                      "size": len(rm.message.data)})
            self._client.acknowledge(request={"subscription": self.name,
                                              "ack_ids": [rm.ack_id for rm in resp.received_messages]})

    def delete(self) -> None:
        try:
            self._client.delete_subscription(request={"subscription": self.name})
        except Exception:  # noqa: BLE001 - expira sola a 1 día
            pass


# --------------------------------------------------------------------------- comandos

def _root(args) -> str:
    if args.root:
        return args.root
    project = args.project or os.environ.get("GCP_PROJECT")
    if not project:
        raise SystemExit("falta --root o GCP_PROJECT")
    return f"gs://{lake_bucket_name(project)}"


def _write_json(path: str | None, data: dict) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _print_checks(checks: list[dict]) -> None:
    for c in checks:
        mark = "OK  " if c["status"] == "ok" else "FALLA"
        print(f"  [{mark}] {c['name']}: {c['detail']}")


def _print_summary(summary: dict) -> None:
    print(f"Lake: {summary['root']}  (horas {summary['hours_scanned'][0]} → {summary['hours_scanned'][-1]})")
    for source, st in summary["raw"].items():
        print(f"  raw/{source:<9} archivos={st['files']:<4} líneas={st['lines']:<6} "
              f"únicos={st['unique_event_ids']:<6} copias_repetidas={st['extra_copies']} "
              f"líneas_malas={st['bad_lines']}")
    d = summary["dlq"]
    print(f"  dlq/ archivos={d['files']} registros={d['records']} por_tipo={d['by_error_type']} "
          f"por_fuente={d['by_source']}")


def cmd_report(args) -> int:
    lake = open_lake(_root(args))
    result = scan(lake, args.sources, hours=args.hours)
    summary = result.summary()
    checks = traffic_checks(result, args.max_extra_copies)
    _print_summary(summary)
    _print_checks(checks)
    if summary["raw_total_lines"] == 0:
        print("  Aviso: no hay archivos en raw/ en esas horas. ¿Está corriendo el pipeline "
              "(y el simulador)? Los archivos aparecen al cerrar cada ventana (--raw_window_seconds).")
    status = PASS if all(c["status"] == "ok" for c in checks) else FAIL
    _write_json(args.json_out, {"command": "report", "verdict": "ok" if status == PASS else "fail",
                                "summary": summary, "checks": checks})
    return status


def cmd_check(args) -> int:
    project = args.project or os.environ.get("GCP_PROJECT")
    if not project:
        print("falta --project o GCP_PROJECT", file=sys.stderr)
        return ERROR
    emulator = bool(os.environ.get("PUBSUB_EMULATOR_HOST"))
    if not emulator and not args.allow_cloud:
        print("Sin PUBSUB_EMULATOR_HOST esto publica en los topics reales de GCP. "
              "Repita con --allow-cloud si es lo que quiere.", file=sys.stderr)
        return ERROR

    root = _root(args)
    lake = open_lake(root)
    run_id = time.strftime("%Y%m%d%H%M%S", time.gmtime()) + "-" + secrets.token_hex(2)
    plan = build_plan(run_id)

    watcher = None
    try:
        if args.watch_dlq:
            watcher = DlqWatcher(project, args.dlq_topic, run_id)
            watcher.create()
        print(f"Corrida {run_id}: publicando {len(plan.messages)} mensajes "
              f"(centinela {plan.sentinel_id}) en {'el emulador' if emulator else 'GCP'}…")
        publish(plan, project, args.topic_template)
    except Exception as exc:  # noqa: BLE001
        print(f"No se pudo preparar la verificación: {exc}", file=sys.stderr)
        if watcher:
            watcher.delete()
        return ERROR

    deadline = time.monotonic() + args.timeout
    checks: list[dict] = []
    result = None
    try:
        while True:
            time.sleep(args.poll)
            if watcher:
                watcher.drain()
            result = scan(lake, KNOWN_SOURCES, hours=1)
            checks = evaluate(plan, result, watcher.messages if watcher else None)
            pending = [c for c in checks if c["status"] != "ok"]
            waited = int(args.timeout - (deadline - time.monotonic()))
            print(f"  … {waited:>4} s: {len(checks) - len(pending)}/{len(checks)} controles OK")
            if not pending:
                # Una vuelta más: la copia duplicada no debe aparecer más tarde.
                time.sleep(args.poll)
                result = scan(lake, KNOWN_SOURCES, hours=1)
                checks = evaluate(plan, result, watcher.messages if watcher else None)
                break
            if time.monotonic() >= deadline:
                break
    finally:
        if watcher:
            watcher.delete()

    checks += traffic_checks(result, args.max_extra_copies)
    status = PASS if all(c["status"] == "ok" for c in checks) else FAIL
    print(f"Resultado ({'OK' if status == PASS else 'FALLA'}):")
    _print_checks(checks)
    _print_summary(result.summary())
    if status != PASS and result.summary()["raw_total_lines"] == 0:
        print("  Pista: raw/ está vacío. Compruebe que el pipeline corre (local: "
              "`scripts/local_dataflow.sh logs`; GCP: `scripts/dataflow_job.sh status`) y "
              "que --timeout supera la ventana del pipeline más un margen.")
    _write_json(args.json_out, {
        "command": "check", "verdict": "ok" if status == PASS else "fail",
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "target": "emulator" if emulator else "cloud", "plan": plan.manifest(),
        "checks": checks, "dlq_topic_messages": len(watcher.messages) if watcher else None,
        "summary": result.summary()})
    return status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="verify_streaming.py", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--root", help="Raíz del lake (gs://… o carpeta). Por defecto gs://{GCP_PROJECT}-ta-lake.")
        p.add_argument("--project", help="Proyecto (por defecto GCP_PROJECT).")
        p.add_argument("--max-extra-copies", type=int, default=0,
                       help="Copias repetidas por event_id toleradas en raw/ (por defecto 0).")
        p.add_argument("--json-out", help="Guarda el resultado en JSON (evidencia).")

    rep = sub.add_parser("report", help="Resumen de raw/ y dlq/ (solo lectura).")
    common(rep)
    rep.add_argument("--hours", type=int, default=2, help="Horas hacia atrás (por defecto 2).")
    rep.add_argument("--sources", nargs="+", default=list(KNOWN_SOURCES), choices=KNOWN_SOURCES)
    rep.set_defaults(func=cmd_report)

    chk = sub.add_parser("check", help="Inyecta mensajes de prueba y verifica raw/, dlq/ y ta-dlq.")
    common(chk)
    chk.add_argument("--timeout", type=int, default=600, help="Espera máxima en segundos (600).")
    chk.add_argument("--poll", type=int, default=15, help="Segundos entre lecturas (15).")
    chk.add_argument("--topic-template", default="ta-raw-{source}")
    chk.add_argument("--dlq-topic", default="ta-dlq")
    chk.add_argument("--no-watch-dlq", dest="watch_dlq", action="store_false",
                     help="No comprobar el topic ta-dlq.")
    chk.add_argument("--allow-cloud", action="store_true",
                     help="Necesario para publicar en los topics reales de GCP.")
    chk.set_defaults(func=cmd_check)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return ERROR
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return ERROR
