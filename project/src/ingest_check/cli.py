"""CLI del verificador de ingesta (Lote 1.4, HU-017).

Dos comandos:

``measure``
    Se suscribe a ``ta-raw-{source}`` durante una ventana y reporta, por fuente,
    tasa de mensajes, ``event_id`` únicos, ``seq`` actual (observado y del
    cursor) e integridad básica (JSON, atributos, schema opcional).

``restart-check``
    Observa antes y después de reiniciar el servicio y comprueba que el ``seq``
    continúa (no vuelve a 0) y que el cursor no retrocede.

Códigos de salida: 0 = pasa (puede haber avisos), 1 = falla, 2 = error de uso o
de infraestructura (topic inexistente, sin credenciales, …).
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

from ingest_check import __version__
from ingest_check.cursor import (
    CursorReader,
    GcsCursorReader,
    LocalCursorReader,
    NullCursorReader,
    read_all,
)
from ingest_check.stats import (
    FAIL,
    OK,
    WARN,
    Check,
    CursorState,
    IngestStats,
    Thresholds,
    cursor_checks,
    measure_checks,
    restart_checks,
    summarize,
    verdict,
)

DEFAULT_SOURCES = "reddit,facebook,instagram"
DEFAULT_TOPIC_TEMPLATE = "ta-raw-{source}"
DEFAULT_SERVICE = "ta-stream-simulator"
RESTART_NONCE_VAR = "TA_RESTART_NONCE"


class Backend(Protocol):
    def setup(self) -> dict[str, str]: ...

    def start(self, on_message) -> None: ...

    def stop(self) -> None: ...

    def teardown(self) -> None: ...


@dataclass
class Runtime:
    """Dependencias inyectables (las pruebas las reemplazan)."""

    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    prompt: Callable[[str], str] = input
    run: Callable[[list[str]], int] = field(default=lambda cmd: subprocess.call(cmd))
    log: Callable[[str], None] = field(default=lambda m: print(m, file=sys.stderr, flush=True))
    stdin_is_tty: Callable[[], bool] = field(default=lambda: sys.stdin.isatty())


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "si", "sí"}


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Argumentos


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    g = common.add_argument_group("entorno")
    g.add_argument("--project", default=os.environ.get("GCP_PROJECT"),
                   help="Proyecto GCP (por defecto GCP_PROJECT).")
    g.add_argument("--sources", default=os.environ.get("SIM_SOURCES", DEFAULT_SOURCES),
                   help="Fuentes separadas por coma (por defecto SIM_SOURCES o las tres).")
    g.add_argument("--topic-template", default=os.environ.get("SIM_TOPIC_TEMPLATE", DEFAULT_TOPIC_TEMPLATE),
                   help="Plantilla del topic (por defecto ta-raw-{source}).")
    g.add_argument("--subscription", action="append", default=[], metavar="FUENTE=NOMBRE",
                   help="Usar una suscripción existente para esa fuente en vez de crear una temporal. "
                        "No use ta-raw-{source}-df: es de Dataflow.")
    g.add_argument("--keep-subscriptions", action="store_true",
                   help="No borrar las suscripciones temporales (expiran solas a 1 día).")

    c = common.add_argument_group("cursor")
    c.add_argument("--cursor", choices=["gcs", "local", "none"],
                   default=os.environ.get("SIM_STATE_BACKEND", "gcs"),
                   help="Dónde leer sim-state/{source}.json (por defecto SIM_STATE_BACKEND o gcs).")
    c.add_argument("--state-bucket", default=os.environ.get("SIM_STATE_BUCKET"),
                   help="Bucket del cursor (por defecto SIM_STATE_BUCKET o {GCP_PROJECT}-ta-datasets).")
    c.add_argument("--state-dir", default=os.environ.get("SIM_STATE_DIR", ".sim-state"),
                   help="Carpeta del cursor con --cursor local.")
    c.add_argument("--seed", type=int, default=int(os.environ.get("SIM_SEED", "7")),
                   help="Semilla esperada en el cursor (por defecto SIM_SEED o 7).")
    c.add_argument("--checkpoint-every", type=int, default=50,
                   help="Cada cuántos mensajes guarda el cursor el simulador (sim.yaml; 50).")

    v = common.add_argument_group("validación")
    v.add_argument("--validate", choices=["auto", "yes", "no"], default="auto",
                   help="Validar cada mensaje contra contracts/envelope.v1.schema.json "
                        "(auto = si están jsonschema, referencing y contracts/).")
    v.add_argument("--contracts", default=None, help="Carpeta contracts/ (por defecto project/contracts).")
    v.add_argument("--expect-chaos", action="store_true", default=_env_bool("SIM_CHAOS_ENABLED"),
                   help="El simulador corre con caos: malformados e inválidos son avisos, no fallos.")

    o = common.add_argument_group("salida")
    o.add_argument("--json-out", default=None, help="Guardar el reporte completo en JSON (evidencia).")
    o.add_argument("--drain", type=float, default=5.0,
                   help="Segundos extra de recepción al final, para mensajes en vuelo (5).")

    th = common.add_argument_group("umbrales")
    th.add_argument("--rate-min", type=float, default=0.35, help="Tasa mínima por fuente, msg/s (0.35).")
    th.add_argument("--rate-max", type=float, default=0.65, help="Tasa máxima por fuente, msg/s (0.65).")
    th.add_argument("--min-messages", type=int, default=10, help="Mínimo de mensajes por fuente (10).")
    th.add_argument("--min-unique-ratio", type=float, default=0.99, help="Mínimo de event_id únicos (0.99).")

    p = argparse.ArgumentParser(
        prog="verify_ingest.py",
        description="Verificación de la ingesta del simulador (Lote 1.4, HU-017).",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    m = sub.add_parser("measure", parents=[common],
                       help="Medir tasa, event_id únicos y seq durante una ventana.")
    m.add_argument("--duration", type=float, default=120.0, help="Ventana de medición en segundos (120).")

    r = sub.add_parser("restart-check", parents=[common],
                       help="Comprobar que el seq continúa tras reiniciar el servicio.")
    r.add_argument("--before", type=float, default=90.0, help="Segundos de observación antes (90).")
    r.add_argument("--after", type=float, default=120.0,
                   help="Segundos de observación después de que el servicio vuelve (120).")
    r.add_argument("--restart", choices=["manual", "wait", "gcloud"], default="manual",
                   help="manual: usted reinicia y pulsa Enter · wait: espera --restart-wait segundos · "
                        "gcloud: crea una revisión nueva con gcloud (ver ARCHITECTURE §3.11).")
    r.add_argument("--restart-wait", type=float, default=180.0, help="Segundos de espera con --restart wait.")
    r.add_argument("--service", default=DEFAULT_SERVICE, help="Servicio de Cloud Run (ta-stream-simulator).")
    r.add_argument("--region", default=os.environ.get("GCP_REGION", "us-central1"), help="Región (GCP_REGION).")
    return p


@dataclass
class Config:
    command: str
    project: str
    sources: list[str]
    topic_template: str
    existing: dict[str, str]
    thresholds: Thresholds
    expected_seed: int | None
    expect_chaos: bool
    drain: float
    json_out: str | None


def parse_subscriptions(values: list[str], sources: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"--subscription espera FUENTE=NOMBRE, recibió {item!r}")
        src, name = (x.strip() for x in item.split("=", 1))
        if src not in sources:
            raise ValueError(f"--subscription: fuente desconocida {src!r}")
        if name.endswith("-df"):
            raise ValueError(f"{name} es la suscripción de Dataflow (§5.3); no la consuma desde aquí")
        out[src] = name
    return out


def make_config(args: argparse.Namespace) -> Config:
    if not args.project:
        raise ValueError("falta el proyecto: use --project o defina GCP_PROJECT")
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    if not sources:
        raise ValueError("--sources vacío")
    if "{source}" not in args.topic_template:
        raise ValueError("--topic-template debe contener {source}")
    if args.rate_min > args.rate_max:
        raise ValueError("--rate-min no puede ser mayor que --rate-max")
    return Config(
        command=args.command,
        project=args.project,
        sources=sources,
        topic_template=args.topic_template,
        existing=parse_subscriptions(args.subscription, sources),
        thresholds=Thresholds(
            rate_min=args.rate_min,
            rate_max=args.rate_max,
            min_messages=args.min_messages,
            min_unique_ratio=args.min_unique_ratio,
            checkpoint_every=args.checkpoint_every,
        ),
        expected_seed=args.seed,
        expect_chaos=args.expect_chaos,
        drain=max(0.0, args.drain),
        json_out=args.json_out,
    )


def make_cursor_reader(args: argparse.Namespace) -> CursorReader:
    if args.cursor == "none":
        return NullCursorReader()
    if args.cursor == "local":
        return LocalCursorReader(args.state_dir)
    bucket = args.state_bucket or f"{args.project}-ta-datasets"
    return GcsCursorReader(bucket, project=args.project)


def make_validator(args: argparse.Namespace, log: Callable[[str], None]):
    if args.validate == "no":
        return None
    from ingest_check.schema import SchemaUnavailable, load_envelope_validator

    try:
        return load_envelope_validator(args.contracts)
    except SchemaUnavailable as exc:
        if args.validate == "yes":
            raise
        log(f"validación contra schema desactivada: {exc}")
        return None


def make_backend(cfg: Config, args: argparse.Namespace, log: Callable[[str], None]) -> Backend:
    from ingest_check.pubsub_io import PubSubBackend, make_run_id

    labels = {
        "project": "trend-analyzer",
        "env": os.environ.get("ENV", "dev"),
        "owner": os.environ.get("OWNER", "alejandro"),
        "component": "ingest-verify",
    }
    return PubSubBackend(
        project=cfg.project,
        topic_template=cfg.topic_template,
        sources=cfg.sources,
        run_id=make_run_id(),
        existing=cfg.existing,
        keep=args.keep_subscriptions,
        labels=labels,
        log=log,
    )


# ---------------------------------------------------------------------------
# Ejecución


def _countdown(rt: Runtime, seconds: float, label: str, step: float = 30.0) -> None:
    remaining = seconds
    while remaining > 0:
        chunk = min(step, remaining)
        rt.sleep(chunk)
        remaining -= chunk
        if remaining > 0:
            rt.log(f"  {label}: faltan {remaining:.0f} s")


def _base_report(cfg: Config, rt: Runtime, cursor_desc: str, validated: bool) -> dict:
    return {
        "tool": f"verify_ingest {__version__}",
        "command": cfg.command,
        "generated_at": _iso(rt.clock()),
        "project": cfg.project,
        "sources": cfg.sources,
        "topic_template": cfg.topic_template,
        "cursor_backend": cursor_desc,
        "schema_validation": validated,
        "expect_chaos": cfg.expect_chaos,
        "thresholds": asdict(cfg.thresholds),
    }


def run_measure(cfg: Config, duration: float, backend: Backend, cursors: CursorReader,
                stats: IngestStats, rt: Runtime) -> dict:
    report = _base_report(cfg, rt, cursors.describe(), stats.validating)
    backend.setup()
    try:
        backend.start(stats.add)
        t0 = rt.clock()
        rt.log(f"midiendo {duration:.0f} s en {', '.join(cfg.sources)}…")
        _countdown(rt, duration, "medición")
        t1 = rt.clock()
        if cfg.drain:
            rt.sleep(cfg.drain)
        backend.stop()
    finally:
        backend.teardown()

    cursor_map = read_all(cursors, cfg.sources)
    obs = stats.observations()
    checks: list[Check] = []
    per_source = {}
    for src in cfg.sources:
        s = summarize(obs, src, t0, t1)
        checks += measure_checks(s, cfg.thresholds, cfg.expect_chaos)
        if not isinstance(cursors, NullCursorReader):
            checks += cursor_checks(src, cursor_map[src], s.seq_max, cfg.thresholds, cfg.expected_seed)
        per_source[src] = {**s.to_dict(), "cursor": _cursor_dict(cursor_map[src])}

    report.update({
        "window": {"start": _iso(t0), "end": _iso(t1), "seconds": round(t1 - t0, 3)},
        "total_rate_msg_s": round(sum(v["rate_msg_s"] for v in per_source.values()), 4),
        "per_source": per_source,
        "checks": [c.to_dict() for c in checks],
        "verdict": verdict(checks),
    })
    return report


def _cursor_dict(c: CursorState | None) -> dict | None:
    return c.to_dict() if c else None


def trigger_restart(args: argparse.Namespace, cfg: Config, rt: Runtime) -> None:
    if args.restart == "gcloud":
        cmd = [
            "gcloud", "run", "services", "update", args.service,
            "--project", cfg.project, "--region", args.region,
            "--update-env-vars", f"{RESTART_NONCE_VAR}={int(rt.clock())}", "--quiet",
        ]
        rt.log("reiniciando con: " + " ".join(shlex.quote(x) for x in cmd))
        code = rt.run(cmd)
        if code != 0:
            raise RuntimeError(f"gcloud terminó con código {code}")
        rt.log(f"revisión nueva lista. Nota: {RESTART_NONCE_VAR} no está en Terraform; "
               "el próximo apply la quita (y vuelve a reiniciar).")
        return

    instructions = (
        "\nReinicie ahora el servicio. Opciones (ver infra/DEPLOY_SIMULATOR.md):\n"
        "  · Terraform: simulator_running = false → apply; luego true → apply.\n"
        f"  · gcloud:    gcloud run services update {args.service} --region {args.region} "
        f"--update-env-vars {RESTART_NONCE_VAR}=$(date +%s)\n"
    )
    rt.log(instructions)
    if args.restart == "wait":
        rt.log(f"esperando {args.restart_wait:.0f} s al reinicio externo…")
        _countdown(rt, args.restart_wait, "reinicio")
        return
    if not rt.stdin_is_tty():
        raise RuntimeError("--restart manual necesita una terminal; use --restart wait o --restart gcloud")
    rt.prompt("Pulse Enter cuando el servicio vuelva a estar Ready… ")


def run_restart_check(cfg: Config, args: argparse.Namespace, backend: Backend, cursors: CursorReader,
                      stats: IngestStats, rt: Runtime) -> dict:
    report = _base_report(cfg, rt, cursors.describe(), stats.validating)
    backend.setup()
    try:
        backend.start(stats.add)
        t0 = rt.clock()
        cursors_before = read_all(cursors, cfg.sources)
        rt.log(f"observando {args.before:.0f} s antes del reinicio…")
        _countdown(rt, args.before, "antes")
        t1 = rt.clock()
        trigger_restart(args, cfg, rt)
        t2 = rt.clock()
        rt.log(f"observando {args.after:.0f} s después del reinicio…")
        _countdown(rt, args.after, "después")
        t3 = rt.clock()
        if cfg.drain:
            rt.sleep(cfg.drain)
        backend.stop()
    finally:
        backend.teardown()

    cursors_after = read_all(cursors, cfg.sources)
    obs = stats.observations()
    checks: list[Check] = []
    per_source = {}
    for src in cfg.sources:
        before = summarize(obs, src, t0, t1)
        transition = summarize(obs, src, t1, t2)
        after = summarize(obs, src, t2, t3)
        resumed = summarize(obs, src, t1, t3)
        checks += restart_checks(
            src, before, resumed, after, cfg.thresholds, obs,
            before_window=(t0, t1), resumed_window=(t1, t3),
            cursor_before=cursors_before[src], cursor_after=cursors_after[src],
        )
        per_source[src] = {
            "before": before.to_dict(),
            "transition": transition.to_dict(),
            "after": after.to_dict(),
            "cursor_before": _cursor_dict(cursors_before[src]),
            "cursor_after": _cursor_dict(cursors_after[src]),
        }

    report.update({
        "restart_mode": args.restart,
        "windows": {
            "before": {"start": _iso(t0), "end": _iso(t1)},
            "transition": {"start": _iso(t1), "end": _iso(t2)},
            "after": {"start": _iso(t2), "end": _iso(t3)},
        },
        "per_source": per_source,
        "checks": [c.to_dict() for c in checks],
        "verdict": verdict(checks),
    })
    return report


# ---------------------------------------------------------------------------
# Presentación

_MARK = {OK: "OK   ", WARN: "AVISO", FAIL: "FALLA"}


def format_text(report: dict) -> str:
    lines = [f"Verificación de ingesta · {report['command']} · proyecto {report['project']}"]
    if report["command"] == "measure":
        w = report["window"]
        lines.append(f"Ventana {w['start']} → {w['end']} ({w['seconds']:.0f} s); "
                     f"tasa total {report['total_rate_msg_s']:.3f} msg/s")
        lines.append("")
        header = f"{'fuente':<10} {'msgs':>5} {'msg/s':>6} {'únicos':>7} {'dup':>4} {'seq min':>8} {'seq max':>8} {'cursor':>8}"
        lines.append(header)
        lines.append("-" * len(header))
        for src, s in report["per_source"].items():
            cur = s["cursor"]["seq"] if s.get("cursor") else "—"
            lines.append(
                f"{src:<10} {s['messages']:>5} {s['rate_msg_s']:>6.3f} {s['unique_event_ids']:>7} "
                f"{s['duplicate_copies']:>4} {str(s['seq_min'] if s['seq_min'] is not None else '—'):>8} "
                f"{str(s['seq_max'] if s['seq_max'] is not None else '—'):>8} {str(cur):>8}"
            )
    else:
        lines.append(f"Modo de reinicio: {report['restart_mode']}")
        lines.append("")
        header = f"{'fuente':<10} {'antes: seq max':>15} {'después: seq min–max':>22} {'cursor antes → después':>24}"
        lines.append(header)
        lines.append("-" * len(header))
        for src, s in report["per_source"].items():
            b = s["before"]["seq_max"]
            a_min = s["transition"]["seq_min"] if s["transition"]["seq_min"] is not None else s["after"]["seq_min"]
            a_max = s["after"]["seq_max"]
            cb = s["cursor_before"]["seq"] if s["cursor_before"] else "—"
            ca = s["cursor_after"]["seq"] if s["cursor_after"] else "—"
            lines.append(f"{src:<10} {str(b):>15} {f'{a_min}–{a_max}':>22} {f'{cb} → {ca}':>24}")
    lines.append("")
    for c in report["checks"]:
        lines.append(f"[{_MARK[c['status']]}] {c['source']:<10} {c['check']}: {c['detail']}")
    lines.append("")
    lines.append(f"Resultado: {_MARK[report['verdict']].strip()}")
    return "\n".join(lines)


def write_json(report: dict, path: str) -> None:
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None, rt: Runtime | None = None,
         backend_factory: Callable | None = None, cursor_factory: Callable | None = None) -> int:
    rt = rt or Runtime()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = make_config(args)
        validator = make_validator(args, rt.log)
        cursors = (cursor_factory or make_cursor_reader)(args)
        backend = (backend_factory or make_backend)(cfg, args, rt.log)
        stats = IngestStats(validator=validator)
        if cfg.command == "measure":
            if args.duration <= 0:
                raise ValueError("--duration debe ser positivo")
            report = run_measure(cfg, args.duration, backend, cursors, stats, rt)
        else:
            if args.before <= 0 or args.after <= 0:
                raise ValueError("--before y --after deben ser positivos")
            report = run_restart_check(cfg, args, backend, cursors, stats, rt)
    except KeyboardInterrupt:
        rt.log("interrumpido")
        return 2
    except Exception as exc:
        rt.log(f"error: {exc}")
        return 2

    print(format_text(report))
    if cfg.json_out:
        write_json(report, cfg.json_out)
        rt.log(f"reporte JSON en {cfg.json_out}")
    return 1 if report["verdict"] == FAIL else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
