"""CLI de la prueba de humo: ``python project/scripts/smoke_e2e.py run|plan`` (§3.26.1)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from smoke import e2e

PROJECT_DIR = Path(__file__).resolve().parents[2]
EVIDENCE_DIR = PROJECT_DIR / "evidencia"
REPORT_NAME = "smoke_e2e_5_4"


def emit(event: dict[str, Any]) -> None:
    print(json.dumps(event, ensure_ascii=False, default=str), flush=True)


def plan(source: str, seq: int | None = None, now: datetime | None = None) -> dict[str, Any]:
    """Los dos eventos que publicaría ``run`` (sin publicar nada) y si cumplen el contrato."""
    now = now or datetime.now(timezone.utc)
    seq = seq if seq is not None else int(now.timestamp())
    emission = e2e.sentinel_emission(source, seq, now)
    warm = e2e.warmup_envelope(f"smoke-{now:%Y%m%dT%H%M%SZ}", seq, now.timestamp())
    env = emission.envelope
    return {"event": "smoke_plan", "source": source, "seq": seq, "event_id": env["event_id"], "generator": env["generator"],
            "event_time": env["event_time"], "messages": len(emission.messages),
            "attributes": sorted(emission.messages[0].attributes), "contract": e2e.envelope_problems(env) or "ok",
            "warmup_event_id": warm["event_id"], "warmup_generator": warm["generator"],
            "warmup_contract": e2e.envelope_problems(warm) or "ok", "objectives_s": e2e.OBJECTIVES}


def write_report(report: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    js, md = out_dir / f"{REPORT_NAME}.json", out_dir / f"{REPORT_NAME}.md"
    js.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    md.write_text(e2e.render_md(report), encoding="utf-8")
    return js, md


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="smoke_e2e.py", description=e2e.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "plan"):
        s = sub.add_parser(name)
        s.add_argument("--source", choices=e2e.SOURCES, default="reddit")
        s.add_argument("--seq", type=int, default=None, help="seq del centinela (por defecto, los segundos de la época)")
        if name == "run":
            s.add_argument("--no-warmup", action="store_true", help="no publicar el calentamiento ta-verify@2.1")
            s.add_argument("--no-trigger", action="store_true", help="no disparar el exportador ni el workflow (esperar los horarios)")
            s.add_argument("--gcp-project", default=None)
            s.add_argument("--aws-profile", default=None)
            s.add_argument("--bucket", default=None, help="bucket del lake (si no, ta-dev-{cuenta}-lake)")
            s.add_argument("--out", type=Path, default=EVIDENCE_DIR, help="carpeta del informe (JSON y Markdown)")
    return p


def main(argv: list[str] | None = None, *, clients_factory: Callable[..., Any] | None = None,
         now: Callable[[], datetime] | None = None, sleep: Callable[[float], None] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "plan":
        out = plan(args.source, args.seq)
        emit(out)
        return 0 if out["contract"] == "ok" and out["warmup_contract"] == "ok" else 1
    from smoke import clouds

    project = args.gcp_project or clouds.default_gcp_project(dict(os.environ))
    if not project:
        emit({"event": "smoke_error", "error": "falta el proyecto de GCP (--gcp-project o TA_GCP_PROJECT)"})
        return 2
    try:
        clients = (clients_factory or clouds.Clouds)(project, aws_profile=args.aws_profile, bucket=args.bucket)
    except Exception as exc:  # noqa: BLE001 - credenciales o librerías
        emit({"event": "smoke_error", "error": f"{type(exc).__name__}: {exc}"})
        return 2
    cfg = e2e.Config(source=args.source, warmup=not args.no_warmup, trigger=not args.no_trigger, seq=args.seq)
    kw = {k: v for k, v in (("now", now), ("sleep", sleep)) if v is not None}
    report = e2e.Smoke(clients, cfg, log=emit, **kw).run()
    js, md = write_report(report, args.out)
    emit({"event": "smoke_report", "json": str(js), "md": str(md), "ok": report["ok"]})
    return report["exit_code"]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
