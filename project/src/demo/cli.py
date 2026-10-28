"""CLI del escenario de demo (Lote 6.3, HU-040). Lo usan el guion (``docs/DEMO.md``) y ``ops.validate63``.

    python project/scripts/demo_scenario.py start [--brand postobon --sentiment negative --multiplier 3
                                                   --duration 300 --source instagram|all --name N]
    python project/scripts/demo_scenario.py status | stop
    python project/scripts/demo_scenario.py check            (sin nube)

* ``start`` / ``status`` / ``stop``: ``POST`` / ``GET`` / ``DELETE /control/scenario`` del simulador en Cloud Run,
  con el ID token de ``gcloud auth print-identity-token``. ``--url http://localhost:8080`` habla con el simulador
  local (sin token).
* ``check``: ola en tiempo virtual con el bucle real del simulador (cadencia, marca, sentimiento, ``sim.scenario``,
  envelope v1.0) y recorrido de la API con su servidor HTTP en ``127.0.0.1``.

Cada orden imprime una línea JSON (``event`` = ``scenario_started``, ``scenario_status``, ``scenario_stopped``,
``scenario_rejected``, ``scenario_error`` o ``scenario_check``). Proyecto: ``--project`` o ``GCP_PROJECT``.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from typing import Any, Callable

from demo import client
from simulator.scenario import SENTIMENTS
from simulator.sim_config import SOURCES

DEFAULT_PROJECT = "project-0568d9ba-848f-413c-88b"


def _print(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def endpoint(args: argparse.Namespace, run: client.Gcloud = client.gcloud) -> tuple[str, str | None]:
    """URL del simulador y token (solo para ``https://``, es decir, Cloud Run; ``--no-token`` lo omite)."""
    url = args.url or client.service_url(args.project, args.region, run)
    return url, (client.identity_token(run) if url.startswith("https://") and not args.no_token else None)


def _reply(event: str, reply: client.Reply, url: str) -> int:
    if reply.ok:
        _print(event, status=reply.status, url=url, **reply.body)
        return 0
    _print("scenario_rejected", status=reply.status, url=url, **reply.body)
    return 1


def cmd_start(args: argparse.Namespace, call: Callable[..., client.Reply], url: str, token: str | None) -> int:
    body = client.scenario_body(args.brand, args.sentiment, args.multiplier, args.duration,
                                None if args.source == "all" else args.source, args.name)
    return _reply("scenario_started", call(url, "POST", body=body, token=token), url)


def cmd_status(args: argparse.Namespace, call: Callable[..., client.Reply], url: str, token: str | None) -> int:
    return _reply("scenario_status", call(url, "GET", token=token), url)


def cmd_stop(args: argparse.Namespace, call: Callable[..., client.Reply], url: str, token: str | None) -> int:
    return _reply("scenario_stopped", call(url, "DELETE", token=token), url)


def cmd_check(args: argparse.Namespace) -> int:
    from demo import local

    logs = io.StringIO()  # el simulador escribe sus logs JSON en stdout: aquí solo va el resultado
    with contextlib.redirect_stdout(logs):
        t = local.timeline(args.brand, args.sentiment, args.multiplier, args.duration,
                           args.source if args.source != "all" else "instagram")
        api = local.api_check(client.scenario_body(args.brand, args.sentiment, args.multiplier, args.duration,
                                                   None if args.source == "all" else args.source))
    problems = local.timeline_problems(t, args.multiplier)
    bad_api = [s["step"] for s in api if not s["ok"]]
    names = [json.loads(ln).get("event") for ln in logs.getvalue().splitlines() if ln.startswith("{")]
    _print("scenario_check", timeline=t, problems=problems, api=[{k: s[k] for k in ("step", "status", "want", "ok")} for s in api],
           api_problems=bad_api, simulator_logs={n: names.count(n) for n in sorted(set(names)) if n.startswith("scenario")})
    return 0 if not problems and not bad_api else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="demo_scenario.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["start", "status", "stop", "check"])
    p.add_argument("--project", default=os.environ.get("GCP_PROJECT") or DEFAULT_PROJECT)
    p.add_argument("--region", default=client.REGION)
    p.add_argument("--url", default=None, help="URL del simulador (por defecto, la de Cloud Run)")
    p.add_argument("--no-token", action="store_true", help="sin ID token (para comprobar que Cloud Run lo exige)")
    d = client.DEFAULTS
    p.add_argument("--brand", default=d["brand"])
    p.add_argument("--sentiment", default=d["sentiment"], choices=SENTIMENTS)
    p.add_argument("--multiplier", type=float, default=d["multiplier"], help="cadencia ×m; 2–10 (por defecto 3)")
    p.add_argument("--duration", type=int, default=d["duration_s"], help="segundos; 60–3600 (por defecto 300)")
    p.add_argument("--source", default=d["source"], choices=[*SOURCES, "all"], help="red (all = las tres)")
    p.add_argument("--name", default=None, help="nombre del escenario (sim.scenario); por defecto demo/{marca}-{sentimiento}/{hora}")
    return p


def main(argv: list[str] | None = None, *, call: Callable[..., client.Reply] = client.call,
         run: client.Gcloud = client.gcloud) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "check":
        return cmd_check(args)
    try:
        url, token = endpoint(args, run)
        return {"start": cmd_start, "status": cmd_status, "stop": cmd_stop}[args.command](args, call, url, token)
    except client.ClientError as exc:
        _print("scenario_error", error=str(exc))
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
