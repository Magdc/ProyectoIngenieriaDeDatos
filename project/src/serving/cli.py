"""``scripts/quicksight.py``: plan, despliegue, refresco y estado del tablero de QuickSight (ARCHITECTURE.md §3.27.1).

    python project/scripts/quicksight.py plan               # lo que se desplegaría y sus comprobaciones, sin nube
    python project/scripts/quicksight.py deploy --yes       # crea o actualiza todo e importa a SPICE
    python project/scripts/quicksight.py refresh            # solo importa otra vez los datasets a SPICE
    python project/scripts/quicksight.py status             # solo lectura
    python project/scripts/quicksight.py embed-url --user ta-quicksight-lector/demo   # URL temporal (no se guarda)

Cada paso imprime una línea JSON (``event`` = paso). Credenciales: el perfil de AWS del desarrollador.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from serving import quicksight_spec as qs
from serving.deploy import Deployer, DeployError


def _print_json(printer: Callable[[str], None]) -> Callable[[dict[str, Any]], None]:
    return lambda ev: printer(json.dumps(ev, ensure_ascii=False, default=str))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="quicksight.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=("plan", "deploy", "refresh", "status", "embed-url"))
    p.add_argument("--yes", action="store_true", help="deploy: no pedir confirmación")
    p.add_argument("--aws-profile", default=None, help="perfil de AWS (si no, AWS_PROFILE o el por defecto)")
    p.add_argument("--no-refresh", action="store_true", help="deploy: no importar a SPICE")
    p.add_argument("--user", default=None, help="embed-url: usuario de QuickSight (p. ej. ta-quicksight-lector/demo)")
    p.add_argument("--minutes", type=int, default=15, help="embed-url: duración de la sesión")
    return p


def make_deployer(args: argparse.Namespace, emit: Callable[[dict[str, Any]], None]) -> tuple[Deployer, Any]:
    import boto3

    session = boto3.Session(profile_name=args.aws_profile, region_name=qs.REGION)
    account = session.client("sts").get_caller_identity()["Account"]
    return Deployer(session.client("quicksight"), account, emit=emit), session


def role_arns(account: str) -> tuple[str, dict[str, str]]:
    service = f"arn:aws:iam::{account}:role/{qs.SERVICE_ROLE}"
    return service, {k: f"arn:aws:iam::{account}:role/{name}" for k, name in qs.ACCESS_ROLES.items()}


def main(argv: list[str] | None = None, *, printer: Callable[[str], None] = print,
         deployer_factory: Callable[..., tuple[Deployer, Any]] | None = None,
         confirm: Callable[[str], bool] | None = None) -> int:
    args = build_parser().parse_args(argv)
    emit = _print_json(printer)
    if args.command == "plan":
        summary = qs.summary()
        emit({"event": "plan", **summary})
        return 1 if summary["problems"] else 0
    problems = [p for d in qs.DATASETS for p in qs.dataset_problems(d)]
    if problems:
        emit({"event": "error", "error": "definiciones con problemas (quicksight.py plan)", "problems": problems})
        return 1
    deployer, _ = (deployer_factory or make_deployer)(args, emit)
    try:
        if args.command == "deploy":
            ask = confirm or (lambda q: input(f"{q} [s/N] ").strip().lower() in {"s", "si", "sí", "y", "yes"})
            if not args.yes and not ask("¿Crear o actualizar la fuente, los 4 datasets, el análisis y el tablero en "
                                        "QuickSight e importar a SPICE?"):
                emit({"event": "error", "error": "no confirmado"})
                return 1
            service, access = role_arns(deployer.account)
            deployer.deploy(service, access, refresh=not args.no_refresh)
        elif args.command == "refresh":
            deployer.refresh()
        elif args.command == "status":
            deployer.status()
        elif args.command == "embed-url":
            if not args.user:
                emit({"event": "error", "error": "falta --user"})
                return 2
            # Va a la terminal, nunca a la evidencia: abre una sesión de QuickSight por unos minutos.
            printer(deployer.embed_url(args.user, args.minutes))
    except DeployError as exc:
        emit({"event": "error", "error": str(exc)})
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
