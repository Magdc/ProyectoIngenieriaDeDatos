"""CLI del exportador (``python -m exporter.cli`` o ``scripts/export_gcs_to_s3.py``).

Subcomandos:
  run          exporta lo nuevo de enriched/ (lo que corre el job de Cloud Run cada 15 min)
  plan         lo mismo sin escribir nada (cuenta lo que subiría)
  probe        sondas de permisos de la identidad del exportador (GCS y AWS)
  token        claims del ID token de sa-exporter y si la confianza del rol lo acepta
  doctor       entorno, dependencias, configuración y adaptadores (antes de una corrida larga)
  verify       GCS enriched/ frente a S3 raw/origin=stream/ (credenciales del desarrollador)
  status       el watermark actual
  lake-report  ambos orígenes en S3 y el esquema de un Parquet de cada uno

Salida: 0 bien, 1 una comprobación falló, 2 error de uso, configuración o identidad.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import warnings

# google.api_core avisa en cada importación que Python 3.11 dejará de tener soporte en 2027;
# no aporta nada en esta salida (el entorno del proyecto es 3.11 a propósito).
warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\.api_core")
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from exporter import EXPORTER_ID
from exporter.config import ConfigError, ExporterConfig, load_config
from exporter.core import new_run_id, run_export
from exporter.identity import (
    IdentityError,
    assume_role,
    check_claims,
    compare_with_trust,
    decode_claims,
    get_token,
)
from exporter.jsonlog import LogFn, make_logger, parse_ts
from exporter.keys import SOURCES, source_prefix
from exporter.watermark import Watermark, WatermarkError

EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2
PUBLIC_CLAIMS = ("iss", "aud", "azp", "sub", "email", "email_verified", "iat", "exp")


@dataclass
class Deps:
    """Fábricas de clientes (las pruebas las reemplazan por dobles)."""

    gcs: Callable[[ExporterConfig], Any] | None = None
    s3_client: Callable[[Any, ExporterConfig], Any] | None = None
    token: Callable[[str, str, str | None], tuple[str, str]] = get_token
    sts_client: Any = None
    profile_session: Callable[[str | None, str], Any] | None = None
    iam_client: Callable[[Any], Any] | None = None
    role_session: Callable[[Any, str], Any] | None = None

    def make_gcs(self, cfg: ExporterConfig) -> Any:
        if self.gcs:
            return self.gcs(cfg)
        from exporter.gcs_io import GcsLake

        return GcsLake(cfg.gcs_bucket, project=cfg.gcp_project)

    def make_s3_client(self, session: Any, cfg: ExporterConfig) -> Any:
        if self.s3_client:
            return self.s3_client(session, cfg)
        from exporter.s3_io import s3_client

        return s3_client(session, cfg.aws_region)

    def make_profile_session(self, profile: str | None, region: str) -> Any:
        if self.profile_session:
            return self.profile_session(profile, region)
        import boto3

        return boto3.Session(profile_name=profile or None, region_name=region)

    def make_role_session(self, assumed: Any, region: str) -> Any:
        if self.role_session:
            return self.role_session(assumed, region)
        return assumed.boto3_session(region)

    def make_iam_client(self, session: Any) -> Any:
        if self.iam_client:
            return self.iam_client(session)
        return session.client("iam")


def _write_json(path: str | None, data: dict[str, Any]) -> None:
    if not path:
        return
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _public_claims(claims: dict[str, Any]) -> dict[str, Any]:
    return {k: claims.get(k) for k in PUBLIC_CLAIMS if k in claims}


def _load_cfg(args: argparse.Namespace, env: dict[str, str] | None, *, require_upload: bool) -> ExporterConfig:
    cfg = load_config(env, require_upload=require_upload)
    overrides: dict[str, Any] = {}
    if getattr(args, "state_key", None):
        if not args.state_key.startswith("export-state/"):
            raise ConfigError("--state-key debe quedar bajo export-state/")
        overrides["state_key"] = args.state_key
    if getattr(args, "max_objects", None):
        overrides["max_objects"] = args.max_objects
    if getattr(args, "workers", None):
        overrides["workers"] = args.workers
    return cfg.with_overrides(**overrides)


def _aws_session(args: argparse.Namespace, cfg: ExporterConfig, deps: Deps, log: LogFn, run_id: str) -> tuple[Any, dict[str, Any]]:
    if getattr(args, "aws_auth", "role") == "profile":
        session = deps.make_profile_session(args.aws_profile, cfg.aws_region)
        return session, {"mode": "profile", "profile": args.aws_profile or "(por defecto)"}
    if not cfg.role_arn:
        raise ConfigError("falta TA_EXPORTER_ROLE_ARN (o use --aws-auth profile)")
    token, path = deps.token(args.token_source, cfg.audience, cfg.sa_email)
    claims = decode_claims(token)
    assumed = assume_role(token, cfg.role_arn, run_id=run_id, region=cfg.aws_region,
                          duration=cfg.session_seconds, sts_client=deps.sts_client)
    info = {"mode": "role", "token_source": path, "claims": _public_claims(claims), **assumed.public()}
    log("export_assumed_role", run_id=run_id, **info)
    return deps.make_role_session(assumed, cfg.aws_region), info


# --- subcomandos -------------------------------------------------------------------------


def cmd_run(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    dry = bool(args.dry_run or args.command == "plan")
    cfg = _load_cfg(args, env, require_upload=not dry)
    run_id = new_run_id()
    session, aws_info = _aws_session(args, cfg, deps, log, run_id)
    from exporter.s3_io import S3Lake

    s3 = S3Lake(cfg.s3_bucket, cfg.kms_key_arn, deps.make_s3_client(session, cfg))
    gcs = deps.make_gcs(cfg)
    report = run_export(cfg, gcs, s3, dry_run=dry, log=log, run_id=run_id)
    data = report.to_json()
    data["aws"] = aws_info
    data["config"] = cfg.public()
    _write_json(args.json_out, data)
    return EXIT_OK if report.ok else EXIT_FAIL


def cmd_probe(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    from exporter.probe import probe_aws, probe_gcs, summarize

    cfg = _load_cfg(args, env, require_upload=False)
    run_id = new_run_id()
    results = []
    aws_info: dict[str, Any] = {}
    if not args.no_aws:
        session, aws_info = _aws_session(args, cfg, deps, log, run_id)
        client = deps.make_s3_client(session, cfg)
        results.extend(probe_aws(client, cfg.s3_bucket, run_id, aws_info.get("assumed_arn")))
    if not args.no_gcs:
        results.extend(probe_gcs(deps.make_gcs(cfg), run_id))
    summary = summarize(results)
    summary.update({"run_id": run_id, "aws": aws_info, "exporter": EXPORTER_ID})
    log("probe_result", "INFO" if summary["ok"] else "ERROR", **summary)
    _write_json(args.json_out, summary)
    return EXIT_OK if summary["ok"] else EXIT_FAIL


def cmd_token(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    cfg = _load_cfg(args, env, require_upload=False)
    token, path = deps.token(args.token_source, cfg.audience, cfg.sa_email)
    claims = decode_claims(token)
    problems = check_claims(claims, audience=cfg.audience, unique_id=args.unique_id, email=cfg.sa_email)
    trust_problems: list[str] | None = None
    trust_source = None
    trust_doc = None
    if args.trust_json:
        trust_doc = json.loads(Path(args.trust_json).read_text(encoding="utf-8"))
        trust_source = args.trust_json
    elif args.trust_from_aws:
        session = deps.make_profile_session(args.aws_profile, cfg.aws_region)
        role_name = (cfg.role_arn or "").split("/")[-1] or "ta-gcp-exporter-role"
        role = deps.make_iam_client(session).get_role(RoleName=role_name)["Role"]
        trust_doc = role["AssumeRolePolicyDocument"]
        trust_source = f"iam get-role {role_name}"
    if trust_doc is not None:
        if isinstance(trust_doc, dict) and "AssumeRolePolicyDocument" in trust_doc:
            trust_doc = trust_doc["AssumeRolePolicyDocument"]
        if isinstance(trust_doc, dict) and "Role" in trust_doc:
            trust_doc = trust_doc["Role"]["AssumeRolePolicyDocument"]
        trust_problems = compare_with_trust(claims, trust_doc)
    out = {
        "token_source": path,
        "claims": _public_claims(claims),
        "problems": problems,
        "trust_source": trust_source,
        "trust_problems": trust_problems,
        "ok": not problems and not trust_problems,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    _write_json(args.json_out, out)
    return EXIT_OK if out["ok"] else EXIT_FAIL


def _doctor_checks(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(cid: str, what: str, status: str, detail: str) -> None:
        checks.append({"id": cid, "what": what, "status": status, "detail": detail})

    py = sys.version_info
    add("X1", "Python", "OK" if (py.major, py.minor) == (3, 11) else "AVISO", platform.python_version())
    versions = []
    missing = []
    for mod, label in (("boto3", "boto3"), ("botocore", "botocore"), ("google.cloud.storage", "google-cloud-storage"), ("google.auth", "google-auth")):
        try:
            module = __import__(mod, fromlist=["__version__"])
            versions.append(f"{label} {getattr(module, '__version__', '?')}")
        except Exception as exc:  # noqa: BLE001
            missing.append(f"{label} ({type(exc).__name__})")
    add("X2", "dependencias del exportador", "FALLA" if missing else "OK",
        ", ".join(versions) + (f"; faltan: {', '.join(missing)}" if missing else ""))

    try:
        cfg = load_config(env, require_upload=False)
        add("X3", "configuración", "OK", f"gcs={cfg.gcs_bucket} s3={cfg.s3_bucket} rol={cfg.role_arn or '—'} aud={cfg.audience}")
    except ConfigError as exc:
        cfg = None
        add("X3", "configuración", "AVISO" if args.offline else "FALLA", str(exc))

    try:
        import pyarrow  # noqa: F401

        add("X4", "pyarrow (parte F)", "OK", f"pyarrow {pyarrow.__version__}")
    except Exception as exc:  # noqa: BLE001
        add("X4", "pyarrow (parte F)", "AVISO", f"no importa ({type(exc).__name__}); solo lo usa lake-report")

    from exporter.lake_check import posts_enriched_column_names

    names, source = posts_enriched_column_names()
    if names is None:
        add("X5", "columnas de posts_enriched (common.enriched_row)", "AVISO", f"{source}; F compara con §5.5")
    else:
        status = "OK" if len(names) == 24 else "AVISO"
        add("X5", "columnas de posts_enriched (common.enriched_row)", status, f"{len(names)} columnas desde {source}")

    if not args.offline and cfg is not None:
        try:
            gcs = deps.make_gcs(cfg)
            first = [o.name for o in gcs.list_objects(source_prefix(SOURCES[0]), max_results=1)]
            add("X6", "GCS: listar enriched/", "OK", first[0] if first else "enriched/ vacío para reddit")
            data, gen = gcs.read_state(cfg.state_key)
            wm = Watermark.from_json(data)
            add("X7", "watermark", "OK", f"{cfg.state_key}: " + (f"cursor {wm.cursor.isoformat()} (generación {gen})" if wm.cursor else "no existe todavía"))
        except WatermarkError as exc:
            add("X7", "watermark", "FALLA", str(exc))
        except Exception as exc:  # noqa: BLE001
            add("X6", "GCS: listar enriched/", "FALLA", f"{type(exc).__name__}: {exc}"[:400])
    return checks


def cmd_doctor(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    checks = _doctor_checks(args, env, deps)
    for c in checks:
        print(f"{c['id']:<3} {c['status']:<5} {c['what']}: {c['detail']}")
    worst = "FALLA" if any(c["status"] == "FALLA" for c in checks) else "OK"
    _write_json(args.json_out, {"status": worst, "checks": checks, "exporter": EXPORTER_ID})
    return EXIT_FAIL if worst == "FALLA" else EXIT_OK


def cmd_verify(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    from exporter.s3_io import S3Lake
    from exporter.verify import mapped, sample_keys, verify_export

    cfg = _load_cfg(args, env, require_upload=False)
    gcs = deps.make_gcs(cfg)
    objects = []
    for source in cfg.sources:
        objects.extend(gcs.list_objects(source_prefix(source)))
    data, _ = gcs.read_state(cfg.state_key)
    wm = Watermark.from_json(data)
    session = deps.make_profile_session(args.aws_profile, cfg.aws_region)
    s3 = S3Lake(cfg.s3_bucket, cfg.kms_key_arn, deps.make_s3_client(session, cfg))
    s3_index = s3.list_prefix("raw/origin=stream/")
    if args.keys_from:
        report = json.loads(Path(args.keys_from).read_text(encoding="utf-8"))
        keys = [k for k in report.get("uploaded_keys", []) if k in s3_index][: max(args.sample, 0) or None]
    else:
        keys = sample_keys([k for k in mapped(objects) if k in s3_index], args.sample)
    heads: dict[str, dict[str, Any]] = {}
    for key in keys:
        try:
            heads[key] = s3.head(key)
        except Exception as exc:  # noqa: BLE001
            heads[key] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    cursor = wm.cursor
    if args.cursor:
        cursor = parse_ts(args.cursor)
    result = verify_export(objects, s3_index, cursor=cursor, heads=heads, kms_key_arn=cfg.kms_key_arn)
    result["state_key"] = cfg.state_key
    for c in result["checks"]:
        print(f"{c['id']:<3} {c['status']:<5} {c['what']}: {c['detail']}")
    _write_json(args.json_out, result)
    return EXIT_FAIL if result["status"] == "FALLA" else EXIT_OK


def cmd_status(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    cfg = _load_cfg(args, env, require_upload=False)
    data, gen = deps.make_gcs(cfg).read_state(cfg.state_key)
    wm = Watermark.from_json(data)
    out = {"state_key": cfg.state_key, "generation": gen, "exists": data is not None, **wm.to_json()}
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    _write_json(args.json_out, out)
    return EXIT_OK


def cmd_lake_report(args: argparse.Namespace, env: dict[str, str] | None, deps: Deps, log: LogFn) -> int:
    from exporter.lake_check import lake_report, posts_enriched_column_names
    from exporter.s3_io import S3Lake

    cfg = _load_cfg(args, env, require_upload=False)
    session = deps.make_profile_session(args.aws_profile, cfg.aws_region)
    s3 = S3Lake(cfg.s3_bucket, cfg.kms_key_arn, deps.make_s3_client(session, cfg))
    names, source = posts_enriched_column_names()

    def first_key(prefix: str) -> str | None:
        for key, _size in s3.iter_keys(prefix, max_keys=1):
            return key
        return None

    report = lake_report(s3.count_prefix, first_key, s3.get_bytes, expected_columns=names, columns_source=source)
    problems = []
    for origin, info in report["origins"].items():
        if info["objects"] == 0:
            problems.append(f"no hay objetos en raw/origin={origin}/")
        problems.extend(f"{origin}: {p}" for p in info.get("sample", {}).get("problems", []))
    report["problems"] = problems
    report["status"] = "FALLA" if problems else ("AVISO" if names is None else "OK")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    _write_json(args.json_out, report)
    return EXIT_FAIL if problems else EXIT_OK


COMMANDS = {
    "run": cmd_run,
    "plan": cmd_run,
    "probe": cmd_probe,
    "token": cmd_token,
    "doctor": cmd_doctor,
    "verify": cmd_verify,
    "status": cmd_status,
    "lake-report": cmd_lake_report,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="export_gcs_to_s3", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser, *, aws_default: str = "role") -> None:
        p.add_argument("--json-out", help="guarda el resultado en este JSON")
        p.add_argument("--token-source", default="auto", choices=["auto", "metadata", "impersonate", "gcloud"],
                       help="de dónde sale el ID token de sa-exporter (auto: metadatos en Cloud Run, impersonación fuera)")
        p.add_argument("--aws-auth", default=aws_default, choices=["role", "profile"],
                       help="role: asume ta-gcp-exporter-role con el token; profile: usa el perfil de AWS CLI")
        p.add_argument("--aws-profile", default=None, help="perfil de AWS CLI con --aws-auth profile")
        p.add_argument("--state-key", default=None, help="otro watermark bajo export-state/ (pruebas)")

    for name in ("run", "plan"):
        p = sub.add_parser(name, help="exportar" if name == "run" else "exportar sin escribir")
        common(p)
        p.add_argument("--dry-run", action="store_true")
        p.add_argument("--max-objects", type=int, default=None)
        p.add_argument("--workers", type=int, default=None)
    p = sub.add_parser("probe", help="sondas de permisos")
    common(p)
    p.add_argument("--no-gcs", action="store_true")
    p.add_argument("--no-aws", action="store_true")
    p = sub.add_parser("token", help="claims del ID token de sa-exporter")
    common(p)
    p.add_argument("--unique-id", default=None, help="uniqueId numérico de sa-exporter")
    p.add_argument("--trust-json", default=None, help="JSON de la confianza del rol (o de aws iam get-role)")
    p.add_argument("--trust-from-aws", action="store_true", help="lee la confianza con iam get-role y el perfil")
    p = sub.add_parser("doctor", help="diagnóstico")
    common(p)
    p.add_argument("--offline", action="store_true", help="sin tocar GCS")
    for name in ("verify", "lake-report"):
        p = sub.add_parser(name)
        common(p, aws_default="profile")
        if name == "verify":
            p.add_argument("--sample", type=int, default=50)
            p.add_argument("--keys-from", default=None, help="JSON de un run: verifica sus uploaded_keys")
            p.add_argument("--cursor", default=None, help="usar este cursor en vez del watermark")
    p = sub.add_parser("status", help="watermark")
    common(p)
    return parser


def main(argv: list[str] | None = None, *, env: dict[str, str] | None = None, deps: Deps | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    log = make_logger()
    deps = deps or Deps()
    try:
        return COMMANDS[args.command](args, env, deps, log)
    except (ConfigError, IdentityError, WatermarkError) as exc:
        log("export_error", "ERROR", command=args.command, error=f"{type(exc).__name__}: {exc}")
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001 - p. ej. StateConflict o un 412 al leer el watermark
        import traceback

        log("export_error", "ERROR", command=args.command, error=f"{type(exc).__name__}: {str(exc)[:500]}")
        traceback.print_exc(file=sys.stderr)
        return EXIT_FAIL


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
