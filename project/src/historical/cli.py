"""CLI del loader histórico: ``python scripts/load_historical.py <comando>``.

Comandos (se corren desde ``project/`` o desde la raíz del repo):

| Comando    | Dónde       | Qué hace |
|------------|-------------|----------|
| `plan`     | cualquiera  | Muestra el plan (conteos, ventana, episodios). Sin dependencias. |
| `doctor`   | host        | Comprueba los adaptadores contra el código real de los lotes 1.1–2.3. |
| `generate` | host        | Etapa 1 (simulador + anonimización). Con `--digest-only`, solo el digest. |
| `nlp`      | contenedor  | Etapa 2 (NLP en lote). Úsela con `scripts/historical_nlp.sh`. |
| `write`    | host        | Etapa 3 (Parquet por source/dt/hr). |
| `verify`   | host        | Reglas H1–H12; `--bq` suma H13 y `--s3` suma H14. |
| `upload`   | host        | Subida idempotente a S3 (asume `ta-historical-loader`). |
| `status`   | cualquiera  | Estado de las etapas. |

Salida: 0 = pasa (con o sin avisos), 1 = falla, 2 = error de uso o de infraestructura.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Sequence

from historical.plan import (UTC, PlanError, format_ts, load_config, plan_summary,
                             resolve_as_of)
from historical.work import DEFAULT_ROOT, WorkDir, atomic_write_json, sha256_file

EXIT_OK, EXIT_FAIL, EXIT_ERROR = 0, 1, 2


def _env_int(name: str) -> int | None:
    value = os.environ.get(name)
    return int(value) if value not in (None, "") else None


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--root", default=os.environ.get("TA_HIST_ROOT") or str(DEFAULT_ROOT),
                   help="espejo local del bucket (por defecto project/.local-s3)")
    p.add_argument("--seed", type=int, default=None, help="semilla (HIST_SEED; por defecto 3101)")
    p.add_argument("--config", default=None, help="otro historical.yaml")
    p.add_argument("--json-out", default=None, help="guarda el resultado en JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="load_historical.py",
                                     description="Carga histórica (Lote 3.1, HU-024)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("plan", help="muestra el plan")
    _common(p)
    _plan_args(p)

    p = sub.add_parser("doctor", help="comprueba los adaptadores contra el código real")
    _common(p)
    p.add_argument("--key-source", default="local_dev", choices=("secret", "env", "local_dev"))
    _key_args(p)
    p.add_argument("--nlp", action="store_true", help="también streaming.nlp (dentro de la imagen)")
    p.add_argument("--nlp-run", action="store_true", help="y una inferencia real de 3 filas")
    p.add_argument("--nlp-only", action="store_true",
                   help="solo el NLP (dentro de la imagen, sin simulador)")
    p.add_argument("--nlp-bench", type=int, default=0, metavar="N",
                   help="mide filas/s y tamaño de lote del NLP con N filas por configuración")
    p.add_argument("--bench-workers", default="2,4",
                   help="procesos de las pruebas en paralelo de --nlp-bench, separados por coma (2,4)")
    p.add_argument("--batch-size", type=int, default=None, help=argparse.SUPPRESS)

    p = sub.add_parser("generate", help="etapa 1: simulador + anonimización")
    _common(p)
    _plan_args(p)
    p.add_argument("--key-source", default="secret", choices=("secret", "env", "local_dev"))
    _key_args(p)
    p.add_argument("--check-key", default="auto", choices=("auto", "bq", "none"),
                   help="cruza user_hash con posts_enriched (auto = bq con la llave del secreto)")
    p.add_argument("--bq-table", default=None, help="por defecto {GCP_PROJECT}.ta_stream.posts_enriched")
    p.add_argument("--validate", default="auto", choices=("auto", "yes", "no"),
                   help="valida cada envelope contra contracts/envelope.v1")
    p.add_argument("--force", action="store_true", help="regenera aunque el plan no cambió")
    p.add_argument("--digest-only", action="store_true",
                   help="no anonimiza ni escribe: solo calcula el digest y lo compara")
    p.add_argument("--new-as-of", action="store_true",
                   help="no reutiliza el as_of del generate.json anterior")

    p = sub.add_parser("nlp", help="etapa 2: NLP en lote (dentro de la imagen de NLP)")
    _common(p)
    p.add_argument("--batch-size", type=int, default=_env_int("TA_HIST_NLP_BATCH_SIZE"))
    p.add_argument("--batch-seconds", type=float, default=None,
                   help="espera para llenar cada lote (por defecto, la del pipeline: 5 s)")
    p.add_argument("--runner", default="bundle", help=argparse.SUPPRESS)
    p.add_argument("--workers", type=int, default=_env_int("TA_HIST_NLP_WORKERS") or 1,
                   help="procesos en paralelo (cada uno con sus modelos y núcleos/procesos hilos)")
    p.add_argument("--max-files", type=int, default=None)
    p.add_argument("--no-retry", action="store_true")
    p.add_argument("--force", action="store_true")

    p = sub.add_parser("write", help="etapa 3: Parquet por source/dt/hr")
    _common(p)
    p.add_argument("--validate", default="auto", choices=("auto", "yes", "no"))
    p.add_argument("--allow-missing-nlp", action="store_true", help="solo para pruebas")

    p = sub.add_parser("verify", help="reglas del lote")
    _common(p)
    p.add_argument("--bq", action="store_true", help="H13: cruce con posts_enriched")
    p.add_argument("--bq-table", default=None)
    p.add_argument("--s3", action="store_true", help="H14: objetos en S3")
    _aws_args(p)
    p.add_argument("--allow-fake-nlp", action="store_true", help=argparse.SUPPRESS)

    p = sub.add_parser("upload", help="subida idempotente a S3")
    _common(p)
    _aws_args(p)
    p.add_argument("--no-role", action="store_true",
                   help="sube con el perfil, sin asumir ta-historical-loader")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--probe", action="store_true",
                   help="comprueba que el rol NO puede escribir fuera de raw/origin=historical/")

    p = sub.add_parser("status", help="estado de las etapas")
    _common(p)
    return parser


def _plan_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--n", type=int, default=None, help="total de registros (HIST_N; 50000)")
    p.add_argument("--days", type=int, default=None, help="días hacia atrás (180)")
    p.add_argument("--as-of", default=None, help="fin de la ventana, AAAA-MM-DD (hoy 00:00 UTC)")
    p.add_argument("--chunk-size", type=int, default=None)


def _key_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--gcp-project", default=os.environ.get("GCP_PROJECT"))
    p.add_argument("--hmac-secret-name", default=os.environ.get("TA_HMAC_SECRET_NAME", "ta-hmac-key"))
    p.add_argument("--key-env", default="TA_HMAC_KEY", help="variable con la llave (--key-source env)")


def _aws_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bucket", default=os.environ.get("TA_LAKE_BUCKET"))
    p.add_argument("--kms-key-arn", default=os.environ.get("TA_LAKE_KMS_KEY_ARN"))
    p.add_argument("--role-arn", default=os.environ.get("TA_HIST_LOADER_ROLE_ARN"))
    p.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))


# ------------------------------------------------------------------ utilidades

def _emit(result: Any, json_out: str | None) -> None:
    if json_out:
        path = Path(json_out)
        atomic_write_json(path, result)
        print(f"evidencia: {path}")


def _config(args) -> Any:
    cfg = load_config(args.config)
    seed = args.seed if args.seed is not None else (_env_int("HIST_SEED") or cfg.seed)
    n = getattr(args, "n", None) or _env_int("HIST_N")
    return cfg.replace(seed=seed, n=n, days=getattr(args, "days", None),
                       chunk_size=getattr(args, "chunk_size", None))


def _work(args, seed: int | None = None) -> WorkDir:
    if seed is None:
        seed = args.seed if args.seed is not None else (_env_int("HIST_SEED") or load_config(args.config).seed)
    return WorkDir(args.root, seed)


def _resolve_plan(args) -> tuple[Any, dt.datetime, WorkDir, dict | None]:
    cfg = _config(args)
    work = WorkDir(args.root, cfg.seed)
    prev = work.read_json(work.generate_json)
    value = args.as_of or (None if getattr(args, "new_as_of", False) or not prev
                           else prev.get("plan", {}).get("as_of")) or cfg.as_of
    as_of = resolve_as_of(value)
    cfg = cfg.replace(as_of=format_ts(as_of))
    return cfg, as_of, work, prev


def _print_checks(report: dict[str, Any]) -> None:
    for c in report["checks"]:
        print(f"  {c['id']:<4} {c['status']:<6} {c['name']}: {c['detail']}")
    print(f"resultado: {report['status']}")


def _validator(mode: str, kind: str):
    if mode == "no":
        return None, "no"
    from historical.schema import SchemaUnavailable, load_validators

    try:
        return load_validators()[kind], "yes"
    except SchemaUnavailable as exc:
        if mode == "yes":
            raise
        print(f"AVISO: sin validación contra contracts/ ({exc})")
        return None, f"omitida: {exc}"


# --------------------------------------------------------------------- comandos

def cmd_plan(args) -> int:
    cfg, as_of, work, _ = _resolve_plan(args)
    summary = plan_summary(cfg, as_of)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    _emit(summary, args.json_out)
    return EXIT_OK


def cmd_doctor(args) -> int:
    from historical import doctor

    report = doctor.run(args)
    for step in report["steps"]:
        print(f"  {step['id']:<4} {step['status']:<6} {step['name']}: {step['detail']}")
    print(f"resultado: {report['status']}")
    _emit(report, args.json_out)
    return EXIT_OK if report["status"] != "FALLA" else EXIT_FAIL


def _stage1_complete(work: WorkDir, prev: dict | None) -> bool:
    if not prev or not prev.get("stage1"):
        return False
    for f in prev["stage1"]:
        path = work.stage1 / f["file"]
        if not path.exists() or sha256_file(path) != f["sha256"]:
            return False
    return True


def cmd_generate(args) -> int:
    from historical import adapters
    from historical.generate import generate, same_plan

    cfg, as_of, work, prev = _resolve_plan(args)
    print(f"plan: seed={cfg.seed} n={cfg.n} days={cfg.days} as_of={format_ts(as_of)} "
          f"root={work.root}")
    if args.digest_only:
        envelopes = adapters.SimulatorEnvelopes.build(cfg.seed)
        result = generate(cfg, as_of, work, envelopes=envelopes, enrich=None, digest_only=True)
        same = prev.get("digest") == result["digest"] if prev else None
        result["previous_digest"] = prev.get("digest") if prev else None
        result["same_as_previous"] = same
        result.pop("seqs", None)
        print(f"digest: {result['digest']} ({'igual al de generate.json' if same else 'distinto' if same is False else 'sin generate.json previo'})")
        _emit(result, args.json_out)
        return EXIT_OK if same in (True, None) else EXIT_FAIL
    same_key = (prev or {}).get("key", {}).get("source") == args.key_source
    if (prev and same_plan(prev, cfg, as_of) and same_key and _stage1_complete(work, prev)
            and not args.force):
        print(f"etapa 1 ya generada con este plan (digest {prev['digest']}); use --force para repetir")
        _emit({k: v for k, v in prev.items() if k != "seqs"}, args.json_out)
        return EXIT_OK
    envelopes = adapters.SimulatorEnvelopes.build(cfg.seed)
    key = adapters.load_key(args.key_source, gcp_project=args.gcp_project,
                            secret_name=args.hmac_secret_name, env_var=args.key_env)
    print(f"llave: {key.detail} · huella {key.fingerprint()}"
          + (f" · intentos previos: {'; '.join(key.attempts)}" if key.attempts else ""))
    check_mode = args.check_key
    if check_mode == "auto":
        check_mode = "bq" if args.key_source == "secret" else "none"
    key_check = None
    table = None
    if check_mode == "bq":
        from historical import bq

        table = bq.normalize_table(args.bq_table or bq.default_table(args.gcp_project))
        if key.alternatives:
            key = _probe_key(key, envelopes, table, as_of)
        key_check = bq.make_key_check(table)
    validate, vmode = _validator(args.validate, "envelope")
    process_time = dt.datetime.now(UTC)
    enrich = adapters.make_enricher(key.value, process_time)
    result = generate(cfg, as_of, work, envelopes=envelopes, enrich=enrich,
                      key_info={"source": key.source, "detail": key.detail,
                                "fingerprint": key.fingerprint(), "check": check_mode,
                                "table": table, "attempts": list(key.attempts)},
                      validate_envelope=validate, key_check=key_check, process_time=process_time)
    result["validation"] = vmode
    atomic_write_json(work.generate_json, result)
    short = {k: v for k, v in result.items() if k not in ("seqs", "config")}
    print(json.dumps({k: short[k] for k in ("digest", "rows", "users", "key_check")
                      if k in short}, ensure_ascii=False, indent=2))
    for s, st in result["sources"].items():
        print(f"  {s}: {st['accepted']} filas, aceptación {st['acceptance']}, "
              f"forzados {st['forced']}, marcas {st['brands']}")
    _emit(short, args.json_out)
    return EXIT_OK


def _probe_key(key, envelopes, table: str, as_of: dt.datetime):
    """Con la lectura por ``gcloud`` hay dos variantes de la llave: usa la que coincide."""
    from historical import adapters, bq

    sample = [envelopes.envelope(s, seq, as_of - dt.timedelta(days=1))
              for s in ("reddit", "facebook", "instagram") for seq in range(200)]
    now = dt.datetime.now(UTC)
    for candidate in (key.value, *key.alternatives):
        enrich = adapters.make_enricher(candidate, now)
        hashes = sorted({enrich(e)["author"]["user_hash"] for e in sample})
        result = bq.run_query(table, hashes)
        if result["hit_users"] > 0:
            if candidate is not key.value:
                print("AVISO: la llave del secreto coincide con el stream sin el salto de línea final")
            return adapters.KeyInfo(candidate, key.source, key.detail)
    return key


def cmd_nlp(args) -> int:
    from historical import adapters
    from historical.nlp_batch import BeamNlpEngine, run_nlp_stage

    if args.runner != "bundle":
        from historical.nlp_batch import FNAPI_UNSAFE

        print(f"ERROR: --runner {args.runner}: "
              + (FNAPI_UNSAFE if args.runner == "fnapi" else "solo existe bundle"),
              file=sys.stderr)
        return EXIT_ERROR
    work = _work(args)
    kwargs = {"batch_size": args.batch_size, "batch_seconds": args.batch_seconds}
    if args.workers > 1:
        summary = run_nlp_stage(work, None, max_files=args.max_files,
                                retry_failed=not args.no_retry, force=args.force,
                                workers=args.workers, engine_spec={"kind": "beam", "kwargs": kwargs},
                                to_row_name="adapters")
    else:
        summary = run_nlp_stage(work, BeamNlpEngine(**kwargs), to_row=adapters.to_row,
                                max_files=args.max_files, retry_failed=not args.no_retry,
                                force=args.force)
    print(json.dumps({k: summary[k] for k in ("files", "pending_files", "rows", "nlp_missing",
                                              "versions", "rows_per_s_median")},
                     ensure_ascii=False, indent=2))
    _emit(summary, args.json_out)
    if summary["pending_files"]:
        print(f"quedan {summary['pending_files']} archivos: repita el comando para seguir")
        return EXIT_FAIL
    return EXIT_OK if summary["nlp_missing"] == 0 else EXIT_FAIL


def cmd_write(args) -> int:
    from historical import adapters
    from historical.writer import write_output

    work = _work(args)
    validate, vmode = _validator(args.validate, "enriched")
    result = write_output(work, to_row=adapters.to_row, rows_to_parquet=adapters.rows_to_parquet,
                          validate_enriched=validate, allow_missing_nlp=args.allow_missing_nlp)
    result["validation"] = vmode
    atomic_write_json(work.output_json, result)
    print(f"{result['n_files']} archivos ({result['written']} escritos, {result['unchanged']} "
          f"sin cambios, {len(result['removed'])} borrados), {result['rows']} filas, "
          f"{result['bytes'] / 1e6:.1f} MB en {work.hist_root}")
    _emit({k: v for k, v in result.items() if k != "files"}, args.json_out)
    return EXIT_OK


def _candidate_fn(work: WorkDir):
    """Marca y ground truth de un candidato del simulador (para H7), o ``None`` si el simulador
    no carga aquí: H7 cae entonces a suponer una mezcla de marcas uniforme y lo dice."""
    from historical import adapters
    from historical.generate import _brand_label

    gen = work.read_json(work.generate_json) or {}
    seed = int((gen.get("config") or {}).get("seed") or work.seed)
    try:
        envelopes = adapters.SimulatorEnvelopes.build(seed)
    except Exception as exc:  # noqa: BLE001
        print(f"AVISO: sin el simulador ({type(exc).__name__}: {exc}); H7 supone que su mezcla "
              "de marcas no cambia con seq")
        return None

    def candidate(source: str, seq: int, when: dt.datetime):
        return _brand_label(envelopes.envelope(source, seq, when))

    return candidate


def cmd_verify(args) -> int:
    from historical.verify import run_checks

    work = _work(args)
    columns = None
    try:
        from historical import adapters

        columns = adapters.posts_enriched_columns()
    except Exception as exc:  # noqa: BLE001
        print(f"AVISO: sin POSTS_ENRICHED_COLUMNS ({exc}); H2 usa la lista de §5.5")
    bq_check = s3_check = None
    if args.bq:
        from historical import bq

        project = os.environ.get("GCP_PROJECT")
        table = bq.normalize_table(args.bq_table or bq.default_table(project))
        bq_check = bq.make_stream_check(table)
    if args.s3:
        from historical import s3io

        if not args.bucket:
            print("falta --bucket (o TA_LAKE_BUCKET)")
            return EXIT_ERROR
        session = s3io.make_session(args.profile, args.region)
        s3_check = s3io.make_s3_check(session.client("s3"), args.bucket,
                                      kms_key_arn=args.kms_key_arn)
    report = run_checks(work, columns=columns, allow_fake_nlp=args.allow_fake_nlp,
                        candidate=_candidate_fn(work), bq_check=bq_check, s3_check=s3_check)
    _print_checks(report)
    _emit(report, args.json_out)
    return EXIT_FAIL if report["status"] == "FALLA" else EXIT_OK


def cmd_upload(args) -> int:
    from historical import s3io

    if not args.bucket:
        print("falta --bucket (o TA_LAKE_BUCKET): terraform output lake_bucket de infra/aws")
        return EXIT_ERROR
    work = _work(args)
    session = s3io.make_session(args.profile, args.region)
    base = s3io.identity(session)
    if not args.no_role:
        if not args.role_arn:
            print("falta --role-arn (o TA_HIST_LOADER_ROLE_ARN), o use --no-role")
            return EXIT_ERROR
        if str(base.get("arn", "")).endswith(":root"):
            print("el perfil es la cuenta raíz de AWS, que no puede asumir roles: use un usuario "
                  "IAM o SSO, o --no-role (sube con el perfil; el cifrado SSE-KMS es el mismo)")
            return EXIT_ERROR
        session = s3io.assume_role_session(session, args.role_arn, args.region)
    who = s3io.identity(session)
    print(f"perfil: {base.get('arn')} · sube como: {who.get('arn')}")
    client = session.client("s3")
    result = s3io.upload(work, client, args.bucket, kms_key_arn=args.kms_key_arn,
                         workers=args.workers, dry_run=args.dry_run, who=who)
    if args.probe:
        result["permission_probe"] = s3io.permission_probe(client, args.bucket)
    print(json.dumps({k: result[k] for k in ("files", "to_upload", "uploaded",
                                             "skipped_same_size", "n_errors", "n_stale")},
                     ensure_ascii=False))
    if result.get("permission_probe"):
        print(f"permisos fuera del prefijo: {result['permission_probe']}")
    _emit(result, args.json_out)
    probe_ok = all(v == "DENEGADO" for v in (result.get("permission_probe") or {}).values())
    return EXIT_OK if not result["n_errors"] and probe_ok else EXIT_FAIL


def cmd_status(args) -> int:
    from historical.nlp_batch import summarize_stage2

    work = _work(args)
    gen = work.read_json(work.generate_json)
    out = work.read_json(work.output_json)
    up = work.read_json(work.upload_json)
    status = {"work": str(work.dir),
              "generate": None if not gen else {k: gen.get(k) for k in
                                                ("digest", "rows", "generated_at", "plan")},
              "nlp": summarize_stage2(work) if gen else None,
              "write": None if not out else {k: out.get(k) for k in
                                             ("n_files", "rows", "bytes", "written_at")},
              "upload": None if not up else {k: up.get(k) for k in
                                             ("bucket", "uploaded", "skipped_same_size",
                                              "n_errors", "uploaded_at")}}
    print(json.dumps(status, ensure_ascii=False, indent=2, default=str))
    _emit(status, args.json_out)
    return EXIT_OK


COMMANDS = {"plan": cmd_plan, "doctor": cmd_doctor, "generate": cmd_generate,
            "nlp": cmd_nlp, "write": cmd_write, "verify": cmd_verify, "upload": cmd_upload,
            "status": cmd_status}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except (PlanError, ValueError) as exc:
        print(f"error ({type(exc).__name__}): {exc}", file=sys.stderr)
        if os.environ.get("TA_HIST_DEBUG"):
            traceback.print_exc()
        elif not isinstance(exc, PlanError):
            print("(TA_HIST_DEBUG=1 muestra dónde ocurrió)", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 - salida 2 con el detalle
        from historical.adapters import AdapterError

        kind = "adaptador" if isinstance(exc, AdapterError) else type(exc).__name__
        print(f"error ({kind}): {exc}", file=sys.stderr)
        if os.environ.get("TA_HIST_DEBUG"):
            traceback.print_exc()
        return EXIT_FAIL if type(exc).__name__ in ("GenerateError", "WriteError",
                                                   "NlpStageError") else EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
