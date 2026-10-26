"""CLI del *profiling* (Lote 5.1): ``python project/scripts/profile_data.py <subcomando>``.

Subcomandos (detalle y salidas en ``ARCHITECTURE.md`` §3.20.1):

* ``sync``: baja a la caché local los Parquet de ``raw/`` o ``trusted/posts/`` (solo lectura de S3).
* ``profile``: perfila una capa desde la caché (o ``--root``) y escribe el JSON y el Markdown.
* ``rules``: el catálogo de reglas propuestas (``--catalog``) o su evaluación sobre un JSON.
* ``synth-trusted``: arma un Trusted simulado (contrato de §5.7) desde una carpeta Raw.
* ``publish``: plan de subida del informe a ``s3://…/quality/`` (sube solo con ``--apply``).

Salida: 0 bien, 1 una comprobación falló (descargas fallidas, ``--fail-on``), 2 error de uso o
configuración. Nunca imprime credenciales ni valores de ``user_hash``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

from quality import layers as L

EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2
SYNTH_MARKER = "_synthetic_trusted.json"


def _write_json(path: str | Path, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=1, default=str) + "\n", encoding="utf-8")


def _bucket(args, env: dict[str, str]) -> str:
    from quality import s3sync

    return s3sync.resolve_bucket(args.bucket, env=env, account=lambda: s3sync.account_id(args.aws_profile))


def _cache(args, env: dict[str, str]) -> Path:
    from quality import s3sync

    return s3sync.resolve_cache(args.cache, env=env)


def cmd_sync(args, env, out: Callable[[str], None], port=None) -> int:
    from quality import s3sync

    cache = _cache(args, env)
    bucket = _bucket(args, env)
    prefix = s3sync.LAYER_PREFIXES[args.layer]
    if args.origin:
        if args.layer != "raw":
            out("error: --origin solo aplica a --layer raw")
            return EXIT_USAGE
        prefix = f"raw/origin={args.origin}/"
    port = port or s3sync.Boto3S3(profile=args.aws_profile, region=env.get("AWS_REGION"))
    rep = s3sync.sync_prefix(port, bucket, prefix, cache, workers=args.workers, dry_run=args.dry_run, progress=out)
    data = rep.to_json()
    out(f"s3://{bucket}/{prefix}: {rep.objects} objetos ({rep.bytes / 1e6:.1f} MB); por bajar {rep.to_download}, bajados {rep.downloaded} "
        f"({rep.downloaded_bytes / 1e6:.1f} MB), ya estaban {rep.skipped_same_size}, fallidos {rep.failed}, "
        f"locales que ya no están en S3 {rep.stale_local}{' (simulado)' if rep.dry_run else ''} · caché {rep.cache_root}")
    for k, v in sorted(rep.by_prefix.items()):
        out(f"  {k}: {v['objects']} objetos, {v['bytes'] / 1e6:.1f} MB")
    for f in rep.failures[:5]:
        out(f"  fallo: {f['key']}: {f['error']}")
    if args.json_out:
        _write_json(args.json_out, data)
    return EXIT_OK if rep.ok else EXIT_FAIL


def _summary_lines(report: dict[str, Any]) -> list[str]:
    s = report["summary"]
    lines = [f"{report['table']}: {s['files']} archivos, {s['bytes'] / 1e6:.1f} MB, {report.get('duration_s')} s"]
    for g in report["groups"]:
        k = g["key"]
        if k["source"] == "*" or k["origin"] != "*":
            lines.append(f"  {k['origin']}/{k['source']}: {g['rows']} filas")
    for origin, kpi in s["kpi"].items():
        lines.append(f"  KPI {origin}: completitud {kpi['completitud']}, unicidad {kpi['unicidad']}, "
                     f"duplicados {kpi['duplicados']}, anonimización {kpi['anonimizacion']}")
    lines.append(f"  anomalías: {s['anomalies']}")
    lines.append(f"  reglas: {s['rules']}")
    return lines


def cmd_profile(args, env, out: Callable[[str], None]) -> int:
    from quality import profiler, report_md, s3sync

    manifest = None
    if args.root:
        root = Path(args.root).expanduser()
        source_uri = "local"
    else:
        cache = _cache(args, env)
        bucket = _bucket(args, env)
        root = s3sync.bucket_root(cache, bucket)
        source_uri = f"s3://{bucket}/{L.layer_spec(args.layer).prefix} (caché local)"
        manifest = s3sync.read_manifest(cache, bucket, args.layer)
    if not (root / L.layer_spec(args.layer).prefix).exists():
        out(f"error: no hay {L.layer_spec(args.layer).prefix} en {root}; corra antes `sync --layer {args.layer}` o use --root")
        return EXIT_USAGE
    raw_report = None
    if args.raw_profile:
        raw_report = json.loads(Path(args.raw_profile).read_text(encoding="utf-8"))
    report = profiler.profile_layer(root, args.layer, as_of=profiler.as_of_from_text(args.as_of), source_uri=source_uri,
                                    max_files=args.max_files, progress=None if args.quiet else out, sync_manifest=manifest,
                                    raw_report=raw_report)
    if args.json_out:
        _write_json(args.json_out, report)
    if args.md_out:
        p = Path(args.md_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(report_md.render(report) + "\n", encoding="utf-8")
    for line in _summary_lines(report):
        out(line)
    sev = report["summary"]["anomalies"]
    if args.fail_on == "error" and sev["error"]:
        return EXIT_FAIL
    if args.fail_on == "warning" and (sev["error"] or sev["warning"]):
        return EXIT_FAIL
    return EXIT_OK


def cmd_rules(args, env, out: Callable[[str], None]) -> int:
    from quality import rules

    if args.catalog or not args.from_json:
        data = [r.public() for r in rules.RULES]
        if args.json_out:
            _write_json(args.json_out, data)
        for r in rules.RULES:
            out(f"{r.id} [{'/'.join(r.layers)}{'' if r.scope == '*' else ' · ' + r.scope}] {r.column} · {r.check} · {r.threshold} · {r.kpi} · {r.severity}")
        return EXIT_OK
    report = json.loads(Path(args.from_json).read_text(encoding="utf-8"))
    raw_report = json.loads(Path(args.raw_profile).read_text(encoding="utf-8")) if args.raw_profile else None
    evaluated = rules.evaluate(report, raw_report)
    if args.json_out:
        _write_json(args.json_out, evaluated)
    for r in evaluated:
        out(f"{r['id']} {r['status']:<9} {r['column']} · {r['check']} · {r['threshold']} · observado {r['observed']}")
    return EXIT_OK


def cmd_synth(args, env, out: Callable[[str], None]) -> int:
    from quality import s3sync, testing

    cache = _cache(args, env)
    if args.from_root:
        raw_root = Path(args.from_root).expanduser()
    else:
        raw_root = s3sync.bucket_root(cache, _bucket(args, env))
    out_root = Path(args.out).expanduser() if args.out else cache / "synthetic" / ("trusted-defects" if args.defects else "trusted")
    s3sync.resolve_cache(out_root, env=env)  # la salida tampoco puede caer dentro del repo
    if not (raw_root / "raw").exists():
        out(f"error: no hay raw/ en {raw_root}")
        return EXIT_USAGE
    marker = out_root / SYNTH_MARKER
    if (out_root / "trusted").exists():
        if not marker.exists():
            out(f"error: {out_root} ya tiene trusted/ y no lo creó synth-trusted (falta {SYNTH_MARKER}); use otra --out")
            return EXIT_USAGE
        import shutil

        shutil.rmtree(out_root / "trusted")
    info = testing.build_trusted(raw_root, out_root, defects=args.defects, limit_files=args.limit_files)
    info["out_root"] = str(out_root)
    marker.write_text(json.dumps(info, ensure_ascii=False) + "\n", encoding="utf-8")
    out(f"Trusted simulado en {out_root}: {info['files']} archivos, {info['distinct_event_ids']} event_id distintos "
        f"de {info['raw_rows']} filas de Raw; defectos: {info['defects'] or 'ninguno'}")
    if args.json_out:
        _write_json(args.json_out, info)
    return EXIT_OK


def cmd_publish(args, env, out: Callable[[str], None], client=None) -> int:
    from quality import publish

    report = json.loads(Path(args.from_json).read_text(encoding="utf-8"))
    files = [Path(args.from_json)] + ([Path(args.md)] if args.md else [])
    bucket = _bucket(args, env)
    items = publish.plan(report, files)
    if args.apply and client is None:
        import boto3

        session = boto3.Session(profile_name=args.aws_profile) if args.aws_profile else boto3.Session()
        client = session.client("s3", region_name=env.get("AWS_REGION", "us-east-1"))
    res = publish.publish(client, bucket, items, kms_key_arn=env.get("TA_LAKE_KMS_KEY_ARN"), apply=args.apply)
    for r in res:
        out(f"{'subido' if r['uploaded'] else 'plan (sin --apply no sube)'}: {r['uri']}")
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="profile_data.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp, *, bucket=True):
        if bucket:
            sp.add_argument("--bucket", default=None, help="bucket del lake (si no: TA_LAKE_BUCKET o ta-{ENV}-{cuenta}-lake)")
            sp.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no: AWS_PROFILE o el por defecto)")
        sp.add_argument("--cache", default=None, help="caché local (si no: TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
        sp.add_argument("--json-out", default=None)

    s = sub.add_parser("sync", help="baja a la caché los Parquet de una capa (solo lectura de S3)")
    s.add_argument("--layer", choices=list(L.LAYERS), required=True)
    s.add_argument("--origin", choices=list(L.ORIGINS), default=None)
    s.add_argument("--workers", type=int, default=16)
    s.add_argument("--dry-run", action="store_true", help="solo lista y cuenta lo que bajaría")
    common(s)

    pr = sub.add_parser("profile", help="perfila una capa y escribe JSON y Markdown")
    pr.add_argument("--layer", choices=list(L.LAYERS), required=True)
    pr.add_argument("--root", default=None, help="carpeta local con el layout del bucket (en vez de la caché)")
    pr.add_argument("--as-of", default=None, help="instante de referencia UTC (por defecto, ahora)")
    pr.add_argument("--raw-profile", default=None, help="Trusted: JSON del profiling de Raw de la misma foto (regla DQ-58)")
    pr.add_argument("--md-out", default=None)
    pr.add_argument("--max-files", type=int, default=None)
    pr.add_argument("--fail-on", choices=["never", "error", "warning"], default="never")
    pr.add_argument("--quiet", action="store_true")
    common(pr)

    r = sub.add_parser("rules", help="catálogo de reglas propuestas o su evaluación sobre un JSON")
    r.add_argument("--catalog", action="store_true")
    r.add_argument("--from", dest="from_json", default=None)
    r.add_argument("--raw-profile", default=None)
    r.add_argument("--json-out", default=None)

    sy = sub.add_parser("synth-trusted", help="Trusted simulado (contrato de §5.7) desde una carpeta Raw")
    sy.add_argument("--from-root", default=None, help="carpeta con raw/ (por defecto la caché del bucket)")
    sy.add_argument("--out", default=None, help="carpeta de salida (por defecto {caché}/synthetic/trusted[-defects])")
    sy.add_argument("--defects", action="store_true", help="añade 4 defectos conocidos")
    sy.add_argument("--limit-files", type=int, default=None)
    common(sy)

    pu = sub.add_parser("publish", help="plan de subida del informe a s3://…/quality/ (sube solo con --apply)")
    pu.add_argument("--from", dest="from_json", required=True)
    pu.add_argument("--md", default=None)
    pu.add_argument("--apply", action="store_true", help="sube de verdad (apagado por defecto)")
    common(pu)
    return p


def main(argv: list[str] | None = None, *, env: dict[str, str] | None = None, out: Callable[[str], None] = print,
         port=None, client=None) -> int:
    from quality.s3sync import CacheError

    env = dict(os.environ) if env is None else env
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return EXIT_USAGE if exc.code else EXIT_OK
    try:
        if args.command == "sync":
            return cmd_sync(args, env, out, port)
        if args.command == "profile":
            return cmd_profile(args, env, out)
        if args.command == "rules":
            return cmd_rules(args, env, out)
        if args.command == "synth-trusted":
            return cmd_synth(args, env, out)
        if args.command == "publish":
            return cmd_publish(args, env, out, client)
    except CacheError as exc:
        out(f"error: {exc}")
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001 - credenciales, red: mensaje corto, sin secretos
        name = type(exc).__name__
        if name in ("NoCredentialsError", "ProfileNotFound", "ClientError", "EndpointConnectionError", "NoRegionError"):
            out(f"error de AWS: {name}: {str(exc)[:200]}")
            return EXIT_USAGE
        raise
    return EXIT_USAGE


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
