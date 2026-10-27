"""CLI del Lote 5.2 (``python project/scripts/run_dq.py …`` con el venv ``~/.venvs/ta-gx``).

    sync   [--layers raw,trusted,curated]   baja de S3 lo que falte en la caché (solo lee S3; como el 5.1)
    run    [--layers …] [--publish s3,bq]   valida con GX, escribe {caché}/dq_runs/{run_id}/ y, si se pide, publica
    rules  [--json]                         el catálogo: regla → expectativa de GX (native, flag o metric)

Salida: una línea JSON por evento (``dq_sync``, ``dq_done``, ``dq_published``); código 0 si ninguna regla de gravedad
``error`` falló, 1 si alguna, 2 si faltan datos o credenciales.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dq.catalog import all_rules, curated_specs, layer_specs

PREFIXES = {"raw": "raw/", "trusted": "trusted/posts/", "curated": "curated/"}


def _emit(event: str, **fields) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def _layers(text: str) -> tuple[str, ...]:
    out = tuple(x.strip() for x in text.split(",") if x.strip())
    bad = [x for x in out if x not in PREFIXES]
    if bad:
        raise SystemExit(f"capas desconocidas: {bad} (raw, trusted, curated)")
    return out


def _bucket(args) -> str:
    from quality.s3sync import account_id, resolve_bucket

    return resolve_bucket(args.bucket, account=lambda: account_id(args.aws_profile))


class NoFolders:
    """Puerto de ``quality.s3sync`` sin las «carpetas» (claves que terminan en ``/``, como ``curated/`` que crea
    Terraform, HU-007): ``sync_prefix`` del 5.1 intentaría bajarlas como archivo y contaría una falla."""

    def __init__(self, port) -> None:
        self.port = port

    def list_objects(self, bucket: str, prefix: str) -> list[dict]:
        return [o for o in self.port.list_objects(bucket, prefix) if not o["key"].endswith("/")]

    def download(self, bucket: str, key: str, dest: Path) -> None:
        self.port.download(bucket, key, dest)


def cmd_sync(args) -> int:
    from quality.s3sync import Boto3S3, resolve_cache, sync_prefix

    cache, bucket = resolve_cache(args.cache), _bucket(args)
    port = NoFolders(Boto3S3(profile=args.aws_profile))
    code = 0
    for layer in _layers(args.layers):
        rep = sync_prefix(port, bucket, PREFIXES[layer], cache)
        _emit("dq_sync", layer=layer, **{k: v for k, v in rep.to_json().items() if k != "failures"}, failures=rep.failures[:5])
        code = code or (0 if rep.ok else 1)
    return code


def cmd_run(args) -> int:
    from quality.s3sync import bucket_root, resolve_cache

    from dq import run as R

    cache = resolve_cache(args.cache)
    bucket = args.bucket or "ta-dev-648617469176-lake"
    root = bucket_root(cache, bucket)
    layers = _layers(args.layers)
    missing = [lay for lay in layers if not (root / PREFIXES[lay]).is_dir()]
    if "curated" in layers and not (root / PREFIXES["trusted"]).is_dir():
        missing.append("trusted (Curated lo necesita)")
    if missing:
        _emit("dq_error", error=f"faltan en la caché {root}: {missing} (corra antes: run_dq.py sync)")
        return 2
    publish = {x.strip() for x in (args.publish or "").split(",") if x.strip()}
    out = Path(args.out).expanduser() if args.out else cache / "dq_runs"
    summary = R.run(root, out, layers=layers, log=lambda m: print(m, file=sys.stderr, flush=True))
    folder = out / summary["run_id"]
    _emit("dq_done", folder=str(folder), **{k: summary[k] for k in ("run_id", "ok", "rows", "passed", "errors_failed",
                                                                   "warnings_failed", "kpis", "suites", "duration_s", "nlp",
                                                                   "great_expectations", "skipped", "inputs")})
    if "s3" in publish:
        import boto3

        session = boto3.Session(profile_name=args.aws_profile) if args.aws_profile else boto3.Session()
        items = R.s3_items(folder, summary["run_id"], summary["run_ts"])
        n = R.publish_s3(session.client("s3"), args.bucket or bucket, items)
        _emit("dq_published", target="s3", bucket=bucket, objects=n, prefix=items[0][0].rsplit("/", 2)[0] + "/")
    if "bq" in publish:
        from google.cloud import bigquery

        project = args.gcp_project or R.default_gcp_project()
        if not project:
            _emit("dq_error", error="sin proyecto de GCP (--gcp-project o TA_GCP_PROJECT)")
            return 2
        rows = json.loads((folder / "dq_results.json").read_text(encoding="utf-8"))
        table = f"{project}.{R.DQ_TABLE}"
        errors = R.publish_bq(bigquery.Client(project=project), table, rows)
        _emit("dq_published", target="bigquery", table=R.DQ_TABLE, rows=len(rows), errors=errors[:5])
        if errors:
            return 1
    return 0 if summary["ok"] else 1


def cmd_rules(args) -> int:
    kinds = {s.rule: s.kind for s in layer_specs("raw") + layer_specs("trusted") + curated_specs()}
    rows = [{"id": r.id, "layers": list(r.layers), "column": r.column, "check": r.check, "kind": kinds.get(r.id),
             "severity": r.severity, "kpi": r.kpi} for r in all_rules()]
    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=1))
    else:
        for r in rows:
            print(f"{r['id']}  {'/'.join(r['layers']):12} {r['kind'] or '—':7} {r['severity']:8} {r['kpi']:13} {r['check']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="run_dq.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("sync", "run"):
        s = sub.add_parser(name)
        s.add_argument("--layers", default="raw,trusted,curated")
        s.add_argument("--cache", default=None, help="caché del profiling (TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
        s.add_argument("--bucket", default=None, help="bucket del lake (TA_LAKE_BUCKET o ta-dev-{cuenta}-lake)")
        s.add_argument("--aws-profile", default=None)
    r = sub.choices["run"]
    r.add_argument("--out", default=None, help="carpeta de salida (por defecto {caché}/dq_runs; fuera del repo)")
    r.add_argument("--publish", default="", help="s3, bq o s3,bq (por defecto, nada)")
    r.add_argument("--gcp-project", default=None)
    sub.add_parser("rules").add_argument("--json", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return {"sync": cmd_sync, "run": cmd_run, "rules": cmd_rules}[args.cmd](args)
