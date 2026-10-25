"""CLI de ``scripts/verify_enriched.py`` (Lote 2.2). Ver ``streaming.enriched_check``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from streaming.enriched_check import (
    FAIL,
    SOURCES,
    InfraError,
    Report,
    Thresholds,
    check_bigquery,
    check_lake,
    open_lake,
)

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _default_lake_root(args: argparse.Namespace, env: dict) -> str | None:
    if args.lake_root:
        return args.lake_root
    if args.local or env.get("PUBSUB_EMULATOR_HOST"):
        return str(PROJECT_DIR / ".local-lake")
    project = args.project or env.get("GCP_PROJECT")
    return f"gs://{project}-ta-lake" if project else None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="verify_enriched.py", description="Verifica enriched/ y posts_enriched (Lote 2.2).")
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("report", help="Revisa las últimas horas de enriched/ (y BigQuery)")
    r.add_argument("--hours", type=float, default=1.0, help="Horas hacia atrás (por defecto 1)")
    r.add_argument("--lake-root", default=None, help="gs://bucket o carpeta local (por defecto según el entorno)")
    r.add_argument("--local", action="store_true", help="Usar project/.local-lake")
    r.add_argument("--project", default=None, help="Proyecto de GCP (por defecto GCP_PROJECT)")
    r.add_argument("--bigquery", choices=("auto", "yes", "no"), default="auto", help="auto: sí si el lake es gs://")
    r.add_argument("--table", default=None, help="proyecto.dataset.tabla (por defecto {project}.ta_stream.posts_enriched)")
    r.add_argument("--sources", default=",".join(SOURCES))
    r.add_argument("--window-seconds", type=int, default=None, help="Ventana de archivos (300 en Dataflow, 60 en local)")
    r.add_argument("--min-rows", type=int, default=1)
    r.add_argument("--min-coverage", type=float, default=0.95)
    r.add_argument("--max-extra-copies", type=int, default=0)
    r.add_argument("--settle-seconds", type=int, default=600, help="Ignorar archivos de raw/ más nuevos que esto")
    r.add_argument("--expect-nlp", action="store_true", help="Exige el NLP del Lote 2.3 en enriched/ y BigQuery")
    r.add_argument("--max-nlp-null-ratio", type=float, default=0.01)
    r.add_argument("--json-out", default=None, help="Guarda el reporte en JSON (evidencia)")
    return p


def run(
    argv: list[str] | None = None,
    *,
    env: dict | None = None,
    now: Callable[[], datetime] | None = None,
    lake_client: Any = None,
    bq_query: Any = None,
    out=sys.stdout,
) -> int:
    env = dict(os.environ) if env is None else env
    args = build_parser().parse_args(argv)
    when = (now or (lambda: datetime.now(timezone.utc)))()
    root = _default_lake_root(args, env)
    if not root:
        print("Falta --lake-root o GCP_PROJECT.", file=out)
        return 2
    is_cloud = root.startswith("gs://")
    window = args.window_seconds or (300 if is_cloud else 60)
    th = Thresholds(
        min_rows=args.min_rows,
        max_extra_copies=args.max_extra_copies,
        min_coverage=args.min_coverage,
        settle_seconds=args.settle_seconds,
        max_nlp_null_ratio=args.max_nlp_null_ratio,
    )
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        print(f"Fuentes desconocidas: {unknown}", file=out)
        return 2
    use_bq = args.bigquery == "yes" or (args.bigquery == "auto" and is_cloud)
    project = args.project or env.get("GCP_PROJECT")
    table = args.table or (f"{project}.ta_stream.posts_enriched" if project else None)
    if use_bq and not table:
        print("Para BigQuery hace falta --table o GCP_PROJECT.", file=out)
        return 2

    report = Report()
    report.stats["lake_root"] = root
    report.stats["hours"] = args.hours
    report.stats["generated_at"] = when.isoformat()
    try:
        lake = open_lake(root, lake_client)
        report, lake_ids = check_lake(lake, hours=args.hours, now=when, window_seconds=window, thresholds=th, sources=sources, report=report, expect_nlp=args.expect_nlp)
        if use_bq:
            check_bigquery(table, since=when - timedelta(hours=args.hours), lake_ids=lake_ids, report=report, thresholds=th, query=bq_query, expect_nlp=args.expect_nlp)
    except InfraError as exc:
        print(f"Error de infraestructura: {exc}", file=out)
        return 2

    print(report.to_text(), file=out)
    for source in sources:
        s = report.stats.get(source) or {}
        print(f"  {source}: {s.get('rows', 0)} filas · {s.get('unique_event_ids', 0)} ids · p95 {s.get('latency_ms_p95')} ms", file=out)
        if args.expect_nlp and s.get("sentiment_counts") is not None:
            gt = s.get("sentiment_vs_ground_truth") or {}
            print(f"    sentimiento {s['sentiment_counts']} · emoción {s['emotion_counts']} · acuerdo con ground truth {gt.get('accuracy')}", file=out)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(report.to_json(), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 1 if report.verdict == FAIL else 0


def main(argv: list[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
