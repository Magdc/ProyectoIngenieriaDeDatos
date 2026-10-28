"""CLI de comprobación de las alertas (Lote 6.2, HU-039). Lo usa ``ops.validate62``; se puede correr a mano.

    python project/scripts/alerts_check.py plan
    python project/scripts/alerts_check.py publish-test [--brand postobon --source instagram]
    python project/scripts/alerts_check.py burst [--brand postobon --source instagram --count 150]

* ``plan`` (sin nube): una alerta de ejemplo con su texto y la ola de prueba frente al contrato.
* ``publish-test``: publica en ``ta-alerts`` una alerta sintética (``origin = check``).
* ``burst``: publica la ola de prueba en ``ta-raw-{source}`` (comentarios ``ta-verify@6.2``).

Cada orden imprime una línea JSON por resultado (``event`` = ``alerts_plan``, ``alert_test_published`` o
``burst_published``). Proyecto: ``--project`` o ``GCP_PROJECT``; credenciales: ADC.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from alerts import check
from common import alert_message as am


def _print(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def _publisher():  # pragma: no cover - red
    from google.cloud import pubsub_v1

    return pubsub_v1.PublisherClient()


def cmd_plan(args: argparse.Namespace) -> int:
    from smoke.e2e import envelope_problems
    from streaming.alerts import DEFAULT_COOLDOWN_SECONDS

    sample = check.synthetic_alert(args.brand, args.source)
    data, attrs = am.encode(sample)
    now = datetime.now(timezone.utc)
    burst = check.burst_envelopes("plan", args.brand, args.source, args.count, now)
    contract = [p for p in (envelope_problems(e) for e in burst) if p]
    record = am.log_record(sample, received_ms=am.to_ms(sample["detected_at"]) + 1500, publish_time=None, message_id="plan")
    _print("alerts_plan", sample=sample, sample_problems=am.alert_problems(sample), bytes=len(data), attributes=attrs,
           text=am.alert_text(sample), aviso=record["aviso"], log_severity=record["severity"], log_alert=record["alert"],
           burst={"count": len(burst), "brand": args.brand, "source": args.source, "generator": check.BURST_GENERATOR,
                  "scenario": burst[0]["sim"]["scenario"] if burst else None, "contract_problems": contract[:5],
                  "problems": check.burst_problems(burst, args.brand, args.source),
                  "labels": sorted({e["sim"]["label_sentiment"] or "null" for e in burst})},
           cooldown_seconds=DEFAULT_COOLDOWN_SECONDS, topic=f"projects/{args.project or 'PROYECTO'}/topics/ta-alerts")
    return 0 if not contract and not am.alert_problems(sample) and not check.burst_problems(burst, args.brand, args.source) else 1


def cmd_publish_test(args: argparse.Namespace, publisher_factory: Callable[[], Any] = _publisher) -> int:
    alert = check.synthetic_alert(args.brand, args.source)
    data, attrs = am.encode(alert)
    client = publisher_factory()
    topic = client.topic_path(args.project, "ta-alerts")
    message_id = client.publish(topic, data, **attrs).result(timeout=60)
    _print("alert_test_published", alert_id=alert["alert_id"], message_id=message_id, alert_type=alert["alert_type"],
           brand=alert["brand"], source=alert["source"], window_end=alert["window_end"],
           published_at=am.format_ts(check.utc_now_ms()), text=am.alert_text(alert))
    return 0


def cmd_burst(args: argparse.Namespace, publisher_factory: Callable[[], Any] = _publisher) -> int:
    from smoke.e2e import envelope_problems
    from streaming.inject import attributes_for, dumps

    run_id = args.run_id or uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)
    envelopes = check.burst_envelopes(run_id, args.brand, args.source, args.count, now, sentiment=args.sentiment)
    bad = [p for p in (envelope_problems(e) for e in envelopes) if p] + check.burst_problems(envelopes, args.brand, args.source)
    if bad:
        _print("burst_invalid", problems=bad[:5])
        return 1
    client = publisher_factory()
    topic = client.topic_path(args.project, f"ta-raw-{args.source}")
    first = am.format_ts(check.utc_now_ms())
    futures = [client.publish(topic, dumps(e), **attributes_for(e)) for e in envelopes]
    ids = [f.result(timeout=120) for f in futures]
    _print("burst_published", run_id=run_id, count=len(ids), brand=args.brand, source=args.source,
           sentiment=args.sentiment, generator=check.BURST_GENERATOR, scenario=envelopes[0]["sim"]["scenario"],
           first_event_time=min(e["event_time"] for e in envelopes), last_event_time=max(e["event_time"] for e in envelopes),
           published_from=first, published_to=am.format_ts(check.utc_now_ms()),
           event_ids=[envelopes[0]["event_id"], envelopes[-1]["event_id"]])
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="alerts_check.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "publish-test", "burst"):
        p = sub.add_parser(name)
        p.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
        p.add_argument("--brand", default=check.DEFAULT_BRAND)
        p.add_argument("--source", default=check.DEFAULT_SOURCE, choices=am.SOURCES)
        if name in ("plan", "burst"):
            p.add_argument("--count", type=int, default=check.DEFAULT_COUNT)
        if name == "burst":
            p.add_argument("--sentiment", default="negative", choices=("negative", "positive", "neutral"))
            p.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd != "plan" and not args.project:
        print("error: defina GCP_PROJECT o pase --project", file=sys.stderr)
        return 2
    return {"plan": cmd_plan, "publish-test": cmd_publish_test, "burst": cmd_burst}[args.cmd](args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
