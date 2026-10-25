"""Topología de Pub/Sub en el emulador local (Lote 1.5, HU-047).

Crea en el emulador los mismos nombres de §5.3: ``ta-raw-{source}``,
``ta-raw-{source}-df`` (para el ``DirectRunner`` del Lote 2.1), ``ta-dlq`` y
``ta-alerts``. Es idempotente.

**Solo** funciona con ``PUBSUB_EMULATOR_HOST`` definido: en GCP la topología la
crea Terraform (§5.15) y este módulo se niega a tocarla.
"""

from __future__ import annotations

import os
from typing import Callable

EXTRA_TOPICS: tuple[str, ...] = ("ta-dlq", "ta-alerts")
DATAFLOW_SUFFIX = "-df"


class NotEmulator(RuntimeError):
    """Se intentó crear la topología fuera del emulador."""


def require_emulator(env: dict[str, str] | None = None) -> str:
    host = (env if env is not None else os.environ).get("PUBSUB_EMULATOR_HOST", "").strip()
    if not host:
        raise NotEmulator(
            "PUBSUB_EMULATOR_HOST no está definido: este script solo crea topics en el emulador local; "
            "en GCP los crea Terraform (§5.15)"
        )
    return host


def ensure_topology(
    project: str,
    sources: list[str],
    publisher,
    subscriber,
    topic_template: str = "ta-raw-{source}",
    extra_topics: tuple[str, ...] = EXTRA_TOPICS,
    log: Callable[[str], None] = print,
) -> dict[str, list[str]]:
    """Crea topics y suscripciones si faltan. Devuelve lo creado y lo que ya existía."""
    from google.api_core import exceptions as gexc

    created: list[str] = []
    existing: list[str] = []

    def topic(name: str) -> str:
        path = publisher.topic_path(project, name)
        try:
            publisher.create_topic(request={"name": path})
            created.append(path)
            log(f"topic creado: {name}")
        except gexc.AlreadyExists:
            existing.append(path)
            log(f"topic ya existía: {name}")
        return path

    for source in sources:
        name = topic_template.format(source=source)
        tpath = topic(name)
        sub = f"{name}{DATAFLOW_SUFFIX}"
        spath = subscriber.subscription_path(project, sub)
        try:
            subscriber.create_subscription(request={"name": spath, "topic": tpath, "ack_deadline_seconds": 60})
            created.append(spath)
            log(f"suscripción creada: {sub}")
        except gexc.AlreadyExists:
            existing.append(spath)
            log(f"suscripción ya existía: {sub}")
    for name in extra_topics:
        topic(name)
    return {"created": created, "existing": existing}


def main(env: dict[str, str] | None = None) -> int:
    env = dict(os.environ if env is None else env)
    try:
        host = require_emulator(env)
    except NotEmulator as exc:
        print(f"error: {exc}")
        return 2
    project = env.get("GCP_PROJECT", "ta-local")
    sources = [s.strip() for s in env.get("SIM_SOURCES", "reddit,facebook,instagram").split(",") if s.strip()]
    template = env.get("SIM_TOPIC_TEMPLATE", "ta-raw-{source}")

    from google.cloud import pubsub_v1

    print(f"emulador {host} · proyecto {project}")
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    try:
        result = ensure_topology(project, sources, publisher, subscriber, template)
    finally:
        subscriber.close()
    print(f"listo: {len(result['created'])} creados, {len(result['existing'])} ya existían")
    return 0
