"""Suscripción a ``ta-raw-{source}`` para el verificador (Lote 1.4).

Por defecto crea una **suscripción temporal por topic**
(``ta-verify-{source}-{run_id}``) y la borra al terminar. No usa las
suscripciones ``ta-raw-{source}-df``: son de Dataflow (§5.3) y consumirlas le
quitaría mensajes. Una suscripción nueva solo recibe lo publicado después de
crearla, así que la tasa medida no arrastra atraso.

Las temporales llevan expiración por inactividad de 1 día: si el proceso muere
sin borrarlas, Pub/Sub las elimina solo. Con el emulador (``PUBSUB_EMULATOR_HOST``)
se crean sin expiración ni etiquetas.
"""

from __future__ import annotations

import os
import secrets
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Mapping

OnMessage = Callable[[str, bytes, Mapping[str, str], str, float, "float | None"], object]

SUBSCRIPTION_PREFIX = "ta-verify"


def make_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%d%H%M%S}-{secrets.token_hex(2)}"


def temp_subscription_name(source: str, run_id: str) -> str:
    return f"{SUBSCRIPTION_PREFIX}-{source}-{run_id}"


class PubSubBackend:
    """Crea (o reutiliza) suscripciones, recibe en *streaming pull* y limpia."""

    def __init__(
        self,
        project: str,
        topic_template: str,
        sources: list[str],
        run_id: str,
        existing: Mapping[str, str] | None = None,
        keep: bool = False,
        labels: Mapping[str, str] | None = None,
        log: Callable[[str], None] | None = None,
    ) -> None:
        from google.cloud import pubsub_v1  # import diferido

        self._pubsub_v1 = pubsub_v1
        self.project = project
        self.topic_template = topic_template
        self.sources = sources
        self.run_id = run_id
        self.existing = dict(existing or {})
        self.keep = keep
        self.labels = dict(labels or {})
        self.log = log or (lambda m: print(m, file=sys.stderr))
        self.emulator = bool(os.environ.get("PUBSUB_EMULATOR_HOST"))
        self._subscriber = pubsub_v1.SubscriberClient()
        self._created: list[str] = []
        self.subscriptions: dict[str, str] = {}
        self._futures: list[object] = []

    # -- ciclo de vida ------------------------------------------------------

    def setup(self) -> dict[str, str]:
        from google.api_core import exceptions as gexc

        for source in self.sources:
            if source in self.existing:
                path = self._subscriber.subscription_path(self.project, self.existing[source])
                self.subscriptions[source] = path
                self.log(f"[{source}] usando la suscripción existente {path}")
                continue
            topic = self.topic_template.format(source=source)
            topic_path = self._subscriber.topic_path(self.project, topic)
            name = temp_subscription_name(source, self.run_id)
            path = self._subscriber.subscription_path(self.project, name)
            request: dict = {"name": path, "topic": topic_path, "ack_deadline_seconds": 60}
            if not self.emulator:
                request["expiration_policy"] = {"ttl": {"seconds": 86400}}
                request["message_retention_duration"] = {"seconds": 600}
                request["labels"] = self.labels
            try:
                self._subscriber.create_subscription(request=request)
            except gexc.NotFound as exc:
                raise RuntimeError(f"no existe el topic {topic_path}") from exc
            self._created.append(path)
            self.subscriptions[source] = path
            self.log(f"[{source}] suscripción temporal {name} → {topic}")
        return dict(self.subscriptions)

    def start(self, on_message: OnMessage) -> None:
        flow = self._pubsub_v1.types.FlowControl(max_messages=1000)
        for source, path in self.subscriptions.items():
            self._futures.append(
                self._subscriber.subscribe(path, callback=self._callback(source, on_message), flow_control=flow)
            )

    def stop(self) -> None:
        for fut in self._futures:
            fut.cancel()
        for fut in self._futures:
            try:
                fut.result(timeout=10)
            except Exception:  # cancelado: es lo esperado
                pass
        self._futures.clear()

    def teardown(self) -> None:
        if self.keep:
            for path in self._created:
                self.log(f"se conserva {path} (expira sola tras 1 día sin uso)")
        else:
            for path in self._created:
                try:
                    self._subscriber.delete_subscription(request={"subscription": path})
                except Exception as exc:  # no tapar el resultado por un fallo al limpiar
                    self.log(f"no se pudo borrar {path}: {exc}")
        self._created.clear()
        self._subscriber.close()

    # -- interno ------------------------------------------------------------

    @staticmethod
    def _callback(source: str, on_message: OnMessage):
        def cb(message) -> None:
            received = time.time()
            published = None
            pt = getattr(message, "publish_time", None)
            if pt is not None:
                try:
                    published = pt.timestamp()
                except Exception:
                    published = None
            try:
                on_message(source, message.data, dict(message.attributes), message.message_id, received, published)
            finally:
                message.ack()

        return cb
