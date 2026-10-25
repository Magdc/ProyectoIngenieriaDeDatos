"""``PubSubBackend`` con un ``SubscriberClient`` falso: nombres, peticiones, ack y limpieza."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pubsub_v1 = pytest.importorskip("google.cloud.pubsub_v1")

from google.api_core import exceptions as gexc  # noqa: E402

from ingest_check import pubsub_io  # noqa: E402
from ingest_check.pubsub_io import PubSubBackend, make_run_id, temp_subscription_name  # noqa: E402


class FakeFuture:
    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    def result(self, timeout=None):
        raise RuntimeError("cancelado")


class FakeSubscriber:
    instances: list["FakeSubscriber"] = []

    def __init__(self, *a, **kw):
        self.created: list[dict] = []
        self.deleted: list[str] = []
        self.subscribed: dict[str, object] = {}
        self.closed = False
        self.missing_topics: set[str] = set()
        FakeSubscriber.instances.append(self)

    @staticmethod
    def topic_path(project, topic):
        return f"projects/{project}/topics/{topic}"

    @staticmethod
    def subscription_path(project, sub):
        return f"projects/{project}/subscriptions/{sub}"

    def create_subscription(self, request):
        if request["topic"] in self.missing_topics:
            raise gexc.NotFound("no")
        self.created.append(request)

    def delete_subscription(self, request):
        self.deleted.append(request["subscription"])

    def subscribe(self, path, callback, flow_control=None):
        self.subscribed[path] = callback
        return FakeFuture()

    def close(self):
        self.closed = True


class FakeMsg:
    def __init__(self, data=b"{}", attributes=None, message_id="m1"):
        self.data = data
        self.attributes = attributes or {}
        self.message_id = message_id
        self.publish_time = datetime(2026, 10, 20, 23, 0, 0, tzinfo=timezone.utc)
        self.acked = False

    def ack(self):
        self.acked = True


@pytest.fixture
def fake_client(monkeypatch):
    FakeSubscriber.instances.clear()
    monkeypatch.setattr(pubsub_v1, "SubscriberClient", FakeSubscriber)
    monkeypatch.delenv("PUBSUB_EMULATOR_HOST", raising=False)
    return FakeSubscriber


def _backend(**kw):
    return PubSubBackend(project="p", topic_template="ta-raw-{source}", sources=["reddit", "facebook"],
                         run_id="20261020230000-abcd", log=lambda m: None, **kw)


def test_names():
    assert temp_subscription_name("reddit", "r1") == "ta-verify-reddit-r1"
    rid = make_run_id(datetime(2026, 10, 20, 23, 0, 0, tzinfo=timezone.utc))
    assert rid.startswith("20261020230000-") and len(rid) == 19


def test_creates_temp_subscriptions_and_deletes_them(fake_client):
    b = _backend(labels={"project": "trend-analyzer"})
    subs = b.setup()
    client = fake_client.instances[0]
    assert subs["reddit"] == "projects/p/subscriptions/ta-verify-reddit-20261020230000-abcd"
    req = client.created[0]
    assert req["topic"] == "projects/p/topics/ta-raw-reddit"
    assert req["ack_deadline_seconds"] == 60
    assert req["expiration_policy"] == {"ttl": {"seconds": 86400}}
    assert req["labels"] == {"project": "trend-analyzer"}
    b.teardown()
    assert sorted(client.deleted) == sorted(subs.values()) and client.closed


def test_never_creates_on_df_subscriptions_and_reuses_existing(fake_client):
    b = _backend(existing={"facebook": "mi-sub"})
    subs = b.setup()
    client = fake_client.instances[0]
    assert subs["facebook"] == "projects/p/subscriptions/mi-sub"
    assert [r["topic"] for r in client.created] == ["projects/p/topics/ta-raw-reddit"]
    b.teardown()
    assert "projects/p/subscriptions/mi-sub" not in client.deleted  # la existente no se borra


def test_keep_does_not_delete(fake_client):
    b = _backend(keep=True)
    b.setup()
    b.teardown()
    assert fake_client.instances[0].deleted == []


def test_emulator_creates_minimal_subscription(fake_client, monkeypatch):
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
    b = _backend()
    b.setup()
    req = fake_client.instances[0].created[0]
    assert "expiration_policy" not in req and "labels" not in req


def test_missing_topic_is_a_clear_error(fake_client):
    b = _backend()
    fake_client.instances[0].missing_topics.add("projects/p/topics/ta-raw-facebook")
    with pytest.raises(RuntimeError, match="no existe el topic"):
        b.setup()
    b.teardown()  # borra la de reddit, que sí alcanzó a crearse
    assert fake_client.instances[0].deleted == ["projects/p/subscriptions/ta-verify-reddit-20261020230000-abcd"]


def test_callback_passes_publish_time_and_always_acks(fake_client):
    got = []
    b = _backend()
    b.setup()
    b.start(lambda *args: got.append(args))
    client = fake_client.instances[0]
    cb = client.subscribed["projects/p/subscriptions/ta-verify-reddit-20261020230000-abcd"]
    m = FakeMsg(b'{"a": 1}', {"event_id": "e"}, "m9")
    cb(m)
    assert m.acked
    source, data, attrs, mid, received, published = got[0]
    assert (source, data, attrs, mid) == ("reddit", b'{"a": 1}', {"event_id": "e"}, "m9")
    assert published == datetime(2026, 10, 20, 23, 0, 0, tzinfo=timezone.utc).timestamp()

    b2 = _backend()
    b2.setup()
    b2.start(lambda *a: (_ for _ in ()).throw(ValueError("x")))
    cb2 = fake_client.instances[1].subscribed["projects/p/subscriptions/ta-verify-facebook-20261020230000-abcd"]
    m2 = FakeMsg()
    with pytest.raises(ValueError):
        cb2(m2)
    assert m2.acked  # aun si falla el procesamiento, no se queda reentregando

    b.stop()
    assert b._futures == []
    assert pubsub_io.SUBSCRIPTION_PREFIX == "ta-verify"
