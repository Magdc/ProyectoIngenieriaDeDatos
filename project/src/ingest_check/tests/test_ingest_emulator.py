"""Topología del emulador (Lote 1.5) con clientes falsos."""

from __future__ import annotations

import pytest

pytest.importorskip("google.api_core")

from google.api_core import exceptions as gexc  # noqa: E402

from ingest_check import emulator  # noqa: E402


class FakePublisher:
    def __init__(self, existing=()):
        self.topics = set(existing)

    @staticmethod
    def topic_path(project, name):
        return f"projects/{project}/topics/{name}"

    def create_topic(self, request):
        if request["name"] in self.topics:
            raise gexc.AlreadyExists("ya")
        self.topics.add(request["name"])


class FakeSubscriber:
    def __init__(self):
        self.subs: dict[str, dict] = {}

    @staticmethod
    def subscription_path(project, name):
        return f"projects/{project}/subscriptions/{name}"

    def create_subscription(self, request):
        if request["name"] in self.subs:
            raise gexc.AlreadyExists("ya")
        self.subs[request["name"]] = request


SOURCES = ["reddit", "facebook", "instagram"]


def test_creates_the_names_of_section_5_3():
    pub, sub = FakePublisher(), FakeSubscriber()
    result = emulator.ensure_topology("ta-local", SOURCES, pub, sub, log=lambda m: None)
    assert pub.topics == {f"projects/ta-local/topics/{n}" for n in
                          ["ta-raw-reddit", "ta-raw-facebook", "ta-raw-instagram", "ta-dlq", "ta-alerts"]}
    assert set(sub.subs) == {f"projects/ta-local/subscriptions/ta-raw-{s}-df" for s in SOURCES}
    req = sub.subs["projects/ta-local/subscriptions/ta-raw-reddit-df"]
    assert req["topic"] == "projects/ta-local/topics/ta-raw-reddit" and req["ack_deadline_seconds"] == 60
    assert len(result["created"]) == 8 and result["existing"] == []


def test_is_idempotent():
    pub, sub = FakePublisher(), FakeSubscriber()
    emulator.ensure_topology("ta-local", SOURCES, pub, sub, log=lambda m: None)
    again = emulator.ensure_topology("ta-local", SOURCES, pub, sub, log=lambda m: None)
    assert again["created"] == [] and len(again["existing"]) == 8


def test_refuses_without_emulator(capsys):
    with pytest.raises(emulator.NotEmulator):
        emulator.require_emulator({})
    assert emulator.main({"GCP_PROJECT": "project-0568d9ba-848f-413c-88b"}) == 2
    assert "PUBSUB_EMULATOR_HOST" in capsys.readouterr().out


def test_require_emulator_returns_host():
    assert emulator.require_emulator({"PUBSUB_EMULATOR_HOST": "pubsub-emulator:8085"}) == "pubsub-emulator:8085"
