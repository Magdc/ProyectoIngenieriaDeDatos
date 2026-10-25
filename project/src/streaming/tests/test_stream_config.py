import json

import pytest

from streaming.config import ConfigError, resolve
from streaming.schemas import build_validator, load_bundle


def _resolve(**kw):
    base = dict(project="p1", runner="DataflowRunner", sources="reddit,facebook,instagram",
                subscription_template="ta-raw-{source}-df", dlq_topic="ta-dlq", output_root=None,
                raw_window_seconds=300, dedup_window_seconds=900, pubsub_id_label="event_id",
                dlq_table="", bundle={})
    base.update(kw)
    return resolve(**base)


def test_valores_por_defecto_de_los_contratos():
    cfg = _resolve()
    assert cfg.subscriptions == {
        "reddit": "projects/p1/subscriptions/ta-raw-reddit-df",
        "facebook": "projects/p1/subscriptions/ta-raw-facebook-df",
        "instagram": "projects/p1/subscriptions/ta-raw-instagram-df",
    }
    assert cfg.dlq_topic == "projects/p1/topics/ta-dlq"
    assert cfg.output_root == "gs://p1-ta-lake"
    assert cfg.id_label == "event_id"
    assert cfg.dlq_table is None
    assert not cfg.direct


def test_proyecto_desde_gcp_project(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "desde-env")
    assert _resolve(project=None).project == "desde-env"


def test_sin_proyecto_falla(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    with pytest.raises(ConfigError, match="GCP_PROJECT"):
        _resolve(project=None)


@pytest.mark.parametrize("runner", [None, "DirectRunner", "direct", "BundleBasedDirectRunner"])
def test_direct_runner_desactiva_id_label(runner):
    cfg = _resolve(runner=runner, output_root="/tmp/lake")
    assert cfg.direct
    assert cfg.id_label is None


def test_emulador_exige_lake_local(monkeypatch):
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
    with pytest.raises(ConfigError, match="output_root"):
        _resolve(runner="DirectRunner")
    assert _resolve(runner="DirectRunner", output_root="/lake/").output_root == "/lake"


@pytest.mark.parametrize("kw, match", [
    ({"sources": ""}, "vacío"),
    ({"sources": "reddit,twitter"}, "twitter"),
    ({"sources": "reddit,reddit"}, "repetidas"),
    ({"subscription_template": "ta-raw-df"}, "source"),
    ({"raw_window_seconds": 5}, "raw_window"),
    ({"dedup_window_seconds": 10}, "dedup"),
])
def test_opciones_invalidas(kw, match):
    with pytest.raises(ConfigError, match=match):
        _resolve(**kw)


def test_dlq_topic_vacio_no_publica_y_rutas_completas_se_respetan():
    cfg = _resolve(dlq_topic="", subscription_template="projects/otro/subscriptions/{source}-x",
                   sources="reddit")
    assert cfg.dlq_topic is None
    assert cfg.subscriptions == {"reddit": "projects/otro/subscriptions/reddit-x"}


# --------------------------------------------------------------------------- schemas

def test_load_bundle_exige_el_envelope(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_bundle(tmp_path / "no-existe")
    (tmp_path / "otro.schema.json").write_text("{}")
    with pytest.raises(FileNotFoundError, match="envelope"):
        load_bundle(tmp_path)


def test_ref_por_nombre_de_archivo_sin_id(tmp_path):
    pytest.importorskip("jsonschema")
    pytest.importorskip("referencing")
    (tmp_path / "envelope.v1.schema.json").write_text(json.dumps(
        {"type": "object", "properties": {"payload": {"$ref": "payload.x.schema.json"}}}))
    (tmp_path / "payload.x.schema.json").write_text(json.dumps(
        {"type": "object", "required": ["kind"]}))
    validator = build_validator(load_bundle(tmp_path), "envelope.v1.schema.json")
    assert validator.is_valid({"payload": {"kind": "post"}})
    assert not validator.is_valid({"payload": {}})


def test_ref_relativo_al_id(mini_bundle):
    validator = build_validator(mini_bundle, "envelope.v1.schema.json")
    base = {"schema_version": "1.0", "event_id": "x" * 36, "origin": "stream", "source": "reddit",
            "event_time": "2026-10-17T14:03:11.482Z", "text": "t"}
    assert validator.is_valid({**base, "payload": {"kind": "post"}})
    assert not validator.is_valid({**base, "payload": {"kind": "otra"}})


def test_contratos_reales_cargan_y_construyen(contracts_bundle):
    from streaming.validate import Validators

    validators = Validators(contracts_bundle)
    assert validators.envelope is not None
