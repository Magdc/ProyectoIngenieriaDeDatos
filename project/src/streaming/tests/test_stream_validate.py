import json

import pytest

from streaming.inject import attributes_for, dumps
from streaming.records import DlqRecord, ValidEvent

NOW = 1_800_000_000.0


def check(validators, source, data, attrs):
    return validators.check(source, data, attrs, now=NOW)


def test_evento_valido_pasa_con_su_cuerpo_intacto(mini_validators, sample_envelope):
    data = dumps(sample_envelope)
    result = check(mini_validators, "reddit", data, attributes_for(sample_envelope))
    assert isinstance(result, ValidEvent)
    assert result.event_id == sample_envelope["event_id"]
    assert result.source == "reddit"
    assert result.body == data.decode("utf-8")
    assert result.process_ts == NOW


def test_cuerpo_con_saltos_de_linea_se_guarda_en_una_linea(mini_validators, sample_envelope):
    data = json.dumps(sample_envelope, indent=2, ensure_ascii=False).encode("utf-8")
    result = check(mini_validators, "reddit", data, attributes_for(sample_envelope))
    assert isinstance(result, ValidEvent)
    assert "\n" not in result.body
    assert json.loads(result.body) == sample_envelope


@pytest.mark.parametrize("data, detail", [
    (b"\xff\xfe", "UTF-8"),
])
def test_encoding(mini_validators, data, detail):
    result = check(mini_validators, "reddit", data, {})
    assert isinstance(result, DlqRecord)
    assert result.error_type == "encoding"
    assert detail in result.error_detail
    assert result.raw_message == "\\xff\\xfe"
    assert result.data == data


@pytest.mark.parametrize("data", [b'{"a": ', b"", b"[1, 2]", b'"texto"', b"42"])
def test_json_invalido_o_no_objeto(mini_validators, data):
    result = check(mini_validators, "reddit", data, {})
    assert isinstance(result, DlqRecord)
    assert result.error_type == "json"


def test_schema_falta_campo(mini_validators, sample_envelope):
    sample_envelope.pop("event_time")
    result = check(mini_validators, "reddit", dumps(sample_envelope), attributes_for(
        {**sample_envelope, "event_time": "x"}))
    assert result.error_type == "schema"
    assert "event_time" in result.error_detail


def test_schema_error_en_payload_referenciado(mini_validators, sample_envelope):
    sample_envelope["payload"]["kind"] = "story"
    result = check(mini_validators, "reddit", dumps(sample_envelope), attributes_for(sample_envelope))
    assert result.error_type == "schema"
    assert result.error_detail.startswith("payload/kind")


def test_atributo_que_no_coincide(mini_validators, sample_envelope):
    attrs = {**attributes_for(sample_envelope), "source": "facebook"}
    result = check(mini_validators, "reddit", dumps(sample_envelope), attrs)
    assert result.error_type == "attributes"
    assert "source" in result.error_detail
    assert result.attributes == attrs


def test_atributo_ausente(mini_validators, sample_envelope):
    attrs = attributes_for(sample_envelope)
    attrs.pop("origin")
    result = check(mini_validators, "reddit", dumps(sample_envelope), attrs)
    assert result.error_type == "attributes"
    assert "origin" in result.error_detail


def test_sin_schema_de_atributos_igual_exige_coherencia(mini_bundle, sample_envelope):
    from streaming.validate import Validators

    mini_bundle.pop("pubsub.attributes.schema.json")
    validators = Validators(mini_bundle)
    assert validators.attributes is None
    attrs = {**attributes_for(sample_envelope), "event_id": "otro"}
    result = check(validators, "reddit", dumps(sample_envelope), attrs)
    assert result.error_type == "attributes"


def test_evento_de_otra_fuente_en_la_suscripcion(mini_validators, sample_envelope):
    result = check(mini_validators, "facebook", dumps(sample_envelope), attributes_for(sample_envelope))
    assert result.error_type == "routing"
    assert result.source == "facebook"


def test_dlq_conserva_mensaje_original(mini_validators):
    result = check(mini_validators, "instagram", b'{"roto"', {"event_id": "e1"})
    assert result.raw_message == '{"roto"'
    assert result.received_at == "2027-01-15T08:00:00.000Z"
    assert result.row()["source"] == "instagram"


def test_detalle_largo_se_trunca(mini_validators):
    result = check(mini_validators, "reddit", b"{" + b'"a":1,' * 2000, {})
    assert len(result.error_detail) <= 2000


def test_json_cortado_a_mitad_de_un_caracter_es_encoding(mini_validators):
    """Un corte por bytes puede partir una tilde: el mensaje cae en la DLQ como `encoding`."""
    data = '{"text": "Postobón"}'.encode("utf-8")
    cut = data[: data.index("ó".encode()) + 1]
    assert check(mini_validators, "reddit", cut, {}).error_type == "encoding"
