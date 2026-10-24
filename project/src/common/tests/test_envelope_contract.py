"""El envelope que arma ``build_envelope`` debe pasar el schema de contracts/ (§5.13).

Se omite si faltan jsonschema/referencing o los schemas (ver conftest.py)."""
from datetime import datetime, timedelta, timezone

from common.envelope import build_envelope

T0 = datetime(2026, 10, 17, 14, 3, 11, 482000, tzinfo=timezone.utc)


def _envelope(payloads, source, text, brands):
    return build_envelope(
        origin="stream", source=source, seed=7, seq=1, event_time=T0,
        ingest_time=T0 + timedelta(milliseconds=1538), user_id="u_000481", handle="u/mariag_89",
        followers=412, text=text, payload=payloads[source], brands=brands,
        generator="comment-gen@1.0", label_sentiment="positive",
    )


def test_envelope_valido_por_fuente(envelope_validator, payloads):
    for source in payloads:
        e = _envelope(payloads, source, "Soy fan d Postobón y no pienso cambiar. #Postobón", ["postobon"])
        assert envelope_validator(e) == [], source


def test_el_schema_rechaza_un_envelope_roto(envelope_validator, payloads):
    e = _envelope(payloads, "reddit", "Hola #Postobón", ["postobon"])
    e["event_time"] = "2026-10-17T14:03:11Z"        # sin milisegundos
    assert envelope_validator(e) != []
