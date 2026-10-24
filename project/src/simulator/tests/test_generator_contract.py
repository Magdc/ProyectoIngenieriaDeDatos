"""Salida del generador validada contra contracts/ (ARCHITECTURE.md §5.12: >= 1.000 eventos por
fuente, caos apagado). Se omite si faltan jsonschema o los schemas."""
from datetime import datetime, timedelta, timezone

from common.envelope import build_envelope, event_rng
from simulator.generator import GENERATOR_ID, generate, load_brands

T0 = datetime(2026, 10, 17, 14, 3, 11, 482000, tzinfo=timezone.utc)
BRANDS = load_brands()


def test_mil_eventos_por_fuente_cumplen_el_schema(envelope_validator, payloads):
    for source in payloads:
        for seq in range(1000):
            c = generate(event_rng(7, source, seq))
            e = build_envelope(
                origin="stream", source=source, seed=7, seq=seq, event_time=T0,
                ingest_time=T0 + timedelta(seconds=1), user_id="u_000001", handle="u/x",
                followers=10, text=c.texto, payload=payloads[source],
                brands=BRANDS.detect(c.texto), generator=GENERATOR_ID,
                label_sentiment=c.sentimiento,
            )
            assert envelope_validator(e) == [], (source, seq, c.texto)
