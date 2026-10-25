from datetime import datetime, timezone

from streaming.records import (
    DlqRecord,
    dlq_partition,
    format_ts,
    raw_partition,
    truncate_utf8,
)

TS = datetime(2026, 10, 17, 14, 3, 11, 482000, tzinfo=timezone.utc).timestamp()


def test_format_ts_iso_milisegundos_utc():
    assert format_ts(TS) == "2026-10-17T14:03:11.482Z"
    assert format_ts(0) == "1970-01-01T00:00:00.000Z"


def test_particiones_por_fecha_y_hora_de_proceso():
    assert raw_partition("reddit", TS) == "raw/source=reddit/dt=2026-10-17/hr=14"
    assert dlq_partition(TS) == "dlq/dt=2026-10-17"


def test_particion_usa_utc_y_no_la_hora_local():
    # 23:30 en Bogotá (UTC-5) es el día siguiente en UTC.
    ts = datetime(2026, 10, 18, 4, 30, tzinfo=timezone.utc).timestamp()
    assert raw_partition("facebook", ts) == "raw/source=facebook/dt=2026-10-18/hr=04"


def test_truncate_utf8_no_parte_caracteres():
    text = "ñ" * 10  # 20 bytes
    cut = truncate_utf8(text, 5)
    assert cut == "ññ"
    assert truncate_utf8("hola", 100) == "hola"


def _record(**kw):
    base = dict(received_at="2026-10-17T14:03:11.482Z", source="reddit", error_type="json",
                error_detail="detalle", raw_message="{", process_ts=TS, data=b"{",
                attributes={"event_id": "x", "source": "reddit"})
    base.update(kw)
    return DlqRecord(**base)


def test_fila_dlq_tiene_exactamente_las_columnas_de_dlq_events():
    assert list(_record().row()) == ["received_at", "source", "error_type", "error_detail",
                                     "raw_message"]


def test_linea_dlq_es_json_de_una_linea():
    import json

    line = _record(raw_message="a\nb").line()
    assert "\n" not in line
    assert json.loads(line)["raw_message"] == "a\nb"


def test_atributos_dlq_conservan_originales_y_suman_los_dlq():
    attrs = _record().pubsub_attributes()
    assert attrs["event_id"] == "x" and attrs["source"] == "reddit"
    assert attrs["dlq_error_type"] == "json"
    assert attrs["dlq_source"] == "reddit"
    assert attrs["dlq_received_at"] == "2026-10-17T14:03:11.482Z"


def test_atributos_dlq_respetan_limites_de_pubsub():
    many = {f"k{i:03d}": "v" for i in range(150)}
    many["dlq_error_type"] = "falso"  # no puede pisar el valor real
    rec = _record(error_detail="é" * 2000, attributes=many)
    attrs = rec.pubsub_attributes()
    assert len(attrs) <= 100
    assert attrs["dlq_error_type"] == "json"
    assert all(len(v.encode("utf-8")) <= 1024 for v in attrs.values())
