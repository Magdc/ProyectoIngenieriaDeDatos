from datetime import datetime, timedelta, timezone

import pytest

from exporter.watermark import (
    WATERMARK_SCHEMA,
    Watermark,
    WatermarkError,
    candidate_threshold,
    listing_since,
    next_cursor,
)

T = datetime(2026, 10, 24, 12, 0, 0, 123456, tzinfo=timezone.utc)


def test_ida_y_vuelta_en_milisegundos_utc():
    wm = Watermark(cursor=T, updated_at=T, run_id="r1", exporter="gcs-to-s3@1.0", last_run={"uploaded": 3})
    data = wm.to_json()
    assert data["schema"] == WATERMARK_SCHEMA
    assert data["cursor_time_created"] == "2026-10-24T12:00:00.123Z"
    back = Watermark.from_json(data)
    assert back.cursor == T.replace(microsecond=123000)
    assert back.last_run == {"uploaded": 3}


def test_vacio_y_schema_desconocido():
    assert Watermark.from_json(None).cursor is None
    with pytest.raises(WatermarkError):
        Watermark.from_json({"schema": "otro", "cursor_time_created": None})
    with pytest.raises(WatermarkError):
        Watermark.from_json({"schema": WATERMARK_SCHEMA, "cursor_time_created": "ayer"})


def test_umbral_y_listado_con_solape_y_margen():
    wm = Watermark(cursor=T)
    assert candidate_threshold(wm, 3600) == T - timedelta(hours=1)
    assert listing_since(wm, 3600, 7200) == T - timedelta(hours=3)
    assert candidate_threshold(Watermark.empty(), 3600) is None
    assert listing_since(Watermark.empty(), 3600, 7200) is None


def test_cursor_sin_pendientes_es_el_maximo():
    times = [T - timedelta(minutes=10), T, T - timedelta(minutes=1)]
    assert next_cursor(None, times, []) == T.replace(microsecond=123000)


def test_cursor_con_pendientes_queda_antes_del_mas_antiguo():
    pending = [T + timedelta(minutes=5), T + timedelta(minutes=2)]
    cur = next_cursor(None, [T, *pending], pending)
    assert cur < T + timedelta(minutes=2)
    assert cur == (T + timedelta(minutes=2)).replace(microsecond=123000) - timedelta(milliseconds=1)


def test_cursor_nunca_retrocede():
    previous = T + timedelta(hours=1)
    assert next_cursor(previous, [T], []) == previous
    assert next_cursor(previous, [], []) == previous
    assert next_cursor(None, [], []) is None
