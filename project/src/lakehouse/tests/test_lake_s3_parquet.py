"""Filas de Parquet en S3 leyendo solo el pie (Lote 4.1), con Parquet reales de pyarrow y un S3 falso."""

from __future__ import annotations

import io

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from lakehouse import s3_parquet as sp  # noqa: E402


def parquet_bytes(rows: int, text: str = "x") -> bytes:
    table = pa.table({"event_id": [f"id-{i}" for i in range(rows)], "text": [text * 50] * rows})
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.ranges = []

    def get_paginator(self, name):
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                yield {"Contents": [{"Key": k, "Size": len(fake.objects[k])} for k in keys]}

        return Paginator()

    def get_object(self, Bucket, Key, Range=None):  # noqa: N803
        data = self.objects[Key]
        self.ranges.append(Range)
        if Range:
            n = int(Range.split("=-")[1])
            data = data[-n:]
        return {"Body": io.BytesIO(data)}


def test_parse_raw_key():
    key = "raw/origin=stream/source=reddit/dt=2026-10-22/hr=07/part-20261022T070000Z-p000.parquet"
    assert sp.parse_raw_key(key) == {"origin": "stream", "source": "reddit", "dt": "2026-10-22", "hr": "07"}
    assert sp.parse_raw_key("raw/origin=stream/source=reddit/x.parquet") is None


def test_filas_desde_el_pie_completo_y_parcial():
    data = parquet_bytes(37)
    assert sp.rows_from_tail(data) == 37
    tail = data[-(sp.footer_length(data) + 8):]
    assert len(tail) < len(data) and sp.rows_from_tail(tail) == 37
    with pytest.raises(ValueError):
        sp.rows_from_tail(tail[1:])
    with pytest.raises(ValueError):
        sp.footer_length(b"no es parquet")


def test_si_el_pie_no_cabe_baja_el_objeto_entero():
    data = parquet_bytes(10)
    s3 = FakeS3({"k.parquet": data})
    assert sp.object_rows(s3, "b", "k.parquet", len(data), tail_bytes=16) == 10
    assert s3.ranges == ["bytes=-16", None]


def test_cuenta_por_fuente_e_informa_errores():
    objects = {
        "raw/origin=stream/source=reddit/dt=2026-10-22/hr=07/a.parquet": parquet_bytes(5),
        "raw/origin=stream/source=reddit/dt=2026-10-22/hr=08/b.parquet": parquet_bytes(7),
        "raw/origin=stream/source=facebook/dt=2026-10-22/hr=07/c.parquet": parquet_bytes(3),
        "raw/origin=stream/source=facebook/dt=2026-10-22/hr=07/roto.parquet": b"PAR1nada",
        "raw/origin=stream/source=facebook/dt=2026-10-22/hr=07/_SUCCESS": b"",
        "raw/origin=historical/source=reddit/dt=2026-05-01/hr=00/h.parquet": parquet_bytes(100),
    }
    rep = sp.count_parquet_rows(FakeS3(objects), "b", "raw/origin=stream/", workers=4)
    assert rep["objects"] == 4 and rep["rows"] == 15 and rep["error_count"] == 1
    assert rep["by_source"] == {"facebook": {"objects": 2, "rows": 3}, "reddit": {"objects": 2, "rows": 12}}
    assert "roto.parquet" in rep["errors"][0]["key"]
