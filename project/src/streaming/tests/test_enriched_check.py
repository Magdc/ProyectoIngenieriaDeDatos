"""Verificador de enriched/ y posts_enriched (Lote 2.2). Sin red."""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta, timezone

from common.enriched_row import envelope_to_row
from common.parquet_rows import rows_to_parquet_bytes
from streaming.enriched_check import BQ_COLUMNS_SQL, GcsLake, file_window_start, hour_prefixes
from streaming.enriched_cli import run
from streaming.enriched_testing import TEST_KEY, make_envelope, ts, write_local_lake

NOW = datetime(2026, 10, 21, 14, 40, 0, tzinfo=timezone.utc)


def _envs():
    return [make_envelope(s, i, user_id=f"u_{i % 4:06d}", event_time=NOW - timedelta(minutes=31)) for i, s in enumerate(["reddit", "facebook", "instagram"] * 10)]


def _run(argv, **kw):
    out = io.StringIO()
    code = run(argv, env={}, now=lambda: NOW, out=out, **kw)
    return code, out.getvalue()


def test_lake_correcto_pasa(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    evidence = tmp_path / "ev.json"
    code, text = _run(["report", "--lake-root", str(tmp_path), "--json-out", str(evidence)])
    assert code == 0, text
    data = json.loads(evidence.read_text(encoding="utf-8"))
    assert data["verdict"] in ("ok", "warn")
    assert data["stats"]["reddit"]["rows"] == 10
    assert any(c["name"] == "user_hash determinista (1 a 1)" and c["status"] == "ok" for c in data["checks"])


def _rewrite_enriched(root, source, mutate):
    for dirpath, _d, names in os.walk(os.path.join(root, "enriched", f"source={source}")):
        for name in names:
            path = os.path.join(dirpath, name)
            from common.parquet_rows import parquet_bytes_to_rows

            rows = parquet_bytes_to_rows(open(path, "rb").read())
            rows = mutate(rows)
            if rows and isinstance(rows[0], dict) and "handle" in rows[0]:
                import pyarrow as pa
                import pyarrow.parquet as pq

                buf = io.BytesIO()
                pq.write_table(pa.Table.from_pylist(rows), buf)
                open(path, "wb").write(buf.getvalue())
            else:
                open(path, "wb").write(rows_to_parquet_bytes(rows))


def test_copias_de_event_id_fallan(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    _rewrite_enriched(str(tmp_path), "facebook", lambda rows: rows + rows[:2])
    code, text = _run(["report", "--lake-root", str(tmp_path)])
    assert code == 1 and "copias de event_id" in text


def test_hash_inconsistente_falla(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)

    def mutate(rows):
        rows[0]["user_hash"] = "f" * 64
        return rows

    _rewrite_enriched(str(tmp_path), "reddit", mutate)
    code, text = _run(["report", "--lake-root", str(tmp_path)])
    assert code == 1 and "más de un hash" in text


def test_columna_de_identidad_falla(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    _rewrite_enriched(str(tmp_path), "instagram", lambda rows: [dict(r, handle="@x") for r in rows])
    code, text = _run(["report", "--lake-root", str(tmp_path)])
    assert code == 1
    assert "sin identidad en claro" in text


def test_cobertura_baja_falla(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    _rewrite_enriched(str(tmp_path), "reddit", lambda rows: rows[:5])
    code, text = _run(["report", "--lake-root", str(tmp_path)])
    assert code == 1 and "cobertura raw → enriched" in text


def test_sin_datos_falla_y_sin_lake_es_error(tmp_path):
    os.makedirs(tmp_path / "vacio")
    code, _ = _run(["report", "--lake-root", str(tmp_path / "vacio")])
    assert code == 1
    code, _ = _run(["report", "--lake-root", str(tmp_path / "no-existe")])
    assert code == 2


def test_bigquery_con_consulta_falsa(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    ids = {}
    for e in _envs():
        ids.setdefault(e["source"], []).append(e["event_id"])

    def query(sql, params):
        if "INFORMATION_SCHEMA" in sql:
            from common.enriched_row import COLUMN_NAMES

            return [{"column_name": c} for c in COLUMN_NAMES]
        if "COUNT(DISTINCT" in sql:
            return [{"source": s, "n": len(v), "ids": len(v), "bad_hash": 0, "bad_shares": 0, "missing_shares": 0, "null_likes": 0, "bad_brand_primary": 0} for s, v in ids.items()]
        return [{"source": s, "event_id": i} for s, v in ids.items() for i in v]

    code, text = _run(["report", "--lake-root", str(tmp_path), "--bigquery", "yes", "--table", "p.ta_stream.posts_enriched"], bq_query=query)
    assert code == 0, text
    assert "cobertura enriched → BigQuery" in text

    def query_dup(sql, params):
        rows = query(sql, params)
        if "COUNT(DISTINCT" in sql:
            rows[0] = dict(rows[0], n=rows[0]["n"] * 2)
        return rows

    code, text = _run(["report", "--lake-root", str(tmp_path), "--bigquery", "yes", "--table", "p.ta_stream.posts_enriched"], bq_query=query_dup)
    assert code == 1


def test_bigquery_tabla_inexistente_es_error_de_infraestructura(tmp_path):
    write_local_lake(str(tmp_path), _envs(), now=NOW)
    code, text = _run(["report", "--lake-root", str(tmp_path), "--bigquery", "yes", "--table", "p.d.t"], bq_query=lambda s, p: [])
    assert code == 2 and "no existe" in text
    assert "INFORMATION_SCHEMA" in BQ_COLUMNS_SQL


class _Blob:
    def __init__(self, name, data):
        self.name, self._data = name, data

    def download_as_bytes(self):
        return self._data


class _Bucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name):
        return _Blob(name, self.store[name])


class _GcsClient:
    def __init__(self, store):
        self.store = store

    def list_blobs(self, bucket, prefix=""):
        return [_Blob(n, d) for n, d in sorted(self.store.items()) if n.startswith(prefix)]

    def bucket(self, name):
        return _Bucket(self.store)


def test_lake_en_gcs_con_cliente_falso():
    rows = [envelope_to_row(make_envelope("reddit", i), TEST_KEY, ts(NOW - timedelta(minutes=10))) for i in range(3)]
    key = "enriched/source=reddit/dt=2026-10-21/hr=14/part-20261021T143000Z-p000.parquet"
    client = _GcsClient({key: rows_to_parquet_bytes(rows), "enriched/source=reddit/dt=2026-10-21/hr=14/.tmp": b""})
    lake = GcsLake("gs://b-ta-lake", client)
    assert lake.list("enriched/source=reddit/") == [key]
    code, text = _run(["report", "--lake-root", "gs://b-ta-lake", "--bigquery", "no", "--sources", "reddit"], lake_client=client)
    assert "filas recientes" in text and "3 filas" in text
    assert code == 0  # sin raw/: cobertura y cruce quedan en aviso


def test_utilidades_de_rutas():
    assert file_window_start("x/part-20261021T143000Z-p000.parquet") == datetime(2026, 10, 21, 14, 30, tzinfo=timezone.utc)
    assert file_window_start("x/otro.txt") is None
    pref = hour_prefixes("raw", "reddit", NOW - timedelta(hours=1), NOW)
    assert pref == ["raw/source=reddit/dt=2026-10-21/hr=13/", "raw/source=reddit/dt=2026-10-21/hr=14/"]
