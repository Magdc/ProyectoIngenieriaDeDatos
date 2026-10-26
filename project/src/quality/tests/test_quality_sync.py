"""Caché local de S3 (``quality.s3sync``) con un S3 falso: sin red ni credenciales."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from quality import s3sync

REPO = Path(__file__).resolve().parents[4]


class FakeS3:
    def __init__(self, objects: dict[str, bytes], fail: set[str] | None = None):
        self.objects = objects
        self.fail = fail or set()
        self.downloads: list[str] = []

    def list_objects(self, bucket, prefix):
        return [{"key": k, "size": len(v), "etag": "x"} for k, v in sorted(self.objects.items()) if k.startswith(prefix)]

    def download(self, bucket, key, dest: Path):
        self.downloads.append(key)
        if key in self.fail:
            raise OSError("AccessDenied simulado")
        dest.write_bytes(self.objects[key])


OBJS = {
    "raw/origin=stream/source=reddit/dt=2026-10-23/hr=03/part-20261023T032000Z-p000.parquet": b"a" * 10,
    "raw/origin=historical/source=facebook/dt=2026-05-01/hr=00/part-historical-s3101-20260501T00.parquet": b"b" * 20,
    "trusted/posts/source=facebook/dt=2026-05-01/part-0.parquet": b"c" * 5,
}


def test_baja_lo_que_falta_y_luego_nada(tmp_path):
    s3 = FakeS3(dict(OBJS))
    rep = s3sync.sync_prefix(s3, "lake", "raw/", tmp_path, workers=4)
    assert rep.ok and rep.objects == 2 and rep.downloaded == 2 and rep.to_download == 2 and rep.bytes == 30
    assert (s3sync.bucket_root(tmp_path, "lake") / list(OBJS)[0]).read_bytes() == b"a" * 10
    assert rep.by_prefix == {"origin=historical/source=facebook": {"objects": 1, "bytes": 20},
                             "origin=stream/source=reddit": {"objects": 1, "bytes": 10}}
    manifest = s3sync.read_manifest(tmp_path, "lake", "raw")
    assert manifest["objects"] == 2 and manifest["ok"] is True
    again = s3sync.sync_prefix(s3, "lake", "raw/", tmp_path)
    assert again.downloaded == 0 and again.skipped_same_size == 2 and again.to_download == 0
    assert len(s3.downloads) == 2


def test_tamano_distinto_se_vuelve_a_bajar_y_no_borra_locales(tmp_path):
    s3 = FakeS3(dict(OBJS))
    s3sync.sync_prefix(s3, "lake", "raw/", tmp_path)
    key = list(OBJS)[0]
    s3.objects[key] = b"z" * 11
    extra = s3sync.bucket_root(tmp_path, "lake") / "raw/origin=stream/source=reddit/dt=2026-10-23/hr=04/viejo.parquet"
    extra.parent.mkdir(parents=True)
    extra.write_bytes(b"v")
    rep = s3sync.sync_prefix(s3, "lake", "raw/", tmp_path)
    assert rep.downloaded == 1 and rep.stale_local == 1 and extra.exists()


def test_dry_run_no_baja_ni_escribe_manifiesto(tmp_path):
    s3 = FakeS3(dict(OBJS))
    rep = s3sync.sync_prefix(s3, "lake", "trusted/posts/", tmp_path, dry_run=True)
    assert rep.objects == 1 and rep.to_download == 1 and rep.downloaded == 0 and s3.downloads == []
    assert s3sync.read_manifest(tmp_path, "lake", "trusted") is None


def test_fallos_se_cuentan_sin_dejar_parciales(tmp_path):
    key = list(OBJS)[1]
    s3 = FakeS3(dict(OBJS), fail={key})
    rep = s3sync.sync_prefix(s3, "lake", "raw/", tmp_path)
    assert not rep.ok and rep.failed == 1 and rep.failures[0]["key"] == key
    assert "AccessDenied" in rep.failures[0]["error"]
    assert not (s3sync.bucket_root(tmp_path, "lake") / key).exists()


def test_manifiesto_por_prefijo_parcial(tmp_path):
    s3 = FakeS3(dict(OBJS))
    s3sync.sync_prefix(s3, "lake", "raw/origin=stream/", tmp_path)
    assert s3sync.read_manifest(tmp_path, "lake", "raw") is None
    assert (s3sync.bucket_root(tmp_path, "lake") / "_sync" / "raw_origin-stream.json").exists()


def test_cache_fuera_del_repo(tmp_path):
    assert s3sync.resolve_cache(tmp_path / "c", env={}) == (tmp_path / "c").resolve()
    assert s3sync.resolve_cache(None, env={"TA_QUALITY_CACHE": str(tmp_path / "e")}) == (tmp_path / "e").resolve()
    assert s3sync.resolve_cache(None, env={}) == Path("~/.cache/trendanalyzer/quality").expanduser().resolve()
    with pytest.raises(s3sync.CacheError):
        s3sync.resolve_cache(REPO / "project" / ".cache-de-prueba", env={})


def test_bucket_por_opcion_entorno_o_cuenta():
    assert s3sync.resolve_bucket("b1", env={"TA_LAKE_BUCKET": "b2"}) == "b1"
    assert s3sync.resolve_bucket(None, env={"TA_LAKE_BUCKET": "b2"}) == "b2"
    assert s3sync.resolve_bucket(None, env={"ENV": "dev"}, account=lambda: "123") == "ta-dev-123-lake"
    with pytest.raises(s3sync.CacheError):
        s3sync.resolve_bucket(None, env={})


class FakeClient:
    def __init__(self):
        self.calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class P:
            def paginate(self, **kw):
                client.calls.append(("list", kw))
                yield {"Contents": [{"Key": "raw/a.parquet", "Size": 3, "ETag": '"e1"'}]}
                yield {}

        return P()

    def download_file(self, bucket, key, dest):
        self.calls.append(("get", bucket, key))
        Path(dest).write_bytes(b"abc")

    def get_caller_identity(self):
        return {"Account": "648617469176", "Arn": "arn:aws:iam::648617469176:root"}


def test_adaptador_boto3_y_cuenta(tmp_path):
    client = FakeClient()
    port = s3sync.Boto3S3(client=client)
    assert port.list_objects("lake", "raw/") == [{"key": "raw/a.parquet", "size": 3, "etag": "e1"}]
    port.download("lake", "raw/a.parquet", tmp_path / "a")
    assert (tmp_path / "a").read_bytes() == b"abc"
    assert client.calls[0] == ("list", {"Bucket": "lake", "Prefix": "raw/"})
    assert s3sync.account_id(client=client) == "648617469176"


def test_reporte_json_serializable(tmp_path):
    rep = s3sync.sync_prefix(FakeS3(dict(OBJS)), "lake", "raw/", tmp_path)
    data = json.loads(json.dumps(rep.to_json()))
    assert data["bucket"] == "lake" and data["prefix"] == "raw/" and "cache_root" in data
