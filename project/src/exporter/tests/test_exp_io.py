"""Los adaptadores reales (boto3 y google-cloud-storage) con clientes falsos, sin red."""

from datetime import datetime, timezone

import pytest

from exporter.core import GcsObject, StateConflict
from exporter.gcs_io import GcsLake
from exporter.s3_io import PARQUET_CONTENT_TYPE, S3Lake
from exporter.testing import crc32c_b64

botocore = pytest.importorskip("botocore")
from botocore.stub import ANY, Stubber  # noqa: E402

KMS = "arn:aws:kms:us-east-1:648617469176:key/11111111-2222-3333-4444-555555555555"


def _client():
    import boto3

    return boto3.client("s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")


def test_put_object_con_sse_kms_bucket_key_y_crc32c():
    client = _client()
    body = b"parquet!"
    crc = crc32c_b64(body)
    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KMS, "BucketKeyEnabled": True, "ETag": '"e"'},
            {
                "Bucket": "b",
                "Key": "raw/origin=stream/source=reddit/dt=2026-10-24/hr=12/part-x.parquet",
                "Body": body,
                "ContentType": PARQUET_CONTENT_TYPE,
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": KMS,
                "BucketKeyEnabled": True,
                "ChecksumCRC32C": crc,
                "Metadata": {"exporter": "gcs-to-s3@1.0"},
            },
        )
        resp = S3Lake("b", KMS, client).put_object(
            "raw/origin=stream/source=reddit/dt=2026-10-24/hr=12/part-x.parquet",
            body, crc32c=crc, md5="bWQ1", metadata={"exporter": "gcs-to-s3@1.0"},
        )
    assert resp == {"sse": "aws:kms", "kms_key_id": KMS, "bucket_key": True, "checksum_crc32c": None, "etag": '"e"'}


def test_put_object_sin_crc32c_usa_content_md5():
    client = _client()
    with Stubber(client) as stub:
        stub.add_response("put_object", {"ServerSideEncryption": "aws:kms"},
                          {"Bucket": "b", "Key": "k", "Body": b"x", "ContentType": ANY, "ServerSideEncryption": "aws:kms",
                           "SSEKMSKeyId": KMS, "BucketKeyEnabled": True, "ContentMD5": "bWQ1", "Metadata": {}})
        S3Lake("b", KMS, client).put_object("k", b"x", crc32c=None, md5="bWQ1")


def test_list_prefix_pagina():
    client = _client()
    prefix = "raw/origin=stream/source=reddit/dt=2026-10-24/hr=12/"
    with Stubber(client) as stub:
        stub.add_response("list_objects_v2", {"Contents": [{"Key": prefix + "a", "Size": 3}], "IsTruncated": True,
                                              "NextContinuationToken": "t"}, {"Bucket": "b", "Prefix": prefix})
        stub.add_response("list_objects_v2", {"Contents": [{"Key": prefix + "b", "Size": 4}], "IsTruncated": False},
                          {"Bucket": "b", "Prefix": prefix, "ContinuationToken": "t"})
        assert S3Lake("b", KMS, client).list_prefix(prefix) == {prefix + "a": 3, prefix + "b": 4}


def test_head_pide_el_checksum():
    client = _client()
    with Stubber(client) as stub:
        stub.add_response("head_object", {"ContentLength": 8, "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KMS,
                                          "BucketKeyEnabled": True, "ChecksumCRC32C": "AAAA", "Metadata": {"a": "b"}},
                          {"Bucket": "b", "Key": "k", "ChecksumMode": "ENABLED"})
        head = S3Lake("b", KMS, client).head("k")
    assert head["size"] == 8 and head["checksum_crc32c"] == "AAAA" and head["metadata"] == {"a": "b"}


class _Blob:
    def __init__(self, store, name, generation=None):
        self.store, self.name, self.generation_req = store, name, generation
        self.generation = None

    def reload(self):
        from google.api_core import exceptions as gexc

        if self.name not in self.store.state:
            raise gexc.NotFound("no")
        self.generation = self.store.state[self.name][1]

    def download_as_bytes(self, **kw):
        self.store.calls.append(("download", self.name, self.generation_req, kw))
        if self.name in self.store.state:
            return self.store.state[self.name][0]
        return b"data"

    def upload_from_string(self, body, content_type=None, if_generation_match=None):
        from google.api_core import exceptions as gexc

        current = self.store.state.get(self.name, (None, 0))[1]
        if if_generation_match is not None and if_generation_match != current:
            raise gexc.PreconditionFailed("412")
        self.store.state[self.name] = (body, current + 1)
        self.generation = current + 1

    def delete(self):
        self.store.state.pop(self.name, None)


class _Bucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name, generation=None):
        return _Blob(self.store, name, generation)


class _ListedBlob:
    def __init__(self, name):
        self.name, self.size, self.generation = name, "7", "42"
        self.time_created = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
        self.crc32c, self.md5_hash = "crc", "md5"


class _Client:
    def __init__(self):
        self.state, self.calls = {}, []

    def bucket(self, name):
        return _Bucket(self)

    def list_blobs(self, bucket, **kw):
        self.calls.append(("list", kw))
        return [_ListedBlob("enriched/source=reddit/dt=2026-10-24/hr=12/part-a.parquet")]


def test_gcs_listado_con_campos_y_start_offset():
    pytest.importorskip("google.api_core")
    client = _Client()
    lake = GcsLake("lake", client=client)
    objs = list(lake.list_objects("enriched/source=reddit/", "enriched/source=reddit/dt=2026-10-24/hr=09/"))
    assert objs == [GcsObject("enriched/source=reddit/dt=2026-10-24/hr=12/part-a.parquet", 7, 42, objs[0].time_created, "crc", "md5")]
    kw = client.calls[0][1]
    assert kw["start_offset"].endswith("hr=09/") and "timeCreated" in kw["fields"] and "crc32c" in kw["fields"]
    lake.download(objs[0])
    assert client.calls[-1][2] == 42  # descarga la generación listada


def test_gcs_watermark_con_precondicion():
    pytest.importorskip("google.api_core")
    client = _Client()
    lake = GcsLake("lake", client=client)
    assert lake.read_state("export-state/watermark.json") == (None, 0)
    lake.write_state("export-state/watermark.json", {"a": 1}, if_generation_match=0)
    data, gen = lake.read_state("export-state/watermark.json")
    assert data == {"a": 1} and gen == 1
    with pytest.raises(StateConflict):
        lake.write_state("export-state/watermark.json", {"a": 2}, if_generation_match=0)
