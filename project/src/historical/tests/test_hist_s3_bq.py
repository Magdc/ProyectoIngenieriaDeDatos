"""Subida a S3 (cliente falso) y cruces con BigQuery (consulta falsa)."""

from __future__ import annotations

import json

import pytest

from historical import bq, s3io
from historical.verify import AVISO, FALLA, OK


class FakeError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self, deny_outside: str | None = None):
        self.objects: dict[str, dict] = {}
        self.puts: list[dict] = []
        self.deny_outside = deny_outside

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        s3 = self

        class P:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for k in s3.objects if k.startswith(Prefix))
                for i in range(0, len(keys), 1000):
                    yield {"Contents": [{"Key": k, "Size": len(s3.objects[k]["Body"])}
                                        for k in keys[i:i + 1000]]}

        return P()

    def put_object(self, **kw):
        if self.deny_outside and not kw["Key"].startswith(self.deny_outside):
            raise FakeError("AccessDenied")
        self.puts.append({k: v for k, v in kw.items() if k != "Body"})
        self.objects[kw["Key"]] = kw

    def head_object(self, Bucket, Key):
        obj = self.objects[Key]
        return {"ServerSideEncryption": obj.get("ServerSideEncryption"),
                "SSEKMSKeyId": obj.get("SSEKMSKeyId")}

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1):
        if self.deny_outside and not Prefix.startswith(self.deny_outside):
            raise FakeError("AccessDenied")
        return {}


KMS = "arn:aws:kms:us-east-1:111122223333:key/abcd-1234"


def test_subida_idempotente(fake_run):
    work, _, _, out = fake_run
    s3 = FakeS3()
    first = s3io.upload(work, s3, "bucket", kms_key_arn=KMS, workers=4)
    assert first["uploaded"] == out["n_files"] and first["n_errors"] == 0
    assert all(p["ServerSideEncryption"] == "aws:kms" and p["SSEKMSKeyId"] == KMS for p in s3.puts)
    assert all(p["Key"].startswith("raw/origin=historical/") for p in s3.puts)
    assert s3.puts[0]["Metadata"]["ta-seed"] == str(work.seed)
    second = s3io.upload(work, s3, "bucket", kms_key_arn=KMS)
    assert second["uploaded"] == 0 and second["skipped_same_size"] == out["n_files"]
    saved = json.loads(work.upload_json.read_text())
    assert saved["skipped_same_size"] == out["n_files"]


def test_tamano_distinto_se_vuelve_a_subir_y_sobrantes_se_informan(fake_run):
    work, _, _, out = fake_run
    s3 = FakeS3()
    s3io.upload(work, s3, "bucket")
    key = out["files"][0]["key"]
    s3.objects[key]["Body"] = b"corto"
    stale = f"raw/origin=historical/source=reddit/dt=2020-01-01/hr=00/part-historical-s{work.seed}-20200101T00.parquet"
    s3.objects[stale] = {"Body": b"viejo"}
    s3.objects["raw/origin=historical/otro.txt"] = {"Body": b"x"}
    res = s3io.upload(work, s3, "bucket")
    assert res["uploaded"] == 1 and res["n_stale"] == 1 and res["other_objects"] == 1
    assert stale in s3.objects, "la subida nunca borra"


def test_dry_run_no_sube(fake_run):
    work, _, _, out = fake_run
    s3 = FakeS3()
    res = s3io.upload(work, s3, "bucket", dry_run=True)
    assert res["to_upload"] == out["n_files"] and not s3.puts and not work.upload_json.exists()


def test_errores_se_cuentan(fake_run):
    work, _, _, _ = fake_run
    s3 = FakeS3(deny_outside="nada/")
    res = s3io.upload(work, s3, "bucket")
    assert res["uploaded"] == 0 and res["n_errors"] == res["to_upload"]


def test_chequeo_s3(fake_run):
    work, _, _, out = fake_run
    s3 = FakeS3()
    check = s3io.make_s3_check(s3, "bucket", kms_key_arn=KMS)
    assert check(out["files"]).status == FALLA
    s3io.upload(work, s3, "bucket", kms_key_arn=KMS)
    result = check(out["files"])
    assert result.status == OK, result
    s3.objects[out["files"][0]["key"]]["ServerSideEncryption"] = "AES256"
    assert "cifrado" in s3io.make_s3_check(s3, "bucket", sample=len(out["files"]))(out["files"]).detail


def test_sonda_de_permisos():
    assert s3io.permission_probe(FakeS3(deny_outside="raw/origin=historical/"), "b") == {
        "put raw/origin=stream/": "DENEGADO", "list trusted/": "DENEGADO"}
    assert s3io.permission_probe(FakeS3(), "b")["put raw/origin=stream/"] == "PERMITIDO"


# ------------------------------------------------------------------- BigQuery

def _query(n_rows, hit_users, versions=("v1",)):
    def q(table, hashes):
        return {"n_rows": n_rows, "hit_rows": hit_users * 3, "hit_users": hit_users,
                "stream_users": 900, "versions": list(versions)}

    return q


def test_verdict_de_la_llave():
    assert bq.make_key_check("t", query=_query(5000, 0))(["a"] * 10)["status"] == FALLA
    assert bq.make_key_check("t", query=_query(5000, 7))(["a"] * 10)["status"] == OK
    assert bq.make_key_check("t", query=_query(20, 0))(["a"])["status"] == AVISO

    def broken(table, hashes):
        raise RuntimeError("sin credenciales")

    assert bq.make_key_check("t", query=broken)(["a"])["status"] == AVISO


def test_cruce_con_el_stream_y_version():
    ok = bq.make_stream_check("t", query=_query(5000, 7, versions=("v1",)))(["a"], {"v1"})
    assert ok.status == OK and "misma nlp_model_version" in ok.detail
    other = bq.make_stream_check("t", query=_query(5000, 7, versions=("v2",)))(["a"], {"v1"})
    assert other.status == AVISO
    bad = bq.make_stream_check("t", query=_query(5000, 0))(["a"], {"v1"})
    assert bad.status == FALLA


def test_nombres_de_tabla(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert bq.normalize_table("p:ta_stream.posts_enriched") == "p.ta_stream.posts_enriched"
    assert bq.default_table("p") == "p.ta_stream.posts_enriched"
    with pytest.raises(ValueError):
        bq.default_table("")
