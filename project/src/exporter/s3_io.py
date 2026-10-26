"""Acceso a S3 con boto3: listado por partición y ``PutObject`` con SSE-KMS (§5.6)."""

from __future__ import annotations

from typing import Any

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


def s3_client(session: Any = None, region: str = "us-east-1") -> Any:
    import boto3
    from botocore.config import Config

    config = Config(region_name=region, retries={"max_attempts": 5, "mode": "standard"}, signature_version="s3v4",
                    max_pool_connections=10)
    if session is None:
        return boto3.client("s3", config=config)
    return session.client("s3", config=config)


class S3Lake:
    """``S3Port`` real. Con el rol ``ta-gcp-exporter-role`` solo puede listar y escribir bajo
    ``raw/origin=stream/`` (sin lectura ni borrado)."""

    def __init__(self, bucket: str, kms_key_arn: str | None, client: Any):
        self.bucket = bucket
        self.kms_key_arn = kms_key_arn
        self._s3 = client

    def list_prefix(self, prefix: str) -> dict[str, int]:
        out: dict[str, int] = {}
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []) or []:
                out[item["Key"]] = int(item["Size"])
        return out

    def put_object(
        self,
        key: str,
        body: bytes,
        *,
        crc32c: str | None = None,
        md5: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Un solo PUT (los Parquet de ``enriched/`` pesan KB): sin *multipart*, que pediría
        además ``kms:Decrypt``. Con el CRC32C de GCS, S3 rechaza el objeto si los bytes no
        coinciden (mismo formato: base64 del CRC32C *big-endian*)."""
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
            "ContentType": PARQUET_CONTENT_TYPE,
            "ServerSideEncryption": "aws:kms",
            "BucketKeyEnabled": True,
            "Metadata": dict(metadata or {}),
        }
        if self.kms_key_arn:
            kwargs["SSEKMSKeyId"] = self.kms_key_arn
        if crc32c:
            kwargs["ChecksumCRC32C"] = crc32c
        elif md5:
            kwargs["ContentMD5"] = md5
        resp = self._s3.put_object(**kwargs)
        return {
            "sse": resp.get("ServerSideEncryption"),
            "kms_key_id": resp.get("SSEKMSKeyId"),
            "bucket_key": resp.get("BucketKeyEnabled"),
            "checksum_crc32c": resp.get("ChecksumCRC32C"),
            "etag": resp.get("ETag"),
        }

    def head(self, key: str) -> dict[str, Any]:
        """Solo con las credenciales del desarrollador (el rol no tiene ``s3:GetObject``)."""
        resp = self._s3.head_object(Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        return {
            "size": int(resp.get("ContentLength", 0)),
            "sse": resp.get("ServerSideEncryption"),
            "kms_key_id": resp.get("SSEKMSKeyId"),
            "bucket_key": resp.get("BucketKeyEnabled"),
            "checksum_crc32c": resp.get("ChecksumCRC32C"),
            "metadata": dict(resp.get("Metadata") or {}),
        }

    def get_bytes(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def count_prefix(self, prefix: str) -> tuple[int, int]:
        n = size = 0
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []) or []:
                n += 1
                size += int(item["Size"])
        return n, size

    def iter_keys(self, prefix: str, max_keys: int | None = None):
        paginator = self._s3.get_paginator("list_objects_v2")
        seen = 0
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []) or []:
                yield item["Key"], int(item["Size"])
                seen += 1
                if max_keys and seen >= max_keys:
                    return
