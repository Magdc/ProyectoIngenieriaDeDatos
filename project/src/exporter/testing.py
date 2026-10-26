"""Dobles de prueba del exportador (sin red ni nube)."""

from __future__ import annotations

import base64
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from exporter.core import GcsObject, StateConflict

T0 = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)


def crc32c_b64(data: bytes) -> str:
    try:
        import google_crc32c

        return base64.b64encode(google_crc32c.Checksum(data).digest()).decode()
    except ImportError:  # pragma: no cover - google-crc32c viene con google-cloud-storage
        import zlib

        return base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()


def gcs_key(source: str, when: datetime, panel: int = 0) -> str:
    start = when.replace(minute=(when.minute // 5) * 5, second=0, microsecond=0)
    return (
        f"enriched/source={source}/dt={start:%Y-%m-%d}/hr={start:%H}/"
        f"part-{start:%Y%m%dT%H%M%S}Z-p{panel:03d}.parquet"
    )


class FakeGcs:
    """``GcsPort`` en memoria. ``time_created`` lo da quien agrega el objeto."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, datetime, int]] = {}
        self.state: dict[str, tuple[dict, int]] = {}
        self.fail_download: set[str] = set()
        self.conflicts_on_write = 0
        self.list_calls: list[tuple[str, str | None]] = []
        self.downloads: list[str] = []
        self._gen = 1000
        self._lock = threading.Lock()

    def _next_gen(self) -> int:
        self._gen += 1
        return self._gen

    def add(self, name: str, body: bytes, created: datetime) -> GcsObject:
        self.objects[name] = (body, created, self._next_gen())
        return self.get(name)

    def get(self, name: str) -> GcsObject:
        body, created, gen = self.objects[name]
        return GcsObject(name, len(body), gen, created, crc32c_b64(body), None)

    def list_objects(self, prefix: str, start_offset: str | None = None, max_results: int | None = None) -> Iterable[GcsObject]:
        self.list_calls.append((prefix, start_offset))
        names = sorted(n for n in self.objects if n.startswith(prefix) and (start_offset is None or n >= start_offset))
        if max_results:
            names = names[:max_results]
        return [self.get(n) for n in names]

    def download(self, obj: GcsObject) -> bytes:
        with self._lock:
            self.downloads.append(obj.name)
        if obj.name in self.fail_download:
            raise RuntimeError("fallo simulado de descarga")
        return self.objects[obj.name][0]

    def read_state(self, key: str) -> tuple[dict | None, int]:
        if key not in self.state:
            return None, 0
        data, gen = self.state[key]
        return json.loads(json.dumps(data)), gen

    def write_state(self, key: str, data: dict, if_generation_match: int) -> int:
        current = self.state.get(key, (None, 0))[1]
        if self.conflicts_on_write > 0:
            self.conflicts_on_write -= 1
            # otra corrida escribió en medio
            other = dict(data)
            self.state[key] = (other, self._next_gen())
            raise StateConflict("generation mismatch (simulado)")
        if if_generation_match != current:
            raise StateConflict(f"generation {current} != {if_generation_match}")
        gen = self._next_gen()
        self.state[key] = (json.loads(json.dumps(data)), gen)
        return gen


@dataclass
class FakeS3:
    """``S3Port`` en memoria que imita lo que responde S3 con SSE-KMS."""

    kms_key_arn: str = "arn:aws:kms:us-east-1:648617469176:key/11111111-2222-3333-4444-555555555555"
    objects: dict[str, dict[str, Any]] = field(default_factory=dict)
    fail_put: set[str] = field(default_factory=set)
    wrong_sse: bool = False
    puts: list[dict[str, Any]] = field(default_factory=list)
    list_calls: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def list_prefix(self, prefix: str) -> dict[str, int]:
        self.list_calls.append(prefix)
        return {k: len(v["body"]) for k, v in self.objects.items() if k.startswith(prefix)}

    def put_object(self, key: str, body: bytes, *, crc32c: str | None, md5: str | None, metadata: dict[str, str]) -> dict[str, Any]:
        with self._lock:
            self.puts.append({"key": key, "size": len(body), "crc32c": crc32c, "metadata": dict(metadata)})
        if key in self.fail_put:
            raise RuntimeError("AccessDenied (simulado)")
        if crc32c and crc32c != crc32c_b64(body):
            raise RuntimeError("BadDigest: el CRC32C no coincide")
        with self._lock:
            self.objects[key] = {"body": body, "metadata": dict(metadata), "crc32c": crc32c}
        if self.wrong_sse:
            return {"sse": "AES256", "kms_key_id": None}
        return {"sse": "aws:kms", "kms_key_id": self.kms_key_arn, "bucket_key": True, "checksum_crc32c": crc32c}

    def head(self, key: str) -> dict[str, Any]:
        obj = self.objects[key]
        return {
            "size": len(obj["body"]),
            "sse": "aws:kms",
            "kms_key_id": self.kms_key_arn,
            "bucket_key": True,
            "checksum_crc32c": obj.get("crc32c"),
            "metadata": obj["metadata"],
        }


def make_jwt(claims: dict[str, Any]) -> str:
    def enc(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{enc({'alg': 'RS256', 'typ': 'JWT'})}.{enc(claims)}.c2lnbmF0dXJh"


def sa_claims(unique_id: str = "112233445566778899001", email: str = "sa-exporter@p.iam.gserviceaccount.com",
              audience: str = "ta-gcp-exporter-role", azp: str | None = "same", minutes: int = 60) -> dict[str, Any]:
    now = int(datetime.now(timezone.utc).timestamp())
    claims: dict[str, Any] = {
        "iss": "https://accounts.google.com",
        "aud": audience,
        "sub": unique_id,
        "email": email,
        "email_verified": True,
        "iat": now,
        "exp": now + minutes * 60,
    }
    if azp == "same":
        claims["azp"] = unique_id
    elif azp is not None:
        claims["azp"] = azp
    return claims


def trust_policy(unique_id: str, email: str, audience: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Federated": "accounts.google.com"},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        "accounts.google.com:aud": [unique_id, email, audience],
                        "accounts.google.com:oaud": audience,
                        "accounts.google.com:sub": unique_id,
                    }
                },
            }
        ],
    }


class AwsError(Exception):
    """Imita ``botocore.exceptions.ClientError`` (atributo ``response``)."""

    def __init__(self, code: str, status: int = 403):
        super().__init__(f"An error occurred ({code})")
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class FakeRoleS3Client:
    """Cliente S3 con los permisos de ``ta-gcp-exporter-role`` (para las sondas)."""

    def __init__(self, allow_prefix: str = "raw/origin=stream/", existing: tuple[str, ...] = (), allow_get: bool = False) -> None:
        self.allow_prefix = allow_prefix
        self.existing = list(existing)
        self.allow_get = allow_get
        self.calls: list[tuple[str, str]] = []

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("list", Prefix))
        if not Prefix.startswith(self.allow_prefix):
            raise AwsError("AccessDenied")
        keys = [k for k in self.existing if k.startswith(Prefix)][:MaxKeys]
        return {"KeyCount": len(keys), "Contents": [{"Key": k, "Size": 1} for k in keys]}

    def put_object(self, Bucket: str, Key: str, **kw: Any) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("put", Key))
        if not Key.startswith(self.allow_prefix):
            raise AwsError("AccessDenied")
        return {"ETag": '"x"'}

    def get_object(self, Bucket: str, Key: str, **kw: Any) -> dict[str, Any]:  # noqa: N803
        """Como S3: sin ``s3:GetObject`` un objeto que existe da 403; uno que no existe da 404
        si el rol puede listar ese prefijo (y 403 si no)."""
        self.calls.append(("get", Key))
        if Key in self.existing:
            if self.allow_get:
                return {"ContentLength": 1}
            raise AwsError("AccessDenied")
        if Key.startswith(self.allow_prefix):
            raise AwsError("NoSuchKey", 404)
        raise AwsError("AccessDenied")

    def delete_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("delete", Key))
        raise AwsError("AccessDenied")


class GcsHttpError(Exception):
    def __init__(self, code: int, name: str = "Forbidden"):
        super().__init__(f"{code} {name}")
        self.code = code


class FakeExporterGcs(FakeGcs):
    """``FakeGcs`` con los permisos de ``sa-exporter`` (para las sondas de GCS)."""

    def __init__(self, enriched_read: bool = True, raw_read: bool = False, enriched_write: bool = False) -> None:
        super().__init__()
        self.enriched_read = enriched_read
        self.raw_read = raw_read
        self.enriched_write = enriched_write
        self.deleted: list[str] = []
        self.written: list[str] = []

    def download_range(self, name: str, start: int = 0, end: int = 0) -> bytes:
        if name.startswith("enriched/") and self.enriched_read:
            return self.objects[name][0][start : end + 1]
        if name.startswith("raw/") and self.raw_read:
            return self.objects[name][0][start : end + 1]
        raise GcsHttpError(403)

    def upload_bytes(self, key: str, body: bytes, *, if_generation_match: int | None = 0, content_type: str = "text/plain") -> None:
        if key.startswith("export-state/") or (key.startswith("enriched/") and self.enriched_write):
            self.written.append(key)
            self.add(key, body, T0 + timedelta(hours=1))
            return
        raise GcsHttpError(403)

    def delete(self, key: str) -> None:
        if not key.startswith("export-state/"):
            raise GcsHttpError(403)
        self.deleted.append(key)
        self.objects.pop(key, None)


class FakeSts:
    def __init__(self, role_arn: str = "arn:aws:iam::648617469176:role/ta-gcp-exporter-role", fail: str | None = None):
        self.role_arn = role_arn
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def assume_role_with_web_identity(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(kw)
        if self.fail:
            raise AwsError(self.fail, 403)
        name = kw["RoleArn"].split("/")[-1]
        return {
            "Credentials": {
                "AccessKeyId": "ASIAFAKE",
                "SecretAccessKey": "secreto",
                "SessionToken": "sesion",
                "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
            },
            "AssumedRoleUser": {"Arn": f"arn:aws:sts::648617469176:assumed-role/{name}/{kw['RoleSessionName']}"},
            "SubjectFromWebIdentityToken": "112233445566778899001",
            "Audience": "112233445566778899001",
            "Provider": "accounts.google.com",
        }
