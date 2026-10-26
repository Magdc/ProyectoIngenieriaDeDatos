"""Acceso a GCS con ``google-cloud-storage`` (la versión del simulador y del stream)."""

from __future__ import annotations

import json
from typing import Any, Iterable

from exporter.core import GcsObject, StateConflict

_LIST_FIELDS = "items(name,size,generation,timeCreated,crc32c,md5Hash),nextPageToken"


class GcsLake:
    """``GcsPort`` real. Con ``sa-exporter`` solo puede leer ``enriched/`` y escribir en
    ``export-state/`` (condiciones de IAM de ``infra/gcp/environments/dev/exporter.tf``)."""

    def __init__(self, bucket: str, project: str | None = None, client: Any = None, credentials: Any = None):
        if client is None:
            from google.cloud import storage

            client = storage.Client(project=project, credentials=credentials)
        self._client = client
        self.bucket_name = bucket
        self._bucket = client.bucket(bucket)

    def list_objects(self, prefix: str, start_offset: str | None = None, max_results: int | None = None) -> Iterable[GcsObject]:
        kwargs: dict[str, Any] = {"prefix": prefix, "fields": _LIST_FIELDS}
        if start_offset:
            kwargs["start_offset"] = start_offset
        if max_results:
            kwargs["max_results"] = max_results
        for blob in self._client.list_blobs(self._bucket, **kwargs):
            yield GcsObject(
                name=blob.name,
                size=int(blob.size or 0),
                generation=int(blob.generation or 0),
                time_created=blob.time_created,
                crc32c=blob.crc32c,
                md5=blob.md5_hash,
            )

    def download(self, obj: GcsObject) -> bytes:
        # La generación fija la versión listada; la librería verifica el checksum al bajar.
        blob = self._bucket.blob(obj.name, generation=obj.generation or None)
        return blob.download_as_bytes()

    def download_range(self, name: str, start: int = 0, end: int = 0) -> bytes:
        return self._bucket.blob(name).download_as_bytes(start=start, end=end)

    def read_state(self, key: str) -> tuple[dict | None, int]:
        from google.api_core import exceptions as gexc

        blob = self._bucket.blob(key)
        try:
            blob.reload()
        except gexc.NotFound:
            return None, 0
        generation = int(blob.generation or 0)
        raw = blob.download_as_bytes(if_generation_match=generation)
        return json.loads(raw.decode("utf-8")), generation

    def write_state(self, key: str, data: dict, if_generation_match: int) -> int:
        from google.api_core import exceptions as gexc

        blob = self._bucket.blob(key)
        body = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False).encode("utf-8")
        try:
            blob.upload_from_string(body, content_type="application/json", if_generation_match=if_generation_match)
        except gexc.PreconditionFailed as exc:
            raise StateConflict(str(exc)) from exc
        return int(blob.generation or 0)

    def delete(self, key: str) -> None:
        self._bucket.blob(key).delete()

    def upload_bytes(self, key: str, body: bytes, *, if_generation_match: int | None = 0, content_type: str = "text/plain") -> None:
        self._bucket.blob(key).upload_from_string(body, content_type=content_type, if_generation_match=if_generation_match)
