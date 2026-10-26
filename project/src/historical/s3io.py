"""Etapa 5 · ``upload``: subida idempotente a ``s3://{lake_bucket}/raw/origin=historical/``.

* Sube exactamente los archivos de ``output.json`` con sus mismas claves (§5.7).
* Idempotente: lista el prefijo una vez y omite los objetos que ya existen con el mismo
  tamaño (como el exportador, §5.6). Reejecutar no duplica: las claves son deterministas.
* Cifrado SSE-KMS con la llave del lake (``--kms-key-arn``; si no se pasa, el cifrado
  por defecto del bucket, que es la misma llave).
* Por defecto asume el rol ``ta-historical-loader`` (``--role-arn``), que solo puede
  escribir y listar bajo ``raw/origin=historical/`` (§5.8). No borra nada: los objetos de
  la semilla que ya no estén en ``output.json`` se informan como sobrantes.

Usa ``boto3`` con el perfil local de AWS CLI (``AWS_PROFILE``), sin llaves en el código.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import time
from typing import Any, Mapping, Sequence

from historical.plan import UTC, format_ts
from historical.verify import AVISO, FALLA, OK, Check
from historical.work import HIST_PREFIX, PART_RE, WorkDir, atomic_write_json, log_event

CONTENT_TYPE = "application/vnd.apache.parquet"


class UploadError(RuntimeError):
    pass


def make_session(profile: str | None, region: str):
    import boto3

    return boto3.Session(profile_name=profile or None, region_name=region)


def assume_role_session(session, role_arn: str, region: str,
                        session_name: str = "ta-historical-loader"):
    import boto3

    creds = session.client("sts").assume_role(RoleArn=role_arn, RoleSessionName=session_name,
                                              DurationSeconds=3600)["Credentials"]
    return boto3.Session(aws_access_key_id=creds["AccessKeyId"],
                         aws_secret_access_key=creds["SecretAccessKey"],
                         aws_session_token=creds["SessionToken"], region_name=region)


def identity(session) -> dict[str, Any]:
    ident = session.client("sts").get_caller_identity()
    return {"account": ident.get("Account"), "arn": ident.get("Arn")}


def list_prefix(client, bucket: str, prefix: str = HIST_PREFIX) -> dict[str, int]:
    out: dict[str, int] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            out[obj["Key"]] = int(obj["Size"])
    return out


def upload(work: WorkDir, client, bucket: str, *, kms_key_arn: str | None = None,
           workers: int = 16, dry_run: bool = False,
           who: Mapping[str, Any] | None = None) -> dict[str, Any]:
    out = work.read_json(work.output_json)
    if not out:
        raise UploadError("falta output.json: corra `write` primero")
    files: Sequence[Mapping[str, Any]] = out["files"]
    t0 = time.monotonic()
    existing = list_prefix(client, bucket)
    todo = [f for f in files if existing.get(f["key"]) != int(f["size"])]
    skipped = len(files) - len(todo)
    listed = {f["key"] for f in files}
    stale = sorted(k for k in existing
                   if k not in listed and (m := PART_RE.match(k)) and int(m["seed"]) == work.seed)
    foreign = sorted(k for k in existing if k not in listed and k not in stale)
    errors: list[dict[str, str]] = []

    def put(f: Mapping[str, Any]) -> None:
        body = work.local_path(f["key"]).read_bytes()
        if len(body) != int(f["size"]):
            raise UploadError(f"{f['key']}: el archivo local cambió desde `write`")
        kwargs: dict[str, Any] = {
            "Bucket": bucket, "Key": f["key"], "Body": body, "ContentType": CONTENT_TYPE,
            "ServerSideEncryption": "aws:kms",
            "Metadata": {"ta-sha256": f["sha256"], "ta-seed": str(work.seed),
                         "ta-digest": str(out.get("digest", ""))[:16], "ta-lote": "3.1"}}
        if kms_key_arn:
            kwargs["SSEKMSKeyId"] = kms_key_arn
        client.put_object(**kwargs)

    uploaded = 0
    if not dry_run and todo:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(put, f): f for f in todo}
            for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                f = futures[fut]
                try:
                    fut.result()
                    uploaded += 1
                except Exception as exc:  # noqa: BLE001 - se informan todos
                    errors.append({"key": f["key"], "error": f"{type(exc).__name__}: {exc}"[:300]})
                if i % 1000 == 0:
                    log_event("hist_upload_progress", done=i, total=len(todo),
                              errors=len(errors))
    result = {"bucket": bucket, "prefix": HIST_PREFIX, "seed": work.seed,
              "digest": out.get("digest"), "files": len(files), "to_upload": len(todo),
              "uploaded": uploaded, "skipped_same_size": skipped, "errors": errors[:50],
              "n_errors": len(errors), "stale_same_seed": stale[:50], "n_stale": len(stale),
              "other_objects": len(foreign), "kms_key_arn": kms_key_arn, "dry_run": dry_run,
              "who": dict(who or {}), "bytes": sum(int(f["size"]) for f in todo),
              "uploaded_at": format_ts(dt.datetime.now(UTC)),
              "seconds": round(time.monotonic() - t0, 1)}
    if not dry_run:
        atomic_write_json(work.upload_json, result)
    log_event("hist_upload_done", uploaded=uploaded, skipped=skipped, errors=len(errors),
              stale=len(stale), seconds=result["seconds"])
    return result


def make_s3_check(client, bucket: str, *, kms_key_arn: str | None = None, sample: int = 12):
    """H14: objetos en S3 = ``output.json`` y cifrado SSE-KMS (con la llave, si se da)."""

    def check(files: Sequence[Mapping[str, Any]]) -> Check:
        try:
            existing = list_prefix(client, bucket)
        except Exception as exc:  # noqa: BLE001
            return Check("H14", "Objetos en S3", FALLA, f"no se pudo listar s3://{bucket}: {exc}"[:400])
        missing = [f["key"] for f in files if f["key"] not in existing]
        wrong = [f["key"] for f in files if f["key"] in existing
                 and existing[f["key"]] != int(f["size"])]
        problems = []
        if missing:
            problems.append(f"faltan {len(missing)} objetos ({missing[:2]})")
        if wrong:
            problems.append(f"{len(wrong)} objetos con otro tamaño ({wrong[:2]})")
        sse_bad = []
        step = max(1, len(files) // max(1, sample))
        heads = 0
        for f in list(files)[::step][:sample]:
            if f["key"] not in existing:
                continue
            try:
                head = client.head_object(Bucket=bucket, Key=f["key"])
            except Exception as exc:  # noqa: BLE001
                sse_bad.append(f"{f['key']}: head_object {type(exc).__name__}")
                continue
            heads += 1
            if head.get("ServerSideEncryption") != "aws:kms":
                sse_bad.append(f"{f['key']}: {head.get('ServerSideEncryption')}")
            elif kms_key_arn and head.get("SSEKMSKeyId") not in (kms_key_arn, None) \
                    and not str(head.get("SSEKMSKeyId", "")).endswith(kms_key_arn.split("/")[-1]):
                sse_bad.append(f"{f['key']}: llave {head.get('SSEKMSKeyId')}")
        if sse_bad:
            problems.append(f"cifrado: {sse_bad[:2]}")
        status = FALLA if problems else OK
        if not problems and heads == 0 and files:
            status = AVISO
            problems.append("no se pudo leer el cifrado de ningún objeto")
        return Check("H14", "Objetos en S3", status,
                     "; ".join(problems) or f"{len(files)} objetos en s3://{bucket}/{HIST_PREFIX} "
                                            f"con el mismo tamaño; SSE-KMS en {heads} revisados",
                     {"bucket": bucket, "objects_under_prefix": len(existing),
                      "missing": missing[:20], "wrong_size": wrong[:20]})

    return check


def permission_probe(client, bucket: str) -> dict[str, Any]:
    """El rol del loader no debe poder escribir fuera de ``raw/origin=historical/``."""
    probes = {
        "put raw/origin=stream/": lambda: client.put_object(
            Bucket=bucket, Key="raw/origin=stream/_ta_probe/denied.txt", Body=b"x",
            ServerSideEncryption="aws:kms"),
        "list trusted/": lambda: client.list_objects_v2(Bucket=bucket, Prefix="trusted/",
                                                        MaxKeys=1),
    }
    out = {}
    for name, fn in probes.items():
        try:
            fn()
            out[name] = "PERMITIDO"
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
            out[name] = "DENEGADO" if code in ("AccessDenied", "AllAccessDisabled") else f"ERROR {code or type(exc).__name__}"
    return out
