"""Publicación **opcional y apagada por defecto** del informe en ``s3://{lake}/quality/{run_date}/`` (§5.7).

El Lote 5.1 no escribe en la nube: ``publish`` sin ``--apply`` solo muestra el plan. Con ``--apply``
sube el JSON y el Markdown con ``PutObject`` (SSE-KMS; la llave la pone el cifrado por defecto del
bucket o ``TA_LAKE_KMS_KEY_ARN``) con las credenciales del perfil. **Sin comprobar contra AWS**:
el rol que escribirá ``quality/`` (``ta-glue-role``, §5.8) llega con el Lote 4.1, y la escritura
habitual de resultados es del Lote 5.2 (``dq_results`` + ``quality/``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

CONTENT_TYPES = {".json": "application/json", ".md": "text/markdown; charset=utf-8"}


def plan(report: dict[str, Any], files: list[Path]) -> list[dict[str, str]]:
    run_date = report["generated_at"][:10]
    layer = report["layer"]
    return [{"file": str(f), "key": f"quality/{run_date}/profiling/{layer}/{Path(f).name}",
             "content_type": CONTENT_TYPES.get(Path(f).suffix, "application/octet-stream")} for f in files]


def publish(client: Any, bucket: str, items: list[dict[str, str]], *, kms_key_arn: str | None = None, apply: bool = False) -> list[dict[str, Any]]:
    out = []
    for item in items:
        res: dict[str, Any] = {"key": item["key"], "uri": f"s3://{bucket}/{item['key']}", "uploaded": False}
        if apply:
            extra: dict[str, Any] = {"ServerSideEncryption": "aws:kms", "ContentType": item["content_type"]}
            if kms_key_arn:
                extra["SSEKMSKeyId"] = kms_key_arn
                extra["BucketKeyEnabled"] = True
            body = Path(item["file"]).read_bytes()
            resp = client.put_object(Bucket=bucket, Key=item["key"], Body=body, **extra)
            res.update({"uploaded": True, "sse": resp.get("ServerSideEncryption"), "bytes": len(body)})
        out.append(res)
    return out
