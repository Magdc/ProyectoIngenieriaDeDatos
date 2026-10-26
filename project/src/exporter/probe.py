"""Sondas de permisos: lo que la identidad del exportador puede y no puede hacer.

Se corren como el exportador de verdad: en Cloud Run (``gcloud run jobs execute … --args=probe``)
con ``sa-exporter`` y el rol asumido; en la máquina del desarrollador solo la parte de AWS
(el token de ``sa-exporter`` se obtiene por impersonación, la parte de GCS usaría las
credenciales del desarrollador y no probaría nada).

Ninguna sonda deja datos: las de escritura que deben fallar apuntan a claves nuevas, y la
única escritura permitida (``export-state/_probe/``) se borra al terminar.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable

ALLOW = "permitido"
DENY = "denegado"


@dataclass
class ProbeResult:
    id: str
    what: str
    expect: str
    got: str
    ok: bool
    detail: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _classify_aws(exc: Exception) -> str:
    code = ""
    status = None
    response = getattr(exc, "response", None) or {}
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "")
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    if code in {"AccessDenied", "AllAccessDisabled", "AccessDeniedException", "Forbidden"} or status == 403:
        return DENY
    return f"error {code or type(exc).__name__}"


def _classify_gcs(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    name = type(exc).__name__
    if code == 403 or name == "Forbidden":
        return DENY
    if code == 404 or name == "NotFound":
        return "no existe"
    return f"error {name}"


def _attempt(fn: Callable[[], Any], classify: Callable[[Exception], str]) -> tuple[str, str]:
    try:
        value = fn()
    except Exception as exc:  # noqa: BLE001
        return classify(exc), f"{type(exc).__name__}: {str(exc)[:300]}"
    return ALLOW, "" if value is None else str(value)[:300]


def _result(pid: str, what: str, expect: str, got: str, detail: str) -> ProbeResult:
    return ProbeResult(pid, what, expect, got, got == expect, detail)


def probe_aws(s3_client: Any, bucket: str, run_id: str, assumed_arn: str | None = None, role_name: str = "ta-gcp-exporter-role") -> list[ProbeResult]:
    """Con las credenciales del rol asumido (no deja objetos)."""
    out: list[ProbeResult] = []
    if assumed_arn is not None:
        ok = f":assumed-role/{role_name}/" in assumed_arn
        out.append(ProbeResult("A1", "AssumeRoleWithWebIdentity", ALLOW, ALLOW if ok else "otro rol", ok, assumed_arn))
    probe_key = f"_probe/{run_id}.txt"
    checks = [
        ("A2", "ListBucket raw/origin=stream/", ALLOW,
         lambda: s3_client.list_objects_v2(Bucket=bucket, Prefix="raw/origin=stream/", MaxKeys=1).get("KeyCount")),
        ("A3", "ListBucket raw/origin=historical/", DENY,
         lambda: s3_client.list_objects_v2(Bucket=bucket, Prefix="raw/origin=historical/", MaxKeys=1).get("KeyCount")),
        ("A4", "ListBucket trusted/", DENY,
         lambda: s3_client.list_objects_v2(Bucket=bucket, Prefix="trusted/", MaxKeys=1).get("KeyCount")),
        ("A5", "PutObject raw/origin=historical/", DENY,
         lambda: s3_client.put_object(Bucket=bucket, Key=f"raw/origin=historical/{probe_key}", Body=b"probe",
                                      ServerSideEncryption="aws:kms").get("ETag")),
        ("A7", "DeleteObject raw/origin=stream/", DENY,
         lambda: s3_client.delete_object(Bucket=bucket, Key=f"raw/origin=stream/{probe_key}").get("ResponseMetadata", {}).get("HTTPStatusCode")),
    ]
    for pid, what, expect, fn in checks:
        got, detail = _attempt(fn, _classify_aws)
        out.append(_result(pid, what, expect, got, detail))
    out.insert(len(out) - 1, _probe_get(s3_client, bucket, probe_key))
    return out


def _probe_get(s3_client: Any, bucket: str, probe_key: str) -> ProbeResult:
    """A6: leer un objeto **existente** de ``raw/origin=stream/`` debe dar AccessDenied.

    Con una clave que no existe no sirve: como el rol tiene ``s3:ListBucket`` en ese prefijo,
    S3 responde ``NoSuchKey`` (404) en vez de 403 aunque el rol no tenga ``s3:GetObject``
    (observado en la parte D de la validación, v2.30). Si todavía no hay objetos, la sonda
    no puede probar la lectura y lo dice (la cubre la política simulada, C5b).
    """
    what = "GetObject de un objeto de raw/origin=stream/"
    existing: list[str] = []
    try:
        listing = s3_client.list_objects_v2(Bucket=bucket, Prefix="raw/origin=stream/source=", MaxKeys=1)
        existing = [o["Key"] for o in listing.get("Contents", []) or []]
    except Exception:  # noqa: BLE001 - si no puede listar, lo dice A2
        existing = []
    if existing:
        key = existing[0]
        got, detail = _attempt(lambda: s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-0").get("ContentLength"),
                               _classify_aws)
        return _result("A6", what, DENY, got, f"{key} {detail}".strip())
    got, detail = _attempt(lambda: s3_client.get_object(Bucket=bucket, Key=f"raw/origin=stream/{probe_key}").get("ContentLength"),
                           _classify_aws)
    if got in (DENY, "error NoSuchKey"):
        return ProbeResult("A6", what, DENY, "sin objetos", True,
                           "raw/origin=stream/ todavía está vacío: no se pudo probar la lectura en vivo (la cubre C5b); "
                           f"una clave inexistente respondió {got}")
    return _result("A6", what, DENY, got, detail)


def probe_gcs(gcs: Any, run_id: str) -> list[ProbeResult]:
    """Con la identidad de ``sa-exporter`` (en Cloud Run). ``gcs`` es un ``GcsLake``."""
    out: list[ProbeResult] = []
    first_enriched: list[str] = []

    def list_enriched() -> int:
        names = [o.name for o in gcs.list_objects("enriched/", max_results=1)]
        first_enriched.extend(names)
        return len(names)

    got, detail = _attempt(list_enriched, _classify_gcs)
    out.append(_result("G1", "listar enriched/", ALLOW, got, detail))

    if first_enriched:
        name = first_enriched[0]
        got, detail = _attempt(lambda: len(gcs.download_range(name, 0, 0)), _classify_gcs)
        out.append(_result("G2", "leer un objeto de enriched/", ALLOW, got, f"{name} {detail}".strip()))
    else:
        out.append(ProbeResult("G2", "leer un objeto de enriched/", ALLOW, "sin objetos", False, "enriched/ vacío"))

    raw_names: list[str] = []
    got_l, detail_l = _attempt(lambda: raw_names.extend(o.name for o in gcs.list_objects("raw/", max_results=1)), _classify_gcs)
    if raw_names:
        name = raw_names[0]
        got, detail = _attempt(lambda: len(gcs.download_range(name, 0, 0)), _classify_gcs)
        out.append(_result("G3", "leer un objeto de raw/", DENY, got, f"{name} {detail}".strip()))
    else:
        out.append(ProbeResult("G3", "leer un objeto de raw/", DENY, "sin objetos", True,
                               f"raw/ vacío o no listable ({got_l} {detail_l}); no se pudo probar la lectura"))

    got, detail = _attempt(lambda: gcs.upload_bytes(f"enriched/_probe/{run_id}.txt", b"probe", if_generation_match=0), _classify_gcs)
    out.append(_result("G4", "escribir en enriched/", DENY, got, detail))

    state_key = f"export-state/_probe/{run_id}.json"
    got, detail = _attempt(lambda: gcs.upload_bytes(state_key, b"{}", if_generation_match=0, content_type="application/json"), _classify_gcs)
    out.append(_result("G5", "escribir en export-state/", ALLOW, got, detail))
    if got == ALLOW:
        got_d, detail_d = _attempt(lambda: gcs.delete(state_key), _classify_gcs)
        out.append(_result("G6", "borrar en export-state/ (sobrescribir el watermark)", ALLOW, got_d, detail_d))
    return out


def summarize(results: list[ProbeResult]) -> dict[str, Any]:
    return {
        "ok": all(r.ok for r in results),
        "passed": sum(1 for r in results if r.ok),
        "total": len(results),
        "results": [r.to_json() for r in results],
    }
