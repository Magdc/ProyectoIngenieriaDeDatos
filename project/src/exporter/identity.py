"""Identidad del exportador: ID token de ``sa-exporter`` → rol de AWS (HU-006, §5.6).

- En Cloud Run el token sale del servidor de metadatos (identidad del job = ``sa-exporter``).
- En la máquina del desarrollador sale por impersonación (``generateIdToken``), que exige
  ``roles/iam.serviceAccountOpenIdTokenCreator`` sobre ``sa-exporter`` (variable
  ``exporter_oidc_token_creators``); como respaldo, ``gcloud auth print-identity-token``.

AWS no necesita credenciales para ``AssumeRoleWithWebIdentity`` (la llamada va sin firmar):
basta el token. La confianza del rol la evalúa AWS con estas claves (documentación de IAM):
``accounts.google.com:aud`` = claim ``azp`` si existe (si no, ``aud``),
``accounts.google.com:oaud`` = claim ``aud`` y ``accounts.google.com:sub`` = claim ``sub``.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

METADATA_IDENTITY_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
GOOGLE_ISSUERS = {"https://accounts.google.com", "accounts.google.com"}
FEDERATED_PRINCIPAL = "accounts.google.com"
_SESSION_RE = re.compile(r"[^\w+=,.@-]")


class IdentityError(RuntimeError):
    """No se pudo obtener el token o asumir el rol."""


def on_gcp_runtime(env: dict[str, str] | None = None) -> bool:
    env = os.environ if env is None else env
    return bool(env.get("CLOUD_RUN_JOB") or env.get("K_SERVICE"))


def fetch_metadata_token(audience: str, *, opener: Callable[..., Any] | None = None, timeout: float = 5.0) -> str:
    """Token del servidor de metadatos. Pide ``format=full`` (agrega el claim ``email``) y, si
    el servidor no lo acepta, repite sin él: la confianza del rol no necesita el correo."""
    open_fn = opener or urllib.request.urlopen
    errors: list[str] = []
    for params in ({"audience": audience, "format": "full"}, {"audience": audience}):
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(f"{METADATA_IDENTITY_URL}?{query}", headers={"Metadata-Flavor": "Google"})
        try:
            with open_fn(request, timeout=timeout) as resp:
                token = resp.read().decode("utf-8").strip()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")
            continue
        if token.count(".") == 2:
            return token
        errors.append("no devolvió un JWT")
    raise IdentityError("servidor de metadatos: " + "; ".join(errors))


def fetch_impersonated_token(sa_email: str, audience: str) -> str:
    try:
        import google.auth
        from google.auth import impersonated_credentials
        from google.auth.transport.requests import Request
    except ImportError as exc:  # pragma: no cover - google-auth viene con google-cloud-storage
        raise IdentityError("falta google-auth") from exc
    try:
        source, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        target = impersonated_credentials.Credentials(
            source_credentials=source,
            target_principal=sa_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=600,
        )
        idt = impersonated_credentials.IDTokenCredentials(target, target_audience=audience, include_email=True)
        idt.refresh(Request())
    except Exception as exc:  # noqa: BLE001
        raise IdentityError(f"impersonación de {sa_email}: {type(exc).__name__}: {exc}") from exc
    if not idt.token:
        raise IdentityError("la impersonación no devolvió token")
    return idt.token


def fetch_gcloud_token(sa_email: str, audience: str, *, run: Callable[..., Any] = subprocess.run) -> str:
    cmd = [
        "gcloud",
        "auth",
        "print-identity-token",
        f"--impersonate-service-account={sa_email}",
        f"--audiences={audience}",
        "--include-email",
    ]
    try:
        proc = run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)
    except Exception as exc:  # noqa: BLE001
        raise IdentityError(f"gcloud: {type(exc).__name__}: {exc}") from exc
    lines = [ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip().count(".") == 2]
    if proc.returncode != 0 or not lines:
        raise IdentityError(f"gcloud print-identity-token falló: {(proc.stderr or '').strip()[-400:]}")
    return lines[-1]


def get_token(mode: str, audience: str, sa_email: str | None) -> tuple[str, str]:
    """Devuelve (token, camino). ``mode``: auto, metadata, impersonate o gcloud."""
    errors: list[str] = []
    if mode == "auto":
        order = ["metadata"] if on_gcp_runtime() else ["impersonate", "gcloud"]
    else:
        order = [mode]
    for path in order:
        try:
            if path == "metadata":
                return fetch_metadata_token(audience), path
            if not sa_email:
                raise IdentityError("falta el correo de sa-exporter (TA_EXPORTER_SA o GCP_PROJECT)")
            if path == "impersonate":
                return fetch_impersonated_token(sa_email, audience), path
            if path == "gcloud":
                return fetch_gcloud_token(sa_email, audience), path
            raise IdentityError(f"modo de token desconocido: {path}")
        except IdentityError as exc:
            errors.append(f"{path}: {exc}")
    raise IdentityError("; ".join(errors))


def decode_claims(token: str) -> dict[str, Any]:
    """Claims del JWT **sin verificar la firma** (solo para diagnosticar; AWS sí la verifica)."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except Exception as exc:  # noqa: BLE001
        raise IdentityError(f"el token no es un JWT legible: {exc}") from exc


def aws_condition_values(claims: dict[str, Any]) -> dict[str, str | None]:
    """Valores que AWS compara con la confianza del rol (ver docstring del módulo)."""
    aud = claims.get("aud")
    if isinstance(aud, list):
        aud = aud[0] if aud else None
    return {
        "accounts.google.com:aud": claims.get("azp") or aud,
        "accounts.google.com:oaud": aud,
        "accounts.google.com:sub": claims.get("sub"),
    }


def check_claims(
    claims: dict[str, Any],
    *,
    audience: str,
    unique_id: str | None = None,
    email: str | None = None,
    now: datetime | None = None,
) -> list[str]:
    problems: list[str] = []
    if claims.get("iss") not in GOOGLE_ISSUERS:
        problems.append(f"iss = {claims.get('iss')!r} (se esperaba accounts.google.com)")
    aud = claims.get("aud")
    if aud != audience:
        problems.append(f"aud = {aud!r} (se esperaba {audience!r})")
    if unique_id and str(claims.get("sub")) != str(unique_id):
        problems.append(f"sub = {claims.get('sub')!r} (uniqueId de sa-exporter = {unique_id})")
    azp = claims.get("azp")
    if azp is None:
        problems.append("el token no trae azp (AWS usará aud como accounts.google.com:aud)")
    elif unique_id:
        allowed = {str(v) for v in (unique_id, email) if v}
        if str(azp) not in allowed:
            problems.append(f"azp = {azp!r} no es sa-exporter ({sorted(allowed)})")
    elif email and not str(azp).isdigit() and str(azp) != email:
        # Sin el uniqueId solo se puede descartar un azp que no sea ni numérico ni el correo.
        problems.append(f"azp = {azp!r} no es sa-exporter ({email})")
    if email and claims.get("email") and claims.get("email") != email:
        problems.append(f"email = {claims.get('email')!r} (se esperaba {email})")
    exp = claims.get("exp")
    current = (now or datetime.now(timezone.utc)).timestamp()
    if isinstance(exp, (int, float)) and exp <= current:
        problems.append("el token ya expiró")
    return problems


def trust_conditions(trust_policy: dict[str, Any] | str) -> dict[str, Any]:
    """Principal federado y valores de las condiciones ``accounts.google.com:*`` del rol."""
    if isinstance(trust_policy, str):
        trust_policy = json.loads(urllib.parse.unquote(trust_policy))
    out: dict[str, Any] = {"federated": set(), "actions": set(), "conditions": {}}
    statements = trust_policy.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for st in statements:
        if st.get("Effect") != "Allow":
            continue
        fed = (st.get("Principal") or {}).get("Federated")
        for value in fed if isinstance(fed, list) else [fed]:
            if value:
                out["federated"].add(value)
        acts = st.get("Action")
        for value in acts if isinstance(acts, list) else [acts]:
            if value:
                out["actions"].add(value)
        for op, conds in (st.get("Condition") or {}).items():
            for key, values in conds.items():
                vals = values if isinstance(values, list) else [values]
                out["conditions"].setdefault(key, {"op": op, "values": set()})["values"].update(str(v) for v in vals)
    return out


def compare_with_trust(claims: dict[str, Any], trust_policy: dict[str, Any] | str) -> list[str]:
    """Qué condición de la confianza del rol fallaría con este token (lista vacía = la acepta)."""
    trust = trust_conditions(trust_policy)
    problems: list[str] = []
    if FEDERATED_PRINCIPAL not in trust["federated"]:
        problems.append(f"Principal.Federated = {sorted(trust['federated'])} (se esperaba {FEDERATED_PRINCIPAL})")
    if "sts:AssumeRoleWithWebIdentity" not in trust["actions"]:
        problems.append("la confianza no permite sts:AssumeRoleWithWebIdentity")
    values = aws_condition_values(claims)
    for key in ("accounts.google.com:aud", "accounts.google.com:oaud", "accounts.google.com:sub"):
        cond = trust["conditions"].get(key)
        if cond is None:
            problems.append(f"la confianza no fija {key} (cualquier token de Google podría asumir el rol)")
            continue
        if cond["op"] not in ("StringEquals", "ForAnyValue:StringEquals"):
            problems.append(f"{key} usa {cond['op']} (se esperaba StringEquals)")
        if values[key] is None or str(values[key]) not in cond["values"]:
            problems.append(f"{key}: el token trae {values[key]!r} y el rol acepta {sorted(cond['values'])}")
    return problems


@dataclass(frozen=True)
class AssumedRole:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime | None
    assumed_arn: str
    subject: str | None
    audience: str | None
    provider: str | None

    def public(self) -> dict[str, Any]:
        return {
            "assumed_arn": self.assumed_arn,
            "expiration": self.expiration.isoformat() if self.expiration else None,
            "subject": self.subject,
            "audience": self.audience,
            "provider": self.provider,
        }

    def boto3_session(self, region: str) -> Any:
        import boto3

        return boto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
            region_name=region,
        )


def session_name(run_id: str) -> str:
    name = _SESSION_RE.sub("-", f"ta-gcs-to-s3-{run_id}")
    return name[:64] if len(name) >= 2 else "ta-gcs-to-s3"


def unsigned_sts_client(region: str) -> Any:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client("sts", region_name=region, config=Config(signature_version=UNSIGNED, retries={"max_attempts": 3, "mode": "standard"}))


def assume_role(
    token: str,
    role_arn: str,
    *,
    run_id: str,
    region: str = "us-east-1",
    duration: int = 3600,
    sts_client: Any = None,
) -> AssumedRole:
    sts = sts_client or unsigned_sts_client(region)
    try:
        resp = sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=session_name(run_id),
            WebIdentityToken=token,
            DurationSeconds=duration,
        )
    except Exception as exc:  # noqa: BLE001
        raise IdentityError(f"AssumeRoleWithWebIdentity sobre {role_arn}: {type(exc).__name__}: {exc}") from exc
    creds = resp["Credentials"]
    return AssumedRole(
        access_key_id=creds["AccessKeyId"],
        secret_access_key=creds["SecretAccessKey"],
        session_token=creds["SessionToken"],
        expiration=creds.get("Expiration"),
        assumed_arn=(resp.get("AssumedRoleUser") or {}).get("Arn", ""),
        subject=resp.get("SubjectFromWebIdentityToken"),
        audience=resp.get("Audience"),
        provider=resp.get("Provider"),
    )
