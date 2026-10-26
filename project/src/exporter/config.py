"""Configuración del exportador desde variables de entorno (§5.1 y §5.6)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from typing import Mapping

from exporter.keys import SOURCES

DEFAULT_STATE_KEY = "export-state/watermark.json"
DEFAULT_AUDIENCE = "ta-gcp-exporter-role"
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_ROLE_NAME = "ta-gcp-exporter-role"

_ROLE_ARN_RE = re.compile(r"^arn:aws:iam::\d{12}:role/[\w+=,.@/-]+$")
_KMS_ARN_RE = re.compile(r"^arn:aws:kms:[a-z0-9-]+:\d{12}:key/(mrk-)?[0-9a-f-]+$")


class ConfigError(ValueError):
    """Falta una variable o tiene un valor inválido."""


@dataclass(frozen=True)
class ExporterConfig:
    gcp_project: str | None
    gcs_bucket: str
    s3_bucket: str
    kms_key_arn: str | None
    role_arn: str | None
    audience: str = DEFAULT_AUDIENCE
    aws_region: str = DEFAULT_AWS_REGION
    state_key: str = DEFAULT_STATE_KEY
    sa_email: str | None = None
    sources: tuple[str, ...] = SOURCES
    workers: int = 8
    max_objects: int = 5000
    overlap_seconds: int = 3600
    margin_seconds: int = 7200
    session_seconds: int = 3600
    extra: dict = field(default_factory=dict)

    def with_overrides(self, **changes) -> "ExporterConfig":
        clean = {k: v for k, v in changes.items() if v is not None}
        return replace(self, **clean)

    def public(self) -> dict:
        """Lo que se puede registrar (no hay secretos en la configuración)."""
        return {
            "gcp_project": self.gcp_project,
            "gcs_bucket": self.gcs_bucket,
            "s3_bucket": self.s3_bucket,
            "kms_key_arn": self.kms_key_arn,
            "role_arn": self.role_arn,
            "audience": self.audience,
            "aws_region": self.aws_region,
            "state_key": self.state_key,
            "sa_email": self.sa_email,
            "sources": list(self.sources),
            "workers": self.workers,
            "max_objects": self.max_objects,
            "overlap_seconds": self.overlap_seconds,
            "margin_seconds": self.margin_seconds,
        }


def _int(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    raw = env.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ConfigError(f"{name} debe ser un entero (vino {raw!r})") from exc
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} debe estar entre {minimum} y {maximum} (vino {value})")
    return value


def load_config(env: Mapping[str, str] | None = None, *, require_upload: bool = True) -> ExporterConfig:
    """Lee la configuración. ``require_upload`` exige lo necesario para subir a S3."""
    env = os.environ if env is None else env
    project = (env.get("GCP_PROJECT") or "").strip() or None
    gcs_bucket = (env.get("TA_GCS_LAKE_BUCKET") or "").strip()
    if not gcs_bucket and project:
        gcs_bucket = f"{project}-ta-lake"
    s3_bucket = (env.get("TA_LAKE_BUCKET") or "").strip()
    kms = (env.get("TA_LAKE_KMS_KEY_ARN") or "").strip() or None
    role = (env.get("TA_EXPORTER_ROLE_ARN") or "").strip() or None
    audience = (env.get("TA_EXPORTER_AUDIENCE") or "").strip() or DEFAULT_AUDIENCE
    region = (env.get("AWS_REGION") or "").strip() or DEFAULT_AWS_REGION
    state_key = (env.get("TA_EXPORT_STATE_KEY") or "").strip() or DEFAULT_STATE_KEY
    sa_email = (env.get("TA_EXPORTER_SA") or "").strip() or None
    if not sa_email and project:
        sa_email = f"sa-exporter@{project}.iam.gserviceaccount.com"

    sources_raw = (env.get("TA_EXPORT_SOURCES") or "").strip()
    sources = tuple(s.strip() for s in sources_raw.split(",") if s.strip()) if sources_raw else SOURCES
    bad = [s for s in sources if s not in SOURCES]
    if bad:
        raise ConfigError(f"TA_EXPORT_SOURCES con fuentes desconocidas: {bad} (válidas: {list(SOURCES)})")

    missing = []
    if not gcs_bucket:
        missing.append("TA_GCS_LAKE_BUCKET (o GCP_PROJECT)")
    if not s3_bucket:
        missing.append("TA_LAKE_BUCKET")
    if require_upload and not kms:
        missing.append("TA_LAKE_KMS_KEY_ARN")
    if missing:
        raise ConfigError("faltan variables: " + ", ".join(missing))
    if role and not _ROLE_ARN_RE.match(role):
        raise ConfigError(f"TA_EXPORTER_ROLE_ARN no parece un ARN de rol: {role!r}")
    if kms and not _KMS_ARN_RE.match(kms):
        raise ConfigError(f"TA_LAKE_KMS_KEY_ARN debe ser el ARN de la llave (no el alias): {kms!r}")
    if not state_key.startswith("export-state/"):
        raise ConfigError(f"TA_EXPORT_STATE_KEY debe quedar bajo export-state/ (vino {state_key!r})")

    return ExporterConfig(
        gcp_project=project,
        gcs_bucket=gcs_bucket,
        s3_bucket=s3_bucket,
        kms_key_arn=kms,
        role_arn=role,
        audience=audience,
        aws_region=region,
        state_key=state_key,
        sa_email=sa_email,
        sources=sources,
        # hasta 10: es el tamaño de los pools HTTP de boto3 y de google-cloud-storage
        workers=_int(env, "TA_EXPORT_WORKERS", 8, 1, 10),
        max_objects=_int(env, "TA_EXPORT_MAX_OBJECTS", 5000, 1, 100000),
        overlap_seconds=_int(env, "TA_EXPORT_OVERLAP_SECONDS", 3600, 0, 7 * 86400),
        margin_seconds=_int(env, "TA_EXPORT_MARGIN_SECONDS", 7200, 0, 7 * 86400),
        session_seconds=_int(env, "TA_EXPORTER_SESSION_SECONDS", 3600, 900, 43200),
    )
