"""Configuración del pipeline: opciones de línea de comandos y valores derivados.

Los nombres por defecto salen de §5.3 y §5.4; el proyecto, la región y el
entorno se leen de `GCP_PROJECT`, `GCP_REGION` y `ENV` (§5.1). No se define
ninguna variable de entorno nueva.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

KNOWN_SOURCES = ("reddit", "facebook", "instagram")
DEFAULT_SUBSCRIPTION_TEMPLATE = "ta-raw-{source}-df"
DEFAULT_DLQ_TOPIC = "ta-dlq"
DEFAULT_RAW_WINDOW_S = 300
DEFAULT_DEDUP_WINDOW_S = 3600
DEFAULT_ID_LABEL = "event_id"

DIRECT_RUNNERS = {None, "", "directrunner", "direct", "bundlebaseddirectrunner"}


class ConfigError(ValueError):
    """Opción inválida o incompleta."""


def is_direct_runner(runner: str | None) -> bool:
    return (runner or "").lower() in DIRECT_RUNNERS


def lake_bucket_name(project: str) -> str:
    """`{GCP_PROJECT}-ta-lake` (§5.4)."""
    return f"{project}-ta-lake"


def add_arguments(parser) -> None:
    """Argumentos propios; se registran como `PipelineOptions` (ver `pipeline.py`)."""
    parser.add_argument("--sources", default=",".join(KNOWN_SOURCES),
                        help="Fuentes a leer, separadas por coma.")
    parser.add_argument("--subscription_template", default=DEFAULT_SUBSCRIPTION_TEMPLATE,
                        help="Nombre de la suscripción por fuente (§5.3).")
    parser.add_argument("--dlq_topic", default=DEFAULT_DLQ_TOPIC,
                        help="Topic de la DLQ (§5.3). Vacío = no publicar en Pub/Sub.")
    parser.add_argument("--output_root", default=None,
                        help="Raíz del lake: gs://bucket o ruta local. "
                             "Por defecto gs://{GCP_PROJECT}-ta-lake.")
    parser.add_argument("--contracts_dir", default=None,
                        help="Carpeta contracts/. Por defecto project/contracts.")
    parser.add_argument("--raw_window_seconds", type=int, default=DEFAULT_RAW_WINDOW_S,
                        help="Cada cuánto se cierra un archivo de raw/ y dlq/ por partición.")
    parser.add_argument("--dedup_window_seconds", type=int, default=DEFAULT_DEDUP_WINDOW_S,
                        help="Tramo de tiempo de proceso dentro del cual se descartan copias "
                             "de un mismo event_id (por defecto 1 h).")
    parser.add_argument("--pubsub_id_label", default=DEFAULT_ID_LABEL,
                        help="Atributo de deduplicación de Pub/Sub en Dataflow (§5.3). "
                             "Se ignora con DirectRunner.")
    parser.add_argument("--dlq_table", default="",
                        help="Tabla de BigQuery para la DLQ (PROYECTO:ta_stream.dlq_events). "
                             "Vacío = no escribir (la tabla llega con el Lote 2.4).")


@dataclass(frozen=True)
class StreamConfig:
    project: str
    sources: tuple[str, ...]
    subscriptions: dict[str, str]
    dlq_topic: str | None
    output_root: str
    raw_window_seconds: int
    dedup_window_seconds: int
    id_label: str | None
    dlq_table: str | None
    direct: bool
    bundle: dict = field(default_factory=dict, repr=False)


def _full_path(kind: str, project: str, name: str) -> str:
    if name.startswith("projects/"):
        return name
    return f"projects/{project}/{kind}/{name}"


def resolve(*, project: str | None, runner: str | None, sources: str,
            subscription_template: str, dlq_topic: str | None, output_root: str | None,
            raw_window_seconds: int, dedup_window_seconds: int, pubsub_id_label: str | None,
            dlq_table: str | None, bundle: dict) -> StreamConfig:
    project = project or os.environ.get("GCP_PROJECT")
    if not project:
        raise ConfigError("falta el proyecto: use --project o la variable GCP_PROJECT (§5.1)")

    names = tuple(s.strip() for s in (sources or "").split(",") if s.strip())
    if not names:
        raise ConfigError("--sources está vacío")
    unknown = sorted(set(names) - set(KNOWN_SOURCES))
    if unknown:
        raise ConfigError(f"fuentes desconocidas: {', '.join(unknown)} (válidas: {', '.join(KNOWN_SOURCES)})")
    if len(set(names)) != len(names):
        raise ConfigError("--sources tiene fuentes repetidas")
    if "{source}" not in subscription_template:
        raise ConfigError("--subscription_template debe contener {source}")
    if raw_window_seconds < 10:
        raise ConfigError("--raw_window_seconds debe ser ≥ 10")
    if dedup_window_seconds < 60:
        raise ConfigError("--dedup_window_seconds debe ser ≥ 60")

    direct = is_direct_runner(runner)
    root = (output_root or f"gs://{lake_bucket_name(project)}").rstrip("/")
    if direct and root.startswith("gs://") and os.environ.get("PUBSUB_EMULATOR_HOST"):
        # Con el emulador no hay credenciales: escribir en GCS fallaría a mitad de camino.
        raise ConfigError("con PUBSUB_EMULATOR_HOST use un --output_root local (p. ej. /lake)")

    return StreamConfig(
        project=project,
        sources=names,
        subscriptions={s: _full_path("subscriptions", project, subscription_template.format(source=s))
                       for s in names},
        dlq_topic=_full_path("topics", project, dlq_topic) if dlq_topic else None,
        output_root=root,
        raw_window_seconds=raw_window_seconds,
        dedup_window_seconds=dedup_window_seconds,
        # Python DirectRunner no admite id_label (lanza NotImplementedError).
        id_label=None if direct else (pubsub_id_label or None),
        dlq_table=dlq_table or None,
        direct=direct,
        bundle=bundle,
    )
