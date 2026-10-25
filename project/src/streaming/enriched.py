"""Rama ``enriched`` del pipeline de streaming (Lote 2.2, HU-020, HU-021).

Se engancha a la ``PCollection`` ``unique`` que devuelve ``build_from_messages()`` del
Lote 2.1 (eventos válidos y únicos, §3.13):

    unique ─► EnrichAnonymize ─► [NLP, Lote 2.3] ─┬─► enriched/source=…/dt=…/hr=…/part-*.parquet
                                                   └─► BigQuery ta_stream.posts_enriched

* **Anonimización (HU-020):** ``author.user_id`` → ``user_hash`` (HMAC-SHA256 hex) con la llave
  de ``ta-hmac-key``; ``user_id`` y ``handle`` desaparecen. La llave se lee en el worker.
* **Normalización (HU-021):** ``likes``, ``shares`` y ``comments`` según §5.2.
* **Salidas:** Parquet con el esquema plano de ``posts_enriched`` (§5.4) y *streaming inserts*
  en BigQuery. Sin NLP todavía: sus columnas van en nulo hasta el Lote 2.3.

Integración (dos líneas en ``pipeline.py``)::

    from streaming.enriched import attach_enriched      # registra las opciones --enriched_*
    ...
    unique = build_from_messages(...)
    attach_enriched(unique)                             # lee las opciones del propio pipeline

Opciones propias (todas con valor por defecto; ``dataflow_job.sh`` no necesita cambios):

==============================  =============================================================
``--enriched_enabled``          ``true`` (``false`` apaga la rama)
``--enriched_root``             raíz del lake; por defecto la de ``raw/`` (``--output_root``)
                                o ``gs://{project}-ta-lake``
``--enriched_window_seconds``   ventana de archivos; por defecto la de ``raw/`` (300 / 60)
``--posts_enriched_table``      ``auto``: ``{project}:ta_stream.posts_enriched`` con
                                ``DataflowRunner``, apagado en local; ``none`` lo apaga
``--hmac_secret_name``          ``TA_HMAC_SECRET_NAME`` o ``ta-hmac-key``
``--hmac_secret_project``       por defecto el proyecto del job
``--hmac_key_env``              nombre de una variable con la llave (solo local/pruebas)
==============================  =============================================================
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Duration, Timestamp

from common.enriched_row import envelope_to_row, format_process_time
from common.hmac_key import KeySource, load_hmac_key
from common.parquet_rows import parse_ts, rows_to_parquet_bytes

LOG = logging.getLogger("streaming.enriched")

DEFAULT_PREFIX = "enriched"
DEFAULT_DATASET = "ta_stream"
DEFAULT_TABLE = "posts_enriched"
METRICS_NS = "enriched"


def _log(event: str, **fields: Any) -> None:
    LOG.info(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))


# --------------------------------------------------------------------------- #
# Opciones y configuración
# --------------------------------------------------------------------------- #


class EnrichedOptions(PipelineOptions):
    """Opciones de la rama enriched. Se registran al importar este módulo."""

    @classmethod
    def _add_argparse_args(cls, parser):  # noqa: D401 - API de Beam
        parser.add_argument("--enriched_enabled", default="true", help="true|false: activa la rama enriched")
        parser.add_argument("--enriched_root", default=None, help="Raíz del lake para enriched/ (gs://… o ruta local)")
        parser.add_argument("--enriched_window_seconds", type=int, default=None, help="Ventana de archivos Parquet (divisor de 3600)")
        parser.add_argument("--posts_enriched_table", default="auto", help="auto | none | [proyecto:]dataset.tabla")
        parser.add_argument("--hmac_secret_name", default=None, help="Secreto de Secret Manager con la llave HMAC")
        parser.add_argument("--hmac_secret_project", default=None, help="Proyecto del secreto (por defecto el del job)")
        parser.add_argument("--hmac_key_env", default=None, help="Variable de entorno con la llave (solo local)")


class EnrichedConfigError(ValueError):
    """Combinación de opciones inválida."""


@dataclass(frozen=True)
class EnrichedConfig:
    enabled: bool
    root: str | None
    prefix: str
    window_seconds: int
    bq_table: str | None
    key_source: KeySource
    runner: str

    def summary(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "root": self.root,
            "prefix": self.prefix,
            "window_seconds": self.window_seconds,
            "bq_table": self.bq_table,
            "hmac_key": self.key_source.describe(),
            "runner": self.runner,
        }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "si", "sí", "on")


def _first(values: Mapping[str, Any], *names: str) -> Any:
    for n in names:
        v = values.get(n)
        if v not in (None, ""):
            return v
    return None


def _bq_table_spec(value: str, project: str | None) -> str:
    v = value.strip()
    if ":" in v:
        return v
    parts = v.split(".")
    if len(parts) == 3:
        return f"{parts[0]}:{parts[1]}.{parts[2]}"
    if len(parts) == 2:
        if not project:
            raise EnrichedConfigError(f"--posts_enriched_table={value} necesita --project")
        return f"{project}:{parts[0]}.{parts[1]}"
    raise EnrichedConfigError(f"--posts_enriched_table inválida: {value!r}")


def resolve_enriched_config(
    options: PipelineOptions | None,
    environ: Mapping[str, str] | None = None,
    *,
    pipeline_values: Mapping[str, Any] | None = None,
) -> EnrichedConfig:
    """Calcula la configuración a partir de las opciones del pipeline (y del entorno).

    ``pipeline_values`` reemplaza a ``options.get_all_options()`` para leer opciones de
    otros módulos (``output_root``, ``raw_window_seconds`` del Lote 2.1); sirve en pruebas,
    donde no se puede registrar otra clase de opciones con los mismos nombres.
    """
    env = os.environ if environ is None else environ
    options = options or PipelineOptions([])
    eo = options.view_as(EnrichedOptions)
    all_opts = dict(options.get_all_options()) if pipeline_values is None else dict(pipeline_values)
    runner = str(options.view_as(StandardOptions).runner or "DirectRunner")
    is_dataflow = "dataflow" in runner.lower()
    project = options.view_as(GoogleCloudOptions).project or env.get("GCP_PROJECT") or None

    enabled = _truthy(eo.enriched_enabled)

    root = eo.enriched_root or _first(all_opts, "output_root", "lake_root")
    if not root and project:
        root = f"gs://{project}-ta-lake"
    root = str(root).rstrip("/") if root else None

    window = eo.enriched_window_seconds or _first(all_opts, "raw_window_seconds") or (300 if is_dataflow else 60)
    window = int(window)
    if window < 10 or 3600 % window != 0:
        raise EnrichedConfigError("--enriched_window_seconds debe ser ≥ 10 y divisor de 3600 (60, 300, 600, 900…)")

    table_opt = (eo.posts_enriched_table or "auto").strip()
    if table_opt.lower() in ("none", "off", "false", ""):
        bq_table = None
    elif table_opt.lower() == "auto":
        bq_table = f"{project}:{DEFAULT_DATASET}.{DEFAULT_TABLE}" if (is_dataflow and project) else None
    else:
        bq_table = _bq_table_spec(table_opt, project)

    cloud_output = bool(bq_table) or bool(root and root.startswith("gs://"))
    secret_name = eo.hmac_secret_name or env.get("TA_HMAC_SECRET_NAME") or "ta-hmac-key"
    secret_project = eo.hmac_secret_project or project
    if eo.hmac_key_env:
        if is_dataflow:
            raise EnrichedConfigError("--hmac_key_env no se admite con DataflowRunner: la llave sale de Secret Manager")
        key_source = KeySource("env", env_var=eo.hmac_key_env)
    elif is_dataflow or cloud_output:
        if not secret_project:
            raise EnrichedConfigError("hace falta --project (o --hmac_secret_project) para leer la llave HMAC")
        key_source = KeySource("secret", project=secret_project, secret_name=secret_name)
    else:
        key_source = KeySource("local_dev")

    if enabled and not root and not bq_table:
        if is_dataflow:
            raise EnrichedConfigError("la rama enriched no tiene salida: indique --enriched_root o --posts_enriched_table")
        # Fuera de Dataflow (p. ej. pruebas del Lote 2.1 con TestStream sin --output_root):
        # sin salida no hay nada que hacer, así que la rama se apaga en vez de fallar.
        _log("enriched_disabled", reason="sin --enriched_root, --output_root ni --project")
        enabled = False

    return EnrichedConfig(
        enabled=enabled,
        root=root,
        prefix=DEFAULT_PREFIX,
        window_seconds=window,
        bq_table=bq_table,
        key_source=key_source,
        runner=runner,
    )


# --------------------------------------------------------------------------- #
# Elemento de entrada → envelope
# --------------------------------------------------------------------------- #

_DICT_ATTRS = ("envelope", "event", "parsed", "record", "value")
_RAW_ATTRS = ("body", "data", "raw", "raw_body", "message", "payload_bytes")


def _parse(value: Any) -> dict | None:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    data = getattr(value, "data", None)  # PubsubMessage
    if isinstance(data, (bytes, bytearray, str)):
        return _parse(data)
    return None


def envelope_of(elem: Any) -> dict:
    """Devuelve el envelope (dict) de un elemento de ``unique``.

    Acepta un dict, el cuerpo JSON (bytes/str), un par ``(clave, valor)`` o un objeto con
    el envelope en ``envelope``/``event``/… o el cuerpo en ``body``/``data``/… (``ValidEvent``
    del Lote 2.1). Si no lo reconoce, lanza ``TypeError`` con los atributos que vio.
    """
    if isinstance(elem, Mapping):
        return dict(elem)
    if isinstance(elem, (bytes, bytearray, str)):
        parsed = _parse(elem)
        if parsed is not None:
            return parsed
    if isinstance(elem, tuple) and len(elem) == 2:
        return envelope_of(elem[1])
    for attr in _DICT_ATTRS + _RAW_ATTRS:
        value = getattr(elem, attr, None)
        if value is None:
            continue
        try:
            parsed = _parse(value)
        except (ValueError, UnicodeDecodeError):
            parsed = None
        if parsed is not None and "event_id" in parsed:
            return parsed
    attrs = sorted(a for a in dir(elem) if not a.startswith("_"))[:30]
    raise TypeError(f"elemento no reconocido: {type(elem).__name__} (atributos: {attrs})")


# --------------------------------------------------------------------------- #
# DoFns y transforms
# --------------------------------------------------------------------------- #


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EnrichFn(beam.DoFn):
    """Envelope válido → fila de ``posts_enriched`` anonimizada y normalizada."""

    def __init__(self, key_source: KeySource, clock: Callable[[], datetime] = _utcnow):
        super().__init__()
        self._source = key_source
        self._clock = clock
        self._key: str | None = None
        self._ok = Metrics.counter(METRICS_NS, "enriched_rows")
        self._errors = Metrics.counter(METRICS_NS, "enrich_errors")
        self._latency = Metrics.distribution(METRICS_NS, "event_to_process_ms")

    def setup(self):
        # La llave se lee aquí, en el worker, con la identidad del job (nunca en el launcher).
        self._key = load_hmac_key(self._source)
        _log("hmac_key_loaded", source=self._source.describe())

    def process(self, elem):
        try:
            env = envelope_of(elem)
            now = self._clock()
            row = envelope_to_row(env, self._key or "", format_process_time(now))
        except Exception as exc:  # noqa: BLE001 - se cuenta y se registra, no se reintenta sin fin
            self._errors.inc()
            _log("enrich_error", error=type(exc).__name__, detail=str(exc)[:500])
            return
        self._ok.inc()
        try:
            et = parse_ts(row["event_time"])
            self._latency.update(int((now - et).total_seconds() * 1000))
        except Exception:  # noqa: BLE001 - métrica informativa
            pass
        yield row


class _StampProcessingTime(beam.DoFn):
    """Re-marca cada fila con la hora de proceso (como ``raw/`` en el Lote 2.1)."""

    def process(self, row, ts=beam.DoFn.TimestampParam):
        now = Timestamp.of(time.time())
        yield TimestampedValue(row, max(now, ts) if isinstance(ts, Timestamp) else now)


def partition_path(root: str, prefix: str, source: str, window_start: datetime, pane_index: int) -> str:
    """``{root}/enriched/source={s}/dt=…/hr=…/part-{inicio}Z-p{panel}.parquet``."""
    ws = window_start.astimezone(timezone.utc)
    return (
        f"{root}/{prefix}/source={source}/dt={ws:%Y-%m-%d}/hr={ws:%H}/"
        f"part-{ws:%Y%m%dT%H%M%S}Z-p{pane_index:03d}.parquet"
    )


def write_bytes(path: str, data: bytes) -> None:
    """Escribe el archivo completo. En GCS el objeto aparece al cerrar; en local se renombra."""
    if "://" in path:
        from apache_beam.io.filesystems import FileSystems

        with FileSystems.create(path, mime_type="application/octet-stream", compression_type=CompressionTypes.UNCOMPRESSED) as fh:
            fh.write(data)
        return
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f".{os.path.basename(path)}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


class _WriteParquetFn(beam.DoFn):
    def __init__(self, root: str, prefix: str):
        super().__init__()
        self._root = root
        self._prefix = prefix
        self._files = Metrics.counter(METRICS_NS, "parquet_files")
        self._rows = Metrics.counter(METRICS_NS, "parquet_rows")

    def process(self, kv, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        source, rows = kv
        rows = list(rows)
        if not rows:
            return
        start = datetime.fromtimestamp(window.start.micros / 1_000_000, tz=timezone.utc)
        index = getattr(pane, "index", 0) or 0
        path = partition_path(self._root, self._prefix, source, start, index)
        write_bytes(path, rows_to_parquet_bytes(rows))
        self._files.inc()
        self._rows.inc(len(rows))
        _log("enriched_file_written", path=path, rows=len(rows))
        yield path


class WriteEnrichedParquet(beam.PTransform):
    """Filas → un Parquet por fuente y ventana de tiempo de proceso (nombre determinista)."""

    def __init__(self, root: str, prefix: str = DEFAULT_PREFIX, window_seconds: int = 300):
        super().__init__()
        self._root = root.rstrip("/")
        self._prefix = prefix
        self._window = int(window_seconds)

    def expand(self, rows):
        return (
            rows
            | "StampProcessTime" >> beam.ParDo(_StampProcessingTime())
            | "Window"
            >> beam.WindowInto(
                FixedWindows(self._window),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                allowed_lateness=Duration(seconds=self._window),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "KeyBySource" >> beam.Map(lambda r: (r["source"], r))
            | "GroupBySource" >> beam.GroupByKey()
            | "WriteParquet" >> beam.ParDo(_WriteParquetFn(self._root, self._prefix))
        )


def to_bq_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Fila para *streaming insert*: timestamps ISO, listas y ``payload`` como texto JSON."""
    out = dict(row)
    out["brands"] = list(out.get("brands") or [])
    out["hashtags"] = list(out.get("hashtags") or [])
    return out


class _LogFailedRows(beam.DoFn):
    def __init__(self):
        super().__init__()
        self._failed = Metrics.counter(METRICS_NS, "bq_failed_rows")

    def process(self, failed):
        self._failed.inc()
        try:
            _table, row, errors = failed
        except (TypeError, ValueError):
            row, errors = failed, None
        event_id = row.get("event_id") if isinstance(row, Mapping) else None
        _log("bq_insert_failed", event_id=event_id, errors=str(errors)[:1000])


class WritePostsEnrichedToBigQuery(beam.PTransform):
    """*Streaming inserts* en ``posts_enriched`` (tabla creada por Terraform, ``CREATE_NEVER``)."""

    def __init__(self, table: str):
        super().__init__()
        self._table = table

    def expand(self, rows):
        bq = beam.io.BigQueryDisposition
        result = (
            rows
            | "ToBqRow" >> beam.Map(to_bq_row)
            | "StreamingInsert"
            >> beam.io.WriteToBigQuery(
                table=self._table,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=bq.CREATE_NEVER,
                write_disposition=bq.WRITE_APPEND,
                insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
                ignore_unknown_columns=False,
            )
        )
        failed = getattr(result, "failed_rows_with_errors", None)
        if failed is not None:
            failed | "LogFailedRows" >> beam.ParDo(_LogFailedRows())
        return result


class EnrichedBranch(beam.PTransform):
    """``unique`` → filas enriquecidas → [NLP] → Parquet y BigQuery. Devuelve las filas."""

    def __init__(self, config: EnrichedConfig, nlp: beam.PTransform | None = None, clock: Callable[[], datetime] = _utcnow):
        super().__init__()
        self._cfg = config
        self._nlp = nlp
        self._clock = clock

    def expand(self, unique):
        rows = unique | "EnrichAnonymize" >> beam.ParDo(EnrichFn(self._cfg.key_source, self._clock))
        if self._nlp is not None:
            rows = rows | "NLP" >> self._nlp
        if self._cfg.root:
            rows | "WriteEnrichedParquet" >> WriteEnrichedParquet(self._cfg.root, self._cfg.prefix, self._cfg.window_seconds)
        if self._cfg.bq_table:
            rows | "WritePostsEnriched" >> WritePostsEnrichedToBigQuery(self._cfg.bq_table)
        return rows


def attach_enriched(
    unique,
    options: PipelineOptions | None = None,
    *,
    nlp: beam.PTransform | None = None,
    config: EnrichedConfig | None = None,
):
    """Engancha la rama enriched a ``unique``. Devuelve las filas o ``None`` si está apagada."""
    if config is None:
        opts = options if options is not None else getattr(unique.pipeline, "options", None)
        config = resolve_enriched_config(opts)
    _log("enriched_config", **config.summary())
    if not config.enabled:
        return None
    return unique | "Enriched" >> EnrichedBranch(config, nlp=nlp)
