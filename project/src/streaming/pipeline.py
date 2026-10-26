"""Pipeline de streaming · Lote 2.1 (HU-018, HU-019).

    ta-raw-{source}-df ─► Validate ─┬─► Dedup(event_id) ─► raw/source=…/dt=…/hr=…/part-*.jsonl.gz
                                    └─► DLQ ─┬─► ta-dlq (Pub/Sub)
                                             ├─► dlq/dt=…/part-*.jsonl.gz
                                             └─► ta_stream.dlq_events (opcional, Lote 2.4)

Uso local (emulador, sin credenciales):

    python -m streaming.pipeline --runner DirectRunner --output_root /lake

En GCP lo lanza `scripts/dataflow_job.sh start` (DataflowRunner).
"""

from __future__ import annotations

from streaming.enriched import attach_enriched
from streaming.trends import attach_trends 

import json
import logging
import sys
import time

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from streaming import PIPELINE_VERSION
from streaming.config import ConfigError, StreamConfig, add_arguments, resolve
from streaming.records import dlq_partition, raw_partition
from streaming.schemas import load_bundle
from streaming.transforms import (
    DLQ_TAG,
    DedupByEventId,
    ValidateFn,
    WritePartitionedJsonl,
    stamp_with_process_time,
    to_dlq_pubsub_message,
)

LOG = logging.getLogger("streaming")


class TrendStreamOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        add_arguments(parser)


def build_from_messages(messages, cfg: StreamConfig) -> dict:
    """Arma el pipeline a partir de una PCollection de `(source, PubsubMessage)`.

    Separado de la lectura para poder probarlo con `TestStream`. Devuelve las
    PCollections intermedias; el Lote 2.2 se engancha en `unique`.
    """
    checked = messages | "Validate" >> beam.ParDo(ValidateFn(cfg.bundle)).with_outputs(
        DLQ_TAG, main="valid")
    unique = checked.valid | "Dedup" >> DedupByEventId(cfg.dedup_window_seconds)
    enriched_rows = attach_enriched(unique) 
    attach_trends(enriched_rows)

    raw_files = (
        unique  # ya marcado con la hora de proceso por DedupByEventId
        | "RawLine" >> beam.Map(lambda e: (raw_partition(e.source, e.process_ts), e.body))
        | "WriteRaw" >> WritePartitionedJsonl(cfg.output_root, cfg.raw_window_seconds)
    )

    dlq = checked[DLQ_TAG]
    dlq_files = (
        dlq
        | "DlqProcessTime" >> beam.Map(stamp_with_process_time)
        | "DlqLine" >> beam.Map(lambda r: (dlq_partition(r.process_ts), r.line()))
        | "WriteDlqFiles" >> WritePartitionedJsonl(cfg.output_root, cfg.raw_window_seconds)
    )

    outputs = {"valid": checked.valid, "unique": unique, "dlq": dlq,
               "raw_files": raw_files, "dlq_files": dlq_files}

    if cfg.dlq_topic:
        outputs["dlq_messages"] = dlq | "DlqMessage" >> beam.Map(to_dlq_pubsub_message)
        outputs["dlq_messages"] | "PublishDlq" >> WriteToPubSub(cfg.dlq_topic, with_attributes=True)

    if cfg.dlq_table:
        from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

        (dlq
         | "DlqRow" >> beam.Map(lambda r: r.row())
         | "WriteDlqTable" >> WriteToBigQuery(
             cfg.dlq_table,
             method=WriteToBigQuery.Method.STREAMING_INSERTS,
             create_disposition=BigQueryDisposition.CREATE_NEVER,
             write_disposition=BigQueryDisposition.WRITE_APPEND))
    return outputs


def build_pipeline(pipeline, cfg: StreamConfig) -> dict:
    reads = []
    for source in cfg.sources:
        msgs = pipeline | f"Read[{source}]" >> ReadFromPubSub(
            subscription=cfg.subscriptions[source], with_attributes=True, id_label=cfg.id_label)
        reads.append(msgs | f"Tag[{source}]" >> beam.Map(lambda m, s=source: (s, m)))
    merged = reads[0] if len(reads) == 1 else tuple(reads) | "Merge" >> beam.Flatten()
    return build_from_messages(merged, cfg)


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        message = record.getMessage()
        try:
            body = json.loads(message)
            if not isinstance(body, dict):
                raise ValueError
        except ValueError:
            body = {"message": message}
        return json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                           "severity": record.levelname, "logger": record.name, **body},
                          ensure_ascii=False)


def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


def make_options(argv=None) -> tuple[PipelineOptions, StreamConfig]:
    options = PipelineOptions(argv)
    std = options.view_as(StandardOptions)
    gcp = options.view_as(GoogleCloudOptions)
    own = options.view_as(TrendStreamOptions)

    std.streaming = True
    if not gcp.project:
        import os

        gcp.project = os.environ.get("GCP_PROJECT")
        if not gcp.region and os.environ.get("GCP_REGION"):
            gcp.region = os.environ["GCP_REGION"]

    cfg = resolve(project=gcp.project, runner=std.runner, sources=own.sources,
                  subscription_template=own.subscription_template, dlq_topic=own.dlq_topic,
                  output_root=own.output_root, raw_window_seconds=own.raw_window_seconds,
                  dedup_window_seconds=own.dedup_window_seconds, pubsub_id_label=own.pubsub_id_label,
                  dlq_table=own.dlq_table, bundle=load_bundle(own.contracts_dir))

    if cfg.direct:
        # El DirectRunner de Python intenta primero Prism, que no admite los timers de
        # tiempo de proceso de la deduplicación y descargaría un binario. Se usa la
        # implementación clásica en streaming, que sí lee del emulador.
        std.runner = "BundleBasedDirectRunner"
    else:
        if not gcp.temp_location:
            gcp.temp_location = f"{cfg.output_root}/_dataflow/temp"
        if not gcp.staging_location:
            gcp.staging_location = f"{cfg.output_root}/_dataflow/staging"
    options.view_as(SetupOptions).save_main_session = False
    return options, cfg


def run(argv=None) -> int:
    setup_logging()
    try:
        options, cfg = make_options(argv)
    except (ConfigError, FileNotFoundError) as exc:
        LOG.error(json.dumps({"event": "config_error", "error": str(exc)}, ensure_ascii=False))
        return 2

    LOG.info(json.dumps({
        "event": "pipeline_start", "version": PIPELINE_VERSION,
        "runner": options.view_as(StandardOptions).runner, "project": cfg.project,
        "subscriptions": cfg.subscriptions, "dlq_topic": cfg.dlq_topic,
        "dlq_table": cfg.dlq_table, "output_root": cfg.output_root,
        "raw_window_seconds": cfg.raw_window_seconds, "dedup_window_seconds": cfg.dedup_window_seconds,
        "id_label": cfg.id_label, "schemas": sorted(cfg.bundle),
    }, ensure_ascii=False))

    pipeline = beam.Pipeline(options=options)
    build_pipeline(pipeline, cfg)
    result = pipeline.run()
    if cfg.direct:
        result.wait_until_finish()
    else:
        job_id = getattr(result, "job_id", lambda: None)()
        LOG.info(json.dumps({"event": "dataflow_submitted", "job_id": job_id,
                             "job_name": options.view_as(GoogleCloudOptions).job_name}))
    return 0


if __name__ == "__main__":
    sys.exit(run(sys.argv[1:]))
