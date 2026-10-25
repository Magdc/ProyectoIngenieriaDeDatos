"""Transformaciones de Beam del Lote 2.1."""

from __future__ import annotations

import gzip
import json
import logging
import typing
import uuid
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.transforms import userstate, window
from apache_beam.utils.timestamp import Duration

from streaming.records import DlqRecord, ValidEvent
from streaming.validate import Validators

LOG = logging.getLogger("streaming")
DLQ_TAG = "dlq"
NAMESPACE = "ta_streaming"


class ValidateFn(beam.DoFn):
    """`(source, PubsubMessage)` → `ValidEvent` (salida principal) o `DlqRecord` (etiqueta `dlq`)."""

    def __init__(self, bundle: dict):
        super().__init__()
        self._bundle = bundle
        self._validators: Validators | None = None
        self._counters: dict = {}

    def setup(self):
        self._validators = Validators(self._bundle)

    def _inc(self, name: str) -> None:
        counter = self._counters.get(name)
        if counter is None:
            counter = self._counters[name] = Metrics.counter(NAMESPACE, name)
        counter.inc()

    def process(self, element):
        source, message = element
        result = self._validators.check(source, message.data, message.attributes)
        if isinstance(result, ValidEvent):
            self._inc(f"valid_{source}")
            yield result
        else:
            self._inc(f"dlq_{result.error_type}")
            yield beam.pvalue.TaggedOutput(DLQ_TAG, result)


class _DedupFn(beam.DoFn):
    """Deja pasar la primera copia de cada `event_id` dentro de su ventana.

    Sin timers a propósito: en Dataflow un timer de tiempo de proceso retiene la
    marca de agua en la hora del elemento hasta que dispara (``hold_timestamp``),
    y eso atrasaba el cierre de las ventanas de `raw/` tanto como el TTL. El
    estado vive en la ventana fija de deduplicación y el runner lo borra al
    cerrarla, sin retener la marca de agua.
    """

    SEEN = userstate.ReadModifyWriteStateSpec("seen", BooleanCoder())

    def __init__(self):
        super().__init__()
        self._duplicates = Metrics.counter(NAMESPACE, "duplicates_dropped")
        self._unique = Metrics.counter(NAMESPACE, "unique_events")

    def process(self, element, seen=beam.DoFn.StateParam(SEEN)):
        _, event = element
        if seen.read():
            self._duplicates.inc()
            return
        seen.write(True)
        self._unique.inc()
        yield event


class DedupByEventId(beam.PTransform):
    """Deduplicación por `event_id` en ventanas fijas de tiempo de proceso.

    Complementa a `id_label` de Pub/Sub (solo en Dataflow): cubre el
    DirectRunner y las copias que llegan después de la ventana de Pub/Sub, como
    las de un reinicio del simulador (§3.6, §3.11), siempre que las dos copias
    caigan en la misma ventana (por defecto 1 h). Trusted vuelve a deduplicar.
    La salida queda marcada con la hora de proceso (lo que necesitan `raw/` y
    sus particiones, §5.4).
    """

    def __init__(self, window_seconds: int):
        super().__init__()
        self._window = window_seconds

    def expand(self, events):
        return (
            events
            | "ProcessTime" >> beam.Map(stamp_with_process_time)
            | "DedupWindow" >> beam.WindowInto(window.FixedWindows(self._window))
            | "KeyByEventId" >> beam.Map(lambda e: (e.event_id, e)).with_output_types(
                typing.Tuple[str, typing.Any])
            | "FirstCopy" >> beam.ParDo(_DedupFn()).with_input_types(
                typing.Tuple[str, typing.Any])
        )


def stamp_with_process_time(record):
    """Re-marca el elemento con su hora de proceso.

    Las particiones de `raw/` y `dlq/` usan fecha/hora de proceso (§5.4), así que
    las ventanas también. Además evita que, al ponerse al día con un backlog,
    el DirectRunner (cuya marca de agua es la hora actual) descarte como tardíos
    los mensajes publicados antes de arrancar.
    """
    return window.TimestampedValue(record, record.process_ts)


class _WriteGroupFn(beam.DoFn):
    """Escribe un `part-*.jsonl.gz` por partición, ventana y panel."""

    def __init__(self, root: str):
        super().__init__()
        self._root = root.rstrip("/")
        self._files = Metrics.counter(NAMESPACE, "files_written")
        self._lines = Metrics.counter(NAMESPACE, "lines_written")

    def process(self, element, win=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        partition, lines = element
        lines = sorted(lines)
        start = datetime.fromtimestamp(float(win.start), tz=timezone.utc)
        index = pane.index if pane is not None else 0
        name = f"part-{start:%Y%m%dT%H%M%S}Z-p{index:03d}.jsonl.gz"
        path = f"{self._root}/{partition}/{name}"
        payload = gzip.compress(("\n".join(lines) + "\n").encode("utf-8"), mtime=0)
        write_atomically(path, payload)
        self._files.inc()
        self._lines.inc(len(lines))
        LOG.info(json.dumps({"event": "file_written", "path": path, "lines": len(lines)}))
        yield path, len(lines)


def write_atomically(path: str, payload: bytes) -> None:
    """GCS: un objeto aparece completo al cerrarse. Local: archivo temporal y `rename`."""
    if path.startswith("gs://"):
        with FileSystems.create(path, mime_type="application/gzip",
                                compression_type=CompressionTypes.UNCOMPRESSED) as fh:
            fh.write(payload)
        return
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:8]}"
    with FileSystems.create(tmp, compression_type=CompressionTypes.UNCOMPRESSED) as fh:
        fh.write(payload)
    if FileSystems.exists(path):
        FileSystems.delete([path])
    FileSystems.rename([tmp], [path])


class WritePartitionedJsonl(beam.PTransform):
    """`(partición, línea)` → archivos `{root}/{partición}/part-*.jsonl.gz`.

    Una ventana fija de `window_seconds` por partición: con la cadencia del
    simulador (~0,5 msg/s por fuente) y 300 s salen unos 12 archivos por hora y
    fuente. Reescribir la misma ventana genera el mismo nombre y el mismo
    contenido, así que un reintento no duplica datos.
    """

    def __init__(self, root: str, window_seconds: int):
        super().__init__()
        self._root = root
        self._window = window_seconds

    def expand(self, keyed_lines):
        return (
            keyed_lines
            | "Window" >> beam.WindowInto(
                window.FixedWindows(self._window),
                allowed_lateness=Duration(seconds=self._window))
            | "GroupByPartition" >> beam.GroupByKey()
            | "WriteFiles" >> beam.ParDo(_WriteGroupFn(self._root))
        )


def to_dlq_pubsub_message(record: DlqRecord):
    from apache_beam.io.gcp.pubsub import PubsubMessage

    return PubsubMessage(data=record.data, attributes=record.pubsub_attributes())
