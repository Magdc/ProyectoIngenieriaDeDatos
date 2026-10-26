"""Glue job ``ta-job-raw-to-trusted`` · Raw → Trusted (Lote 4.2, HU-026; ARCHITECTURE.md §3.21, ADR-25).

Reemplaza al marcador de posición del Lote 4.1. Terraform lo sube a
``s3://{lake}/_glue/scripts/ta-job-raw-to-trusted.py``; corre en Glue **Python shell 3.9** con el
*analytics library set* y solo usa la biblioteca estándar, boto3/botocore y pyarrow.

Qué hace en cada corrida (reconstruye Trusted entero desde Raw; mismo Raw ⇒ mismos bytes):

1. Lista y lee en paralelo todos los Parquet de ``raw/origin=stream/`` y ``raw/origin=historical/``
   directamente de S3 (el rol no tiene Athena). Un Parquet sin el esquema de §5.7 detiene el job.
2. Corrige tipos al contrato (marcas de tiempo ``timestamp[ms, UTC]``, enteros ``int64``, …).
3. Descarta las filas sin ``event_id``/``event_time``, con ``source`` fuera de los tres valores o con
   ``dt`` anterior al inicio de la *partition projection*, y los centinelas de los verificadores
   (``generator`` que empieza por ``ta-verify@``).
4. Deduplica por ``event_id``: se queda con la fila de menor ``process_time`` (empate: la primera en
   el orden de las claves de Raw).
5. Conserva las filas sin NLP (columnas de NLP en nulo) y las cuenta.
6. Calcula ``text_clean`` (regla ``text-clean@1``, §5.7) y escribe un Parquet + Snappy por partición en
   ``trusted/posts/source={s}/dt={AAAA-MM-DD}/part-00000.parquet`` con las 24 columnas de
   ``contracts/athena/posts.sql`` (sin ``source`` ni ``dt``, que van en la ruta). Sobrescribir un objeto
   de S3 es atómico; al final borra lo que sobre bajo ``trusted/posts/`` (idempotente). Nunca escribe
   fuera de ``trusted/posts/`` y ``quality/`` (``trusted/_ta_placeholder/`` del 4.1 queda intacto).
7. Deja el reporte de la corrida en ``quality/{AAAA-MM-DD}/raw_to_trusted/{corrida}.json`` y en
   ``quality/_latest/raw_to_trusted.json`` (conteos de leídas, descartadas, centinelas, duplicadas,
   sin NLP, escritas y particiones), y una línea JSON ``glue_job_done`` en
   ``/aws-glue/python-jobs/output``. Dentro del workflow deja ``ta_trusted_report``,
   ``ta_trusted_rows`` y ``ta_trusted_partitions_changed`` en las propiedades de la corrida.

Como el marcador del 4.1, falla a propósito (antes de escribir nada) si ``ta_force_fail`` (propiedad de
la corrida o argumento) nombra su clave, su nombre o ``all``.

En local (sin nube) lee un árbol con el layout del bucket y escribe en otra carpeta fuera del repo:

    python project/src/lakehouse/glue_jobs/raw_to_trusted.py --lake_bucket ta-dev-648617469176-lake \
        --ta_local_src ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake \
        --ta_local_dst ~/.cache/trendanalyzer/raw_to_trusted_local

Compatible con Python 3.9 y pyarrow ≥ 2 (Glue trae el de awswrangler 2.15.1: < 7.1).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

JOB_KEY = "raw_to_trusted"
JOB_NAME = "ta-job-raw-to-trusted"
JOB_VERSION = "raw-to-trusted@1.0"
REPORT_SCHEMA = "ta-raw-to-trusted-report@1"
TEXT_CLEAN_RULE = "text-clean@1"
LOTE = "4.2"

DEFAULTS = {
    "ta_input_prefix": "raw/",
    "ta_output_prefix": "trusted/",
    "ta_report_prefix": "quality/",
    "ta_region": "",
    "ta_force_fail": "",
    "ta_workers": "32",
    "ta_dry_run": "false",
    "ta_local_src": "",
    "ta_local_dst": "",
}

ORIGINS = ("stream", "historical")
SOURCES = ("reddit", "facebook", "instagram")
#: Primer día de ``projection.dt.range`` de ``ta_trusted.posts``: Athena no ve particiones anteriores.
PROJECTION_START = "2026-01-01"
#: ``generator`` de los eventos centinela de los verificadores (``verify_streaming.py``, §3.13).
SENTINEL_PREFIX = "ta-verify@"
NLP_COLUMNS = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version")
#: Desde aquí todo el stream trae NLP (§3.20, DQ-13): una fila sin NLP posterior es una anomalía.
NLP_SINCE = "2026-10-22T23:00:00Z"
TABLE_DIR = "posts/"
PART_FILE = "part-00000.parquet"
REPORT_DIR = "raw_to_trusted/"
LATEST_REPORT = "_latest/raw_to_trusted.json"
MS_PER_DAY = 86400000
_NO_TIME = 2 ** 62  # process_time nulo: pierde frente a cualquier copia con hora

RAW_KEY_RE = re.compile(
    r"^(?P<prefix>.*)origin=(?P<origin>[^/]+)/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hr=(?P<hr>\d{2})/[^/]+\.parquet$"
)

# Columnas de Raw (= posts_enriched, §5.5) y de Trusted (= contracts/athena/posts.sql), en orden.
# (nombre, tipo lógico, admite nulos). Las pruebas las comparan con common.parquet_rows,
# lakehouse.catalog_spec y quality.layers.
RAW_COLUMNS = (
    ("event_id", "string", False),
    ("origin", "string", True),
    ("source", "string", True),
    ("generator", "string", True),
    ("event_time", "timestamp", False),
    ("ingest_time", "timestamp", True),
    ("process_time", "timestamp", True),
    ("user_hash", "string", True),
    ("followers", "int64", True),
    ("text", "string", True),
    ("lang", "string", True),
    ("brands", "list", True),
    ("brand_primary", "string", True),
    ("hashtags", "list", True),
    ("likes", "int64", True),
    ("shares", "int64", True),
    ("comments", "int64", True),
    ("sentiment_label", "string", True),
    ("sentiment_score", "double", True),
    ("emotion_label", "string", True),
    ("emotion_score", "double", True),
    ("label_sentiment", "string", True),
    ("nlp_model_version", "string", True),
    ("payload", "string", True),
)


def _trusted_columns():
    out = []
    for name, kind, nullable in RAW_COLUMNS:
        if name == "source":
            continue
        out.append((name, kind, nullable))
        if name == "text":
            out.append(("text_clean", "string", True))
    return tuple(out)


TRUSTED_COLUMNS = _trusted_columns()
RAW_NAMES = tuple(c[0] for c in RAW_COLUMNS)
TRUSTED_NAMES = tuple(c[0] for c in TRUSTED_COLUMNS)


class RawSchemaError(RuntimeError):
    """Un Parquet de Raw no tiene (ni se puede llevar a) el esquema de §5.7."""


# --- argumentos y logs ------------------------------------------------------------------------------


def parse_args(argv):
    """``--clave valor`` o ``--clave=valor`` (como los pasa Glue) → dict sin los ``--``."""
    out = dict(DEFAULTS)
    i = 0
    while i < len(argv):
        token = argv[i]
        if token.startswith("--"):
            key = token[2:]
            if "=" in key:
                key, value = key.split("=", 1)
            elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                value = argv[i + 1]
                i += 1
            else:
                value = "true"
            out[key] = value
        i += 1
    return out


def truthy(value):
    return str(value or "").strip().lower() in ("1", "true", "yes", "si", "sí")


def now_iso(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log(event, **fields):
    record = {"time": now_iso(), "event": event, "job": JOB_NAME, "lote": LOTE, "version": JOB_VERSION}
    record.update(fields)
    print(json.dumps(record, ensure_ascii=False, sort_keys=True, default=str), flush=True)
    return record


def wants_failure(value):
    names = {v.strip() for v in str(value or "").split(",") if v.strip()}
    return bool(names & {JOB_KEY, JOB_NAME, "all", "true", "1"})


def peak_rss_mb():
    try:
        import resource

        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)
    except Exception:  # noqa: BLE001 - solo informativo (no existe en Windows)
        return None


def library_versions():
    out = {"python": sys.version.split()[0]}
    for mod in ("pyarrow", "boto3", "botocore"):
        try:
            out[mod] = __import__(mod).__version__
        except Exception:  # noqa: BLE001 - solo informativo
            out[mod] = None
    return out


# --- almacenes: S3 (Glue) o una carpeta con el layout del bucket (local) ----------------------------


class S3Store:
    """Lo mínimo de S3 que usa el job, sobre un cliente de boto3."""

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def uri(self, key):
        return "s3://%s/%s" % (self.bucket, key)

    def list(self, prefix):
        out = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                out.append({"key": obj["Key"], "size": int(obj.get("Size") or 0)})
        return out

    def get(self, key):
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put(self, key, data, content_type):
        # Sin cabeceras SSE: el bucket cifra con SSE-KMS (llave del lake y bucket key, §5.7).
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def delete(self, keys):
        deleted = 0
        keys = list(keys)
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            resp = self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            errors = resp.get("Errors") or []
            if errors:
                raise RuntimeError("no se pudieron borrar %d objetos (p. ej. %s: %s)"
                                   % (len(errors), errors[0].get("Key"), errors[0].get("Code")))
            deleted += len(batch)
        return deleted


class DirStore:
    """Una carpeta con el layout del bucket (``raw/…``, ``trusted/…``): para correr el job en local."""

    def __init__(self, root, bucket="local"):
        self.root = os.path.abspath(os.path.expanduser(root))
        self.bucket = bucket

    def _path(self, key):
        path = os.path.normpath(os.path.join(self.root, key))
        if not (path == self.root or path.startswith(self.root + os.sep)):
            raise ValueError("clave fuera de la carpeta: %s" % key)
        return path

    def uri(self, key):
        return "file://" + self._path(key)

    def list(self, prefix):
        out = []
        base = self._path(prefix.rsplit("/", 1)[0]) if "/" in prefix else self.root
        if not os.path.isdir(base):
            return out
        for folder, _dirs, files in os.walk(base):
            for name in files:
                full = os.path.join(folder, name)
                key = os.path.relpath(full, self.root).replace(os.sep, "/")
                if key.startswith(prefix) and not name.endswith(".part"):
                    out.append({"key": key, "size": os.path.getsize(full)})
        out.sort(key=lambda o: o["key"])
        return out

    def get(self, key):
        with open(self._path(key), "rb") as fh:
            return fh.read()

    def put(self, key, data, content_type):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".part"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)

    def delete(self, keys):
        n = 0
        for key in keys:
            try:
                os.remove(self._path(key))
                n += 1
            except FileNotFoundError:
                pass
        return n


def inside_git_repo(path):
    path = os.path.abspath(os.path.expanduser(path))
    while True:
        if os.path.exists(os.path.join(path, ".git")):
            return True
        parent = os.path.dirname(path)
        if parent == path:
            return False
        path = parent


# --- esquema y tipos --------------------------------------------------------------------------------


def arrow_type(kind):
    import pyarrow as pa

    if kind == "string":
        return pa.string()
    if kind == "timestamp":
        return pa.timestamp("ms", tz="UTC")
    if kind == "int64":
        return pa.int64()
    if kind == "double":
        return pa.float64()
    if kind == "list":
        return pa.list_(pa.field("element", pa.string()))
    raise ValueError(kind)


def raw_schema():
    """Esquema interno de Raw (todas las columnas admiten nulos: las filas sin clave se descartan)."""
    import pyarrow as pa

    return pa.schema([pa.field(n, arrow_type(k), nullable=True) for n, k, _ in RAW_COLUMNS])


def trusted_schema():
    """Esquema de los Parquet de Trusted (= ``quality.testing.trusted_arrow_schema()``)."""
    import pyarrow as pa

    return pa.schema([pa.field(n, arrow_type(k), nullable=nul) for n, k, nul in TRUSTED_COLUMNS])


def _coerce(col, kind, target, key, name):
    import pyarrow as pa

    t = col.type
    try:
        if kind == "timestamp":
            if not pa.types.is_timestamp(t):
                raise TypeError("no es una marca de tiempo")
            if t.tz is None:  # sin zona = UTC (§5.1)
                col = col.cast(pa.timestamp(t.unit, tz="UTC"))
            return col.cast(target, safe=False)  # us/ns → ms (trunca lo que pase del milisegundo)
        if kind == "int64":
            if not (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_null(t)):
                raise TypeError("no es numérico")
            return col.cast(target)
        if kind == "double":
            if not (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t) or pa.types.is_null(t)):
                raise TypeError("no es numérico")
            return col.cast(target)
        if kind == "string":
            if pa.types.is_dictionary(t):
                col = col.cast(t.value_type)
                t = col.type
            if not (pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_null(t)):
                raise TypeError("no es texto")
            return col.cast(target)
        if kind == "list":
            if not (pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_null(t)):
                raise TypeError("no es una lista")
            return col.cast(target)
    except Exception as exc:  # noqa: BLE001 - se informa con la clave y la columna
        raise RawSchemaError("%s: la columna %s es %s y no se puede llevar a %s (%s)" % (key, name, t, target, exc))
    raise RawSchemaError("%s: tipo lógico desconocido %s" % (key, kind))


def normalize_raw_table(table, key, schema=None):
    """Una tabla de un Parquet de Raw → las 24 columnas de Raw, en orden y con los tipos del contrato."""
    import pyarrow as pa

    schema = schema or raw_schema()
    names = table.column_names
    missing = [n for n in RAW_NAMES if n not in names]
    if missing:
        raise RawSchemaError("%s: faltan columnas %s" % (key, ", ".join(missing)))
    arrays = []
    for (name, kind, _), field in zip(RAW_COLUMNS, schema):
        col = table.column(name)
        if not col.type.equals(field.type):
            col = _coerce(col, kind, field.type, key, name)
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=schema)


def read_raw_object(data, key, schema=None):
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        table = pq.read_table(pa.BufferReader(data), use_threads=False)
    except Exception as exc:  # noqa: BLE001
        raise RawSchemaError("%s: no se puede leer como Parquet (%s: %s)" % (key, type(exc).__name__, exc))
    return normalize_raw_table(table, key, schema)


# --- texto limpio (text-clean@1, ARCHITECTURE.md §5.7) ----------------------------------------------

_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"(?<![\w@])@[\w@]+")
_HASHTAG_RE = re.compile(r"(?<![\w#])#[\w#]+")
_SPACE_BEFORE_CLOSE_RE = re.compile(r"(?<=\w)\s+([.,;:!?)\]}»…])")
_SPACE_AFTER_OPEN_RE = re.compile(r"([¿¡(\[{«])\s+(?=\w)")
_WS_RE = re.compile(r"\s+")
#: Categorías Unicode que se quitan: emojis y pictogramas (So), modificadores como los tonos de piel
#: (Sk), marcas que encierran como el keycap (Me), formato como el ZWJ (Cf) y no asignados o
#: privados (Cn, Co, Cs). Los controles (Cc: saltos de línea, tabuladores) pasan a espacio.
_DROP_CATEGORIES = frozenset(("So", "Sk", "Me", "Cf", "Cn", "Co", "Cs"))


def _is_variation_selector(ch):
    cp = ord(ch)
    return 0xFE00 <= cp <= 0xFE0F or 0xE0100 <= cp <= 0xE01EF


def _strip_symbols(text):
    out = []
    for ch in text:
        if ch.isascii():
            out.append(" " if ch in "\t\n\r\x0b\x0c" or ord(ch) < 32 or ord(ch) == 127 else ch)
            continue
        cat = unicodedata.category(ch)
        if cat == "Cc":
            out.append(" ")
        elif cat in _DROP_CATEGORIES or _is_variation_selector(ch):
            continue
        else:
            out.append(ch)
    return "".join(out)


def _lower_same_length(text):
    # str.lower() cambia el largo de unos pocos caracteres (p. ej. «İ» → «i̇»): esos se dejan igual.
    low = text.lower()
    if len(low) == len(text):
        return low
    return "".join(c.lower() if len(c.lower()) == 1 else c for c in text)


def _clean_once(s):
    nfc = unicodedata.normalize("NFC", s)
    if len(nfc) <= len(s):
        s = nfc
    s = _strip_symbols(s)
    s = _URL_RE.sub(" ", s)
    s = _MENTION_RE.sub(" ", s)
    s = _HASHTAG_RE.sub(" ", s)
    s = _lower_same_length(s)
    s = _WS_RE.sub(" ", s)
    s = _SPACE_BEFORE_CLOSE_RE.sub(r"\1", s)
    s = _SPACE_AFTER_OPEN_RE.sub(r"\1", s)
    return s.strip()


def clean_text(text):
    """``text`` → ``text_clean`` (regla ``text-clean@1``). ``None`` si ``text`` es nulo.

    1. Normaliza a Unicode NFC (si no alarga el texto).
    2. Quita emojis y otros símbolos pictográficos (categorías Unicode So, Sk, Me, Cf, Cn, Co, Cs y los
       selectores de variante); los caracteres de control pasan a espacio.
    3. Quita URLs (``http(s)://…``, ``www.…``), menciones (``@usuario``) y hashtags (``#Etiqueta``: ya
       están canónicos en la columna ``hashtags``).
    4. Pasa a minúsculas sin cambiar el largo; conserva tildes, ñ, dígitos y la puntuación.
    5. Colapsa los espacios, quita el que quede entre una palabra y ``. , ; : ! ? ) ] } » …`` y entre
       ``¿ ¡ ( [ { «`` y una palabra (los que dejó un emoji o un hashtag quitado) y recorta los extremos.

    Se repite hasta que el texto no cambia (con los textos del generador, una pasada), así que aplicarla
    dos veces da lo mismo. Ningún paso alarga el texto: nunca es más largo que ``text`` (DQ-57). Puede
    quedar vacío (``""``) si el texto solo tenía emojis o hashtags.
    """
    if text is None:
        return None
    s = str(text)
    for _ in range(8):
        cleaned = _clean_once(s)
        if cleaned == s:
            break
        s = cleaned
    return s


# --- selección de filas (lógica pura) ---------------------------------------------------------------

_DAY_CACHE = {}


def ms_to_dt(ms):
    """Milisegundos desde 1970 (UTC) → ``AAAA-MM-DD`` (``dt = DATE(event_time)`` UTC, §5.7)."""
    day = ms // MS_PER_DAY
    out = _DAY_CACHE.get(day)
    if out is None:
        out = (datetime(1970, 1, 1) + timedelta(days=day)).strftime("%Y-%m-%d")
        _DAY_CACHE[day] = out
    return out


def iso_to_ms(text):
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int((dt - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1000)


def select_rows(event_ids, event_ms, process_ms, generators, sources):
    """Qué filas pasan a Trusted y en qué partición. Listas paralelas (una entrada por fila de Raw).

    Devuelve ``{"partitions": {(source, dt): [índices ordenados por event_id]}, "rejected": {...},
    "sentinels": {...}, "duplicates": {...}, "duplicate_groups": {event_id: [índices]}}``. El orden de
    las filas de entrada desempata (el job lee Raw en el orden de sus claves).
    """
    rejected = {"null_event_id": 0, "null_event_time": 0, "invalid_source": 0, "dt_before_projection": 0}
    sentinel_rows, sentinel_ids, sentinel_generators = 0, set(), {}
    best, dup_ids, dup_rows = {}, set(), 0
    for i, eid in enumerate(event_ids):
        if eid is None:
            rejected["null_event_id"] += 1
            continue
        ems = event_ms[i]
        if ems is None:
            rejected["null_event_time"] += 1
            continue
        if sources[i] not in SOURCES:
            rejected["invalid_source"] += 1
            continue
        if ms_to_dt(ems) < PROJECTION_START:
            rejected["dt_before_projection"] += 1
            continue
        gen = generators[i]
        if gen is not None and gen.startswith(SENTINEL_PREFIX):
            sentinel_rows += 1
            sentinel_ids.add(eid)
            sentinel_generators[gen] = sentinel_generators.get(gen, 0) + 1
            continue
        pk = process_ms[i]
        rank = (_NO_TIME if pk is None else pk, i)
        current = best.get(eid)
        if current is None:
            best[eid] = rank
        else:
            dup_rows += 1
            dup_ids.add(eid)
            if rank < current:
                best[eid] = rank
    groups = {}
    if dup_ids:
        for i, eid in enumerate(event_ids):
            if eid in dup_ids and (generators[i] is None or not generators[i].startswith(SENTINEL_PREFIX)) \
                    and event_ms[i] is not None and sources[i] in SOURCES:
                groups.setdefault(eid, []).append(i)
    partitions = {}
    for eid, (_, i) in best.items():
        partitions.setdefault((sources[i], ms_to_dt(event_ms[i])), []).append(i)
    for idxs in partitions.values():
        idxs.sort(key=lambda j: event_ids[j])
    return {
        "partitions": partitions,
        "rejected": rejected,
        "sentinels": {"rows": sentinel_rows, "event_ids": len(sentinel_ids), "generators": sentinel_generators},
        "duplicates": {"rows_removed": dup_rows, "event_ids": len(dup_ids)},
        "duplicate_groups": groups,
    }


# --- el job -----------------------------------------------------------------------------------------


def partition_key(output_prefix, source, dt):
    return "%s%ssource=%s/dt=%s/%s" % (output_prefix, TABLE_DIR, source, dt, PART_FILE)


def parquet_bytes(table):
    import io

    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_dictionary=True, coerce_timestamps="ms",
                   allow_truncated_timestamps=False, use_deprecated_int96_timestamps=False, write_statistics=True)
    return buf.getvalue()


def build_partition(table, idxs, schema=None):
    """Filas ``idxs`` de la tabla de Raw → tabla de Trusted (sin ``source``; con ``text_clean``)."""
    import pyarrow as pa

    schema = schema or trusted_schema()
    sub = table.take(pa.array(idxs, type=pa.int64()))
    clean = [clean_text(t) for t in sub.column("text").to_pylist()]
    arrays = []
    for name in TRUSTED_NAMES:
        arrays.append(pa.array(clean, type=pa.string()) if name == "text_clean" else sub.column(name))
    return pa.Table.from_arrays(arrays, schema=schema), clean


def _ms_list(column):
    import pyarrow as pa

    return column.cast(pa.int64()).to_pylist()


def _duplicates_with_differences(table, groups):
    """Grupos de copias de un mismo ``event_id`` que difieren en algo más que ``process_time``."""
    import pyarrow as pa

    differing = 0
    for idxs in groups.values():
        sub = table.take(pa.array(idxs, type=pa.int64()))
        cols = {}
        for name in sub.column_names:
            if name == "process_time":
                continue
            col = sub.column(name)
            # Las marcas de tiempo se comparan como enteros: con pyarrow < 8, convertir una marca con
            # zona a datetime exige pytz (comprobado con pyarrow 7.0.0, el de Glue Python shell 3.9).
            cols[name] = (col.cast(pa.int64()) if pa.types.is_timestamp(col.type) else col).to_pylist()
        rows = [tuple(cols[n][j] for n in cols) for j in range(len(idxs))]
        if any(r != rows[0] for r in rows[1:]):
            differing += 1
    return differing


def read_raw(src, input_prefix, workers):
    """Lista y lee los Parquet de Raw de los dos orígenes. Devuelve ``(tabla, inventario)``."""
    import pyarrow as pa

    keys, skipped, by_origin, total_bytes = [], [], {}, 0
    for origin in ORIGINS:
        prefix = "%sorigin=%s/" % (input_prefix, origin)
        for obj in src.list(prefix):
            key = obj["key"]
            m = RAW_KEY_RE.match(key)
            if not m or m.group("origin") != origin or m.group("prefix") != input_prefix:
                if not key.endswith("/"):
                    skipped.append(key)
                continue
            keys.append(key)
            total_bytes += obj["size"]
            by_origin.setdefault(origin, {"objects": 0, "rows": 0, "bytes": 0})
            by_origin[origin]["objects"] += 1
            by_origin[origin]["bytes"] += obj["size"]
    keys.sort()
    schema = raw_schema()

    def one(key):
        return read_raw_object(src.get(key), key, schema)

    chunks, pending, rows_by_key = [], [], {}
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for key, table in zip(keys, pool.map(one, keys)):
            origin = RAW_KEY_RE.match(key).group("origin")
            by_origin[origin]["rows"] += table.num_rows
            rows_by_key[key] = table.num_rows
            pending.append(table)
            if len(pending) >= 512:  # compacta: miles de tablas pequeñas ocupan mucho más que sus datos
                chunks.append(pa.concat_tables(pending).combine_chunks())
                pending = []
    if pending:
        chunks.append(pa.concat_tables(pending).combine_chunks())
    table = pa.concat_tables(chunks).combine_chunks() if chunks else schema.empty_table()
    inventory = {
        "prefixes": ["%sorigin=%s/" % (input_prefix, o) for o in ORIGINS],
        "objects": len(keys),
        "bytes": total_bytes,
        "by_origin": by_origin,
        "skipped_objects": len(skipped),
        "skipped_examples": skipped[:5],
        "read_seconds": round(time.monotonic() - started, 1),
    }
    return table, inventory


def run_properties(glue, args):
    workflow, run_id = args.get("WORKFLOW_NAME"), args.get("WORKFLOW_RUN_ID")
    if not (glue and workflow and run_id):
        return {}
    resp = glue.get_workflow_run_properties(Name=workflow, RunId=run_id)
    return resp.get("RunProperties") or {}


def transform_and_write(table, src_inventory, dst, args, run_id, started_at, previous):
    """Selecciona, limpia y escribe Trusted; devuelve el reporte (sin escribirlo)."""
    output_prefix = args["ta_output_prefix"]
    if output_prefix.startswith(args["ta_input_prefix"]) or output_prefix.startswith("raw/"):
        raise RuntimeError("--ta_output_prefix %s no puede estar dentro de Raw" % output_prefix)
    posts_prefix = output_prefix + TABLE_DIR
    dry = truthy(args.get("ta_dry_run"))
    t0 = time.monotonic()

    event_ids = table.column("event_id").to_pylist()
    sel = select_rows(event_ids, _ms_list(table.column("event_time")), _ms_list(table.column("process_time")),
                      table.column("generator").to_pylist(), table.column("source").to_pylist())
    del event_ids
    differing = _duplicates_with_differences(table, sel["duplicate_groups"]) if sel["duplicate_groups"] else 0

    origins = table.column("origin").to_pylist()
    process_ms = _ms_list(table.column("process_time"))
    nlp_null = [col.to_pylist() for col in (table.column(c) for c in NLP_COLUMNS)]
    since_ms = iso_to_ms(NLP_SINCE)
    nlp = {"rows": 0, "partial_rows": 0, "since_nlp_since": 0, "by_origin": {}}
    by_origin_out = {}
    for idxs in sel["partitions"].values():
        for i in idxs:
            origin = origins[i] or "?"
            by_origin_out[origin] = by_origin_out.get(origin, 0) + 1
            nulls = sum(1 for col in nlp_null if col[i] is None)
            if nulls == len(NLP_COLUMNS):
                nlp["rows"] += 1
                nlp["by_origin"][origin] = nlp["by_origin"].get(origin, 0) + 1
                if process_ms[i] is not None and process_ms[i] >= since_ms:
                    nlp["since_nlp_since"] += 1
            elif nulls:
                nlp["partial_rows"] += 1
    del nlp_null, process_ms, origins

    schema = trusted_schema()
    existing = [o["key"] for o in dst.list(posts_prefix)]
    files, text_empty, text_null, total_bytes = [], 0, 0, 0
    by_source = {}

    def write_one(item):
        (source, dt), idxs = item
        out, clean = build_partition(table, idxs, schema)
        data = parquet_bytes(out)
        key = partition_key(output_prefix, source, dt)
        if not dry:
            dst.put(key, data, "application/vnd.apache.parquet")
        empty = sum(1 for c in clean if c == "")
        null = sum(1 for c in clean if c is None)
        return {"key": key, "source": source, "dt": dt, "rows": out.num_rows, "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest()}, empty, null

    items = sorted(sel["partitions"].items())
    workers = max(1, min(8, int(args.get("ta_workers") or 8)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for info, empty, null in pool.map(write_one, items):
            files.append(info)
            text_empty += empty
            text_null += null
            total_bytes += info["bytes"]
            agg = by_source.setdefault(info["source"], {"partitions": 0, "rows": 0})
            agg["partitions"] += 1
            agg["rows"] += info["rows"]
    files.sort(key=lambda f: f["key"])
    written = {f["key"] for f in files}
    stale = sorted(k for k in existing if k not in written and not k.endswith("/"))
    for key in stale:
        if not key.startswith(posts_prefix):
            raise RuntimeError("se iba a borrar fuera de %s: %s" % (posts_prefix, key))
    deleted = 0 if dry else (dst.delete(stale) if stale else 0)

    prev_files = {f.get("key"): f.get("sha256") for f in (previous or {}).get("output", {}).get("files", [])}
    changed = [f["key"] for f in files if prev_files.get(f["key"]) != f["sha256"]]
    removed = sorted(k for k in prev_files if k not in written)
    rows_read = table.num_rows
    rows_written = sum(f["rows"] for f in files)
    rejected_total = sum(sel["rejected"].values())
    dts = sorted({f["dt"] for f in files})
    finished = datetime.now(timezone.utc)
    report = {
        "schema": REPORT_SCHEMA,
        "job": JOB_NAME,
        "version": JOB_VERSION,
        "text_clean_rule": TEXT_CLEAN_RULE,
        "run_id": run_id,
        "job_run_id": args.get("JOB_RUN_ID"),
        "workflow": args.get("WORKFLOW_NAME"),
        "workflow_run_id": args.get("WORKFLOW_RUN_ID"),
        "dry_run": dry,
        "started_at": now_iso(started_at),
        "finished_at": now_iso(finished),
        "duration_s": round((finished - started_at).total_seconds(), 1),
        "transform_seconds": round(time.monotonic() - t0, 1),
        "bucket": getattr(dst, "bucket", None),
        "input": src_inventory,
        "counts": {
            "rows_read": rows_read,
            "rejected": dict(sel["rejected"], total=rejected_total),
            "sentinels": sel["sentinels"],
            "duplicates": dict(sel["duplicates"], groups_with_other_differences=differing),
            "nlp_missing": nlp,
            "text_clean": {"empty": text_empty, "null": text_null},
            "rows_written": rows_written,
            "rows_written_by_origin": by_origin_out,
        },
        "conciliation": {
            "formula": "rows_read = rows_written + rejected + sentinels + duplicates_removed",
            "ok": rows_read == rows_written + rejected_total + sel["sentinels"]["rows"] + sel["duplicates"]["rows_removed"],
        },
        "output": {
            "prefix": posts_prefix,
            "partitions": len(files),
            "objects_written": 0 if dry else len(files),
            "objects_deleted": deleted,
            "stale_objects": len(stale),
            "bytes": total_bytes,
            "by_source": by_source,
            "dt_min": dts[0] if dts else None,
            "dt_max": dts[-1] if dts else None,
            "partitions_changed": len(changed),
            "partitions_removed": len(removed),
            "changed_keys": changed,
            "removed_keys": removed,
            "files": files,
        },
        "previous_report": (previous or {}).get("run_id"),
        "environment": library_versions(),
        "peak_rss_mb": peak_rss_mb(),
    }
    return report


def read_previous(dst, latest_key):
    """El último reporte (para contar las particiones que cambian) o ``None``.

    Se lista antes de leer: ``ta-glue-role`` tiene ``s3:ListBucket`` solo con la condición ``s3:prefix``,
    que no viaja en un ``GetObject``, así que pedir un objeto que no existe da 403 (AccessDenied) y no 404.
    """
    if not any(o["key"] == latest_key for o in dst.list(latest_key)):
        return None
    try:
        return json.loads(dst.get(latest_key).decode("utf-8"))
    except ValueError:
        return None


def report_keys(report_prefix, run_id, started_at):
    day = started_at.strftime("%Y-%m-%d")
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    return "%s%s/%s%s.json" % (report_prefix, day, REPORT_DIR, safe), report_prefix + LATEST_REPORT


def summary_fields(report, report_key):
    c = report["counts"]
    o = report["output"]
    return {
        "run_id": report["run_id"],
        "workflow_run_id": report["workflow_run_id"],
        "dry_run": report["dry_run"],
        "report": report_key,
        "input_objects": report["input"]["objects"],
        "input_by_origin": report["input"]["by_origin"],
        "skipped_objects": report["input"]["skipped_objects"],
        "rows_read": c["rows_read"],
        "rejected": c["rejected"]["total"],
        "sentinels": c["sentinels"]["rows"],
        "duplicates_removed": c["duplicates"]["rows_removed"],
        "duplicate_groups_with_other_differences": c["duplicates"]["groups_with_other_differences"],
        "nlp_missing": c["nlp_missing"]["rows"],
        "nlp_missing_since_nlp_since": c["nlp_missing"]["since_nlp_since"],
        "text_clean_empty": c["text_clean"]["empty"],
        "rows_written": c["rows_written"],
        "rows_written_by_origin": c["rows_written_by_origin"],
        "conciliation_ok": report["conciliation"]["ok"],
        "partitions": o["partitions"],
        "objects_deleted": o["objects_deleted"],
        "partitions_changed": o["partitions_changed"],
        "bytes_written": o["bytes"],
        "dt_min": o["dt_min"],
        "dt_max": o["dt_max"],
        "read_seconds": report["input"]["read_seconds"],
        "transform_seconds": report["transform_seconds"],
        "duration_s": report["duration_s"],
        "peak_rss_mb": report["peak_rss_mb"],
        "environment": report["environment"],
    }


def run(args, src, dst, glue=None):
    started_at = datetime.now(timezone.utc)
    run_id = (args.get("WORKFLOW_RUN_ID") or args.get("JOB_RUN_ID")
              or "manual-%s-%s" % (started_at.strftime("%Y%m%dT%H%M%SZ"), uuid.uuid4().hex[:8]))
    log("glue_job_start", run_id=run_id, workflow=args.get("WORKFLOW_NAME"), workflow_run_id=args.get("WORKFLOW_RUN_ID"),
        input_prefix=args["ta_input_prefix"], output_prefix=args["ta_output_prefix"], dry_run=truthy(args.get("ta_dry_run")),
        arg_keys=sorted(k for k in args if k not in DEFAULTS), environment=library_versions())
    props = run_properties(glue, args)
    if wants_failure(args.get("ta_force_fail")) or wants_failure(props.get("ta_force_fail")):
        log("glue_job_forced_failure", run_id=run_id, workflow_run_id=args.get("WORKFLOW_RUN_ID"))
        raise RuntimeError("ta_force_fail: fallo provocado por la validación (%s)" % JOB_NAME)

    workers = int(args.get("ta_workers") or 32)
    table, inventory = read_raw(src, args["ta_input_prefix"], workers)
    log("glue_trusted_read", run_id=run_id, objects=inventory["objects"], rows=table.num_rows,
        by_origin=inventory["by_origin"], skipped_objects=inventory["skipped_objects"],
        read_seconds=inventory["read_seconds"], peak_rss_mb=peak_rss_mb())

    report_key, latest_key = report_keys(args["ta_report_prefix"], run_id, started_at)
    report = transform_and_write(table, inventory, dst, args, run_id, started_at, read_previous(dst, latest_key))
    report["report_key"] = report_key
    if not report["conciliation"]["ok"]:
        raise RuntimeError("la conciliación de filas no cuadra: %s" % json.dumps(report["counts"], sort_keys=True))
    if not report["dry_run"]:
        body = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")
        dst.put(report_key, body, "application/json")
        dst.put(latest_key, body, "application/json")
        if glue and args.get("WORKFLOW_NAME") and args.get("WORKFLOW_RUN_ID"):
            glue.put_workflow_run_properties(
                Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"],
                RunProperties={"ta_trusted_report": report_key, "ta_trusted_rows": str(report["counts"]["rows_written"]),
                               "ta_trusted_partitions_changed": str(report["output"]["partitions_changed"])})
    log("glue_job_done", **summary_fields(report, None if report["dry_run"] else report_key))
    return report


def build_clients(args):
    import boto3
    from botocore.config import Config

    region = args.get("ta_region") or None
    workers = int(args.get("ta_workers") or 32)
    config = Config(max_pool_connections=max(10, workers + 4), retries={"max_attempts": 8, "mode": "standard"})
    return boto3.client("s3", region_name=region, config=config), boto3.client("glue", region_name=region)


def main(argv=None, s3=None, glue=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    bucket = args.get("lake_bucket")
    if not bucket:
        raise RuntimeError("falta --lake_bucket")
    if args.get("ta_local_src") or args.get("ta_local_dst"):
        if not (args.get("ta_local_src") and args.get("ta_local_dst")):
            raise RuntimeError("en local hacen falta --ta_local_src y --ta_local_dst")
        if inside_git_repo(args["ta_local_dst"]):
            raise RuntimeError("--ta_local_dst no puede estar dentro de un repositorio: los datos no van a Git")
        return run(args, DirStore(args["ta_local_src"], bucket), DirStore(args["ta_local_dst"], bucket), glue)
    if s3 is None:
        s3, glue_client = build_clients(args)
        glue = glue or glue_client
    store = S3Store(s3, bucket)
    return run(args, store, store, glue)


# Glue Python shell ejecuta el archivo como script; las pruebas lo importan sin argumentos de Glue.
if __name__ == "__main__" or "--lake_bucket" in sys.argv[1:]:
    main()
