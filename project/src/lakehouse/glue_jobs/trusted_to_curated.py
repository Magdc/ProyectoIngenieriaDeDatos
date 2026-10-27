"""Glue job ``ta-job-trusted-to-curated`` · Trusted → Curated (Lote 4.3, HU-027; ARCHITECTURE.md §3.22, ADR-26).

Reemplaza al marcador de posición del Lote 4.1. Terraform lo sube a
``s3://{lake}/_glue/scripts/ta-job-trusted-to-curated.py``; corre en Glue **Python shell 3.9** con el
*analytics library set* (0,0625 DPU) y solo usa la biblioteca estándar, boto3/botocore y pyarrow. En el
workflow ``ta-wf-raw-to-curated`` corre después de ``ta-job-raw-to-trusted`` (si terminó en SUCCEEDED).

Qué hace en cada corrida (reconstruye Curated entero desde Trusted; mismo Trusted ⇒ mismos bytes):

1. Lista y lee en paralelo los Parquet de ``trusted/posts/source={s}/dt={d}/`` directamente de S3 (el rol
   no tiene Athena), solo las columnas que usa Curated; ``source`` y ``dt`` salen de la ruta.
2. Construye las tablas de ``contracts/athena/curated.sql`` salvo las aristas (``edges_*``, Lote 4.4):
   ``fact_posts`` (una fila por ``event_id``, ``date_key = yyyymmdd`` de ``dt``, NLP nulo en las filas sin
   NLP), ``dim_date``, ``dim_source``, ``dim_brand``, ``dim_user``, ``agg_emotion_by_brand_hour`` (sin las
   filas sin emoción) y ``agg_trend_daily`` (``n_posts`` cuenta todas las filas; ``n_negative``,
   ``n_neutral``, ``n_positive`` y ``avg_sentiment``, solo las que tienen sentimiento).
3. Concilia antes de escribir (filas de ``fact_posts`` = filas de Trusted, sumas de los agregados = conteos
   de ``fact_posts``, llaves foráneas): si algo no cuadra falla sin tocar Curated.
4. Escribe un Parquet + Snappy por partición de ``fact_posts``
   (``curated/fact_posts/dt={AAAA-MM-DD}/part-00000.parquet``) y uno por tabla
   (``curated/{tabla}/part-00000.parquet``). Solo sube los objetos cuyo sha256 cambió frente al último
   reporte (o que faltan); sobrescribir un objeto de S3 es atómico. Después borra lo que sobre, **solo**
   bajo los prefijos de sus siete tablas (nunca ``curated/edges_*`` ni ``curated/_ta_placeholder/``).
5. Deja el reporte de la corrida en ``quality/{AAAA-MM-DD}/trusted_to_curated/{corrida}.json`` y en
   ``quality/_latest/trusted_to_curated.json`` y una línea JSON ``glue_job_done`` en
   ``/aws-glue/python-jobs/output``. Dentro del workflow deja ``ta_curated_report``, ``ta_curated_rows`` y
   ``ta_curated_files_changed`` en las propiedades de la corrida.

Como los demás jobs del workflow, falla a propósito (antes de leer o escribir) si ``ta_force_fail``
(propiedad de la corrida o argumento) nombra su clave, su nombre o ``all``.

En local (sin nube) lee un árbol con el layout del bucket y escribe en otra carpeta fuera del repo:

    python project/src/lakehouse/glue_jobs/trusted_to_curated.py --lake_bucket ta-dev-648617469176-lake \
        --ta_local_src ~/.cache/trendanalyzer/quality/s3/ta-dev-648617469176-lake \
        --ta_local_dst ~/.cache/trendanalyzer/quality/trusted_to_curated_local

Compatible con Python 3.9 y pyarrow 7 (el de Glue). Con pyarrow 7, un campo declarado no nulo que trae
nulos se escribe como un Parquet corrupto sin error (pyarrow ≥ 8 lo rechaza): el job lo comprueba antes.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

JOB_KEY = "trusted_to_curated"
JOB_NAME = "ta-job-trusted-to-curated"
JOB_VERSION = "trusted-to-curated@1.0"
REPORT_SCHEMA = "ta-trusted-to-curated-report@1"
LOTE = "4.3"

DEFAULTS = {
    "ta_input_prefix": "trusted/",
    "ta_output_prefix": "curated/",
    "ta_report_prefix": "quality/",
    "ta_region": "",
    "ta_force_fail": "",
    "ta_workers": "16",
    "ta_dry_run": "false",
    "ta_local_src": "",
    "ta_local_dst": "",
}

ORIGINS = ("stream", "historical")
SOURCES = ("reddit", "facebook", "instagram")
#: Primer día de ``projection.dt.range`` (Trusted y ``fact_posts``): Athena no ve particiones anteriores.
PROJECTION_START = "2026-01-01"
SENTIMENT_LABELS = ("negative", "neutral", "positive")
EMOTION_LABELS = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
#: Sin NLP = las cinco columnas nulas a la vez (la misma definición que el reporte del 4.2).
NLP_COLUMNS = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version")
TRUSTED_DIR = "posts/"
PART_FILE = "part-00000.parquet"
REPORT_DIR = "trusted_to_curated/"
LATEST_REPORT = "_latest/trusted_to_curated.json"
MS_PER_HOUR = 3600000
MS_PER_DAY = 86400000

TRUSTED_KEY_RE = re.compile(
    r"^(?P<prefix>.*)posts/source=(?P<source>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/[^/]+\.parquet$"
)

#: Columnas de Trusted que lee (subconjunto de contracts/athena/posts.sql) con su tipo lógico.
READ_COLUMNS = (
    ("event_id", "string"),
    ("origin", "string"),
    ("event_time", "timestamp"),
    ("user_hash", "string"),
    ("followers", "int64"),
    ("brands", "list"),
    ("brand_primary", "string"),
    ("likes", "int64"),
    ("shares", "int64"),
    ("comments", "int64"),
    ("sentiment_label", "string"),
    ("sentiment_score", "double"),
    ("emotion_label", "string"),
    ("emotion_score", "double"),
    ("label_sentiment", "string"),
    ("nlp_model_version", "string"),
)
READ_NAMES = tuple(c[0] for c in READ_COLUMNS)

# Tablas de Curated (= contracts/athena/curated.sql salvo edges_*): (nombre, tipo lógico, admite nulos).
# Los tipos lógicos se traducen a Arrow en arrow_type(); las pruebas los comparan con catalog_spec.
FACT_COLUMNS = (
    ("event_id", "string", False),
    ("date_key", "int32", False),
    ("origin", "string", True),
    ("source", "string", False),
    ("brand_primary", "string", True),
    ("brands", "list", True),
    ("user_hash", "string", True),
    ("event_time", "timestamp", False),
    ("likes", "int64", True),
    ("shares", "int64", True),
    ("comments", "int64", True),
    ("sentiment_label", "string", True),
    ("sentiment_score", "double", True),
    ("emotion_label", "string", True),
    ("emotion_score", "double", True),
    ("label_sentiment", "string", True),
)
DIM_DATE_COLUMNS = (
    ("date_key", "int32", False),
    ("full_date", "date", False),
    ("year", "int32", False),
    ("month", "int32", False),
    ("day", "int32", False),
    ("iso_week", "int32", False),
    ("day_of_week", "int32", False),
    ("is_weekend", "bool", False),
)
DIM_SOURCE_COLUMNS = (
    ("source", "string", False),
    ("display_name", "string", False),
    ("kind_mix", "string", False),
)
DIM_BRAND_COLUMNS = (
    ("brand", "string", False),
    ("display", "string", True),
    ("role", "string", True),
)
DIM_USER_COLUMNS = (
    ("user_hash", "string", False),
    ("first_seen", "timestamp", False),
    ("last_seen", "timestamp", False),
    ("n_posts", "int64", False),
    ("max_followers", "int64", True),
)
AGG_EMOTION_COLUMNS = (
    ("hour_start", "timestamp", False),
    ("brand", "string", False),
    ("source", "string", False),
    ("emotion_label", "string", False),
    ("n_posts", "int64", False),
)
AGG_TREND_COLUMNS = (
    ("full_date", "date", False),
    ("brand", "string", False),
    ("n_posts", "int64", False),
    ("n_negative", "int64", False),
    ("n_neutral", "int64", False),
    ("n_positive", "int64", False),
    ("avg_sentiment", "double", True),
)
TABLES = (
    ("fact_posts", FACT_COLUMNS),
    ("dim_date", DIM_DATE_COLUMNS),
    ("dim_source", DIM_SOURCE_COLUMNS),
    ("dim_brand", DIM_BRAND_COLUMNS),
    ("dim_user", DIM_USER_COLUMNS),
    ("agg_emotion_by_brand_hour", AGG_EMOTION_COLUMNS),
    ("agg_trend_daily", AGG_TREND_COLUMNS),
)
TABLE_NAMES = tuple(t[0] for t in TABLES)
TABLE_COLUMNS = dict(TABLES)
#: Del Lote 4.4: este job no las escribe ni las borra.
EDGE_TABLES = ("edges_user_brand", "edges_hashtag_cooccurrence")

#: dim_source (ARCHITECTURE.md §3.3; mezcla de ``kind`` de src/simulator/config/sim.yaml). Las pruebas
#: la comparan con sim.yaml.
SOURCE_ROWS = (
    ("facebook", "Facebook", "post 15% · comment 85%"),
    ("instagram", "Instagram", "comment 100%"),
    ("reddit", "Reddit", "post 30% · comment 70%"),
)
#: dim_brand (src/simulator/config/brands.yaml: clave, display y role). Las pruebas la comparan con el
#: YAML; una marca de los datos que no esté aquí entra con display y role nulos (y se cuenta).
BRAND_ROWS = (
    ("pepsi", "Pepsi", "competidor"),
    ("postobon", "Postobón", "propia"),
    ("sprite", "Sprite", "competidor"),
)


class TrustedSchemaError(RuntimeError):
    """Un Parquet de Trusted no tiene (ni se puede llevar a) las columnas que usa Curated."""


class CuratedBuildError(RuntimeError):
    """Una tabla de Curated no cumple su contrato (nulos en una columna no nula) o no concilia."""


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


def cpu_seconds():
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        return round(usage.ru_utime + usage.ru_stime, 1)
    except Exception:  # noqa: BLE001 - solo informativo
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
# (Copiados de raw_to_trusted.py: un job es un solo archivo; contrato en ARCHITECTURE.md §3.21.1.)


class S3Store:
    """Lo mínimo de S3 que usa el job, sobre un cliente de boto3."""

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

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
    """Una carpeta con el layout del bucket (``trusted/…``, ``curated/…``): para correr el job en local."""

    def __init__(self, root, bucket="local"):
        self.root = os.path.abspath(os.path.expanduser(root))
        self.bucket = bucket

    def _path(self, key):
        path = os.path.normpath(os.path.join(self.root, key))
        if not (path == self.root or path.startswith(self.root + os.sep)):
            raise ValueError("clave fuera de la carpeta: %s" % key)
        return path

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


# --- esquemas y tipos -------------------------------------------------------------------------------


def arrow_type(kind):
    import pyarrow as pa

    if kind == "string":
        return pa.string()
    if kind == "timestamp":
        return pa.timestamp("ms", tz="UTC")
    if kind == "int64":
        return pa.int64()
    if kind == "int32":
        return pa.int32()
    if kind == "double":
        return pa.float64()
    if kind == "date":
        return pa.date32()
    if kind == "bool":
        return pa.bool_()
    if kind == "list":
        return pa.list_(pa.field("element", pa.string()))
    raise ValueError(kind)


def table_schema(name):
    """Esquema Arrow de una tabla de Curated (``fact_posts`` sin ``dt``, que va en la ruta)."""
    import pyarrow as pa

    return pa.schema([pa.field(n, arrow_type(k), nullable=nul) for n, k, nul in TABLE_COLUMNS[name]])


def read_schema():
    import pyarrow as pa

    return pa.schema([pa.field(n, arrow_type(k), nullable=True) for n, k in READ_COLUMNS])


def _coerce(col, kind, target, key, name):
    import pyarrow as pa

    t = col.type
    try:
        if kind == "timestamp":
            if not pa.types.is_timestamp(t):
                raise TypeError("no es una marca de tiempo")
            if t.tz is None:  # sin zona = UTC (§5.1)
                col = col.cast(pa.timestamp(t.unit, tz="UTC"))
            return col.cast(target, safe=False)  # us/ns → ms
        if kind in ("int64", "double"):
            if not (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_null(t)):
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
        raise TrustedSchemaError("%s: la columna %s es %s y no se puede llevar a %s (%s)" % (key, name, t, target, exc))
    raise TrustedSchemaError("%s: tipo lógico desconocido %s" % (key, kind))


def read_trusted_object(data, key, schema=None):
    """Bytes de un Parquet de Trusted → tabla con ``READ_COLUMNS`` en orden y con sus tipos."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = schema or read_schema()
    try:
        names = pq.read_schema(pa.BufferReader(data)).names
    except Exception as exc:  # noqa: BLE001
        raise TrustedSchemaError("%s: no se puede leer como Parquet (%s: %s)" % (key, type(exc).__name__, exc))
    missing = [n for n in READ_NAMES if n not in names]
    if missing:
        raise TrustedSchemaError("%s: faltan columnas %s" % (key, ", ".join(missing)))
    table = pq.read_table(pa.BufferReader(data), columns=list(READ_NAMES), use_threads=False)
    arrays = []
    for (name, kind), field in zip(READ_COLUMNS, schema):
        col = table.column(name)
        if not col.type.equals(field.type):
            col = _coerce(col, kind, field.type, key, name)
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=schema)


def parquet_bytes(table):
    import io

    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_dictionary=True, coerce_timestamps="ms",
                   allow_truncated_timestamps=False, use_deprecated_int96_timestamps=False, write_statistics=True)
    return buf.getvalue()


def check_not_null(name, table):
    """Con pyarrow 7 un campo no nulo con nulos sale como un Parquet corrupto (sin error): se comprueba aquí."""
    bad = [f.name for f in table.schema if not f.nullable and table.column(f.name).null_count]
    if bad:
        raise CuratedBuildError("%s: nulos en columnas no nulas %s" % (name, ", ".join(bad)))


def table_from_columns(name, columns):
    """``{columna: lista de valores Python}`` → tabla Arrow con el esquema de ``name``."""
    import pyarrow as pa

    schema = table_schema(name)
    arrays = []
    for field in schema:
        values = columns[field.name]
        if pa.types.is_timestamp(field.type):  # enteros en ms → marca de tiempo (sin pasar por datetime)
            arrays.append(pa.array(values, type=pa.int64()).cast(field.type))
        else:
            arrays.append(pa.array(values, type=field.type))
    table = pa.Table.from_arrays(arrays, schema=schema)
    check_not_null(name, table)
    return table


# --- lógica pura ------------------------------------------------------------------------------------

_DAY_CACHE = {}


def ms_to_dt(ms):
    """Milisegundos desde 1970 (UTC) → ``AAAA-MM-DD``."""
    day = ms // MS_PER_DAY
    out = _DAY_CACHE.get(day)
    if out is None:
        out = (datetime(1970, 1, 1) + timedelta(days=day)).strftime("%Y-%m-%d")
        _DAY_CACHE[day] = out
    return out


def date_key(dt):
    """``AAAA-MM-DD`` → entero ``yyyymmdd`` (``fact_posts.date_key``, §9 #9)."""
    return int(dt.replace("-", ""))


def signed_sentiment(label, score):
    """Valor con signo de un post (la definición de ``trend_windows``, §5.5): +score, −score, 0 o ``None``."""
    if label == "neutral":
        return 0.0
    if label == "positive" and score is not None:
        return float(score)
    if label == "negative" and score is not None:
        return -float(score)
    return None


def avg_sentiment(values):
    """Media con signo en [-1, 1] redondeada a 6 decimales (como ``trend_windows``); ``None`` sin valores.

    ``math.fsum`` da la suma correctamente redondeada: el resultado no depende del orden de las filas.
    """
    if not values:
        return None
    value = math.fsum(values) / len(values)
    return round(max(-1.0, min(1.0, value)), 6)


def date_rows(dts):
    """Calendario continuo del menor al mayor ``dt`` (filas de ``dim_date``)."""
    rows = {n: [] for n, _, _ in DIM_DATE_COLUMNS}
    if not dts:
        return rows
    day, last = date.fromisoformat(min(dts)), date.fromisoformat(max(dts))
    while day <= last:
        iso = day.isocalendar()
        rows["date_key"].append(int(day.strftime("%Y%m%d")))
        rows["full_date"].append(day)
        rows["year"].append(day.year)
        rows["month"].append(day.month)
        rows["day"].append(day.day)
        rows["iso_week"].append(iso[1])
        rows["day_of_week"].append(day.isoweekday())  # 1 = lunes … 7 = domingo
        rows["is_weekend"].append(day.isoweekday() >= 6)
        day += timedelta(days=1)
    return rows


class Accumulator:
    """Recorre Trusted archivo por archivo y acumula lo que necesitan las dimensiones y los agregados."""

    def __init__(self):
        self.rows = 0
        self.by_source, self.by_origin = {}, {}
        self.event_ids, self.duplicates = set(), 0
        self.dts = set()
        self.dt_mismatch = 0
        self.users = {}  # user_hash → [first_ms, last_ms, n_posts, max_followers]
        self.rows_without_user = 0
        self.rows_without_brand = 0
        self.brands = {}
        self.trend = {}  # (dt, brand) → [n, neg, neu, pos, [valores con signo]]
        self.emotion = {}  # (hour_ms, brand, source, emotion) → n
        self.nlp_missing, self.nlp_missing_by_origin = 0, {}
        self.with_sentiment, self.with_emotion = 0, 0
        self.unknown_sentiment, self.unknown_emotion = 0, 0

    def add(self, source, dt, table):
        import pyarrow as pa

        n = table.num_rows
        self.rows += n
        self.by_source[source] = self.by_source.get(source, 0) + n
        self.dts.add(dt)
        col = table.column
        ids = col("event_id").to_pylist()
        origins = col("origin").to_pylist()
        # Marcas de tiempo como enteros: con pyarrow < 8 pasar a datetime una marca con zona exige pytz.
        ems = col("event_time").cast(pa.int64()).to_pylist()
        users = col("user_hash").to_pylist()
        followers = col("followers").to_pylist()
        brands = col("brand_primary").to_pylist()
        sl, ss = col("sentiment_label").to_pylist(), col("sentiment_score").to_pylist()
        el, es = col("emotion_label").to_pylist(), col("emotion_score").to_pylist()
        nv = col("nlp_model_version").to_pylist()
        seen = self.event_ids
        for i in range(n):
            eid = ids[i]
            if eid in seen:
                self.duplicates += 1
            else:
                seen.add(eid)
            origin = origins[i] or "?"
            self.by_origin[origin] = self.by_origin.get(origin, 0) + 1
            ms = ems[i]
            if ms is None or ms_to_dt(ms) != dt:
                self.dt_mismatch += 1
            label, emo = sl[i], el[i]
            if label is None and ss[i] is None and emo is None and es[i] is None and nv[i] is None:
                self.nlp_missing += 1
                self.nlp_missing_by_origin[origin] = self.nlp_missing_by_origin.get(origin, 0) + 1
            user = users[i]
            if user is None:
                self.rows_without_user += 1
            elif ms is not None:
                rec = self.users.get(user)
                fol = followers[i]
                if rec is None:
                    self.users[user] = [ms, ms, 1, fol]
                else:
                    if ms < rec[0]:
                        rec[0] = ms
                    if ms > rec[1]:
                        rec[1] = ms
                    rec[2] += 1
                    if fol is not None and (rec[3] is None or fol > rec[3]):
                        rec[3] = fol
            brand = brands[i]
            if brand is None:
                self.rows_without_brand += 1
                continue
            self.brands[brand] = self.brands.get(brand, 0) + 1
            agg = self.trend.get((dt, brand))
            if agg is None:
                agg = self.trend[(dt, brand)] = [0, 0, 0, 0, []]
            agg[0] += 1
            if label in SENTIMENT_LABELS:
                agg[1 + SENTIMENT_LABELS.index(label)] += 1
                self.with_sentiment += 1
            elif label is not None:
                self.unknown_sentiment += 1
            signed = signed_sentiment(label, ss[i])
            if signed is not None:
                agg[4].append(signed)
            if emo is not None and ms is not None:
                self.with_emotion += 1
                if emo not in EMOTION_LABELS:
                    self.unknown_emotion += 1
                key = (ms // MS_PER_HOUR * MS_PER_HOUR, brand, source, emo)
                self.emotion[key] = self.emotion.get(key, 0) + 1


def build_dimensions_and_aggregates(acc):
    """Tablas Arrow de ``dim_*`` y ``agg_*`` desde el acumulador (filas en orden determinista)."""
    tables = {"dim_date": table_from_columns("dim_date", date_rows(acc.dts))}

    src = {n: [] for n, _, _ in DIM_SOURCE_COLUMNS}
    for source, display, mix in SOURCE_ROWS:
        src["source"].append(source)
        src["display_name"].append(display)
        src["kind_mix"].append(mix)
    tables["dim_source"] = table_from_columns("dim_source", src)

    known = {b for b, _, _ in BRAND_ROWS}
    unknown = sorted(b for b in acc.brands if b not in known)
    rows = sorted(list(BRAND_ROWS) + [(b, None, None) for b in unknown])
    tables["dim_brand"] = table_from_columns("dim_brand", {
        "brand": [r[0] for r in rows], "display": [r[1] for r in rows], "role": [r[2] for r in rows]})

    users = sorted(acc.users.items())
    tables["dim_user"] = table_from_columns("dim_user", {
        "user_hash": [u for u, _ in users], "first_seen": [r[0] for _, r in users], "last_seen": [r[1] for _, r in users],
        "n_posts": [r[2] for _, r in users], "max_followers": [r[3] for _, r in users]})

    emo = sorted(acc.emotion.items())
    tables["agg_emotion_by_brand_hour"] = table_from_columns("agg_emotion_by_brand_hour", {
        "hour_start": [k[0] for k, _ in emo], "brand": [k[1] for k, _ in emo], "source": [k[2] for k, _ in emo],
        "emotion_label": [k[3] for k, _ in emo], "n_posts": [n for _, n in emo]})

    trend = sorted(acc.trend.items())
    tables["agg_trend_daily"] = table_from_columns("agg_trend_daily", {
        "full_date": [date.fromisoformat(k[0]) for k, _ in trend], "brand": [k[1] for k, _ in trend],
        "n_posts": [v[0] for _, v in trend], "n_negative": [v[1] for _, v in trend], "n_neutral": [v[2] for _, v in trend],
        "n_positive": [v[3] for _, v in trend], "avg_sentiment": [avg_sentiment(v[4]) for _, v in trend]})
    return tables, unknown


def build_fact_partition(parts):
    """Tablas de Trusted de un mismo ``dt`` (una por fuente) → partición de ``fact_posts`` ordenada por ``event_id``."""
    import pyarrow as pa
    import pyarrow.compute as pc

    schema = table_schema("fact_posts")
    pieces = []
    for source, dt, table in parts:
        n = table.num_rows
        arrays = []
        for field in schema:
            if field.name == "date_key":
                arrays.append(pa.array([date_key(dt)] * n, type=pa.int32()))
            elif field.name == "source":
                arrays.append(pa.array([source] * n, type=pa.string()))
            else:
                arrays.append(table.column(field.name))
        pieces.append(pa.Table.from_arrays(arrays, schema=schema))
    table = pa.concat_tables(pieces).combine_chunks()
    table = table.take(pc.sort_indices(table, sort_keys=[("event_id", "ascending")]))
    check_not_null("fact_posts", table)
    return table


def conciliation_checks(acc, fact_rows, tables):
    """Lo que tiene que cuadrar antes de escribir (ARCHITECTURE.md §3.22): ``{nombre: {expected, observed, ok}}``."""
    trend, emo, users = tables["agg_trend_daily"], tables["agg_emotion_by_brand_hour"], tables["dim_user"]

    def total(table, name):
        return sum(v for v in table.column(name).to_pylist() if v is not None)

    with_brand = acc.rows - acc.rows_without_brand
    trend_sentiment = total(trend, "n_negative") + total(trend, "n_neutral") + total(trend, "n_positive")
    date_keys = set(tables["dim_date"].column("date_key").to_pylist())
    fact_keys = {date_key(d) for d in acc.dts}
    brands = set(tables["dim_brand"].column("brand").to_pylist())
    sources = set(tables["dim_source"].column("source").to_pylist())
    n_days = 0
    if acc.dts:
        n_days = (date.fromisoformat(max(acc.dts)) - date.fromisoformat(min(acc.dts))).days + 1
    raw = {
        "fact_rows_eq_trusted_rows": (acc.rows, fact_rows),
        "unique_event_id": (0, acc.duplicates),
        "trend_n_posts_eq_rows_with_brand": (with_brand, total(trend, "n_posts")),
        "trend_sentiment_eq_rows_with_sentiment": (acc.with_sentiment, trend_sentiment),
        "trend_rows_without_sentiment": (with_brand - acc.with_sentiment, total(trend, "n_posts") - trend_sentiment),
        "emotion_n_posts_eq_rows_with_emotion": (acc.with_emotion, total(emo, "n_posts")),
        "user_n_posts_eq_rows_with_user": (acc.rows - acc.rows_without_user, total(users, "n_posts")),
        "fk_source_in_dim_source": (0, len(set(acc.by_source) - sources)),
        "fk_brand_in_dim_brand": (0, len(set(acc.brands) - brands)),
        "fk_date_key_in_dim_date": (0, len(fact_keys - date_keys)),
        "dim_date_continuous": (n_days, tables["dim_date"].num_rows),
    }
    return {k: {"expected": e, "observed": o, "ok": e == o} for k, (e, o) in raw.items()}


# --- el job -----------------------------------------------------------------------------------------


def read_trusted(src, input_prefix, workers):
    """Lista y lee los Parquet de ``trusted/posts/``. Devuelve ``([(source, dt, tabla)], inventario)``."""
    prefix = input_prefix + TRUSTED_DIR
    keys, skipped, total_bytes = [], [], 0
    for obj in src.list(prefix):
        key = obj["key"]
        m = TRUSTED_KEY_RE.match(key)
        if not m or m.group("prefix") != input_prefix or m.group("source") not in SOURCES \
                or m.group("dt") < PROJECTION_START:
            if not key.endswith("/"):
                skipped.append(key)
            continue
        keys.append(key)
        total_bytes += obj["size"]
    keys.sort()
    schema = read_schema()

    def one(key):
        return read_trusted_object(src.get(key), key, schema)

    started = time.monotonic()
    parts = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for key, table in zip(keys, pool.map(one, keys)):
            m = TRUSTED_KEY_RE.match(key)
            parts.append((m.group("source"), m.group("dt"), table))
    inventory = {
        "prefix": prefix,
        "objects": len(keys),
        "bytes": total_bytes,
        "skipped_objects": len(skipped),
        "skipped_examples": skipped[:5],
        "read_seconds": round(time.monotonic() - started, 1),
    }
    return parts, inventory


def run_properties(glue, args):
    workflow, run_id = args.get("WORKFLOW_NAME"), args.get("WORKFLOW_RUN_ID")
    if not (glue and workflow and run_id):
        return {}
    resp = glue.get_workflow_run_properties(Name=workflow, RunId=run_id)
    return resp.get("RunProperties") or {}


def table_key(output_prefix, name, dt=None):
    if dt is not None:
        return "%s%s/dt=%s/%s" % (output_prefix, name, dt, PART_FILE)
    return "%s%s/%s" % (output_prefix, name, PART_FILE)


def build_outputs(parts, output_prefix):
    """Todas las tablas de Curated: ``(acumulador, [(clave, tabla, dt, tabla Arrow)], tablas sin partición)``."""
    acc = Accumulator()
    by_dt = {}
    for source, dt, table in parts:
        acc.add(source, dt, table)
        by_dt.setdefault(dt, []).append((source, dt, table))
    outputs = []
    for dt in sorted(by_dt):
        outputs.append((table_key(output_prefix, "fact_posts", dt), "fact_posts", dt, build_fact_partition(by_dt[dt])))
    tables, unknown = build_dimensions_and_aggregates(acc)
    for name in TABLE_NAMES[1:]:
        outputs.append((table_key(output_prefix, name), name, None, tables[name]))
    return acc, outputs, tables, unknown


def transform_and_write(parts, inventory, dst, args, run_id, started_at, previous, props):
    """Construye, concilia y escribe Curated; devuelve el reporte (sin escribirlo)."""
    output_prefix = args["ta_output_prefix"]
    if output_prefix.startswith(args["ta_input_prefix"]) or output_prefix.startswith(("raw/", "trusted/")):
        raise RuntimeError("--ta_output_prefix %s no puede estar dentro de Raw ni de Trusted" % output_prefix)
    dry = truthy(args.get("ta_dry_run"))
    t0 = time.monotonic()

    acc, outputs, tables, unknown = build_outputs(parts, output_prefix)
    fact_rows = sum(t.num_rows for _, name, _, t in outputs if name == "fact_posts")
    checks = conciliation_checks(acc, fact_rows, tables)
    if not all(c["ok"] for c in checks.values()):
        bad = {k: v for k, v in checks.items() if not v["ok"]}
        raise CuratedBuildError("Curated no concilia con Trusted (no se escribió nada): %s" % json.dumps(bad, sort_keys=True))

    table_prefixes = ["%s%s/" % (output_prefix, name) for name in TABLE_NAMES]
    existing = {}
    for prefix in table_prefixes:
        for obj in dst.list(prefix):
            existing[obj["key"]] = obj["size"]
    prev_files = {f.get("key"): f.get("sha256") for f in (previous or {}).get("output", {}).get("files", [])}

    def write_one(item):
        key, name, dt, table = item
        data = parquet_bytes(table)
        sha = hashlib.sha256(data).hexdigest()
        unchanged = prev_files.get(key) == sha and existing.get(key) == len(data)
        if not dry and not unchanged:
            dst.put(key, data, "application/vnd.apache.parquet")
        return {"key": key, "table": name, "dt": dt, "rows": table.num_rows, "bytes": len(data), "sha256": sha}, unchanged

    files, unchanged_n = [], 0
    workers = max(1, min(8, int(args.get("ta_workers") or 8)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for info, unchanged in pool.map(write_one, outputs):
            files.append(info)
            unchanged_n += 1 if unchanged else 0
    files.sort(key=lambda f: f["key"])
    written = {f["key"] for f in files}
    stale = sorted(k for k in existing if k not in written and not k.endswith("/"))
    for key in stale:
        if not any(key.startswith(p) for p in table_prefixes):
            raise RuntimeError("se iba a borrar fuera de las tablas de Curated: %s" % key)
    deleted = 0 if dry else (dst.delete(stale) if stale else 0)

    changed = [f["key"] for f in files if prev_files.get(f["key"]) != f["sha256"]]
    removed = sorted(k for k in prev_files if k not in written)
    by_table = {}
    for f in files:
        agg = by_table.setdefault(f["table"], {"rows": 0, "files": 0, "bytes": 0, "files_changed": 0})
        agg["rows"] += f["rows"]
        agg["files"] += 1
        agg["bytes"] += f["bytes"]
        agg["files_changed"] += 1 if f["key"] in changed else 0
    dts = sorted(acc.dts)
    expected_rows = props.get("ta_trusted_rows")
    finished = datetime.now(timezone.utc)
    return {
        "schema": REPORT_SCHEMA,
        "job": JOB_NAME,
        "version": JOB_VERSION,
        "run_id": run_id,
        "job_run_id": args.get("JOB_RUN_ID"),
        "workflow": args.get("WORKFLOW_NAME"),
        "workflow_run_id": args.get("WORKFLOW_RUN_ID"),
        "dry_run": dry,
        "started_at": now_iso(started_at),
        "finished_at": now_iso(finished),
        "duration_s": round((finished - started_at).total_seconds(), 1),
        "transform_seconds": round(time.monotonic() - t0, 1),
        "cpu_seconds": cpu_seconds(),
        "bucket": getattr(dst, "bucket", None),
        "input": dict(inventory, rows=acc.rows, by_source=acc.by_source, by_origin=acc.by_origin, partitions=len(parts),
                      dt_min=dts[0] if dts else None, dt_max=dts[-1] if dts else None,
                      trusted_report=props.get("ta_trusted_report"), trusted_rows_expected=expected_rows,
                      trusted_rows_match=(str(acc.rows) == str(expected_rows)) if expected_rows is not None else None),
        "counts": {
            "fact_rows": fact_rows,
            "nlp_missing": acc.nlp_missing,
            "nlp_missing_by_origin": acc.nlp_missing_by_origin,
            "with_sentiment": acc.with_sentiment,
            "with_emotion": acc.with_emotion,
            "rows_without_brand": acc.rows_without_brand,
            "rows_without_user": acc.rows_without_user,
            "dt_mismatch": acc.dt_mismatch,
            "duplicate_event_ids": acc.duplicates,
            "unknown_brands": unknown,
            "unknown_sentiment_labels": acc.unknown_sentiment,
            "unknown_emotion_labels": acc.unknown_emotion,
            "brands": dict(sorted(acc.brands.items())),
        },
        "tables": by_table,
        "checks": checks,
        "conciliation": {"ok": True, "formula": "fact_posts = Trusted; sumas de agg_* y dim_user = conteos de fact_posts; "
                                                "llaves foráneas en sus dimensiones (checks)"},
        "output": {
            "prefix": output_prefix,
            "tables": list(TABLE_NAMES),
            "objects": len(files),
            "objects_written": 0 if dry else len(files) - unchanged_n,
            "objects_unchanged": unchanged_n,
            "objects_deleted": deleted,
            "stale_objects": len(stale),
            "bytes": sum(f["bytes"] for f in files),
            "files_changed": len(changed),
            "files_removed": len(removed),
            "changed_keys": changed,
            "removed_keys": removed,
            "files": files,
        },
        "previous_report": (previous or {}).get("run_id"),
        "environment": library_versions(),
        "peak_rss_mb": peak_rss_mb(),
    }


def read_previous(dst, latest_key):
    """El último reporte (para saber qué objetos cambian) o ``None``.

    Se lista antes de leer: con el ``s3:ListBucket`` condicionado por prefijo de ``ta-glue-role``, pedir un
    objeto que no existe da 403 (AccessDenied) y no 404.
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
    c, o, i = report["counts"], report["output"], report["input"]
    return {
        "run_id": report["run_id"],
        "workflow_run_id": report["workflow_run_id"],
        "dry_run": report["dry_run"],
        "report": report_key,
        "input_objects": i["objects"],
        "skipped_objects": i["skipped_objects"],
        "rows_read": i["rows"],
        "rows_read_by_origin": i["by_origin"],
        "trusted_rows_match": i["trusted_rows_match"],
        "fact_rows": c["fact_rows"],
        "fact_partitions": report["tables"].get("fact_posts", {}).get("files", 0),
        "tables": {k: v["rows"] for k, v in sorted(report["tables"].items())},
        "nlp_missing": c["nlp_missing"],
        "rows_without_brand": c["rows_without_brand"],
        "dt_mismatch": c["dt_mismatch"],
        "conciliation_ok": report["conciliation"]["ok"],
        "objects": o["objects"],
        "objects_written": o["objects_written"],
        "objects_unchanged": o["objects_unchanged"],
        "objects_deleted": o["objects_deleted"],
        "files_changed": o["files_changed"],
        "bytes_written": o["bytes"],
        "dt_min": i["dt_min"],
        "dt_max": i["dt_max"],
        "read_seconds": i["read_seconds"],
        "transform_seconds": report["transform_seconds"],
        "duration_s": report["duration_s"],
        "cpu_seconds": report["cpu_seconds"],
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

    workers = int(args.get("ta_workers") or 16)
    parts, inventory = read_trusted(src, args["ta_input_prefix"], workers)
    log("glue_curated_read", run_id=run_id, objects=inventory["objects"], rows=sum(t.num_rows for _, _, t in parts),
        skipped_objects=inventory["skipped_objects"], read_seconds=inventory["read_seconds"], peak_rss_mb=peak_rss_mb())

    report_key, latest_key = report_keys(args["ta_report_prefix"], run_id, started_at)
    report = transform_and_write(parts, inventory, dst, args, run_id, started_at, read_previous(dst, latest_key), props)
    report["report_key"] = report_key
    if not report["dry_run"]:
        body = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")
        dst.put(report_key, body, "application/json")
        dst.put(latest_key, body, "application/json")
        if glue and args.get("WORKFLOW_NAME") and args.get("WORKFLOW_RUN_ID"):
            glue.put_workflow_run_properties(
                Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"],
                RunProperties={"ta_curated_report": report_key, "ta_curated_rows": str(report["counts"]["fact_rows"]),
                               "ta_curated_files_changed": str(report["output"]["files_changed"])})
    log("glue_job_done", **summary_fields(report, None if report["dry_run"] else report_key))
    return report


def build_clients(args):
    import boto3
    from botocore.config import Config

    region = args.get("ta_region") or None
    workers = int(args.get("ta_workers") or 16)
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
