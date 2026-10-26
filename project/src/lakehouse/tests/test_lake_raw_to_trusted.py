"""Job real Raw → Trusted (Lote 4.2, HU-026) con un S3 y un Glue falsos y Parquet reales de Raw."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.enriched_row import COLUMN_NAMES
from common.parquet_rows import arrow_schema, rows_to_parquet_bytes
from lakehouse import catalog_spec
from quality import layers as L
from quality import testing as qt

JOB_PATH = Path(__file__).resolve().parents[1] / "glue_jobs" / "raw_to_trusted.py"
LAKEHOUSE_TF = Path(__file__).resolve().parents[3] / "infra" / "aws" / "environments" / "dev" / "lakehouse.tf"
REPO = Path(__file__).resolve().parents[4]
BUCKET = "ta-dev-648617469176-lake"
WF = ["--WORKFLOW_NAME", "ta-wf-raw-to-curated", "--WORKFLOW_RUN_ID", "wr_123"]
UTC = timezone.utc


def load():
    spec = importlib.util.spec_from_file_location("glue_job_raw_to_trusted_42", JOB_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def job():
    return load()


# --- S3 y Glue falsos ---------------------------------------------------------------------------


class AccessDenied(Exception):
    """Lo que da S3 a ``ta-glue-role`` al pedir un objeto que no existe: su ``s3:ListBucket`` tiene la
    condición ``s3:prefix``, que no viaja en un ``GetObject`` (403 en vez de 404)."""


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.puts, self.deleted, self.gets = [], [], []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):  # noqa: N803 - firma de boto3
                assert Bucket == BUCKET
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                for i in range(0, max(len(keys), 1), 3):
                    yield {"Contents": [{"Key": k, "Size": len(fake.objects[k])} for k in keys[i:i + 3]]} if keys else {}

        return Paginator()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.gets.append(Key)
        if Key not in self.objects:
            raise AccessDenied(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        assert not Key.startswith("raw/"), Key
        assert not Key.startswith("trusted/_ta_placeholder/"), Key
        self.puts.append(Key)
        self.objects[Key] = Body

    def delete_objects(self, Bucket, Delete):  # noqa: N803
        keys = [o["Key"] for o in Delete["Objects"]]
        assert all(k.startswith("trusted/posts/") for k in keys), keys
        for k in keys:
            self.objects.pop(k, None)
        self.deleted.extend(keys)
        return {}


class FakeGlue:
    def __init__(self, props=None):
        self.props = dict(props or {})
        self.puts = []

    def get_workflow_run_properties(self, Name, RunId):  # noqa: N803
        return {"RunProperties": dict(self.props)}

    def put_workflow_run_properties(self, Name, RunId, RunProperties):  # noqa: N803
        self.puts.append((Name, RunId, dict(RunProperties)))
        self.props.update(RunProperties)


# --- datos de Raw ---------------------------------------------------------------------------------


def ts(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def raw_objects(rows):
    by = defaultdict(list)
    for r in rows:
        by[qt.raw_key(r)].append(r)
    return {k: rows_to_parquet_bytes(v) for k, v in by.items()}


def world_rows():
    """Histórico (2 fuentes, cruza medianoche), stream con una copia, un centinela y filas sin NLP."""
    rows = qt.make_rows("historical", "reddit", 6, ts("2026-05-01T20:00:00"), step=timedelta(hours=2))
    rows += qt.make_rows("historical", "facebook", 4, ts("2026-05-02T08:00:00"), step=timedelta(hours=1))
    stream = qt.make_rows("stream", "instagram", 5, ts("2026-10-23T23:50:00"), step=timedelta(minutes=3))
    no_nlp = qt.make_rows("stream", "facebook", 2, ts("2026-10-21T22:10:00"), step=timedelta(minutes=5))
    for r in no_nlp:
        for c in ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version"):
            r[c] = None
    sentinel = qt.make_row("stream", "reddit", 99, ts("2026-10-22T23:01:00"))
    sentinel["generator"] = "ta-verify@2.1"
    copy = dict(stream[1])  # re-emisión: misma fila, procesada 2 h después y con el score apenas distinto
    copy["process_time"] = iso(datetime.strptime(stream[1]["process_time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
                               + timedelta(hours=2))
    copy["sentiment_score"] = stream[1]["sentiment_score"] + 1e-7
    return rows + stream + no_nlp + [sentinel, copy], stream, copy


@pytest.fixture
def world():
    rows, stream, copy = world_rows()
    objects = raw_objects(rows)
    objects["trusted/"] = b""
    objects["trusted/_ta_placeholder/ta-job-raw-to-trusted/wr_0ab6.json"] = b"{}"
    return {"rows": rows, "stream": stream, "copy": copy, "objects": objects}


def argv(*extra):
    return ["--job-bookmark-option", "job-bookmark-disable", "--lake_bucket", BUCKET, "--ta_region", "us-east-1",
            "--ta_input_prefix", "raw/", "--ta_output_prefix", "trusted/", "--ta_workers", "4", *extra]


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


def trusted_keys(s3):
    return sorted(k for k in s3.objects if k.startswith("trusted/posts/"))


def read(s3, key):
    return pq.read_table(pa.BufferReader(s3.objects[key]))


# --- contrato -------------------------------------------------------------------------------------


def test_columnas_iguales_al_contrato(job):
    assert job.TRUSTED_NAMES == tuple(catalog_spec.table("ta_trusted.posts").column_names)
    assert job.TRUSTED_NAMES == L.TRUSTED_COLUMNS
    assert job.RAW_NAMES == tuple(COLUMN_NAMES)
    assert len(job.TRUSTED_NAMES) == 24 and "source" not in job.TRUSTED_NAMES
    assert job.TRUSTED_NAMES.index("text_clean") == job.TRUSTED_NAMES.index("text") + 1


def test_esquemas_arrow_iguales_a_los_del_proyecto(job):
    assert job.trusted_schema().equals(qt.trusted_arrow_schema())
    expected = {f.name: f.type for f in arrow_schema()}
    assert {f.name: f.type for f in job.raw_schema()} == expected
    assert [f.name for f in job.trusted_schema() if not f.nullable] == ["event_id", "event_time"]


def test_constantes_iguales_a_las_del_profiler(job):
    from quality import checks

    assert job.PROJECTION_START == L.PROJECTION_START
    assert job.SOURCES == L.SOURCES and job.ORIGINS == L.ORIGINS
    assert job.NLP_COLUMNS == L.NLP_COLUMNS
    assert job.iso_to_ms(job.NLP_SINCE) == int(checks.NLP_SINCE.timestamp() * 1000)


# --- text_clean -----------------------------------------------------------------------------------


@pytest.mark.parametrize("text,clean", [
    ("Queda registrado. #Gaseosas", "queda registrado."),
    ("¡Qué rico! 😋", "¡qué rico!"),
    ("Qué sabor 🥤.", "qué sabor."),
    ("Mano, me emociona ver Postobón en Guataép con mi novia. 🥹 ...", "mano, me emociona ver postobón en guataép con mi novia. ..."),
    ("Viva Colombia 🇨🇴🇨🇴 #Colombia #Postobón", "viva colombia"),
    ("Familia 👨‍👩‍👧 feliz 👍🏽 con 1️⃣ Pepsi", "familia feliz con 1 pepsi"),
    ("Mira esto https://ejemplo.com/a?b=1 y www.algo.co ya", "mira esto y ya"),
    ("Hola @usuario_1, ¿ viste ?", "hola, ¿viste?"),
    ("dos  espacios\ty\nsalto", "dos espacios y salto"),
    ("Año ÑANDÚ 100% natural, sin azúcar", "año ñandú 100% natural, sin azúcar"),
    ("correo a#b y #", "correo a#b y #"),
    ("🔥🔥 #Sprite", ""),
    ("", ""),
])
def test_clean_text_ejemplos(job, text, clean):
    assert job.clean_text(text) == clean


def test_clean_text_nulo(job):
    assert job.clean_text(None) is None


def test_clean_text_propiedades(job):
    tricky = ["İstanbul İ", "Straße ß", "क़ख़ क़", "é café", "ﬁ ligadura", "Ⅻ romano", "x‍y",
              "a️b", "control\x00\x07fin", "«cita» (paréntesis) [x] {y}", "… ¿¡!?", "#a#b @c@d"]
    templates = [t.format(b=b) for t in qt.TEMPLATES for b in qt.DISPLAY.values()]
    for text in tricky + templates + [r["text"] for r in world_rows()[0]]:
        clean = job.clean_text(text)
        assert len(clean) <= len(text), text
        assert job.clean_text(clean) == clean, text
        assert clean == clean.strip() and "  " not in clean
        assert not re.search(r"(?<![\w#])#\w", clean), text
        assert not any(unicodedata.category(c) in ("So", "Sk", "Cc", "Cf") for c in clean), text


# --- selección de filas ---------------------------------------------------------------------------


def test_ms_to_dt_en_los_bordes_del_dia(job):
    ms = job.iso_to_ms("2026-10-23T00:00:00Z")
    assert job.ms_to_dt(ms - 1) == "2026-10-22" and job.ms_to_dt(ms) == "2026-10-23"
    assert job.ms_to_dt(ms + job.MS_PER_DAY - 1) == "2026-10-23" and job.ms_to_dt(-1) == "1969-12-31"


def test_select_rows_descarta_deduplica_y_reparte(job):
    d = job.iso_to_ms("2026-10-23T10:00:00Z")
    ids = ["b", "a", "a", "c", None, "d", "e", "f", "a"]
    ev = [d, d, d, d, d, None, d, job.iso_to_ms("2025-12-31T23:00:00Z"), d]
    pr = [d + 5, d + 9, d + 3, d, d, d, d, d, None]
    gen = ["comment-gen@1.0"] * 3 + ["ta-verify@2.1"] + ["comment-gen@1.0"] * 5
    src = ["reddit", "reddit", "reddit", "reddit", "reddit", "reddit", "twitter", "reddit", "reddit"]
    sel = job.select_rows(ids, ev, pr, gen, src)
    assert sel["rejected"] == {"null_event_id": 1, "null_event_time": 1, "invalid_source": 1, "dt_before_projection": 1}
    assert sel["sentinels"] == {"rows": 1, "event_ids": 1, "generators": {"ta-verify@2.1": 1}}
    assert sel["duplicates"] == {"rows_removed": 2, "event_ids": 1}
    assert sel["partitions"] == {("reddit", "2026-10-23"): [2, 0]}  # "a" (menor process_time) y "b", por event_id
    assert sorted(sel["duplicate_groups"]["a"]) == [1, 2, 8]


def test_select_rows_desempata_por_orden_y_process_time_nulo_pierde(job):
    d = job.iso_to_ms("2026-10-23T10:00:00Z")
    sel = job.select_rows(["x", "x", "y", "y"], [d] * 4, [d, d, None, d + 1], ["g@1.0"] * 4, ["facebook"] * 4)
    assert sel["partitions"][("facebook", "2026-10-23")] == [0, 3]


# --- el job de punta a punta ----------------------------------------------------------------------


def test_job_de_punta_a_punta(job, world, capsys):
    s3, glue = FakeS3(world["objects"]), FakeGlue()
    report = job.main(argv(*WF), s3=s3, glue=glue)
    keys = trusted_keys(s3)
    assert keys == sorted({
        "trusted/posts/source=reddit/dt=2026-05-01/part-00000.parquet",
        "trusted/posts/source=reddit/dt=2026-05-02/part-00000.parquet",
        "trusted/posts/source=facebook/dt=2026-05-02/part-00000.parquet",
        "trusted/posts/source=instagram/dt=2026-10-23/part-00000.parquet",
        "trusted/posts/source=instagram/dt=2026-10-24/part-00000.parquet",
        "trusted/posts/source=facebook/dt=2026-10-21/part-00000.parquet",
    })
    c = report["counts"]
    assert c["rows_read"] == len(world["rows"]) == 19
    assert c["sentinels"]["rows"] == 1 and c["duplicates"]["rows_removed"] == 1
    assert c["duplicates"]["groups_with_other_differences"] == 1
    assert c["nlp_missing"]["rows"] == 2 and c["nlp_missing"]["since_nlp_since"] == 0 and c["nlp_missing"]["partial_rows"] == 0
    assert c["rows_written"] == 17 and c["rejected"]["total"] == 0 and report["conciliation"]["ok"]
    assert c["rows_written_by_origin"] == {"historical": 10, "stream": 7}
    assert report["output"]["partitions"] == 6 and report["output"]["partitions_changed"] == 6
    assert report["input"]["by_origin"]["historical"]["rows"] == 10 and report["input"]["skipped_objects"] == 0

    total = 0
    for key in keys:
        table = read(s3, key)
        assert table.schema.equals(qt.trusted_arrow_schema()), key
        ids = table.column("event_id").to_pylist()
        assert ids == sorted(ids) and len(set(ids)) == len(ids)
        dt = re.search(r"dt=([\d-]+)/", key).group(1)
        assert all(t.astimezone(UTC).strftime("%Y-%m-%d") == dt for t in table.column("event_time").to_pylist())
        assert all(g == "comment-gen@1.0" for g in table.column("generator").to_pylist())
        texts, cleans = table.column("text").to_pylist(), table.column("text_clean").to_pylist()
        assert cleans == [job.clean_text(t) for t in texts]
        total += table.num_rows
    assert total == 17

    # la copia se resuelve por el menor process_time
    inst = read(s3, "trusted/posts/source=instagram/dt=2026-10-23/part-00000.parquet").to_pylist()
    kept = next(r for r in inst if r["event_id"] == world["copy"]["event_id"])
    assert iso(kept["process_time"]) == world["stream"][1]["process_time"]

    report_keys = [k for k in s3.puts if k.startswith("quality/")]
    assert len(report_keys) == 2 and "quality/_latest/raw_to_trusted.json" in report_keys
    dated = next(k for k in report_keys if "_latest" not in k)
    assert re.match(r"quality/\d{4}-\d{2}-\d{2}/raw_to_trusted/wr_123\.json$", dated)
    stored = json.loads(s3.objects[dated])
    assert stored["counts"] == json.loads(json.dumps(c)) and stored["report_key"] == dated
    assert glue.puts == [("ta-wf-raw-to-curated", "wr_123", {"ta_trusted_report": dated, "ta_trusted_rows": "17",
                                                             "ta_trusted_partitions_changed": "6"})]
    events = logs(capsys)
    assert [e["event"] for e in events] == ["glue_job_start", "glue_trusted_read", "glue_job_done"]
    done = events[-1]
    assert done["rows_written"] == 17 and done["report"] == dated and done["conciliation_ok"] is True
    assert "lake_bucket" in events[0]["arg_keys"] and events[0]["environment"]["pyarrow"] == pa.__version__
    assert s3.objects["trusted/_ta_placeholder/ta-job-raw-to-trusted/wr_0ab6.json"] == b"{}" and "trusted/" in s3.objects
    assert not any(k.startswith("raw/") for k in s3.puts + s3.deleted)
    # primera corrida: el reporte anterior no existe y no se pide (daría AccessDenied, no NoSuchKey)
    assert "quality/_latest/raw_to_trusted.json" not in s3.gets


def test_reporte_sin_valores_sensibles(job, world, capsys):
    s3 = FakeS3(world["objects"])
    job.main(argv(), s3=s3, glue=FakeGlue())
    text = s3.objects["quality/_latest/raw_to_trusted.json"].decode("utf-8")
    for r in world["rows"]:
        assert r["event_id"] not in text and r["user_hash"] not in text and r["text"] not in text
    capsys.readouterr()


def test_idempotente_mismos_bytes(job, world, capsys):
    s3 = FakeS3(world["objects"])
    job.main(argv(*WF), s3=s3, glue=FakeGlue())
    first = {k: hashlib.sha256(s3.objects[k]).hexdigest() for k in trusted_keys(s3)}
    s3.puts.clear()
    second = job.main(argv(*WF), s3=s3, glue=FakeGlue())
    assert {k: hashlib.sha256(s3.objects[k]).hexdigest() for k in trusted_keys(s3)} == first
    assert second["output"]["partitions_changed"] == 0 and second["output"]["objects_deleted"] == 0
    assert second["previous_report"] == "wr_123" and s3.deleted == []
    assert sorted(k for k in s3.puts if k.startswith("trusted/")) == sorted(first)
    assert {f["key"]: f["sha256"] for f in second["output"]["files"]} == first
    capsys.readouterr()


def test_borra_lo_que_sobra_solo_en_trusted_posts(job, world, capsys):
    objects = dict(world["objects"])
    objects["trusted/posts/"] = b""
    objects["trusted/posts/source=reddit/dt=2026-01-05/part-00000.parquet"] = b"PAR1viejoPAR1"
    objects["trusted/posts/source=reddit/dt=2026-05-01/part-00001.parquet"] = b"PAR1otroPAR1"
    s3 = FakeS3(objects)
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert sorted(s3.deleted) == ["trusted/posts/source=reddit/dt=2026-01-05/part-00000.parquet",
                                  "trusted/posts/source=reddit/dt=2026-05-01/part-00001.parquet"]
    assert report["output"]["objects_deleted"] == 2 and "trusted/posts/" in s3.objects
    assert "trusted/_ta_placeholder/ta-job-raw-to-trusted/wr_0ab6.json" in s3.objects
    capsys.readouterr()


def test_cambios_frente_al_reporte_anterior(job, world, capsys):
    s3 = FakeS3(world["objects"])
    job.main(argv(), s3=s3, glue=FakeGlue())
    prev = json.loads(s3.objects["quality/_latest/raw_to_trusted.json"])
    prev["output"]["files"][0]["sha256"] = "0" * 64
    prev["output"]["files"].append({"key": "trusted/posts/source=reddit/dt=2026-02-01/part-00000.parquet", "sha256": "1"})
    s3.objects["quality/_latest/raw_to_trusted.json"] = json.dumps(prev).encode()
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["output"]["partitions_changed"] == 1 and report["output"]["partitions_removed"] == 1
    assert report["output"]["changed_keys"] == [prev["output"]["files"][0]["key"]]
    assert report["output"]["removed_keys"] == ["trusted/posts/source=reddit/dt=2026-02-01/part-00000.parquet"]
    capsys.readouterr()


@pytest.mark.parametrize("value", ["raw_to_trusted", "ta-job-raw-to-trusted", "all"])
def test_fallo_provocado_antes_de_escribir(job, world, value, capsys):
    s3 = FakeS3(world["objects"])
    with pytest.raises(RuntimeError, match="ta_force_fail"):
        job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
    assert s3.puts == [] and s3.deleted == []
    assert "glue_job_forced_failure" in [e["event"] for e in logs(capsys)]


def test_fallo_provocado_por_argumento_y_otro_job_no(job, world, capsys):
    with pytest.raises(RuntimeError):
        job.main(argv("--ta_force_fail", "true"), s3=FakeS3(world["objects"]), glue=FakeGlue())
    s3 = FakeS3(world["objects"])
    job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": "trusted_to_curated"}))
    assert trusted_keys(s3)
    capsys.readouterr()


def test_dry_run_no_escribe(job, world, capsys):
    s3, glue = FakeS3(world["objects"]), FakeGlue()
    report = job.main(argv("--ta_dry_run", "true", *WF), s3=s3, glue=glue)
    assert s3.puts == [] and s3.deleted == [] and glue.puts == []
    assert report["dry_run"] and report["counts"]["rows_written"] == 17 and report["output"]["objects_written"] == 0
    capsys.readouterr()


def test_parquet_ilegible_o_sin_columnas_detiene_el_job(job, world, capsys):
    bad_key = "raw/origin=stream/source=reddit/dt=2026-10-23/hr=10/part-20261023T100000Z-p000.parquet"
    s3 = FakeS3({**world["objects"], bad_key: b"PAR1basuraPAR1"})
    with pytest.raises(job.RawSchemaError, match=re.escape(bad_key)):
        job.main(argv(), s3=s3, glue=FakeGlue())
    assert s3.puts == []
    table = pq.read_table(pa.BufferReader(rows_to_parquet_bytes([qt.make_row("stream", "reddit", 1, ts("2026-10-23T10:00:00"))])))
    buf = io.BytesIO()
    pq.write_table(table.drop(["payload"]), buf)
    s3 = FakeS3({bad_key: buf.getvalue()})
    with pytest.raises(job.RawSchemaError, match="payload"):
        job.main(argv(), s3=s3, glue=FakeGlue())
    capsys.readouterr()


def test_corrige_tipos(job, capsys):
    row = qt.make_row("historical", "facebook", 7, ts("2026-06-01T12:34:56"))
    table = pq.read_table(pa.BufferReader(rows_to_parquet_bytes([row])))
    cols = {}
    for name in table.column_names:
        col = table.column(name)
        if pa.types.is_timestamp(col.type):
            col = col.cast(pa.timestamp("us"))  # sin zona y en microsegundos
        elif name == "likes":
            col = col.cast(pa.int32())
        cols[name] = col
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    key = qt.raw_key(row)
    s3 = FakeS3({key: buf.getvalue()})
    job.main(argv(), s3=s3, glue=FakeGlue())
    out = read(s3, "trusted/posts/source=facebook/dt=2026-06-01/part-00000.parquet")
    assert out.schema.field("event_time").type == pa.timestamp("ms", tz="UTC") and out.schema.field("likes").type == pa.int64()
    got = out.to_pylist()[0]
    assert iso(got["event_time"]) == row["event_time"] and got["likes"] == row["likes"]
    capsys.readouterr()


def test_claves_fuera_del_layout_se_omiten(job, world, capsys):
    extra = {"raw/origin=stream/_SUCCESS": b"", "raw/origin=stream/source=reddit/dt=2026-10-23/hr=10/notas.txt": b"x",
             "raw/origin=stream/": b"", "raw/origin=otro/source=reddit/dt=2026-10-23/hr=10/x.parquet": b"PAR1"}
    s3 = FakeS3({**world["objects"], **extra})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["input"]["skipped_objects"] == 2 and report["counts"]["rows_read"] == 19
    assert "raw/origin=otro/source=reddit/dt=2026-10-23/hr=10/x.parquet" not in s3.gets
    capsys.readouterr()


def test_salida_dentro_de_raw_se_rechaza(job, world, capsys):
    with pytest.raises(RuntimeError, match="Raw"):
        job.main(argv("--ta_output_prefix", "raw/trusted/"), s3=FakeS3(world["objects"]), glue=FakeGlue())
    capsys.readouterr()


def test_raw_vacio_no_escribe_datos(job, capsys):
    s3 = FakeS3({"trusted/posts/source=reddit/dt=2026-05-01/part-00000.parquet": b"PAR1PAR1"})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["counts"]["rows_read"] == 0 and report["output"]["partitions"] == 0
    assert report["output"]["objects_deleted"] == 1 and trusted_keys(s3) == []
    capsys.readouterr()


# --- en local (carpetas) y con el profiler del 5.1 ------------------------------------------------


def test_local_con_carpetas_y_profiler_del_51(job, tmp_path, capsys):
    from quality.profiler import find_group, profile_layer

    src, dst = tmp_path / "src", tmp_path / "dst"
    rows, _, _ = world_rows()
    qt.write_raw_layer(src, rows)
    report = job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(src), "--ta_local_dst", str(dst), "--ta_workers", "2"])
    assert report["counts"]["rows_written"] == 17 and (dst / "quality" / "_latest" / "raw_to_trusted.json").exists()
    assert not (src / "trusted").exists()
    prof = profile_layer(dst, "trusted", as_of=datetime(2026, 10, 26, tzinfo=UTC))
    assert prof["files"]["recognized"] == 6 and prof["files"]["unrecognized"] == 0
    assert all(v["matches_contract"] for v in prof["files"]["schema_variants"])
    assert not [a for a in prof["anomalies"] if a["severity"] == "error"]
    status = {r["id"]: r["status"] for r in prof["rules"]}
    for rule in ("DQ-01", "DQ-49", "DQ-54", "DQ-56", "DQ-57", "DQ-63"):
        assert status[rule] == "cumple", rule
    assert find_group(prof)["rows"] == 17
    capsys.readouterr()


def test_local_rechaza_destino_dentro_del_repo(job, tmp_path):
    with pytest.raises(RuntimeError, match="repositorio"):
        job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(tmp_path), "--ta_local_dst", str(REPO / "project" / "x")])
    with pytest.raises(RuntimeError, match="ta_local_dst"):
        job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(tmp_path)])


def test_terraform_le_da_1_dpu(job):
    tf = LAKEHOUSE_TF.read_text(encoding="utf-8")
    block = tf.split('key         = "raw_to_trusted"', 1)[1].split("},", 1)[0]
    assert re.search(r"^\s*max_capacity\s*=\s*1\s*$", block, flags=re.M)
    other = tf.split('key         = "trusted_to_curated"', 1)[1].split("},", 1)[0]
    assert "max_capacity" not in other
