"""Job real Trusted → Curated (Lote 4.3, HU-027) con un S3 y un Glue falsos.

El Trusted de entrada lo escribe el job real del 4.2 (``raw_to_trusted.py`` en modo local) a partir de
Raw sintético de ``quality.testing``: así se prueba la cadena con el formato real de Trusted. Lo esperado
se recalcula aquí con código aparte (sin las funciones del job)."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from lakehouse import catalog_spec
from quality import layers as L
from quality import testing as qt

JOBS = Path(__file__).resolve().parents[1] / "glue_jobs"
PROJECT = Path(__file__).resolve().parents[3]
LAKEHOUSE_TF = PROJECT / "infra" / "aws" / "environments" / "dev" / "lakehouse.tf"
QUERIES = PROJECT / "src" / "lakehouse" / "queries"
BUCKET = "ta-dev-648617469176-lake"
WF = ["--WORKFLOW_NAME", "ta-wf-raw-to-curated", "--WORKFLOW_RUN_ID", "wr_123"]
UTC = timezone.utc
NLP = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version")
ATHENA_TO_ARROW = {
    "string": pa.string(), "int": pa.int32(), "bigint": pa.int64(), "double": pa.float64(), "date": pa.date32(),
    "boolean": pa.bool_(), "timestamp": pa.timestamp("ms", tz="UTC"),
    "array<string>": pa.list_(pa.field("element", pa.string())),
}


def load(name):
    spec = importlib.util.spec_from_file_location(f"glue_job_{name}_43", JOBS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def job():
    return load("trusted_to_curated")


# --- S3 y Glue falsos ---------------------------------------------------------------------------


class AccessDenied(Exception):
    """Lo que da S3 a ``ta-glue-role`` al pedir un objeto que no existe (403, no 404: §3.21.1)."""


TABLE_PREFIXES = tuple(f"curated/{t}/" for t in ("fact_posts", "dim_date", "dim_source", "dim_brand", "dim_user",
                                                  "agg_emotion_by_brand_hour", "agg_trend_daily"))


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
                for i in range(0, max(len(keys), 1), 7):
                    yield {"Contents": [{"Key": k, "Size": len(fake.objects[k])} for k in keys[i:i + 7]]} if keys else {}

        return Paginator()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.gets.append(Key)
        if Key not in self.objects:
            raise AccessDenied(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        assert Key.startswith(TABLE_PREFIXES) or Key.startswith("quality/"), Key
        self.puts.append(Key)
        self.objects[Key] = Body

    def delete_objects(self, Bucket, Delete):  # noqa: N803
        keys = [o["Key"] for o in Delete["Objects"]]
        assert all(k.startswith(TABLE_PREFIXES) for k in keys), keys
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


# --- Trusted de entrada (escrito por el job del 4.2) ------------------------------------------------


def ts(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


def raw_world():
    """Histórico que cruza medianoche, stream sin NLP, una comparativa, una fila en el borde de la hora."""
    rows = qt.make_rows("historical", "reddit", 6, ts("2026-05-01T20:00:00"), step=timedelta(hours=2))
    rows += qt.make_rows("historical", "facebook", 4, ts("2026-05-03T08:00:00"), step=timedelta(hours=1))
    rows += qt.make_rows("stream", "instagram", 5, ts("2026-10-23T23:50:00"), step=timedelta(minutes=3))
    no_nlp = qt.make_rows("stream", "facebook", 3, ts("2026-10-21T22:10:00"), step=timedelta(minutes=5))
    for r in no_nlp:
        for c in NLP:
            r[c] = None
    rows += no_nlp
    edge = qt.make_row("stream", "reddit", 50, ts("2026-10-23T23:00:00"))
    edge["brands"], edge["brand_primary"] = ["pepsi", "postobon"], "pepsi"
    edge["sentiment_label"], edge["emotion_label"] = "neutral", "joy"
    rows.append(edge)
    return rows


def build_trusted(tmp):
    raw_dir, trusted_dir = tmp / "raw_src", tmp / "trusted_dst"
    qt.write_raw_layer(raw_dir, raw_world())
    load("raw_to_trusted").main(["--lake_bucket", BUCKET, "--ta_local_src", str(raw_dir), "--ta_local_dst", str(trusted_dir),
                                 "--ta_workers", "2"])
    return {p.relative_to(trusted_dir).as_posix(): p.read_bytes() for p in (trusted_dir / "trusted").rglob("*.parquet")}


@pytest.fixture(scope="module")
def trusted(tmp_path_factory):
    return build_trusted(tmp_path_factory.mktemp("t43"))


@pytest.fixture
def world(trusted):
    objects = dict(trusted)
    objects["curated/"] = b""
    objects["curated/_ta_placeholder/ta-job-trusted-to-curated/wr_0ab6.json"] = b"{}"
    objects["curated/edges_user_brand/part-00000.parquet"] = b"PAR1aristasPAR1"
    return objects


def trusted_rows(trusted):
    out = []
    for key, data in sorted(trusted.items()):
        m = re.search(r"source=([^/]+)/dt=([^/]+)/", key)
        table = pq.read_table(pa.BufferReader(data))
        for i, r in enumerate(table.to_pylist()):
            r["source"], r["dt"] = m.group(1), m.group(2)
            r["event_ms"] = table.column("event_time").cast(pa.int64())[i].as_py()
            out.append(r)
    return out


def argv(*extra):
    return ["--job-bookmark-option", "job-bookmark-disable", "--lake_bucket", BUCKET, "--ta_region", "us-east-1",
            "--ta_input_prefix", "trusted/", "--ta_output_prefix", "curated/", "--ta_workers", "3", *extra]


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


def curated_keys(s3, table=None):
    prefix = f"curated/{table}/" if table else TABLE_PREFIXES
    return sorted(k for k in s3.objects if k.startswith(prefix))


def read_table(s3, table):
    keys = curated_keys(s3, table)
    return pa.concat_tables([pq.read_table(pa.BufferReader(s3.objects[k])) for k in keys])


# --- contrato -------------------------------------------------------------------------------------


def test_tablas_y_tipos_iguales_al_contrato(job):
    specs = {s.name: s for s in catalog_spec.load_contract_tables() if s.database == "ta_curated"}
    assert set(job.TABLE_NAMES) | set(job.EDGE_TABLES) == set(specs)
    for name in job.TABLE_NAMES:
        spec = specs[name]
        assert [c[0] for c in job.TABLE_COLUMNS[name]] == list(spec.column_names), name
        schema = job.table_schema(name)
        assert [(f.name, f.type) for f in schema] == [(c.name, ATHENA_TO_ARROW[c.type]) for c in spec.columns], name
    assert specs["fact_posts"].partition_names == ["dt"] and "dt" not in job.table_schema("fact_posts").names


def test_columnas_leidas_existen_en_trusted_con_su_tipo(job):
    trusted = {f.name: f.type for f in qt.trusted_arrow_schema()}
    assert set(job.READ_NAMES) <= set(catalog_spec.table("ta_trusted.posts").column_names)
    assert all(job.read_schema().field(n).type == trusted[n] for n in job.READ_NAMES)
    fact = {n for n, _, _ in job.FACT_COLUMNS} - {"date_key", "source"}
    assert fact <= set(job.READ_NAMES)


def test_constantes_iguales_a_las_del_proyecto(job):
    from common import label_map

    assert job.SOURCES == L.SOURCES and job.ORIGINS == L.ORIGINS and job.PROJECTION_START == L.PROJECTION_START
    assert job.SENTIMENT_LABELS == label_map.SENTIMENT_LABELS and job.EMOTION_LABELS == label_map.EMOTION_LABELS
    assert job.NLP_COLUMNS == load("raw_to_trusted").NLP_COLUMNS


def test_dim_brand_igual_a_brands_yaml(job):
    data = yaml.safe_load((PROJECT / "src" / "simulator" / "config" / "brands.yaml").read_text(encoding="utf-8"))["brands"]
    assert sorted(job.BRAND_ROWS) == sorted((k, v["display"], v["role"]) for k, v in data.items())


def test_dim_source_igual_a_sim_yaml(job):
    kinds = yaml.safe_load((PROJECT / "src" / "simulator" / "config" / "sim.yaml").read_text(encoding="utf-8"))["kinds"]
    assert sorted(s for s, _, _ in job.SOURCE_ROWS) == sorted(job.SOURCES) == sorted(kinds)
    for source, display, mix in job.SOURCE_ROWS:
        post = round(kinds[source]["post"] * 100)
        assert display.lower() == source
        assert mix == (f"post {post}% · comment {100 - post}%" if post else "comment 100%")


def test_sentimiento_con_signo_igual_a_trend_windows(job):
    from streaming.trends_core import signed_sentiment

    for label in ("positive", "negative", "neutral", None, "otro"):
        for score in (0.7, None):
            assert job.signed_sentiment(label, score) == signed_sentiment(label, score)
    assert job.avg_sentiment([]) is None
    assert job.avg_sentiment([0.1] * 10) == 0.1  # fsum: sin el error de sumar 0.1 diez veces
    assert job.avg_sentiment([1.0, 1.0, -0.4]) == round(1.6 / 3, 6)
    assert job.avg_sentiment([0.3, -0.2]) == job.avg_sentiment([-0.2, 0.3])


def test_date_rows_calendario_continuo(job):
    rows = job.date_rows({"2026-10-30", "2026-11-02"})
    assert rows["date_key"] == [20261030, 20261031, 20261101, 20261102]
    assert rows["day_of_week"] == [5, 6, 7, 1] and rows["is_weekend"] == [False, True, True, False]
    assert rows["iso_week"] == [44, 44, 44, 45] and rows["full_date"][0] == date(2026, 10, 30)
    assert job.date_rows(set())["date_key"] == [] and job.date_key("2026-04-27") == 20260427


def test_check_not_null_evita_el_parquet_corrupto_de_pyarrow_7(job):
    # Con pyarrow 7 un campo no nulo con nulos se escribe sin error y sale ilegible (comprobado en v2.37).
    with pytest.raises(job.CuratedBuildError, match="user_hash"):
        job.table_from_columns("dim_user", {"user_hash": [None], "first_seen": [0], "last_seen": [0], "n_posts": [1],
                                            "max_followers": [None]})
    t = job.table_from_columns("dim_user", {"user_hash": ["a"], "first_seen": [0], "last_seen": [3600000], "n_posts": [1],
                                            "max_followers": [None]})
    assert t.schema.field("first_seen").type == pa.timestamp("ms", tz="UTC") and t.column("max_followers").null_count == 1


# --- el job de punta a punta ----------------------------------------------------------------------


def expected_from(rows):
    """Lo esperado, calculado aparte de la lógica del job."""
    trend = defaultdict(lambda: [0, 0, 0, 0, []])
    emotion, users = Counter(), {}
    for r in rows:
        t = trend[(r["dt"], r["brand_primary"])]
        t[0] += 1
        lab = r["sentiment_label"]
        if lab:
            t[1 + ["negative", "neutral", "positive"].index(lab)] += 1
            t[4].append({"positive": 1, "negative": -1, "neutral": 0}[lab] * (r["sentiment_score"] if lab != "neutral" else 0))
        if r["emotion_label"]:
            hour = datetime.fromtimestamp(r["event_ms"] // 1000, UTC).replace(minute=0, second=0)
            emotion[(hour, r["brand_primary"], r["source"], r["emotion_label"])] += 1
        u = users.setdefault(r["user_hash"], [r["event_ms"], r["event_ms"], 0, None])
        u[0], u[1], u[2] = min(u[0], r["event_ms"]), max(u[1], r["event_ms"]), u[2] + 1
        u[3] = r["followers"] if u[3] is None else max(u[3], r["followers"])
    return trend, emotion, users


def test_job_de_punta_a_punta(job, trusted, world, capsys):
    rows = trusted_rows(trusted)
    s3, glue = FakeS3(world), FakeGlue({"ta_trusted_report": "quality/x/raw_to_trusted/wr_123.json",
                                         "ta_trusted_rows": str(len(rows))})
    report = job.main(argv(*WF), s3=s3, glue=glue)
    dts = sorted({r["dt"] for r in rows})
    assert curated_keys(s3, "fact_posts") == [f"curated/fact_posts/dt={d}/part-00000.parquet" for d in dts]
    for name in job.TABLE_NAMES[1:]:
        assert curated_keys(s3, name) == [f"curated/{name}/part-00000.parquet"], name

    # fact_posts: una fila por event_id, date_key de dt, ordenada por event_id, NLP nulo donde no hay NLP
    fact = read_table(s3, "fact_posts")
    assert fact.num_rows == len(rows) == report["counts"]["fact_rows"] and report["input"]["trusted_rows_match"] is True
    for key in curated_keys(s3, "fact_posts"):
        part = pq.read_table(pa.BufferReader(s3.objects[key]))
        assert part.schema.equals(job.table_schema("fact_posts")), key
        ids = part.column("event_id").to_pylist()
        assert ids == sorted(ids)
        dt = re.search(r"dt=([\d-]+)/", key).group(1)
        assert set(part.column("date_key").to_pylist()) == {int(dt.replace("-", ""))}
    by_id = {r["event_id"]: r for r in fact.to_pylist()}
    assert len(by_id) == len(rows)
    for r in rows:
        f = by_id[r["event_id"]]
        assert f["source"] == r["source"] and f["origin"] == r["origin"] and f["brands"] == r["brands"]
        assert f["sentiment_label"] == r["sentiment_label"] and f["user_hash"] == r["user_hash"]
    no_nlp = [r for r in rows if all(r[c] is None for c in NLP)]
    assert len(no_nlp) == 3 and report["counts"]["nlp_missing"] == 3

    trend, emotion, users = expected_from(rows)
    got = {(r["full_date"].isoformat(), r["brand"]): r for r in read_table(s3, "agg_trend_daily").to_pylist()}
    assert set(got) == set(trend)
    for k, (n, neg, neu, pos, vals) in trend.items():
        g = got[k]
        assert (g["n_posts"], g["n_negative"], g["n_neutral"], g["n_positive"]) == (n, neg, neu, pos), k
        assert (g["avg_sentiment"] is None) == (not vals)
        if vals:
            assert g["avg_sentiment"] == pytest.approx(sum(vals) / len(vals), abs=1e-6)
    # decisión 2 (v2.37): los días con filas sin NLP cuentan en n_posts pero no en el sentimiento
    short = {k: g for k, g in got.items() if g["n_posts"] != g["n_negative"] + g["n_neutral"] + g["n_positive"]}
    assert {k[0] for k in short} == {"2026-10-21"}
    assert sum(g["n_posts"] - g["n_negative"] - g["n_neutral"] - g["n_positive"] for g in got.values()) == 3

    emo = read_table(s3, "agg_emotion_by_brand_hour").to_pylist()
    assert {(e["hour_start"], e["brand"], e["source"], e["emotion_label"]): e["n_posts"] for e in emo} == dict(emotion)
    assert sum(e["n_posts"] for e in emo) == len(rows) - 3  # sin las filas sin emoción
    edge = next(e for e in emo if e["source"] == "reddit" and e["hour_start"] == ts("2026-10-23T23:00:00"))
    assert edge["brand"] == "pepsi" and edge["emotion_label"] == "joy"

    du = read_table(s3, "dim_user")
    assert du.column("user_hash").to_pylist() == sorted(users)
    for r in du.to_pylist():
        u = users[r["user_hash"]]
        assert (round(r["first_seen"].timestamp() * 1000), round(r["last_seen"].timestamp() * 1000), r["n_posts"],
                r["max_followers"]) == tuple(u)

    dd = read_table(s3, "dim_date").to_pylist()
    first, last = date.fromisoformat(dts[0]), date.fromisoformat(dts[-1])
    assert len(dd) == (last - first).days + 1 and dd[0]["full_date"] == first and dd[-1]["full_date"] == last
    assert [b["brand"] for b in read_table(s3, "dim_brand").to_pylist()] == ["pepsi", "postobon", "sprite"]
    assert read_table(s3, "dim_source").column("source").to_pylist() == ["facebook", "instagram", "reddit"]
    for name in job.TABLE_NAMES:
        assert read_table(s3, name).schema.equals(job.table_schema(name)), name

    assert all(c["ok"] for c in report["checks"].values()) and report["conciliation"]["ok"]
    assert report["checks"]["trend_rows_without_sentiment"] == {"expected": 3, "observed": 3, "ok": True}
    assert report["output"]["objects_written"] == report["output"]["objects"] == len(dts) + 6
    assert report["output"]["files_changed"] == len(dts) + 6 and report["output"]["objects_deleted"] == 0
    reports = [k for k in s3.puts if k.startswith("quality/")]
    dated = next(k for k in reports if "_latest" not in k)
    assert re.match(r"quality/\d{4}-\d{2}-\d{2}/trusted_to_curated/wr_123\.json$", dated)
    assert "quality/_latest/trusted_to_curated.json" in reports
    assert glue.puts == [("ta-wf-raw-to-curated", "wr_123", {"ta_curated_report": dated, "ta_curated_rows": str(len(rows)),
                                                             "ta_curated_files_changed": str(len(dts) + 6)})]
    events = logs(capsys)
    assert [e["event"] for e in events] == ["glue_job_start", "glue_curated_read", "glue_job_done"]
    assert events[-1]["fact_rows"] == len(rows) and events[-1]["tables"]["dim_source"] == 3
    # lo que no es suyo, intacto; y la primera corrida no pide el reporte anterior (daría 403)
    assert s3.objects["curated/edges_user_brand/part-00000.parquet"] == b"PAR1aristasPAR1"
    assert "curated/_ta_placeholder/ta-job-trusted-to-curated/wr_0ab6.json" in s3.objects and "curated/" in s3.objects
    assert "quality/_latest/trusted_to_curated.json" not in s3.gets
    assert not any(k.startswith(("trusted/", "raw/")) for k in s3.puts + s3.deleted)


def test_reporte_sin_valores_sensibles(job, trusted, world, capsys):
    s3 = FakeS3(world)
    job.main(argv(), s3=s3, glue=FakeGlue())
    text = s3.objects["quality/_latest/trusted_to_curated.json"].decode("utf-8")
    for r in trusted_rows(trusted):
        assert r["event_id"] not in text and r["user_hash"] not in text
    capsys.readouterr()


def test_idempotente_solo_escribe_lo_que_cambia(job, world, capsys):
    s3 = FakeS3(world)
    first = job.main(argv(*WF), s3=s3, glue=FakeGlue())
    shas = {k: hashlib.sha256(s3.objects[k]).hexdigest() for k in curated_keys(s3)}
    s3.puts.clear()
    second = job.main(argv(*WF), s3=s3, glue=FakeGlue())
    assert {k: hashlib.sha256(s3.objects[k]).hexdigest() for k in curated_keys(s3)} == shas
    assert second["output"]["objects_written"] == 0 and second["output"]["objects_unchanged"] == first["output"]["objects"]
    assert second["output"]["files_changed"] == 0 and second["output"]["objects_deleted"] == 0
    assert second["previous_report"] == "wr_123" and [k for k in s3.puts if not k.startswith("quality/")] == []
    assert {f["key"]: f["sha256"] for f in second["output"]["files"]} == shas
    # un objeto que falta en S3 (o con otro tamaño) se vuelve a subir aunque el reporte diga lo mismo
    missing = curated_keys(s3, "dim_user")[0]
    del s3.objects[missing]
    s3.objects[curated_keys(s3, "dim_date")[0]] += b"x"
    third = job.main(argv(), s3=s3, glue=FakeGlue())
    assert third["output"]["objects_written"] == 2 and third["output"]["files_changed"] == 0
    assert hashlib.sha256(s3.objects[missing]).hexdigest() == shas[missing]
    capsys.readouterr()


def test_borra_lo_que_sobra_solo_en_sus_tablas(job, world, capsys):
    objects = dict(world)
    extra = {"curated/fact_posts/dt=2026-01-05/part-00000.parquet": b"PAR1viejoPAR1",
             "curated/dim_user/part-00001.parquet": b"PAR1otroPAR1",
             "curated/edges_hashtag_cooccurrence/part-00000.parquet": b"PAR1",
             "curated/fact_posts/": b""}
    s3 = FakeS3({**objects, **extra})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert sorted(s3.deleted) == ["curated/dim_user/part-00001.parquet", "curated/fact_posts/dt=2026-01-05/part-00000.parquet"]
    assert report["output"]["objects_deleted"] == 2 and "curated/fact_posts/" in s3.objects
    assert "curated/edges_hashtag_cooccurrence/part-00000.parquet" in s3.objects
    capsys.readouterr()


def test_cambios_frente_al_reporte_anterior(job, world, capsys):
    s3 = FakeS3(world)
    job.main(argv(), s3=s3, glue=FakeGlue())
    prev = json.loads(s3.objects["quality/_latest/trusted_to_curated.json"])
    prev["output"]["files"][0]["sha256"] = "0" * 64
    prev["output"]["files"].append({"key": "curated/fact_posts/dt=2026-02-01/part-00000.parquet", "sha256": "1"})
    s3.objects["quality/_latest/trusted_to_curated.json"] = json.dumps(prev).encode()
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["output"]["files_changed"] == 1 and report["output"]["objects_written"] == 1
    assert report["output"]["changed_keys"] == [prev["output"]["files"][0]["key"]]
    assert report["output"]["removed_keys"] == ["curated/fact_posts/dt=2026-02-01/part-00000.parquet"]
    capsys.readouterr()


@pytest.mark.parametrize("value", ["trusted_to_curated", "ta-job-trusted-to-curated", "all"])
def test_fallo_provocado_antes_de_leer(job, world, value, capsys):
    s3 = FakeS3(world)
    with pytest.raises(RuntimeError, match="ta_force_fail"):
        job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
    assert s3.puts == [] and s3.deleted == [] and s3.gets == []
    assert "glue_job_forced_failure" in [e["event"] for e in logs(capsys)]


def test_fallo_de_otro_job_no_lo_detiene(job, world, capsys):
    s3 = FakeS3(world)
    job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": "raw_to_trusted"}))
    assert curated_keys(s3, "fact_posts")
    with pytest.raises(RuntimeError):
        job.main(argv("--ta_force_fail", "true"), s3=FakeS3(world), glue=FakeGlue())
    capsys.readouterr()


def test_dry_run_no_escribe(job, world, capsys):
    s3, glue = FakeS3(world), FakeGlue()
    report = job.main(argv("--ta_dry_run", "true", *WF), s3=s3, glue=glue)
    assert s3.puts == [] and s3.deleted == [] and glue.puts == []
    assert report["dry_run"] and report["output"]["objects_written"] == 0 and report["counts"]["fact_rows"] > 0
    capsys.readouterr()


def test_trusted_sin_columnas_o_ilegible_detiene_el_job(job, world, capsys):
    key = next(k for k in world if k.startswith("trusted/posts/"))
    table = pq.read_table(pa.BufferReader(world[key]))
    buf = io.BytesIO()
    pq.write_table(table.drop(["user_hash"]), buf)
    s3 = FakeS3({**world, key: buf.getvalue()})
    with pytest.raises(job.TrustedSchemaError, match=re.escape(key) + ".*user_hash"):
        job.main(argv(), s3=s3, glue=FakeGlue())
    assert s3.puts == []
    s3 = FakeS3({**world, key: b"PAR1basuraPAR1"})
    with pytest.raises(job.TrustedSchemaError, match="Parquet"):
        job.main(argv(), s3=s3, glue=FakeGlue())
    capsys.readouterr()


def test_tipos_de_trusted_se_corrigen(job, world, capsys):
    key = next(k for k in world if k.startswith("trusted/posts/"))
    table = pq.read_table(pa.BufferReader(world[key]))
    cols = {n: (table.column(n).cast(pa.timestamp("us")) if pa.types.is_timestamp(table.column(n).type)
                else table.column(n).cast(pa.int32()) if n == "likes" else table.column(n)) for n in table.column_names}
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    s3 = FakeS3({**world, key: buf.getvalue()})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["counts"]["fact_rows"] == len(trusted_rows({k: v for k, v in world.items() if k.startswith("trusted/")}))
    capsys.readouterr()


def test_duplicado_entre_particiones_no_concilia_y_no_escribe(job, world, capsys):
    keys = sorted(k for k in world if k.startswith("trusted/posts/"))
    a = pq.read_table(pa.BufferReader(world[keys[0]]))
    b = pq.read_table(pa.BufferReader(world[keys[1]]))
    ids = b.column("event_id").to_pylist()
    ids[0] = a.column("event_id")[0].as_py()
    b = b.set_column(b.schema.get_field_index("event_id"), b.schema.field("event_id"), pa.array(ids, pa.string()))
    buf = io.BytesIO()
    pq.write_table(b, buf)
    s3 = FakeS3({**world, keys[1]: buf.getvalue()})
    with pytest.raises(job.CuratedBuildError, match="unique_event_id"):
        job.main(argv(), s3=s3, glue=FakeGlue())
    assert s3.puts == [] and s3.deleted == []
    capsys.readouterr()


def test_marca_desconocida_entra_en_dim_brand_y_se_cuenta(job, world, capsys):
    key = next(k for k in world if k.startswith("trusted/posts/"))
    table = pq.read_table(pa.BufferReader(world[key]))
    idx = table.schema.get_field_index("brand_primary")
    table = table.set_column(idx, table.schema.field(idx), pa.array(["colombiana"] * table.num_rows, pa.string()))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3 = FakeS3({**world, key: buf.getvalue()})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    brands = read_table(s3, "dim_brand").to_pylist()
    assert {"brand": "colombiana", "display": None, "role": None} in brands and report["counts"]["unknown_brands"] == ["colombiana"]
    capsys.readouterr()


def test_claves_fuera_del_layout_se_omiten(job, world, capsys):
    extra = {"trusted/posts/_SUCCESS": b"", "trusted/posts/source=twitter/dt=2026-10-23/part-00000.parquet": b"PAR1",
             "trusted/posts/source=reddit/dt=2025-12-31/part-00000.parquet": b"PAR1", "trusted/posts/": b""}
    s3 = FakeS3({**world, **extra})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["input"]["skipped_objects"] == 3
    assert "trusted/posts/source=twitter/dt=2026-10-23/part-00000.parquet" not in s3.gets
    capsys.readouterr()


def test_trusted_vacio_deja_las_dimensiones_fijas(job, capsys):
    s3 = FakeS3({"curated/fact_posts/dt=2026-05-01/part-00000.parquet": b"PAR1PAR1"})
    report = job.main(argv(), s3=s3, glue=FakeGlue())
    assert report["counts"]["fact_rows"] == 0 and curated_keys(s3, "fact_posts") == []
    assert report["output"]["objects_deleted"] == 1 and read_table(s3, "dim_source").num_rows == 3
    assert read_table(s3, "dim_brand").num_rows == 3 and read_table(s3, "dim_date").num_rows == 0
    capsys.readouterr()


def test_salida_dentro_de_trusted_o_raw_se_rechaza(job, world, capsys):
    for prefix in ("trusted/curated/", "raw/curated/"):
        with pytest.raises(RuntimeError, match="Trusted"):
            job.main(argv("--ta_output_prefix", prefix), s3=FakeS3(world), glue=FakeGlue())
    capsys.readouterr()


def test_local_con_carpetas(job, trusted, tmp_path, capsys):
    src, dst = tmp_path / "src", tmp_path / "dst"
    for key, data in trusted.items():
        (src / key).parent.mkdir(parents=True, exist_ok=True)
        (src / key).write_bytes(data)
    report = job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(src), "--ta_local_dst", str(dst), "--ta_workers", "2"])
    assert (dst / "quality" / "_latest" / "trusted_to_curated.json").exists() and not (src / "curated").exists()
    assert (dst / "curated" / "dim_user" / "part-00000.parquet").exists() and report["output"]["objects_written"] > 0
    again = job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(src), "--ta_local_dst", str(dst)])
    assert again["output"]["objects_written"] == 0 and again["output"]["files_changed"] == 0
    with pytest.raises(RuntimeError, match="repositorio"):
        job.main(["--lake_bucket", BUCKET, "--ta_local_src", str(src), "--ta_local_dst", str(PROJECT / "x")])
    capsys.readouterr()


# --- Terraform y consultas guardadas (HU-030) -----------------------------------------------------


def test_terraform_deja_0_0625_dpu_al_job(job):
    tf = LAKEHOUSE_TF.read_text(encoding="utf-8")
    block = tf.split('key         = "trusted_to_curated"', 1)[1].split("},", 1)[0]
    assert "max_capacity" not in block.split("arguments", 1)[0]  # el valor por defecto del módulo: 0.0625
    assert '"--ta_input_prefix"  = "trusted/"' in block and '"--ta_output_prefix" = "curated/"' in block


CURATED_QUERIES = sorted(QUERIES.glob("curated_*.sql"))


def test_consultas_de_curated_en_terraform():
    tf = LAKEHOUSE_TF.read_text(encoding="utf-8")
    assert len(CURATED_QUERIES) == 5
    for path in CURATED_QUERIES:
        name = "ta-" + path.stem.replace("_", "-")
        assert f'"{name}" = {{' in tf and f"queries/{path.name}" in tf, name
        assert f'-- {name} (Lote 4.3' in path.read_text(encoding="utf-8").splitlines()[0]


@pytest.mark.parametrize("path", CURATED_QUERIES, ids=lambda p: p.stem)
def test_consultas_de_curated_son_un_solo_select(path):
    sql = "\n".join(ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.strip().startswith("--"))
    assert "ta_curated." in sql and ";" not in sql and sql.strip().upper().startswith(("SELECT", "WITH"))
    assert "user_hash" not in sql  # las consultas de ejemplo no muestran usuarios
