"""Marcadores de posición de los jobs de Glue (Lote 4.1, ADR-23) con clientes falsos de S3 y Glue."""

from __future__ import annotations

import ast
import importlib.util
import io
import json
import re
from pathlib import Path

import pytest

JOBS_DIR = Path(__file__).resolve().parents[1] / "glue_jobs"
LAKEHOUSE_TF = Path(__file__).resolve().parents[3] / "infra" / "aws" / "environments" / "dev" / "lakehouse.tf"
BUCKET = "ta-dev-648617469176-lake"
SCRIPTS = {"raw_to_trusted": "ta-job-raw-to-trusted", "trusted_to_curated": "ta-job-trusted-to-curated"}


def load(key):
    spec = importlib.util.spec_from_file_location(f"glue_job_{key}", JOBS_DIR / f"{key}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Body(io.BytesIO):
    pass


class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.puts = []
        self.gets = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):  # noqa: N803 - firma de boto3
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                for i in range(0, max(len(keys), 1), 2):
                    yield {"Contents": [{"Key": k, "Size": len(fake.objects[k])} for k in keys[i:i + 2]]}

        return Paginator()

    def get_object(self, Bucket, Key, Range=None):  # noqa: N803
        self.gets.append((Key, Range))
        if Key not in self.objects:
            raise NoSuchKey(Key)
        data = self.objects[Key]
        if Range:
            start, end = (int(x) for x in Range.split("=")[1].split("-"))
            data = data[start:end + 1]
        return {"Body": Body(data)}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.puts.append(Key)
        self.objects[Key] = Body


class FakeGlue:
    def __init__(self, props=None):
        self.props = props or {}
        self.calls = []

    def get_workflow_run_properties(self, Name, RunId):  # noqa: N803
        self.calls.append((Name, RunId))
        return {"RunProperties": dict(self.props)}


RAW = {
    "raw/origin=historical/source=reddit/dt=2026-05-01/hr=03/part-historical-s3101-20260501T03.parquet": b"PAR1xxxxPAR1",
    "raw/origin=stream/source=facebook/dt=2026-10-22/hr=10/part-20261022T100000Z-p000.parquet": b"PAR1yyyyPAR1",
    "raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-20261022T100500Z-p000.parquet": b"PAR1zzzzPAR1",
}
WF = ["--WORKFLOW_NAME", "ta-wf-raw-to-curated", "--WORKFLOW_RUN_ID", "wr_123"]


def argv(*extra):
    return ["--job-bookmark-option", "job-bookmark-disable", "--lake_bucket", BUCKET, "--ta_region", "us-east-1", *extra]


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_parse_args_como_los_pasa_glue(key):
    job = load(key)
    args = job.parse_args(["--a", "1", "--b=dos", "--flag", "--c", "x y"])
    assert args["a"] == "1" and args["b"] == "dos" and args["flag"] == "true" and args["c"] == "x y"
    assert args["ta_force_fail"] == "" and args["ta_input_prefix"].endswith("/")


def test_raw_to_trusted_cuenta_lee_y_deja_su_marca(capsys):
    job = load("raw_to_trusted")
    s3, glue = FakeS3(RAW), FakeGlue()
    summary = job.main(argv(*WF), s3=s3, glue=glue)
    assert summary["input_objects"] == 3 and summary["input_by_origin"] == {"historical": 1, "stream": 2}
    assert summary["read_probe"] == "PAR1" and summary["upstream_marker_found"] is None
    assert s3.puts == ["trusted/_ta_placeholder/ta-job-raw-to-trusted/wr_123.json"]
    marker = json.loads(s3.objects[s3.puts[0]])
    assert marker["workflow_run_id"] == "wr_123" and marker["job"] == "ta-job-raw-to-trusted"
    assert glue.calls == [("ta-wf-raw-to-curated", "wr_123")]
    events = [e["event"] for e in logs(capsys)]
    assert events == ["glue_job_start", "glue_placeholder_done"]
    assert any(r == "bytes=0-3" for _, r in s3.gets)


def test_trusted_to_curated_lee_la_marca_del_anterior(capsys):
    first = load("raw_to_trusted")
    s3 = FakeS3(RAW)
    first.main(argv(*WF), s3=s3, glue=FakeGlue())
    second = load("trusted_to_curated")
    summary = second.main(argv("--ta_input_prefix", "trusted/", "--ta_output_prefix", "curated/", *WF), s3=s3, glue=FakeGlue())
    assert summary["upstream_marker_found"] is True
    assert summary["input_objects"] == 0 and summary["read_probe"] == "sin objetos Parquet"
    assert s3.puts[-1] == "curated/_ta_placeholder/ta-job-trusted-to-curated/wr_123.json"
    capsys.readouterr()


def test_trusted_to_curated_sin_marca_lo_informa(capsys):
    job = load("trusted_to_curated")
    summary = job.main(argv(*WF), s3=FakeS3({}), glue=FakeGlue())
    assert summary["upstream_marker_found"] is False
    assert "glue_placeholder_upstream_missing" in [e["event"] for e in logs(capsys)]


@pytest.mark.parametrize("key,value,fails", [
    ("raw_to_trusted", "raw_to_trusted", True),
    ("raw_to_trusted", "ta-job-raw-to-trusted", True),
    ("raw_to_trusted", "all", True),
    ("raw_to_trusted", "trusted_to_curated", False),
    ("trusted_to_curated", "trusted_to_curated", True),
    ("trusted_to_curated", "raw_to_trusted", False),
])
def test_fallo_provocado_por_propiedad_de_corrida(key, value, fails, capsys):
    job = load(key)
    s3 = FakeS3(RAW)
    if fails:
        with pytest.raises(RuntimeError, match="ta_force_fail"):
            job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
        assert s3.puts == []
        assert "glue_job_forced_failure" in [e["event"] for e in logs(capsys)]
    else:
        job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
        assert len(s3.puts) == 1


def test_fallo_provocado_por_argumento_fuera_del_workflow(capsys):
    job = load("raw_to_trusted")
    glue = FakeGlue()
    with pytest.raises(RuntimeError):
        job.main(argv("--ta_force_fail", "true"), s3=FakeS3(RAW), glue=glue)
    assert glue.calls == []  # sin workflow no hay propiedades que leer
    capsys.readouterr()


def test_fuera_del_workflow_la_marca_es_manual(capsys):
    job = load("raw_to_trusted")
    s3 = FakeS3(RAW)
    job.main(argv(), s3=s3, glue=FakeGlue())
    assert re.match(r"trusted/_ta_placeholder/ta-job-raw-to-trusted/manual-[0-9a-f]{12}\.json$", s3.puts[0])
    capsys.readouterr()


def test_falla_si_un_parquet_no_lo_es(capsys):
    job = load("raw_to_trusted")
    bad = {"raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-x.parquet": b"JSON{}"}
    with pytest.raises(RuntimeError, match="PAR1"):
        job.main(argv(*WF), s3=FakeS3(bad), glue=FakeGlue())
    with pytest.raises(RuntimeError, match="lake_bucket"):
        job.main(["--ta_region", "us-east-1"], s3=FakeS3({}), glue=FakeGlue())
    capsys.readouterr()


def test_ignora_carpetas_y_marcas_al_contar(capsys):
    job = load("trusted_to_curated")
    objs = {"trusted/": b"", "trusted/_ta_placeholder/ta-job-raw-to-trusted/x.json": b"{}",
            "trusted/posts/source=reddit/dt=2026-10-22/part-0.parquet": b"PAR1....PAR1"}
    summary = job.main(argv(), s3=FakeS3(objs), glue=FakeGlue())
    assert summary["input_objects"] == 1 and summary["read_probe"] == "PAR1"
    capsys.readouterr()


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_python_39_y_solo_stdlib_y_boto3(key):
    tree = ast.parse((JOBS_DIR / f"{key}.py").read_text(encoding="utf-8"), feature_version=(3, 9))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= {"__future__", "json", "sys", "uuid", "datetime", "boto3"}, imported


@pytest.mark.parametrize("key,name", list(SCRIPTS.items()))
def test_nombres_coinciden_con_terraform(key, name):
    job = load(key)
    assert job.JOB_KEY == key and job.JOB_NAME == name
    tf = LAKEHOUSE_TF.read_text(encoding="utf-8")
    assert f'key         = "{key}"' in tf and f'name        = "{name}"' in tf
    assert f"glue_jobs/{key}.py" in tf


def test_importar_no_ejecuta_el_job(capsys):
    load("raw_to_trusted")
    assert capsys.readouterr().out == ""
