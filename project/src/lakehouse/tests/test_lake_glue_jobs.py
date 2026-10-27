"""Lo común a los dos jobs de Glue (argumentos, fallo provocado, Python 3.9, bibliotecas, nombres de
Terraform) con clientes falsos de S3 y Glue. Cada job real se prueba aparte: ``raw_to_trusted`` (Lote 4.2) en
``test_lake_raw_to_trusted.py`` y ``trusted_to_curated`` (Lote 4.3, reemplazó al marcador de posición del 4.1)
en ``test_lake_trusted_to_curated.py``."""

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

    def put_workflow_run_properties(self, Name, RunId, RunProperties):  # noqa: N803
        self.calls.append((Name, RunId, "put"))


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


@pytest.mark.parametrize("key,value,fails", [
    ("raw_to_trusted", "raw_to_trusted", True),
    ("raw_to_trusted", "ta-job-raw-to-trusted", True),
    ("raw_to_trusted", "all", True),
    ("trusted_to_curated", "trusted_to_curated", True),
    ("trusted_to_curated", "ta-job-trusted-to-curated", True),
    ("trusted_to_curated", "raw_to_trusted", False),
])
def test_fallo_provocado_por_propiedad_de_corrida(key, value, fails, capsys):
    # Los casos «no falla» con datos están en test_lake_raw_to_trusted.py y test_lake_trusted_to_curated.py.
    job = load(key)
    s3 = FakeS3(RAW)
    if fails:
        with pytest.raises(RuntimeError, match="ta_force_fail"):
            job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
        assert s3.puts == []
        assert "glue_job_forced_failure" in [e["event"] for e in logs(capsys)]
    else:
        # Sin Trusted: escribe las dimensiones fijas (dim_source, dim_brand), las tablas vacías y su reporte.
        job.main(argv(*WF), s3=s3, glue=FakeGlue({"ta_force_fail": value}))
        assert "curated/dim_source/part-00000.parquet" in s3.puts
        assert not any(k.startswith("curated/_ta_placeholder/") for k in s3.puts)
        capsys.readouterr()


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_fallo_provocado_por_argumento_fuera_del_workflow(key, capsys):
    job = load(key)
    glue = FakeGlue()
    with pytest.raises(RuntimeError):
        job.main(argv("--ta_force_fail", "true"), s3=FakeS3(RAW), glue=glue)
    assert glue.calls == []  # sin workflow no hay propiedades que leer
    capsys.readouterr()


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_fuera_del_workflow_el_reporte_es_manual(key, capsys):
    # Glue no pasa --JOB_RUN_ID a un job Python shell (v2.36): fuera del workflow la corrida es manual-….
    job = load(key)
    s3 = FakeS3({})
    report = job.main(argv("--ta_input_prefix", "raw/" if key == "raw_to_trusted" else "trusted/"), s3=s3, glue=FakeGlue())
    assert re.match(r"manual-\d{8}T\d{6}Z-[0-9a-f]{8}$", report["run_id"])
    assert any(re.match(r"quality/\d{4}-\d{2}-\d{2}/%s/manual-" % key, k) for k in s3.puts)
    capsys.readouterr()


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_falla_si_un_parquet_no_lo_es(key, capsys):
    job = load(key)
    bad = {"raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-x.parquet": b"JSON{}",
           "trusted/posts/source=reddit/dt=2026-10-22/part-x.parquet": b"JSON{}"}
    prefix = "raw/" if key == "raw_to_trusted" else "trusted/"
    with pytest.raises(RuntimeError, match="PAR1|Parquet"):
        job.main(argv("--ta_input_prefix", prefix, *WF), s3=FakeS3(bad), glue=FakeGlue())
    with pytest.raises(RuntimeError, match="lake_bucket"):
        job.main(["--ta_region", "us-east-1"], s3=FakeS3({}), glue=FakeGlue())
    capsys.readouterr()


def test_trusted_to_curated_ignora_carpetas_y_marcas(capsys):
    job = load("trusted_to_curated")
    objs = {"trusted/": b"", "trusted/_ta_placeholder/ta-job-raw-to-trusted/x.json": b"{}", "trusted/posts/": b""}
    report = job.main(argv(), s3=FakeS3(objs), glue=FakeGlue())
    assert report["input"]["objects"] == 0 and report["input"]["skipped_objects"] == 0
    capsys.readouterr()


ALLOWED_IMPORTS = {
    # Jobs reales (4.2 y 4.3): biblioteca estándar, boto3/botocore y pyarrow (en Glue lo trae el analytics library set).
    "trusted_to_curated": {"__future__", "hashlib", "io", "json", "math", "os", "re", "resource", "sys", "time", "uuid",
                           "concurrent", "datetime", "boto3", "botocore", "pyarrow"},
    "raw_to_trusted": {"__future__", "hashlib", "io", "json", "os", "re", "resource", "sys", "time", "unicodedata",
                       "uuid", "concurrent", "datetime", "boto3", "botocore", "pyarrow"},
}


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_python_39_y_solo_las_bibliotecas_de_glue(key):
    tree = ast.parse((JOBS_DIR / f"{key}.py").read_text(encoding="utf-8"), feature_version=(3, 9))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= ALLOWED_IMPORTS[key], imported - ALLOWED_IMPORTS[key]


@pytest.mark.parametrize("key,name", list(SCRIPTS.items()))
def test_nombres_coinciden_con_terraform(key, name):
    job = load(key)
    assert job.JOB_KEY == key and job.JOB_NAME == name
    tf = LAKEHOUSE_TF.read_text(encoding="utf-8")
    assert f'key         = "{key}"' in tf and f'name        = "{name}"' in tf
    assert f"glue_jobs/{key}.py" in tf


@pytest.mark.parametrize("key", list(SCRIPTS))
def test_importar_no_ejecuta_el_job(key, capsys):
    load(key)
    assert capsys.readouterr().out == ""
