"""Plantilla Flex: argumentos del lanzador y comandos de build/run (Lote 2.4, HU-005). Sin red."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from streaming import template_tools as tt
from streaming.flex_main import prepare_argv

OUTPUTS = {
    "project_id": {"value": "proj-x"},
    "region": {"value": "us-central1"},
    "labels": {"value": {"project": "trend-analyzer", "env": "dev", "owner": "alejandro"}},
    "lake_bucket": {"value": "proj-x-ta-lake"},
    "artifact_registry_url": {"value": "us-central1-docker.pkg.dev/proj-x/ta-images"},
    "service_accounts": {"value": {"simulator": "sa-simulator@proj-x.iam.gserviceaccount.com",
                                   "dataflow": "sa-dataflow@proj-x.iam.gserviceaccount.com"}},
    "posts_enriched_table": {"value": "proj-x:ta_stream.posts_enriched"},
    "trend_windows_table": {"value": "proj-x:ta_stream.trend_windows"},
    "dlq_events_table": {"value": "proj-x:ta_stream.dlq_events"},
    "dlq_df_subscription": {"value": "projects/proj-x/subscriptions/ta-dlq-df"},
    "dataflow_template_spec": {"value": "gs://proj-x-ta-lake/_dataflow/templates/ta-stream.json"},
    "dataflow_launcher_image": {"value": "us-central1-docker.pkg.dev/proj-x/ta-images/dataflow-launcher"},
}


def _proc(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


class FakeRunner:
    def __init__(self, responses=None):
        self.calls: list[list[str]] = []
        self.responses = responses or {}

    def __call__(self, cmd):
        cmd = list(cmd)
        self.calls.append(cmd)
        for prefix, response in self.responses.items():
            if " ".join(cmd).startswith(prefix):
                return response
        return _proc()


# --------------------------------------------------------------------------- flex_main

def test_prepare_argv_completa_y_exporta_el_proyecto():
    env: dict[str, str] = {}
    argv = prepare_argv(["--project=proj-x", "--region", "us-central1", "--output_root=gs://b"], env)
    assert "--streaming" in argv and "--runner=DataflowRunner" in argv
    assert env == {"GCP_PROJECT": "proj-x", "GCP_REGION": "us-central1"}


def test_prepare_argv_pasa_la_imagen_de_nlp_a_ta_nlp_image():
    env: dict[str, str] = {}
    argv = prepare_argv(["--nlp_container_image=reg/dataflow-nlp:abc", "--output_root=gs://b",
                         "--nlp_container_image", "reg/dataflow-nlp:def"], env)
    assert env["TA_NLP_IMAGE"] == "reg/dataflow-nlp:abc"
    assert not any("nlp_container_image" in a for a in argv) and "reg/dataflow-nlp:def" not in argv
    assert "--output_root=gs://b" in argv


def test_etiquetas_del_lanzador_quedan_una_por_clave():
    """Argumentos reales del lanzador (validación de v2.26): las etiquetas llegan juntas y sueltas."""
    from streaming.flex_main import normalize_labels

    argv = ["--project=p", "--labels=component=streaming,env=dev,owner=alejandro,project=trend-analyzer",
            "--region=us-central1", "--labels=component=streaming", "--labels=env=dev",
            "--labels=owner=alejandro", "--labels=project=trend-analyzer", "--streaming"]
    out = normalize_labels(argv)
    labels = [a for a in out if a.startswith("--labels=")]
    assert labels == ["--labels=component=streaming", "--labels=env=dev", "--labels=owner=alejandro",
                      "--labels=project=trend-analyzer"]
    assert [a for a in out if not a.startswith("--labels")] == ["--project=p", "--region=us-central1", "--streaming"]
    # El orden ya no importa: el valor junto al final da el mismo resultado.
    reordered = normalize_labels(["--labels=env=dev", "--labels=component=streaming,env=dev"])
    assert reordered == ["--labels=env=dev", "--labels=component=streaming"]
    assert normalize_labels(["--labels", "a=b", "--x"]) == ["--labels=a=b", "--x"]
    assert normalize_labels(['--labels={"a": "b"}']) == ['--labels={"a": "b"}']


def test_beam_tomaria_mal_la_etiqueta_junta():
    """Lo que evita normalize_labels: Beam parte solo en el primer '='."""
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.runners.dataflow.internal import apiclient

    source = open(apiclient.__file__, encoding="utf-8").read()
    assert "label.split('=', 1)" in source
    opts = PipelineOptions(["--labels=component=streaming,env=dev"])
    assert opts.get_all_options()["labels"] == ["component=streaming,env=dev"]


def test_resultado_sin_job_no_falla():
    """Con --template_location (lo pasa el lanzador Flex), Beam devuelve un resultado sin job."""
    from types import SimpleNamespace

    from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
    from apache_beam.runners.runner import PipelineState

    from streaming.flex_main import patch_dataflow_result

    assert patch_dataflow_result() is True
    assert patch_dataflow_result() is True  # idempotente
    empty = DataflowPipelineResult(None, None)
    assert empty.job_id() is None
    assert empty.wait_until_finish() == PipelineState.DONE
    assert DataflowPipelineResult(SimpleNamespace(id="2026-10-23_x"), None).job_id() == "2026-10-23_x"


def test_main_trabaja_desde_src_como_dataflow_job_sh(monkeypatch, tmp_path):
    import os
    import sys as _sys

    from streaming import flex_main

    seen = {}

    def fake_run_module(name, run_name=None, alter_sys=False):
        seen.update(name=name, run_name=run_name, cwd=os.getcwd(), argv=list(_sys.argv),
                    image=os.environ.get("TA_NLP_IMAGE"))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(flex_main.runpy, "run_module", fake_run_module)
    monkeypatch.setattr(_sys, "argv", ["flex_main.py", "--project=p", "--region=r",
                                       "--template_location=gs://b/job_object", "--nlp_container_image=img:1"])
    monkeypatch.delenv("TA_NLP_IMAGE", raising=False)
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.delenv("GCP_REGION", raising=False)
    flex_main.main()
    assert seen["name"] == "streaming.pipeline" and seen["run_name"] == "__main__"
    assert seen["cwd"] == str(flex_main.SRC_DIR) and (flex_main.SRC_DIR / "streaming").is_dir()
    assert seen["image"] == "img:1" and "--template_location=gs://b/job_object" in seen["argv"]
    assert "--streaming" in seen["argv"] and "--runner=DataflowRunner" in seen["argv"]


def test_prepare_argv_respeta_lo_que_ya_viene():
    env = {"GCP_PROJECT": "otro"}
    argv = prepare_argv(["--project=proj-x", "--streaming", "--runner", "DataflowRunner"], env)
    assert argv.count("--streaming") == 1 and not any(a.startswith("--runner=") for a in argv)
    assert env["GCP_PROJECT"] == "otro"


# --------------------------------------------------------------------------- outputs

def test_env_desde_los_outputs():
    env = tt.env_from_outputs(OUTPUTS, environ={})
    assert env.project == "proj-x" and env.env == "dev" and env.job_name == "ta-stream-dev"
    assert env.service_account == "sa-dataflow@proj-x.iam.gserviceaccount.com"
    assert env.lake_root == "gs://proj-x-ta-lake"


def test_outputs_minimos_derivan_plantilla_e_imagen():
    minimal = {k: OUTPUTS[k] for k in ("project_id", "region", "lake_bucket", "artifact_registry_url",
                                       "service_accounts")}
    env = tt.env_from_outputs(minimal, environ={"ENV": "qa"})
    assert env.env == "qa"
    assert env.template_spec == "gs://proj-x-ta-lake/_dataflow/templates/ta-stream.json"
    assert env.launcher_image.endswith("/ta-images/dataflow-launcher")
    assert "trend_windows_table" not in tt.template_parameters(env)


def test_falta_un_output_obligatorio():
    with pytest.raises(tt.TemplateError, match="lake_bucket"):
        tt.env_from_outputs({k: v for k, v in OUTPUTS.items() if k != "lake_bucket"}, environ={})


def test_service_accounts_sin_dataflow():
    bad = dict(OUTPUTS, service_accounts={"value": ["sa-simulator@p.iam.gserviceaccount.com"]})
    with pytest.raises(tt.TemplateError, match="sa-dataflow"):
        tt.env_from_outputs(bad, environ={})


# --------------------------------------------------------------------------- parámetros y comandos

def test_parametros_salen_de_terraform():
    env = tt.env_from_outputs(OUTPUTS, environ={})
    params = tt.template_parameters(env, nlp_tag="abc123")
    assert params == {
        "output_root": "gs://proj-x-ta-lake",
        "nlp_container_image": "us-central1-docker.pkg.dev/proj-x/ta-images/dataflow-nlp:abc123",
        "posts_enriched_table": "proj-x:ta_stream.posts_enriched",
        "trend_windows_table": "proj-x:ta_stream.trend_windows",
        "dlq_events_table": "proj-x:ta_stream.dlq_events",
        "dlq_df_subscription": "projects/proj-x/subscriptions/ta-dlq-df",
    }
    with pytest.raises(tt.TemplateError, match="comas"):
        tt.template_parameters(env, extra={"x": "a,b"})


def test_parametros_coinciden_con_metadata():
    metadata = json.loads((tt.FLEX_DIR / "metadata.json").read_text(encoding="utf-8"))
    declared = {p["name"] for p in metadata["parameters"]}
    env = tt.env_from_outputs(OUTPUTS, environ={})
    assert set(tt.template_parameters(env)) <= declared
    assert all(p.get("isOptional") for p in metadata["parameters"])


def test_comando_run_con_las_caracteristicas_del_job_del_2_1():
    env = tt.env_from_outputs(OUTPUTS, environ={})
    cmd = tt.run_command(env, tt.template_parameters(env))
    text = " ".join(cmd)
    assert cmd[:5] == ["gcloud", "dataflow", "flex-template", "run", "ta-stream-dev"]
    for fragment in ("--worker-machine-type=e2-standard-2", "--num-workers=1", "--max-workers=1",
                     "--enable-streaming-engine", "--service-account-email=sa-dataflow@",
                     "--temp-location=gs://proj-x-ta-lake/_dataflow/temp",
                     "component=streaming", "trend_windows_table=proj-x:ta_stream.trend_windows"):
        assert fragment in text, fragment
    assert "proj-x" not in json.dumps(tt.FLEX_DIR.joinpath("metadata.json").read_text())


def test_comandos_de_build():
    env = tt.env_from_outputs(OUTPUTS, environ={})
    cmds = tt.build_commands(env, "abc123", Path("/ctx"), Path("/m.json"))
    assert cmds[0][:4] == ["gcloud", "builds", "submit", "/ctx"]
    assert "--tag=us-central1-docker.pkg.dev/proj-x/ta-images/dataflow-launcher:abc123" in cmds[0]
    assert cmds[2][4] == "gs://proj-x-ta-lake/_dataflow/templates/ta-stream-abc123.json"
    assert cmds[3][-2] == "gs://proj-x-ta-lake/_dataflow/templates/ta-stream.json"


def test_etiqueta_commit_o_fecha():
    assert tt.resolve_tag("x1") == "x1"
    runner = FakeRunner({"git": _proc("9f8e7d6\n")})
    assert tt.resolve_tag(None, runner) == "9f8e7d6"
    failing = FakeRunner({"git": _proc("", 128)})
    now = datetime(2026, 10, 22, 19, 5, tzinfo=timezone.utc)
    assert tt.resolve_tag(None, failing, now) == "20261022-1905"


def test_stage_context(tmp_path):
    project = tmp_path / "project"
    (project / "src" / "streaming" / "tests").mkdir(parents=True)
    (project / "src" / "streaming" / "requirements.txt").write_text("apache-beam[gcp]==2.77.0\n")
    (project / "src" / "streaming" / "x.py").write_text("x = 1\n")
    (project / "src" / "streaming" / "tests" / "t.py").write_text("")
    (project / "src" / "ta_streaming.egg-info").mkdir()
    (project / "contracts").mkdir()
    (project / "contracts" / "envelope.v1.schema.json").write_text("{}")
    ctx = tt.stage_context(project, tmp_path / "ctx")
    assert (ctx / "Dockerfile").exists() and (ctx / "requirements.txt").exists()
    assert (ctx / "project" / "src" / "streaming" / "x.py").exists()
    assert not (ctx / "project" / "src" / "streaming" / "tests").exists()
    assert not (ctx / "project" / "src" / "ta_streaming.egg-info").exists()
    assert (ctx / "project" / "contracts" / "envelope.v1.schema.json").exists()


def test_dockerfile_usa_el_punto_de_entrada_y_beam_2_77():
    text = (tt.FLEX_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert "python311-template-launcher-base" in text
    assert "FLEX_TEMPLATE_PYTHON_PY_FILE=/template/project/src/streaming/flex_main.py" in text
    assert "FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/project/src/setup.py" in text
    assert "2.77.0" in text
    assert "WORKDIR /template/project/src" in text and "test -d ../contracts" in text
    assert "patch_dataflow_result" in text


# --------------------------------------------------------------------------- CLI

def _cli(argv, runner):
    return tt.main(["--tf-dir", "/tf", *argv], runner=runner)


def test_cli_run_se_niega_si_hay_job_activo(capsys):
    runner = FakeRunner({
        "terraform": _proc(json.dumps(OUTPUTS)),
        "gcloud dataflow jobs list": _proc(json.dumps([{"name": "ta-stream-dev", "id": "j1", "state": "Running"}])),
    })
    assert _cli(["run"], runner) == 2
    assert "ya hay un job activo" in capsys.readouterr().err
    assert not any(c[:4] == ["gcloud", "dataflow", "flex-template", "run"] for c in runner.calls)


def test_cli_run_lanza(tmp_path):
    runner = FakeRunner({"terraform": _proc(json.dumps(OUTPUTS)), "gcloud dataflow jobs list": _proc("[]")})
    out = tmp_path / "run.json"
    assert tt.main(["--tf-dir", "/tf", "--json-out", str(out), "run", "--nlp-tag", "n1"], runner=runner) == 0
    launched = [c for c in runner.calls if c[:4] == ["gcloud", "dataflow", "flex-template", "run"]]
    assert len(launched) == 1
    assert json.loads(out.read_text())["parameters"]["nlp_container_image"].endswith(":n1")


def test_cli_build_dry_run_no_ejecuta(capsys, tmp_path, monkeypatch):
    project = tmp_path / "project"
    (project / "src" / "streaming").mkdir(parents=True)
    (project / "src" / "streaming" / "requirements.txt").write_text("apache-beam[gcp]==2.77.0\n")
    monkeypatch.setattr(tt, "PROJECT_DIR", project)
    runner = FakeRunner({"terraform": _proc(json.dumps(OUTPUTS)), "git": _proc("abc\n")})
    assert _cli(["build", "--dry-run"], runner) == 0
    assert all(c[0] in ("terraform", "git") for c in runner.calls)
    assert "gcloud builds submit" in capsys.readouterr().out


def test_cli_terraform_falla():
    runner = FakeRunner({"terraform": _proc("", 1, "no state")})
    assert _cli(["params"], runner) == 2
