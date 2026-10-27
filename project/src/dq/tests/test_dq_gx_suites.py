"""Suites de Great Expectations del Lote 5.2 sobre un lake sintético (solo en el venv de GX, ``~/.venvs/ta-gx``).

Raw sale de ``quality.testing``; Trusted y Curated, de los jobs reales del 4.2 y del 4.3/4.4 en modo local, así que
las tres capas tienen el formato real. Lo esperado se comprueba con cuentas aparte; los defectos se inyectan en una
copia del lake."""

from __future__ import annotations

import importlib.util
import io
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import great_expectations.expectations as gxe
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.stream_tables import DQ_KPIS, DQ_LAYERS, DQ_RESULTS_COLUMNS
from dq import cli, frames as F, run as R
from dq.catalog import CURATED_RULES, METRIC_BOUNDS, all_rules, curated_specs, layer_specs, rule_kind
from dq.nlp import precision_report, render_md
from quality import testing as qt
from quality.rules import RULES

PROJECT = Path(__file__).resolve().parents[3]
JOBS = PROJECT / "src" / "lakehouse" / "glue_jobs"
ARCH = PROJECT.parent / "docs" / "ARCHITECTURE.md"
BUCKET = "ta-dev-648617469176-lake"
UTC = timezone.utc


def ts(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


def load_job(name):
    spec = importlib.util.spec_from_file_location(f"glue_job_{name}_dq", JOBS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def raw_rows():
    rows = []
    for i, src in enumerate(("reddit", "facebook", "instagram")):
        rows += qt.make_rows("historical", src, 6, ts("2026-05-01T20:00:00") + timedelta(minutes=i), step=timedelta(hours=2))
        rows += qt.make_rows("stream", src, 6, ts("2026-10-23T22:00:00") + timedelta(minutes=i), step=timedelta(minutes=7))
    rows[0]["hashtags"] = ["colombia", "gaseosa"]
    rows[7]["hashtags"] = ["bebidas", "colombia", "gaseosa"]
    return rows


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    base = tmp_path_factory.mktemp("dq")
    src, t, c, root = base / "raw_src", base / "t", base / "c", base / "s3" / BUCKET
    qt.write_raw_layer(src, raw_rows())
    load_job("raw_to_trusted").main(["--lake_bucket", BUCKET, "--ta_local_src", str(src), "--ta_local_dst", str(t), "--ta_workers", "2"])
    load_job("trusted_to_curated").main(["--lake_bucket", BUCKET, "--ta_local_src", str(t), "--ta_local_dst", str(c), "--ta_workers", "2"])
    shutil.copytree(src / "raw", root / "raw")
    shutil.copytree(t / "trusted", root / "trusted")
    shutil.copytree(c / "curated", root / "curated")
    return root


@pytest.fixture(scope="module")
def clean_run(lake, tmp_path_factory):
    out = tmp_path_factory.mktemp("out")
    return R.run(lake, out, log=lambda m: None), out


def copy_lake(lake, tmp_path):
    root = tmp_path / "s3" / BUCKET
    shutil.copytree(lake, root)
    return root


# --- catálogo ---------------------------------------------------------------------------------------


def test_cada_regla_tiene_su_expectativa_de_gx():
    specs = layer_specs("raw") + layer_specs("trusted") + curated_specs()
    for s in specs:
        assert hasattr(gxe, s.ge), s
    assert {r.id for r in RULES if "raw" in r.layers} == {s.rule for s in layer_specs("raw")}
    assert {r.id for r in RULES if "trusted" in r.layers} == {s.rule for s in layer_specs("trusted")}
    assert {r.id for r in CURATED_RULES} == {s.rule for s in curated_specs()}
    kinds = {r.id: rule_kind(r) for r in RULES}
    assert set(METRIC_BOUNDS) == {k for k, v in kinds.items() if v == "metric"}
    assert kinds["DQ-03"] == "native" and kinds["DQ-04"] == "flag" and kinds["DQ-05"] == "metric" and kinds["DQ-40"] == "native"
    assert [r.id for r in all_rules()] == [f"DQ-{i:02d}" for i in range(1, 78)]


def test_curated_de_architecture_igual_al_codigo():
    text = ARCH.read_text(encoding="utf-8")
    rows = {m.group(1): m.group(2) for m in re.finditer(r"^\| (DQ-(?:6[4-9]|7[0-7])) \| curated \|[^|]*\|[^|]*\| `([^`]+)` \|", text, re.M)}
    assert rows == {r.id: r.check for r in CURATED_RULES}


# --- corrida limpia -----------------------------------------------------------------------------------


def test_corrida_sintetica_sin_errores(clean_run):
    summary, out = clean_run
    assert summary["ok"] and summary["errors_failed"] == [] and summary["great_expectations"].startswith("1.")
    rows = json.loads((out / summary["run_id"] / "dq_results.json").read_text())
    assert len(rows) == summary["rows"] and {r["layer"] for r in rows} == {"raw", "trusted", "curated"}
    names = [c.name for c in DQ_RESULTS_COLUMNS]
    for r in rows:
        assert list(r) == names and r["layer"] in DQ_LAYERS and r["kpi"] in DQ_KPIS and isinstance(r["passed"], bool)
    checks = {r["check_name"] for r in rows}
    assert {"DQ-01[origin=stream]", "DQ-58[origin=historical]", "DQ-59[origin=stream]", "DQ-75[edges_user_brand.*]",
            "DQ-70[fact_posts.user_hash]", "NLP[accuracy]"} <= checks
    assert "DQ-59[origin=historical]" not in checks and "DQ-51[origin=stream]" not in checks  # alcance por origen
    by = {r["check_name"] + r["layer"]: r for r in rows}
    assert by["DQ-58[origin=historical]trusted"]["observed_value"] == 1.0
    for rule in ("DQ-72", "DQ-73", "DQ-74", "DQ-75", "DQ-76"):  # Curated = recalculado aparte
        r = next(x for x in rows if x["details"]["rule"] == rule)
        assert r["passed"] and r["observed_value"] == 0.0, rule
    # warnings registradas con passed = false sin hacer fallar la corrida (el NLP sintético acierta poco)
    assert any(w.startswith("DQ-62[") for w in summary["warnings_failed"])
    kpis = {(k["layer"], k["origin"], k["kpi"]) for k in summary["kpis"]}
    assert ("trusted", "stream", "duplicados") in kpis and ("curated", None, "anonimizacion") in kpis
    assert all(k["passed"] for k in summary["kpis"])


def test_salidas_sin_datos_de_filas(clean_run, lake):
    summary, out = clean_run
    folder = out / summary["run_id"]
    names = sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file())
    assert "summary.json" in names and "dq_results.json" in names and "nlp_precision.md" in names
    assert "ge/ge_raw_posts__stream.json" in names and "ge/ge_curated_metrics.json" in names
    text = "\n".join(p.read_text(encoding="utf-8") for p in folder.rglob("*") if p.is_file())
    fact = pq.read_table(next((lake / "curated" / "fact_posts").rglob("*.parquet"))).to_pylist()
    for row in fact:
        assert row["event_id"] not in text and row["user_hash"] not in text
    raw = pq.read_table(next((lake / "raw").rglob("*.parquet"))).to_pylist()
    assert all(r["text"] not in text for r in raw)
    ge = json.loads((folder / "ge" / "ge_curated_fact_posts.json").read_text())
    vs = [x["expectation_config"]["kwargs"]["value_set"] for x in ge["results"]
          if x["expectation_config"]["kwargs"].get("column") == "user_hash"]
    assert vs and all(isinstance(v, str) and "valores" in v for v in vs)


# --- defectos -----------------------------------------------------------------------------------------


def _rewrite(path, fn):
    t = pq.read_table(path)
    buf = io.BytesIO()
    pq.write_table(fn(t), buf)
    path.write_bytes(buf.getvalue())


def test_defectos_hacen_fallar_sus_reglas(lake, tmp_path):
    root = copy_lake(lake, tmp_path)
    raw_file = sorted((root / "raw" / "origin=stream").rglob("*.parquet"))[0]
    _rewrite(raw_file, lambda t: t.append_column("user_id", pa.array(["u"] * t.num_rows)))
    trusted = sorted((root / "trusted" / "posts").rglob("*.parquet"))
    first = pq.read_table(trusted[0]).column("event_id")[0].as_py()
    _rewrite(trusted[1], lambda t: t.set_column(0, "event_id", pa.array([first] + t.column("event_id").to_pylist()[1:])))
    trend = root / "curated" / "agg_trend_daily" / "part-00000.parquet"
    _rewrite(trend, lambda t: t.set_column(t.schema.get_field_index("n_posts"), t.schema.field("n_posts"),
                                           pa.array([v + 1 for v in t.column("n_posts").to_pylist()], pa.int64())))
    edges = root / "curated" / "edges_hashtag_cooccurrence" / "part-00000.parquet"
    _rewrite(edges, lambda t: pa.table({"hashtag_a": t.column("hashtag_b"), "hashtag_b": t.column("hashtag_a"),
                                        "weight": t.column("weight")}, schema=t.schema))
    summary = R.run(root, tmp_path / "out", log=lambda m: None)
    failed = {f.split(" ")[0] for f in summary["errors_failed"]}
    assert not summary["ok"]
    assert {"DQ-01[origin=stream]", "DQ-02[origin=stream]"} <= failed  # columna user_id en Raw
    assert "DQ-49[origin=historical]" in failed or "DQ-49[origin=stream]" in failed  # event_id repetido en Trusted
    assert "DQ-72[agg_trend_daily.*]" in failed and "DQ-76[edges_hashtag_cooccurrence.*]" in failed
    assert "DQ-77[edges_hashtag_cooccurrence.hashtag_a<hashtag_b]" in failed
    assert "DQ-01[origin=historical]" not in failed  # el otro origen no se contagia


# --- partes sueltas -------------------------------------------------------------------------------------


def test_precision_nlp_con_matriz_conocida():
    df = pd.DataFrame({"label_sentiment": ["positive"] * 4 + ["negative"] * 3 + ["neutral"] * 2 + [None, "positive"],
                       "sentiment_label": ["positive", "positive", "positive", "negative", "negative", "negative", "neutral",
                                           "neutral", "positive", "joy?", None],
                       "emotion_label": ["joy"] * 5 + [None] * 6, "origin": ["stream"] * 11})
    rep = precision_report(df)
    assert rep["evaluated"] == 9 and rep["without_label"] == 1 and rep["without_prediction"] == 1
    assert rep["confusion"]["positive"] == {"negative": 1, "neutral": 0, "positive": 3}
    assert rep["accuracy"] == pytest.approx(6 / 9) and rep["per_class"]["positive"]["precision"] == pytest.approx(3 / 4)
    assert rep["per_class"]["negative"]["recall"] == pytest.approx(2 / 3) and rep["emotion_distribution"]["counts"]["joy"] == 5
    md = render_md(rep, run_id="dq-x", as_of="2026-10-27")
    assert "Matriz de confusión" in md and "optimistas" in md and "66,7 %" in md and "ADR-13)" in md


def test_mismatches_y_recalculos_pequenos():
    a = pd.DataFrame({"k": ["x", "y"], "v": [1.0, 2.0]})
    assert F._mismatches(a, a.copy(), ["k"], ["v"]) == 0
    assert F._mismatches(a, pd.DataFrame({"k": ["x", "z"], "v": [1.0000001, 2.0]}), ["k"], ["v"], tol={"v": 1e-6}) == 2
    trusted = pd.DataFrame({"event_id": ["1", "2"], "hashtags": [["b", "a", "a"], ["a", "b", "c"]]})
    assert F.recompute_edges_hashtags(trusted).to_dict("records") == [
        {"hashtag_a": "a", "hashtag_b": "b", "weight": 2}, {"hashtag_a": "a", "hashtag_b": "c", "weight": 1},
        {"hashtag_a": "b", "hashtag_b": "c", "weight": 1}]
    fact = pd.DataFrame({"user_hash": ["u", "u", None], "brands": [["pepsi", "pepsi"], ["pepsi", "sprite"], ["sprite"]]})
    assert F.recompute_edges_user_brand(fact).to_dict("records") == [
        {"user_hash": "u", "brand": "pepsi", "weight": 2}, {"user_hash": "u", "brand": "sprite", "weight": 1}]


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803 - firma de boto3
        self.puts.append((Bucket, Key, ContentType, len(Body)))


class FakeBQ:
    def __init__(self):
        self.calls = []

    def insert_rows_json(self, table, rows):
        self.calls.append((table, rows))
        return []


def test_publicar_en_s3_y_bigquery_con_falsos(clean_run):
    summary, out = clean_run
    folder = out / summary["run_id"]
    items = R.s3_items(folder, summary["run_id"], summary["run_ts"])
    s3 = FakeS3()
    assert R.publish_s3(s3, BUCKET, items) == len(items)
    keys = [k for _, k, _, _ in s3.puts]
    day = summary["run_ts"][:10]
    assert all(k.startswith(f"quality/{day}/ge/{summary['run_id']}/") for k in keys[:-1]) and keys[-1] == R.LATEST_SUMMARY
    assert any(k.endswith("nlp_precision.md") for k in keys)
    rows = json.loads((folder / "dq_results.json").read_text())
    bq = FakeBQ()
    assert R.publish_bq(bq, "p.ta_stream.dq_results", rows, chunk=50) == []
    sent = [r for _, chunk in bq.calls for r in chunk]
    assert len(sent) == len(rows) and len(bq.calls) == -(-len(rows) // 50)
    assert all(isinstance(r["details"], str) and json.loads(r["details"])["rule"] for r in sent[:-2])
    assert R.default_gcp_project({"TA_GCP_PROJECT": "p-1"}) == "p-1"


def test_sync_ignora_las_carpetas(tmp_path):
    from quality.s3sync import sync_prefix

    class Port:
        def list_objects(self, bucket, prefix):
            return [{"key": "curated/", "size": 0, "etag": "x"}, {"key": "curated/dim_date/part-00000.parquet", "size": 3, "etag": "y"}]

        def download(self, bucket, key, dest):
            Path(dest).write_bytes(b"PAR")

    rep = sync_prefix(cli.NoFolders(Port()), BUCKET, "curated/", tmp_path)
    assert rep.ok and rep.objects == 1 and rep.downloaded == 1
    assert not sync_prefix(Port(), BUCKET, "curated/", tmp_path / "otra").ok  # sin el filtro, la «carpeta» falla


def test_cli_rules_y_run_sin_cache(tmp_path, capsys):
    assert cli.main(["rules", "--json"]) == 0
    rules = json.loads(capsys.readouterr().out)
    assert len(rules) == 77 and {r["kind"] for r in rules} == {"native", "flag", "metric"}
    assert cli.main(["run", "--cache", str(tmp_path / "vacia"), "--layers", "trusted"]) == 2
    assert "run_dq.py sync" in capsys.readouterr().out
