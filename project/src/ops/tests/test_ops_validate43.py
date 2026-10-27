"""Pruebas del validador del Lote 4.3 con un AWS, un Terraform y un job falsos (sin red, Docker ni nube)."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ops import validate41 as v41
from ops import validate42 as v42
from ops import validate43 as v

ACCOUNT = "648617469176"
BUCKET = f"ta-dev-{ACCOUNT}-lake"
NOW = datetime(2026, 10, 27, 18, 0, 0, tzinfo=timezone.utc)
ROLE = f"arn:aws:iam::{ACCOUNT}:role/ta-glue-role"
DTS = ("2026-04-27", "2026-10-23", "2026-10-24")
TABLE_ROWS = {"fact_posts": 221629, "dim_date": 181, "dim_source": 3, "dim_brand": 3, "dim_user": 5936,
              "agg_emotion_by_brand_hour": 39954, "agg_trend_daily": 543}
CREATES = sorted(v.EXPECTED_CREATES)
UPDATES = sorted(v.EXPECTED_UPDATES)


def load_job():
    spec = importlib.util.spec_from_file_location("glue_job_t2c_v43", v.JOB_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


JOB = load_job()


def arg(cmd, name):
    for i, c in enumerate(cmd):
        if c == name and i + 1 < len(cmd):
            return cmd[i + 1]
    return None


def ok(data):
    return v.Result(0, json.dumps(data))


def err(code):
    return v.Result(254, "", f"\nAn error occurred ({code}) when calling the Operation: x")


SAMPLE = {"string": "pepsi", "int32": 20261024, "int64": 1, "double": 0.5, "date": date(2026, 10, 24), "bool": True,
          "timestamp": datetime(2026, 10, 24, 10, tzinfo=timezone.utc), "list": ["pepsi"]}


def table_parquet(name, *, wrong=False):
    cols = {n: [SAMPLE[k]] for n, k, _ in JOB.TABLE_COLUMNS[name]}
    table = pa.Table.from_pydict(cols, schema=JOB.table_schema(name))
    if wrong and "date_key" in table.column_names:  # int64 en vez de int: Athena no lo leería como int
        i = table.schema.get_field_index("date_key")
        table = table.set_column(i, pa.field("date_key", pa.int64()), table.column("date_key").cast(pa.int64()))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def curated_keys():
    return [f"curated/fact_posts/dt={d}/part-00000.parquet" for d in DTS] + \
        [f"curated/{t}/part-00000.parquet" for t in v.TABLES if t != "fact_posts"]


def make_report(run_id, wf_run_id=None, *, changed=None, written=None, sha="a", bad_check=False, rows=221629,
                trusted_expected=None):
    keys = curated_keys()
    files = [{"key": k, "table": v.table_of(k), "rows": 1, "bytes": 100, "sha256": hashlib.sha256(f"{sha}{k}".encode()).hexdigest()}
             for k in keys]
    checks = {"fact_rows_eq_trusted_rows": {"expected": rows, "observed": rows, "ok": True},
              "trend_n_posts_eq_rows_with_brand": {"expected": rows, "observed": rows, "ok": True},
              "trend_sentiment_eq_rows_with_sentiment": {"expected": 156366, "observed": 156366, "ok": True},
              "trend_rows_without_sentiment": {"expected": 65263, "observed": 65263, "ok": True},
              "emotion_n_posts_eq_rows_with_emotion": {"expected": 156366, "observed": 156366, "ok": True},
              "user_n_posts_eq_rows_with_user": {"expected": rows, "observed": rows, "ok": True}}
    if bad_check:
        checks["emotion_n_posts_eq_rows_with_emotion"] = {"expected": 156366, "observed": 156300, "ok": False}
    changed = len(keys) if changed is None else changed
    tables = {t: {"rows": n if t != "fact_posts" else rows, "files": 3 if t == "fact_posts" else 1} for t, n in TABLE_ROWS.items()}
    return {
        "schema": "ta-trusted-to-curated-report@1", "run_id": run_id, "workflow_run_id": wf_run_id,
        "report_key": f"quality/2026-10-27/trusted_to_curated/{run_id}.json",
        "input": {"objects": 543, "rows": rows, "dt_min": "2026-04-27", "dt_max": "2026-10-24",
                  "trusted_rows_expected": trusted_expected, "trusted_rows_match": None if trusted_expected is None else True},
        "counts": {"fact_rows": rows, "nlp_missing": 65263, "with_sentiment": 156366, "with_emotion": 156366},
        "tables": tables, "checks": checks, "conciliation": {"ok": not bad_check},
        "output": {"objects": len(keys), "objects_written": changed if written is None else written,
                   "objects_unchanged": len(keys) - changed, "objects_deleted": 0, "files_changed": changed,
                   "bytes": 18_000_000, "files": files},
    }


# --- AWS falso ---------------------------------------------------------------------------------


class World:
    def __init__(self):
        self.caller = f"arn:aws:iam::{ACCOUNT}:user/franklin"
        self.schedule = {"Name": v.SCHEDULE, "GroupName": "default", "State": "DISABLED", "ScheduleExpression": "rate(1 hour)",
                         "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
                         "Target": {"Arn": v41.SCHEDULER_TARGET, "RoleArn": "r", "Input": json.dumps({"Name": v.WORKFLOW}),
                                    "RetryPolicy": {"MaximumRetryAttempts": 0}}}
        self.updates = []
        self.capacity = 0.0625
        self.script = v.JOB_FILE.read_text(encoding="utf-8")
        self.objects = {"trusted/": "", "curated/": "", "curated/edges_user_brand/part-00000.parquet": "PAR1"}
        self.runs, self.details, self.props = [], {}, {}
        self.job_runs, self.running_job, self.logs, self.queries = {}, [], [], {}
        self.job_state = "SUCCEEDED"
        self.report_overrides = {}
        self.rerun_changed = 0
        self.busy_once = False
        self.n = 0
        self.removed = []
        self.peak = 430.0
        self.touch_edges = False
        self.add_placeholder_on_run = False
        self.extra_key = None
        self.named = {n: (v.QUERIES / f).read_text(encoding="utf-8") for n, f in v.CURATED_QUERIES.items()}
        self.named["ta-raw-conteo-por-origen"] = "SELECT 1"
        self.query_ms = 1500
        self.sql = {"links": {"trusted": 221629, "fact": 221629, "date_key_mal": 0, "sin_fecha": 0, "sin_fuente": 0, "sin_marca": 0,
                              "sin_usuario": 0, "dias": 181, "dias_esperados": 181},
                    "trend": {"claves_distintas": 0, "conteos_distintos": 0, "promedios_distintos": 0, "filas_con_posts_sin_nlp": 6,
                              "n_posts": 221629, "con_sentimiento": 156366},
                    "emotion": {"claves_distintas": 0, "conteos_distintos": 0, "n_posts": 156366, "sin_emocion": 65263},
                    "user": {"claves_distintas": 0, "filas_distintas": 0, "n_posts": 221629, "usuarios": 5936}}
        self.origins = {"historical": 50000, "stream": 171629}

    def run_curated(self, job_run_id, wf_run_id=None):
        first = v.LATEST_REPORT not in self.objects
        changed = None if first else self.rerun_changed
        report = make_report(wf_run_id or f"manual-{job_run_id}", wf_run_id, changed=changed,
                             trusted_expected=221629 if wf_run_id else None, **self.report_overrides)
        for k in curated_keys():
            self.objects[k] = "PAR1"
        if self.extra_key:
            self.objects[self.extra_key] = "PAR1"
        if self.touch_edges:
            self.objects["curated/edges_user_brand/part-00000.parquet"] = "PAR1nuevo"
        if self.add_placeholder_on_run:
            self.objects[f"curated/_ta_placeholder/{v.JOB}/x.json"] = "{}"
        text = json.dumps(report)
        self.objects[report["report_key"]] = text
        self.objects[v.LATEST_REPORT] = text
        if wf_run_id:
            self.props.setdefault(wf_run_id, {}).update({
                "ta_curated_report": report["report_key"], "ta_curated_rows": str(report["counts"]["fact_rows"]),
                "ta_curated_files_changed": str(report["output"]["files_changed"])})
        done = {"event": "glue_job_done", "report": report["report_key"], "rows_read": 221629, "fact_rows": 221629,
                "fact_partitions": 181, "tables": TABLE_ROWS, "read_seconds": 3.0, "duration_s": 25.0, "cpu_seconds": 20.0,
                "peak_rss_mb": self.peak, "environment": {"python": "3.9.10", "pyarrow": "7.0.0", "boto3": "1.21.21"}}
        self.logs.append((v.GLUE_OUTPUT_LOG, job_run_id, json.dumps(done)))

    def run_trusted(self, wf_run_id):
        key = f"quality/2026-10-27/raw_to_trusted/{wf_run_id}.json"
        rep = {"run_id": wf_run_id, "input": {"objects": 13257}, "output": {"objects_written": 543}}
        self.objects[key] = json.dumps(rep)
        self.props.setdefault(wf_run_id, {}).update({"ta_validacion": "4.3", "ta_trusted_report": key, "ta_trusted_rows": "221629",
                                                     "ta_trusted_partitions_changed": "0"})

    def handle(self, cmd):  # noqa: C901 - despachador del AWS falso
        svc, op = cmd[1], cmd[2]
        if (svc, op) == ("sts", "get-caller-identity"):
            return ok({"Account": ACCOUNT, "Arn": self.caller})
        if svc == "scheduler":
            if op == "get-schedule":
                return ok(self.schedule) if arg(cmd, "--name") == v.SCHEDULE else err("ResourceNotFoundException")
            if op == "update-schedule":
                body = json.loads(Path(arg(cmd, "--cli-input-json")[len("file://"):]).read_text())
                self.updates.append(body)
                self.schedule.update(body)
                return ok({})
            if op == "list-schedules":
                return ok({"Schedules": []})
        if svc == "glue":
            if op == "get-job":
                assert arg(cmd, "--job-name") == v.JOB
                return ok({"Job": {"Name": v.JOB, "Role": ROLE, "MaxRetries": 0, "MaxCapacity": self.capacity,
                                   "Command": {"Name": "pythonshell", "PythonVersion": "3.9",
                                               "ScriptLocation": f"s3://{BUCKET}/_glue/scripts/{v.JOB}.py"},
                                   "DefaultArguments": {"--lake_bucket": BUCKET, "--ta_input_prefix": "trusted/",
                                                        "--ta_output_prefix": "curated/", "library-set": "analytics"}}})
            if op == "start-job-run":
                assert arg(cmd, "--job-name") == v.JOB
                if self.busy_once:
                    self.busy_once = False
                    return err("ConcurrentRunsExceededException")
                assert json.loads(arg(cmd, "--arguments")) == {"--ta_validacion": "4.3"}
                self.n += 1
                jid = f"jr_d{self.n}"
                self.job_runs[jid] = {"Id": jid, "JobRunState": self.job_state, "ExecutionTime": 40, "MaxCapacity": self.capacity,
                                      "ErrorMessage": "MemoryError" if self.job_state != "SUCCEEDED" else None}
                if self.job_state == "SUCCEEDED":
                    self.run_curated(jid)
                return ok({"JobRunId": jid})
            if op == "get-job-run":
                assert arg(cmd, "--job-name") == v.JOB
                return ok({"JobRun": self.job_runs[arg(cmd, "--run-id")]})
            if op == "get-job-runs":
                name = arg(cmd, "--job-name")
                return ok({"JobRuns": [{"Id": i, "JobRunState": "RUNNING"} for i in self.running_job] if name == v.JOB else []})
            if op == "get-workflow-runs":
                return ok({"Runs": list(self.runs)})
            if op == "start-workflow-run":
                self.n += 1
                rid = f"wr_{self.n:04d}"
                j1, j2 = f"jr_{rid}_1", f"jr_{rid}_2"
                self.run_trusted(rid)
                self.run_curated(j2, rid)
                nodes = [{"Type": "JOB", "Name": v.PREV_JOB, "JobDetails": {"JobRuns": [{"Id": j1, "JobRunState": "SUCCEEDED", "ExecutionTime": 90}]}},
                         {"Type": "JOB", "Name": v.JOB, "JobDetails": {"JobRuns": [{"Id": j2, "JobRunState": "SUCCEEDED", "ExecutionTime": 45}]}}]
                self.details[rid] = {"WorkflowRunId": rid, "Status": "COMPLETED", "Graph": {"Nodes": nodes}, "Statistics": {}}
                self.runs.insert(0, {"WorkflowRunId": rid, "Status": "COMPLETED", "StartedOn": NOW.isoformat()})
                return ok({"RunId": rid})
            if op == "get-workflow-run":
                return ok({"Run": self.details[arg(cmd, "--run-id")]})
            if op == "get-workflow-run-properties":
                return ok({"RunProperties": self.props.get(arg(cmd, "--run-id"), {})})
            if op in ("stop-workflow-run", "batch-stop-job-run"):
                self.running_job = []
                return ok({})
        if svc == "logs" and op == "filter-log-events":
            prefix = arg(cmd, "--log-stream-name-prefix") or ""
            needle = arg(cmd, "--filter-pattern").strip('"')
            return ok({"events": [{"message": m} for g, s, m in self.logs
                                  if g == arg(cmd, "--log-group-name") and s.startswith(prefix) and needle in m]})
        if svc == "s3" and op == "cp":
            key = cmd[3].split(f"s3://{BUCKET}/", 1)[1]
            if key.startswith("_glue/scripts/"):
                return v.Result(0, self.script)
            return v.Result(0, self.objects[key]) if key in self.objects else err("404")
        if svc == "s3" and op == "rm":
            prefix = cmd[3].split(f"s3://{BUCKET}/", 1)[1]
            gone = [k for k in self.objects if k.startswith(prefix)]
            for k in gone:
                del self.objects[k]
            self.removed += gone
            return v.Result(0, "")
        if svc == "s3api" and op == "list-objects-v2":
            keys = sorted(k for k in self.objects if k.startswith(arg(cmd, "--prefix")))
            return ok({"Contents": [{"Key": k, "Size": len(self.objects[k]) or 1} for k in keys]} if keys else {})
        if svc == "athena":
            if op == "start-query-execution":
                qid = f"q{len(self.queries) + 1}"
                self.queries[qid] = arg(cmd, "--query-string")
                return ok({"QueryExecutionId": qid})
            if op == "get-query-execution":
                return ok({"QueryExecution": {"Status": {"State": "SUCCEEDED"},
                                              "Statistics": {"DataScannedInBytes": 4_000_000, "EngineExecutionTimeInMillis": 800,
                                                             "TotalExecutionTimeInMillis": self.query_ms},
                                              "ResultConfiguration": {"OutputLocation": f"s3://{BUCKET}/athena-results/x.csv"}}})
            if op == "get-query-results":
                cols, rows = self.query_result(self.queries[arg(cmd, "--query-execution-id")])
                return ok({"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": c} for c in cols]},
                                         "Rows": [{"Data": [{"VarCharValue": c} for c in cols]}]
                                         + [{"Data": [{"VarCharValue": str(x)} for x in r]} for r in rows]}})
            if op == "list-named-queries":
                return ok({"NamedQueryIds": [f"id-{n}" for n in self.named]})
            if op == "batch-get-named-query":
                return ok({"NamedQueries": [{"Name": n, "QueryString": q, "Database": "ta_curated" if "curated" in n else "ta_raw",
                                             "NamedQueryId": f"id-{n}"} for n, q in self.named.items()]})
        return v.Result(0, "{}")

    def query_result(self, sql):
        for name, needle in (("links", "dias_esperados"), ("trend", "filas_con_posts_sin_nlp"), ("emotion", "sin_emocion"),
                             ("user", "filas_distintas")):
            if needle in sql:
                return list(self.sql[name]), [list(self.sql[name].values())]
        if "cardinality(brands)" in sql:
            return (["origin", "filas", "event_ids", "desde", "hasta", "comparativas"],
                    [[o, n, n, "2026-04-27", "2026-10-24", 100] for o, n in self.origins.items()])
        if sql.strip() == v.COUNT_SQL:
            return ["filas", "event_ids"], [[221629, 221629]]
        return ["x"], [[1], [2]]


class FakeRunner:
    def __init__(self, world, *, plans=None, pytest_out=None, local_out=None):
        self.world = world
        self.plans = list(plans or [])
        self.pytest_out = pytest_out or {}
        self.local_out = list(local_out or [])
        self.calls = []
        self.apply_text = None

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        if cmd[0] == "aws":
            return self.world.handle(cmd)
        if cmd[0] == "terraform":
            sub = cmd[2] if cmd[1].startswith("-chdir") else cmd[1]
            if sub == "show":
                return v.Result(0, json.dumps(self.plans.pop(0) if self.plans else {"resource_changes": []}))
            if sub == "apply":
                return v.Result(0, self.apply_text or "Apply complete! Resources: 5 added, 2 changed, 0 destroyed.")
            if sub == "output":
                return ok({"lake_bucket": {"value": BUCKET}})
            return v.Result(0, "ok")
        if "pytest" in cmd:
            target = next(c for c in cmd if c.startswith(("src/", "contracts/")))
            default = {"src/lakehouse": v.B1_EXPECTED, "src/ops/tests/test_ops_validate43.py": v.B2_EXPECTED,
                       "contracts/tests": 18, "src/ops/tests/test_ops_validate42.py": v.B4_EXPECTED,
                       "src/ops/tests/test_ops_validate41.py": v.B5_EXPECTED}
            return v.Result(0, self.pytest_out.get(target, f"{default.get(target, 1)} passed in 1s"))
        if len(cmd) > 1 and cmd[1].endswith("trusted_to_curated.py"):
            return v.Result(0, self.local_out.pop(0) if self.local_out else "")
        if cmd[:2] == ["gcloud", "dataflow"]:
            return v.Result(0, "[]")
        return v.Result(0, "")

    def count(self, text):
        return sum(1 for c in self.calls if text in " ".join(c))


def plan(updates=(), creates=(), job_diff=("description",)):
    rcs = []
    for a in updates:
        if "aws_glue_job" in a:
            before, after = {k: 0 for k in job_diff}, {k: 1 for k in job_diff}
        else:
            before, after = {"source_hash": "x"}, {"source_hash": "y"}
        rcs.append({"address": a, "mode": "managed", "change": {"actions": ["update"], "before": before, "after": after}})
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["create"]}} for a in creates]
    return {"resource_changes": rcs}


def s3_rows_fake(trusted=221629, tables=None):
    tables = dict(TABLE_ROWS, **(tables or {}))

    def count(bucket, prefix):
        if prefix.startswith("trusted/"):
            return {"objects": 543, "rows": trusted, "by_source": {}, "errors": [], "error_count": 0, "seconds": 1.0}
        table = prefix.split("/")[1]
        return {"objects": 3 if table == "fact_posts" else 1, "rows": tables.get(table, 0), "by_source": {}, "errors": [],
                "error_count": 0, "seconds": 0.5}
    return count


def s3_get_fake(wrong=False):
    def get(bucket, key):
        return table_parquet(v.table_of(key), wrong=wrong)
    return get


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    aws, ev = tmp_path / "aws", tmp_path / "evidencia"
    aws.mkdir()
    ev.mkdir()
    (aws / "backend.hcl").write_text('bucket = "x"\n')
    (aws / "terraform.tfvars").write_text(f'aws_account_id = "{ACCOUNT}"\n')
    monkeypatch.setattr(v, "AWS_ENV", aws)
    monkeypatch.setattr(v41, "AWS_ENV", aws)
    monkeypatch.setattr(v, "EVIDENCE_DIR", ev)
    monkeypatch.setattr(v, "on_windows", lambda: False)
    return tmp_path


def run_part(runner, *argv, sandbox_dir, confirm=None, s3_rows=None, s3_get=None):
    lines = []
    state = {"t": NOW}

    def now():
        return state["t"]

    def sleep(s):
        state["t"] += timedelta(seconds=s)

    code = v.main(list(argv), runner=runner, printer=lines.append, confirm=confirm, sleep=sleep, now=now,
                  evidence_path=sandbox_dir / "evidencia" / "validacion_4_3.json", s3_rows=s3_rows or s3_rows_fake(),
                  s3_get=s3_get or s3_get_fake())
    evidence = json.loads((sandbox_dir / "evidencia" / "validacion_4_3.json").read_text())
    return code, lines, evidence


def steps(evidence, part):
    return {s["id"]: s for s in evidence["parts"][part]["steps"]}


# --- utilidades puras ---------------------------------------------------------------------------


def test_table_of_y_layout_problems():
    assert v.table_of("curated/dim_user/part-00000.parquet") == "dim_user"
    assert v.table_of("curated/edges_user_brand/part-00000.parquet") is None and v.table_of("curated/_ta_placeholder/x") is None
    assert v.layout_problems(curated_keys() + ["curated/fact_posts/"]) == []
    bad = v.layout_problems(curated_keys() + ["curated/fact_posts/dt=2026-10-24/part-00001.parquet",
                                              "curated/dim_user/part-00001.parquet", "curated/fact_posts/x.parquet",
                                              "curated/fact_posts/dt=2025-12-31/part-00000.parquet"])
    assert sum("archivo de más" in b for b in bad) == 2 and any("fuera" in b for b in bad) and any("proyección" in b for b in bad)
    assert any("falta la tabla dim_date" in b for b in v.layout_problems([k for k in curated_keys() if "dim_date" not in k]))


def test_curated_schema_problems_con_parquet_real():
    for table in v.TABLES:
        problems, n = v.curated_schema_problems(table_parquet(table), table)
        assert problems == [] and n == 1, table
    problems, _ = v.curated_schema_problems(table_parquet("fact_posts", wrong=True), "fact_posts")
    assert problems and "date_key" in problems[0] and "int" in problems[0]


def test_costos_resumen_y_cifras_de_hoy():
    report = make_report("r1")
    assert v.curated_s3_cost(report) == pytest.approx(544 * 0.0004 / 1000 + (9 + 2 + 10) * 0.005 / 1000)
    assert "files" not in v.report_summary(report)["output"] and len(v.files_by_key(report)) == 9
    assert v.failed_checks(report) == {} and list(v.failed_checks(make_report("r", bad_check=True))) == ["emotion_n_posts_eq_rows_with_emotion"]
    assert v.compare_today({"dim_user": 5936, "trusted_rows": 221629}) == []
    assert v.compare_today({"dim_user": 6000}) == ["dim_user 6000 (el 26-oct: 5936)"]


def test_start_job_run_de_validate42_con_otro_job(sandbox):
    world = World()
    runner = FakeRunner(world)
    ctx = v.Ctx(runner=runner, options=v.build_parser().parse_args(["run"]), printer=lambda s: None, sleep=lambda s: None,
                evidence_path=sandbox / "evidencia" / "x.json")
    part = v.Part(ctx, "X", "x")
    jr, error, tries = v42.start_job_run(ctx, {"--ta_validacion": "4.3"}, part, job=v.JOB)
    assert jr == "jr_d1" and error == "" and tries == 1
    assert v42.wait_job_run(ctx, part, jr, job=v.JOB)["JobRunState"] == "SUCCEEDED"
    assert v42.get_job(ctx, v.JOB)["MaxCapacity"] == 0.0625


# --- A y B -------------------------------------------------------------------------------------


def test_preflight_ok_sin_trusted_local(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "preflight", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert code == 0 and s["A2"]["status"] == "OK" and s["A4"]["status"] == "OK" and s["A6"]["status"] == "OK"
    assert s["A8"]["status"] == "AVISO" and "sync --layer trusted" in s["A8"]["detail"]
    assert ev["lote"] == "4.3" and ev["hu"] == ["HU-027", "HU-030"]


def test_preflight_horario_encendido_avisa_con_off_y_on(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "preflight", sandbox_dir=sandbox)
    a6 = steps(ev, "A")["A6"]
    assert code == 0 and a6["status"] == "AVISO" and "estado normal" in a6["detail"]
    assert "validate_4_3.py off" in a6["detail"] and "on --yes" in a6["detail"]


def test_preflight_encuentra_el_trusted_local(sandbox):
    (sandbox / "cache" / "s3" / BUCKET / "trusted" / "posts").mkdir(parents=True)
    code, _, ev = run_part(FakeRunner(World()), "preflight", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "A")["A8"]["status"] == "OK" and "copia de S3" in steps(ev, "A")["A8"]["detail"]


def test_tests_ok_y_falla_si_cambia_el_conteo(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "tests", sandbox_dir=sandbox)
    assert code == 0 and all(s["status"] == "OK" for s in steps(ev, "B").values()) and len(steps(ev, "B")) == 5
    runner = FakeRunner(World(), pytest_out={"src/lakehouse": f"{v.B1_EXPECTED - 1} passed, 1 skipped in 1s"})
    code, _, ev = run_part(runner, "tests", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "B")["B1"]["status"] == "FALLA"


# --- L (local) ---------------------------------------------------------------------------------


def done_line(**kw):
    base = {"event": "glue_job_done", "rows_read": 221629, "fact_rows": 221629, "fact_partitions": 181, "tables": TABLE_ROWS,
            "nlp_missing": 65263, "objects_written": 187, "objects_unchanged": 0, "files_changed": 187, "objects_deleted": 0,
            "conciliation_ok": True, "duration_s": 4.3, "cpu_seconds": 6.1, "peak_rss_mb": 429.0,
            "environment": {"python": "3.9.25", "pyarrow": "7.0.0"}}
    base.update(kw)
    return json.dumps({"event": "glue_job_start"}) + "\n" + json.dumps(base) + "\n"


def seed_local(sandbox, wrong=False):
    (sandbox / "cache" / "s3" / BUCKET / "trusted" / "posts").mkdir(parents=True)
    dst = sandbox / "cache" / "trusted_to_curated_local" / "curated"
    for key in curated_keys():
        path = dst.parent / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(table_parquet(v.table_of(key), wrong=wrong))


def test_local_sin_trusted_avisa(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "L")["L0"]["status"] == "AVISO"


def test_local_con_trusted_idempotencia_y_esquemas(sandbox):
    seed_local(sandbox)
    runner = FakeRunner(World(), local_out=[done_line(), done_line(objects_written=0, objects_unchanged=187, files_changed=0)])
    code, lines, ev = run_part(runner, "local", "--cache", str(sandbox / "cache"), "--python", "/py39/bin/python", sandbox_dir=sandbox)
    s = steps(ev, "L")
    assert code == 0, "\n".join(lines)
    assert [s[k]["status"] for k in ("L0", "L1", "L2", "L3")] == ["OK"] * 4
    cmd = next(c for c in runner.calls if len(c) > 1 and c[1].endswith("trusted_to_curated.py"))
    assert cmd[0] == "/py39/bin/python" and str(sandbox / "cache" / "trusted_to_curated_local") in cmd
    assert "pyarrow 7.0.0" in s["L1"]["detail"]


def test_local_avisa_cifras_distintas_y_falla_si_escribe_otra_vez(sandbox):
    seed_local(sandbox)
    runner = FakeRunner(World(), local_out=[done_line(tables=dict(TABLE_ROWS, dim_user=6000)), done_line(files_changed=3)])
    code, _, ev = run_part(runner, "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    s = steps(ev, "L")
    assert code == 1 and s["L1"]["status"] == "AVISO" and "dim_user 6000" in s["L1"]["detail"] and s["L2"]["status"] == "FALLA"


def test_local_esquema_distinto_falla(sandbox):
    seed_local(sandbox, wrong=True)
    runner = FakeRunner(World(), local_out=[done_line(), done_line(objects_written=0, files_changed=0)])
    code, _, ev = run_part(runner, "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "L")["L3"]["status"] == "FALLA" and "date_key" in steps(ev, "L")["L3"]["detail"]


# --- C (Terraform) -----------------------------------------------------------------------------


def test_terraform_aplica_lo_esperado(sandbox):
    runner = FakeRunner(World(), plans=[plan(UPDATES, CREATES), plan()])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C2"]["status"] == "OK" and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK"
    assert runner.count("-target=module.glue_workflow -target=module.athena") == 2 and runner.count(" apply ") == 1


def test_terraform_no_aplica_si_cambia_otra_cosa(sandbox):
    other = 'module.glue_workflow.aws_glue_job.this["raw_to_trusted"]'
    runner = FakeRunner(World(), plans=[plan(UPDATES + [other], CREATES)])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C2"]["status"] == "FALLA" and runner.count(" apply ") == 0
    runner = FakeRunner(World(), plans=[plan(UPDATES, CREATES, job_diff=("description", "max_capacity"))])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "max_capacity" in steps(ev, "C")["C2"]["detail"] and runner.count(" apply ") == 0
    runner = FakeRunner(World(), plans=[plan(UPDATES, CREATES + ['module.athena.aws_athena_named_query.this["otra"]'])])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and runner.count(" apply ") == 0


def test_terraform_ya_aplicado_y_sin_apply(sandbox):
    code, _, ev = run_part(FakeRunner(World(), plans=[plan(), plan()]), "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and "ya aplicado" in s["C2"]["detail"] and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK"
    code, _, ev = run_part(FakeRunner(World(), plans=[plan(UPDATES, CREATES)]), "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C3"]["status"] == "AVISO" and s["C3b"]["status"] == "OMITIDO"


# --- D (corrida real) --------------------------------------------------------------------------


def test_run_completo(sandbox):
    world = World()
    world.objects[f"curated/_ta_placeholder/{v.JOB}/viejo.json"] = "{}"
    runner = FakeRunner(world)
    code, lines, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 0, "\n".join(lines)
    assert list(s) == ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"]
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert s["D3"]["data"]["max_capacity"] == 0.0625 and "pyarrow 7.0.0" in s["D4"]["detail"]
    assert "decisión 2" in s["D6"]["detail"] and "files" not in s["D5"]["data"]["report"]["output"]
    assert runner.count("start-job-run") == 1


def test_run_avisa_si_el_horario_esta_encendido(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "D")["D0"]["status"] == "AVISO" and steps(ev, "D")["D8"]["status"] == "OK"


def test_run_falla_sin_aplicar(sandbox):
    world = World()
    world.capacity = 1.0
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "MaxCapacity" in steps(ev, "D")["D1"]["detail"] and runner.count("start-job-run") == 0
    world = World()
    world.script = "print('marcador')\n"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "script de S3" in steps(ev, "D")["D1"]["detail"]


def test_run_no_confirmado_y_job_fallido(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "run", sandbox_dir=sandbox, confirm=lambda q: False)
    assert code == 0 and steps(ev, "D")["D3"]["status"] == "AVISO" and runner.count("start-job-run") == 0
    world = World()
    world.job_state = "FAILED"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    d3 = steps(ev, "D")["D3"]
    assert code == 1 and d3["status"] == "FALLA" and "MemoryError" in d3["detail"] and "D4" not in steps(ev, "D")


def test_run_reintenta_por_concurrencia(sandbox):
    world = World()
    world.busy_once = True
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    assert code == 0 and "intento 2" in steps(ev, "D")["D3"]["detail"] and runner.count("start-job-run") == 2


def test_run_conciliacion_que_no_cuadra(sandbox):
    world = World()
    world.report_overrides = {"bad_check": True}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D6"]["status"] == "FALLA" and "emotion_n_posts" in steps(ev, "D")["D6"]["detail"]
    code, _, ev = run_part(FakeRunner(World()), "run", "--yes", sandbox_dir=sandbox, s3_rows=s3_rows_fake(trusted=221600))
    assert code == 1 and steps(ev, "D")["D6"]["status"] == "FALLA"


def test_run_detecta_marcas_aristas_y_layout(sandbox):
    world = World()
    world.add_placeholder_on_run = True
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "0 → 1" in steps(ev, "D")["D7"]["detail"]
    world = World()
    world.touch_edges = True
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D7"]["status"] == "FALLA"
    world = World()
    world.extra_key = "curated/dim_user/part-00001.parquet"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "archivo de más" in steps(ev, "D")["D7"]["detail"]


def test_run_esquema_distinto_y_pico_alto(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "run", "--yes", sandbox_dir=sandbox, s3_get=s3_get_fake(wrong=True))
    assert code == 1 and steps(ev, "D")["D8"]["status"] == "FALLA"
    world = World()
    world.peak = 900.0
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "D")["D4"]["status"] == "AVISO" and "1 DPU" in steps(ev, "D")["D4"]["detail"]


# --- E (Athena) ----------------------------------------------------------------------------------


def with_report(world):
    world.run_curated("jr_prev")
    return world


def test_athena_ok(sandbox):
    world = with_report(World())
    code, lines, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert len(s["E6"]["data"]["queries"]) == 5 and s["E7"]["data"]["queries"] == 10
    assert "65263 filas sin NLP" in s["E3"]["detail"]


def test_athena_detecta_diferencias(sandbox):
    world = with_report(World())
    world.origins = {"historical": 50000}
    world.sql["trend"]["promedios_distintos"] = 2
    world.sql["links"]["sin_usuario"] = 1
    world.sql["user"]["filas_distintas"] = 3
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 1 and [s[k]["status"] for k in ("E1", "E2", "E3", "E4", "E5")] == ["FALLA", "FALLA", "FALLA", "OK", "FALLA"]
    world = with_report(World())
    world.sql["trend"]["con_sentimiento"] = 221629  # sin NLP contadas en el sentimiento: contra la decisión 2
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert code == 1 and "sin NLP" in steps(ev, "E")["E3"]["detail"]


def test_athena_consulta_guardada_falta_o_distinta(sandbox):
    world = with_report(World())
    del world.named["ta-curated-precision-nlp"]
    world.named["ta-curated-tendencia-diaria"] = "SELECT 1"
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    e6 = steps(ev, "E")["E6"]
    assert code == 1 and "ta-curated-precision-nlp: no está" in e6["detail"] and "texto guardado" in e6["detail"]


def test_athena_latencia_alta_avisa(sandbox):
    world = with_report(World())
    world.query_ms = 4200
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "E")["E8"]["status"] == "AVISO" and "SPICE" in steps(ev, "E")["E8"]["detail"]


# --- F (segunda corrida) -------------------------------------------------------------------------


def test_rerun_idempotente_y_costo(sandbox):
    world = with_report(World())
    runner = FakeRunner(world)
    code, lines, ev = run_part(runner, "rerun", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "F")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values() if x["id"] != "F7"), {k: x["detail"] for k, x in s.items()}
    assert s["F7"]["status"] == "OK" and "validate_4_3.py on --yes" in s["F7"]["detail"]
    f8 = s["F8"]["data"]
    assert f8["usd_per_day"] == pytest.approx(f8["usd_per_run"] * 24, rel=1e-3) and f8["curated_usd"] < f8["trusted_usd"]
    assert f8["usd_per_month"] == pytest.approx(f8["usd_per_day"] * 30, rel=1e-2)
    assert s["F6"]["data"]["properties"]["ta_curated_files_changed"] == "0"


def test_rerun_detecta_cambios_y_sin_corrida_previa(sandbox):
    world = with_report(World())
    world.rerun_changed = 2
    code, _, ev = run_part(FakeRunner(world), "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F3"]["status"] == "FALLA"
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F1"]["status"] == "FALLA" and runner.count("start-workflow-run") == 0


# --- status, off, on y CLI -----------------------------------------------------------------------


def test_status_encendido_es_lo_normal(sandbox):
    world = with_report(World())
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "OK" and "estado normal" in s["S1"]["detail"] and "9 objetos" in s["S3"]["detail"]
    world.schedule["State"] = "DISABLED"
    world.objects[f"curated/_ta_placeholder/{v.JOB}/x.json"] = "{}"
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert s["S1"]["status"] == "AVISO" and "on --yes" in s["S1"]["detail"] and s["S5"]["status"] == "AVISO"


def test_off_y_on_dejan_el_horario_encendido(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    world.objects[f"curated/_ta_placeholder/{v.JOB}/x.json"] = "{}"
    code, _, ev = run_part(FakeRunner(world), "off", sandbox_dir=sandbox)
    assert code == 0 and world.schedule["State"] == "DISABLED" and ev["lote"] == "4.3"
    assert f"curated/_ta_placeholder/{v.JOB}/x.json" in world.removed
    questions = []
    code, _, ev = run_part(FakeRunner(world), "on", sandbox_dir=sandbox, confirm=lambda q: questions.append(q) or True)
    s = steps(ev, "on")
    assert code == 0 and world.schedule["State"] == "ENABLED" and s["N1"]["status"] == "OK" and s["N2"]["status"] == "OK"
    assert world.updates[-1]["Target"]["RetryPolicy"]["MaximumRetryAttempts"] == 0
    assert "estimado" in questions[0] and str(round(v.ESTIMATED_USD_PER_RUN * 24, 3)) in questions[0]


def test_on_usa_el_costo_medido_y_no_confirmado_no_enciende(sandbox):
    world = with_report(World())
    run_part(FakeRunner(world), "rerun", "--yes", sandbox_dir=sandbox)
    questions = []
    code, _, ev = run_part(FakeRunner(world), "on", sandbox_dir=sandbox, confirm=lambda q: questions.append(q) or False)
    assert code == 0 and steps(ev, "on")["N1"]["status"] == "AVISO" and world.schedule["State"] == "DISABLED"
    assert "medido en F8" in questions[0]


def test_fuera_de_wsl_sale_con_2(sandbox, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    code = v.main(["run"], runner=FakeRunner(World()), printer=lines.append, evidence_path=sandbox / "evidencia" / "x.json")
    assert code == 2 and "Windows" in lines[-1]


def test_main_por_script(sandbox):
    path = v.PROJECT / "scripts" / "validate_4_3.py"
    spec = importlib.util.spec_from_file_location("validate_4_3_script", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main is v.main and sys.modules["ops.validate43"] is v
