"""Pruebas del validador del Lote 4.2 con un AWS, un Terraform y un job falsos (sin red, Docker ni nube)."""

from __future__ import annotations

import hashlib
import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.parquet_rows import parse_ts
from ops import validate41 as v41
from ops import validate42 as v
from quality import testing as qt

ACCOUNT = "648617469176"
BUCKET = f"ta-dev-{ACCOUNT}-lake"
NOW = datetime(2026, 10, 26, 18, 0, 0, tzinfo=timezone.utc)
ROLE = f"arn:aws:iam::{ACCOUNT}:role/ta-glue-role"
PARTS = [("facebook", "2026-10-21"), ("facebook", "2026-10-22"), ("instagram", "2026-10-23"),
         ("reddit", "2026-05-01"), ("reddit", "2026-10-24"), ("instagram", "2026-04-27")]
TRUSTED_BY_SOURCE = {"facebook": 73890, "instagram": 73946, "reddit": 73793}
JOB_PLAN_UPDATES = sorted(v.EXPECTED_UPDATES)


def arg(cmd, name):
    for i, c in enumerate(cmd):
        if c == name and i + 1 < len(cmd):
            return cmd[i + 1]
    return None


def ok(data):
    return v.Result(0, json.dumps(data))


def err(code):
    return v.Result(254, "", f"\nAn error occurred ({code}) when calling the Operation: x")


def key_of(source, dt):
    return f"trusted/posts/source={source}/dt={dt}/part-00000.parquet"


def make_report(run_id, wf_run_id=None, *, changed=6, rows_read=221637, sentinels=6, dups=2, written=221629,
                stream_rows=171637, deleted=0, sha="a"):
    files = [{"key": key_of(s, d), "rows": 10, "bytes": 1000, "sha256": hashlib.sha256(f"{sha}{s}{d}".encode()).hexdigest()}
             for s, d in PARTS]
    return {
        "schema": "ta-raw-to-trusted-report@1", "run_id": run_id, "workflow_run_id": wf_run_id,
        "report_key": f"quality/2026-10-26/raw_to_trusted/{run_id}.json",
        "input": {"objects": 13257, "by_origin": {"stream": {"rows": stream_rows, "objects": 1155},
                                                  "historical": {"rows": rows_read - stream_rows, "objects": 12102}}},
        "counts": {"rows_read": rows_read, "rejected": {"total": 0}, "sentinels": {"rows": sentinels},
                   "duplicates": {"rows_removed": dups, "groups_with_other_differences": 1},
                   "nlp_missing": {"rows": 65263, "since_nlp_since": 0}, "rows_written": written},
        "output": {"partitions": len(PARTS), "objects_written": len(PARTS), "objects_deleted": deleted,
                   "partitions_changed": changed, "by_source": {s: {"rows": n} for s, n in TRUSTED_BY_SOURCE.items()},
                   "dt_min": "2026-04-27", "dt_max": "2026-10-24", "bytes": 67_000_000, "files": files},
    }


def trusted_parquet(columns=None):
    row = qt.make_row("stream", "reddit", 1, datetime(2026, 10, 23, 10, tzinfo=timezone.utc))
    row["text_clean"] = "x"
    schema = qt.trusted_arrow_schema()
    cols = {}
    for f in schema:
        value = row.get(f.name)
        cols[f.name] = [parse_ts(value) if pa.types.is_timestamp(f.type) else value]
    table = pa.Table.from_pydict(cols, schema=schema)
    if columns:
        table = table.select(columns)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# --- AWS falso ---------------------------------------------------------------------------------


class World:
    def __init__(self):
        self.caller = f"arn:aws:iam::{ACCOUNT}:user/franklin"
        self.schedule = {"Name": v.SCHEDULE, "GroupName": "default", "State": "DISABLED", "ScheduleExpression": "rate(1 hour)",
                         "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
                         "Target": {"Arn": v41.SCHEDULER_TARGET, "RoleArn": "r", "Input": json.dumps({"Name": v.WORKFLOW}),
                                    "RetryPolicy": {"MaximumRetryAttempts": 0}}}
        self.updates = []
        self.capacity = 1.0
        self.script = v.JOB_FILE.read_text(encoding="utf-8")
        self.objects = {"trusted/": "", "curated/": ""}
        self.runs, self.details, self.props = [], {}, {}
        self.job_runs = {}
        self.running_job = []
        self.logs = []
        self.queries = {}
        self.job_state = "SUCCEEDED"
        self.report_overrides = {}
        self.rerun_changed = 0
        self.busy_once = False
        self.dup_rows = 0
        self.quality = {"centinelas": 0, "dt_distinto": 0, "text_clean_nulo": 0, "text_clean_mas_largo": 0,
                        "clave_nula": 0, "sin_nlp": 65263, "sin_nlp_desde": 0}
        self.add_placeholder_on_run = False
        self.extra_key = None
        self.n = 0
        self.removed = []

    def run_job(self, job_run_id, wf_run_id=None):
        """Lo que haría el job real: reescribe trusted/posts/ y deja su reporte."""
        first = "quality/_latest/raw_to_trusted.json" not in self.objects
        report = make_report(wf_run_id or f"manual-{job_run_id}", wf_run_id,
                             changed=len(PARTS) if first else self.rerun_changed, **self.report_overrides)
        for s, d in PARTS:
            self.objects[key_of(s, d)] = "PAR1"
        if self.extra_key:
            self.objects[self.extra_key] = "PAR1"
        if self.add_placeholder_on_run:
            self.objects["trusted/_ta_placeholder/ta-job-raw-to-trusted/x.json"] = "{}"
        text = json.dumps(report)
        self.objects[report["report_key"]] = text
        self.objects["quality/_latest/raw_to_trusted.json"] = text
        if wf_run_id:
            self.props[wf_run_id] = {"ta_validacion": "4.2", "ta_trusted_report": report["report_key"],
                                     "ta_trusted_rows": str(report["counts"]["rows_written"]),
                                     "ta_trusted_partitions_changed": str(report["output"]["partitions_changed"])}
        done = {"event": "glue_job_done", "report": report["report_key"], "rows_read": report["counts"]["rows_read"],
                "rows_written": report["counts"]["rows_written"], "partitions": len(PARTS), "read_seconds": 4.1,
                "duration_s": 30.2, "peak_rss_mb": 650.0, "environment": {"python": "3.9.18", "pyarrow": "7.0.0", "boto3": "1.21.21"}}
        self.logs.append((v.GLUE_OUTPUT_LOG, job_run_id, json.dumps(done)))

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
                return ok({"Job": {"Name": v.JOB, "Role": ROLE, "MaxRetries": 0, "MaxCapacity": self.capacity,
                                   "Command": {"Name": "pythonshell", "PythonVersion": "3.9",
                                               "ScriptLocation": f"s3://{BUCKET}/_glue/scripts/{v.JOB}.py"},
                                   "DefaultArguments": {"--lake_bucket": BUCKET, "--ta_input_prefix": "raw/",
                                                        "--ta_output_prefix": "trusted/", "library-set": "analytics"}}})
            if op == "start-job-run":
                if self.busy_once:
                    self.busy_once = False
                    return err("ConcurrentRunsExceededException")
                assert json.loads(arg(cmd, "--arguments")) == {"--ta_validacion": "4.2"}
                self.n += 1
                jid = f"jr_d{self.n}"
                self.job_runs[jid] = {"Id": jid, "JobRunState": self.job_state, "ExecutionTime": 95, "MaxCapacity": self.capacity,
                                      "ErrorMessage": "MemoryError" if self.job_state != "SUCCEEDED" else None}
                if self.job_state == "SUCCEEDED":
                    self.run_job(jid)
                return ok({"JobRunId": jid})
            if op == "get-job-run":
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
                self.run_job(j1, rid)
                self.objects[f"curated/_ta_placeholder/{v.NEXT_JOB}/{rid}.json"] = "{}"
                nodes = [{"Type": "JOB", "Name": v.JOB, "JobDetails": {"JobRuns": [{"Id": j1, "JobRunState": "SUCCEEDED", "ExecutionTime": 88}]}},
                         {"Type": "JOB", "Name": v.NEXT_JOB, "JobDetails": {"JobRuns": [{"Id": j2, "JobRunState": "SUCCEEDED", "ExecutionTime": 12}]}}]
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
                return ok({"QueryExecution": {"Status": {"State": "SUCCEEDED"}, "Statistics": {"DataScannedInBytes": 5_000_000},
                                              "ResultConfiguration": {"OutputLocation": f"s3://{BUCKET}/athena-results/x.csv"}}})
            if op == "get-query-results":
                cols, rows = self.query_result(self.queries[arg(cmd, "--query-execution-id")])
                return ok({"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": c} for c in cols]},
                                         "Rows": [{"Data": [{"VarCharValue": c} for c in cols]}]
                                         + [{"Data": [{"VarCharValue": str(x)} for x in r]} for r in rows]}})
        return v.Result(0, "{}")

    def query_result(self, sql):
        total = sum(TRUSTED_BY_SOURCE.values())
        if "ta_raw.posts_raw" in sql:
            return ["event_ids"], [[221635]]
        if "count_if" in sql:
            return list(self.quality), [list(self.quality.values())]
        if "GROUP BY source, origin" in sql:
            rows = []
            for s, n in TRUSTED_BY_SOURCE.items():
                rows.append([s, "historical", 16667 if s != "instagram" else 16666, 180, "2026-04-27", "2026-10-23"])
                rows.append([s, "stream", n - rows[-1][2], 4, "2026-10-21", "2026-10-24"])
            return ["source", "origin", "filas", "dts", "desde", "hasta"], rows
        return ["filas", "event_ids"], [[total + self.dup_rows, total]]


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
                return v.Result(0, self.apply_text or "Apply complete! Resources: 0 added, 2 changed, 0 destroyed.")
            if sub == "output":
                return ok({"lake_bucket": {"value": BUCKET}})
            return v.Result(0, "ok")
        if "pytest" in cmd:
            target = next(c for c in cmd if c.startswith(("src/", "contracts/")))
            default = {"src/lakehouse": v.B1_EXPECTED, "src/ops/tests/test_ops_validate42.py": v.B2_EXPECTED,
                       "contracts/tests": 18, "src/ops/tests/test_ops_validate41.py": v.B4_EXPECTED,
                       "src/ops/tests/test_ops_validate24.py": v.B5_EXPECTED}
            return v.Result(0, self.pytest_out.get(target, f"{default.get(target, 1)} passed in 1s"))
        if cmd[0] == sys.executable and cmd[1].endswith("raw_to_trusted.py"):
            return v.Result(0, self.local_out.pop(0) if self.local_out else "")
        if cmd[:2] == ["gcloud", "dataflow"]:
            return v.Result(0, "[]")
        return v.Result(0, "")

    def count(self, text):
        return sum(1 for c in self.calls if text in " ".join(c))


def plan(updates=(), creates=(), job_diff=("max_capacity", "description")):
    rcs = []
    for a in updates:
        before, after = ({"max_capacity": 0.0625, "description": "a"}, {"max_capacity": 1, "description": "b"}) \
            if "aws_glue_job" in a else ({"source_hash": "x"}, {"source_hash": "y"})
        if "aws_glue_job" in a:
            before = {k: before.get(k, 0) for k in job_diff}
            after = {k: after.get(k, 1) for k in job_diff}
        rcs.append({"address": a, "mode": "managed", "change": {"actions": ["update"], "before": before, "after": after}})
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["create"]}} for a in creates]
    return {"resource_changes": rcs}


N_PARTS = len(PARTS)


def raw_rows(stream=171637, historical=50000, trusted=221629, trusted_objects=N_PARTS):
    def count(bucket, prefix):
        if prefix.startswith("trusted/"):
            return {"objects": trusted_objects, "rows": trusted, "by_source": {s: {"objects": 2, "rows": n} for s, n in TRUSTED_BY_SOURCE.items()},
                    "errors": [], "error_count": 0, "seconds": 1.0}
        rows = stream if "stream" in prefix else historical
        return {"objects": 1155 if "stream" in prefix else 12102, "rows": rows, "by_source": {}, "errors": [], "error_count": 0,
                "seconds": 2.0}
    return count


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
                  evidence_path=sandbox_dir / "evidencia" / "validacion_4_2.json", s3_rows=s3_rows or raw_rows(),
                  s3_get=s3_get or (lambda b, k: trusted_parquet()))
    evidence = json.loads((sandbox_dir / "evidencia" / "validacion_4_2.json").read_text())
    return code, lines, evidence


def steps(evidence, part):
    return {s["id"]: s for s in evidence["parts"][part]["steps"]}


# --- utilidades puras ---------------------------------------------------------------------------


def test_layout_problems():
    good = [key_of(s, d) for s, d in PARTS] + ["trusted/posts/"]
    assert v.layout_problems(good) == []
    bad = v.layout_problems([key_of("twitter", "2026-10-01"), key_of("reddit", "2025-12-31"),
                             "trusted/posts/source=reddit/dt=2026-10-01/part-00001.parquet",
                             key_of("reddit", "2026-10-01"), "trusted/posts/x.json"])
    assert any("desconocida" in b for b in bad) and any("proyección" in b for b in bad)
    assert any("archivo de más" in b for b in bad) and any("2 archivos" in b for b in bad) and any("fuera" in b for b in bad)


def test_conciliacion_y_resumen():
    report = make_report("r1")
    good, removed, parts = v.conciliation(report)
    assert good and removed == 8 and parts["rows_written"] == 221629
    assert not v.conciliation(make_report("r1", written=221620))[0]
    summary = v.report_summary(report)
    assert "files" not in summary["output"] and summary["output"]["partitions"] == len(PARTS)
    assert len(v.files_by_key(report)) == len(PARTS)


def test_normalized_y_costos():
    assert v.normalized("a\r\nb\n") == b"a\nb\n" == v.normalized(b"a\nb\n")
    assert v.job_cost(30, 1.0) == pytest.approx(60 * 0.44 / 3600)
    assert v.job_cost(120, 1.0) == pytest.approx(120 * 0.44 / 3600)
    assert v.s3_request_cost(make_report("r")) == pytest.approx(13257 * 0.0004 / 1000 + (len(PARTS) + 2 + 16) * 0.005 / 1000)


def test_schema_problems_con_parquet_real():
    problems, n = v.schema_problems(trusted_parquet())
    assert problems == [] and n == 1
    names = [f.name for f in qt.trusted_arrow_schema() if f.name != "text_clean"]
    problems, _ = v.schema_problems(trusted_parquet(columns=names))
    assert problems and "columnas" in problems[0]


# --- A y B -------------------------------------------------------------------------------------


def test_preflight_ok(sandbox):
    code, lines, ev = run_part(FakeRunner(World()), "preflight", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert code == 0 and s["A2"]["status"] == "OK" and s["A4"]["status"] == "OK" and s["A6"]["status"] == "OK"
    assert s["A8"]["status"] == "AVISO" and "profile_data.py sync" in s["A8"]["detail"]
    assert ev["lote"] == "4.2" and ev["hu"] == ["HU-026"]


def test_preflight_avisa_horario_encendido_y_job_corriendo(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    world.running_job = ["jr_x"]
    code, _, ev = run_part(FakeRunner(world), "preflight", sandbox_dir=sandbox)
    a6 = steps(ev, "A")["A6"]
    assert code == 0 and a6["status"] == "AVISO" and "jr_x" in a6["detail"] and "off" in a6["detail"]


def test_tests_ok_y_falla_si_cambia_el_conteo(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "tests", sandbox_dir=sandbox)
    assert code == 0 and all(s["status"] == "OK" for s in steps(ev, "B").values()) and len(steps(ev, "B")) == 5
    runner = FakeRunner(World(), pytest_out={"src/lakehouse": "89 passed, 1 skipped in 1s"})
    code, _, ev = run_part(runner, "tests", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "B")["B1"]["status"] == "FALLA"


# --- L (local) ---------------------------------------------------------------------------------


def done_line(**kw):
    base = {"event": "glue_job_done", "rows_read": 221637, "rows_written": 221629, "sentinels": 6, "duplicates_removed": 2,
            "nlp_missing": 65263, "partitions": 543, "partitions_changed": 543, "objects_deleted": 0, "conciliation_ok": True,
            "duration_s": 25.0, "peak_rss_mb": 800.0}
    base.update(kw)
    return json.dumps({"event": "glue_job_start"}) + "\n" + json.dumps(base) + "\n"


def test_local_sin_cache_avisa(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "L")["L0"]["status"] == "AVISO"


def test_local_con_cache_e_idempotencia(sandbox):
    (sandbox / "cache" / "s3" / BUCKET / "raw").mkdir(parents=True)
    runner = FakeRunner(World(), local_out=[done_line(), done_line(partitions_changed=0)])
    code, _, ev = run_part(runner, "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    s = steps(ev, "L")
    assert code == 0 and s["L1"]["status"] == "OK" and s["L2"]["status"] == "OK" and s["L3"]["status"] == "OMITIDO"
    cmd = next(c for c in runner.calls if c[0] == sys.executable)
    assert "--ta_local_dst" in cmd and str(sandbox / "cache" / "raw_to_trusted_local") in cmd
    runner = FakeRunner(World(), local_out=[done_line(), done_line(partitions_changed=3)])
    code, _, ev = run_part(runner, "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "L")["L2"]["status"] == "FALLA"


# --- C (Terraform) -----------------------------------------------------------------------------


def test_terraform_aplica_los_dos_cambios_esperados(sandbox):
    runner = FakeRunner(World(), plans=[plan(JOB_PLAN_UPDATES), plan()])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C2"]["status"] == "OK" and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK"
    assert runner.count("-target=module.glue_workflow") == 2 and runner.count(" apply ") == 1


def test_terraform_no_aplica_si_cambia_otra_cosa(sandbox):
    runner = FakeRunner(World(), plans=[plan(JOB_PLAN_UPDATES + ['module.glue_workflow.aws_glue_job.this["trusted_to_curated"]'])])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C2"]["status"] == "FALLA" and runner.count(" apply ") == 0
    runner = FakeRunner(World(), plans=[plan(JOB_PLAN_UPDATES, job_diff=("max_capacity", "role_arn"))])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "role_arn" in steps(ev, "C")["C2"]["detail"] and runner.count(" apply ") == 0


def test_terraform_ya_aplicado_y_sin_apply(sandbox):
    code, _, ev = run_part(FakeRunner(World(), plans=[plan(), plan()]), "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and "ya aplicado" in s["C2"]["detail"] and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK"
    code, _, ev = run_part(FakeRunner(World(), plans=[plan(JOB_PLAN_UPDATES)]), "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C3"]["status"] == "AVISO" and s["C3b"]["status"] == "OMITIDO"


# --- D (corrida real) --------------------------------------------------------------------------


def test_run_completo(sandbox):
    world = World()
    world.objects["trusted/_ta_placeholder/ta-job-raw-to-trusted/viejo.json"] = "{}"
    runner = FakeRunner(world)
    code, lines, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 0, "\n".join(lines)
    assert [k for k in s] == ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"]
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert s["D6"]["data"]["rows_written"] == 221629 and s["D3"]["data"]["max_capacity"] == 1.0
    assert "pyarrow 7.0.0" in s["D4"]["detail"] and "files" not in s["D5"]["data"]["report"]["output"]
    assert runner.count("start-job-run") == 1


def test_run_falla_sin_aplicar_la_capacidad_o_el_script(sandbox):
    world = World()
    world.capacity = 0.0625
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "MaxCapacity" in steps(ev, "D")["D1"]["detail"] and runner.count("start-job-run") == 0
    world = World()
    world.script = "print('marcador')\n"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "script de S3" in steps(ev, "D")["D1"]["detail"]


def test_run_script_con_crlf_es_el_mismo(sandbox):
    world = World()
    world.script = world.script.replace("\n", "\r\n")
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "D")["D1"]["status"] == "OK"


def test_run_no_confirmado_no_corre(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "run", sandbox_dir=sandbox, confirm=lambda q: False)
    assert code == 0 and steps(ev, "D")["D3"]["status"] == "AVISO" and runner.count("start-job-run") == 0


def test_run_job_fallido(sandbox):
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
    world.report_overrides = {"rows_read": 221640, "stream_rows": 171640}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D6"]["status"] == "FALLA"


def test_run_raw_cambiado_concilia_y_cifras_de_hoy_distintas_avisan(sandbox):
    world = World()
    world.report_overrides = {"rows_read": 221737, "stream_rows": 171737, "written": 221729}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox,
                           s3_rows=raw_rows(stream=171737, trusted=221729))
    assert code == 0 and steps(ev, "D")["D6"]["status"] == "OK"
    world = World()
    world.report_overrides = {"sentinels": 5, "written": 221630}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox, s3_rows=raw_rows(trusted=221630))
    assert code == 0 and steps(ev, "D")["D6"]["status"] == "AVISO"


def test_run_detecta_marcas_nuevas_y_layout(sandbox):
    world = World()
    world.add_placeholder_on_run = True
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "0 → 1" in steps(ev, "D")["D7"]["detail"]
    world = World()
    world.extra_key = "trusted/posts/source=reddit/dt=2026-05-01/part-00001.parquet"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "archivo de más" in steps(ev, "D")["D7"]["detail"]


def test_run_esquema_distinto(sandbox):
    names = [f.name for f in qt.trusted_arrow_schema() if f.name != "text_clean"]
    code, _, ev = run_part(FakeRunner(World()), "run", "--yes", sandbox_dir=sandbox, s3_get=lambda b, k: trusted_parquet(names))
    assert code == 1 and steps(ev, "D")["D8"]["status"] == "FALLA"


# --- E (Athena) ----------------------------------------------------------------------------------


def with_report(world):
    world.run_job("jr_prev")
    return world


def test_athena_ok(sandbox):
    world = with_report(World())
    code, lines, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()) and s["E1"]["data"]["duplicates_ratio"] == 0
    assert s["E4"]["data"]["raw_distinct"] == 221635 and 0.99 <= s["E4"]["data"]["ratio"] <= 1
    assert any("ta_trusted.posts" in q for q in world.queries.values())


def test_athena_duplicados_y_reglas(sandbox):
    world = with_report(World())
    world.dup_rows = 10
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert steps(ev, "E")["E1"]["status"] == "AVISO"
    world = with_report(World())
    world.dup_rows = 5000
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "E")["E1"]["status"] == "FALLA"
    world = with_report(World())
    world.quality["centinelas"] = 6
    world.quality["sin_nlp"] = 65264
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    e3 = steps(ev, "E")["E3"]
    assert code == 1 and "centinelas = 6" in e3["detail"] and "sin_nlp = 65264" in e3["detail"]


# --- F (segunda corrida) -------------------------------------------------------------------------


def test_rerun_idempotente_y_costo(sandbox):
    world = with_report(World())
    runner = FakeRunner(world)
    code, lines, ev = run_part(runner, "rerun", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "F")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert s["F6"]["data"]["properties"]["ta_trusted_partitions_changed"] == "0"
    assert s["F8"]["data"]["usd_per_run"] > 0 and s["F8"]["data"]["usd_per_day"] == pytest.approx(s["F8"]["data"]["usd_per_run"] * 24, rel=1e-3)
    assert not any(k.startswith("curated/_ta_placeholder/") for k in world.objects)
    assert world.schedule["State"] == "DISABLED"


def test_rerun_detecta_cambios(sandbox):
    world = with_report(World())
    world.rerun_changed = 2
    code, _, ev = run_part(FakeRunner(world), "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F3"]["status"] == "FALLA"


def test_rerun_sin_corrida_previa(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F1"]["status"] == "FALLA" and runner.count("start-workflow-run") == 0


# --- status, off y CLI ---------------------------------------------------------------------------


def test_status(sandbox):
    world = with_report(World())
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "OK" and "6 objetos" in s["S3"]["detail"] and "221629" in s["S3"]["detail"]


def test_off_reutiliza_el_del_4_1(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    world.objects["curated/_ta_placeholder/ta-job-trusted-to-curated/x.json"] = "{}"
    code, _, ev = run_part(FakeRunner(world), "off", sandbox_dir=sandbox)
    s = steps(ev, "off")
    assert code == 0 and world.schedule["State"] == "DISABLED" and s["O1"]["status"] == "OK"
    assert "curated/_ta_placeholder/ta-job-trusted-to-curated/x.json" in world.removed
    assert ev["lote"] == "4.2"


def test_fuera_de_wsl_sale_con_2(sandbox, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    code = v.main(["run"], runner=FakeRunner(World()), printer=lines.append, evidence_path=sandbox / "evidencia" / "x.json")
    assert code == 2 and "Windows" in lines[-1]
