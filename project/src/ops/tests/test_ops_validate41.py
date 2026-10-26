"""Pruebas del validador del Lote 4.1 con un AWS y un Terraform falsos (sin red, Docker ni nube)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lakehouse import catalog_spec as cs
from ops import validate41 as v

ACCOUNT = "648617469176"
BUCKET = f"ta-dev-{ACCOUNT}-lake"
KMS = f"arn:aws:kms:us-east-1:{ACCOUNT}:key/11111111-2222-3333-4444-555555555555"
NOW = datetime(2026, 10, 26, 15, 0, 10, tzinfo=timezone.utc)
ROLE_ARNS = {k: f"arn:aws:iam::{ACCOUNT}:role/{n}" for k, n in v.ROLES.items()}
OUTPUTS = {
    "lake_bucket": BUCKET, "kms_key_arn": KMS, "glue_db_names": dict(v.DATABASES), "athena_workgroup": v.WORKGROUP,
    "athena_results_location": f"s3://{BUCKET}/athena-results/", "athena_bytes_scanned_cutoff": v.DEFAULT_CUTOFF,
    "glue_role_arn": ROLE_ARNS["glue"], "quicksight_role_arn": ROLE_ARNS["quicksight"], "glue_workflow_name": v.WORKFLOW,
    "glue_job_names": dict(v.JOBS), "lakehouse_schedule_name": v.SCHEDULE,
    "lakehouse_scheduler_role_arn": ROLE_ARNS["scheduler"], "glue_failures_log_group": v.FAIL_LOG_GROUP,
    "exporter_role_arn": f"arn:aws:iam::{ACCOUNT}:role/ta-gcp-exporter-role",
}
HIST = {"reddit": 16667, "facebook": 16667, "instagram": 16666}
STREAM = {"reddit": 400, "facebook": 380, "instagram": 375}


def arg(cmd, name):
    for i, c in enumerate(cmd):
        if c == name and i + 1 < len(cmd):
            return cmd[i + 1]
    return None


def args_after(cmd, name):
    out, take = [], False
    for c in cmd:
        if c == name:
            take = True
            continue
        if take:
            if c.startswith("--"):
                break
            out.append(c)
    return out


def iso(dt):
    return dt.isoformat()


def not_found(code="ResourceNotFoundException"):
    return v.Result(254, "", f"\nAn error occurred ({code}) when calling the Operation: not found")


def ok(data):
    return v.Result(0, json.dumps(data))


# --- AWS falso ---------------------------------------------------------------------------------


class World:
    def __init__(self):
        self.caller = f"arn:aws:iam::{ACCOUNT}:user/franklin"
        self.schedule = {"Name": v.SCHEDULE, "GroupName": "default", "Arn": "arn:x", "State": "DISABLED",
                         "ScheduleExpression": "rate(1 hour)", "ScheduleExpressionTimezone": "UTC",
                         "FlexibleTimeWindow": {"Mode": "OFF"}, "Description": "d", "ActionAfterCompletion": "NONE",
                         "Target": {"Arn": v.SCHEDULER_TARGET, "RoleArn": ROLE_ARNS["scheduler"],
                                    "Input": json.dumps({"Name": v.WORKFLOW}),
                                    "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 3600}}}
        self.updates = []
        self.temp = {}
        self.created_temp = []
        self.runs = []
        self.details = {}
        self.job_runs = {n: [] for n in v.JOBS.values()}
        self.objects = {"raw/origin=historical/x.parquet", "raw/origin=stream/y.parquet", "trusted/", "curated/", "quality/"}
        self.removed = []
        self.logs = {}
        self.queries = {}
        self.stopped = []
        self.order_ok = True
        self.fail_event = True
        self.scheduler_runs = True
        self.scanned = 45_000_000
        self.hist_rows = dict(HIST)
        self.query_state = "SUCCEEDED"
        self.cutoff = v.DEFAULT_CUTOFF
        self.extra_glue_action = None
        self.glue_allows_raw_put = False
        self.previous_actions = set(v.PREVIOUS_ACTIONS)
        self.tables = cs.load_tables_json()
        self.table_override = {}
        self.script_override = {}
        self.n = 0
        # Como Glue (v2.32): tras cada corrida arrancada a mano, los próximos `busy_after_run`
        # StartWorkflowRun dan ConcurrentRunsExceededException aunque la corrida ya esté COMPLETED.
        self.busy_after_run = 0
        self._busy = 0
        # get-workflow-runs muestra una corrida RUNNING durante estas consultas.
        self.running_polls = 0

    # glue
    def glue_table(self, spec):
        t = cs.resolve(spec, BUCKET)
        data = {"Name": t.name, "TableType": "EXTERNAL_TABLE", "Parameters": {**t.parameters, **cs.EXTRA_PARAMETERS},
                "PartitionKeys": [{"Name": c.name, "Type": c.type} for c in t.partition_keys],
                "StorageDescriptor": {"Columns": [{"Name": c.name, "Type": c.type} for c in t.columns], "Location": t.location,
                                      "InputFormat": cs.PARQUET_INPUT, "SerdeInfo": {"SerializationLibrary": cs.PARQUET_SERDE}}}
        return self.table_override.get(spec.full_name, data)

    def start_run(self, props, started):
        self.n += 1
        rid = f"wr_{self.n:04d}"
        j1, j2 = v.JOBS["raw_to_trusted"], v.JOBS["trusted_to_curated"]
        t0 = started + timedelta(seconds=5)
        fail = "ta_force_fail" in props
        jr1 = {"Id": f"jr_{rid}_1", "JobRunState": "FAILED" if fail else "SUCCEEDED", "StartedOn": iso(t0),
               "CompletedOn": iso(t0 + timedelta(seconds=70)), "ExecutionTime": 70,
               "ErrorMessage": "RuntimeError: ta_force_fail: fallo provocado" if fail else None}
        nodes = [{"Type": "TRIGGER", "Name": v.START_TRIGGER}, {"Type": "JOB", "Name": j1, "JobDetails": {"JobRuns": [jr1]}}]
        if not fail:
            s2 = t0 + timedelta(seconds=90 if self.order_ok else 30)
            jr2 = {"Id": f"jr_{rid}_2", "JobRunState": "SUCCEEDED", "StartedOn": iso(s2), "CompletedOn": iso(s2 + timedelta(seconds=40)),
                   "ExecutionTime": 40}
            nodes.append({"Type": "JOB", "Name": j2, "JobDetails": {"JobRuns": [jr2]}})
            self.objects |= {f"trusted/_ta_placeholder/{j1}/{rid}.json", f"curated/_ta_placeholder/{j2}/{rid}.json"}
            self.logs.setdefault(v.GLUE_OUTPUT_LOG, []).extend([
                (jr1["Id"], json.dumps({"event": "glue_placeholder_done", "input_objects": 13257, "input_prefix": "raw/",
                                        "read_probe": "PAR1", "upstream_marker_found": None})),
                (jr2["Id"], json.dumps({"event": "glue_placeholder_done", "input_objects": 0, "input_prefix": "trusted/",
                                        "read_probe": "sin objetos Parquet", "upstream_marker_found": True}))])
        else:
            nodes.append({"Type": "JOB", "Name": j2})
            self.logs.setdefault(v.GLUE_ERROR_LOG, []).append((jr1["Id"], "Traceback ... RuntimeError: ta_force_fail: fallo"))
            if self.fail_event:
                self.logs.setdefault(v.FAIL_LOG_GROUP, []).append(
                    ("ev", json.dumps({"detail-type": "Glue Job State Change", "detail": {"jobName": j1, "state": "FAILED", "jobRunId": jr1["Id"]}})))
        stats = {"TotalActions": len([n for n in nodes if n.get("JobDetails")]), "FailedActions": 1 if fail else 0,
                 "SucceededActions": 1 if fail else 2}
        self.details[rid] = {"WorkflowRunId": rid, "Status": "COMPLETED", "Statistics": stats, "Graph": {"Nodes": nodes},
                             "StartedOn": iso(started)}
        self.runs.insert(0, {"WorkflowRunId": rid, "Status": "COMPLETED", "StartedOn": iso(started)})
        return rid

    def allows(self, role, action, resource, ctx_entries):
        via = "kms:ViaService" in ctx_entries
        key = resource.split(":::", 1)[1].split("/", 1)[1] if ":::" in resource and "/" in resource.split(":::", 1)[1] else ""
        prefix = re.search(r"ContextKeyName=s3:prefix,ContextKeyValues=([^,]+)", ctx_entries)
        prefix = prefix.group(1) if prefix else ""
        if role == "ta-glue-role":
            if action == "s3:GetObject":
                return key.startswith(("raw/", "trusted/", "curated/", "quality/", "_glue/"))
            if action in ("s3:PutObject", "s3:DeleteObject"):
                return key.startswith(("trusted/", "curated/", "quality/", "_glue/temp/")) or (
                    self.glue_allows_raw_put and key.startswith("raw/"))
            if action == "s3:ListBucket":
                return prefix.startswith(("raw/", "trusted/", "curated/", "quality/", "_glue/"))
            if action.startswith("kms:"):
                return via
            if action.startswith("logs:"):
                # Como el simulador real (v2.32): con el ARN de un flujo da implicitDeny siempre.
                return ":log-group:/aws-glue/" in resource and ":log-stream:" not in resource
            return action in ("glue:GetTable", "glue:GetWorkflowRunProperties")
        if role == "ta-quicksight-role":
            if action.startswith("athena:"):
                return resource.endswith("workgroup/ta-analytics") and action != "athena:DeleteWorkGroup"
            if action == "glue:GetTable":
                return "ta_curated" in resource
            if action == "s3:GetObject":
                return key.startswith(("curated/", "athena-results/"))
            if action == "s3:PutObject":
                return key.startswith("athena-results/")
            if action == "s3:ListBucket":
                return prefix.startswith(("curated/", "athena-results/"))
            if action.startswith("kms:"):
                return via
            return False
        return action == "glue:StartWorkflowRun" and resource.endswith(f"workflow/{v.WORKFLOW}")

    def query_result(self, sql):
        if "part_origin AS origin" in sql and "COUNT(DISTINCT event_id)" in sql:
            rows = [["historical", s, str(n), str(n)] for s, n in self.hist_rows.items()]
            rows += [["stream", s, str(n), str(n)] for s, n in STREAM.items()]
            cols = ["origin", "source", "filas", "event_ids"]
            return cols, rows, self.scanned
        if "COUNT(*) AS n" in sql:
            return ["n"], [["0"]], 0
        return ["origin", "brand", "sentiment_label", "filas"], [["stream", "pepsi", "positive", "12"]], 2_000_000

    def handle(self, cmd):  # noqa: C901 - despachador del AWS falso
        svc, op = cmd[1], cmd[2]
        if (svc, op) == ("sts", "get-caller-identity"):
            return ok({"Account": ACCOUNT, "Arn": self.caller})
        if svc == "scheduler":
            name = arg(cmd, "--name")
            if op == "get-schedule":
                if name == v.SCHEDULE and self.schedule:
                    return ok(self.schedule)
                if name in self.temp:
                    return ok(self.temp[name])
                return not_found()
            if op in ("update-schedule", "create-schedule"):
                body = json.loads(Path(arg(cmd, "--cli-input-json")[len("file://"):]).read_text())
                if op == "update-schedule":
                    self.updates.append(body)
                    self.schedule.update(body)
                    return ok({})
                self.created_temp.append(body)
                m = re.match(r"at\((.+)\)", body["ScheduleExpression"])
                at = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
                if self.scheduler_runs:
                    self.start_run({}, at + timedelta(seconds=2))
                else:
                    self.temp[body["Name"]] = body
                return ok({"ScheduleArn": "arn:temp"})
            if op == "delete-schedule":
                self.temp.pop(name, None)
                return ok({})
            if op == "list-schedules":
                return ok({"Schedules": [{"Name": n} for n in self.temp if n.startswith(arg(cmd, "--name-prefix"))]})
        if svc == "glue":
            if op == "get-databases":
                return ok({"DatabaseList": [{"Name": n, "LocationUri": f"s3://{BUCKET}/{k}/"} for k, n in v.DATABASES.items()]})
            if op == "get-tables":
                db = arg(cmd, "--database-name")
                return ok({"TableList": [self.glue_table(t) for t in self.tables if t.database == db]})
            if op == "get-trigger":
                name = arg(cmd, "--name")
                if name == v.START_TRIGGER:
                    return ok({"Trigger": {"Type": "ON_DEMAND", "WorkflowName": v.WORKFLOW, "Actions": [{"JobName": v.JOBS["raw_to_trusted"]}]}})
                return ok({"Trigger": {"Type": "CONDITIONAL", "State": "ACTIVATED", "WorkflowName": v.WORKFLOW,
                                       "Predicate": {"Conditions": [{"JobName": v.JOBS["raw_to_trusted"], "State": "SUCCEEDED"}]},
                                       "Actions": [{"JobName": v.JOBS["trusted_to_curated"]}]}})
            if op == "get-job":
                name = arg(cmd, "--job-name")
                return ok({"Job": {"Name": name, "Role": ROLE_ARNS["glue"], "MaxRetries": 0,
                                   "Command": {"Name": "pythonshell", "ScriptLocation": f"s3://{BUCKET}/_glue/scripts/{name}.py"},
                                   "DefaultArguments": {"--lake_bucket": BUCKET}}})
            if op == "get-workflow-runs":
                if self.running_polls > 0:
                    self.running_polls -= 1
                    return ok({"Runs": [{"WorkflowRunId": "wr_ajena", "Status": "RUNNING", "StartedOn": iso(NOW)}] + list(self.runs)})
                return ok({"Runs": list(self.runs)})
            if op == "start-workflow-run":
                if self._busy > 0:
                    self._busy -= 1
                    return not_found("ConcurrentRunsExceededException")
                props = dict(p.split("=", 1) for p in (arg(cmd, "--run-properties") or "").split(",") if p)
                rid = self.start_run(props, NOW)
                self._busy = self.busy_after_run
                return ok({"RunId": rid})
            if op == "get-workflow-run":
                return ok({"Run": self.details[arg(cmd, "--run-id")]})
            if op == "get-job-runs":
                return ok({"JobRuns": self.job_runs.get(arg(cmd, "--job-name"), [])})
            if op in ("stop-workflow-run", "batch-stop-job-run"):
                self.stopped.append((op, arg(cmd, "--run-id") or args_after(cmd, "--job-run-ids")))
                for r in self.runs:
                    if r["WorkflowRunId"] == arg(cmd, "--run-id"):
                        r["Status"] = "STOPPED"
                for runs in self.job_runs.values():
                    for jr in runs:
                        if jr["Id"] in args_after(cmd, "--job-run-ids"):
                            jr["JobRunState"] = "STOPPED"
                return ok({})
        if svc == "athena":
            if op == "start-query-execution":
                qid = f"q{len(self.queries) + 1}"
                self.queries[qid] = arg(cmd, "--query-string")
                return ok({"QueryExecutionId": qid})
            if op == "get-query-execution":
                qid = arg(cmd, "--query-execution-id")
                _, _, scanned = self.query_result(self.queries[qid])
                return ok({"QueryExecution": {"Status": {"State": self.query_state, "StateChangeReason": "" if self.query_state == "SUCCEEDED" else "boom"},
                                              "Statistics": {"DataScannedInBytes": scanned, "EngineExecutionTimeInMillis": 12000},
                                              "ResultConfiguration": {"OutputLocation": f"s3://{BUCKET}/athena-results/{qid}.csv"}}})
            if op == "get-query-results":
                cols, rows, _ = self.query_result(self.queries[arg(cmd, "--query-execution-id")])
                return ok({"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": c} for c in cols]},
                                         "Rows": [{"Data": [{"VarCharValue": c} for c in cols]}] + [{"Data": [{"VarCharValue": x} for x in r]} for r in rows]}})
            if op == "get-work-group":
                return ok({"WorkGroup": {"State": "ENABLED", "Configuration": {
                    "EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": self.cutoff,
                    "EngineVersion": {"EffectiveEngineVersion": "Athena engine version 3"},
                    "ResultConfiguration": {"OutputLocation": f"s3://{BUCKET}/athena-results/",
                                            "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": KMS}}}}})
            if op == "list-named-queries":
                return ok({"NamedQueryIds": ["n1", "n2"]})
            if op == "batch-get-named-query":
                return ok({"NamedQueries": [{"Name": "ta-raw-conteo-por-origen", "QueryString": "SELECT part_origin AS origin …"},
                                            {"Name": "ta-raw-sentimiento-7-dias", "QueryString": "SELECT 7 dias"}]})
        if svc == "iam":
            if op == "get-role":
                name = arg(cmd, "--role-name")
                key = next(k for k, n in v.ROLES.items() if n == name)
                return ok({"Role": {"Arn": ROLE_ARNS[key], "AssumeRolePolicyDocument": {"Statement": [
                    {"Effect": "Allow", "Principal": {"Service": v.TRUST_PRINCIPALS[key]}, "Action": "sts:AssumeRole"}]}}})
            if op == "get-role-policy":
                name = arg(cmd, "--role-name")
                if name in v.PREVIOUS_ROLES:
                    actions = sorted(self.previous_actions)
                else:
                    key = next(k for k, n in v.ROLES.items() if n == name)
                    actions = sorted(v.EXPECTED_ACTIONS[key] | ({self.extra_glue_action} if key == "glue" and self.extra_glue_action else set()))
                return ok({"PolicyDocument": {"Statement": [{"Effect": "Allow", "Action": actions, "Resource": "*"}]}})
            if op == "simulate-principal-policy":
                role = arg(cmd, "--policy-source-arn").rsplit("/", 1)[1]
                ctx_entries = " ".join(args_after(cmd, "--context-entries") + [c for c in cmd if c.startswith("ContextKeyName")])
                allowed = self.allows(role, arg(cmd, "--action-names"), arg(cmd, "--resource-arns"), ctx_entries)
                return ok({"EvaluationResults": [{"EvalDecision": "allowed" if allowed else "implicitDeny"}]})
        if svc == "s3api":
            if op == "list-objects-v2":
                keys = sorted(k for k in self.objects if k.startswith(arg(cmd, "--prefix")))
                limit = int(arg(cmd, "--max-items") or 10**9)
                return ok({"Contents": [{"Key": k, "Size": 1} for k in keys[:limit]]} if keys else {})
            if op == "head-object":
                key = arg(cmd, "--key")
                if key.startswith("athena-results/"):
                    return ok({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KMS})
                return ok({"ContentLength": 1}) if key in self.objects else not_found("404")
            if op == "get-bucket-encryption":
                return ok({"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": KMS}, "BucketKeyEnabled": True}]}})
        if svc == "s3":
            if op == "cp":
                name = cmd[3].rsplit("/", 1)[1][:-3]
                key = next(k for k, n in v.JOBS.items() if n == name)
                text = self.script_override.get(key) or (v.GLUE_JOBS_DIR / f"{key}.py").read_text(encoding="utf-8")
                return v.Result(0, text)
            if op == "rm":
                prefix = cmd[3].split(f"s3://{BUCKET}/", 1)[1]
                gone = {k for k in self.objects if k.startswith(prefix)}
                self.objects -= gone
                self.removed += sorted(gone)
                return v.Result(0, "")
        if svc == "events":
            if op == "describe-rule":
                pattern = {"source": ["aws.glue"], "detail-type": ["Glue Job State Change"],
                           "detail": {"jobName": [{"prefix": "ta-job-"}], "state": ["FAILED", "TIMEOUT", "STOPPED", "ERROR"]}}
                return ok({"Name": v.FAIL_RULE, "State": "ENABLED", "EventPattern": json.dumps(pattern)})
            if op == "list-targets-by-rule":
                return ok({"Targets": [{"Id": "cloudwatch-logs", "Arn": f"arn:aws:logs:us-east-1:{ACCOUNT}:log-group:{v.FAIL_LOG_GROUP}"}]})
        if svc == "logs":
            if op == "describe-log-groups":
                return ok({"logGroups": [{"logGroupName": v.FAIL_LOG_GROUP, "retentionInDays": 30}]})
            if op == "describe-resource-policies":
                return ok({"resourcePolicies": [{"policyName": v.FAIL_LOG_POLICY}]})
            if op == "filter-log-events":
                group = arg(cmd, "--log-group-name")
                prefix = arg(cmd, "--log-stream-name-prefix") or ""
                needle = arg(cmd, "--filter-pattern").strip('"')
                events = [{"message": m} for s, m in self.logs.get(group, []) if s.startswith(prefix) and needle in m]
                return ok({"events": events})
        return v.Result(0, "{}")


class FakeRunner:
    def __init__(self, world, *, plans=None, pytest_out=None, gcp_on=False, tools=("terraform", "aws", "bash", "gcloud")):
        self.world = world
        self.plans = list(plans or [])
        self.pytest_out = pytest_out or {}
        self.gcp_on = gcp_on
        self.tools = set(tools)
        self.calls = []
        self.apply_text = None
        self.outputs = dict(OUTPUTS)

    def which(self, name):
        return f"/usr/bin/{name}" if name in self.tools else None

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        if cmd[0] == "aws":
            return self.world.handle(cmd)
        if cmd[0] == "terraform":
            sub = cmd[2] if cmd[1].startswith("-chdir") else cmd[1]
            if sub == "version":
                return ok({"terraform_version": "1.16.6"})
            if sub == "show":
                return v.Result(0, json.dumps(self.plans.pop(0) if self.plans else {"resource_changes": []}))
            if sub == "apply":
                return v.Result(0, self.apply_text or "Apply complete! Resources: 40 added, 0 changed, 0 destroyed.")
            if sub == "output":
                return ok({k: {"value": val} for k, val in self.outputs.items()})
            return v.Result(0, "ok")
        if "pytest" in cmd:
            target = next(c for c in cmd if c.startswith(("src/", "contracts/")))
            return v.Result(0, self.pytest_out.get(target, {"src/lakehouse": f"{v.B1_EXPECTED} passed in 1s",
                                                            "src/ops/tests/test_ops_validate41.py": f"{v.B2_EXPECTED} passed in 1s",
                                                            "contracts/tests": "18 passed in 1s"}.get(target, "30 passed in 1s")))
        if cmd[:4] == ["gcloud", "run", "services", "describe"]:
            return v.Result(0, "https://ta-stream-simulator.run.app" if self.gcp_on else "")
        if cmd[:2] == ["gcloud", "dataflow"]:
            return v.Result(0, "[]")
        return v.Result(0, "")

    def count(self, text):
        return sum(1 for c in self.calls if text in " ".join(c))


def plan(creates=(), updates=(), deletes=()):
    rcs = [{"address": a, "mode": "managed", "change": {"actions": ["create"]}} for a in creates]
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["update"], "before": {"x": 1}, "after": {"x": 2}}} for a in updates]
    rcs += [{"address": a, "mode": "managed", "change": {"actions": ["delete"]}} for a in deletes]
    rcs.append({"address": "data.aws_iam_policy_document.x", "mode": "data", "change": {"actions": ["read"]}})
    return {"resource_changes": rcs}


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    aws, ev = tmp_path / "aws", tmp_path / "evidencia"
    aws.mkdir()
    ev.mkdir()
    (aws / "backend.hcl").write_text('bucket = "x"\n')
    (aws / "terraform.tfvars").write_text(f'aws_account_id = "{ACCOUNT}"\n')
    (aws / "gcp_exporter.auto.tfvars").write_text('gcp_exporter_sa_unique_id = "1"\n')
    monkeypatch.setattr(v, "AWS_ENV", aws)
    monkeypatch.setattr(v, "EVIDENCE_DIR", ev)
    monkeypatch.setattr(v, "on_windows", lambda: False)
    return tmp_path


def clock():
    """Reloj que avanza con cada sleep (los bucles de espera terminan)."""
    state = {"t": NOW}

    def now():
        return state["t"]

    def sleep(s):
        state["t"] += timedelta(seconds=s)

    return now, sleep


def run_part(runner, *argv, sandbox_dir, confirm=None, s3_rows=None, moving=False):
    lines = []
    now, sleep = clock() if moving else ((lambda: NOW), (lambda s: None))
    code = v.main(list(argv), runner=runner, printer=lines.append, confirm=confirm, sleep=sleep, now=now,
                  evidence_path=sandbox_dir / "evidencia" / "validacion_4_1.json", s3_rows=s3_rows)
    evidence = json.loads((sandbox_dir / "evidencia" / "validacion_4_1.json").read_text())
    return code, lines, evidence


def steps(evidence, part):
    return {s["id"]: s for s in evidence["parts"][part]["steps"]}


def stream_rows(rows=None, errors=0):
    by = {s: {"objects": 385, "rows": n} for s, n in (rows or STREAM).items()}
    return lambda bucket, prefix: {"objects": 1155, "bytes": 51_865_466, "rows": sum(x["rows"] for x in by.values()),
                                   "by_source": by, "errors": [], "error_count": errors, "seconds": 3.2}


# --- utilidades puras ---------------------------------------------------------------------------


def test_expected_creates_son_40():
    addrs = v.expected_creates(cs.load_tables_json())
    assert len(addrs) == 40
    assert 'module.glue_catalog.aws_glue_catalog_table.this["ta_raw.posts_raw"]' in addrs
    assert "module.lakehouse_schedule.aws_scheduler_schedule.this" in addrs


def test_athena_rows_quita_el_encabezado():
    data = {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "a"}, {"Name": "b"}]},
                          "Rows": [{"Data": [{"VarCharValue": "a"}, {"VarCharValue": "b"}]}, {"Data": [{"VarCharValue": "1"}, {}]}]}}
    assert v.athena_rows(data) == (["a", "b"], [{"a": "1", "b": None}])
    assert v.athena_rows({}) == ([], [])


def test_parse_time_formatos():
    assert v.parse_time("2026-10-26T15:04:05.123000+00:00").second == 5
    assert v.parse_time("2026-10-26T15:04:05Z").tzinfo is not None
    assert v.parse_time(0).year == 1970 and v.parse_time(None) is None and v.parse_time("x") is None


def test_job_runs_by_name():
    run = {"Graph": {"Nodes": [{"Type": "TRIGGER", "Name": "t"}, {"Type": "JOB", "Name": "a", "JobDetails": {"JobRuns": [{"Id": "1"}]}},
                               {"Type": "JOB", "Name": "b"}]}}
    assert v.job_runs_by_name(run) == {"a": [{"Id": "1"}], "b": []}


def test_costo_con_minimo_de_un_minuto():
    assert v.billable_seconds(12) == 60 and v.billable_seconds(75) == 75 and v.billable_seconds(None) == 60
    assert v.job_cost(30) == pytest.approx(60 * 0.0625 * 0.44 / 3600)


def test_schedule_update_input_conserva_todo_menos_lo_de_solo_lectura():
    sched = World().schedule
    body = v.schedule_update_input(sched, "ENABLED")
    assert body["State"] == "ENABLED" and body["Target"] == sched["Target"] and "Arn" not in body
    assert body["FlexibleTimeWindow"] == {"Mode": "OFF"} and body["ScheduleExpressionTimezone"] == "UTC"


def test_at_expression_minuto_entero():
    expr, at = v.at_expression(datetime(2026, 10, 26, 15, 0, 50, tzinfo=timezone.utc))
    assert expr == "at(2026-10-26T15:03:00)" and at.second == 0
    assert (at - datetime(2026, 10, 26, 15, 0, 50, tzinfo=timezone.utc)).total_seconds() >= 90


def test_schedule_problems():
    sched = World().schedule
    assert v.schedule_problems(sched, ROLE_ARNS["scheduler"]) == []
    bad = json.loads(json.dumps(sched))
    bad["ScheduleExpression"] = "rate(5 minutes)"
    bad["Target"]["RetryPolicy"]["MaximumRetryAttempts"] = 185
    problems = v.schedule_problems(bad, ROLE_ARNS["scheduler"])
    assert any("expresión" in p for p in problems) and any("reintentos" in p for p in problems)


def test_trust_services_y_errores_de_aws():
    doc = {"Statement": [{"Principal": {"Service": ["a.amazonaws.com", "b.amazonaws.com"]}}]}
    assert v.trust_services(doc) == {"a.amazonaws.com", "b.amazonaws.com"}
    assert v.aws_error(not_found()) == "ResourceNotFoundException" and v.aws_error(v.Result(0, "")) is None


def test_strip_sql_comments():
    assert v.strip_sql_comments("-- a\r\nSELECT 1\n  -- b\nFROM t\n") == "SELECT 1\nFROM t"


def test_casos_simulados_permitidos_y_denegados():
    for role, total, allowed in (("glue", 20, 11), ("quicksight", 13, 7), ("scheduler", 3, 1)):
        cases = v.simulate_cases(role, OUTPUTS, ACCOUNT)
        assert len(cases) == total and sum(1 for c in cases if c[4]) == allowed


# --- A · preflight ---------------------------------------------------------------------------------


def test_preflight_sin_nada_encendido(sandbox):
    world = World()
    world.schedule = None
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "preflight", sandbox_dir=sandbox)
    s = steps(ev, "A")
    for sid in ("A2", "A3", "A4", "A5", "A6", "A7", "A8"):
        assert s[sid]["status"] == "OK", (sid, s[sid]["detail"])
    assert "NO_EXISTE" in s["A6"]["detail"]


def test_preflight_avisa_horario_encendido_y_simulador(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    runner = FakeRunner(world, gcp_on=True)
    (sandbox / "aws" / "gcp_exporter.auto.tfvars").unlink()
    _, _, ev = run_part(runner, "preflight", sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert s["A6"]["status"] == "AVISO" and "validate_4_1.py off" in s["A6"]["detail"]
    assert s["A7"]["status"] == "AVISO" and "ENCENDIDO" in s["A7"]["detail"]
    assert s["A8"]["status"] == "AVISO"


def test_preflight_falla_si_tables_json_no_esta_al_dia(sandbox, monkeypatch):
    monkeypatch.setattr(cs, "check", lambda *a, **k: ["ta_curated.dim_user distinta"])
    code, _, ev = run_part(FakeRunner(World()), "preflight", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "A")["A4"]["status"] == "FALLA"


# --- B · pruebas ------------------------------------------------------------------------------------


def test_tests_cuenta_lo_esperado(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "tests", sandbox_dir=sandbox)
    assert code == 0 and all(s["status"] == "OK" for s in steps(ev, "B").values())


def test_tests_falla_con_omitidas(sandbox):
    runner = FakeRunner(World(), pytest_out={"src/lakehouse": f"{v.B1_EXPECTED - 1} passed, 1 skipped in 1s"})
    code, _, ev = run_part(runner, "tests", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "B")["B1"]["status"] == "FALLA"


# --- C · Terraform ----------------------------------------------------------------------------------


def expected():
    return sorted(v.expected_creates(cs.load_tables_json()))


def test_terraform_sin_apply_planea_40_y_avisa(sandbox):
    runner = FakeRunner(World(), plans=[plan(expected())])
    code, _, ev = run_part(runner, "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C2"]["status"] == "OK" and "40 a crear, 0 a cambiar, 0 a destruir" in s["C2"]["detail"]
    assert s["C3"]["status"] == "AVISO" and s["C3b"]["status"] == "OMITIDO"
    assert runner.count(" apply ") == 0
    plan_cmd = next(c for c in runner.calls if c[0] == "terraform" and "plan" in c)
    assert {c.split("=", 1)[1] for c in plan_cmd if c.startswith("-target=")} == set(v.TERRAFORM_TARGETS)


def test_terraform_con_apply_aplica_y_replanea_sin_cambios(sandbox):
    runner = FakeRunner(World(), plans=[plan(expected()), plan()])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK" and s["C4"]["status"] == "OK"


def test_terraform_no_aplica_si_el_plan_cambia_otra_cosa(sandbox):
    runner = FakeRunner(World(), plans=[plan(expected(), updates=["module.lake_bucket.aws_s3_bucket.this"])])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 1 and s["C2"]["status"] == "FALLA" and "module.lake_bucket.aws_s3_bucket.this" in s["C2"]["detail"]
    assert "x: 1 → 2" in s["C2"]["detail"] and runner.count(" apply ") == 0


def test_terraform_deriva_tras_aplicar_es_falla(sandbox):
    table = 'module.glue_catalog.aws_glue_catalog_table.this["ta_raw.posts_raw"]'
    runner = FakeRunner(World(), plans=[plan(expected()), plan(updates=[table])])
    code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C3b"]["status"] == "FALLA"


def test_terraform_ya_aplicado_y_salida_del_exportador_vacia(sandbox):
    runner = FakeRunner(World(), plans=[plan(), plan()])
    runner.outputs.pop("exporter_role_arn")
    code, _, ev = run_part(runner, "terraform", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0 and "ya aplicado" in s["C2"]["detail"] and s["C3b"]["status"] == "OK"
    assert s["C4"]["status"] == "AVISO" and "exporter_role_arn" in s["C4"]["detail"]


def test_terraform_otra_cuenta_o_sin_backend_falla(sandbox):
    world = World()
    world.caller = "arn:aws:iam::111111111111:user/x"
    runner = FakeRunner(world)
    world.handle = (lambda orig: (lambda cmd: ok({"Account": "111111111111", "Arn": world.caller})
                                  if cmd[1:3] == ["sts", "get-caller-identity"] else orig(cmd)))(world.handle)
    code, _, ev = run_part(runner, "terraform", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C0"]["status"] == "FALLA"
    (sandbox / "aws" / "backend.hcl").unlink()
    code, _, ev = run_part(FakeRunner(World()), "terraform", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "C")["C0b"]["status"] == "FALLA"


# --- D · catálogo, workgroup e IAM --------------------------------------------------------------


def test_catalog_ok(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "catalog", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert "20 de 20" in s["D6"]["detail"] and "13 de 13" in s["D7"]["detail"] and "3 de 3" in s["D8"]["detail"]
    cases = s["D6"]["data"]["cases"]
    assert s["D6"]["data"]["role_arn"] == ROLE_ARNS["glue"] and len(cases) == 20 and all(c["ok"] for c in cases)


def test_catalog_logs_se_simulan_con_el_arn_del_grupo():
    """Regresión de la validación (v2.32): el simulador niega PutLogEvents sobre el ARN de un flujo."""
    logs = [c for c in v.simulate_cases("glue", OUTPUTS, ACCOUNT) if c[1] == "logs:PutLogEvents"]
    assert [(c[2].endswith(f"log-group:{v.GLUE_OUTPUT_LOG}"), c[4]) for c in logs] == [(True, True), (False, False)]
    assert all(":log-stream:" not in c[2] for c in logs)
    assert logs[1][2].endswith(f"log-group:{v.FAIL_LOG_GROUP}")


def test_catalog_tabla_distinta_falla(sandbox):
    world = World()
    spec = next(t for t in world.tables if t.full_name == "ta_curated.dim_user")
    broken = world.glue_table(spec)
    broken["StorageDescriptor"]["Columns"] = broken["StorageDescriptor"]["Columns"][:-1]
    world.table_override["ta_curated.dim_user"] = broken
    code, _, ev = run_part(FakeRunner(world), "catalog", sandbox_dir=sandbox)
    assert code == 1 and "ta_curated.dim_user: columnas" in steps(ev, "D")["D2"]["detail"]


def test_catalog_workgroup_con_otro_limite_falla(sandbox):
    world = World()
    world.cutoff = 10 * 2**30
    code, _, ev = run_part(FakeRunner(world), "catalog", sandbox_dir=sandbox)
    assert code == 1 and "límite" in steps(ev, "D")["D3"]["detail"]


def test_catalog_rol_con_permisos_de_mas_falla(sandbox):
    world = World()
    world.extra_glue_action = "glue:CreateTable"
    world.glue_allows_raw_put = True
    code, _, ev = run_part(FakeRunner(world), "catalog", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 1 and "glue:CreateTable" in s["D5"]["detail"] and "put raw/origin=stream/" in s["D6"]["detail"]
    assert f"s3:PutObject sobre arn:aws:s3:::{BUCKET}/raw/origin=stream/" in s["D6"]["detail"]
    bad = {c["case"]: c for c in s["D6"]["data"]["cases"] if not c["ok"]}
    assert set(bad) == {"put raw/origin=stream/", "delete raw/origin=historical/"}
    put = bad["put raw/origin=stream/"]
    assert put["decision"] == "allowed" and put["expected"] == "denied"
    assert put["resource"].startswith(f"arn:aws:s3:::{BUCKET}/raw/origin=stream/")


def test_catalog_roles_previos_cambiados_avisa(sandbox):
    world = World()
    world.previous_actions = {"s3:PutObject", "s3:GetObject"}
    code, _, ev = run_part(FakeRunner(world), "catalog", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "D")["D9"]["status"] == "AVISO"


# --- E · Athena ------------------------------------------------------------------------------------


def test_athena_dos_origenes_y_conciliacion(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "athena", sandbox_dir=sandbox, s3_rows=stream_rows())
    s = steps(ev, "E")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert "histórico 50000 · stream 1155" in s["E1"]["detail"]
    assert "45.0 MB" in s["E4"]["detail"] and s["E6"]["data"]["counts"]["ta_trusted.posts"] == 0
    sent = runner.world.queries["q1"]
    assert sent.startswith("SELECT") and "--" not in sent and v.WORKGROUP in " ".join(
        next(c for c in runner.calls if "start-query-execution" in c))


def test_athena_stream_no_concilia_falla(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "athena", sandbox_dir=sandbox,
                           s3_rows=stream_rows({"reddit": 400, "facebook": 381, "instagram": 375}))
    assert code == 1 and steps(ev, "E")["E3"]["status"] == "FALLA"


def test_athena_historico_distinto_falla(sandbox):
    world = World()
    world.hist_rows["reddit"] = 16000
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox, s3_rows=stream_rows())
    assert code == 1 and steps(ev, "E")["E2"]["status"] == "FALLA"


def test_athena_sobre_el_limite_falla(sandbox):
    world = World()
    world.scanned = v.DEFAULT_CUTOFF + 1
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox, s3_rows=stream_rows())
    assert code == 1 and steps(ev, "E")["E4"]["status"] == "FALLA"


def test_athena_consulta_fallida_corta(sandbox):
    world = World()
    world.query_state = "FAILED"
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox, s3_rows=stream_rows())
    s = steps(ev, "E")
    assert code == 1 and s["E1"]["status"] == "FALLA" and "E2" not in s


def test_athena_concilia_tambien_el_historico(sandbox):
    calls = []

    def rows(bucket, prefix):
        calls.append(prefix)
        if prefix.startswith("raw/origin=historical/"):
            by = {s: {"objects": 4034, "rows": n} for s, n in HIST.items()}
            return {"objects": 12102, "rows": 50000, "by_source": by, "errors": [], "error_count": 0, "seconds": 9.0}
        return stream_rows()(bucket, prefix)

    code, _, ev = run_part(FakeRunner(World()), "athena", "--reconcile-historical", sandbox_dir=sandbox, s3_rows=rows)
    assert code == 0 and calls == ["raw/origin=historical/", "raw/origin=stream/"]
    assert "12102 objetos" in steps(ev, "E")["E2"]["detail"]


# --- F · workflow ------------------------------------------------------------------------------------


def test_workflow_completo(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "workflow", "--yes", sandbox_dir=sandbox, moving=True)
    s = steps(ev, "F")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert "en orden" in s["F4"]["detail"] and "no corrió" in s["F6"]["detail"]
    assert "borrado solo" in s["F8"]["detail"] and s["F10"]["detail"].startswith("DISABLED")
    assert runner.world.created_temp[0]["Target"]["RoleArn"] == ROLE_ARNS["scheduler"]
    assert runner.world.created_temp[0]["ActionAfterCompletion"] == "DELETE"
    assert not any("_ta_placeholder" in k for k in runner.world.objects)
    assert ev["temp_schedules"][0]["cleaned"] is True
    assert s["F11"]["data"]["usd_per_run"] == pytest.approx(v.job_cost(70) + v.job_cost(40), abs=1e-6)


def test_workflow_sin_confirmar_no_corre(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "workflow", sandbox_dir=sandbox, confirm=lambda q: False)
    assert code == 0 and steps(ev, "F")["F4"]["status"] == "AVISO" and runner.count("start-workflow-run") == 0


def test_workflow_orden_incorrecto_falla(sandbox):
    world = World()
    world.order_ok = False
    code, _, ev = run_part(FakeRunner(world), "workflow", "--yes", "--no-tick", sandbox_dir=sandbox, moving=True)
    assert code == 1 and "ORDEN NO COMPROBADO" in steps(ev, "F")["F4"]["detail"]


def test_workflow_fallo_sin_evento_en_cloudwatch(sandbox):
    world = World()
    world.fail_event = False
    code, _, ev = run_part(FakeRunner(world), "workflow", "--yes", "--no-tick", sandbox_dir=sandbox, moving=True)
    s = steps(ev, "F")
    assert code == 1 and s["F7"]["status"] == "FALLA" and s["F8"]["status"] == "OMITIDO"


def test_workflow_horario_de_prueba_que_no_dispara_se_borra(sandbox):
    world = World()
    world.scheduler_runs = False
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "workflow", "--yes", sandbox_dir=sandbox, moving=True)
    assert code == 1 and steps(ev, "F")["F8"]["status"] == "FALLA"
    assert world.temp == {} and runner.count("delete-schedule") >= 1


def test_workflow_script_distinto_al_repo_falla(sandbox):
    world = World()
    world.script_override["raw_to_trusted"] = "print('otro')\n"
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "workflow", "--yes", sandbox_dir=sandbox)
    assert code == 1 and "no es src/lakehouse/glue_jobs/raw_to_trusted.py" in steps(ev, "F")["F1"]["detail"]
    assert runner.count("start-workflow-run") == 0


def test_workflow_reintenta_si_glue_no_libero_el_cupo(sandbox):
    """Regresión de la validación (v2.32): F6 justo después de F4 recibió ConcurrentRunsExceededException."""
    world = World()
    world.busy_after_run = 2
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "workflow", "--yes", sandbox_dir=sandbox, moving=True)
    s = steps(ev, "F")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert s["F6"]["status"] == "OK" and "arrancó al intento 3" in s["F6"]["detail"] and s["F6"]["data"]["start_attempts"] == 3
    assert s["F7"]["status"] == "OK" and s["F8"]["status"] == "OK"
    assert runner.count("start-workflow-run") == 4  # F4 + F6 (dos rechazos y el bueno)


def test_workflow_sin_cupo_f6_falla_y_f7_queda_omitido(sandbox):
    world = World()
    world.busy_after_run = 99
    code, _, ev = run_part(FakeRunner(world), "workflow", "--yes", "--no-tick", sandbox_dir=sandbox, moving=True)
    s = steps(ev, "F")
    assert code == 1 and s["F6"]["status"] == "FALLA"
    assert f"ConcurrentRunsExceededException tras {v.CONCURRENT_TRIES} intentos" in s["F6"]["detail"]
    assert s["F7"]["status"] == "OMITIDO" and "F6 no pudo arrancar" in s["F7"]["detail"]


def test_start_run_espera_a_que_no_haya_corridas_en_curso(sandbox):
    world = World()
    world.running_polls = 2
    sleeps = []
    ctx = v.Ctx(runner=FakeRunner(world), options=v.build_parser().parse_args(["workflow"]), printer=lambda s: None,
                sleep=sleeps.append, now=lambda: NOW, evidence_path=sandbox / "evidencia" / "x.json")
    run_id, err, tries = v.start_run(ctx, {"ta_validacion": "4.1"})
    assert run_id and err == "" and tries == 1 and sleeps == [15, 15]
    world.running_polls = 10**6
    run_id, err, tries = v.start_run(ctx, {})
    assert run_id is None and "wr_ajena" in err and tries == 0


# --- status, off y on -----------------------------------------------------------------------------


def test_status_muestra_lo_encendido(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    world.runs = [{"WorkflowRunId": "wr_9", "Status": "RUNNING", "StartedOn": iso(NOW)}]
    world.temp["ta-wf-raw-to-curated-validacion-1"] = {}
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert s["S1"]["status"] == "AVISO" and "wr_9" in s["S2"]["detail"] and s["S4"]["status"] == "AVISO"


def test_off_deshabilita_detiene_y_limpia(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    world.runs = [{"WorkflowRunId": "wr_9", "Status": "RUNNING", "StartedOn": iso(NOW)}]
    world.job_runs[v.JOBS["raw_to_trusted"]] = [{"Id": "jr_1", "JobRunState": "RUNNING"}]
    world.temp["ta-wf-raw-to-curated-validacion-1"] = {}
    world.objects.add("trusted/_ta_placeholder/ta-job-raw-to-trusted/wr_9.json")
    code, _, ev = run_part(FakeRunner(world), "off", sandbox_dir=sandbox)
    s = steps(ev, "off")
    assert code == 0, {k: (x["status"], x["detail"]) for k, x in s.items()}
    assert world.schedule["State"] == "DISABLED" and world.updates[0]["Target"]["RetryPolicy"]["MaximumRetryAttempts"] == 0
    assert ("stop-workflow-run", "wr_9") in world.stopped and world.temp == {}
    assert "1 borradas" in s["O4"]["detail"]


def test_off_ya_apagado_no_cambia_nada(sandbox):
    world = World()
    code, _, ev = run_part(FakeRunner(world), "off", sandbox_dir=sandbox)
    assert code == 0 and world.updates == [] and "ya" not in steps(ev, "off")["O1"]["detail"]


def test_on_pide_confirmacion_y_enciende(sandbox):
    world = World()
    code, _, ev = run_part(FakeRunner(world), "on", sandbox_dir=sandbox, confirm=lambda q: False)
    assert code == 0 and world.schedule["State"] == "DISABLED" and steps(ev, "on")["N1"]["status"] == "AVISO"
    code, _, ev = run_part(FakeRunner(world), "on", "--yes", sandbox_dir=sandbox)
    assert code == 0 and world.schedule["State"] == "ENABLED" and "DISABLED → ENABLED" in steps(ev, "on")["N1"]["detail"]


def test_main_fuera_de_wsl_sale_con_2(sandbox, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    assert v.main(["status"], runner=FakeRunner(World()), printer=lines.append) == 2
    assert "WSL" in lines[-1] or "Windows" in lines[-1]
