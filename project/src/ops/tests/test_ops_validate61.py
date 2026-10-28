"""Pruebas del validador del Lote 6.1 con aws, terraform, Athena y QuickSight falsos (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ops import validate61 as v
from serving import quicksight_spec as qs

ACC = "111122223333"
NOW = datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc)


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err)


def rs(cols, rows):
    """Respuesta de ``athena get-query-results``: ``cols`` = [(nombre, tipo)], ``rows`` = listas de texto."""
    head = [{"VarCharValue": c} for c, _ in cols]
    return {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": c, "Type": t} for c, t in cols]},
                          "Rows": [{"Data": head}] + [{"Data": [{"VarCharValue": str(x)} for x in r]} for r in rows]}}


QS_TYPES = {"STRING": "varchar", "INTEGER": "bigint", "DECIMAL": "double", "DATETIME": "timestamp"}


class Fake:
    def __init__(self, *, subscribed=True, rows=7, extra_perm=False, leak=False, plan_creates=4, roles=True, pro=False):
        self.subscribed, self.rows, self.extra_perm, self.leak = subscribed, rows, extra_perm, leak
        self.plan_creates, self.roles, self.pro = plan_creates, roles, pro
        self.queries: dict[str, str] = {}
        self.calls: list[list[str]] = []
        self.applied = False

    def which(self, name):
        return f"/usr/bin/{name}"

    # --- QuickSight ---------------------------------------------------------------------------------------
    def users(self):
        admin = {"UserName": ACC, "Role": "ADMIN_PRO" if self.pro else "ADMIN", "Arn": qs.principal_arn(ACC, "user", ACC)}
        return [admin, {"UserName": "ta-quicksight-lector/demo", "Role": "READER", "Arn": qs.principal_arn(ACC, "user", "ta-quicksight-lector/demo")},
                {"UserName": "ta-quicksight-autor/demo", "Role": "AUTHOR", "Arn": qs.principal_arn(ACC, "user", "ta-quicksight-autor/demo")}]

    def quicksight(self, cmd):
        op = cmd[2]
        arg = lambda k: cmd[cmd.index(k) + 1]  # noqa: E731
        if op == "describe-account-subscription":
            if not self.subscribed:
                return R(254, "", "An error occurred (ResourceNotFoundException) when calling: The account does not exist.")
            return R(0, json.dumps({"AccountInfo": {"AccountSubscriptionStatus": "ACCOUNT_CREATED", "Edition": "ENTERPRISE",
                                                    "AuthenticationType": "IDENTITY_POOL"}}))
        if op == "list-ingestions":
            return R(0, json.dumps({"Ingestions": [{"IngestionStatus": "COMPLETED", "RowInfo": {"RowsIngested": self.rows},
                                                    "CreatedTime": "2026-10-28T11:00:00Z"}]}))
        if op == "describe-dashboard":
            return R(0, json.dumps({"Dashboard": {"Version": {"VersionNumber": 3, "Status": "CREATION_SUCCESSFUL", "Errors": []}}}))
        if op == "describe-dashboard-definition":
            arns = {d.key: f"arn:aws:quicksight:us-east-1:{ACC}:dataset/{d.dataset_id}" for d in qs.DATASETS}
            return R(0, json.dumps({"Definition": qs.definition(arns)}))
        if op == "describe-data-set":
            d = next(x for x in qs.DATASETS if x.dataset_id == arg("--data-set-id"))
            cols = d.column_names + (["user_hash"] if self.leak and d.key == "marcas" else [])
            return R(0, json.dumps({"DataSet": {"ImportMode": "SPICE", "Arn": f"arn:aws:quicksight:us-east-1:{ACC}:dataset/{d.dataset_id}",
                                                "OutputColumns": [{"Name": c} for c in cols]}}))
        if op == "describe-data-source":
            return R(0, json.dumps({"DataSource": {"DataSourceParameters": {"AthenaParameters": {
                "WorkGroup": "ta-analytics", "RoleArn": f"arn:aws:iam::{ACC}:role/ta-quicksight-role"}}}}))
        if op == "list-users":
            return R(0, json.dumps({"UserList": self.users()}))
        if op == "describe-dashboard-permissions":
            perms = [{"Principal": qs.principal_arn(ACC, "group", "ta-autores"), "Actions": qs.DASHBOARD_OWNER},
                     {"Principal": qs.principal_arn(ACC, "user", ACC), "Actions": qs.DASHBOARD_OWNER},
                     {"Principal": qs.principal_arn(ACC, "group", "ta-lectores"),
                      "Actions": qs.DASHBOARD_VIEWER + (["quicksight:UpdateDashboard"] if self.extra_perm else [])}]
            return R(0, json.dumps({"Permissions": perms}))
        if op == "list-group-memberships":
            g = arg("--group-name")
            members = {"ta-autores": [ACC, "ta-quicksight-autor/demo"], "ta-lectores": ["ta-quicksight-lector/demo"]}[g]
            return R(0, json.dumps({"GroupMemberList": [{"MemberName": m} for m in members]}))
        if op == "generate-embed-url-for-registered-user":
            return R(0, json.dumps({"EmbedUrl": "https://x/embed?code=secreto"}))
        if op == "list-refresh-schedules":
            return R(0, json.dumps({"RefreshSchedules": [{"ScheduleId": "ta-cada-hora", "ScheduleFrequency": {"Interval": "HOURLY"}}]}))
        if op in ("update-account-settings", "delete-account-subscription"):
            return R(0, json.dumps({"Status": 200}))
        raise AssertionError(cmd)

    # --- Athena ---------------------------------------------------------------------------------------------
    def athena(self, cmd):
        op = cmd[2]
        if op == "start-query-execution":
            qid = f"q{len(self.queries)}"
            self.queries[qid] = cmd[cmd.index("--query-string") + 1]
            return R(0, json.dumps({"QueryExecutionId": qid}))
        qid = cmd[cmd.index("--query-execution-id") + 1]
        sql = self.queries[qid]
        if op == "get-query-execution":
            return R(0, json.dumps({"QueryExecution": {"Status": {"State": "SUCCEEDED"},
                                                       "Statistics": {"TotalExecutionTimeInMillis": 900, "DataScannedInBytes": 12_000_000}}}))
        if "count_if(date_diff" in sql:
            return R(0, json.dumps(rs([("total", "bigint"), ("malas", "bigint"), ("dias_malos", "bigint")], [[221630, 0, 0]])))
        if sql.startswith("SELECT count(*) AS n"):
            return R(0, json.dumps(rs([("n", "bigint")], [[7]])))
        if sql.endswith("LIMIT 1"):
            d = next(x for x in qs.DATASETS if v.v41.strip_sql_comments(x.sql).strip() in sql)
            return R(0, json.dumps(rs([(c.name, QS_TYPES[c.type]) for c in d.columns], [])))
        return R(0, json.dumps(rs([("x", "varchar")], [["1"]])))

    # --- IAM ------------------------------------------------------------------------------------------------
    def iam(self, cmd):
        op = cmd[2]
        role = cmd[cmd.index("--role-name") + 1] if "--role-name" in cmd else None
        if op == "get-role":
            if role in v.SELF_REGISTER and not self.roles:
                return R(254, "", "An error occurred (NoSuchEntity) when calling GetRole")
            trust = {"Statement": [{"Principal": {"AWS": f"arn:aws:iam::{ACC}:root"}, "Action": "sts:AssumeRole"}]}
            return R(0, json.dumps({"Role": {"RoleName": role, "AssumeRolePolicyDocument": trust}}))
        if op == "list-role-policies":
            return R(0, json.dumps({"PolicyNames": [f"{role}-quicksight"]}))
        if op == "get-role-policy":
            return R(0, json.dumps({"PolicyDocument": {"Statement": [{"Action": v.SELF_REGISTER[role]}]}}))
        if op == "list-attached-role-policies":
            return R(0, json.dumps({"AttachedPolicies": []}))
        if op == "simulate-principal-policy":
            names = cmd[cmd.index("--action-names") + 1: cmd.index("--output")]
            return R(0, json.dumps({"EvaluationResults": [{"EvalActionName": n, "EvalDecision": "implicitDeny"} for n in names]}))
        raise AssertionError(cmd)

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        text = " ".join(cmd)
        if cmd[1:3] == ["-m", "pytest"]:
            target = next((c for c in cmd if c.startswith("src/")), "")
            counts = {"src/serving": v.B1_EXPECTED, "src/ops/tests/test_ops_validate61.py": v.B2_EXPECTED,
                      "src/lakehouse": v.B3_EXPECTED}
            return R(0, f"{counts.get(target, 1)} passed in 1.0s")
        if "quicksight.py plan" in text:
            return R(0, json.dumps({"event": "plan", **qs.summary()}))
        if "quicksight.py deploy" in text:
            evs = [{"event": "subscription", "status": "ACCOUNT_CREATED", "edition": "ENTERPRISE"},
                   {"event": "no_pro_users", "changed": [{"user": ACC, "from": "ADMIN_PRO", "to": "ADMIN"}]},
                   {"event": "access_users", "users": [
                       {"user": "ta-quicksight-autor/demo", "role": "AUTHOR", "group": "ta-autores"},
                       {"user": "ta-quicksight-lector/demo", "role": "READER", "group": "ta-lectores"}]},
                   {"event": "data_source", "action": "updated", "status": "UPDATE_SUCCESSFUL"}]
            evs += [{"event": "dataset", "id": d.dataset_id, "action": "updated"} for d in qs.DATASETS]
            evs += [{"event": "ingestion", "dataset": d.dataset_id, "status": "COMPLETED", "rows": 7, "dropped": 0, "seconds": 40}
                    for d in qs.DATASETS]
            evs += [{"event": "refresh_schedules", "schedule": "ta-cada-hora", "frequency": {"Interval": "HOURLY"},
                     "datasets": [d.dataset_id for d in qs.DATASETS]},
                    {"event": "analysis", "action": "updated", "status": "UPDATE_SUCCESSFUL", "errors": [], "sheets": ["Tendencias", "Relaciones"]},
                    {"event": "dashboard", "action": "updated", "version": 3, "status": "CREATION_SUCCESSFUL", "errors": [],
                     "sheets": ["Tendencias", "Relaciones"]},
                    {"event": "deploy_done", "ok": True}]
            return R(0, "\n".join(json.dumps(e) for e in evs))
        if cmd[0] == "aws":
            if cmd[1] == "sts":
                return R(0, json.dumps({"Account": ACC, "Arn": f"arn:aws:iam::{ACC}:root"}))
            if cmd[1] == "quicksight":
                return self.quicksight(cmd)
            if cmd[1] == "athena":
                return self.athena(cmd)
            if cmd[1] == "iam":
                return self.iam(cmd)
            if cmd[1:3] == ["scheduler", "get-schedule"]:
                return R(0, json.dumps({"State": "ENABLED"}))
        if "infra/validate.sh" in text:
            return R(0, "OK")
        if cmd[0] == "terraform":
            if "init" in cmd:
                return R(0, "Terraform has been successfully initialized!")
            if "plan" in cmd:
                return R(0, "Plan: ...")
            if "show" in cmd:
                n = 0 if self.applied else self.plan_creates
                addrs = [f.format(r) for r in v.SELF_REGISTER for f in (v.ROLE_ADDR, v.POLICY_ADDR)][:n]
                return R(0, json.dumps({"resource_changes": [{"address": a, "change": {"actions": ["create"]}} for a in addrs]}))
            if "apply" in cmd:
                self.applied = True
                return R(0, "Apply complete! Resources: 4 added, 0 changed, 0 destroyed.")
        raise AssertionError(text)


def run(part, fake, tmp_path, *extra, monkeypatch=None):
    lines: list[str] = []
    ev = tmp_path / "validacion_6_1.json"
    rc = v.main([part, "--yes", *extra], runner=fake, printer=lines.append, sleep=lambda s: None, now=lambda: NOW,
                evidence_path=ev)
    data = json.loads(ev.read_text(encoding="utf-8")) if ev.exists() else {}
    key = {"preflight": "A", "tests": "B", "local": "L", "terraform": "C", "deploy": "D", "checks": "Q",
           "latency": "E", "status": "status", "unsubscribe": "U"}[part]
    steps = {s["id"]: s for s in data.get("parts", {}).get(key, {}).get("steps", [])}
    return rc, steps, "\n".join(lines)


def no_windows(monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)


# --- utilidades puras ---------------------------------------------------------------------------------


def test_prueba_gratis_costo_y_reduccion():
    assert v.trial_end() == datetime(2026, 11, 26, 18, 46, tzinfo=timezone.utc)
    assert v.monthly_cost([{"Role": "ADMIN"}, {"Role": "AUTHOR"}, {"Role": "READER"}]) == 51
    red = v.report_reduction()
    assert red["manual_min"] == 95 and red["tablero_min"] == 25 and red["reduction_pct"] >= 50


def test_parse_samples():
    assert v.parse_samples("restablecer=2.10, hoja=1.65;malo=x;sin_igual") == [("restablecer", 2.1), ("hoja", 1.65)]
    assert v.parse_samples(None) == []


def test_permission_problems():
    admin = qs.principal_arn(ACC, "user", ACC)
    good = [{"Principal": qs.principal_arn(ACC, "group", "ta-autores"), "Actions": qs.DASHBOARD_OWNER},
            {"Principal": admin, "Actions": qs.DASHBOARD_OWNER},
            {"Principal": qs.principal_arn(ACC, "group", "ta-lectores"), "Actions": qs.DASHBOARD_VIEWER}]
    assert v.permission_problems(good, ACC, [admin]) == []
    bad = good[:2] + [{"Principal": qs.principal_arn(ACC, "group", "ta-lectores"),
                       "Actions": qs.DASHBOARD_VIEWER + ["quicksight:DeleteDashboard"]},
                      {"Principal": qs.principal_arn(ACC, "user", "otro"), "Actions": qs.DASHBOARD_VIEWER}]
    out = " | ".join(v.permission_problems(bad, ACC, [admin]))
    assert "sobran ['quicksight:DeleteDashboard']" in out and "principal inesperado" in out
    assert "falta ta-lectores" in " ".join(v.permission_problems(good[:2], ACC, [admin]))


def test_role_problems():
    trust = {"Statement": [{"Principal": {"AWS": f"arn:aws:iam::{ACC}:root"}}]}
    assert v.role_problems({"AssumeRolePolicyDocument": trust}, {"p": {"Statement": [{"Action": "quicksight:CreateReader"}]}},
                           ACC, "quicksight:CreateReader") == []
    wide = {"Statement": [{"Principal": {"AWS": "*"}}]}
    out = v.role_problems({"AssumeRolePolicyDocument": wide},
                          {"p": {"Statement": [{"Action": ["quicksight:CreateReader", "s3:GetObject"]}]}}, ACC, "quicksight:CreateReader")
    assert len(out) == 2


# --- partes ----------------------------------------------------------------------------------------------


def test_preflight(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, _ = run("preflight", Fake(), tmp_path)
    assert rc == 0 and steps["A4"]["status"] == "OK" and steps["A6"]["status"] == "OK"
    assert steps["A4"]["data"]["trial_end"].startswith("2026-11-26")
    rc, steps, _ = run("preflight", Fake(subscribed=False), tmp_path)
    assert rc == 1 and steps["A4"]["status"] == "FALLA" and "a mano" in steps["A4"]["detail"]


def test_tests_y_local(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, _ = run("tests", Fake(), tmp_path)
    assert rc == 0 and {s["status"] for s in steps.values()} == {"OK"}
    rc, steps, _ = run("local", Fake(), tmp_path)
    assert rc == 0 and [s["status"] for s in steps.values()] == ["OK"] * 4
    assert "Tendencias: 11 visuales" in steps["L3"]["detail"] and "Relaciones: 5 visuales" in steps["L3"]["detail"]


def test_terraform_sin_apply_avisa_y_con_apply_aplica(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    monkeypatch.setattr(v, "EVIDENCE_DIR", tmp_path)
    monkeypatch.setattr(v, "tfvars_value", lambda path, key: ACC)
    rc, steps, _ = run("terraform", Fake(), tmp_path)
    assert rc == 0 and steps["C2"]["status"] == "OK" and steps["C3"]["status"] == "AVISO"
    fake = Fake()
    rc, steps, _ = run("terraform", fake, tmp_path, "--apply")
    assert rc == 0 and steps["C3"]["status"] == "OK" and steps["C4"]["status"] == "OK" and fake.applied
    rc, steps, _ = run("terraform", Fake(plan_creates=0), tmp_path)
    assert "(ya aplicado)" in steps["C2"]["detail"] and "C3" not in steps


def test_deploy(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, _ = run("deploy", Fake(), tmp_path)
    assert rc == 0 and all(s["status"] == "OK" for s in steps.values()), {k: s["detail"] for k, s in steps.items()}
    assert "ADMIN_PRO" in steps["D2"]["detail"] and steps["D9"]["data"]["version"] == 3


def test_checks_ok(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, out = run("checks", Fake(), tmp_path)
    assert rc == 0, out
    assert all(s["status"] == "OK" for s in steps.values()), {k: s["detail"] for k, s in steps.items() if s["status"] != "OK"}
    assert "221630 filas · 0 con otra diferencia" in steps["Q3"]["detail"]
    assert steps["Q7"]["data"]["monthly_usd_after_trial"] == 51
    assert "code=" not in (tmp_path / "validacion_6_1.json").read_text(encoding="utf-8")


def test_checks_detecta_problemas(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, _ = run("checks", Fake(rows=6, extra_perm=True, leak=True, roles=False, pro=True), tmp_path)
    assert rc == 1
    assert steps["Q1"]["status"] == "AVISO" and "SPICE 6 · Athena 7" in steps["Q1"]["detail"]
    assert steps["Q5"]["status"] == "FALLA" and "user_hash" in steps["Q5"]["detail"]
    assert steps["Q6"]["status"] == "FALLA" and "UpdateDashboard" in steps["Q6"]["detail"]
    assert steps["Q7"]["status"] == "FALLA" and steps["Q8"]["status"] == "FALLA"


def test_latency(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    monkeypatch.setattr(v, "EVIDENCE", tmp_path / "validacion_6_1.json")
    rc, steps, _ = run("latency", Fake(), tmp_path, "--samples", "restablecer=2.10,hoja_relaciones=1.65")
    assert rc == 0 and steps["E1"]["data"]["worst_s"] == 2.1 and steps["E3"]["status"] == "OK"
    assert len(steps["E2"]["data"]["athena"]) == 4
    rc, steps, _ = run("latency", Fake(), tmp_path)  # sin --samples conserva las anteriores
    assert steps["E1"]["status"] == "OK" and steps["E1"]["data"]["samples"]["restablecer"] == 2.1
    rc, steps, _ = run("latency", Fake(), tmp_path, "--samples", "filtro=3.4")
    assert rc == 1 and steps["E1"]["status"] == "FALLA"


def test_status_y_unsubscribe(tmp_path, monkeypatch):
    no_windows(monkeypatch)
    rc, steps, _ = run("status", Fake(), tmp_path)
    assert rc == 0 and "2026-11-26" in steps["S1"]["detail"] and "51 USD/mes" in steps["S2"]["detail"]
    fake = Fake()
    rc, steps, _ = run("unsubscribe", fake, tmp_path)
    assert steps["U1"]["status"] == "OMITIDO" and not any("delete-account-subscription" in c for c in fake.calls)
    rc, steps, _ = run("unsubscribe", fake, tmp_path, "--confirm-unsubscribe")
    assert steps["U1"]["status"] == "OK" and any("delete-account-subscription" in c for c in fake.calls)
