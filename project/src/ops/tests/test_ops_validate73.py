"""Pruebas del validador del Lote 7.3 con el Terraform y los scripts reales y la nube falsa (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ops import validate73 as v

NOW = datetime(2026, 10, 29, 6, 0, tzinfo=timezone.utc)


def test_dataflow_runs_one_worker():
    workers = v.dataflow_workers((v.PROJECT / "scripts" / "dataflow_job.sh").read_text(encoding="utf-8"))
    assert workers == {"num_workers": "1", "max_num_workers": "1", "autoscaling_algorithm": "NONE"}
    assert v.dataflow_workers("--num_workers 1 --max_num_workers=2") == {"num_workers": "1", "max_num_workers": "2"}


def test_every_aws_module_has_a_component_provider():
    tf = "\n".join(p.read_text(encoding="utf-8") for p in sorted(v.AWS_ENV.glob("*.tf")))
    providers = v.module_providers(tf)
    assert providers and all(providers.values()), providers
    assert {"lake", "historical", "exporter", "lakehouse", "serving", "cicd"} <= set(providers.values()) <= v.COMPONENTS_AWS | {"cicd"}
    assert v.module_providers('module "x" {\n  source = "y"\n}') == {"x": None}


def test_gcp_labels_carry_a_component():
    tf = "\n".join(p.read_text(encoding="utf-8") for p in sorted(v.GCP_ENV.glob("*.tf")))
    loose = [ln for ln in v.gcp_label_blocks(tf) if "local.platform_labels" in ln]
    assert len(loose) == 1  # Pub/Sub: su módulo pone component = ingest
    assert v.gcp_label_blocks('  labels = merge(local.platform_labels, { component = "x" })\n  labels = local.labels') == ["labels = local.labels"]


def test_lifecycle_parsers():
    gcs = {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["raw/"]}},
                    {"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["dlq/"]}},
                    {"action": {"type": "SetStorageClass"}, "condition": {"age": 1}}]}
    assert v.gcs_rules(gcs) == {"raw/": 30, "dlq/": 30} and v.gcs_rules(None) == {}
    s3 = {"Rules": [{"Status": "Enabled", "Filter": {"Prefix": "athena-results/"}, "Expiration": {"Days": 7}},
                    {"Status": "Disabled", "Filter": {"Prefix": "x/"}, "Expiration": {"Days": 1}},
                    {"Status": "Enabled", "Filter": {}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]}
    assert v.s3_rules(s3) == {"athena-results/": 7}


def test_tags_and_policies():
    res = [{"Tags": [{"Key": "component", "Value": "lake"}]}, {"Tags": [{"Key": "project", "Value": "trend-analyzer"}]},
           {"Tags": [{"Key": "component", "Value": "lake"}]},
           {"ResourceARN": "arn:aws:dynamodb:us-east-1:1:table/ta-dev-tflock", "Tags": [{"Key": "project", "Value": "x"}]}]
    assert v.tag_components(res) == {"lake": 2, "(sin component)": 1, "(bootstrap)": 1}
    assert v.policy_problems({"enabled": True, "notificationChannels": ["a", "b"]}, 2) == []
    assert v.policy_problems({"enabled": False, "notificationChannels": ["a"]}, 2) == ["deshabilitada", "1 canales (esperado ≥ 2)"]


class Fake:
    def __init__(self, *, policies=True, lifecycle_days=30):
        self.policies, self.days, self.calls = policies, lifecycle_days, []

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        from ops.validate32 import Result

        self.calls.append(cmd)
        text = " ".join(cmd)
        if cmd[:2] == ["bash", "project/infra/validate.sh"]:
            return Result(0, "OK")
        if "billing projects describe" in text:
            return Result(0, "billingAccounts/0X0X0X-111111-222222\n")
        if "billing budgets list" in text:
            return Result(0, json.dumps([{"displayName": "ta-dev-budget", "thresholdRules": [{"thresholdPercent": 0.5}, {"thresholdPercent": 1.0}]}]))
        if "budgets describe-budgets" in text:
            return Result(0, json.dumps({"Budgets": [{"BudgetName": "ta-dev-monthly", "BudgetLimit": {"Amount": "50.0", "Unit": "USD"}}]}))
        if "storage buckets describe" in text:
            return Result(0, json.dumps({"labels": {"component": "lake"}, "lifecycle_config": {"rule": [
                {"action": {"type": "Delete"}, "condition": {"age": self.days, "matchesPrefix": [p]}} for p in ("raw/", "dlq/")]}}))
        if cmd[0] == "bq":
            return Result(0, json.dumps({"timePartitioning": {"expirationMs": str(90 * 86_400_000)}}))
        if "get-bucket-lifecycle-configuration" in text:
            return Result(0, json.dumps({"Rules": [{"Status": "Enabled", "Filter": {"Prefix": "athena-results/"}, "Expiration": {"Days": 7}}]}))
        if "get-resources" in text:
            return Result(0, json.dumps({"ResourceTagMappingList": [{"Tags": [{"Key": "component", "Value": c}]} for c in v.COMPONENTS_AWS]}))
        if "artifacts repositories describe" in text:
            return Result(0, json.dumps({"labels": {"component": "images"}}))
        if "pubsub topics describe" in text:
            return Result(0, json.dumps({"labels": {"component": "ingest"}}))
        if "monitoring policies list" in text:
            pols = [{"displayName": n, "enabled": True, "notificationChannels": ["c1", "c2"]} for n in v.POLICIES] if self.policies else []
            return Result(0, json.dumps(pols))
        if cmd[0] == "terraform" and "output" in cmd:
            return Result(0, json.dumps({"alert_email_channels": {"value": 1}}))
        if "dataflow jobs list" in text:
            return Result(0, "[]")
        if "run services describe" in text:
            return Result(1, "", "NOT_FOUND")
        return Result(1, "", f"inesperado: {text}")


def _run(part, tmp_path, monkeypatch, fake):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    lines = []
    code = v.main([part, "--project", "proj-x", "--yes"], runner=fake, printer=lines.append, sleep=lambda s: None,
                  now=lambda: NOW, evidence_path=tmp_path / "ev.json")
    ev = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    return code, {s["id"]: s for p in ev["parts"].values() for s in p["steps"]}


def test_local_with_the_real_files(tmp_path, monkeypatch):
    code, steps = _run("local", tmp_path, monkeypatch, Fake())
    assert code == 0, {k: s["detail"] for k, s in steps.items() if s["status"] != "OK"}
    assert steps["L5"]["data"]["gcs_raw/"] == 30 and steps["L5"]["data"]["bigquery_posts_enriched"] == 90


def test_cloud_ok_and_failures(tmp_path, monkeypatch):
    code, steps = _run("cloud", tmp_path, monkeypatch, Fake())
    assert code == 0, {k: s["detail"] for k, s in steps.items() if s["status"] != "OK"}
    assert steps["K1"]["data"]["thresholds"] == [0.5, 1.0]
    code, steps = _run("cloud", tmp_path, monkeypatch, Fake(policies=False, lifecycle_days=60))
    assert code == 1 and steps["K3"]["status"] == "FALLA" and steps["K6"]["status"] == "FALLA"


def test_apply_saved_pushes_the_errored_state(tmp_path, monkeypatch):
    from ops.validate32 import Result

    calls: list = []

    class Ctx:
        def printer(self, text):
            calls.append(text)

        def tf(self, root, *args, timeout=900):
            calls.append(args[0])
            if args[0] == "apply":
                (root / "errored.tfstate").write_text("{}")
                return Result(1, "", "Error: Failed to save state\n\nError: Failed to persist state to backend")
            return Result(0, "")

    monkeypatch.setattr(v, "GCP_ENV", tmp_path)
    monkeypatch.setattr(v, "ERRORED_DIR", tmp_path / "fuera")
    assert v.apply_saved(Ctx(), "test").ok
    assert calls[:2] == ["apply", "state"] and "Failed to save state" in calls[2]
    assert not (tmp_path / "errored.tfstate").exists() and len(list((tmp_path / "fuera").iterdir())) == 1
    calls.clear()

    class Plain(Ctx):
        def tf(self, root, *args, timeout=900):
            calls.append(args[0])
            return Result(1, "", "Error: 409")

    assert not v.apply_saved(Plain(), "test").ok and calls == ["apply"]


def test_thresholds_read_from_the_cloud():
    def policy(**cond):
        return {"conditions": [{"conditionThreshold": cond}]}

    got = v.thresholds({v.POLICIES[0]: policy(thresholdValue=360), v.POLICIES[1]: policy(duration="28800s")})
    assert got == {v.POLICIES[0]: v.LOW_SECONDS, v.POLICIES[1]: 8 * 3600.0}
    assert v.thresholds({v.POLICIES[1]: policy()}) == {v.POLICIES[1]: 0.0}
