"""Pruebas del validador del Lote 7.1 con los workflows reales y GitHub, Docker y Terraform falsos (sin red)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import yaml

from ops import validate71 as v

NOW = datetime(2026, 10, 29, 3, 0, tzinfo=timezone.utc)


def test_the_real_ci_workflow_meets_hu041():
    path = v.REPO / ".github/workflows/ci.yml"
    wf = v.load_workflow(path)
    assert v.ci_problems(wf, path.read_text(encoding="utf-8")) == []
    assert set(wf["jobs"]) == {"pytest", "terraform", "images", "secrets"}
    assert wf["jobs"]["images"]["strategy"]["matrix"]["image"] == list(v.IMAGES)


def test_ci_problems_catch_missing_triggers_steps_and_secrets():
    wf = {"on": {"push": {"branches": ["dev"]}}, "permissions": {"contents": "read", "id-token": "write"}}
    problems = v.ci_problems(wf, "run: echo ${{ secrets.X }}")
    assert "no corre en pull_request" in problems and "no corre en push a main" in problems
    assert any(p.startswith("permisos") for p in problems) and "la CI no debe usar secretos" in problems
    assert sum(p.startswith("falta el paso") for p in problems) == len(v.CI_STEPS)


def test_yaml_on_key_is_normalized():
    wf = yaml.safe_load("on:\n  pull_request:\njobs: {}\n")
    assert True in wf  # YAML 1.1
    path = v.REPO / ".github/workflows/cd.yml"
    assert "on" in v.load_workflow(path) and True not in v.load_workflow(path)


def test_the_real_cd_is_off_and_keyless():
    wf = v.load_workflow(v.REPO / ".github/workflows/cd.yml")
    assert v.cd_problems(wf) == []
    bad = {"permissions": {}, "jobs": {"images": {"if": "true"}}, "x": "credentials_json"}
    problems = v.cd_problems(bad)
    assert "sin permissions.id-token: write (OIDC)" in problems and "falta el trabajo plan" in problems
    assert "images no está condicionado a vars.TA_CD_ENABLED" in problems and "usa llaves en lugar de OIDC" in problems


def test_precommit_gitleaks_and_tfvars():
    cfg = yaml.safe_load((v.REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    assert v.precommit_problems(cfg) == []
    assert v.precommit_problems({"repos": []}) == ["falta el hook gitleaks", "falta el hook terraform-fmt"]
    report = [{"RuleID": "generic-api-key", "File": "a.py", "StartLine": 3, "Commit": "abcdef123"}]
    assert v.gitleaks_findings(report) == ["generic-api-key a.py:3 (abcdef1)"] and v.gitleaks_findings(None) == []
    files = ["project/infra/gcp/environments/dev/terraform.tfvars.example", "x/terraform.tfvars", "y/alerts.auto.tfvars"]
    assert v.tracked_tfvars(files) == ["x/terraform.tfvars", "y/alerts.auto.tfvars"]


def test_emulation_counts():
    text = "== contratos\n18 passed in 4s\n== identidad\n9 passed, 40 deselected in 3s\n== suite\nruido\n1639 passed, 3 skipped in 600s\n"
    c = v.emulation_counts(text)
    assert c["contratos"]["passed"] == 18 and c["identidad"]["passed"] == 9
    assert c["suite"]["passed"] == 1639 and c["suite"]["skipped"] == 3


class FakeRunner:
    def __init__(self):
        self.calls = []

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        from ops.validate32 import Result

        self.calls.append(cmd)
        return Result(0, "abc1234deadbeef\n" if "rev-parse" in cmd else "", "")


def _run(argv, tmp_path, http):
    lines = []
    code = v.main(argv, runner=FakeRunner(), printer=lines.append, sleep=lambda s: None, now=lambda: NOW,
                  evidence_path=tmp_path / "ev.json", http=http)
    return code, json.loads((tmp_path / "ev.json").read_text(encoding="utf-8")), "\n".join(lines)


def _runs(ci_conclusion="success", cd=True):
    runs = [{"id": 1, "name": "CI", "run_number": 7, "status": "completed", "conclusion": ci_conclusion, "event": "push",
             "html_url": "https://github.com/x/y/actions/runs/1", "run_started_at": "2026-10-29T02:40:00Z",
             "updated_at": "2026-10-29T02:52:30Z"}]
    if cd:
        runs.append({"id": 2, "name": "CD", "run_number": 1, "status": "completed", "conclusion": "skipped", "event": "push"})
    return runs


def test_github_part_ok_and_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)

    def http(path, conclusion="success"):
        if "/runs/1/jobs" in path:
            return {"jobs": [{"name": "pytest", "conclusion": conclusion}, {"name": "terraform", "conclusion": "success"}]}
        if "/runs/2/jobs" in path:
            return {"jobs": [{"name": "images", "conclusion": "skipped"}, {"name": "plan (gcp)", "conclusion": "skipped"}]}
        return {"workflow_runs": _runs(conclusion)}

    code, ev, _out = _run(["github"], tmp_path, http)
    steps = {s["id"]: s for s in ev["parts"]["G"]["steps"]}
    assert code == 0 and steps["G1"]["status"] == "OK" and steps["G2"]["status"] == "OK"
    assert steps["G1"]["data"]["minutes"] == 12.5 and steps["G1"]["data"]["sha"] == "abc1234deadbeef"
    code, ev, _out = _run(["github"], tmp_path, lambda p: http(p, "failure"))
    assert code == 1 and {s["id"]: s["status"] for s in ev["parts"]["G"]["steps"]}["G1"] == "FALLA"


def test_github_part_without_runs_waits_and_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    code, ev, out = _run(["github", "--max-wait-minutes", "2"], tmp_path, lambda p: {"workflow_runs": []})
    assert code == 1 and "esperando la CI" in out and "¿se hizo push?" in ev["parts"]["G"]["steps"][0]["detail"]


def test_status(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    code, ev, _out = _run(["status"], tmp_path, lambda p: {"workflow_runs": [{**_runs()[0], "head_sha": "abcdef1234"}]})
    assert code == 0 and ev["parts"]["status"]["steps"][0]["status"] == "OK"
