"""Validación automática del Lote 7.1 (HU-041, CI/CD): ``docs/validation/validation-7-1.md``.

    python project/scripts/validate_7_1.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes, en el orden de uso: ``preflight`` (A), ``tests`` (B), ``local`` (L: workflows, pre-commit, Terraform sin
backend, gitleaks sobre el historial, tfvars fuera del repo y las tres imágenes en modo ``test``, todo sin nube),
``emulate`` (E: la CI completa en un contenedor ``python:3.11`` limpio, ~15 min), ``plan`` (C: ``terraform plan`` de
las dos raíces con el CD apagado: no debe cambiar nada), ``github`` (G: después del push, la corrida de CI del commit
en GitHub Actions y la de CD, que se salta mientras esté apagada) y ``status``. Cada parte imprime un paso por línea
(OK / AVISO / FALLA / OMITIDO) y guarda su resumen en ``project/evidencia/validacion_7_1.json``.

No cambia la nube: el CD (federación GitHub → GCP y AWS) queda apagado (``github_cd_enabled = false``) hasta que el
usuario decida activarlo (ADR-34).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from ops import validate41 as v41
from ops.validate32 import (
    AVISO,
    FALLA,
    OK,
    WSL_HINT,
    Part,
    Runner,
    on_windows,
    parse_json,
    pytest_counts,
    summarize_plan,
)

REPO = v41.REPO
PROJECT = v41.PROJECT
EVIDENCE = PROJECT / "evidencia" / "validacion_7_1.json"
GCP_ENV = PROJECT / "infra" / "gcp" / "environments" / "dev"
AWS_ENV = PROJECT / "infra" / "aws" / "environments" / "dev"
GITHUB_REPO = "AlejandroSPosada/Trend_Analyzer"
GITHUB_API = "https://api.github.com"
GITLEAKS_IMAGE = "ghcr.io/gitleaks/gitleaks:v8.28.0"
IMAGES = ("simulator", "exporter", "alerts")

LOT_FILES = [
    ".github/workflows/ci.yml",
    ".github/workflows/cd.yml",
    ".pre-commit-config.yaml",
    ".gitleaks.toml",
    ".gitleaksignore",
    "project/requirements-ci.txt",
    "project/infra/gcp/modules/github_cd/main.tf",
    "project/infra/gcp/environments/dev/cicd.tf",
    "project/infra/aws/modules/github_cd_role/main.tf",
    "project/infra/aws/environments/dev/cicd.tf",
    "project/src/ops/validate71.py",
    "project/src/ops/tests/test_ops_validate71.py",
    "project/scripts/validate_7_1.py",
]
#: Lo que la CI de cada PR debe correr (HU-041): nombre del paso → texto que lo identifica en ci.yml.
CI_STEPS = {
    "pytest (suite completa)": "python -m pytest -q -p no:cacheprovider -rs",
    "contratos (HU-001)": "pytest contracts/tests",
    "identidad (sin handle ni user_id)": "-k \"identidad or hmac\"",
    "terraform fmt y validate": "bash project/infra/validate.sh",
    "imágenes (etapa test)": "build_${{ matrix.image }}_image.sh test",
    "gitleaks (historial)": "--gitleaks-ignore-path",
    "tfvars fuera del repo": "git check-ignore -q",
}

B1_EXPECTED = 9


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE
    http: Callable[[str], Any] | None = None

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "7.1", "hu": ["HU-041"], "parts": {}}

    def github(self, path: str) -> Any:
        """GET a la API pública de GitHub (el repositorio es público: sin token, 60 consultas por hora)."""
        if self.http is not None:
            return self.http(path)
        req = urllib.request.Request(f"{GITHUB_API}{path}", headers={"Accept": "application/vnd.github+json",
                                                                     "User-Agent": "trendanalyzer-validate71"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - API pública fija
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            return {"_error": str(exc)}


# --- utilidades puras ----------------------------------------------------------------------------


def load_workflow(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if True in data and "on" not in data:  # YAML 1.1: la clave `on` se lee como True
        data["on"] = data.pop(True)
    return data


def ci_problems(wf: dict[str, Any], text: str) -> list[str]:
    """Lo que HU-041 pide a la CI: disparadores, trabajos, pasos y sin credenciales de nube."""
    out = []
    on = wf.get("on") or {}
    if "pull_request" not in on:
        out.append("no corre en pull_request")
    if "main" not in ((on.get("push") or {}).get("branches") or []):
        out.append("no corre en push a main")
    if (wf.get("permissions") or {}).get("contents") != "read" or "id-token" in (wf.get("permissions") or {}):
        out.append("permisos: debe ser solo contents: read (sin id-token)")
    out += [f"falta el paso «{name}»" for name, marker in CI_STEPS.items() if marker not in text]
    if "secrets." in text:
        out.append("la CI no debe usar secretos")
    return out


def cd_problems(wf: dict[str, Any]) -> list[str]:
    """El CD: apagado mientras falte TA_CD_ENABLED, con OIDC (id-token) y sin llaves."""
    out = []
    if (wf.get("permissions") or {}).get("id-token") != "write":
        out.append("sin permissions.id-token: write (OIDC)")
    jobs = wf.get("jobs") or {}
    for name in ("images", "plan"):
        job = jobs.get(name)
        if not job:
            out.append(f"falta el trabajo {name}")
        elif "vars.TA_CD_ENABLED == 'true'" not in str(job.get("if", "")):
            out.append(f"{name} no está condicionado a vars.TA_CD_ENABLED")
    text = json.dumps(wf)
    if re.search(r"credentials_json|aws-secret-access-key|AWS_SECRET_ACCESS_KEY", text):
        out.append("usa llaves en lugar de OIDC")
    return out


def precommit_problems(cfg: dict[str, Any]) -> list[str]:
    hooks = {h.get("id") for r in (cfg.get("repos") or []) for h in (r.get("hooks") or [])}
    return [f"falta el hook {h}" for h in ("gitleaks", "terraform-fmt") if h not in hooks]


def gitleaks_findings(report: Any) -> list[str]:
    return [f"{x.get('RuleID')} {x.get('File')}:{x.get('StartLine')} ({str(x.get('Commit', ''))[:7]})"
            for x in (report or []) if isinstance(x, dict)]


def tracked_tfvars(files: list[str]) -> list[str]:
    return [f for f in files if re.search(r"(^|/)terraform\.tfvars$|\.auto\.tfvars$", f)]


def run_summary(run: dict[str, Any] | None) -> str:
    if not run:
        return "sin corrida"
    return f"{run.get('name')} #{run.get('run_number')} {run.get('status')}/{run.get('conclusion')} ({run.get('event')})"


def jobs_summary(jobs: list[dict[str, Any]]) -> dict[str, str]:
    return {j.get("name", "?"): str(j.get("conclusion") or j.get("status")) for j in jobs}


# --- Parte A · preflight -----------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código y de las herramientas")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    absent = [f for f in LOT_FILES if not (REPO / f).exists()]
    part.add("A1", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else "presentes")
    tools = {t: ctx.runner.which(t) for t in ("docker", "terraform", "git")}
    need = [t for t in tools if not tools[t]]
    part.add("A2", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {need}" if need else ""))
    docker = ctx.run(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=60)
    part.add("A3", "Docker en marcha", OK if docker.ok else FALLA, f"servidor {docker.out.strip()}" if docker.ok else docker.tail(3))
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    res = ctx.pytest("src/ops/tests/test_ops_validate71.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    good = res.ok and c["passed"] == B1_EXPECTED and not c["failed"] and not c["skipped"]
    part.add("B1", "pytest test_ops_validate71.py", OK if good else FALLA,
             f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {B1_EXPECTED})"
             + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · local ----------------------------------------------------------------------------------------


def git_files(ctx: Ctx) -> list[str]:
    res = ctx.run(["git", "-c", "safe.directory=*", "ls-files"], cwd=REPO, timeout=60)
    return [ln for ln in res.out.splitlines() if ln.strip()]


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Workflows, pre-commit, Terraform, gitleaks, tfvars e imágenes (sin nube)")
    ci_path, cd_path = REPO / ".github/workflows/ci.yml", REPO / ".github/workflows/cd.yml"
    ci_text = ci_path.read_text(encoding="utf-8")
    ci = load_workflow(ci_path)
    problems = ci_problems(ci, ci_text)
    part.add("L1", "ci.yml: PR y push a main, pytest, contratos, identidad, Terraform, imágenes, gitleaks y tfvars",
             FALLA if problems else OK, "\n".join(problems) or f"trabajos: {', '.join(ci.get('jobs') or {})}",
             jobs=list(ci.get("jobs") or {}))
    cd = load_workflow(cd_path)
    problems = cd_problems(cd)
    part.add("L2", "cd.yml: imágenes y plan con OIDC, apagado sin TA_CD_ENABLED", FALLA if problems else OK,
             "\n".join(problems) or f"trabajos: {', '.join(cd.get('jobs') or {})} (se saltan hasta que el usuario active el CD)")
    pc = yaml.safe_load((REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8")) or {}
    problems = precommit_problems(pc)
    part.add("L3", "pre-commit: gitleaks y terraform fmt", FALLA if problems else OK, "\n".join(problems) or "2 hooks")
    res = ctx.run(["bash", "project/infra/validate.sh"], cwd=REPO, timeout=900)
    part.add("L4", "terraform fmt -check y validate de las 4 raíces (con los módulos del CD)", OK if res.ok else FALLA,
             "OK" if res.ok else res.tail(12))
    out_dir = Path("/tmp/validate71")
    out_dir.mkdir(parents=True, exist_ok=True)
    report = out_dir / "gitleaks.json"
    report.unlink(missing_ok=True)
    res = ctx.run(["docker", "run", "--rm", "-v", f"{REPO}:/repo:ro", "-v", f"{out_dir}:/out",
                   "-e", "GIT_CONFIG_COUNT=1", "-e", "GIT_CONFIG_KEY_0=safe.directory", "-e", "GIT_CONFIG_VALUE_0=*",
                   GITLEAKS_IMAGE, "git", "/repo", "--no-banner", "--redact", "--config", "/repo/.gitleaks.toml",
                   "--gitleaks-ignore-path", "/repo/.gitleaksignore", "--report-format", "json",
                   "--report-path", "/out/gitleaks.json"], timeout=600)
    found = gitleaks_findings(parse_json(report.read_text(encoding="utf-8")) if report.exists() else None)
    ignored = [ln for ln in (REPO / ".gitleaksignore").read_text(encoding="utf-8").splitlines() if ln.strip()]
    part.add("L5", "gitleaks sobre todo el historial (con .gitleaksignore)", OK if res.ok and not found else FALLA,
             f"0 hallazgos nuevos; {len(ignored)} revisados a mano en .gitleaksignore (uno es un token de Mastodon del "
             "25-ago-2026 que hay que revocar: ver §3.30)" if res.ok and not found else "\n".join(found) or res.tail(6),
             ignored=len(ignored))
    tracked = tracked_tfvars(git_files(ctx))
    ignored_ok = all(ctx.run(["git", "-c", "safe.directory=*", "check-ignore", "-q", str(p.relative_to(REPO))], cwd=REPO).ok
                     for p in (GCP_ENV / "terraform.tfvars", AWS_ENV / "terraform.tfvars"))
    part.add("L6", "terraform.tfvars y *.auto.tfvars ignorados y sin versionar", OK if not tracked and ignored_ok else FALLA,
             "ninguno versionado; los dos terraform.tfvars ignorados" if not tracked and ignored_ok else f"versionados: {tracked}")
    if getattr(ctx.options, "skip_images", False):
        part.note("imágenes omitidas (--skip-images)")
        return part.finish()
    for name in IMAGES:
        res = ctx.run(["bash", f"project/scripts/build_{name}_image.sh", "test"], cwd=REPO, timeout=1200)
        passed = re.findall(r"(\d+) passed", res.out)
        detail = (f"{passed[-1]} pruebas dentro en {res.seconds:.0f} s" if passed else
                  f"capas en la caché de Docker ({res.seconds:.0f} s): mismo contenido que una construcción que ya pasó sus pruebas")
        part.add(f"L7{name[0]}", f"imagen {name}: etapa test (pruebas dentro, sin publicar)", OK if res.ok else FALLA,
                 detail if res.ok else res.tail(10),
                 passed=int(passed[-1]) if passed else None, seconds=round(res.seconds))
    return part.finish()


# --- Parte E · emular la CI ---------------------------------------------------------------------------------


EMULATE = ("set -e; python -m pip install -q -r requirements-ci.txt; "
           "echo '== contratos'; python -m pytest contracts/tests -q -p no:cacheprovider | tail -1; "
           "echo '== identidad'; python -m pytest -q -p no:cacheprovider -k 'identidad or hmac' "
           "src/streaming/tests/test_enriched_row.py src/dq/tests/test_dq_identity.py src/smoke/tests/test_smoke_e2e.py | tail -1; "
           "echo '== suite'; python -m pytest -q -p no:cacheprovider -rs | tail -15")


def emulation_counts(text: str) -> dict[str, dict[str, int]]:
    out, current = {}, None
    for line in text.splitlines():
        m = re.match(r"^== (\w+)", line)
        if m:
            current = m.group(1)
        elif current and re.search(r"\d+ (passed|failed)", line):
            out[current] = pytest_counts(line)
    return out


def part_emulate(ctx: Ctx) -> int:
    part = Part(ctx, "E", "La CI de pytest en un contenedor python:3.11 limpio (sin venv ni credenciales)")
    tree = Path("/tmp/validate71/tree")
    if tree.exists():
        shutil.rmtree(tree)
    tree.mkdir(parents=True)
    copy = f"git -c safe.directory='*' ls-files -co --exclude-standard -z | tar --null -T - -cf - | tar -xf - -C {tree}"
    # `.git` vacío: como en el checkout de GitHub, algunas pruebas miran si una ruta está dentro de un repositorio.
    res = ctx.run(["bash", "-c", f"{copy} && mkdir -p {tree}/.git"], cwd=REPO, timeout=300)
    if not res.ok:
        part.add("E1", "copia del árbol", FALLA, res.tail(5))
        return part.finish()
    res = ctx.run(["docker", "run", "--rm", "-v", f"{tree}:/w", "-w", "/w/project", "-e", "PYTHONDONTWRITEBYTECODE=1",
                   "python:3.11", "bash", "-c", EMULATE], timeout=3600)
    counts = emulation_counts(res.out)
    for sid, key, title in (("E1", "contratos", "contratos (HU-001)"), ("E2", "identidad", "identidad: sin handle ni user_id"),
                            ("E3", "suite", "suite completa")):
        c = counts.get(key) or {}
        good = res.ok and c.get("passed", 0) > 0 and not c.get("failed")
        part.add(sid, title, OK if good else FALLA,
                 f"{c.get('passed', 0)} passed, {c.get('skipped', 0)} skipped, {c.get('failed', 0)} failed"
                 + ("" if good else "\n" + res.tail(12)),
                 **c)
    part.add("E4", "tiempo de la emulación (instalar + pytest)", OK, f"{res.seconds / 60:.1f} min", seconds=round(res.seconds))
    return part.finish()


# --- Parte C · plan --------------------------------------------------------------------------------------------


def part_plan(ctx: Ctx) -> int:
    part = Part(ctx, "C", "terraform plan con el CD apagado (no debe cambiar nada)")
    for sid, root, name in (("C1", GCP_ENV, "gcp"), ("C2", AWS_ENV, "aws")):
        plan_path = Path(f"/tmp/validate71/tfplan_{name}")
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        res = ctx.tf(root, "plan", "-input=false", "-lock-timeout=60s", f"-out={plan_path}", timeout=1500)
        if not res.ok:
            part.add(sid, f"plan de infra/{name}", FALLA, res.tail(12))
            continue
        show = ctx.tf(root, "show", "-json", str(plan_path), timeout=600)
        summary = summarize_plan(parse_json(show.out) or {})
        changes = sum(len(v) for v in summary.values())
        part.add(sid, f"plan de infra/{name}: sin cambios (github_cd_enabled = false)", OK if not changes else FALLA,
                 "0 a crear, 0 a cambiar, 0 a destruir" if not changes else json.dumps(summary, ensure_ascii=False), **summary)
    return part.finish()


# --- Parte G · GitHub Actions -----------------------------------------------------------------------------------


def head_sha(ctx: Ctx) -> str:
    return ctx.run(["git", "-c", "safe.directory=*", "rev-parse", "HEAD"], cwd=REPO, timeout=60).out.strip()


def runs_for(ctx: Ctx, sha: str) -> list[dict[str, Any]]:
    data = ctx.github(f"/repos/{GITHUB_REPO}/actions/runs?head_sha={sha}&per_page=20")
    return list((data or {}).get("workflow_runs") or [])


def part_github(ctx: Ctx) -> int:
    part = Part(ctx, "G", "GitHub Actions: la CI del commit en main y el CD apagado")
    sha = getattr(ctx.options, "sha", None) or head_sha(ctx)
    wait = float(getattr(ctx.options, "max_wait_minutes", 45) or 0) * 60
    waited, runs = 0.0, []
    while True:
        runs = runs_for(ctx, sha)
        ci = next((r for r in runs if r.get("name") == "CI"), None)
        if ci and ci.get("status") == "completed":
            break
        if waited >= wait:
            break
        part.note(f"esperando la CI de {sha[:7]}: {run_summary(ci)}")
        ctx.sleep(60)
        waited += 60
    ci = next((r for r in runs if r.get("name") == "CI"), None)
    if not ci:
        part.add("G1", "corrida de CI del commit", FALLA, f"no hay corrida de CI para {sha[:7]} (¿se hizo push?)")
        return part.finish()
    jobs = (ctx.github(f"/repos/{GITHUB_REPO}/actions/runs/{ci['id']}/jobs?per_page=50") or {}).get("jobs") or []
    per_job = jobs_summary(jobs)
    good = ci.get("conclusion") == "success"
    part.add("G1", "CI en GitHub Actions", OK if good else (AVISO if ci.get("status") != "completed" else FALLA),
             f"{run_summary(ci)} · {', '.join(f'{k}: {v}' for k, v in per_job.items())} · {ci.get('html_url')}",
             run_id=ci.get("id"), conclusion=ci.get("conclusion"), jobs=per_job, sha=sha,
             minutes=round((datetime.fromisoformat(ci["updated_at"].replace("Z", "+00:00"))
                            - datetime.fromisoformat(ci["run_started_at"].replace("Z", "+00:00"))).total_seconds() / 60, 1)
             if ci.get("run_started_at") and ci.get("updated_at") else None)
    cd = next((r for r in runs if r.get("name") == "CD"), None)
    if cd:
        cd_jobs = jobs_summary((ctx.github(f"/repos/{GITHUB_REPO}/actions/runs/{cd['id']}/jobs?per_page=50") or {}).get("jobs") or [])
        skipped = cd.get("conclusion") in ("skipped", "success") and all(v == "skipped" for v in cd_jobs.values())
        part.add("G2", "CD apagado: sus trabajos se saltan", OK if skipped else AVISO, f"{run_summary(cd)} · {cd_jobs}", jobs=cd_jobs)
    else:
        part.add("G2", "CD apagado", OK, "sin corrida de CD para este commit (no tocó rutas del CD o se saltó entera)")
    return part.finish()


# --- status ---------------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    data = ctx.github(f"/repos/{GITHUB_REPO}/actions/runs?branch=main&per_page=5")
    runs = [r for r in (data or {}).get("workflow_runs") or [] if r.get("name") == "CI"]
    last = runs[0] if runs else None
    part.add("S1", "última CI en main", OK if last and last.get("conclusion") == "success" else AVISO,
             f"{run_summary(last)} · {str(last.get('head_sha'))[:7]}" if last else "sin corridas")
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "emulate": part_emulate,
    "plan": part_plan,
    "github": part_github,
    "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_7_1.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--skip-images", action="store_true", help="L: no construir las imágenes")
    parser.add_argument("--sha", default=None, help="G: commit a revisar (por defecto, HEAD)")
    parser.add_argument("--max-wait-minutes", type=float, default=45, help="G: espera máxima de la CI")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         sleep: Callable[[float], None] | None = None, now: Callable[[], datetime] | None = None,
         evidence_path: Path | None = None, http: Callable[[str], Any] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, sleep=sleep or time.sleep,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE, http=http)
    printer(f"Lote 7.1 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    return PARTS[args.part](ctx)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
