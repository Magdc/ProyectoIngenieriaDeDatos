"""Validación automática del Lote 3.2 (HU-006, HU-025): ``docs/validation/validation-3-2.md``.

    python project/scripts/validate_3_2.py <parte> [opciones]      (desde la raíz del repo)

Partes: ``preflight`` (A), ``tests`` (B), ``terraform`` (C), ``federation`` (D), ``job`` (E),
``lake`` (F); además ``status`` (solo lectura) y ``off`` (Apagado: pausa el Scheduler, cancela
ejecuciones y lo confirma). Cada parte imprime un paso por línea (OK / AVISO / FALLA) y guarda
su resumen en ``project/evidencia/validacion_3_2.json``.

Los comandos corren sin entrada estándar (un comando que pida confirmación falla en vez de
esperar en silencio) y los largos muestran su salida mientras corren. Lo que cambia la nube
(``terraform apply``, reanudar el Scheduler) pide confirmación, salvo con ``--yes``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import warnings
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\.api_core")

REPO = Path(__file__).resolve().parents[3]
PROJECT = REPO / "project"
SRC = PROJECT / "src"
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_3_2.json"
AWS_ENV = PROJECT / "infra" / "aws" / "environments" / "dev"
GCP_ENV = PROJECT / "infra" / "gcp" / "environments" / "dev"
AWS_AUTO = AWS_ENV / "gcp_exporter.auto.tfvars"
GCP_AUTO = GCP_ENV / "aws_exporter.auto.tfvars"
EXPORT_CLI = PROJECT / "scripts" / "export_gcs_to_s3.py"
BUILD_SCRIPT = PROJECT / "scripts" / "build_exporter_image.sh"

DEFAULT_PROJECT = "project-0568d9ba-848f-413c-88b"
DEFAULT_REGION = "us-central1"
AWS_REGION = "us-east-1"
JOB = "ta-gcs-to-s3"
SCHEDULER = "ta-gcs-to-s3-cada-15-min"
ROLE = "ta-gcp-exporter-role"
AUDIENCE = "ta-gcp-exporter-role"
VALIDATION_STATE = "export-state/_validation/watermark-d.json"
# D6 planea sin cursor (este watermark nunca se escribe): todo lo de D4 debe salir omitido.
VALIDATION_STATE_IDEM = "export-state/_validation/watermark-d-idem.json"
HISTORICAL_OBJECTS = 12102

# Pruebas esperadas (B1, B2): las cuenta docs/validation/validation-3-2.md.
B1_EXPECTED = 120
B2_EXPECTED = 68

# Precios usados para estimar (Cloud Run Jobs, us-central1, Tier 1; consultados en oct-2026).
CPU_USD_PER_S = 0.000018
MEM_USD_PER_GIB_S = 0.000002
JOB_VCPU, JOB_GIB = 1.0, 0.5
RUNS_PER_DAY = 96

OK, AVISO, FALLA, OMITIDO = "OK", "AVISO", "FALLA", "OMITIDO"

LOT_FILES = [
    "src/exporter/__init__.py",
    "src/exporter/cli.py",
    "src/exporter/config.py",
    "src/exporter/core.py",
    "src/exporter/gcs_io.py",
    "src/exporter/identity.py",
    "src/exporter/jsonlog.py",
    "src/exporter/keys.py",
    "src/exporter/lake_check.py",
    "src/exporter/probe.py",
    "src/exporter/s3_io.py",
    "src/exporter/testing.py",
    "src/exporter/verify.py",
    "src/exporter/watermark.py",
    "src/exporter/Dockerfile",
    "src/exporter/cloudbuild.yaml",
    "src/exporter/requirements.txt",
    "src/exporter/EXPORTER.md",
    "scripts/export_gcs_to_s3.py",
    "scripts/build_exporter_image.sh",
    "scripts/validate_3_2.py",
    "src/ops/validate32.py",
    "infra/aws/modules/gcp_exporter_role/main.tf",
    "infra/aws/environments/dev/exporter.tf",
    "infra/aws/environments/dev/variables_exporter.tf",
    "infra/aws/environments/dev/outputs_exporter.tf",
    "infra/aws/environments/dev/gcp_exporter.auto.tfvars.example",
    "infra/gcp/modules/cloud_run_exporter/main.tf",
    "infra/gcp/environments/dev/exporter.tf",
    "infra/gcp/environments/dev/variables_exporter.tf",
    "infra/gcp/environments/dev/outputs_exporter.tf",
    "infra/gcp/environments/dev/aws_exporter.auto.tfvars.example",
]
PREVIOUS_FILES = [
    "infra/validate.sh",
    "infra/gcp/environments/dev/main.tf",
    "infra/gcp/environments/dev/variables.tf",
    "infra/gcp/environments/dev/platform.tf",
    "infra/aws/environments/dev/main.tf",
    "infra/aws/environments/dev/variables.tf",
    "infra/aws/environments/dev/lake.tf",
    "src/common/enriched_row.py",
    "scripts/status.py",
    "scripts/off.py",
]

AWS_PLAN_CREATES = {
    "module.gcp_exporter_role[0].aws_iam_role.this",
    "module.gcp_exporter_role[0].aws_iam_role_policy.s3",
}
GCP_BASE_TARGETS = [
    "google_storage_bucket_iam_member.exporter_lake_enriched",
    "google_storage_bucket_iam_member.exporter_lake_read",
    "google_service_account_iam_member.exporter_oidc_token_creator",
]
GCP_JOB_CREATES = {
    "module.exporter_job[0].google_cloud_run_v2_job.this",
    "module.exporter_job[0].google_cloud_run_v2_job_iam_member.scheduler_invoker",
    "module.exporter_job[0].google_cloud_scheduler_job.this",
}


# --- ejecución de comandos ---------------------------------------------------------------


@dataclass
class Result:
    code: int
    out: str = ""
    err: str = ""
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.code == 0

    @property
    def text(self) -> str:
        return (self.out or "") + (("\n" + self.err) if self.err else "")

    def tail(self, n: int = 12) -> str:
        lines = [ln for ln in self.text.splitlines() if ln.strip()]
        return "\n".join(lines[-n:])


class Runner:
    """Ejecuta comandos sin entrada estándar; con ``stream`` muestra la salida al vuelo."""

    def __init__(self, printer: Callable[[str], None] = print):
        self.printer = printer

    def which(self, name: str) -> str | None:
        return shutil.which(name)

    def run(
        self,
        cmd: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 600,
        stream: bool = False,
    ) -> Result:
        start = time.monotonic()
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(cwd) if cwd else None,
                env=full_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT if stream else subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except FileNotFoundError as exc:
            return Result(127, "", f"no se encontró {cmd[0]}: {exc}", 0.0)
        if not stream:
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                out, err = proc.communicate()
                return Result(124, out or "", (err or "") + f"\ntiempo agotado ({timeout:.0f} s)", time.monotonic() - start)
            return Result(proc.returncode, out or "", err or "", time.monotonic() - start)

        lines: list[str] = []

        def pump() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                lines.append(line)
                self.printer("    │ " + line.rstrip("\n"))

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            reader.join(5)
            return Result(124, "".join(lines), f"tiempo agotado ({timeout:.0f} s)", time.monotonic() - start)
        reader.join(5)
        return Result(proc.returncode, "".join(lines), "", time.monotonic() - start)


# --- pasos y evidencia ---------------------------------------------------------------------


@dataclass
class Step:
    id: str
    title: str
    status: str
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)


class Part:
    def __init__(self, ctx: "Ctx", key: str, title: str):
        self.ctx, self.key, self.title = ctx, key, title
        self.steps: list[Step] = []
        self.started = ctx.now()
        ctx.printer(f"\n== Parte {key} · {title} ==")

    def add(self, sid: str, title: str, status: str, detail: str = "", **data: Any) -> Step:
        step = Step(sid, title, status, detail, data)
        self.steps.append(step)
        first, *rest = (detail or "").splitlines() or [""]
        self.ctx.printer(f"{sid:<4} {status:<6} {title}" + (f": {first}" if first else ""))
        for line in rest[:15]:
            self.ctx.printer(f"            {line}")
        return step

    def note(self, text: str) -> None:
        self.ctx.printer(f"     … {text}")

    @property
    def worst(self) -> str:
        statuses = {s.status for s in self.steps}
        for status in (FALLA, AVISO):
            if status in statuses:
                return status
        return OK

    def finish(self) -> int:
        counts = {s: sum(1 for st in self.steps if st.status == s) for s in (OK, AVISO, FALLA, OMITIDO)}
        summary = ", ".join(f"{n} {s}" for s, n in counts.items() if n)
        self.ctx.printer(f"-- Parte {self.key}: {self.worst} ({summary})")
        self.ctx.save_part(self)
        return 1 if self.worst == FALLA else 0


@dataclass
class Ctx:
    runner: Runner
    options: argparse.Namespace
    printer: Callable[[str], None] = print
    confirm: Callable[[str], bool] | None = None
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    sleep: Callable[[float], None] = time.sleep
    evidence_path: Path = EVIDENCE
    cache: dict[str, Any] = field(default_factory=dict)

    @property
    def project(self) -> str:
        return getattr(self.options, "project", None) or os.environ.get("GCP_PROJECT") or DEFAULT_PROJECT

    @property
    def region(self) -> str:
        return getattr(self.options, "region", None) or os.environ.get("GCP_REGION") or DEFAULT_REGION

    @property
    def lake_bucket(self) -> str:
        return f"{self.project}-ta-lake"

    @property
    def sa_email(self) -> str:
        return f"sa-exporter@{self.project}.iam.gserviceaccount.com"

    def env(self) -> dict[str, str]:
        env = {"GCP_PROJECT": self.project, "GCP_REGION": self.region, "AWS_REGION": AWS_REGION,
               "CLOUDSDK_CORE_DISABLE_PROMPTS": "1"}
        profile = getattr(self.options, "aws_profile", None)
        if profile:
            env["AWS_PROFILE"] = profile
        return env

    def ask(self, question: str) -> bool:
        if getattr(self.options, "yes", False):
            return True
        if self.confirm is not None:
            return self.confirm(question)
        try:
            return input(f"{question} [s/N] ").strip().lower() in {"s", "si", "sí", "y", "yes"}
        except EOFError:
            return False

    # evidencia
    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "3.2", "hu": ["HU-006", "HU-025"], "parts": {}}

    def save_part(self, part: Part) -> None:
        data = self.load_evidence()
        data.setdefault("parts", {})[part.key] = {
            "title": part.title,
            "status": part.worst,
            "started_at": part.started.isoformat(),
            "finished_at": self.now().isoformat(),
            "steps": [asdict(s) for s in part.steps],
        }
        data["updated_at"] = self.now().isoformat()
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    def part_data(self, key: str) -> dict[str, Any]:
        return self.load_evidence().get("parts", {}).get(key, {})

    def step_data(self, part_key: str, step_id: str) -> dict[str, Any]:
        for step in self.part_data(part_key).get("steps", []):
            if step.get("id") == step_id:
                return step.get("data", {})
        return {}

    # comandos
    def run(self, cmd: list[str], **kw: Any) -> Result:
        env = self.env()
        env.update(kw.pop("env", None) or {})
        return self.runner.run(cmd, env=env, **kw)

    def gcloud(self, *args: str, timeout: float = 300, stream: bool = False) -> Result:
        return self.run(["gcloud", *args, f"--project={self.project}"], timeout=timeout, stream=stream)

    def aws(self, *args: str, timeout: float = 120) -> Result:
        return self.run(["aws", *args, "--output", "json", "--region", AWS_REGION], timeout=timeout)

    def tf(self, root: Path, *args: str, timeout: float = 900, stream: bool = False) -> Result:
        return self.run(["terraform", f"-chdir={root}", *args], timeout=timeout, stream=stream)

    def exporter(self, *args: str, env: dict[str, str] | None = None, timeout: float = 1800, stream: bool = True) -> Result:
        full = {"PYTHONPATH": str(SRC)}
        full.update(env or {})
        return self.run([sys.executable, str(EXPORT_CLI), *args], env=full, timeout=timeout, stream=stream, cwd=REPO)

    def pytest(self, *args: str, timeout: float = 900) -> Result:
        return self.run([sys.executable, "-m", "pytest", *args], cwd=PROJECT, timeout=timeout)


# --- utilidades puras ----------------------------------------------------------------------


def parse_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        start = min([i for i in (text.find("{"), text.find("[")) if i >= 0], default=-1)
        if start < 0:
            return None
        try:
            return json.loads(text[start:])
        except ValueError:
            return None


def pytest_counts(text: str) -> dict[str, int]:
    tail = "\n".join((text or "").splitlines()[-5:])
    counts = {}
    for key, pattern in (("passed", "passed"), ("failed", "failed"), ("skipped", "skipped"),
                         ("error", "errors?"), ("deselected", "deselected")):
        m = re.search(rf"(\d+) {pattern}\b", tail)
        counts[key] = int(m.group(1)) if m else 0
    return counts


def summarize_plan(plan: dict[str, Any]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {"create": [], "update": [], "delete": [], "replace": []}
    for rc in (plan or {}).get("resource_changes", []) or []:
        if rc.get("mode", "managed") != "managed":
            continue
        actions = rc.get("change", {}).get("actions", [])
        addr = rc.get("address", "?")
        if actions in (["no-op"], ["read"]) or not actions:
            continue
        if actions == ["create"]:
            out["create"].append(addr)
        elif actions == ["update"]:
            out["update"].append(addr)
        elif actions == ["delete"]:
            out["delete"].append(addr)
        else:
            out["replace"].append(addr)
    for key in out:
        out[key].sort()
    return out


def _short(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True) if not isinstance(value, str) else repr(value)
    return text if len(text) <= 120 else text[:117] + "…"


def _at(tree: Any, path: tuple) -> Any:
    for key in path:
        if isinstance(tree, dict):
            tree = tree.get(key)
        elif isinstance(tree, list) and isinstance(key, int) and key < len(tree):
            tree = tree[key]
        else:
            return None
    return tree


def _diff(before: Any, after: Any, path: tuple, unknown: Any, sensitive: Any, out: list[str]) -> None:
    if _at(unknown, path) is True:
        out.append(f"{_path(path)}: {_short(before)} → (se sabrá al aplicar)")
        return
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after)):
            _diff(before.get(key), after.get(key), path + (key,), unknown, sensitive, out)
        return
    if isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        for i, (b, a) in enumerate(zip(before, after)):
            _diff(b, a, path + (i,), unknown, sensitive, out)
        return
    if before != after:
        if _at(sensitive, path) is True:
            out.append(f"{_path(path)}: (valor sensible) → (valor sensible)")
        else:
            out.append(f"{_path(path)}: {_short(before)} → {_short(after)}")


def _path(path: tuple) -> str:
    return ".".join(str(p) for p in path) or "(todo)"


def plan_diffs(plan: dict[str, Any], limit: int = 12) -> dict[str, list[str]]:
    """Atributos que cambian en cada recurso a cambiar o reemplazar (antes → después)."""
    out: dict[str, list[str]] = {}
    for rc in (plan or {}).get("resource_changes", []) or []:
        change = rc.get("change", {}) or {}
        actions = change.get("actions", [])
        if rc.get("mode", "managed") != "managed" or actions in (["no-op"], ["read"], ["create"], ["delete"]) or not actions:
            continue
        lines: list[str] = []
        sensitive = change.get("after_sensitive") or change.get("before_sensitive") or {}
        _diff(change.get("before") or {}, change.get("after") or {}, (), change.get("after_unknown") or {}, sensitive, lines)
        if change.get("replace_paths"):
            lines.append("fuerza el reemplazo: " + ", ".join(_path(tuple(p)) for p in change["replace_paths"]))
        out[rc.get("address", "?")] = lines[:limit] + ([f"… y {len(lines) - limit} más"] if len(lines) > limit else [])
    return out


def diff_text(summary: dict[str, Any], addresses: Any = None) -> str:
    diffs = summary.get("diffs") or {}
    keep = set(addresses) if addresses is not None else set(diffs)
    lines = []
    for addr in sorted(a for a in diffs if a in keep):
        lines.append(f"{addr}:")
        lines += [f"  {ln}" for ln in diffs[addr]] or ["  (sin diferencias visibles en el plan)"]
    return ("\n" + "\n".join(lines)) if lines else ""


ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def tf_error(res: "Result", n: int = 12) -> str:
    """El bloque ``Error:`` de Terraform sin colores ni marcos (o las últimas líneas si no hay)."""
    lines = [ANSI.sub("", ln).strip().lstrip("│╷╵").strip() for ln in (res.text or "").splitlines()]
    lines = [ln for ln in lines if ln]
    first = next((i for i, ln in enumerate(lines) if ln.startswith("Error:")), None)
    return "\n".join(lines[first:first + n] if first is not None else lines[-n:])


def errored_state_help(root: Path) -> str | None:
    """Si un apply anterior no pudo guardar el estado, Terraform lo dejó en ``errored.tfstate``."""
    path = root / "errored.tfstate"
    if not path.exists():
        return None
    rel = root.relative_to(PROJECT) if root.is_relative_to(PROJECT) else root
    return (f"hay un estado sin guardar en {rel}/errored.tfstate (un apply creó recursos y no pudo escribir el estado). "
            f"No planee ni aplique antes de subirlo: terraform -chdir={rel} state push errored.tfstate "
            f"(se niega si el estado remoto cambió), compruebe con state list y mueva el archivo fuera del repo. "
            f"Si ya lo subió, solo muévalo.")


def plan_line(summary: dict[str, list[str]]) -> str:
    return (f"{len(summary['create'])} a crear, {len(summary['update'])} a cambiar, "
            f"{len(summary['delete'])} a destruir, {len(summary['replace'])} a reemplazar")


def tfvars_value(path: Path, key: str) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    m = re.search(rf'^\s*{re.escape(key)}\s*=\s*"([^"]*)"', text, flags=re.M)
    return m.group(1) if m else None


def render_tfvars(header: str, values: dict[str, Any]) -> str:
    """HCL con los ``=`` alineados como ``terraform fmt`` (luego se corre fmt igual)."""
    width = max(len(k) for k in values)
    lines = [f"# {ln}" if ln else "#" for ln in header.splitlines()]
    lines.append("")
    for key, value in values.items():
        if isinstance(value, list):
            rendered = "[" + ", ".join(json.dumps(v) for v in value) + "]"
        else:
            rendered = json.dumps(value)
        lines.append(f"{key.ljust(width)} = {rendered}")
    return "\n".join(lines) + "\n"


def parse_tfvars(path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return out
    for m in re.finditer(r'^\s*([a-z0-9_]+)\s*=\s*(.+?)\s*$', text, flags=re.M):
        raw = m.group(2)
        try:
            out[m.group(1)] = json.loads(raw)
        except ValueError:
            out[m.group(1)] = raw
    return out


def trust_problems(doc: Any, unique_id: str, email: str, audience: str) -> list[str]:
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from exporter.identity import trust_conditions

    trust = trust_conditions(doc)
    problems = []
    if trust["federated"] != {"accounts.google.com"}:
        problems.append(f"Principal.Federated = {sorted(trust['federated'])}")
    if "sts:AssumeRoleWithWebIdentity" not in trust["actions"]:
        problems.append("falta sts:AssumeRoleWithWebIdentity")
    conds = trust["conditions"]
    expect = {
        "accounts.google.com:aud": {unique_id},
        "accounts.google.com:oaud": {audience},
        "accounts.google.com:sub": {unique_id},
    }
    for key, must in expect.items():
        got = conds.get(key, {}).get("values", set())
        if not must <= got:
            problems.append(f"{key}: {sorted(got)} (debe incluir {sorted(must)})")
    if conds.get("accounts.google.com:sub", {}).get("values", set()) - {unique_id}:
        problems.append("accounts.google.com:sub acepta otras cuentas")
    return problems


def policy_actions(doc: Any) -> tuple[set[str], list[str]]:
    if isinstance(doc, str):
        from urllib.parse import unquote

        doc = json.loads(unquote(doc))
    actions: set[str] = set()
    resources: list[str] = []
    statements = doc.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for st in statements:
        if st.get("Effect") != "Allow":
            continue
        acts = st.get("Action", [])
        actions.update(acts if isinstance(acts, list) else [acts])
        res = st.get("Resource", [])
        resources.extend(res if isinstance(res, list) else [res])
    return actions, resources


def billable_seconds(duration: float) -> float:
    return max(60.0, math.ceil(duration * 10) / 10)


def cost_per_run(duration: float) -> float:
    secs = billable_seconds(duration)
    return secs * (JOB_VCPU * CPU_USD_PER_S + JOB_GIB * MEM_USD_PER_GIB_S)


def execution_name(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    meta = data.get("metadata") or {}
    name = meta.get("name") or data.get("name")
    return name.split("/")[-1] if name else None


def execution_times(data: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    status = data.get("status") or {}

    def ts(*values: Any) -> datetime | None:
        for v in values:
            if v:
                try:
                    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                except ValueError:
                    continue
        return None

    start = ts(status.get("startTime"), data.get("startTime"))
    end = ts(status.get("completionTime"), data.get("completionTime"))
    return start, end


def execution_created(data: dict[str, Any]) -> datetime | None:
    meta = data.get("metadata") or {}
    for v in (meta.get("creationTimestamp"), data.get("createTime"), data.get("creationTimestamp")):
        if v:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except ValueError:
                continue
    return None


def execution_succeeded(data: dict[str, Any]) -> bool | None:
    status = data.get("status") or {}
    if int(status.get("succeededCount") or data.get("succeededCount") or 0) >= 1:
        return True
    if int(status.get("failedCount") or data.get("failedCount") or 0) >= 1 and not status.get("runningCount"):
        return False
    for cond in status.get("conditions") or data.get("conditions") or []:
        if cond.get("type") == "Completed":
            if cond.get("status") == "True" or cond.get("state") == "CONDITION_SUCCEEDED":
                return True
            if cond.get("status") == "False" or cond.get("state") == "CONDITION_FAILED":
                return False
    return None


def execution_running(data: dict[str, Any]) -> bool:
    status = data.get("status") or {}
    if status.get("completionTime") or data.get("completionTime"):
        return False
    return execution_succeeded(data) is None


# --- piezas que usan varias partes -------------------------------------------------------


def aws_outputs(ctx: Ctx, part: Part | None = None) -> dict[str, Any]:
    if "aws_outputs" in ctx.cache:
        return ctx.cache["aws_outputs"]
    if part:
        part.note("leyendo terraform output de infra/aws (en /mnt/c puede tardar un minuto)…")
    res = ctx.tf(AWS_ENV, "output", "-json", timeout=300)
    data = parse_json(res.out) if res.ok else None
    outputs = {k: v.get("value") for k, v in (data or {}).items() if isinstance(v, dict)}
    ctx.cache["aws_outputs"] = outputs
    return outputs


def exporter_env(ctx: Ctx, outputs: dict[str, Any]) -> dict[str, str]:
    env = {
        "GCP_PROJECT": ctx.project,
        "TA_GCS_LAKE_BUCKET": ctx.lake_bucket,
        "TA_EXPORTER_SA": ctx.sa_email,
        "AWS_REGION": AWS_REGION,
        "TA_EXPORTER_AUDIENCE": outputs.get("exporter_oidc_audience") or AUDIENCE,
    }
    for key, out in (("TA_LAKE_BUCKET", "lake_bucket"), ("TA_LAKE_KMS_KEY_ARN", "kms_key_arn"), ("TA_EXPORTER_ROLE_ARN", "exporter_role_arn")):
        if outputs.get(out):
            env[key] = str(outputs[out])
    return env


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def scheduler_state(ctx: Ctx) -> tuple[str | None, dict[str, Any]]:
    res = ctx.gcloud("scheduler", "jobs", "describe", SCHEDULER, f"--location={ctx.region}", "--format=json", timeout=120)
    if not res.ok:
        if "NOT_FOUND" in res.text or "not found" in res.text.lower():
            return "NO_EXISTE", {}
        return None, {"error": res.tail(4)}
    data = parse_json(res.out) or {}
    return data.get("state"), data


def list_executions(ctx: Ctx, limit: int = 20) -> list[dict[str, Any]] | None:
    res = ctx.gcloud("run", "jobs", "executions", "list", f"--job={JOB}", f"--region={ctx.region}",
                     "--format=json", f"--limit={limit}", timeout=120)
    if not res.ok:
        if "NOT_FOUND" in res.text or "not found" in res.text.lower():
            return []
        return None
    data = parse_json(res.out)
    return data if isinstance(data, list) else []


def job_log_event(ctx: Ctx, execution: str, event: str, *, wait_s: float = 180) -> dict[str, Any] | None:
    flt = (f'resource.type="cloud_run_job" AND resource.labels.job_name="{JOB}" '
           f'AND labels."run.googleapis.com/execution_name"="{execution}" AND jsonPayload.event="{event}"')
    for attempt in range(int(wait_s // 15) + 1):
        res = ctx.gcloud("logging", "read", flt, "--limit=5", "--order=desc", "--freshness=1d", "--format=json", timeout=120)
        entries = parse_json(res.out) if res.ok else None
        if entries:
            return entries[0].get("jsonPayload") or {}
        if attempt < int(wait_s // 15):
            ctx.sleep(15)
    return None


# 840 s por intento × 2 intentos (1 reintento) + margen: el validador no corta antes que Cloud Run.
EXECUTE_TIMEOUT = 840 * 2 + 300


def execute_job(ctx: Ctx, part: Part, sid: str, title: str, args: list[str] | None = None, timeout: float = EXECUTE_TIMEOUT) -> dict[str, Any]:
    cmd = ["run", "jobs", "execute", JOB, f"--region={ctx.region}", "--wait", "--format=json"]
    if args:
        cmd.append("--args=" + ",".join(args))
    part.note(f"gcloud run jobs execute {JOB}" + (f" --args={','.join(args)}" if args else "") + " --wait …")
    res = ctx.gcloud(*cmd, timeout=timeout)
    data = parse_json(res.out)
    name = execution_name(data)
    if name is None:
        m = re.search(rf"({JOB}-[a-z0-9]+)", res.text)
        name = m.group(1) if m else None
    start, end = execution_times(data or {})
    if name and not (start and end):
        desc = ctx.gcloud("run", "jobs", "executions", "describe", name, f"--region={ctx.region}", "--format=json", timeout=120)
        start, end = execution_times(parse_json(desc.out) or {})
    duration = (end - start).total_seconds() if start and end else None
    return {"ok": res.ok, "execution": name, "duration_s": duration, "tail": res.tail(6)}


# --- Parte A · preflight --------------------------------------------------------------------


WSL_HINT = ("corra desde WSL: cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal "
            "&& source ~/.venvs/ta-streaming/bin/activate")


def on_windows() -> bool:
    return os.name == "nt" or sys.platform.startswith("win")


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA,
                 f"está corriendo en Windows ({sys.platform}, Python {sys.version.split()[0]}): fuera del venv del proyecto y "
                 f"sin poder ejecutar gcloud desde Python; {WSL_HINT}")
    py = sys.version_info
    missing, versions = [], []
    for mod in ("boto3", "botocore", "google.cloud.storage", "google.auth"):
        try:
            module = __import__(mod, fromlist=["__version__"])
            versions.append(f"{mod} {getattr(module, '__version__', '?')}")
        except Exception:  # noqa: BLE001
            missing.append(mod)
    try:
        import pyarrow  # noqa: F401

        arrow = "pyarrow OK"
    except Exception:  # noqa: BLE001
        arrow = "falta pyarrow (lo usa F)"
    status = FALLA if missing or (py.major, py.minor) != (3, 11) else (OK if arrow == "pyarrow OK" else AVISO)
    part.add("A1", "Python y dependencias", status,
             f"Python {py.major}.{py.minor}.{py.micro}; " + ", ".join(versions) + f"; {arrow}"
             + (f"; faltan: {', '.join(missing)}" if missing else ""))

    absent = [f for f in LOT_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(LOT_FILES)} presentes")

    absent = [f for f in PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A3", "archivos de lotes anteriores que usa el lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(PREVIOUS_FILES)} presentes")

    platform = (GCP_ENV / "platform.tf").read_text(encoding="utf-8") if (GCP_ENV / "platform.tf").exists() else ""
    exporter_tf = (GCP_ENV / "exporter.tf").read_text(encoding="utf-8") if (GCP_ENV / "exporter.tf").exists() else ""
    old = 'resource "google_storage_bucket_iam_member" "exporter_lake_read"' in platform
    new = '"exporter_lake_enriched"' in exporter_tf
    part.add("A4", "platform.tf sin el objectViewer amplio de sa-exporter", FALLA if old or not new else OK,
             "platform.tf todavía declara exporter_lake_read: copie el platform.tf del lote" if old
             else ("falta exporter_lake_enriched en exporter.tf" if not new else "lectura de sa-exporter limitada a enriched/"))

    tools = {}
    for tool in ("gcloud", "terraform", "aws", "bash", "docker"):
        tools[tool] = ctx.runner.which(tool)
    need = [t for t in ("gcloud", "terraform", "aws", "bash") if not tools[t]]
    tf_version = ""
    if tools["terraform"]:
        res = ctx.run(["terraform", "version", "-json"], timeout=60)
        data = parse_json(res.out) or {}
        tf_version = data.get("terraform_version", "")
    part.add("A5", "herramientas", FALLA if need else OK,
             ", ".join(f"{t}{' ' + tf_version if t == 'terraform' and tf_version else ''}" for t in tools if tools[t])
             + (f"; faltan: {', '.join(need)}" if need else "")
             + ("" if tools["docker"] else "; sin docker (solo lo usa build_exporter_image.sh docker)"))

    res = ctx.exporter("doctor", "--offline", stream=False, timeout=120)
    part.add("A6", "export_gcs_to_s3.py doctor --offline", OK if res.ok else FALLA, res.tail(8))

    gitignore = PROJECT / "infra" / ".gitignore"
    text = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
    part.add("A7", "*.auto.tfvars fuera de git", OK if "*.auto.tfvars" in text else AVISO,
             "infra/.gitignore los excluye" if "*.auto.tfvars" in text else
             "agregue *.auto.tfvars a infra/.gitignore (los escribe C y llevan su correo)")

    if tools["docker"]:
        res = ctx.run(["docker", "ps", "--format", "{{.Names}}"], timeout=60)
        names = [n for n in res.out.split() if n] if res.ok else []
        busy = [n for n in names if n.startswith("trendanalyzer-local") or "nlp" in n]
        if not res.ok:
            part.add("A8", "contenedores encendidos", OK, "Docker no responde (este lote no lo necesita)")
        elif busy:
            part.add("A8", "contenedores encendidos", AVISO,
                     f"encendidos: {', '.join(busy)}; no se usan aquí y comen CPU: bash project/scripts/local_dataflow.sh down-all")
        else:
            part.add("A8", "contenedores encendidos", OK, "ninguno del entorno local del stream ni de NLP")
    else:
        part.add("A8", "contenedores encendidos", OK, "sin docker")

    res = ctx.run([sys.executable, str(PROJECT / "scripts" / "status.py")], cwd=REPO, timeout=300)
    part.add("A9", "status.py (qué cobra)", OK if res.ok else AVISO, res.tail(14))
    jobs = ctx.gcloud("dataflow", "jobs", "list", f"--region={ctx.region}", "--status=active", "--format=value(name)", timeout=120)
    svcs = ctx.gcloud("run", "services", "list", f"--region={ctx.region}", "--format=value(name)", timeout=120)
    running = [n for n in jobs.out.split() if n] + [n for n in svcs.out.split() if n == "ta-stream-simulator"]
    state, _ = scheduler_state(ctx)
    detail = []
    unknown = [what for what, res in (("Dataflow", jobs), ("Cloud Run", svcs)) if not res.ok] + ([] if state else ["Scheduler"])
    if running:
        detail.append(f"encendido: {', '.join(running)} (cobra; este lote no lo necesita: python project/scripts/off.py)")
    detail.append(f"Scheduler del exportador: {state or 'sin dato'}")
    if unknown:
        # Sin respuesta de gcloud no se puede afirmar que nada cobra.
        detail.append(f"no se pudo comprobar: {', '.join(unknown)} (¿gcloud y sus credenciales en esta terminal?)")
    part.add("A10", "nube encendida", AVISO if running or state == "ENABLED" or unknown else OK, "\n".join(detail),
             running=running, scheduler=state, unknown=unknown)
    return part.finish()


# --- Parte B · pruebas ----------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    part.note("B1 tarda menos de un minuto")
    res = ctx.pytest("src/exporter", "-q", "-rs", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B1_EXPECTED and c["skipped"] == 0
    part.add("B1", "pytest src/exporter", OK if ok else FALLA,
             f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {B1_EXPECTED} passed, 0 skipped)"
             + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("src/ops/tests/test_ops_validate32.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B2_EXPECTED
    part.add("B2", "pytest test_ops_validate32.py", OK if ok else FALLA,
             f"{c['passed']} passed, {c['failed']} failed (esperado {B2_EXPECTED})" + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("contracts/tests", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    part.add("B3", "pytest contracts/tests", OK if res.ok and c["passed"] == 18 else FALLA, f"{c['passed']} passed (esperado 18)", **c)
    infra = PROJECT / "src/historical/tests/test_hist_infra.py"
    if infra.exists():
        res = ctx.pytest(str(infra.relative_to(PROJECT)), "-q", "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        part.add("B4", "regresión Terraform AWS del 3.1 (test_hist_infra.py)", OK if res.ok else FALLA,
                 f"{c['passed']} passed, {c['failed']} failed" + ("" if res.ok else "\n" + res.tail(10)), **c)
    else:
        part.add("B4", "regresión Terraform AWS del 3.1", AVISO, "no está src/historical/tests/test_hist_infra.py")
    return part.finish()


# --- Parte C · Terraform ----------------------------------------------------------------------


def write_aws_auto(ctx: Ctx, unique_id: str, email: str) -> Path:
    header = ("Lote 3.2 · generado por validate_3_2.py terraform (C0c). No se versiona.\n"
              "Datos de sa-exporter para la confianza de ta-gcp-exporter-role.")
    AWS_AUTO.write_text(render_tfvars(header, {
        "gcp_exporter_sa_unique_id": unique_id,
        "gcp_exporter_sa_email": email,
        "gcp_exporter_audience": AUDIENCE,
    }), encoding="utf-8")
    ctx.run(["terraform", "fmt", str(AWS_AUTO)], timeout=60)
    return AWS_AUTO


def write_gcp_auto(ctx: Ctx, outputs: dict[str, Any], account: str | None, image: str | None = None) -> Path:
    current = parse_tfvars(GCP_AUTO)
    values = {
        "exporter_role_arn": outputs.get("exporter_role_arn") or current.get("exporter_role_arn", ""),
        "exporter_s3_bucket": outputs.get("lake_bucket") or current.get("exporter_s3_bucket", ""),
        "exporter_kms_key_arn": outputs.get("kms_key_arn") or current.get("exporter_kms_key_arn", ""),
        "exporter_audience": outputs.get("exporter_oidc_audience") or current.get("exporter_audience", AUDIENCE),
        "exporter_aws_region": AWS_REGION,
        "exporter_oidc_token_creators": [f"user:{account}"] if account else current.get("exporter_oidc_token_creators", []),
        "exporter_image": image if image is not None else current.get("exporter_image", ""),
    }
    header = ("Lote 3.2 · generado por validate_3_2.py (C4b y E2). No se versiona.\n"
              "Valores de infra/aws (terraform output) y quién emite ID tokens de sa-exporter.")
    GCP_AUTO.write_text(render_tfvars(header, values), encoding="utf-8")
    ctx.run(["terraform", "fmt", str(GCP_AUTO)], timeout=60)
    return GCP_AUTO


def tf_plan(ctx: Ctx, root: Path, targets: list[str], plan_path: Path) -> tuple[Result, dict[str, list[str]] | None]:
    pending = errored_state_help(root)
    if pending:
        return Result(1, "", pending), None
    args = ["plan", "-input=false", "-lock-timeout=60s", f"-out={plan_path}", *[f"-target={t}" for t in targets]]
    res = ctx.tf(root, *args, timeout=1200, stream=True)
    if not res.ok:
        return Result(res.code, "", tf_error(res), res.seconds), None
    show = ctx.tf(root, "show", "-json", str(plan_path), timeout=600)
    data = parse_json(show.out) if show.ok else None
    if data is None:
        return Result(1, show.out, show.err or "terraform show -json no devolvió JSON"), None
    summary: dict[str, Any] = summarize_plan(data)
    summary["diffs"] = plan_diffs(data)
    return res, summary


def tf_apply(ctx: Ctx, root: Path, plan_path: Path) -> Result:
    """Aplica el plan guardado. Si falla, ``err`` trae el bloque ``Error:`` y, si Terraform no pudo
    guardar el estado, cómo subirlo antes de cualquier otro plan."""
    res = ctx.tf(root, "apply", "-input=false", "-lock-timeout=60s", str(plan_path), timeout=1800, stream=True)
    if res.ok:
        return res
    detail = tf_error(res)
    pending = errored_state_help(root)
    return Result(res.code, res.out, (pending + "\n" + detail) if pending else detail, res.seconds)


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (AWS y GCP)")
    only = getattr(ctx.options, "only", None)
    apply = bool(getattr(ctx.options, "apply", False))

    res = ctx.aws("sts", "get-caller-identity")
    ident = parse_json(res.out) or {}
    tf_account = tfvars_value(AWS_ENV / "terraform.tfvars", "aws_account_id")
    arn = ident.get("Arn", "")
    if not res.ok:
        part.add("C0", "identidad de AWS", FALLA, "aws sts get-caller-identity falló: " + res.tail(3))
        return part.finish()
    if tf_account and ident.get("Account") != tf_account:
        part.add("C0", "identidad de AWS", FALLA, f"el perfil apunta a {ident.get('Account')} y terraform.tfvars a {tf_account}")
        return part.finish()
    part.add("C0", "identidad de AWS", AVISO if arn.endswith(":root") else OK,
             f"{arn} (cuenta raíz: sirve para aplicar y verificar; el rol lo asume sa-exporter con su token, no este perfil)"
             if arn.endswith(":root") else arn, account=ident.get("Account"))

    missing = [str(p.relative_to(PROJECT)) for p in (AWS_ENV / "backend.hcl", AWS_ENV / "terraform.tfvars",
                                                     GCP_ENV / "backend.hcl", GCP_ENV / "terraform.tfvars") if not p.exists()]
    part.add("C0b", "backend.hcl y terraform.tfvars de las dos raíces", FALLA if missing else OK,
             "faltan: " + ", ".join(missing) if missing else "presentes (no se versionan)")
    if missing:
        return part.finish()

    acc = ctx.run(["gcloud", "config", "get-value", "account"], timeout=60)
    account = (acc.out or "").strip().splitlines()[-1].strip() if acc.ok and acc.out.strip() else None
    if account and ("unset" in account or "@" not in account):
        account = None
    res = ctx.gcloud("iam", "service-accounts", "describe", ctx.sa_email, "--format=json", timeout=120)
    sa = parse_json(res.out) or {}
    unique_id = str(sa.get("uniqueId") or "")
    if not res.ok or not unique_id:
        part.add("C0c", "sa-exporter y enlace con AWS", FALLA, f"no se pudo leer {ctx.sa_email}: " + res.tail(3))
        return part.finish()
    write_aws_auto(ctx, unique_id, sa.get("email") or ctx.sa_email)
    part.add("C0c", "sa-exporter y enlace con AWS", OK if account else AVISO,
             f"uniqueId {unique_id} → {AWS_AUTO.relative_to(PROJECT)}; cuenta de gcloud: {account or 'sin cuenta (D no podrá emitir el token)'}",
             unique_id=unique_id, account=account)

    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()

    if only in (None, "aws"):
        res = ctx.tf(AWS_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
        part.add("C2a", "terraform init (AWS, instala gcp_exporter_role)", OK if res.ok else FALLA, res.tail(4))
        if not res.ok:
            return part.finish()
        plan_path = EVIDENCE_DIR / "tfplan_3_2_aws"
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        res, summary = tf_plan(ctx, AWS_ENV, ["module.gcp_exporter_role"], plan_path)
        if summary is None:
            part.add("C2", "plan AWS", FALLA, res.err or res.tail(8))
            return part.finish()
        creates = set(summary["create"])
        extra = (creates - AWS_PLAN_CREATES) | set(summary["update"] + summary["delete"] + summary["replace"])
        if extra:
            part.add("C2", "plan AWS", FALLA, f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra),
                     **summary)
            return part.finish()
        if creates and creates != AWS_PLAN_CREATES:
            part.add("C2", "plan AWS", FALLA, f"{plan_line(summary)}; se esperaban {sorted(AWS_PLAN_CREATES)}", **summary)
            return part.finish()
        part.add("C2", "plan AWS", OK, plan_line(summary) + (" (ya aplicado)" if not creates else ""), **summary)
        if creates:
            if not apply:
                part.add("C3", "apply AWS", AVISO, "sin --apply: no se aplicó (repita con --apply)")
            elif ctx.ask("¿Aplicar el plan de AWS (2 recursos: rol y política)?"):
                res = tf_apply(ctx, AWS_ENV, plan_path)
                good = res.ok and "Apply complete! Resources: 2 added, 0 changed, 0 destroyed." in res.out
                part.add("C3", "apply AWS", OK if good else FALLA, res.tail(4) if res.ok else res.err)
                if not res.ok:
                    return part.finish()
            else:
                part.add("C3", "apply AWS", AVISO, "no confirmado")
        else:
            part.add("C3", "apply AWS", OK, "nada que aplicar")
        ctx.cache.pop("aws_outputs", None)

    outputs = aws_outputs(ctx, part)
    needed = ["lake_bucket", "kms_key_arn", "exporter_role_arn", "exporter_oidc_audience"]
    absent = [k for k in needed if not outputs.get(k)]
    part.add("C4", "outputs de infra/aws", AVISO if absent else OK,
             ("faltan: " + ", ".join(absent) + " (aplique C con --apply)") if absent else
             f"{outputs['exporter_role_arn']} · {outputs['lake_bucket']} · audiencia {outputs['exporter_oidc_audience']}",
             outputs={k: outputs.get(k) for k in needed})
    write_gcp_auto(ctx, outputs, account)
    part.add("C4b", "valores de AWS para el job", OK if not absent else AVISO,
             f"{GCP_AUTO.relative_to(PROJECT)} con el rol, el bucket, la llave y exporter_oidc_token_creators = user:{account}")

    if outputs.get("exporter_role_arn"):
        res = ctx.aws("iam", "get-role", "--role-name", ROLE)
        role = (parse_json(res.out) or {}).get("Role", {})
        problems = trust_problems(role.get("AssumeRolePolicyDocument") or {}, unique_id, sa.get("email") or ctx.sa_email,
                                  outputs.get("exporter_oidc_audience") or AUDIENCE) if res.ok else [res.tail(3)]
        res2 = ctx.aws("iam", "get-role-policy", "--role-name", ROLE, "--policy-name", f"{ROLE}-s3")
        pol = parse_json(res2.out) or {}
        actions, resources = policy_actions(pol.get("PolicyDocument") or {}) if res2.ok else (set(), [])
        if actions != {"s3:PutObject", "s3:ListBucket", "kms:GenerateDataKey"}:
            problems.append(f"acciones {sorted(actions)}")
        bucket = outputs.get("lake_bucket")
        if f"arn:aws:s3:::{bucket}/raw/origin=stream/*" not in resources:
            problems.append(f"PutObject no está limitado a raw/origin=stream/ ({resources})")
        part.add("C5", "confianza y política del rol (iam get-role)", FALLA if problems else OK,
                 "\n".join(problems) if problems else "Federated accounts.google.com; aud/oaud/sub de sa-exporter; "
                 "PutObject y ListBucket solo en raw/origin=stream/, GenerateDataKey vía S3")
        cases = simulate_cases(outputs)
        bad = []
        for label, action, resource, context, expected in cases:
            args = ["iam", "simulate-principal-policy", "--policy-source-arn", outputs["exporter_role_arn"],
                    "--action-names", action, "--resource-arns", resource]
            for key, value in context:
                args += ["--context-entries", f"ContextKeyName={key},ContextKeyValues={value},ContextKeyType=string"]
            r = ctx.aws(*args)
            decision = ((parse_json(r.out) or {}).get("EvaluationResults") or [{}])[0].get("EvalDecision")
            if (decision == "allowed") != expected:
                bad.append(f"{label}: {decision}")
        part.add("C5b", f"política simulada ({len(cases)} casos)", FALLA if bad else OK,
                 "\n".join(bad) if bad else f"{len(cases)} de {len(cases)} como se espera (3 permitidos, {len(cases) - 3} denegados)")
    else:
        part.add("C5", "confianza y política del rol", OMITIDO, "el rol todavía no existe")

    if only in (None, "gcp"):
        res = ctx.tf(GCP_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
        part.add("C6a", "terraform init (GCP, instala cloud_run_exporter)", OK if res.ok else FALLA, res.tail(4))
        if not res.ok:
            return part.finish()
        plan_path = EVIDENCE_DIR / "tfplan_3_2_gcp"
        res, summary = tf_plan(ctx, GCP_ENV, GCP_BASE_TARGETS, plan_path)
        if summary is None:
            part.add("C6", "plan GCP (permisos de sa-exporter)", FALLA, res.err or res.tail(8))
            return part.finish()
        creators = parse_tfvars(GCP_AUTO).get("exporter_oidc_token_creators") or []
        allowed_create = {"google_storage_bucket_iam_member.exporter_lake_enriched"} | {
            f'google_service_account_iam_member.exporter_oidc_token_creator["{m}"]' for m in creators}
        allowed_delete = {"google_storage_bucket_iam_member.exporter_lake_read"}
        extra = (set(summary["create"]) - allowed_create) | (set(summary["delete"]) - allowed_delete) | set(
            summary["update"] + summary["replace"])
        if extra:
            part.add("C6", "plan GCP (permisos de sa-exporter)", FALLA,
                     f"{plan_line(summary)}; no aplique: {sorted(extra)}" + diff_text(summary, extra), **summary)
            return part.finish()
        changes = summary["create"] + summary["delete"]
        part.add("C6", "plan GCP (permisos de sa-exporter)", OK,
                 plan_line(summary) + (" (ya aplicado)" if not changes else
                                       "\nesperado: crear exporter_lake_enriched y el permiso de token; destruir exporter_lake_read"),
                 **summary)
        applied = not changes
        if changes:
            if not apply:
                part.add("C7", "apply GCP", AVISO, "sin --apply: no se aplicó")
            elif ctx.ask(f"¿Aplicar el plan de GCP ({plan_line(summary)})?"):
                res = tf_apply(ctx, GCP_ENV, plan_path)
                applied = res.ok and "Apply complete!" in res.out
                part.add("C7", "apply GCP", OK if applied else FALLA, res.tail(4) if res.ok else res.err)
                if not res.ok:
                    return part.finish()
            else:
                part.add("C7", "apply GCP", AVISO, "no confirmado")
        else:
            part.add("C7", "apply GCP", OK, "nada que aplicar")
        if applied:
            part.add(*check_bucket_iam(ctx, account))
        else:
            # El IAM real sigue siendo el de antes del plan: compararlo daría una falla falsa.
            part.add("C8", "IAM real del lake y de sa-exporter", OMITIDO, "pendiente: el plan de GCP no se aplicó (repita con --apply)")
    return part.finish()


def simulate_cases(outputs: dict[str, Any]) -> list[tuple[str, str, str, list[tuple[str, str]], bool]]:
    bucket = outputs.get("lake_bucket")
    key = outputs.get("kms_key_arn")
    obj = f"arn:aws:s3:::{bucket}/raw/origin=stream/source=reddit/dt=2026-10-24/hr=12/part-x.parquet"
    hist = f"arn:aws:s3:::{bucket}/raw/origin=historical/source=reddit/dt=2026-10-24/hr=12/part-x.parquet"
    via = [("kms:ViaService", f"s3.{AWS_REGION}.amazonaws.com")]
    return [
        ("put raw/origin=stream/", "s3:PutObject", obj, [], True),
        ("list raw/origin=stream/", "s3:ListBucket", f"arn:aws:s3:::{bucket}", [("s3:prefix", "raw/origin=stream/source=reddit/")], True),
        ("kms:GenerateDataKey vía S3", "kms:GenerateDataKey", str(key), via, True),
        ("put raw/origin=historical/", "s3:PutObject", hist, [], False),
        ("get raw/origin=stream/", "s3:GetObject", obj, [], False),
        ("delete raw/origin=stream/", "s3:DeleteObject", obj, [], False),
        ("list trusted/", "s3:ListBucket", f"arn:aws:s3:::{bucket}", [("s3:prefix", "trusted/")], False),
        ("kms:Decrypt", "kms:Decrypt", str(key), via, False),
    ]


def check_bucket_iam(ctx: Ctx, account: str | None) -> tuple[str, str, str, str]:
    member = f"serviceAccount:{ctx.sa_email}"
    res = ctx.gcloud("storage", "buckets", "get-iam-policy", f"gs://{ctx.lake_bucket}", "--format=json", timeout=120)
    policy = parse_json(res.out) or {}
    if not res.ok or not policy.get("bindings"):
        tok = ctx.run(["gcloud", "auth", "print-access-token"], timeout=60)
        if tok.ok and tok.out.strip():
            url = f"https://storage.googleapis.com/storage/v1/b/{ctx.lake_bucket}/iam?optionsRequestedPolicyVersion=3"
            res = ctx.run(["curl", "-sS", "-H", f"Authorization: Bearer {tok.out.strip().splitlines()[-1]}", url], timeout=60)
            policy = parse_json(res.out) or {}
    problems = []
    found_read = found_state = False
    for binding in policy.get("bindings", []) or []:
        if member not in (binding.get("members") or []):
            continue
        cond = (binding.get("condition") or {}).get("expression", "")
        role = binding.get("role")
        if role in ("roles/storage.objectViewer", "roles/storage.objectUser", "roles/storage.objectAdmin") and not cond:
            problems.append(f"{role} sin condición")
        if role == "roles/storage.objectViewer" and "/objects/enriched/" in cond:
            found_read = True
        if role == "roles/storage.objectUser" and "/objects/export-state/" in cond:
            found_state = True
    if not found_read:
        problems.append("no está el objectViewer con condición a enriched/")
    if not found_state:
        problems.append("no está el objectUser con condición a export-state/")
    if account:
        r = ctx.gcloud("iam", "service-accounts", "get-iam-policy", ctx.sa_email, "--format=json", timeout=120)
        sa_policy = parse_json(r.out) or {}
        ok = any(b.get("role") == "roles/iam.serviceAccountOpenIdTokenCreator" and f"user:{account}" in (b.get("members") or [])
                 for b in sa_policy.get("bindings", []) or [])
        if not ok:
            problems.append(f"user:{account} no tiene serviceAccountOpenIdTokenCreator sobre sa-exporter")
    if not res.ok:
        problems = ["no se pudo leer la política del bucket: " + res.tail(3)]
    return ("C8", "IAM real del lake y de sa-exporter", FALLA if problems else OK,
            "\n".join(problems) if problems else "sa-exporter: lee solo enriched/ y escribe solo export-state/; token OIDC para su cuenta")


# --- Parte D · federación desde la máquina del equipo ----------------------------------------


def part_federation(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Federación real desde su máquina (sin Cloud Run)")
    outputs = aws_outputs(ctx, part)
    if not outputs.get("exporter_role_arn"):
        part.add("D0", "rol de AWS", FALLA, "no hay exporter_role_arn: corra C con --apply")
        return part.finish()
    env = exporter_env(ctx, outputs)
    unique_id = ctx.step_data("C", "C0c").get("unique_id")
    if not unique_id:
        r = ctx.gcloud("iam", "service-accounts", "describe", ctx.sa_email, "--format=value(uniqueId)", timeout=120)
        unique_id = r.out.strip() if r.ok else None
    part.add("D0", "configuración del exportador", OK, f"rol {env['TA_EXPORTER_ROLE_ARN']} · bucket {env['TA_LAKE_BUCKET']}")

    token_json = EVIDENCE_DIR / "exporter_token_3_2.json"
    args = ["token", "--token-source", "impersonate", "--trust-from-aws", "--json-out", str(token_json)]
    if unique_id:
        args += ["--unique-id", unique_id]
    res = None
    for attempt in range(4):
        res = ctx.exporter(*args, env=env, stream=False, timeout=180)
        if res.code != 2 or not re.search(r"403|PERMISSION_DENIED|denied", res.text, flags=re.I) or attempt == 3:
            break
        part.note("el permiso de token todavía no se propaga; reintento en 30 s…")
        ctx.sleep(30)
    data = load_json_file(token_json)
    claims = data.get("claims", {})
    detail = (f"azp={claims.get('azp')} sub={claims.get('sub')} aud={claims.get('aud')} email={claims.get('email')}"
              if claims else res.tail(5))
    problems = (data.get("problems") or []) + (data.get("trust_problems") or [])
    part.add("D1", "ID token de sa-exporter frente a la confianza del rol", OK if res.ok else FALLA,
             detail + ("\n" + "\n".join(problems) if problems else ""), claims=claims, problems=problems)
    if not res.ok:
        return part.finish()

    probe_json = EVIDENCE_DIR / "exporter_probe_local_3_2.json"
    res = ctx.exporter("probe", "--no-gcs", "--token-source", "impersonate", "--json-out", str(probe_json), env=env, stream=False, timeout=300)
    probe = load_json_file(probe_json)
    failed = [f"{r['id']} {r['what']}: {r['got']} (esperado {r['expect']})" for r in probe.get("results", []) if not r.get("ok")]
    part.add("D2", "AssumeRoleWithWebIdentity real y sondas A1–A7", OK if res.ok and not failed else FALLA,
             (probe.get("aws", {}).get("assumed_arn", "") + "\n" + "\n".join(failed)).strip() or res.tail(5),
             passed=probe.get("passed"), total=probe.get("total"))
    if not res.ok:
        return part.finish()

    plan_json = EVIDENCE_DIR / "exporter_plan_d_3_2.json"
    res = ctx.exporter("plan", "--token-source", "impersonate", "--state-key", VALIDATION_STATE, "--json-out", str(plan_json),
                       env=env, stream=False, timeout=900)
    plan = load_json_file(plan_json)
    to_upload = int(plan.get("to_upload") or 0)
    part.add("D3", "plan (sin escribir): lo que hay para exportar", OK if res.ok and plan.get("listed") else (AVISO if res.ok else FALLA),
             f"{plan.get('listed', 0)} objetos en enriched/, {plan.get('candidates', 0)} candidatos, {to_upload} por subir "
             f"en {plan.get('duration_s', '?')} s" if res.ok else res.tail(5),
             listed=plan.get("listed"), to_upload=to_upload, duration_s=plan.get("duration_s"))
    if not res.ok or not plan.get("listed"):
        if res.ok:
            part.note("enriched/ está vacío: hace falta que el stream haya corrido alguna vez (Lote 2.2+)")
        return part.finish()

    run_json = EVIDENCE_DIR / "exporter_run_d_3_2.json"
    limit = int(getattr(ctx.options, "limit", None) or 20)
    res = ctx.exporter("run", "--token-source", "impersonate", "--state-key", VALIDATION_STATE, "--max-objects", str(limit),
                       "--json-out", str(run_json), env=env, stream=False, timeout=900)
    run = load_json_file(run_json)
    expected = min(limit, to_upload)
    done = run.get("uploaded", 0) + run.get("overwritten", 0)
    # Con el stream encendido pueden aparecer archivos nuevos entre D3 y D4: basta con >= .
    good = res.ok and expected <= done <= limit and not run.get("failed_count")
    part.add("D4", f"run limitado a {limit} objetos con el rol asumido", OK if good else FALLA,
             f"{run.get('uploaded', 0)} subidos, {run.get('failed_count', 0)} fallidos, {run.get('bytes_uploaded', 0)} bytes "
             f"en {run.get('duration_s', '?')} s (esperado {expected})" if run else res.tail(5),
             uploaded=run.get("uploaded"), duration_s=run.get("duration_s"), keys=run.get("uploaded_keys", [])[:limit])
    if not good:
        return part.finish()

    if done == 0:
        part.add("D5", "en S3 con SSE-KMS, llave del lake y el CRC32C de GCS", OMITIDO, "D4 no subió nada (ya estaba todo en S3)")
        part.add("D6", "idempotencia", OMITIDO, "nada nuevo que comparar")
    else:
        verify_json = EVIDENCE_DIR / "exporter_verify_d_3_2.json"
        res = ctx.exporter("verify", "--keys-from", str(run_json), "--sample", str(limit), "--state-key", VALIDATION_STATE,
                           "--json-out", str(verify_json), env=env, stream=False, timeout=900)
        ver = load_json_file(verify_json)
        v4 = next((c for c in ver.get("checks", []) if c.get("id") == "V4"), {})
        v1 = next((c for c in ver.get("checks", []) if c.get("id") == "V1"), {})
        good = res.ok and v4.get("status") == OK and v1.get("status") == OK
        part.add("D5", "en S3 con SSE-KMS, llave del lake y el CRC32C de GCS", OK if good else FALLA,
                 f"V1 {v1.get('status')}: {v1.get('detail')}\nV4 {v4.get('status')}: {v4.get('detail')}" if ver else res.tail(5))

        res = ctx.exporter("plan", "--token-source", "impersonate", "--state-key", VALIDATION_STATE_IDEM, "--json-out", str(plan_json),
                           env=env, stream=False, timeout=900)
        again = load_json_file(plan_json)
        uploaded_keys = set(run.get("uploaded_keys") or [])
        still = uploaded_keys & set(again.get("planned_keys") or [])
        good = res.ok and not still and int(again.get("skipped_same_size") or 0) >= done
        part.add("D6", "idempotencia: lo subido en D4 ya no está por subir", OK if good else FALLA,
                 f"por subir {to_upload} → {again.get('to_upload')}; omitidos por mismo tamaño: {again.get('skipped_same_size')}"
                 + (f"; siguen planeados: {sorted(still)[:3]}" if still else ""))

    res = ctx.gcloud("storage", "rm", f"gs://{ctx.lake_bucket}/{VALIDATION_STATE}", timeout=120)
    part.add("D7", "borrar el watermark de prueba", OK if res.ok else AVISO,
             f"gs://{ctx.lake_bucket}/{VALIDATION_STATE}" + ("" if res.ok else ": " + res.tail(2)))
    return part.finish()


# --- Parte E · Cloud Run Job y Scheduler ------------------------------------------------------


def latest_image_from_build(text: str) -> str | None:
    m = re.findall(r'^exporter_image = "([^"]+)"', text or "", flags=re.M)
    return m[-1] if m else None


def part_job(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Cloud Run Job y Cloud Scheduler")
    t_start = ctx.now()
    outputs = aws_outputs(ctx, part)
    current = parse_tfvars(GCP_AUTO)
    if not current.get("exporter_role_arn") or not outputs.get("exporter_role_arn"):
        part.add("E0", "valores de AWS", FALLA, f"falta exporter_role_arn en {GCP_AUTO.name}: corra C con --apply")
        return part.finish()
    part.add("E0", "valores de AWS", OK, f"{GCP_AUTO.relative_to(PROJECT)}: {current.get('exporter_role_arn')}")

    image = getattr(ctx.options, "image", None)
    if image:
        part.add("E1", "imagen", OK, f"{image} (--image)")
    elif getattr(ctx.options, "skip_build", False):
        image = current.get("exporter_image") or None
        part.add("E1", "imagen", OK if image else FALLA, image or "--skip-build sin exporter_image en el auto.tfvars")
    else:
        part.note("construyendo la imagen en Cloud Build (pruebas + imagen; 3–8 min)…")
        res = ctx.run(["bash", str(BUILD_SCRIPT), "cloudbuild"], cwd=PROJECT, timeout=2400, stream=True)
        image = latest_image_from_build(res.out) if res.ok else None
        part.add("E1", "build_exporter_image.sh cloudbuild", OK if image else FALLA,
                 f"{image} en {res.seconds:.0f} s" if image else res.tail(8), image=image, seconds=round(res.seconds, 1))
    if not image:
        return part.finish()

    write_gcp_auto(ctx, outputs, None, image=image)
    plan_path = EVIDENCE_DIR / "tfplan_3_2_job"
    res, summary = tf_plan(ctx, GCP_ENV, ["module.exporter_job"], plan_path)
    if summary is None:
        part.add("E2", "plan del job y el Scheduler", FALLA, res.err or res.tail(8))
        return part.finish()
    creates, updates = set(summary["create"]), set(summary["update"])
    job_addr = "module.exporter_job[0].google_cloud_run_v2_job.this"
    extra = (creates - GCP_JOB_CREATES) | (updates - {job_addr}) | set(summary["delete"] + summary["replace"])
    if extra:
        part.add("E2", "plan del job y el Scheduler", FALLA,
                 f"{plan_line(summary)}; no aplique: {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    part.add("E2", "plan del job y el Scheduler", OK,
             plan_line(summary) + (" (ya aplicado con esta imagen)" if not creates and not updates else "")
             + diff_text(summary, updates), **summary)
    if creates or updates:
        if not ctx.ask(f"¿Aplicar el plan del job ({plan_line(summary)})?"):
            part.add("E3", "apply", AVISO, "no confirmado")
            return part.finish()
        res = tf_apply(ctx, GCP_ENV, plan_path)
        part.add("E3", "apply", OK if res.ok else FALLA, res.tail(4) if res.ok else res.err)
        if not res.ok:
            return part.finish()
    else:
        part.add("E3", "apply", OK, "nada que aplicar")

    state, sched = scheduler_state(ctx)
    jres = ctx.gcloud("run", "jobs", "describe", JOB, f"--region={ctx.region}", "--format=json", timeout=120)
    job = parse_json(jres.out) or {}
    job_text = json.dumps(job)
    sa_ok = ctx.sa_email in job_text
    part.add("E4", "estado de ahorro y el job desplegado", OK if state == "PAUSED" and sa_ok and image in job_text else AVISO,
             f"Scheduler {state} ({sched.get('schedule', '?')} {sched.get('timeZone', '')}); job con {ctx.sa_email if sa_ok else '¿otra SA?'} "
             f"e imagen {'correcta' if image in job_text else 'distinta'}", scheduler=state)

    probe = execute_job(ctx, part, "E5", "sonda", args=["probe"])
    payload = job_log_event(ctx, probe["execution"], "probe_result") if probe["execution"] else None
    failed = [f"{r['id']} {r['what']}: {r['got']} (esperado {r['expect']})" for r in (payload or {}).get("results", []) if not r.get("ok")]
    good = probe["ok"] and payload is not None and payload.get("ok") and not failed
    part.add("E5", "sonda como sa-exporter en Cloud Run (G1–G6, A1–A7)", OK if good else FALLA,
             (f"{payload.get('passed')}/{payload.get('total')} · {probe['execution']}" if payload else
              f"sin probe_result en los logs de {probe['execution']}\n{probe['tail']}") + ("\n" + "\n".join(failed) if failed else ""),
             execution=probe["execution"])
    if not good:
        return part.finish()

    durations: list[float] = []

    def export_step(sid: str, title: str, expect: Callable[[dict[str, Any]], tuple[bool, str]]) -> dict[str, Any] | None:
        ex = execute_job(ctx, part, sid, title)
        done = job_log_event(ctx, ex["execution"], "export_done") if ex["execution"] else None
        if ex["duration_s"]:
            durations.append(ex["duration_s"])
        if not done:
            part.add(sid, title, FALLA, f"sin export_done en los logs de {ex['execution']}\n{ex['tail']}")
            return None
        good, why = expect(done)
        good = good and ex["ok"] and done.get("ok", False)
        part.add(sid, title, OK if good else FALLA,
                 f"{done.get('uploaded', 0)} subidos, {done.get('skipped_same_size', 0)} omitidos, {done.get('failed', 0)} fallidos, "
                 f"{done.get('bytes_uploaded', 0)} bytes; ejecución {ex['duration_s'] or '?'} s ({ex['execution']}) · {why}",
                 execution=ex["execution"], duration_s=ex["duration_s"], **{k: done.get(k) for k in
                 ("uploaded", "skipped_same_size", "failed", "bytes_uploaded", "candidates", "listed", "watermark_after")})
        return done

    first = export_step("E6", "primera ejecución (el atraso de enriched/)",
                        lambda d: (d.get("failed", 1) == 0, f"watermark {d.get('watermark_after')}"))
    if first is None or first.get("failed"):
        return part.finish()

    outputs_env = exporter_env(ctx, outputs)
    verify_json = EVIDENCE_DIR / "exporter_verify_3_2.json"
    res = ctx.exporter("verify", "--sample", "50", "--json-out", str(verify_json), env=outputs_env, stream=False, timeout=1200)
    ver = load_json_file(verify_json)
    checks = {c["id"]: c for c in ver.get("checks", [])}
    good = res.ok and ver.get("status") in (OK, AVISO) and checks.get("V1", {}).get("status") == OK
    part.add("E7", "verify: GCS enriched/ frente a S3 raw/origin=stream/", OK if good and ver.get("status") == OK else (AVISO if good else FALLA),
             "\n".join(f"{c['id']} {c['status']}: {c['detail']}" for c in ver.get("checks", [])) or res.tail(5),
             gcs_enriched=ver.get("gcs_enriched"), s3_stream=ver.get("s3_stream"))

    jobs = ctx.gcloud("dataflow", "jobs", "list", f"--region={ctx.region}", "--status=active", "--format=value(name)", timeout=120)
    stream_on = bool(jobs.out.strip())

    pending_after_first = int(first.get("pending") or 0)

    def second(d: dict[str, Any]) -> tuple[bool, str]:
        if d.get("overwritten", 0):
            return False, "sobrescribió objetos que ya estaban"
        if stream_on:
            return True, "el stream está encendido: lo subido es lo nuevo de enriched/"
        if pending_after_first:
            ok = d.get("uploaded", 0) <= pending_after_first
            return ok, f"E6 cortó en el máximo por corrida y dejó {pending_after_first} pendientes: siguen aquí"
        return d.get("uploaded", 1) == 0, "idempotente" if d.get("uploaded", 1) == 0 else "subió objetos sin que el stream corriera"

    export_step("E8", "segunda ejecución (no vuelve a subir lo exportado)", second)

    if not getattr(ctx.options, "no_canary", False):
        canary_step(ctx, part, outputs, export_step)
    else:
        part.add("E9", "canario incremental", OMITIDO, "--no-canary")

    scheduler_step(ctx, part, durations)

    flt = (f'resource.type="cloud_run_job" AND resource.labels.job_name="{JOB}" AND severity>=ERROR '
           f'AND timestamp>="{t_start.strftime("%Y-%m-%dT%H:%M:%SZ")}"')
    res = ctx.gcloud("logging", "read", flt, "--limit=20", "--format=json", timeout=120)
    entries = parse_json(res.out) or []
    sample = [((e.get("jsonPayload") or {}).get("event") or e.get("textPayload", "")[:120]) for e in entries[:5]]
    part.add("E11", "logs del job con severidad ERROR desde el inicio de E", OK if res.ok and not entries else (AVISO if res.ok else FALLA),
             f"{len(entries)} entradas" + (f": {sample}" if sample else ""))

    if durations:
        per_run = [cost_per_run(d) for d in durations]
        avg = sum(per_run) / len(per_run)
        part.add("E12", "costo estimado con el Scheduler encendido", OK,
                 f"ejecuciones medidas: {', '.join(f'{d:.0f} s' for d in durations)} → se cobra ≥ 60 s cada una; "
                 f"≈ {avg:.5f} USD por ejecución, ≈ {avg * RUNS_PER_DAY:.2f} USD/día (96 al día; sin nivel gratuito)",
                 durations=durations, usd_per_run=round(avg, 6), usd_per_day=round(avg * RUNS_PER_DAY, 4))
    return part.finish()


def canary_step(ctx: Ctx, part: Part, outputs: dict[str, Any], export_step: Callable[..., Any]) -> None:
    keys = ctx.step_data("D", "D4").get("keys") or []
    source = None
    if keys:
        source = keys[0].replace("raw/origin=stream/", "enriched/", 1)
    else:
        res = ctx.gcloud("storage", "ls", f"gs://{ctx.lake_bucket}/enriched/source=reddit/", timeout=120)
        dts = sorted(ln.strip() for ln in res.out.splitlines() if "/dt=" in ln)
        if dts:
            hrs = ctx.gcloud("storage", "ls", dts[-1], timeout=120)
            hr_dirs = sorted(ln.strip() for ln in hrs.out.splitlines() if "/hr=" in ln)
            if hr_dirs:
                files = ctx.gcloud("storage", "ls", hr_dirs[-1], timeout=120)
                parts = sorted(ln.strip() for ln in files.out.splitlines() if ln.strip().endswith(".parquet"))
                if parts:
                    source = parts[0].replace(f"gs://{ctx.lake_bucket}/", "")
    if not source:
        part.add("E9", "canario incremental", AVISO, "no encontré un Parquet de enriched/ para copiar")
        return
    now = ctx.now()
    canary = f"enriched/source=reddit/dt={now:%Y-%m-%d}/hr={now:%H}/part-{now:%Y%m%dT%H%M%S}Z-p999.parquet"
    s3_key = canary.replace("enriched/", "raw/origin=stream/", 1)
    res = ctx.gcloud("storage", "cp", f"gs://{ctx.lake_bucket}/{source}", f"gs://{ctx.lake_bucket}/{canary}", timeout=180)
    if not res.ok:
        part.add("E9", "canario incremental", FALLA, "no se pudo copiar el canario: " + res.tail(3))
        return
    record = {"gcs": canary, "s3": s3_key, "cleaned": False}
    ev = ctx.load_evidence()
    ev.setdefault("canaries", []).append(record)
    ctx.evidence_path.write_text(json.dumps(ev, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    done = export_step("E9", "canario: un archivo nuevo en enriched/ sale en la siguiente ejecución",
                       lambda d: (d.get("uploaded", 0) >= 1, f"canario {canary.rsplit('/', 1)[-1]}"))
    head = ctx.aws("s3api", "head-object", "--bucket", str(outputs.get("lake_bucket")), "--key", s3_key, "--checksum-mode", "ENABLED")
    meta = parse_json(head.out) or {}
    kms_ok = meta.get("ServerSideEncryption") == "aws:kms" and meta.get("SSEKMSKeyId") == outputs.get("kms_key_arn")
    part.add("E9b", "el canario en S3", OK if head.ok and kms_ok else FALLA,
             f"s3://{outputs.get('lake_bucket')}/{s3_key}: {meta.get('ServerSideEncryption')} {meta.get('SSEKMSKeyId')}" if head.ok else head.tail(3))
    cleaned = cleanup_canary(ctx, outputs, record)
    part.add("E9c", "limpiar el canario (GCS y S3)", OK if cleaned else AVISO,
             "borrado de los dos lados" if cleaned else "no se pudo borrar: repita `validate_3_2.py off`")
    _ = done


def cleanup_canary(ctx: Ctx, outputs: dict[str, Any], record: dict[str, Any]) -> bool:
    r1 = ctx.gcloud("storage", "rm", f"gs://{ctx.lake_bucket}/{record['gcs']}", timeout=120)
    r2 = ctx.aws("s3", "rm", f"s3://{outputs.get('lake_bucket')}/{record['s3']}")
    gone_gcs = r1.ok or "not found" in r1.text.lower() or "No URLs matched" in r1.text
    gone_s3 = r2.ok
    ok = gone_gcs and gone_s3
    ev = ctx.load_evidence()
    for item in ev.get("canaries", []):
        if item.get("gcs") == record["gcs"]:
            item["cleaned"] = ok
    ctx.evidence_path.write_text(json.dumps(ev, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return ok


def scheduler_step(ctx: Ctx, part: Part, durations: list[float]) -> None:
    if not ctx.ask("¿Reanudar el Scheduler para probar un disparo real? (se vuelve a pausar al final)"):
        part.add("E10", "disparo del Scheduler", AVISO, "no confirmado: el Scheduler sigue en pausa")
        return
    before = {execution_name(e) for e in (list_executions(ctx) or [])}
    t0 = ctx.now()
    try:
        res = ctx.gcloud("scheduler", "jobs", "resume", SCHEDULER, f"--location={ctx.region}", timeout=120)
        if not res.ok:
            part.add("E10", "disparo del Scheduler", FALLA, "no se pudo reanudar: " + res.tail(3))
            return
        wait_tick = bool(getattr(ctx.options, "wait_tick", False))
        if not wait_tick:
            res = ctx.gcloud("scheduler", "jobs", "run", SCHEDULER, f"--location={ctx.region}", timeout=120)
            if not res.ok:
                part.add("E10", "disparo del Scheduler", FALLA, "gcloud scheduler jobs run falló: " + res.tail(3))
                return
        limit = 18 * 60 if wait_tick else 6 * 60
        part.note(("esperando el próximo disparo programado (hasta 18 min)…" if wait_tick else
                   "disparo forzado con `gcloud scheduler jobs run`; esperando la ejecución…"))
        new_exec: dict[str, Any] | None = None
        for i in range(limit // 20 + 1):
            execs = list_executions(ctx) or []
            fresh = [e for e in execs if execution_name(e) not in before]
            if fresh:
                new_exec = fresh[0]
                if not execution_running(new_exec):
                    break
            if i and i % 6 == 0:
                part.note(f"sigue esperando ({i * 20 // 60} min)…")
            ctx.sleep(20)
        if new_exec is None:
            part.add("E10", "disparo del Scheduler", FALLA, f"no apareció ninguna ejecución en {limit // 60} min")
            return
        name = execution_name(new_exec)
        start, end = execution_times(new_exec)
        if start and end:
            durations.append((end - start).total_seconds())
        succeeded = execution_succeeded(new_exec)
        creator = json.dumps(new_exec.get("metadata", {}).get("annotations", {}))
        by_sa = ctx.sa_email in creator or "sa-exporter" in creator
        part.add("E10", "disparo del Scheduler → ejecución con sa-exporter", OK if succeeded else FALLA,
                 f"{name}: {'terminó bien' if succeeded else 'no terminó bien'}; creada por "
                 f"{'sa-exporter (Scheduler)' if by_sa else 'otra identidad'} "
                 f"{(execution_created(new_exec) or t0).strftime('%H:%M:%S')} UTC", execution=name)
    finally:
        res = ctx.gcloud("scheduler", "jobs", "pause", SCHEDULER, f"--location={ctx.region}", timeout=120)
        state, _ = scheduler_state(ctx)
        part.add("E10b", "el Scheduler vuelve a la pausa", OK if state == "PAUSED" else FALLA, f"estado {state}")


# --- Parte F · ambos orígenes ----------------------------------------------------------------


def part_lake(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Ambos orígenes en S3 (Listo cuando de la Fase 3)")
    outputs = aws_outputs(ctx, part)
    env = exporter_env(ctx, outputs)
    report_json = EVIDENCE_DIR / "lake_report_3_2.json"
    part.note("cuenta los objetos de raw/ en S3 (unos 12.000 del histórico; 1–2 min)…")
    res = ctx.exporter("lake-report", "--aws-auth", "profile", "--json-out", str(report_json), env=env, stream=False, timeout=1200)
    rep = load_json_file(report_json)
    origins = rep.get("origins", {})
    hist = origins.get("historical", {})
    stream = origins.get("stream", {})
    part.add("F1", "raw/origin=historical/", OK if hist.get("objects") == HISTORICAL_OBJECTS else (AVISO if hist.get("objects") else FALLA),
             f"{hist.get('objects', 0)} objetos ({hist.get('bytes', 0) / 1e6:.1f} MB; esperado {HISTORICAL_OBJECTS} del 3.1) · "
             + ", ".join(f"{s} {v['objects']}" for s, v in hist.get("by_source", {}).items()))
    ver = load_json_file(EVIDENCE_DIR / "exporter_verify_3_2.json")
    gcs_n = ver.get("gcs_enriched")
    part.add("F2", "raw/origin=stream/", OK if stream.get("objects") else FALLA,
             f"{stream.get('objects', 0)} objetos ({stream.get('bytes', 0) / 1e6:.1f} MB) · "
             + ", ".join(f"{s} {v['objects']}" for s, v in stream.get("by_source", {}).items())
             + (f" · en GCS enriched/ al verificar (E7): {gcs_n}" if gcs_n is not None else ""))
    problems = []
    for origin, info in origins.items():
        sample = info.get("sample", {})
        problems.extend(f"{origin}: {p}" for p in sample.get("problems", []))
    samples = "; ".join(f"{o}: {i.get('sample', {}).get('rows', '?')} filas, {i.get('sample', {}).get('columns', '?')} columnas"
                        for o, i in origins.items())
    part.add("F3", "esquema de un Parquet de cada origen", FALLA if problems else (OK if rep.get("columns_source", "").startswith("common.") else AVISO),
             (samples + f" (frente a {rep.get('columns_source')})") + ("\n" + "\n".join(problems) if problems else ""))
    both = bool(hist.get("objects")) and bool(stream.get("objects")) and not problems and res.ok
    part.add("F4", "Fase 3 · Listo cuando: datos de ambos orígenes en s3://…/raw/", OK if both else FALLA,
             "historical y stream con las mismas columnas de posts_enriched" if both else "falta un origen o hay problemas")
    cost = ctx.step_data("E", "E12")
    if cost:
        part.add("F5", "costo del exportador (de E12)", OK,
                 f"≈ {cost.get('usd_per_day')} USD/día con el Scheduler encendido; pausado: 0 USD de Cloud Run "
                 "(el Scheduler pausado cuenta como job: 0,10 USD/mes si pasa de los 3 gratuitos de la cuenta)")
    return part.finish()


# --- status y off -------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del exportador (solo lectura)")
    state, sched = scheduler_state(ctx)
    part.add("S1", "Scheduler", OK if state in ("PAUSED", "NO_EXISTE") else AVISO,
             f"{state}" + (f" · próximo disparo {sched.get('scheduleTime')}" if state == "ENABLED" else "")
             + (" (cobra ~0,1 USD/día mientras está encendido)" if state == "ENABLED" else ""))
    execs = list_executions(ctx, 10)
    running = [execution_name(e) for e in (execs or []) if execution_running(e)]
    recent = [f"{execution_name(e)} {'OK' if execution_succeeded(e) else ('…' if execution_running(e) else 'FALLA')}" for e in (execs or [])[:3]]
    part.add("S2", "ejecuciones", AVISO if running else OK,
             (f"en curso: {', '.join(running)}; " if running else "ninguna en curso; ") + ("últimas: " + ", ".join(recent) if recent else "sin ejecuciones"))
    s3_bucket = parse_tfvars(GCP_AUTO).get("exporter_s3_bucket") or "sin-s3"
    res = ctx.exporter("status", env={"GCP_PROJECT": ctx.project, "TA_GCS_LAKE_BUCKET": ctx.lake_bucket, "TA_LAKE_BUCKET": s3_bucket},
                       stream=False, timeout=120)
    wm = parse_json(res.out) or {}
    part.add("S3", "watermark", OK if res.ok else AVISO,
             f"cursor {wm.get('cursor_time_created')} · actualizado {wm.get('updated_at')} · última corrida "
             f"{(wm.get('last_run') or {}).get('uploaded', '?')} subidos" if wm.get("exists") else "todavía no existe")
    return part.finish()


def part_off(ctx: Ctx) -> int:
    part = Part(ctx, "off", "Apagado (estado de ahorro)")
    state, _ = scheduler_state(ctx)
    if state == "ENABLED":
        res = ctx.gcloud("scheduler", "jobs", "pause", SCHEDULER, f"--location={ctx.region}", timeout=120)
        state, _ = scheduler_state(ctx)
        part.add("O1", "pausar el Scheduler", OK if state == "PAUSED" else FALLA, f"estado {state}" + ("" if res.ok else ": " + res.tail(2)))
    else:
        part.add("O1", "pausar el Scheduler", OK if state in ("PAUSED", "NO_EXISTE") else AVISO, f"estado {state}")

    execs = list_executions(ctx, 20)
    running = [execution_name(e) for e in (execs or []) if execution_running(e)]
    for name in running:
        ctx.gcloud("run", "jobs", "executions", "cancel", name, f"--region={ctx.region}", "--quiet", timeout=180)
    if running:
        ctx.sleep(15)
        still = [execution_name(e) for e in (list_executions(ctx, 20) or []) if execution_running(e)]
    else:
        still = []
    part.add("O2", "ejecuciones del job", OK if not still else FALLA,
             ("canceladas: " + ", ".join(running) + "; " if running else "") + ("ninguna en curso" if not still else "siguen: " + ", ".join(still)))

    ev = ctx.load_evidence()
    pending = [c for c in ev.get("canaries", []) if not c.get("cleaned")]
    if pending:
        outputs = aws_outputs(ctx, part)
        results = [cleanup_canary(ctx, outputs, c) for c in pending]
        part.add("O3", "canarios de E9", OK if all(results) else AVISO, f"{sum(results)} de {len(results)} borrados")
    else:
        part.add("O3", "canarios de E9", OK, "ninguno pendiente")
    res = ctx.gcloud("storage", "rm", "--recursive", f"gs://{ctx.lake_bucket}/export-state/_validation/", timeout=120)
    part.add("O4", "watermark de prueba (D)", OK, "borrado" if res.ok else "no quedaba nada")

    res = ctx.run([sys.executable, str(PROJECT / "scripts" / "status.py")], cwd=REPO, timeout=300)
    charging = [ln.strip() for ln in (res.out or "").splitlines() if "ENCENDIDO" in ln]
    hint = ("\nsigue cobrando: " + "; ".join(charging) + ". Si ningún lote lo necesita: python project/scripts/off.py "
            "(pide confirmación; --keep-simulator deja el simulador)") if charging else ""
    part.add("O5", "status.py (lo demás que cobra)", OK if res.ok and not charging else AVISO,
             res.tail(14) + "\n(status.py no conoce el exportador: O1–O2 lo confirman aquí)" + hint)
    return part.finish()


# --- CLI --------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "terraform": part_terraform,
    "federation": part_federation,
    "job": part_job,
    "lake": part_lake,
    "status": part_status,
    "off": part_off,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_3_2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, el de AWS_PROFILE o el por defecto)")
    parser.add_argument("--apply", action="store_true", help="C: aplicar los planes (pide confirmación)")
    parser.add_argument("--only", choices=["aws", "gcp"], default=None, help="C: solo una de las dos raíces")
    parser.add_argument("--limit", type=int, default=20, help="D: objetos a subir en la corrida limitada")
    parser.add_argument("--skip-build", action="store_true", help="E: usar la imagen ya escrita en aws_exporter.auto.tfvars")
    parser.add_argument("--image", default=None, help="E: usar esta imagen")
    parser.add_argument("--no-canary", action="store_true", help="E: sin el canario de E9")
    parser.add_argument("--wait-tick", action="store_true", help="E10: esperar el disparo programado en vez de forzarlo")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 3.2 · {args.part} · proyecto {ctx.project} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        # Fuera de WSL gcloud no se puede ejecutar desde Python y no está el venv: no tocar la nube a medias.
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si quedó el Scheduler encendido: python project/scripts/validate_3_2.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
