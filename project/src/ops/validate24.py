"""Validación automática del Lote 2.4 (``docs/validation/validation-2-4.md``).

Partes (cada una se corre por separado; ``all`` corre A, B, C sin aplicar, D y E):

``preflight``  A · entorno de Python, archivos, enganche en pipeline.py, herramientas y estado.
``tests``      B · pruebas del lote, de ``streaming/``, de ``contracts/`` y de ``ops/``.
``terraform``  C · ``validate.sh`` y ``plan`` limitado a los recursos del lote; ``--apply`` aplica.
``local``      D · emulador + DirectRunner con NLP: ventanas en ``.local-lake/trend_windows``.
``cloud``      E · job de Dataflow con el código del 2.4: ``dlq_events``, ``trend_windows``,
               latencia p95, ``dq_results`` y regresión de los lotes 2.1 a 2.3.
``template``   F · plantilla Flex: build, lanzamiento del job desde la plantilla y comprobación.

Se corre desde la raíz del repositorio con ``~/.venvs/ta-streaming`` activado. No usa
``ops/services.py``: llama a los scripts documentados (``dataflow_job.sh start``,
``local_*.sh``, verificadores) y a ``gcloud``/``terraform`` directamente. El drenado se pide
con ``gcloud dataflow jobs drain`` y se espera consultando el estado. Los comandos corren sin
entrada estándar y los largos muestran su salida mientras corren. Resumen
OK / AVISO / FALLA y evidencia en ``project/evidencia/validacion_2_4.json``.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

REPO = Path(__file__).resolve().parents[3]
PROJECT = REPO / "project"
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE_FILE = EVIDENCE_DIR / "validacion_2_4.json"
TF_DIR = Path("infra/gcp/environments/dev")  # relativo a project/

OK, AVISO, FALLA, OMITIDO = "OK", "AVISO", "FALLA", "OMITIDO"

LOTE_TESTS = (
    "src/streaming/tests/test_trends_core.py",
    "src/streaming/tests/test_trends_config.py",
    "src/streaming/tests/test_trends_pipeline.py",
    "src/streaming/tests/test_trends_verifier.py",
    "src/streaming/tests/test_trends_terraform.py",
    "src/streaming/tests/test_trends_contract.py",
    "src/streaming/tests/test_trends_hook.py",
    "src/streaming/tests/test_flex_template.py",
    "src/ops/tests/test_ops_validate24.py",
)
# Pruebas del lote en el repositorio con contracts/, pipeline.py y platform.tf presentes.
EXPECTED_LOTE_TESTS = 224

LOTE_FILES = (
    "src/common/stream_tables.py",
    "src/streaming/trends.py", "src/streaming/trends_core.py", "src/streaming/bq_sink.py",
    "src/streaming/dlq_bq.py", "src/streaming/trends_check.py", "src/streaming/trends_bq.py",
    "src/streaming/trends_cli.py", "src/streaming/template_tools.py", "src/streaming/flex_main.py",
    "src/streaming/flex_template/Dockerfile", "src/streaming/flex_template/metadata.json",
    "src/streaming/TRENDS.md",
    "scripts/verify_trends.py", "scripts/dataflow_template.py", "scripts/validate_2_4.py",
    "infra/gcp/modules/bigquery_stream_tables/main.tf",
    "infra/gcp/environments/dev/trends.tf", "infra/gcp/environments/dev/variables_trends.tf",
    "infra/gcp/environments/dev/outputs_trends.tf",
)
PREVIOUS_FILES = (
    "src/streaming/pipeline.py", "src/streaming/enriched.py", "src/streaming/nlp.py",
    "src/streaming/requirements.txt", "src/setup.py",
    "scripts/dataflow_job.sh", "scripts/verify_streaming.py", "scripts/verify_enriched.py",
    "scripts/local_dev.sh", "scripts/local_dataflow.sh", "scripts/local_nlp.sh", "infra/validate.sh",
)

TERRAFORM_TARGETS = (
    "module.trends_bq_tables",
    "google_pubsub_subscription.trends_dlq_df",
    "google_pubsub_subscription_iam_member.trends_dataflow_viewer",
    "google_pubsub_subscription_iam_member.trends_dataflow_dlq_subscriber",
)
EXPECTED_CREATES = frozenset({
    "module.trends_bq_tables.google_bigquery_table.trend_windows",
    "module.trends_bq_tables.google_bigquery_table.dlq_events",
    "module.trends_bq_tables.google_bigquery_table.dq_results",
    "module.trends_bq_tables.google_bigquery_table.trend_windows_latest",
    "google_pubsub_subscription.trends_dlq_df",
    'google_pubsub_subscription_iam_member.trends_dataflow_viewer["reddit"]',
    'google_pubsub_subscription_iam_member.trends_dataflow_viewer["facebook"]',
    'google_pubsub_subscription_iam_member.trends_dataflow_viewer["instagram"]',
    'google_pubsub_subscription_iam_member.trends_dataflow_viewer["dlq"]',
    "google_pubsub_subscription_iam_member.trends_dataflow_dlq_subscriber",
})
EXPECTED_OUTPUTS = ("trend_windows_table", "trend_windows_latest_view", "dlq_events_table", "dq_results_table",
                    "dlq_df_subscription", "dataflow_template_spec", "dataflow_launcher_image")
# dataflow_template.py los deriva de lake_bucket y artifact_registry_url si faltan.
DERIVABLE_OUTPUTS = ("dataflow_template_spec", "dataflow_launcher_image")
ACTIVE_STATES = {"Running", "Pending", "Queued", "Draining", "Cancelling", "Updating", "Starting", "Stopped",
                 "JOB_STATE_RUNNING", "JOB_STATE_PENDING", "JOB_STATE_QUEUED", "JOB_STATE_DRAINING"}
LOG_MARKERS = {
    "trend_bq_insert_failed": FALLA,
    "dlq_bq_insert_failed": FALLA,
    "trend_row_unrecognized": FALLA,
    "nlp_inference_failed": AVISO,
    "GETTING_PUBSUB_SUBSCRIPTION_FAILED": AVISO,
}


# --------------------------------------------------------------------------- infraestructura

@dataclass
class Step:
    id: str
    title: str
    status: str
    detail: str = ""
    data: dict = field(default_factory=dict)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def default_run(cmd: Sequence[str], *, cwd: Path | None = None, timeout: float = 600,
                env: Mapping[str, str] | None = None,
                stream: Callable[[str], None] | None = None) -> subprocess.CompletedProcess:
    """Corre un comando sin entrada estándar (un script que pida confirmación falla en vez de quedarse
    esperando en silencio). Con ``stream``, cada línea de salida se muestra mientras corre; stdout y
    stderr van juntos en ``stdout``."""
    if stream is None:
        try:
            return subprocess.run(list(cmd), cwd=cwd or PROJECT, capture_output=True, text=True,
                                  stdin=subprocess.DEVNULL, timeout=timeout,
                                  env=dict(env) if env else None, check=False)
        except subprocess.TimeoutExpired as exc:
            out = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            return subprocess.CompletedProcess(list(cmd), 124, stdout=out, stderr=f"tiempo agotado ({timeout:.0f} s)")
        except FileNotFoundError as exc:
            return subprocess.CompletedProcess(list(cmd), 127, stdout="", stderr=str(exc))
    try:
        process = subprocess.Popen(list(cmd), cwd=cwd or PROJECT, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True, bufsize=1,
                                   env=dict(env) if env else None)
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(list(cmd), 127, stdout="", stderr=str(exc))
    lines: list[str] = []

    def pump():
        for line in process.stdout:
            lines.append(line)
            stream(line.rstrip("\n"))

    reader = threading.Thread(target=pump, daemon=True)
    reader.start()
    try:
        returncode = process.wait(timeout=timeout)
        stderr = ""
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        returncode, stderr = 124, f"tiempo agotado ({timeout:.0f} s)"
    reader.join(timeout=5)
    return subprocess.CompletedProcess(list(cmd), returncode, stdout="".join(lines), stderr=stderr)


@dataclass
class Ctx:
    run: Callable[..., subprocess.CompletedProcess] = default_run
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    now: Callable[[], datetime] = _now
    confirm: Callable[[str], bool] = lambda prompt: input(prompt).strip().lower() in {"s", "si", "sí", "y", "yes"}
    say: Callable[[str], None] = print
    which: Callable[[str], str | None] = shutil.which
    python: str = sys.executable
    project_dir: Path = PROJECT
    evidence_dir: Path = EVIDENCE_DIR
    env: dict = field(default_factory=lambda: dict(os.environ))
    steps: list = field(default_factory=list)

    def add(self, step: Step) -> Step:
        self.steps.append(step)
        self.say(f"[{step.status:6}] {step.id} {step.title}: {step.detail}")
        return step

    def sh(self, cmd: Sequence[str], timeout: float = 600, stream: bool = False) -> subprocess.CompletedProcess:
        """Corre y muestra el comando; con ``stream`` muestra también su salida mientras corre."""
        self.say("  $ " + " ".join(cmd))
        extra = {"stream": lambda line: self.say("    │ " + line)} if stream else {}
        return self.run(list(cmd), cwd=self.project_dir, timeout=timeout, env=self.env, **extra)


def tail(text: str | None, lines: int = 15) -> str:
    return "\n".join((text or "").strip().splitlines()[-lines:])


def parse_pytest_summary(text: str) -> dict:
    counts = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
    lines = [l for l in (text or "").splitlines() if re.search(r"\d+ (passed|failed|skipped|errors?)", l)]
    if not lines:
        return {**counts, "found": False}
    for number, kind in re.findall(r"(\d+) (passed|failed|skipped|errors?|error)\b", lines[-1]):
        key = "errors" if kind.startswith("error") else kind
        counts[key] += int(number)
    return {**counts, "found": True}


def skip_reasons(text: str) -> list[str]:
    return [l.strip() for l in (text or "").splitlines() if l.strip().startswith("SKIPPED")][:20]


def hook_problems(pipeline_src: str | None, enriched_src: str | None) -> list[str]:
    """Lo que falta para enganchar la rama (TRENDS.md §2). Lista vacía = enganchada."""
    problems = []
    if pipeline_src is None:
        return ["no está src/streaming/pipeline.py (Lote 2.1)"]
    tree = ast.parse(pipeline_src)

    def fname(call):
        return getattr(call.func, "id", getattr(call.func, "attr", None))

    imported = any(isinstance(n, ast.ImportFrom) and n.module == "streaming.trends"
                   and any(a.name == "attach_trends" for a in n.names) for n in ast.walk(tree))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and fname(n) == "attach_trends"]
    if not imported:
        problems.append("pipeline.py: falta 'from streaming.trends import attach_trends'")
    if not calls:
        problems.append("pipeline.py: falta 'attach_trends(enriched_rows)' después de attach_enriched(unique)")
    else:
        assigned = {t.id for n in ast.walk(tree) if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                    and fname(n.value) == "attach_enriched" for t in n.targets if isinstance(t, ast.Name)}
        first = calls[0].args[0] if calls[0].args else None
        ok = (isinstance(first, ast.Name) and first.id in assigned) or (
            isinstance(first, ast.Call) and fname(first) == "attach_enriched")
        if not ok:
            problems.append("pipeline.py: attach_trends debe recibir lo que devuelve attach_enriched(unique)")
    if enriched_src is not None:
        etree = ast.parse(enriched_src)
        funcs = [n for n in etree.body if isinstance(n, ast.FunctionDef) and n.name == "attach_enriched"]
        if funcs:
            returns = [r for r in ast.walk(funcs[0]) if isinstance(r, ast.Return) and r.value is not None
                       and not (isinstance(r.value, ast.Constant) and r.value.value is None)]
            if not returns:
                problems.append("enriched.py: attach_enriched() no devuelve las filas (añada 'return rows')")
    return problems


def classify_plan(plan: Mapping) -> dict:
    out = {"create": [], "update": [], "delete": [], "replace": [], "noop": 0,
           "outputs": sorted(name for name, change in (plan.get("output_changes") or {}).items()
                             if change.get("actions") not in (["no-op"], None))}
    for change in plan.get("resource_changes", []):
        actions = change.get("change", {}).get("actions", [])
        address = change.get("address", "")
        if actions == ["no-op"] or actions == ["read"]:
            out["noop"] += 1
        elif actions == ["create"]:
            out["create"].append(address)
        elif actions == ["update"]:
            out["update"].append(address)
        elif actions == ["delete"]:
            out["delete"].append(address)
        elif set(actions) == {"delete", "create"}:
            out["replace"].append(address)
    return out


def check_plan(classified: Mapping) -> tuple[str, str]:
    creates = set(classified["create"])
    if classified["delete"] or classified["replace"]:
        return FALLA, f"el plan borraría o reemplazaría {classified['delete'] + classified['replace']}: no aplique"
    unexpected = sorted(creates - EXPECTED_CREATES)
    missing = sorted(EXPECTED_CREATES - creates)
    if not creates and not classified["update"]:
        if classified.get("outputs"):
            return OK, f"sin cambios de recursos; outputs a guardar: {classified['outputs']}"
        return OK, "sin cambios: el lote ya está aplicado"
    if unexpected:
        return AVISO, f"crea además {unexpected}"
    if classified["update"]:
        return AVISO, f"{len(creates)} a crear y actualiza {classified['update']} (deriva de otros lotes)"
    if missing:
        return AVISO, f"{len(creates)} a crear; ya existen {len(missing)} del lote (aplicación parcial previa)"
    return OK, f"{len(creates)} a crear, 0 a cambiar, 0 a destruir"


def parse_json_lines(text: str, event: str) -> list[dict]:
    found = []
    for line in (text or "").splitlines():
        line = line.strip()
        if event not in line or not line.startswith("{"):
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        if data.get("event") == event:
            found.append(data)
    return found


def load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def save_evidence(part: str, steps: Sequence[Step], extra: Mapping | None = None,
                  path: Path = EVIDENCE_FILE, now: datetime | None = None) -> dict:
    data = load_json(path) or {"lote": "2.4", "parts": {}}
    data.setdefault("parts", {})[part] = {
        "verdict": verdict(steps), "finished_at": iso(now or _now()),
        "steps": [asdict(s) for s in steps], **(extra or {}),
    }
    data["updated_at"] = iso(now or _now())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return data


def verdict(steps: Iterable[Step]) -> str:
    statuses = {s.status for s in steps}
    return FALLA if FALLA in statuses else AVISO if AVISO in statuses else OK


# --------------------------------------------------------------------------- GCP

def resolve_gcp(ctx: Ctx) -> dict:
    """Proyecto, región y entorno: variables de entorno, terraform output o gcloud."""
    info = {"project": ctx.env.get("GCP_PROJECT"), "region": ctx.env.get("GCP_REGION"),
            "env": ctx.env.get("ENV") or "dev"}
    if not info["project"] or not info["region"]:
        ctx.say("  Proyecto y región desde terraform output (puede tardar en /mnt/c; para saltarlo, "
                "exporte GCP_PROJECT y GCP_REGION)…")
        proc = ctx.sh(["terraform", f"-chdir={TF_DIR}", "output", "-json"], timeout=300)
        if proc.returncode == 124:
            ctx.say("  terraform output no respondió: se usa gcloud config.")
        if proc.returncode == 0:
            try:
                outputs = json.loads(proc.stdout or "{}")
            except ValueError:
                outputs = {}
            info["project"] = info["project"] or (outputs.get("project_id") or {}).get("value")
            info["region"] = info["region"] or (outputs.get("region") or {}).get("value")
            labels = (outputs.get("labels") or {}).get("value") or {}
            info["env"] = ctx.env.get("ENV") or labels.get("env") or "dev"
    if not info["project"]:
        proc = ctx.sh(["gcloud", "config", "get-value", "project"], timeout=60)
        info["project"] = (proc.stdout or "").strip().splitlines()[-1] if (proc.stdout or "").strip() else None
    info["region"] = info["region"] or "us-central1"
    if info["project"]:
        ctx.env["GCP_PROJECT"] = info["project"]
    ctx.env["GCP_REGION"] = info["region"]
    ctx.env.setdefault("ENV", info["env"])
    return info


def list_jobs(ctx: Ctx, gcp: Mapping, name: str) -> list[dict]:
    proc = ctx.run(["gcloud", "dataflow", "jobs", "list", f"--project={gcp['project']}",
                    f"--region={gcp['region']}", "--status=active", f"--filter=name={name}", "--format=json"],
                   cwd=ctx.project_dir, timeout=120, env=ctx.env)
    if proc.returncode != 0:
        raise RuntimeError(f"gcloud dataflow jobs list: {tail(proc.stderr, 5)}")
    try:
        jobs = json.loads(proc.stdout or "[]")
    except ValueError:
        jobs = []
    return [j for j in jobs if j.get("name") == name]


def wait_for(ctx: Ctx, predicate: Callable[[], object], timeout_s: float, every_s: float = 20.0):
    deadline = ctx.clock() + timeout_s
    while True:
        value = predicate()
        if value:
            return value
        if ctx.clock() >= deadline:
            return None
        ctx.sleep(every_s)


def _running(ctx: Ctx, gcp: Mapping, name: str):
    jobs = list_jobs(ctx, gcp, name)
    running = [j for j in jobs if str(j.get("state", "")).replace("JOB_STATE_", "").lower() == "running"]
    return running[0] if running else None


def _epoch_s(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def job_has_lote_24_options(ctx: Ctx, gcp: Mapping, job_id: str) -> dict:
    """El job se lanzó con el código del 2.4 si sus opciones incluyen las de la rama de tendencias."""
    proc = ctx.sh(["gcloud", "dataflow", "jobs", "describe", job_id, f"--project={gcp['project']}",
                   f"--region={gcp['region']}", "--full", "--format=json"], timeout=120)
    text = proc.stdout or ""
    missing = [name for name in ("trend_windows_table", "dlq_df_subscription") if name not in text]
    return {"ok": proc.returncode == 0 and not missing, "missing": missing if proc.returncode == 0 else "describe falló"}


def _is_draining(job: Mapping) -> bool:
    return "drain" in str(job.get("state", "")).lower()


def request_drain(ctx: Ctx, gcp: Mapping, jobs: Sequence[Mapping]) -> list[str]:
    """Pide el drenado con gcloud (vuelve enseguida). Devuelve los errores."""
    errors = []
    for job in jobs:
        if _is_draining(job):
            continue
        proc = ctx.sh(["gcloud", "dataflow", "jobs", "drain", str(job.get("id")), f"--project={gcp['project']}",
                       f"--region={gcp['region']}"], timeout=180)
        if proc.returncode != 0:
            errors.append(f"{job.get('id')}: {tail(proc.stderr or proc.stdout, 4)}")
    return errors


def drain_and_wait(ctx: Ctx, gcp: Mapping, name: str, launcher: str, step_id: str,
                   timeout_s: float = 1800, cancel_after_s: float = 0, heartbeat_s: float = 120) -> Step:
    """Drena el job activo con gcloud y espera a que termine (no usa dataflow_job.sh drain).

    Cada ``heartbeat_s`` muestra el estado y el tiempo transcurrido. Con ``cancel_after_s`` > 0,
    si el drenado no terminó en ese tiempo, cancela el job (se pierden los datos en curso y las
    ventanas abiertas no se emiten) y espera a que desaparezca.
    """
    title = "Drenar el job anterior"
    jobs = list_jobs(ctx, gcp, name)
    if not jobs:
        return Step(step_id, title, OK, f"no hay {name} activo")
    states = [f"{j.get('id')} {j.get('state')}" for j in jobs]
    ctx.say(f"  Activo: {', '.join(states)}")
    errors = request_drain(ctx, gcp, jobs)
    if errors:
        return Step(step_id, title, FALLA, "; ".join(errors))
    seen: dict[str, str] = {}
    start = ctx.clock()
    last_report = [start]
    cancelled: list[str] = []

    def finished():
        current = list_jobs(ctx, gcp, name)
        now = ctx.clock()
        changed = False
        for job in current:
            state = str(job.get("state"))
            if seen.get(job.get("id")) != state:
                seen[job.get("id")] = state
                changed = True
                ctx.say(f"  … {job.get('id')}: {state} ({(now - start) / 60:.0f} min)")
        if current and not changed and now - last_report[0] >= heartbeat_s:
            ctx.say(f"  … sigue {', '.join(sorted(set(seen.values())))} ({(now - start) / 60:.0f} min de "
                    f"{timeout_s / 60:.0f}); estado en la consola: Dataflow → {name} → Data freshness por paso")
            last_report[0] = now
        if changed:
            last_report[0] = now
        if current and cancel_after_s and not cancelled and now - start >= cancel_after_s:
            for job in current:
                job_id = str(job.get("id"))
                ctx.say(f"  El drenado pasó de {cancel_after_s / 60:.0f} min: se cancela {job_id}.")
                ctx.sh(["gcloud", "dataflow", "jobs", "cancel", job_id, f"--project={gcp['project']}",
                        f"--region={gcp['region']}"], timeout=180)
                cancelled.append(job_id)
        return not current

    ctx.say(f"  Esperando a que {name} termine de drenar (suele tardar 3–10 min; hasta {timeout_s / 60:.0f} min)…")
    if not wait_for(ctx, finished, timeout_s, 30):
        ids = " ".join(seen) or "<id>"
        return Step(step_id, title, FALLA,
                    f"{name} sigue activo tras {timeout_s / 60:.0f} min ({seen}). Revise en la consola qué paso "
                    f"retiene el drenado (Data freshness); para seguir, cancélelo con 'gcloud dataflow jobs cancel "
                    f"{ids} --region={gcp['region']}' o repita con --cancel-after-minutes")
    minutes = (ctx.clock() - start) / 60
    if cancelled:
        return Step(step_id, title, AVISO,
                    f"cancelado tras {minutes:.0f} min sin terminar de drenar ({', '.join(cancelled)}): los datos en "
                    f"curso se pierden y las ventanas abiertas no se emiten", {"minutes": round(minutes, 1)})
    return Step(step_id, title, OK, f"drenado en {minutes:.0f} min ({', '.join(states)}) para lanzar el del {launcher}",
                {"minutes": round(minutes, 1)})


# --------------------------------------------------------------------------- partes

def part_preflight(ctx: Ctx, args) -> list[Step]:
    steps = []
    version = sys.version_info
    try:
        import apache_beam  # noqa: F401
        beam_version = apache_beam.__version__
    except ImportError:
        beam_version = None
    try:
        bq = importlib.util.find_spec("google.cloud.bigquery") is not None
    except ModuleNotFoundError:
        bq = False
    py_ok = (version.major, version.minor) == (3, 11)
    status = OK if py_ok and beam_version == "2.77.0" and bq else (FALLA if beam_version is None else AVISO)
    steps.append(ctx.add(Step("A1", "Python 3.11 con Beam 2.77.0 y google-cloud-bigquery", status,
                              f"Python {version.major}.{version.minor}, Beam {beam_version}, bigquery {bq} "
                              f"({ctx.python})")))
    missing_lote = [f for f in LOTE_FILES if not (ctx.project_dir / f).exists()]
    steps.append(ctx.add(Step("A2", "Archivos del Lote 2.4", FALLA if missing_lote else OK,
                              f"faltan {missing_lote}" if missing_lote else f"{len(LOTE_FILES)} presentes")))
    missing_prev = [f for f in PREVIOUS_FILES if not (ctx.project_dir / f).exists()]
    steps.append(ctx.add(Step("A3", "Archivos de los lotes 2.1 a 2.3", FALLA if missing_prev else OK,
                              f"faltan {missing_prev}" if missing_prev else f"{len(PREVIOUS_FILES)} presentes")))

    def read(rel):
        path = ctx.project_dir / rel
        return path.read_text(encoding="utf-8") if path.exists() else None

    problems = hook_problems(read("src/streaming/pipeline.py"), read("src/streaming/enriched.py"))
    steps.append(ctx.add(Step("A4", "Enganche de attach_trends (TRENDS.md §2)", FALLA if problems else OK,
                              "; ".join(problems) or "pipeline.py llama a attach_trends con las filas de attach_enriched")))
    tools = {name: bool(ctx.which(name)) for name in ("gcloud", "terraform", "docker", "bash")}
    absent = [n for n, ok in tools.items() if not ok]
    steps.append(ctx.add(Step("A5", "Herramientas", AVISO if absent else OK,
                              f"faltan {absent} (C necesita terraform, D docker, E y F gcloud)" if absent
                              else "gcloud, terraform, docker y bash", tools)))
    status_script = ctx.project_dir / "scripts" / "status.py"
    if status_script.exists():
        proc = ctx.sh([ctx.python, "scripts/status.py"], timeout=180)
        ctx.say(tail(proc.stdout, 25))
        steps.append(ctx.add(Step("A6", "Estado de los servicios (status.py)", OK if proc.returncode == 0 else AVISO,
                                  "ver la salida de arriba" if proc.returncode == 0 else tail(proc.stderr, 5))))
    else:
        steps.append(ctx.add(Step("A6", "Estado de los servicios (status.py)", OMITIDO, "no está scripts/status.py")))
    return steps


def _pytest_step(ctx: Ctx, step_id: str, title: str, targets: Sequence[str], *, expect_min: int = 0,
                 strict_skips: bool = False, timeout: float = 1800) -> Step:
    proc = ctx.sh([ctx.python, "-m", "pytest", *targets, "-q", "-rs", "-p", "no:cacheprovider"], timeout=timeout)
    summary = parse_pytest_summary(proc.stdout)
    detail = f"{summary['passed']} passed, {summary['failed']} failed, {summary['skipped']} skipped"
    if not summary["found"] or proc.returncode not in (0,) and not summary["failed"] and not summary["errors"]:
        return Step(step_id, title, FALLA, f"pytest no terminó bien (código {proc.returncode}): "
                                           f"{tail(proc.stdout or proc.stderr, 6)}", summary)
    if summary["failed"] or summary["errors"]:
        return Step(step_id, title, FALLA, detail + "\n" + tail(proc.stdout, 12), summary)
    if strict_skips and summary["skipped"]:
        return Step(step_id, title, AVISO, detail + "; omitidas: " + "; ".join(skip_reasons(proc.stdout)),
                    summary)
    if expect_min and summary["passed"] < expect_min:
        return Step(step_id, title, AVISO, detail + f" (se esperaban al menos {expect_min})", summary)
    return Step(step_id, title, OK, detail, summary)


def part_tests(ctx: Ctx, args) -> list[Step]:
    return [
        ctx.add(_pytest_step(ctx, "B1", "Pruebas del Lote 2.4 (sin omitidas)", LOTE_TESTS,
                             expect_min=EXPECTED_LOTE_TESTS, strict_skips=True)),
        ctx.add(_pytest_step(ctx, "B2", "Todo streaming/ (regresión 2.1 a 2.3)", ["src/streaming"], timeout=3600)),
        ctx.add(_pytest_step(ctx, "B3", "contracts/tests", ["contracts/tests"], expect_min=18)),
        ctx.add(_pytest_step(ctx, "B4", "ops/ (validadores)", ["src/ops/tests"])),
    ]


def part_terraform(ctx: Ctx, args) -> list[Step]:
    steps = []
    proc = ctx.sh(["bash", "infra/validate.sh"], timeout=900)
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        hint = ""
        if "undeclared module" in output.lower():
            hint = (" · trends.tf usa module.pubsub y module.bigquery_stream: cambie el nombre por el de "
                    "platform.tf/streaming.tf")
        steps.append(ctx.add(Step("C1", "infra/validate.sh (fmt + validate)", FALLA, tail(output, 10) + hint)))
        return steps
    steps.append(ctx.add(Step("C1", "infra/validate.sh (fmt + validate)", OK, "fmt y validate en verde")))

    ctx.evidence_dir.mkdir(parents=True, exist_ok=True)
    plan_file = (ctx.evidence_dir / "tfplan_2_4").resolve()
    targets = [] if args.full_plan else [f"-target={t}" for t in TERRAFORM_TARGETS]
    proc = ctx.sh(["terraform", f"-chdir={TF_DIR}", "plan", "-input=false", "-no-color", *targets,
                   f"-out={plan_file}"], timeout=900)
    if proc.returncode != 0:
        steps.append(ctx.add(Step("C2", "terraform plan del lote", FALLA, tail(proc.stderr or proc.stdout, 12))))
        return steps
    show = ctx.sh(["terraform", f"-chdir={TF_DIR}", "show", "-json", str(plan_file)], timeout=300)
    try:
        classified = classify_plan(json.loads(show.stdout or "{}"))
    except ValueError:
        steps.append(ctx.add(Step("C2", "terraform plan del lote", FALLA, "terraform show -json no devolvió JSON")))
        return steps
    status, detail = check_plan(classified)
    steps.append(ctx.add(Step("C2", "terraform plan del lote" + (" (completo)" if args.full_plan else " (-target)"),
                              status, detail, classified)))
    if not args.apply:
        steps.append(ctx.add(Step("C3", "terraform apply", OMITIDO,
                                  "corra 'validate_2_4.py terraform --apply' para aplicar este plan")))
        return steps
    if status == FALLA:
        steps.append(ctx.add(Step("C3", "terraform apply", FALLA, "no se aplica un plan que borra recursos")))
        return steps
    if classified["create"] or classified["update"] or classified.get("outputs"):
        if not args.yes and not ctx.confirm(f"¿Aplicar el plan ({detail})? [s/N] "):
            steps.append(ctx.add(Step("C3", "terraform apply", OMITIDO, "cancelado por el usuario")))
            return steps
        proc = ctx.sh(["terraform", f"-chdir={TF_DIR}", "apply", "-input=false", "-no-color", str(plan_file)],
                      timeout=1200)
        if proc.returncode != 0:
            steps.append(ctx.add(Step("C3", "terraform apply", FALLA, tail(proc.stderr or proc.stdout, 12))))
            return steps
        summary = [l for l in (proc.stdout or "").splitlines() if l.startswith("Apply complete!")]
        steps.append(ctx.add(Step("C3", "terraform apply", OK, summary[-1] if summary else tail(proc.stdout, 2))))
    else:
        steps.append(ctx.add(Step("C3", "terraform apply", OK, "nada que aplicar")))
    proc = ctx.sh(["terraform", f"-chdir={TF_DIR}", "output", "-json"], timeout=120)
    try:
        outputs = json.loads(proc.stdout or "{}")
    except ValueError:
        outputs = {}
    absent = [o for o in EXPECTED_OUTPUTS if o not in outputs]
    hard = [o for o in absent if o not in DERIVABLE_OUTPUTS]
    if hard:
        status, detail = FALLA, f"faltan {absent}"
    elif absent:
        status, detail = AVISO, (f"faltan {absent}: dataflow_template.py los deriva de lake_bucket y "
                                 "artifact_registry_url; repita 'terraform --apply' para guardarlos")
    else:
        status, detail = OK, ", ".join(f"{o}={outputs[o].get('value')}" for o in EXPECTED_OUTPUTS[:3])
    steps.append(ctx.add(Step("C4", "Outputs del lote (§5.9)", status, detail)))
    return steps


def _local_trend_rows(lake: Path, since_ms: int) -> list[dict]:
    rows = []
    root = lake / "trend_windows"
    for path in root.rglob("*.jsonl") if root.exists() else []:
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            start = row.get("window_start")
            if isinstance(start, str):
                ms = int(datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp() * 1000)
                if ms >= since_ms:
                    rows.append(row)
    return rows


def part_local(ctx: Ctx, args) -> list[Step]:
    steps = []
    proc = ctx.sh(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=60)
    if proc.returncode != 0:
        steps.append(ctx.add(Step("D1", "Docker disponible", FALLA, "abra Docker Desktop: " + tail(proc.stderr, 3))))
        return steps
    steps.append(ctx.add(Step("D1", "Docker disponible", OK, f"Docker {proc.stdout.strip()}")))

    # El emulador guarda todo en memoria: reiniciarlo vacía las suscripciones -df. Si el pipeline
    # estuvo parado con el simulador encendido, el NLP local (~3 comentarios/s) tardaría en ponerse al día.
    ctx.sh(["bash", "scripts/local_dataflow.sh", "down"], timeout=300)
    ctx.sh(["bash", "scripts/local_dev.sh", "down"], timeout=300)
    proc = ctx.sh(["bash", "scripts/local_dev.sh", "up"], timeout=900, stream=True)
    steps.append(ctx.add(Step("D2", "Emulador y simulador reiniciados, sin backlog (local_dev.sh down + up)",
                              OK if proc.returncode == 0 else FALLA, tail(proc.stdout or proc.stderr, 3))))
    if proc.returncode != 0:
        return steps

    image = ctx.sh(["docker", "image", "inspect", "ta-dataflow-nlp-local:local", "--format", "{{.Id}}"], timeout=60)
    if image.returncode != 0:
        ctx.say("  La imagen de NLP local no existe: se construye (descarga los modelos, puede tardar).")
        build = ctx.sh(["bash", "scripts/local_nlp.sh", "build"], timeout=3600, stream=True)
        if build.returncode != 0:
            steps.append(ctx.add(Step("D3", "Pipeline local con NLP y tendencias", FALLA,
                                      "local_nlp.sh build: " + tail(build.stderr or build.stdout, 8))))
            return steps
    since = ctx.now()
    proc = ctx.sh(["bash", "scripts/local_nlp.sh", "up"], timeout=1200, stream=True)
    if proc.returncode != 0:
        steps.append(ctx.add(Step("D3", "Pipeline local con NLP y tendencias", FALLA, tail(proc.stderr or proc.stdout, 8))))
        return steps

    def trends_config():
        names = ctx.run(["docker", "ps", "--filter", "name=dataflow-local", "--format", "{{.Names}}"],
                        cwd=ctx.project_dir, timeout=60, env=ctx.env)
        name = (names.stdout or "").strip().splitlines()
        if not name:
            return None
        logs = ctx.run(["docker", "logs", "--since", iso(since), name[0]], cwd=ctx.project_dir, timeout=60,
                       env=ctx.env)
        found = parse_json_lines((logs.stdout or "") + (logs.stderr or ""), "trends_config")
        return found[-1] if found else None

    config = wait_for(ctx, trends_config, 300, 15)
    if not config:
        steps.append(ctx.add(Step("D3", "Pipeline local con NLP y tendencias", FALLA,
                                  "no apareció trends_config en los logs de dataflow-local: revise el enganche (A4) "
                                  "con 'bash scripts/local_dataflow.sh logs'")))
        return steps
    ok = config.get("enabled") and config.get("local_root")
    steps.append(ctx.add(Step("D3", "Pipeline local con NLP y tendencias", OK if ok else FALLA,
                              f"trends_config: enabled={config.get('enabled')}, local_root={config.get('local_root')}, "
                              f"late_delay={config.get('trend', {}).get('late_delay_seconds')}", config)))
    if not ok:
        return steps

    lake = ctx.project_dir / ".local-lake"
    since_ms = int(since.timestamp() * 1000)

    def enough_windows():
        rows = _local_trend_rows(lake, since_ms)
        closed = {r["window_start"] for r in rows
                  if int(datetime.fromisoformat(r["window_end"].replace("Z", "+00:00")).timestamp() * 1000)
                  <= int(ctx.clock() * 1000) - 120_000}
        return len(closed) >= args.min_windows and rows

    ctx.say(f"  Esperando ≥ {args.min_windows} ventanas cerradas (hasta {args.wait_minutes} min)…")
    rows = wait_for(ctx, enough_windows, args.wait_minutes * 60, 30)
    steps.append(ctx.add(Step("D4", "Ventanas en .local-lake/trend_windows", OK if rows else FALLA,
                              f"{len(rows)} filas desde {iso(since)}" if rows
                              else f"no hubo {args.min_windows} ventanas cerradas en {args.wait_minutes} min")))
    if not rows:
        return steps

    report = ctx.evidence_dir / "local_trends_report.json"
    proc = ctx.sh([ctx.python, "scripts/verify_trends.py", "--json-out", str(report), "report", "--local",
                   "--since", iso(since), "--warmup-minutes", "3"], timeout=600)
    steps.append(ctx.add(_verifier_step("D5", "verify_trends.py report --local", proc, report)))
    out = ctx.evidence_dir / "local_enriched_report_2_4.json"
    proc = ctx.sh([ctx.python, "scripts/verify_enriched.py", "report", "--local", "--expect-nlp",
                   "--json-out", str(out)], timeout=600)
    steps.append(ctx.add(Step("D6", "Regresión 2.2/2.3: verify_enriched.py --local --expect-nlp",
                              OK if proc.returncode == 0 else FALLA, tail(proc.stdout or proc.stderr, 3))))
    proc = ctx.sh(["bash", "scripts/local_dataflow.sh", "check"], timeout=1200, stream=True)
    steps.append(ctx.add(Step("D7", "Regresión 2.1: local_dataflow.sh check", OK if proc.returncode == 0 else FALLA,
                              tail(proc.stdout or proc.stderr, 3))))
    if args.down:
        proc = ctx.sh(["bash", "scripts/local_dataflow.sh", "down-all"], timeout=300)
        steps.append(ctx.add(Step("D8", "Apagado local (down-all)", OK if proc.returncode == 0 else AVISO,
                                  tail(proc.stdout or proc.stderr, 2))))
    return steps


def _verifier_step(step_id: str, title: str, proc: subprocess.CompletedProcess, report: Path) -> Step:
    data = load_json(report) or {}
    checks = {c["id"]: c["status"] for c in data.get("checks", [])}
    if proc.returncode == 2:
        return Step(step_id, title, FALLA, "error de uso o de infraestructura: " + tail(proc.stderr, 4), checks)
    status = FALLA if proc.returncode == 1 else (data.get("verdict") if data.get("verdict") in (OK, AVISO) else OK)
    summary = " · ".join(f"{k} {v}" for k, v in checks.items()) or tail(proc.stdout, 3)
    return Step(step_id, title, status, summary, {"report": str(report), "checks": checks})


JOB_ID_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}-\d+)\b")
TERMINAL_STATES = {"JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_DONE", "JOB_STATE_DRAINED",
                   "JOB_STATE_UPDATED", "JOB_STATE_STOPPED"}


def job_id_from_output(text: str) -> str | None:
    """Último id de job de Dataflow en la salida (``dataflow_job.sh start`` o ``flex-template run``)."""
    found = JOB_ID_RE.findall(text or "")
    return found[-1] if found else None


def job_state(ctx: Ctx, gcp: Mapping, job_id: str) -> str | None:
    proc = ctx.run(["gcloud", "dataflow", "jobs", "describe", job_id, f"--project={gcp['project']}",
                    f"--region={gcp['region']}", "--format=value(currentState)"],
                   cwd=ctx.project_dir, timeout=120, env=ctx.env)
    state = (proc.stdout or "").strip().splitlines()
    return state[-1].strip() if proc.returncode == 0 and state else None


ERROR_LINE_RE = re.compile(r"Traceback|Error|Exception|error:|failed|No such file", re.I)


def job_errors(ctx: Ctx, gcp: Mapping, job_id: str, limit: int = 80) -> list[str]:
    """Errores del job en Cloud Logging, del más antiguo al último.

    Incluye los logs del lanzador de la plantilla Flex sin filtro de severidad: la salida del
    programa (el traceback de Python) llega con severidad INFO y el error del servicio solo dice
    "Template launch failed: exit status 1".
    """
    query = (f'resource.type="dataflow_step" AND resource.labels.job_id="{job_id}" AND (severity>=ERROR '
             'OR logName:"launcher" OR textPayload:"Traceback" OR jsonPayload.message:"Traceback")')
    proc = ctx.run(["gcloud", "logging", "read", query, f"--project={gcp['project']}", "--freshness=2h",
                    f"--limit={limit}", "--format=json"], cwd=ctx.project_dir, timeout=180, env=ctx.env)
    try:
        entries = json.loads(proc.stdout or "[]") if proc.returncode == 0 else []
    except ValueError:
        entries = []
    lines = []
    for entry in reversed(entries):
        payload = entry.get("jsonPayload") or {}
        message = payload.get("message") or entry.get("textPayload") or json.dumps(payload)[:300]
        log = str(entry.get("logName", "")).rsplit("%2F", 1)[-1]
        lines.append(f"[{log}] {str(message).strip()[:400]}")
    return lines


def _start_job(ctx: Ctx, gcp: Mapping, name: str, cmd: Sequence[str], step_id: str, title: str,
               timeout_s: float, heartbeat_s: float = 120) -> tuple[Step, dict | None]:
    """Lanza el job y espera a *Running*. Si termina antes (p. ej. falla el lanzador), avisa enseguida."""
    proc = ctx.sh(cmd, timeout=1800, stream=True)
    text = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return Step(step_id, title, FALLA, tail(text, 12)), None
    configs = parse_json_lines(text, "trends_config")
    job_id = job_id_from_output(text)
    start = ctx.clock()
    seen = {"state": None, "reported": start}

    def check():
        if not job_id:
            running = _running(ctx, gcp, name)
            return ("running", running) if running else None
        state = job_state(ctx, gcp, job_id)
        now = ctx.clock()
        if state != seen["state"] or now - seen["reported"] >= heartbeat_s:
            ctx.say(f"  … {job_id}: {state} ({(now - start) / 60:.0f} min)")
            seen.update(state=state, reported=now)
        if state in TERMINAL_STATES:
            return ("terminal", state)
        if state == "JOB_STATE_RUNNING":
            return ("running", {"id": job_id, "name": name, "state": state})
        return None

    result = wait_for(ctx, check, timeout_s, 30)
    if result and result[0] == "terminal":
        errors = job_errors(ctx, gcp, job_id)
        relevant = [line for line in errors if ERROR_LINE_RE.search(line)] or errors
        detail = f"{job_id} terminó en {result[1]} sin llegar a Running"
        if relevant:
            detail += ":\n" + "\n".join(relevant[-10:])
        return Step(step_id, title, FALLA, detail, {"job_id": job_id, "state": result[1], "errors": errors}), None
    if not result:
        return Step(step_id, title, FALLA,
                    f"{name} no llegó a Running en {timeout_s / 60:.0f} min (último estado: {seen['state']})",
                    {"job_id": job_id}), None
    job = result[1]
    data = {"job": job, "trends_config": configs[-1] if configs else None}
    return Step(step_id, title, OK, f"{name} en Running ({job.get('id')})", data), job


def _check_trends_config(config: Mapping | None, step_id: str) -> Step:
    title = "trends_config del lanzamiento: tendencias y DLQ a BigQuery"
    if not config:
        return Step(step_id, title, AVISO, "la salida del lanzamiento no traía trends_config (se revisa en E6)")
    ok = config.get("enabled") and config.get("bq_table") and config.get("dlq_enabled")
    detail = (f"bq_table={config.get('bq_table')}, dlq_table={config.get('dlq_table')}, "
              f"late_delay={config.get('trend', {}).get('late_delay_seconds')} s")
    if not ok:
        detail = f"enabled={config.get('enabled')} ({config.get('reason')}), dlq={config.get('dlq_reason')}; " + detail
    return Step(step_id, title, OK if ok else FALLA, detail, dict(config))


def _logs_step(ctx: Ctx, gcp: Mapping, job_id: str | None, step_id: str) -> Step:
    title = "Logs de los workers sin errores del lote"
    if not job_id:
        return Step(step_id, title, OMITIDO, "sin id de job")
    terms = " OR ".join(f'"{m}"' for m in [*LOG_MARKERS, "trend_alert"])
    query = f'resource.type="dataflow_step" AND resource.labels.job_id="{job_id}" AND ({terms})'
    proc = ctx.sh(["gcloud", "logging", "read", query, f"--project={gcp['project']}", "--freshness=3h",
                   "--limit=300", "--format=json"], timeout=300)
    if proc.returncode != 0:
        return Step(step_id, title, AVISO, "no se pudieron leer los logs: " + tail(proc.stderr, 3))
    try:
        entries = json.loads(proc.stdout or "[]")
    except ValueError:
        entries = []
    counts = {m: 0 for m in [*LOG_MARKERS, "trend_alert"]}
    for entry in entries:
        text = json.dumps(entry, ensure_ascii=False)
        for marker in counts:
            if marker in text:
                counts[marker] += 1
    worst = OK
    for marker, level in LOG_MARKERS.items():
        if counts[marker]:
            worst = FALLA if level == FALLA or worst == FALLA else AVISO
    detail = ", ".join(f"{k} {v}" for k, v in counts.items())
    return Step(step_id, title, worst, detail, counts)


def part_cloud(ctx: Ctx, args) -> list[Step]:
    steps = []
    gcp = resolve_gcp(ctx)
    name = f"ta-stream-{gcp['env']}"
    if not gcp["project"]:
        steps.append(ctx.add(Step("E1", "Proyecto y credenciales", FALLA, "no hay proyecto (GCP_PROJECT)")))
        return steps
    adc = ctx.sh(["gcloud", "auth", "application-default", "print-access-token"], timeout=60)
    steps.append(ctx.add(Step("E1", "Proyecto y credenciales", OK if adc.returncode == 0 else FALLA,
                              f"{gcp['project']} · {gcp['region']} · job {name}" if adc.returncode == 0
                              else "gcloud auth application-default login", gcp)))
    if adc.returncode != 0:
        return steps

    tables = ctx.evidence_dir / "trends_tables.json"
    proc = ctx.sh([ctx.python, "scripts/verify_trends.py", "--json-out", str(tables), "tables"], timeout=300)
    steps.append(ctx.add(_verifier_step("E2", "Tablas trend_windows, dlq_events, dq_results y vista", proc, tables)))
    if steps[-1].status == FALLA:
        ctx.say("  Aplique primero la parte C: validate_2_4.py terraform --apply")
        return steps

    sim = ctx.sh(["gcloud", "run", "services", "describe", "ta-stream-simulator", f"--project={gcp['project']}",
                  f"--region={gcp['region']}", "--format=value(status.url)"], timeout=120)
    steps.append(ctx.add(Step("E3", "Simulador encendido", OK if sim.returncode == 0 else FALLA,
                              (sim.stdout or "").strip() if sim.returncode == 0 else
                              "encienda el simulador (docs/ACTIVE_SERVICES.md, scripts/on.py) y repita")))
    if sim.returncode != 0:
        return steps

    warmup = args.warmup_seconds
    if args.no_restart:
        jobs = list_jobs(ctx, gcp, name)
        if not jobs:
            steps.append(ctx.add(Step("E4", "Job con el código del 2.4", FALLA, f"no hay {name} activo")))
            return steps
        job = jobs[0]
        since = job.get("creationTime") or iso(ctx.now())
        found = job_has_lote_24_options(ctx, gcp, str(job.get("id")))
        if not found["ok"]:
            steps.append(ctx.add(Step("E4", "Job con el código del 2.4", FALLA,
                                      f"el job activo {job.get('id')} no tiene las opciones del 2.4 ({found['missing']}): "
                                      "corra 'cloud' sin --no-restart", {"job": job})))
            return steps
        steps.append(ctx.add(Step("E4", "Job con el código del 2.4 (--no-restart)", OK,
                                  f"job existente {job.get('id')}, creado {since}: tiene las opciones "
                                  "trend_windows_table y dlq_df_subscription", {"job": job})))
        steps.append(ctx.add(Step("E5", "trends_config del lanzamiento", OMITIDO,
                                  "solo se ve al lanzar el job (--no-restart)")))
        age = ctx.clock() - (_epoch_s(since) or ctx.clock())
        warmup = max(0.0, args.warmup_seconds - age)
    else:
        since = iso(ctx.now())
        step = ctx.add(drain_and_wait(ctx, gcp, name, "Lote 2.4", "E4a",
                                      cancel_after_s=args.cancel_after_minutes * 60))
        steps.append(step)
        if step.status == FALLA:
            return steps
        step, job = _start_job(ctx, gcp, name, ["bash", "scripts/dataflow_job.sh", "start"], "E4",
                               "Job con el código del 2.4 (dataflow_job.sh start)", 1200)
        steps.append(ctx.add(step))
        if not job:
            return steps
        config = step.data.get("trends_config")
        steps.append(ctx.add(_check_trends_config(config, "E5")))

    if warmup > 0:
        ctx.say(f"  Esperando a que los workers carguen los modelos ({warmup / 60:.0f} min)…")
        ctx.sleep(warmup)
    dlq = ctx.evidence_dir / "dlq_check_2_4.json"
    proc = ctx.sh([ctx.python, "scripts/verify_trends.py", "--json-out", str(dlq), "dlq-check", "--allow-cloud"],
                  timeout=1200, stream=True)
    steps.append(ctx.add(_verifier_step("E6", "ta-dlq → dlq_events (dlq-check)", proc, dlq)))

    report = ctx.evidence_dir / "trends_report.json"
    proc = ctx.sh([ctx.python, "scripts/verify_trends.py", "--json-out", str(report), "report", "--since", since,
                   "--wait-caught-up-minutes", str(args.catch_up_minutes), "--watch-minutes", str(args.minutes),
                   "--write-dq"], timeout=(args.minutes + args.catch_up_minutes) * 60 + 1200, stream=True)
    steps.append(ctx.add(_verifier_step("E7", "trend_windows, latencia y dq_results (report)", proc, report)))

    out = ctx.evidence_dir / "streaming_check_2_4.json"
    proc = ctx.sh([ctx.python, "scripts/verify_streaming.py", "check", "--allow-cloud", "--json-out", str(out)],
                  timeout=1500, stream=True)
    steps.append(ctx.add(Step("E8", "Regresión 2.1: verify_streaming.py check", OK if proc.returncode == 0 else FALLA,
                              tail(proc.stdout or proc.stderr, 3))))
    out = ctx.evidence_dir / "nlp_report_2_4.json"
    proc = ctx.sh([ctx.python, "scripts/verify_enriched.py", "report", "--expect-nlp", "--hours", "0.5",
                   "--json-out", str(out)], timeout=900)
    steps.append(ctx.add(Step("E9", "Regresión 2.2/2.3: verify_enriched.py --expect-nlp",
                              OK if proc.returncode == 0 else FALLA, tail(proc.stdout or proc.stderr, 3))))
    steps.append(ctx.add(_logs_step(ctx, gcp, (job or {}).get("id"), "E10")))
    if args.shutdown:
        errors = request_drain(ctx, gcp, list_jobs(ctx, gcp, name))
        steps.append(ctx.add(Step("E11", "Apagado: drenado pedido (termina solo en unos minutos)",
                                  AVISO if errors else OK, "; ".join(errors) or f"{name} drenando")))
    else:
        ctx.say("  El job sigue encendido (~0,25 USD/h): 'bash project/scripts/dataflow_job.sh drain' o "
                "'python project/scripts/off.py' al terminar.")
    return steps


def part_template(ctx: Ctx, args) -> list[Step]:
    steps = []
    gcp = resolve_gcp(ctx)
    name = f"ta-stream-{gcp['env']}"
    params = ctx.evidence_dir / "template_params.json"
    proc = ctx.sh([ctx.python, "scripts/dataflow_template.py", "--json-out", str(params), "params"], timeout=300)
    steps.append(ctx.add(Step("F1", "Parámetros desde los outputs de Terraform", OK if proc.returncode == 0 else FALLA,
                              tail(proc.stderr, 3) if proc.returncode else
                              ", ".join(sorted((load_json(params) or {}).get("parameters", {}))))))
    if proc.returncode != 0:
        return steps
    if args.skip_build:
        steps.append(ctx.add(Step("F2", "Imagen dataflow-launcher y plantilla", OMITIDO, "--skip-build")))
    else:
        build = ctx.evidence_dir / "template_build.json"
        proc = ctx.sh([ctx.python, "scripts/dataflow_template.py", "--json-out", str(build), "build"], timeout=3600,
                      stream=True)
        data = load_json(build) or {}
        steps.append(ctx.add(Step("F2", "Imagen dataflow-launcher y plantilla", OK if proc.returncode == 0 else FALLA,
                                  data.get("image", "") if proc.returncode == 0 else tail(proc.stdout + proc.stderr, 12),
                                  data)))
        if proc.returncode != 0:
            return steps
    since = iso(ctx.now())
    step = ctx.add(drain_and_wait(ctx, gcp, name, "lanzamiento desde la plantilla", "F3",
                                  cancel_after_s=args.cancel_after_minutes * 60))
    steps.append(step)
    if step.status == FALLA:
        return steps
    run = ctx.evidence_dir / "template_run.json"
    step, job = _start_job(ctx, gcp, name, [ctx.python, "scripts/dataflow_template.py", "--json-out", str(run), "run"],
                           "F4", "Job lanzado desde la plantilla Flex", 1500)
    steps.append(ctx.add(step))
    if not job:
        return steps
    describe = ctx.sh(["gcloud", "dataflow", "jobs", "describe", str(job.get("id")), f"--project={gcp['project']}",
                       f"--region={gcp['region']}", "--full", "--format=json"], timeout=120)
    text = describe.stdout or ""
    found = {k: k in text for k in ("dataflow-nlp", "trend_windows", "dlq_events")}
    steps.append(ctx.add(Step("F5", "El job usa la imagen de NLP y las tablas del 2.4",
                              OK if all(found.values()) else AVISO, str(found), found)))
    ctx.say("  Esperando a que los workers carguen los modelos (5 min)…")
    ctx.sleep(args.warmup_seconds)
    report = ctx.evidence_dir / "template_trends_report.json"
    proc = ctx.sh([ctx.python, "scripts/verify_trends.py", "--json-out", str(report), "report", "--since", since,
                   "--wait-caught-up-minutes", str(args.catch_up_minutes), "--watch-minutes", str(args.minutes)],
                  timeout=(args.minutes + args.catch_up_minutes) * 60 + 1200, stream=True)
    steps.append(ctx.add(_verifier_step("F6", "trend_windows con el job de la plantilla", proc, report)))
    out = ctx.evidence_dir / "streaming_check_template.json"
    proc = ctx.sh([ctx.python, "scripts/verify_streaming.py", "check", "--allow-cloud", "--json-out", str(out)],
                  timeout=1500, stream=True)
    steps.append(ctx.add(Step("F7", "verify_streaming.py check con el job de la plantilla",
                              OK if proc.returncode == 0 else FALLA, tail(proc.stdout or proc.stderr, 3))))
    steps.append(ctx.add(_logs_step(ctx, gcp, job.get("id"), "F8")))
    return steps


PARTS = {
    "preflight": part_preflight,
    "tests": part_tests,
    "terraform": part_terraform,
    "local": part_local,
    "cloud": part_cloud,
    "template": part_template,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_2_4.py", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=[*PARTS, "all"])
    parser.add_argument("--apply", action="store_true", help="C: aplica el plan del lote.")
    parser.add_argument("--yes", action="store_true", help="C: no pide confirmación antes de aplicar.")
    parser.add_argument("--full-plan", action="store_true", help="C: plan completo en vez de -target del lote.")
    parser.add_argument("--wait-minutes", type=float, default=15, help="D: espera máxima de ventanas locales.")
    parser.add_argument("--min-windows", type=int, default=3, help="D: ventanas cerradas que se esperan.")
    parser.add_argument("--down", action="store_true", help="D: apaga el entorno local al final.")
    parser.add_argument("--minutes", type=float, default=12, help="E/F: minutos observando trend_windows (≥ 6).")
    parser.add_argument("--catch-up-minutes", type=float, default=15,
                        help="E/F: espera máxima a que el job se ponga al día antes de medir la detección.")
    parser.add_argument("--warmup-seconds", type=float, default=300, help="E/F: espera tras lanzar el job.")
    parser.add_argument("--no-restart", action="store_true",
                        help="E: usa el job activo si tiene las opciones del 2.4 (no drena ni relanza).")
    parser.add_argument("--shutdown", action="store_true", help="E: drena el job al terminar.")
    parser.add_argument("--skip-build", action="store_true", help="F: usa la plantilla ya publicada.")
    parser.add_argument("--cancel-after-minutes", type=float, default=0,
                        help="E/F: si el drenado del job anterior pasa de N min, lo cancela (0 = nunca).")
    return parser


def main(argv: Sequence[str] | None = None, ctx: Ctx | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = ctx or Ctx()
    parts = ["preflight", "tests", "terraform", "local", "cloud"] if args.part == "all" else [args.part]
    overall = []
    for part in parts:
        ctx.say(f"\n=== Lote 2.4 · {part} ===")
        start = len(ctx.steps)
        try:
            PARTS[part](ctx, args)
        except RuntimeError as exc:
            ctx.add(Step("X", f"Error en {part}", FALLA, str(exc)))
        steps = ctx.steps[start:]
        save_evidence(part, steps, path=ctx.evidence_dir / EVIDENCE_FILE.name)
        overall.extend(steps)
        ctx.say(f"--- {part}: {verdict(steps)}")
    ctx.say("\n=== Resumen ===")
    for step in overall:
        ctx.say(f"{step.status:7} {step.id:4} {step.title}")
    final = verdict(overall)
    ctx.say(f"\nResultado: {final} · evidencia en {ctx.evidence_dir / EVIDENCE_FILE.name}")
    return 1 if final == FALLA else 0
