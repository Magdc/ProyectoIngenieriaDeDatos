"""Encender, apagar y revisar los servicios del proyecto (local y GCP).

Lo usan ``scripts/on.py``, ``scripts/off.py`` y ``scripts/status.py``. No reemplaza a los
scripts de cada lote: los llama (``local_dataflow.sh``, ``local_nlp.sh``, ``dataflow_job.sh``,
Terraform, ``gcloud``) en el orden de ``docs/ACTIVE_SERVICES.md``.

Qué hace cada parte:

* **local**: emulador de Pub/Sub, simulador y ``dataflow-local`` (con NLP si la imagen local
  ``ta-dataflow-nlp-local:local`` existe). Gratis. ``off.py`` no lo apaga salvo que se pida
  (``off.py local`` u ``off.py all``).
* **cloud**: simulador en Cloud Run (``simulator_running`` en ``terraform.tfvars`` + un
  ``apply`` **vigilado**) y el job de Dataflow ``ta-stream-{ENV}``. Es lo único que cobra.

Seguridad:

* Terraform se aplica solo si el ``plan`` cambia **únicamente** recursos de Cloud Run (el
  simulador). Si propone cualquier otra cosa, se detiene sin aplicar y lo explica.
* Nada se borra salvo el servicio del simulador (al apagar) y las suscripciones temporales
  ``ta-verify-*`` de los verificadores. Topics, buckets, BigQuery, el secreto y las imágenes
  no se tocan.
* Antes de actuar muestra lo que va a hacer y pide confirmación (``--yes`` la omite).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence

PROJECT_DIR = Path(__file__).resolve().parents[2]
TF_DIR = PROJECT_DIR / "infra" / "gcp" / "environments" / "dev"
TFVARS = TF_DIR / "terraform.tfvars"
COMPOSE_BASE = ["-f", "docker-compose.yml", "-f", "docker-compose.dataflow.yml"]
LOCAL_NLP_IMAGE = "ta-dataflow-nlp-local:local"

_SIM_RE = re.compile(r"^(?P<lead>\s*simulator_running\s*=\s*)(?P<val>true|false)\b(?P<rest>.*)$", re.M)


# --------------------------------------------------------------------------- #
# Ejecución de comandos (inyectable en pruebas)
# --------------------------------------------------------------------------- #


@dataclass
class Result:
    code: int
    out: str = ""
    err: str = ""

    @property
    def ok(self) -> bool:
        return self.code == 0


class Runner:
    """Ejecuta comandos en ``project/``. ``capture=False`` muestra la salida en vivo."""

    def __init__(self, cwd: Path = PROJECT_DIR, echo: Callable[[str], None] = print):
        self.cwd = cwd
        self.echo = echo

    def __call__(self, args: Sequence[str], *, capture: bool = True, env: dict | None = None, check: bool = False) -> Result:
        full_env = dict(os.environ)
        if env:
            for k, v in env.items():
                if v is None:
                    full_env.pop(k, None)
                else:
                    full_env[k] = v
        if not capture:
            self.echo(f"$ {' '.join(args)}")
        try:
            proc = subprocess.run(
                list(args), cwd=self.cwd, env=full_env, text=True,
                stdout=subprocess.PIPE if capture else None,
                stderr=subprocess.PIPE if capture else None,
            )
        except FileNotFoundError:
            return Result(127, "", f"no se encontró el comando: {args[0]}")
        res = Result(proc.returncode, proc.stdout or "", proc.stderr or "")
        if check and not res.ok:
            raise OpsError(f"falló: {' '.join(args)}\n{res.err.strip() or res.out.strip()}")
        return res


class OpsError(RuntimeError):
    """Error que detiene la operación con un mensaje claro."""


# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #


@dataclass
class Settings:
    project: str
    region: str = "us-central1"
    env: str = "dev"
    nlp: str = "auto"  # auto | on | off
    yes: bool = False
    wait: bool = True
    force: bool = False

    @property
    def job_prefix(self) -> str:
        return f"ta-stream-{self.env}"

    @property
    def nlp_repo(self) -> str:
        return f"{self.region}-docker.pkg.dev/{self.project}/ta-images/dataflow-nlp"


def detect_project(run: Runner) -> str:
    project = os.environ.get("GCP_PROJECT")
    if project:
        return project
    res = run(["gcloud", "config", "get-value", "project"])
    value = res.out.strip()
    if res.ok and value and value != "(unset)":
        return value
    raise OpsError("no hay proyecto de GCP: export GCP_PROJECT=project-0568d9ba-848f-413c-88b")


# --------------------------------------------------------------------------- #
# terraform.tfvars
# --------------------------------------------------------------------------- #


def read_simulator_running(text: str) -> bool | None:
    m = _SIM_RE.search(text)
    return None if m is None else m.group("val") == "true"


def set_simulator_running(text: str, value: bool) -> str:
    """Cambia o agrega ``simulator_running`` sin tocar la alineación del resto."""
    val = "true" if value else "false"
    if _SIM_RE.search(text):
        return _SIM_RE.sub(lambda m: f"{m.group('lead')}{val}{m.group('rest')}", text, count=1)
    sep = "" if text.endswith("\n") or not text else "\n"
    return f"{text}{sep}\nsimulator_running = {val}\n"


# --------------------------------------------------------------------------- #
# Plan de Terraform vigilado
# --------------------------------------------------------------------------- #


def plan_changes(plan_json: dict) -> list[tuple[str, str, list[str]]]:
    """``[(dirección, tipo, acciones)]`` de los recursos que cambian (sin ``no-op``/``read``)."""
    out = []
    for rc in plan_json.get("resource_changes") or []:
        actions = list((rc.get("change") or {}).get("actions") or [])
        if not actions or actions in (["no-op"], ["read"]):
            continue
        out.append((rc.get("address", "?"), rc.get("type", "?"), actions))
    return out


def unexpected_changes(changes: list[tuple[str, str, list[str]]], wanted: str) -> list[str]:
    """Cambios que no son el esperado. ``wanted`` = ``create`` (encender) o ``delete`` (apagar).

    Solo se admiten recursos de Cloud Run, y con la acción esperada o un ``update`` (p. ej.
    la deriva de ``TA_RESTART_NONCE`` del Lote 1.4)."""
    bad = []
    for address, rtype, actions in changes:
        if not rtype.startswith("google_cloud_run"):
            bad.append(f"{address} ({'/'.join(actions)})")
            continue
        if not set(actions) <= {wanted, "update"}:
            bad.append(f"{address} ({'/'.join(actions)})")
    return bad


def terraform_simulator(run: Runner, value: bool, s: Settings, say: Callable[[str], None]) -> str:
    """Pone ``simulator_running`` y aplica solo si el plan toca únicamente Cloud Run."""
    if not TFVARS.exists():
        raise OpsError(f"no existe {TFVARS.relative_to(PROJECT_DIR)}")
    original = TFVARS.read_text(encoding="utf-8")
    updated = set_simulator_running(original, value)
    if updated != original:
        TFVARS.write_text(updated, encoding="utf-8")
        say(f"terraform.tfvars: simulator_running = {'true' if value else 'false'}")
    with tempfile.TemporaryDirectory() as tmp:
        plan_file = os.path.join(tmp, "tfplan")
        res = run(["terraform", f"-chdir={TF_DIR}", "plan", "-input=false", "-no-color",
                   "-lock-timeout=60s", f"-out={plan_file}"])
        if not res.ok:
            raise OpsError("terraform plan falló:\n" + (res.err or res.out)[-2000:])
        shown = run(["terraform", f"-chdir={TF_DIR}", "show", "-json", plan_file], check=True)
        changes = plan_changes(json.loads(shown.out))
        if not changes:
            return "sin cambios"
        bad = unexpected_changes(changes, "create" if value else "delete")
        if bad:
            raise OpsError(
                "el plan de Terraform cambia más que el simulador; no se aplica nada:\n  - "
                + "\n  - ".join(bad)
                + "\nRevíselo a mano: terraform -chdir=project/infra/gcp/environments/dev plan"
            )
        res = run(["terraform", f"-chdir={TF_DIR}", "apply", "-input=false", "-no-color", plan_file])
        if not res.ok:
            raise OpsError("terraform apply falló:\n" + (res.err or res.out)[-2000:])
        return ", ".join(f"{a} ({'/'.join(x)})" for a, _t, x in changes)


# --------------------------------------------------------------------------- #
# GCP: estado
# --------------------------------------------------------------------------- #


def active_jobs(run: Runner, s: Settings) -> list[dict]:
    res = run(["gcloud", "dataflow", "jobs", "list", "--region", s.region, "--status=active",
               "--project", s.project, "--format=json"])
    if not res.ok:
        raise OpsError("no se pudo listar los jobs de Dataflow:\n" + res.err.strip())
    jobs = json.loads(res.out or "[]")
    return [j for j in jobs if str(j.get("name", "")).startswith(s.job_prefix)]


def simulator_service(run: Runner, s: Settings) -> str | None:
    res = run(["gcloud", "run", "services", "list", "--region", s.region, "--project", s.project,
               "--format=value(metadata.name)"])
    names = res.out.split() if res.ok else []
    return "ta-stream-simulator" if "ta-stream-simulator" in names else None


def verify_subscriptions(run: Runner, s: Settings) -> list[str]:
    res = run(["gcloud", "pubsub", "subscriptions", "list", "--project", s.project, "--format=value(name)"])
    if not res.ok:
        return []
    return [n.rsplit("/", 1)[-1] for n in res.out.split() if "/ta-verify-" in n or n.startswith("ta-verify-")]


def ongoing_builds(run: Runner, s: Settings) -> int:
    res = run(["gcloud", "builds", "list", "--region", s.region, "--project", s.project, "--ongoing", "--format=value(id)"])
    return len(res.out.split()) if res.ok else 0


def latest_nlp_image(run: Runner, s: Settings) -> str | None:
    """La imagen ``dataflow-nlp`` publicada más reciente, con su etiqueta de commit."""
    res = run(["gcloud", "artifacts", "docker", "images", "list", s.nlp_repo, "--include-tags",
               "--sort-by=~UPDATE_TIME", "--limit=1", "--format=json", "--project", s.project])
    if not res.ok:
        return None
    items = json.loads(res.out or "[]")
    if not items:
        return None
    tags = items[0].get("tags") or []
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    commit_tags = [t for t in tags if t != "latest"]
    tag = commit_tags[0] if commit_tags else ("latest" if "latest" in tags else None)
    return f"{s.nlp_repo}:{tag}" if tag else None


def nlp_code_present() -> bool:
    return (PROJECT_DIR / "src" / "streaming" / "nlp.py").exists()


def nlp_env(run: Runner, s: Settings, say: Callable[[str], None]) -> dict:
    """Variables para ``dataflow_job.sh start`` según ``--nlp`` (Lote 2.3)."""
    if not nlp_code_present():
        return {"TA_NLP_ENABLED": None, "TA_NLP_IMAGE": None}
    if s.nlp == "off":
        say("NLP: apagado (--nlp off)")
        return {"TA_NLP_ENABLED": "false", "TA_NLP_IMAGE": None}
    image = os.environ.get("TA_NLP_IMAGE") or latest_nlp_image(run, s)
    if image:
        say(f"NLP: encendido con {image}")
        return {"TA_NLP_ENABLED": None, "TA_NLP_IMAGE": image}
    if s.nlp == "on":
        raise OpsError("--nlp on pero no hay imagen dataflow-nlp publicada: bash scripts/build_nlp_image.sh cloudbuild")
    say("NLP: apagado (no hay imagen dataflow-nlp publicada todavía)")
    return {"TA_NLP_ENABLED": "false", "TA_NLP_IMAGE": None}


# --------------------------------------------------------------------------- #
# Acciones
# --------------------------------------------------------------------------- #


@dataclass
class Report:
    lines: list[str] = field(default_factory=list)

    def __call__(self, line: str) -> None:
        self.lines.append(line)
        print(f"  · {line}", flush=True)


def confirm(prompt: str, s: Settings) -> bool:
    if s.yes:
        return True
    if not sys.stdin.isatty():
        raise OpsError("sin terminal para confirmar: use --yes")
    return input(f"{prompt} [s/N] ").strip().lower() in ("s", "si", "sí", "y", "yes")


def docker_ok(run: Runner) -> bool:
    return run(["docker", "info", "--format", "{{.ServerVersion}}"]).ok


def local_on(run: Runner, s: Settings, say: Report) -> None:
    if not docker_ok(run):
        raise OpsError("Docker no responde: abra Docker Desktop (con la integración de WSL) y repita")
    res = run(["bash", "scripts/local_dataflow.sh", "up"], capture=False)
    if not res.ok:
        raise OpsError("local_dataflow.sh up falló")
    want_nlp = s.nlp != "off" and (PROJECT_DIR / "scripts" / "local_nlp.sh").exists()
    has_image = run(["docker", "image", "inspect", LOCAL_NLP_IMAGE]).ok
    if want_nlp and has_image:
        res = run(["bash", "scripts/local_nlp.sh", "up"], capture=False)
        if not res.ok:
            raise OpsError("local_nlp.sh up falló")
        say("local: emulador, simulador y dataflow-local con NLP")
    elif s.nlp == "on" and not has_image:
        raise OpsError("--nlp on pero no existe la imagen local: bash scripts/local_nlp.sh build")
    else:
        say("local: emulador, simulador y dataflow-local" + (" (sin NLP: falta la imagen local)" if want_nlp else ""))


def local_off(run: Runner, s: Settings, say: Report) -> None:
    if not docker_ok(run):
        say("local: Docker no responde; nada que apagar")
        return
    res = run(["bash", "scripts/local_dataflow.sh", "down-all"], capture=False)
    if not res.ok:
        raise OpsError("local_dataflow.sh down-all falló")
    say("local: apagado (se conservan el cursor y .local-lake/)")


def wait_running(run: Runner, s: Settings, say: Report, timeout_s: int = 900) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        jobs = active_jobs(run, s)
        states = {j.get("state") or j.get("currentState") for j in jobs}
        if any(str(x).lower().endswith("running") for x in states):
            say(f"Dataflow: {jobs[0].get('name')} en Running")
            return
        time.sleep(20)
    say("Dataflow: aún no está en Running; revise con bash scripts/dataflow_job.sh status")


def cloud_on(run: Runner, s: Settings, say: Report, restart_job: bool = False) -> None:
    sim = terraform_simulator(run, True, s, say)
    say(f"simulador: {sim}")
    jobs = active_jobs(run, s)
    if jobs and not restart_job:
        say(f"Dataflow: ya hay un job activo ({jobs[0].get('name')}, {jobs[0].get('id')}); se deja como está. "
            "Use --restart-job si copió código nuevo")
        return
    if jobs and restart_job:
        drain_jobs(run, s, say)
    if not run([sys.executable, "-c", "import apache_beam"]).ok:
        raise OpsError("este Python no tiene Apache Beam: active ~/.venvs/ta-streaming antes de lanzar el job")
    env = nlp_env(run, s, say)
    env.update({"GCP_PROJECT": s.project, "GCP_REGION": s.region, "ENV": s.env})
    res = run(["bash", "scripts/dataflow_job.sh", "start"], capture=False, env=env)
    if not res.ok:
        raise OpsError("dataflow_job.sh start falló")
    if s.wait:
        wait_running(run, s, say)


def drain_jobs(run: Runner, s: Settings, say: Report, timeout_s: int = 900) -> None:
    jobs = active_jobs(run, s)
    if not jobs:
        say("Dataflow: ningún job activo")
        return
    for j in jobs:
        jid = j.get("id") or j.get("jobId")
        r = run(["gcloud", "dataflow", "jobs", "drain", jid, "--region", s.region, "--project", s.project])
        if not r.ok:
            raise OpsError(f"no se pudo drenar {jid}: {r.err.strip()}")
        say(f"Dataflow: drenando {j.get('name')} ({jid})")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not active_jobs(run, s):
            say("Dataflow: drenado")
            return
        time.sleep(20)
    left = active_jobs(run, s)
    if s.force:
        for j in left:
            jid = j.get("id") or j.get("jobId")
            run(["gcloud", "dataflow", "jobs", "cancel", jid, "--region", s.region, "--project", s.project])
            say(f"Dataflow: cancelado {jid} (seguía drenando)")
    else:
        say("Dataflow: sigue drenando tras 15 min; repita off con --force para cancelarlo")


def cloud_off(run: Runner, s: Settings, say: Report, keep_simulator: bool = False) -> None:
    drain_jobs(run, s, say)
    if keep_simulator:
        say("simulador: se deja encendido (--keep-simulator)")
    else:
        sim = terraform_simulator(run, False, s, say)
        say(f"simulador: {sim}")
    for sub in verify_subscriptions(run, s):
        run(["gcloud", "pubsub", "subscriptions", "delete", sub, "--project", s.project, "--quiet"])
        say(f"suscripción temporal borrada: {sub}")
    n = ongoing_builds(run, s)
    if n:
        say(f"AVISO: {n} construcción(es) de Cloud Build en curso; terminan solas")


def status(run: Runner, s: Settings | None) -> list[str]:
    lines = []
    if docker_ok(run):
        res = run(["docker", "compose", *COMPOSE_BASE, "--profile", "dataflow", "ps", "--format", "{{.Service}}:{{.State}}"])
        services = [x for x in res.out.split() if x] if res.ok else []
        lines.append("local: " + (", ".join(services) if services else "nada encendido"))
    else:
        lines.append("local: Docker no responde")
    if s is None:
        lines.append("GCP: sin proyecto (export GCP_PROJECT=…)")
        return lines
    tf = read_simulator_running(TFVARS.read_text(encoding="utf-8")) if TFVARS.exists() else None
    lines.append(f"GCP simulador: {'ENCENDIDO (cobra)' if simulator_service(run, s) else 'apagado'}"
                 f" · terraform.tfvars simulator_running = {tf}")
    try:
        jobs = active_jobs(run, s)
    except OpsError as exc:
        lines.append(f"GCP Dataflow: {exc}")
        jobs = []
    if jobs:
        for j in jobs:
            lines.append(f"GCP Dataflow: {j.get('name')} {j.get('state') or j.get('currentState')} (cobra) · {j.get('id')}")
    else:
        lines.append("GCP Dataflow: ningún job activo")
    subs = verify_subscriptions(run, s)
    builds = ongoing_builds(run, s)
    if subs:
        lines.append(f"GCP suscripciones temporales: {', '.join(subs)}")
    if builds:
        lines.append(f"GCP Cloud Build: {builds} en curso")
    return lines


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _parse(argv: list[str], action: str):
    import argparse

    p = argparse.ArgumentParser(prog=f"{action}.py", description=f"{'Enciende' if action == 'on' else 'Apaga'} los servicios del proyecto.")
    default = "all" if action == "on" else "cloud"
    p.add_argument("target", nargs="?", choices=("all", "local", "cloud"), default=default,
                   help=f"qué {'encender' if action == 'on' else 'apagar'} (por defecto: {default})")
    p.add_argument("--yes", "-y", action="store_true", help="no pedir confirmación")
    if action == "on":
        p.add_argument("--nlp", choices=("auto", "on", "off"), default="auto",
                       help="NLP del Lote 2.3: auto (si hay imagen), on u off")
        p.add_argument("--restart-job", action="store_true", help="drena el job activo y lanza uno nuevo (tras copiar código)")
        p.add_argument("--no-wait", action="store_true", help="no esperar a que el job esté en Running")
    else:
        p.add_argument("--keep-simulator", action="store_true", help="apaga Dataflow pero deja el simulador")
        p.add_argument("--force", action="store_true", help="cancela el job si sigue drenando tras 15 min")
    p.add_argument("--region", default=os.environ.get("GCP_REGION", "us-central1"))
    p.add_argument("--env", default=os.environ.get("ENV", "dev"))
    return p.parse_args(argv)


def main(action: str, argv: list[str] | None = None, run: Runner | None = None) -> int:
    run = run or Runner()
    if action == "status":
        try:
            s = Settings(project=detect_project(run), region=os.environ.get("GCP_REGION", "us-central1"), env=os.environ.get("ENV", "dev"))
        except OpsError:
            s = None
        for line in status(run, s):
            print(line)
        return 0
    args = _parse(sys.argv[1:] if argv is None else argv, action)
    try:
        cloud = args.target in ("all", "cloud")
        s = Settings(
            project=detect_project(run) if cloud else "", region=args.region, env=args.env,
            nlp=getattr(args, "nlp", "auto"), yes=args.yes,
            wait=not getattr(args, "no_wait", False), force=getattr(args, "force", False),
        )
        plan = []
        if args.target in ("all", "local"):
            plan.append("local: " + ("emulador, simulador y dataflow-local" if action == "on" else "apagar todo (down-all)"))
        if cloud:
            if action == "on":
                plan.append(f"GCP ({s.project}): simulador en Cloud Run y job {s.job_prefix} (~2 USD/día + ~0,25 USD/h)")
            else:
                plan.append(f"GCP ({s.project}): drenar {s.job_prefix}"
                            + ("" if getattr(args, "keep_simulator", False) else ", apagar el simulador")
                            + " y borrar suscripciones ta-verify-*")
        print(("Encender:" if action == "on" else "Apagar:"))
        for line in plan:
            print(f"  - {line}")
        if not confirm("¿Continuar?", s):
            print("Cancelado.")
            return 1
        say = Report()
        if action == "on":
            if args.target in ("all", "local"):
                local_on(run, s, say)
            if cloud:
                cloud_on(run, s, say, restart_job=args.restart_job)
        else:
            if cloud:
                cloud_off(run, s, say, keep_simulator=args.keep_simulator)
            if args.target in ("all", "local"):
                local_off(run, s, say)
        print("\nEstado:")
        for line in status(run, s if cloud else None):
            print(f"  {line}")
        return 0
    except OpsError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nInterrumpido.", file=sys.stderr)
        return 130
