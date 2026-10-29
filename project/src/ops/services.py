"""Encender, apagar y revisar los servicios del proyecto (local y GCP).

Lo usan ``scripts/on.py``, ``scripts/off.py`` y ``scripts/status.py``. No reemplaza a los
scripts de cada lote: los llama (``local_dataflow.sh``, ``local_nlp.sh``, ``dataflow_job.sh``,
Terraform, ``gcloud``) en el orden de ``docs/ACTIVE_SERVICES.md``.

Qué hace cada parte:

* **local**: emulador de Pub/Sub, simulador y ``dataflow-local`` (con NLP si la imagen local
  ``ta-dataflow-nlp-local:local`` existe). Gratis. ``off.py`` no lo apaga salvo que se pida
  (``off.py local`` u ``off.py all``).
* **cloud**: simulador en Cloud Run (``simulator_running`` en ``terraform.tfvars`` + un
  ``apply`` **vigilado**) y el job de Dataflow ``ta-stream-{ENV}``. Es lo que más cobra.
* **flujo** *(v2.51)*: lo que lleva el dato de GCS al tablero. El Scheduler del exportador
  ``ta-gcs-to-s3-cada-15-min`` (Lote 3.2) y el horario del lakehouse
  ``ta-wf-raw-to-curated-cada-hora`` (Lote 4.1). ``on.py cloud`` reanuda los dos.
  ``off.py`` pausa el exportador después de una última corrida que lleva a S3 lo que quedó en
  GCS, y deja el horario del lakehouse como está, encendido por decisión del usuario; con
  ``--lakehouse`` lo deshabilita. ``--stream-only`` vuelve al comportamiento anterior, solo
  simulador y Dataflow. ``cloud_on`` y ``cloud_off`` no cambian, porque los reutiliza
  ``validate62``.

``status.py`` muestra además las alertas (Lote 6.2), QuickSight (Lote 6.1) y una línea
«Flujo de punta a punta» que dice hasta dónde llega hoy un dato nuevo.

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
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

PROJECT_DIR = Path(__file__).resolve().parents[2]
TF_DIR = PROJECT_DIR / "infra" / "gcp" / "environments" / "dev"
TFVARS = TF_DIR / "terraform.tfvars"
COMPOSE_BASE = ["-f", "docker-compose.yml", "-f", "docker-compose.dataflow.yml"]
LOCAL_NLP_IMAGE = "ta-dataflow-nlp-local:local"

EXPORTER_JOB = "ta-gcs-to-s3"
EXPORTER_SCHEDULER = "ta-gcs-to-s3-cada-15-min"
AWS_REGION = "us-east-1"
AWS_ENV = {"AWS_PAGER": ""}
LAKEHOUSE_SCHEDULE = "ta-wf-raw-to-curated-cada-hora"
ALERT_NOTIFIER = "ta-alert-notifier"
ALERT_POLICY = "ta-alerta-tendencia"
#: Costos medidos en USD/día (ACTIVE_SERVICES §0): exportador v2.31, lakehouse v2.38. Solo se muestran.
EXPORTER_USD_DAY = "0,16"
LAKEHOUSE_USD_DAY = "0,47"
#: Lo que tarda un dato nuevo en llegar a cada capa con todo encendido.
#: Archivo de 5 min en GCS, exportador cada 15 min, Glue cada hora y SPICE cada hora.
FLOW_LATENCY = "BigQuery ~20 s · S3 ≤ 25 min · Curated ≤ 1 h 30 min · tablero ≤ 2 h 30 min · alertas < 5 min"

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


INIT_HINT = (
    "Terraform no está inicializado en infra/gcp/environments/dev (falta .terraform/ o el backend).\n"
    "Desde la raíz del repo:\n"
    "  terraform -chdir=project/infra/gcp/environments/dev init -backend-config=backend.hcl\n"
    "Si falta backend.hcl: cópielo de backend.hcl.example "
    "(bucket = \"project-0568d9ba-848f-413c-88b-ta-tfstate\") y repita."
)


def terraform_ready() -> bool:
    return (TF_DIR / ".terraform" / "terraform.tfstate").exists()


def terraform_simulator(run: Runner, value: bool, s: Settings, say: Callable[[str], None]) -> str:
    """Pone ``simulator_running`` y aplica solo si el plan toca únicamente Cloud Run."""
    if not TFVARS.exists():
        raise OpsError(f"no existe {TFVARS.relative_to(PROJECT_DIR)}")
    if not terraform_ready():
        raise OpsError(INIT_HINT)
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
            text = res.err or res.out
            if "terraform init" in text or "Backend initialization required" in text:
                raise OpsError(INIT_HINT)
            raise OpsError("terraform plan falló:\n" + text[-2000:])
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
        if not res.ok and not push_errored_state(run, say):
            raise OpsError("terraform apply falló:\n" + (res.err or res.out)[-2000:])
        return ", ".join(f"{a} ({'/'.join(x)})" for a, _t, x in changes)


def push_errored_state(run: Runner, say: Callable[[str], None]) -> bool:
    """Si el apply cambió la nube pero no pudo guardar el estado (``errored.tfstate``; pasa cuando la red de WSL corta la
    conexión con el bucket), lo sube con ``state push``, que se niega si el estado remoto cambió, y lo saca del repo."""
    errored = TF_DIR / "errored.tfstate"
    if not errored.exists():
        return False
    if not run(["terraform", f"-chdir={TF_DIR}", "state", "push", "errored.tfstate"]).ok:
        say(f"terraform no pudo guardar el estado y state push también falló: queda en {errored}")
        return False
    kept = Path(tempfile.gettempdir()) / f"errored-{int(time.time())}.tfstate"
    shutil.move(errored, kept)
    say(f"terraform no pudo guardar el estado; subido con state push (copia en {kept})")
    return True


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
    """URL del simulador si el servicio existe en Cloud Run.

    Se usa ``describe`` y no ``list``: con Cloud Run v2, ``list --format=value(metadata.name)``
    puede devolver vacío aunque el servicio exista.
    """
    res = run(["gcloud", "run", "services", "describe", "ta-stream-simulator", "--region", s.region,
               "--project", s.project, "--format=value(status.url)"])
    url = res.out.strip() if res.ok else ""
    return url or None


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


SCRIPTS_USED = ("local_dataflow.sh", "local_nlp.sh", "dataflow_job.sh", "build_nlp_image.sh", "hmac_key.sh", "preflight.sh")


def crlf_scripts(scripts_dir: Path | None = None) -> list[str]:
    """Scripts con finales de línea de Windows (CRLF): bash falla con ``set: pipefail``."""
    base = scripts_dir or (PROJECT_DIR / "scripts")
    bad = []
    for name in SCRIPTS_USED:
        f = base / name
        if f.exists() and b"\r\n" in f.read_bytes():
            bad.append(f"project/scripts/{name}")
    return bad


def check_line_endings() -> None:
    bad = crlf_scripts()
    if bad:
        raise OpsError(
            "estos scripts tienen finales de línea de Windows (CRLF) y bash no puede correrlos:\n  - "
            + "\n  - ".join(bad)
            + "\nArréglelo desde la raíz del repo con:\n  sed -i 's/\\r$//' " + " ".join(bad)
            + "\ny agregue un .gitattributes con '*.sh text eol=lf' (ver docs/ACTIVE_SERVICES.md)."
        )


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


SIM_IMAGE_RE = re.compile(r'^\s*simulator_image\s*=\s*"(?P<img>[^"]*)"', re.M)


def read_simulator_image(text: str) -> str | None:
    m = SIM_IMAGE_RE.search(text)
    return m.group("img") if m else None


def cloud_on(run: Runner, s: Settings, say: Report, restart_job: bool = False) -> None:
    image = read_simulator_image(TFVARS.read_text(encoding="utf-8")) if TFVARS.exists() else None
    if not image:
        raise OpsError(
            "simulator_image está vacío o falta en terraform.tfvars: sin imagen, Terraform no crea el "
            "simulador (§5.15).\nVea las imágenes publicadas con:\n"
            f"  gcloud artifacts docker images list {s.region}-docker.pkg.dev/{s.project}/ta-images/stream-simulator "
            "--include-tags --sort-by=~UPDATE_TIME --limit=3 --format=\"value(tags)\"\n"
            f'y agregue: simulator_image = "{s.region}-docker.pkg.dev/{s.project}/ta-images/stream-simulator:ETIQUETA"'
        )
    sim = terraform_simulator(run, True, s, say)
    say(f"simulador: {sim}")
    url = simulator_service(run, s)
    if not url:
        raise OpsError(
            "Terraform terminó pero ta-stream-simulator no existe en Cloud Run. Revise "
            "simulator_image y simulator_running en terraform.tfvars y el plan:\n"
            "  terraform -chdir=project/infra/gcp/environments/dev plan"
        )
    say(f"simulador en Cloud Run: {url}")
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


# --------------------------------------------------------------------------- #
# Flujo GCS → S3 → Glue: exportador (Lote 3.2) y horario del lakehouse (Lote 4.1)
# --------------------------------------------------------------------------- #


def _json(text: str):
    try:
        return json.loads(text or "null")
    except ValueError:
        return None


def exporter_state(run: Runner, s: Settings) -> str | None:
    """``ENABLED``, ``PAUSED``, ``NO_EXISTE`` o ``None`` (sin dato) del Scheduler del exportador."""
    res = run(["gcloud", "scheduler", "jobs", "describe", EXPORTER_SCHEDULER, "--location", s.region,
               "--project", s.project, "--format=value(state)"])
    if not res.ok:
        return "NO_EXISTE" if "NOT_FOUND" in res.err + res.out else None
    return res.out.strip() or None


def exporter_running(run: Runner, s: Settings) -> list[str]:
    """Ejecuciones del job del exportador sin ``completionTime`` (siguen en curso)."""
    res = run(["gcloud", "run", "jobs", "executions", "list", f"--job={EXPORTER_JOB}", "--region", s.region,
               "--project", s.project, "--limit=10", "--format=json"])
    items = _json(res.out) if res.ok else None
    running = []
    for e in items if isinstance(items, list) else []:
        if not ((e.get("status") or {}).get("completionTime") or e.get("completionTime")):
            running.append(str((e.get("metadata") or {}).get("name") or e.get("name") or "?").rsplit("/", 1)[-1])
    return running


def exporter_set(run: Runner, s: Settings, enabled: bool) -> tuple[str | None, str | None]:
    """Reanuda o pausa el Scheduler (Terraform ignora ese campo: no deja deriva). ``(antes, después)``."""
    want = "ENABLED" if enabled else "PAUSED"
    before = exporter_state(run, s)
    if before in ("NO_EXISTE", want):
        return before, before
    res = run(["gcloud", "scheduler", "jobs", "resume" if enabled else "pause", EXPORTER_SCHEDULER,
               "--location", s.region, "--project", s.project])
    after = exporter_state(run, s)
    if after != want:
        raise OpsError(f"no se pudo {'reanudar' if enabled else 'pausar'} {EXPORTER_SCHEDULER} (quedó {after}):\n"
                       + (res.err.strip() or res.out.strip())[-600:])
    return before, after


def exporter_final_run(run: Runner, s: Settings, say: Report, timeout_s: int = 900) -> None:
    """Una última corrida, ya drenado Dataflow, para que lo que quedó en GCS llegue a S3.

    Espera antes a la corrida programada que siga en curso. El watermark se escribe con
    ``if_generation_match``, así que dos corridas a la vez no lo dañan, pero una sola es más clara."""
    deadline = time.time() + timeout_s
    while exporter_running(run, s):
        if time.time() >= deadline:
            say("AVISO: sigue una corrida del exportador tras 15 min; la última corrida queda para el próximo on.py")
            return
        time.sleep(15)
    res = run(["gcloud", "run", "jobs", "execute", EXPORTER_JOB, "--region", s.region, "--project", s.project, "--wait"],
              capture=False)
    if res.ok:
        say("exportador: última corrida OK (lo que quedó en GCS ya está en S3; Glue lo lleva a Curated en su próxima hora)")
    else:
        say(f"AVISO: la última corrida del exportador falló; lo pendiente sale en el próximo on.py "
            f"(o: gcloud run jobs execute {EXPORTER_JOB} --region {s.region} --wait)")


def schedule_update_input(schedule: dict, state: str) -> dict:
    """Lo que ``aws scheduler update-schedule`` necesita para cambiar solo el estado.

    La API reemplaza la definición entera: lo que se omite vuelve al valor por defecto (por ejemplo,
    185 reintentos). Es la misma función que ``validate41.schedule_update_input``."""
    keys = ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow", "Target",
            "Description", "StartDate", "EndDate", "KmsKeyArn", "ActionAfterCompletion")
    out = {k: schedule[k] for k in keys if schedule.get(k) not in (None, "")}
    out["State"] = state
    return out


def lakehouse_schedule(run: Runner) -> tuple[str | None, dict]:
    """Estado (``ENABLED``, ``DISABLED``, ``NO_EXISTE`` o ``None``) y definición del horario del lakehouse."""
    res = run(["aws", "scheduler", "get-schedule", "--name", LAKEHOUSE_SCHEDULE, "--region", AWS_REGION,
               "--output", "json"], env=AWS_ENV)
    if not res.ok:
        return ("NO_EXISTE" if "ResourceNotFoundException" in res.err + res.out else None), {}
    data = _json(res.out)
    data = data if isinstance(data, dict) else {}
    return data.get("State"), data


def lakehouse_set(run: Runner, enabled: bool) -> tuple[str | None, str | None]:
    """Habilita o deshabilita el horario sin tocar el resto de su definición. ``(antes, después)``."""
    want = "ENABLED" if enabled else "DISABLED"
    before, data = lakehouse_schedule(run)
    if before in ("NO_EXISTE", want):
        return before, before
    if before is None:
        raise OpsError(f"no se pudo leer el horario {LAKEHOUSE_SCHEDULE}; revise las credenciales de AWS en WSL:\n"
                       f"  aws scheduler get-schedule --name {LAKEHOUSE_SCHEDULE} --region {AWS_REGION}")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "schedule.json"
        path.write_text(json.dumps(schedule_update_input(data, want)), encoding="utf-8")
        res = run(["aws", "scheduler", "update-schedule", "--cli-input-json", f"file://{path}",
                   "--region", AWS_REGION, "--output", "json"], env=AWS_ENV)
    after, _ = lakehouse_schedule(run)
    if after != want:
        raise OpsError(f"no se pudo poner {LAKEHOUSE_SCHEDULE} en {want} (quedó {after}):\n"
                       + (res.err.strip() or res.out.strip())[-600:])
    return before, after


def _change(before: str | None, after: str | None) -> str:
    return f"ya estaba {after}" if before == after else f"{before or 'sin dato'} → {after}"


def flow_on(run: Runner, s: Settings, say: Report) -> None:
    """Reanuda el exportador y habilita el horario del lakehouse (lo que lleva el dato al tablero)."""
    before, after = exporter_set(run, s, True)
    if after == "NO_EXISTE":
        say("exportador: no existe (Lote 3.2 sin aplicar); el dato se queda en GCS")
    else:
        say(f"exportador: {_change(before, after)} (cada 15 min, ≈ {EXPORTER_USD_DAY} USD/día)")
    before, after = lakehouse_set(run, True)
    if after == "NO_EXISTE":
        say("lakehouse: el horario no existe (Lote 4.1 sin aplicar); el dato se queda en S3 Raw")
    else:
        say(f"lakehouse: horario {_change(before, after)} (cada hora, ≈ {LAKEHOUSE_USD_DAY} USD/día)")


def flow_off(run: Runner, s: Settings, say: Report, final_export: bool = True, lakehouse: bool = False) -> None:
    """Pausa el exportador (con una última corrida si estaba encendido) y, si se pide, el lakehouse."""
    before, after = exporter_set(run, s, False)
    if after == "NO_EXISTE":
        say("exportador: no existe (Lote 3.2 sin aplicar)")
    else:
        say(f"exportador: {_change(before, after)}")
    if final_export and before == "ENABLED":
        if active_jobs(run, s):
            say("exportador: Dataflow sigue drenando; lo que falta sale en la primera corrida del próximo on.py")
        else:
            exporter_final_run(run, s, say)
    if lakehouse:
        before, after = lakehouse_set(run, False)
        say(f"lakehouse: horario {_change(before, after)}")
    else:
        state, _ = lakehouse_schedule(run)
        say(f"lakehouse: horario {state or 'sin dato'}; se deja como está (off.py --lakehouse lo deshabilita)")


# --------------------------------------------------------------------------- #
# Estado
# --------------------------------------------------------------------------- #


def flow_summary(simulator: bool, dataflow: bool, exporter: str | None, lakehouse: str | None) -> str:
    """Hasta dónde llega hoy un dato nuevo: simulador → Pub/Sub → Dataflow → GCS → S3 → Glue → tablero."""
    links = [
        ("simulador", simulator, "no entra dato nuevo"),
        ("Dataflow", dataflow, "el dato se queda en Pub/Sub"),
        ("exportador", exporter == "ENABLED", "llega a BigQuery, GCS y alertas, no a AWS"),
        ("horario del lakehouse", lakehouse == "ENABLED", "llega a S3 Raw, no a Curated ni al tablero"),
    ]
    missing = [(name, reach) for name, on, reach in links if not on]
    if not missing:
        return f"Flujo de punta a punta: COMPLETO ({FLOW_LATENCY})"
    first, reach = missing[0]
    others = [name for name, _r in missing[1:]]
    return (f"Flujo de punta a punta: CORTADO en {first} ({reach})"
            + (f"; también apagado: {', '.join(others)}" if others else ""))


def alerts_line(run: Runner, s: Settings) -> str:
    res = run(["gcloud", "run", "services", "describe", ALERT_NOTIFIER, "--region", s.region,
               "--project", s.project, "--format=value(status.url)"])
    if not (res.ok and res.out.strip()):
        return "GCP alertas: sin notificador (Lote 6.2 sin aplicar)"
    pol = run(["gcloud", "monitoring", "policies", "list", "--project", s.project,
               f'--filter=displayName="{ALERT_POLICY}"', "--format=value(enabled)"])
    enabled = pol.out.strip().lower() if pol.ok else ""
    policy = {"true": "política activa", "false": "política DESACTIVADA"}.get(enabled, "política sin dato")
    return f"GCP alertas: {ALERT_NOTIFIER} listo (escala a cero) · {policy} · solo hay alertas con Dataflow"


def quicksight_line(run: Runner, now: datetime | None = None) -> str:
    """Suscripción de QuickSight y días de prueba (Lote 6.1). Después de la prueba, cobra por usuario."""
    ident = run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"], env=AWS_ENV)
    account = ident.out.strip() if ident.ok else ""
    if not account.isdigit():
        return "AWS QuickSight: sin dato (¿credenciales de AWS?)"
    res = run(["aws", "quicksight", "describe-account-subscription", "--aws-account-id", account,
               "--region", AWS_REGION, "--output", "json"], env=AWS_ENV)
    if not res.ok:
        return "AWS QuickSight: sin suscripción" if "ResourceNotFoundException" in res.err + res.out else "AWS QuickSight: sin dato"
    info = (_json(res.out) or {}).get("AccountInfo") or {}
    sub = info.get("AccountSubscriptionStatus") or "?"
    if sub == "UNSUBSCRIBED":
        return "AWS QuickSight: UNSUBSCRIBED (no cobra)"
    try:
        from ops.validate61 import trial_end  # tarde: validate61 importa este módulo
        end = trial_end()
    except Exception:  # noqa: BLE001 - el estado solo informa
        return f"AWS QuickSight: {sub}"
    left = (end - (now or datetime.now(timezone.utc))).days
    unsubscribe = "python project/scripts/validate_6_1.py unsubscribe --confirm-unsubscribe"
    if left >= 0:
        return f"AWS QuickSight: {sub} · prueba gratis hasta ≈ {end:%Y-%m-%d} ({left} días); al terminar el proyecto: {unsubscribe}"
    return f"AWS QuickSight: {sub} (cobra) por usuario desde ≈ {end:%Y-%m-%d}; si ya no se usa: {unsubscribe}"


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
    url = simulator_service(run, s)
    lines.append(f"GCP simulador: {'ENCENDIDO (cobra) · ' + url if url else 'apagado'}"
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
    exporter = exporter_state(run, s)
    lines.append("GCP exportador: " + {
        "ENABLED": f"ENCENDIDO (cobra) · {EXPORTER_SCHEDULER} cada 15 min, ≈ {EXPORTER_USD_DAY} USD/día",
        "PAUSED": f"en pausa ({EXPORTER_SCHEDULER} PAUSED)",
        "NO_EXISTE": "no existe (Lote 3.2 sin aplicar)",
    }.get(exporter or "", f"sin dato ({exporter})" if exporter else "sin dato"))
    lines.append(alerts_line(run, s))
    lakehouse, _ = lakehouse_schedule(run)
    lines.append("AWS lakehouse: " + {
        "ENABLED": f"horario {LAKEHOUSE_SCHEDULE} ENABLED (cada hora, ≈ {LAKEHOUSE_USD_DAY} USD/día; estado normal)",
        "DISABLED": f"horario {LAKEHOUSE_SCHEDULE} DISABLED (ahorro; on.py lo habilita)",
        "NO_EXISTE": "el horario no existe (Lote 4.1 sin aplicar)",
    }.get(lakehouse or "", "sin dato (¿credenciales de AWS?)"))
    lines.append(quicksight_line(run))
    subs = verify_subscriptions(run, s)
    builds = ongoing_builds(run, s)
    if subs:
        lines.append(f"GCP suscripciones temporales: {', '.join(subs)}")
    if builds:
        lines.append(f"GCP Cloud Build: {builds} en curso")
    running = any(str(j.get("state") or j.get("currentState")).lower().endswith("running") for j in jobs)
    lines.append(flow_summary(bool(url), running, exporter, lakehouse))
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
        p.add_argument("--stream-only", action="store_true",
                       help="solo simulador y Dataflow: no reanuda el exportador ni habilita el horario del lakehouse")
    else:
        p.add_argument("--keep-simulator", action="store_true", help="apaga Dataflow pero deja el simulador")
        p.add_argument("--force", action="store_true", help="cancela el job si sigue drenando tras 15 min")
        p.add_argument("--stream-only", action="store_true", help="solo Dataflow y simulador: no pausa el exportador")
        p.add_argument("--no-final-export", action="store_true",
                       help="pausa el exportador sin la última corrida que lleva a S3 lo que quedó en GCS")
        p.add_argument("--lakehouse", action="store_true",
                       help=f"también deshabilita el horario del lakehouse de AWS (≈ {LAKEHOUSE_USD_DAY} USD/día)")
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
        flow = cloud and not args.stream_only
        plan = []
        if args.target in ("all", "local"):
            plan.append("local: " + ("emulador, simulador y dataflow-local" if action == "on" else "apagar todo (down-all)"))
        if cloud:
            if action == "on":
                plan.append(f"GCP ({s.project}): simulador en Cloud Run y job {s.job_prefix} (~2 USD/día + ~0,25 USD/h)")
                if flow:
                    plan.append(f"GCP: reanudar el exportador GCS → S3 cada 15 min (≈ {EXPORTER_USD_DAY} USD/día)")
                    plan.append(f"AWS: horario del lakehouse cada hora (≈ {LAKEHOUSE_USD_DAY} USD/día; "
                                "normalmente ya está encendido)")
            else:
                plan.append(f"GCP ({s.project}): drenar {s.job_prefix}"
                            + ("" if getattr(args, "keep_simulator", False) else ", apagar el simulador")
                            + " y borrar suscripciones ta-verify-*")
                if flow:
                    plan.append("GCP: pausar el exportador" + ("" if args.no_final_export else
                                ", tras una última corrida que lleva a S3 lo que quede en GCS"))
                    plan.append("AWS: deshabilitar el horario del lakehouse" if args.lakehouse else
                                "AWS: el horario del lakehouse se deja como está (--lakehouse lo deshabilita)")
        check_line_endings()
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
            if flow:
                flow_on(run, s, say)  # primero: es rápido y falla pronto si faltan las credenciales de AWS
            if cloud:
                cloud_on(run, s, say, restart_job=args.restart_job)
            if flow:
                say(f"con todo encendido: {FLOW_LATENCY}")
        else:
            if cloud:
                cloud_off(run, s, say, keep_simulator=args.keep_simulator)
            if flow:
                flow_off(run, s, say, final_export=not args.no_final_export, lakehouse=args.lakehouse)
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
