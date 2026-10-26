"""Validación automática del Lote 3.1 (HU-024): ``docs/validation/validation-3-1.md``.

Desde la raíz del repo, con ``~/.venvs/ta-streaming`` activado::

    python project/scripts/validate_3_1.py preflight          # A · código, dependencias y doctor
    python project/scripts/validate_3_1.py tests              # B · pruebas
    python project/scripts/validate_3_1.py terraform [--apply]   # C · bucket, KMS y rol en AWS
    python project/scripts/validate_3_1.py local [--build]    # D · prueba de humo con NLP real (600 filas)
    python project/scripts/validate_3_1.py load               # E · carga completa (50.000 filas)
    python project/scripts/validate_3_1.py upload             # F · subida a S3 e idempotencia

Cada parte deja su resumen en ``project/evidencia/validacion_3_1.json``. Los comandos
corren desde ``project/`` sin entrada estándar; los largos muestran su salida en vivo.
No usa ``ops/services.py`` (no estaba disponible al prepararlo, igual que el 2.4): llama a
los scripts documentados y a ``terraform``, ``aws``, ``gcloud`` y ``docker``.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Sequence

PROJECT_DIR = Path(__file__).resolve().parents[2]
EVIDENCE = PROJECT_DIR / "evidencia" / "validacion_3_1.json"
AWS_ENV = "infra/aws/environments/dev"
TARGETS = ("module.lake_kms", "module.lake_bucket", "module.historical_loader")
EXPECTED_ADD = 10
NLP_IMAGE = "ta-dataflow-nlp-local:local"
SMOKE = {"root": ".local-s3/_smoke", "seed": "3199", "n": "600", "days": "60"}
OK, AVISO, FALLA = "OK", "AVISO", "FALLA"

LOT_FILES = (
    "src/historical/__init__.py", "src/historical/plan.py", "src/historical/work.py",
    "src/historical/adapters.py", "src/historical/schema.py", "src/historical/generate.py",
    "src/historical/nlp_batch.py", "src/historical/writer.py", "src/historical/verify.py",
    "src/historical/bq.py", "src/historical/s3io.py", "src/historical/doctor.py",
    "src/historical/cli.py", "src/historical/testing.py", "src/historical/requirements.txt",
    "src/historical/HISTORICAL.md", "src/historical/config/historical.yaml",
    "scripts/load_historical.py", "scripts/historical_nlp.sh", "scripts/validate_3_1.py",
    "src/ops/validate31.py", f"{AWS_ENV}/lake.tf", f"{AWS_ENV}/variables_lake.tf",
    f"{AWS_ENV}/outputs_lake.tf", "infra/aws/modules/kms_key/main.tf",
    "infra/aws/modules/s3_lake/main.tf", "infra/aws/modules/historical_loader_role/main.tf",
)
PREVIOUS_FILES = (
    "src/simulator/factory.py", "src/simulator/sim_config.py", "src/simulator/config/sim.yaml",
    "src/simulator/generator/brands.py", "src/simulator/constraints.txt",
    "src/simulator/requirements.txt", "src/common/envelope.py", "src/common/enriched_row.py",
    "src/common/parquet_rows.py", "src/common/hmac_key.py", "src/streaming/nlp.py",
    "contracts/envelope.v1.schema.json", "contracts/envelope.enriched.v1.schema.json",
    "scripts/local_nlp.sh", "infra/validate.sh",
)
CORE_IMPORTS = ("yaml", "faker", "pyarrow", "jsonschema", "referencing")
CLOUD_IMPORTS = ("google.auth", "google.cloud.bigquery", "boto3")
INSTALL_HINT = ("(desde la raíz) python -m pip install -c project/src/simulator/constraints.txt "
                "-r project/src/simulator/requirements.txt -r project/src/historical/requirements.txt")


# ======================================================================= núcleo

@dataclasses.dataclass
class Step:
    id: str
    name: str
    status: str
    detail: str
    data: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Result:
    rc: int
    out: str
    seconds: float = 0.0


Runner = Callable[..., Result]


def run_command(cmd: Sequence[str], *, cwd: Path | None = None, env: dict | None = None,
                live: bool = False, timeout: float | None = None) -> Result:
    """Corre sin entrada estándar; con ``live`` muestra la salida mientras la guarda."""
    t0 = time.monotonic()
    full_env = {**os.environ, **(env or {})}
    try:
        if not live:
            proc = subprocess.run(list(cmd), cwd=cwd, env=full_env, stdin=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                  timeout=timeout)
            return Result(proc.returncode, proc.stdout, time.monotonic() - t0)
        proc = subprocess.Popen(list(cmd), cwd=cwd, env=full_env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                bufsize=1)
        lines = []
        assert proc.stdout is not None
        for line in proc.stdout:
            print(f"    │ {line.rstrip()}", flush=True)
            lines.append(line)
        rc = proc.wait(timeout=timeout)
        return Result(rc, "".join(lines), time.monotonic() - t0)
    except FileNotFoundError as exc:
        return Result(127, f"no se encontró el comando: {exc}", time.monotonic() - t0)
    except subprocess.TimeoutExpired as exc:
        return Result(124, f"tiempo agotado ({timeout} s): {exc}", time.monotonic() - t0)


@dataclasses.dataclass
class Ctx:
    args: argparse.Namespace
    run: Runner = run_command
    which: Callable[[str], str | None] = shutil.which
    ask: Callable[[str], bool] = lambda q: input(q).strip().lower() in ("s", "si", "sí", "y", "yes")
    project: Path = PROJECT_DIR
    evidence: Path = EVIDENCE
    importer: Callable[[str], Any] = importlib.import_module
    steps: list[Step] = dataclasses.field(default_factory=list)

    def py(self, *argv: str) -> list[str]:
        return [sys.executable, *argv]

    def sh(self, cmd: Sequence[str], **kw) -> Result:
        kw.setdefault("cwd", self.project)
        return self.run(cmd, **kw)

    def add(self, step: Step) -> Step:
        self.steps.append(step)
        mark = {"OK": "✔", "AVISO": "!", "FALLA": "✘"}.get(step.status, "?")
        print(f"  {mark} {step.id:<4} {step.status:<6} {step.name}: {step.detail}", flush=True)
        return step

    def failed(self) -> bool:
        return any(s.status == FALLA for s in self.steps)


def tail(text: str, n: int = 12) -> str:
    return "\n".join(text.strip().splitlines()[-n:])


def save_evidence(ctx: Ctx, part: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if ctx.evidence.exists():
        try:
            data = json.loads(ctx.evidence.read_text(encoding="utf-8"))
        except ValueError:
            data = {}
    status = FALLA if ctx.failed() else (AVISO if any(s.status == AVISO for s in ctx.steps) else OK)
    data.setdefault("lote", "3.1")
    data.setdefault("parts", {})
    data["parts"][part] = {"status": status,
                           "at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "steps": [dataclasses.asdict(s) for s in ctx.steps], **(extra or {})}
    ctx.evidence.parent.mkdir(parents=True, exist_ok=True)
    ctx.evidence.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n",
                            encoding="utf-8")
    return data["parts"][part]


# ===================================================================== parsers

def parse_pytest(out: str) -> dict[str, int]:
    """Conteos del resumen final de pytest (``5 passed, 1 skipped in 2.0s``)."""
    counts = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
    summary = [l for l in out.strip().splitlines() if re.search(r"\b(passed|failed|error|skipped)\b", l)]
    if not summary:
        return counts
    line = summary[-1]
    for n, word in re.findall(r"(\d+) (passed|failed|skipped|errors?|deselected|xfailed|xpassed)", line):
        key = "errors" if word.startswith("error") else word
        if key in counts:
            counts[key] = int(n)
    return counts


def parse_plan(out: str) -> dict[str, int] | None:
    if re.search(r"No changes\.", out):
        return {"add": 0, "change": 0, "destroy": 0}
    m = re.search(r"Plan: (\d+) to add, (\d+) to change, (\d+) to destroy", out)
    if not m:
        return None
    return {"add": int(m[1]), "change": int(m[2]), "destroy": int(m[3])}


def parse_json_tail(out: str) -> Any:
    """Último objeto JSON de una salida (las CLIs imprimen texto y luego JSON)."""
    start = out.find("{")
    while start != -1:
        try:
            return json.loads(out[start:])
        except ValueError:
            start = out.find("{", start + 1)
    return None


def read_tfvar(path: Path, name: str) -> str | None:
    if not path.exists():
        return None
    m = re.search(rf'^\s*{name}\s*=\s*"([^"]*)"', path.read_text(encoding="utf-8"), flags=re.M)
    return m[1] if m else None


# ============================================================ CPU para el NLP

HIST_CONTAINER = "ta-hist-nlp"
STREAM_DOWN = "bash project/scripts/local_dataflow.sh down-all"


def competing_containers(ctx: Ctx) -> list[str] | None:
    """Contenedores encendidos que compiten con el NLP del histórico por la CPU, o que ocupan
    el nombre de su contenedor. El entorno local del stream (``trendanalyzer-local-*``) corre el
    mismo NLP sin parar sobre lo que emite el simulador: con él encendido la carga fue ~13 veces
    más lenta (2,6 frente a 35 filas/s, v2.28). ``None`` si Docker no responde."""
    res = ctx.sh(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"])
    if res.rc != 0:
        return None
    found = []
    for line in res.out.splitlines():
        name, _, image = line.strip().partition("\t")
        if name and (name.startswith("trendanalyzer-local-") or name == HIST_CONTAINER
                     or "ta-dataflow" in image):
            found.append(name)
    return found


def busy_detail(names: Sequence[str]) -> str:
    parts = []
    others = [n for n in names if n != HIST_CONTAINER]
    if others:
        parts.append(f"compiten por la CPU: {', '.join(others)}. El entorno local del stream corre "
                     "el mismo NLP sin parar y la carga va ~13 veces más lenta (2,6 frente a 35 "
                     f"filas/s); apáguelo con `{STREAM_DOWN}` (conserva el cursor del simulador y "
                     "`.local-lake/`); otro contenedor de NLP, con `docker stop <nombre>`")
    if HIST_CONTAINER in names:
        parts.append(f"{HIST_CONTAINER} ya está corriendo (otra carga o un doctor): espere a que "
                     "termine o `bash project/scripts/historical_nlp.sh stop`")
    return "; ".join(parts)


# ==================================================================== parte A

def part_preflight(ctx: Ctx) -> None:
    v = sys.version_info
    missing_core, missing_cloud = [], []
    for name in CORE_IMPORTS + CLOUD_IMPORTS:
        try:
            ctx.importer(name)
        except Exception:  # noqa: BLE001
            (missing_core if name in CORE_IMPORTS else missing_cloud).append(name)
    status = FALLA if missing_core else (AVISO if missing_cloud or (v.major, v.minor) != (3, 11) else OK)
    detail = f"Python {v.major}.{v.minor}.{v.micro}"
    if missing_core or missing_cloud:
        detail += f"; faltan {', '.join(missing_core + missing_cloud)} → {INSTALL_HINT}"
    ctx.add(Step("A1", "Python y dependencias", status, detail,
                 {"missing_core": missing_core, "missing_cloud": missing_cloud}))
    for sid, name, files in (("A2", "Archivos del lote", LOT_FILES),
                             ("A3", "Archivos de lotes anteriores", PREVIOUS_FILES)):
        missing = [f for f in files if not (ctx.project / f).exists()]
        ctx.add(Step(sid, name, FALLA if missing else OK,
                     f"faltan {missing}" if missing else f"{len(files)} presentes"))
    doc = ctx.sh(ctx.py("scripts/load_historical.py", "doctor", "--json-out",
                        "evidencia/historical_doctor.json"))
    lines = [l for l in doc.out.splitlines() if re.match(r"\s+D\d", l)]
    ctx.add(Step("A4", "Adaptadores contra el código real (doctor)", OK if doc.rc == 0 else FALLA,
                 "; ".join(l.strip() for l in lines if "FALLA" in l) or
                 (f"{len(lines)} pasos OK" if doc.rc == 0 else tail(doc.out, 6)),
                 {"output": tail(doc.out, 30)}))
    tools = {t: bool(ctx.which(t)) for t in ("docker", "terraform", "aws", "gcloud", "bash")}
    missing_tools = [t for t, ok in tools.items() if not ok]
    ctx.add(Step("A5", "Herramientas", AVISO if missing_tools else OK,
                 f"faltan {missing_tools} (docker: D/E; terraform y aws: C/F; gcloud: E)"
                 if missing_tools else "docker, terraform, aws, gcloud, bash", tools))
    if tools["docker"]:
        daemon = ctx.sh(["docker", "info", "--format", "{{.ServerVersion}}"])
        if daemon.rc != 0:
            ctx.add(Step("A6", "Imagen de NLP local", AVISO,
                         "Docker no responde (¿Docker Desktop abierto, con la integración de WSL?): "
                         "no se pudo comprobar la imagen; hace falta en D y E"))
        else:
            img = ctx.sh(["docker", "image", "inspect", NLP_IMAGE, "--format", "{{.Id}}"])
            ctx.add(Step("A6", "Imagen de NLP local", OK if img.rc == 0 else AVISO,
                         f"{NLP_IMAGE} presente" if img.rc == 0 else
                         f"Docker responde pero no está {NLP_IMAGE}: la parte D la construye con "
                         "--build (bash scripts/local_nlp.sh build, 10-15 min)"))
            busy = competing_containers(ctx) or []
            ctx.add(Step("A8", "CPU libre para el NLP (D y E)", AVISO if busy else OK,
                         busy_detail(busy) if busy else
                         "ningún contenedor del entorno local del stream ni de NLP encendido",
                         {"containers": busy}))
    gitignore = ctx.project.parent / ".gitignore"
    ignored = gitignore.exists() and ".local-s3" in gitignore.read_text(encoding="utf-8", errors="replace")
    ctx.add(Step("A7", ".gitignore", OK if ignored else AVISO,
                 ".local-s3/ ignorado" if ignored else
                 "agregue .local-s3/ al .gitignore de la raíz (espejo local del bucket)"))


# ==================================================================== parte B

def _pytest_step(ctx: Ctx, sid: str, name: str, paths: Sequence[str], expect_min: int = 1) -> None:
    res = ctx.sh(ctx.py("-m", "pytest", *paths, "-q", "-rs", "-p", "no:cacheprovider"), live=True)
    c = parse_pytest(res.out)
    status = OK
    if res.rc != 0 or c["failed"] or c["errors"] or c["passed"] < expect_min:
        status = FALLA
    elif c["skipped"]:
        status = AVISO
    skipped = [l.strip() for l in res.out.splitlines() if l.startswith("SKIPPED")]
    ctx.add(Step(sid, name, status,
                 f"{c['passed']} passed, {c['failed']} failed, {c['skipped']} skipped"
                 + (f" ({skipped[0][:160]})" if skipped else ""), {"counts": c, "skipped": skipped}))


def part_tests(ctx: Ctx) -> None:
    _pytest_step(ctx, "B1", "Pruebas del loader (src/historical)", ["src/historical"], 80)
    _pytest_step(ctx, "B2", "Pruebas del validador", ["src/ops/tests/test_ops_validate31.py"], 10)
    _pytest_step(ctx, "B3", "Contratos (regresión)", ["contracts/tests"], 18)
    _pytest_step(ctx, "B4", "common/ (regresión)", ["src/common"], 1)


# ==================================================================== parte C

def _tf(ctx: Ctx, *argv: str, live: bool = False) -> Result:
    return ctx.sh(["terraform", f"-chdir={AWS_ENV}", *argv], live=live)


def aws_outputs(ctx: Ctx) -> dict[str, Any]:
    res = _tf(ctx, "output", "-json")
    if res.rc != 0:
        return {}
    data = parse_json_tail(res.out) or {}
    return {k: (v.get("value") if isinstance(v, dict) else v) for k, v in data.items()}


def part_terraform(ctx: Ctx) -> None:
    tfvars = ctx.project / AWS_ENV / "terraform.tfvars"
    expected_account = os.environ.get("AWS_ACCOUNT_ID") or read_tfvar(tfvars, "aws_account_id")
    ident = ctx.sh(["aws", "sts", "get-caller-identity", "--output", "json"]
                   + (["--profile", ctx.args.profile] if ctx.args.profile else []))
    info = parse_json_tail(ident.out) or {}
    account = info.get("Account")
    if ident.rc != 0:
        ctx.add(Step("C0", "Credenciales de AWS", FALLA,
                     f"aws sts get-caller-identity falló: {tail(ident.out, 3)} (aws configure / AWS_PROFILE)"))
        return
    same_account = not expected_account or account == expected_account
    root = is_root_arn(info.get("Arn"))
    ctx.add(Step("C0", "Credenciales de AWS",
                 FALLA if not same_account else (AVISO if root else OK),
                 f"cuenta {account} ({info.get('Arn')})"
                 + ("" if same_account else f" ≠ {expected_account} de terraform.tfvars")
                 + (f"; {ROOT_NOTE}" if root and same_account else "")))
    if ctx.failed():
        return
    env_dir = ctx.project / AWS_ENV
    missing_local = [f for f in ("backend.hcl", "terraform.tfvars") if not (env_dir / f).exists()]
    ctx.add(Step("C0b", "backend.hcl y terraform.tfvars del entorno AWS",
                 FALLA if missing_local else OK,
                 (f"faltan {', '.join(missing_local)} en {AWS_ENV} (no se versionan): cópielos de "
                  "los *.example y llénelos con los valores del Lote 1.0 (§5.9 y §5.14): "
                  'backend.hcl → bucket = "ta-dev-648617469176-tfstate", dynamodb_table = '
                  '"ta-dev-tflock", region = "us-east-1"; terraform.tfvars → aws_account_id = '
                  '"648617469176" (y los demás valores del ejemplo)') if missing_local
                 else "presentes"))
    if missing_local:
        return
    val = ctx.sh(["bash", "infra/validate.sh"], live=True)
    ctx.add(Step("C1", "infra/validate.sh", OK if val.rc == 0 else FALLA,
                 "fmt y validate en verde" if val.rc == 0 else tail(val.out, 8)))
    if val.rc != 0:
        return
    backend = ctx.project / AWS_ENV / "backend.hcl"
    init_args = ["init", "-input=false", "-no-color"]
    if backend.exists():
        init_args.append("-backend-config=backend.hcl")
    init = _tf(ctx, *init_args)
    ctx.add(Step("C2a", "terraform init (módulos nuevos)", OK if init.rc == 0 else FALLA,
                 ("módulos instalados" + ("" if backend.exists() else " (sin backend.hcl)"))
                 if init.rc == 0 else tail(init.out, 8)
                 + ("" if backend.exists() else
                    f"\nfalta {AWS_ENV}/backend.hcl (cópielo de backend.hcl.example, §5.14)")))
    if init.rc != 0:
        return
    plan_file = str(ctx.project / "evidencia" / "tfplan_3_1")
    (ctx.project / "evidencia").mkdir(exist_ok=True)
    targets = [f"-target={t}" for t in TARGETS]
    plan = _tf(ctx, "plan", "-input=false", "-no-color", *targets, f"-out={plan_file}", live=True)
    counts = parse_plan(plan.out)
    if plan.rc != 0 or counts is None:
        ctx.add(Step("C2", "Plan", FALLA, tail(plan.out, 10)))
        return
    applied_before = counts == {"add": 0, "change": 0, "destroy": 0}
    good = counts == {"add": EXPECTED_ADD, "change": 0, "destroy": 0} or applied_before
    ctx.add(Step("C2", "Plan", OK if good else FALLA,
                 (f"{counts['add']} a crear, {counts['change']} a cambiar, {counts['destroy']} a destruir"
                  + (" (ya aplicado)" if applied_before else ""))
                 + ("" if good else f"; se esperaban {EXPECTED_ADD}/0/0: no aplique y revise"),
                 counts))
    if not good:
        return
    applied = applied_before
    if ctx.args.apply and not applied_before:
        if not ctx.args.yes and not ctx.ask(f"¿Aplicar el plan ({counts['add']} recursos)? [s/N] "):
            ctx.add(Step("C3", "Apply", AVISO, "no se aplicó (sin confirmación)"))
        else:
            app = _tf(ctx, "apply", "-input=false", "-no-color", plan_file, live=True)
            m = re.search(r"Apply complete! Resources: (\d+) added, (\d+) changed, (\d+) destroyed", app.out)
            applied = app.rc == 0 and bool(m)
            ctx.add(Step("C3", "Apply", OK if applied else FALLA,
                         m[0] if m else tail(app.out, 8)))
    elif not applied_before:
        ctx.add(Step("C3", "Apply", AVISO, "falta aplicar: repita con --apply"))
    if not applied:
        return
    outs = aws_outputs(ctx)
    need = ("lake_bucket", "kms_key_arn", "historical_loader_role_arn")
    missing = [k for k in need if not outs.get(k)]
    ctx.add(Step("C4", "Outputs", FALLA if missing else OK,
                 f"faltan {missing}" if missing else ", ".join(f"{k}={outs[k]}" for k in need),
                 {k: outs.get(k) for k in need + ("kms_key_alias", "lake_bucket_arn")}))
    if missing:
        return
    bucket = outs["lake_bucket"]
    prof = ["--profile", ctx.args.profile] if ctx.args.profile else []
    enc = ctx.sh(["aws", "s3api", "get-bucket-encryption", "--bucket", bucket, "--output", "json", *prof])
    pab = ctx.sh(["aws", "s3api", "get-public-access-block", "--bucket", bucket, "--output", "json", *prof])
    enc_ok = enc.rc == 0 and "aws:kms" in enc.out and outs["kms_key_arn"].split("/")[-1] in enc.out
    pab_json = parse_json_tail(pab.out) or {}
    pab_conf = pab_json.get("PublicAccessBlockConfiguration", {})
    pab_ok = pab.rc == 0 and len(pab_conf) == 4 and all(pab_conf.values())
    ctx.add(Step("C5", "Bucket cifrado y privado", OK if enc_ok and pab_ok else FALLA,
                 f"SSE-KMS con la llave del lake: {'sí' if enc_ok else 'no'}; acceso público "
                 f"bloqueado: {'sí' if pab_ok else 'no'}"))


# ==================================================================== D y E

def _hist(*argv: str) -> list[str]:
    return [sys.executable, "scripts/load_historical.py", *argv]


def _stage_args(root: str, seed: str) -> list[str]:
    return ["--root", root, "--seed", seed]


def _run_nlp(ctx: Ctx, sid: str, root: str, seed: str) -> bool:
    busy = competing_containers(ctx) or []
    if busy:
        blocking = HIST_CONTAINER in busy or not ctx.args.allow_busy
        ctx.add(Step(f"{sid}a", "CPU libre para el NLP", FALLA if blocking else AVISO,
                     busy_detail(busy) + ("; o repita con --allow-busy para correr igual"
                                          if blocking and HIST_CONTAINER not in busy else ""),
                     {"containers": busy}))
        if blocking:
            return False
    cmd = ["bash", "scripts/historical_nlp.sh", *_stage_args(root, seed)]
    if ctx.args.nlp_batch_size:
        cmd += ["--batch-size", str(ctx.args.nlp_batch_size)]
    if ctx.args.nlp_batch_seconds is not None:
        cmd += ["--batch-seconds", f"{ctx.args.nlp_batch_seconds:g}"]
    if ctx.args.nlp_workers:
        cmd += ["--workers", str(ctx.args.nlp_workers)]
    res = ctx.sh(cmd, live=True)
    status_res = ctx.sh(_hist("status", *_stage_args(root, seed)))
    st = parse_json_tail(status_res.out) or {}
    nlp = st.get("nlp") or {}
    ok = res.rc == 0 and nlp.get("pending_files") == 0 and nlp.get("nlp_missing") == 0
    ctx.add(Step(sid, "NLP en lote (imagen de NLP)", OK if ok else FALLA,
                 (f"{nlp.get('rows')} filas, {nlp.get('nlp_missing')} sin NLP, versiones "
                  f"{list((nlp.get('versions') or {}).keys())}, ~{nlp.get('rows_per_s_median')} filas/s "
                  f"en {res.seconds / 60:.1f} min") if ok else
                 (f"quedan {nlp.get('pending_files')} archivos: repita la parte (sigue donde iba)"
                  if res.rc == 1 and nlp.get("pending_files") else tail(res.out, 8)),
                 {"nlp": nlp, "minutes": round(res.seconds / 60, 1)}))
    return ok


def _verify_step(ctx: Ctx, sid: str, root: str, seed: str, extra: Sequence[str], json_out: str) -> dict | None:
    res = ctx.sh(_hist("verify", *_stage_args(root, seed), *extra, "--json-out", json_out), live=True)
    path = ctx.project / json_out
    report = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    if not report:
        ctx.add(Step(sid, "Verificación", FALLA, tail(res.out, 8)))
        return None
    bad = [f"{c['id']} {c['detail']}" for c in report["checks"] if c["status"] == FALLA]
    warn = [f"{c['id']} {c['detail']}" for c in report["checks"] if c["status"] == AVISO]
    ids = ", ".join(c["id"] for c in report["checks"])
    ctx.add(Step(sid, "Verificación", FALLA if bad else (AVISO if warn else OK),
                 "; ".join(bad or warn) or f"{ids} OK ({report['summary'].get('rows')} filas, "
                                           f"{report['summary'].get('files')} archivos)",
                 {"status": report["status"], "summary": report["summary"]}))
    return report


def part_local(ctx: Ctx) -> None:
    root, seed = SMOKE["root"], SMOKE["seed"]
    info = ctx.sh(["docker", "info", "--format", "{{.ServerVersion}}"])
    ctx.add(Step("D1", "Docker", OK if info.rc == 0 else FALLA,
                 f"Docker {info.out.strip()}" if info.rc == 0 else
                 "Docker no responde: abra Docker Desktop (integración con WSL)"))
    if info.rc != 0:
        return
    img = ctx.sh(["docker", "image", "inspect", NLP_IMAGE, "--format", "{{.Id}}"])
    if img.rc != 0 and ctx.args.build:
        build = ctx.sh(["bash", "scripts/local_nlp.sh", "build"], live=True)
        img = ctx.sh(["docker", "image", "inspect", NLP_IMAGE, "--format", "{{.Id}}"])
        if build.rc != 0:
            img = Result(1, tail(build.out, 8))
    ctx.add(Step("D2", "Imagen de NLP", OK if img.rc == 0 else FALLA,
                 f"{NLP_IMAGE} {img.out.strip()[:19]}" if img.rc == 0 else
                 f"falta {NLP_IMAGE}: repita con --build (bash scripts/local_nlp.sh build)"))
    if img.rc != 0:
        return
    doc = ctx.sh(["bash", "scripts/historical_nlp.sh", "doctor"], live=True)
    ctx.add(Step("D3", "NLP real en el contenedor (doctor)", OK if doc.rc == 0 else FALLA,
                 "inferencia de 3 filas con streaming.nlp" if doc.rc == 0 else tail(doc.out, 8)))
    if doc.rc != 0:
        return
    gen = ctx.sh(_hist("generate", *_stage_args(root, seed), "--n", SMOKE["n"], "--days", SMOKE["days"],
                       "--key-source", "local_dev", "--force",
                       "--json-out", "evidencia/historical_smoke_generate.json"), live=True)
    ctx.add(Step("D4", "generate (600 filas, llave local)", OK if gen.rc == 0 else FALLA,
                 "etapa 1 escrita en .local-s3/_smoke" if gen.rc == 0 else tail(gen.out, 8)))
    if gen.rc != 0 or not _run_nlp(ctx, "D5", root, seed):
        return
    wr = ctx.sh(_hist("write", *_stage_args(root, seed)), live=True)
    ctx.add(Step("D6", "write", OK if wr.rc == 0 else FALLA, tail(wr.out, 1)))
    if wr.rc != 0:
        return
    _verify_step(ctx, "D7", root, seed, [], "evidencia/historical_smoke_verify.json")
    again = ctx.sh(_hist("generate", *_stage_args(root, seed), "--n", SMOKE["n"], "--days",
                         SMOKE["days"], "--digest-only"))
    ctx.add(Step("D8", "Reproducible (digest)", OK if again.rc == 0 else FALLA,
                 tail(again.out, 1)))
    wr2 = ctx.sh(_hist("write", *_stage_args(root, seed)))
    m = re.search(r"\((\d+) escritos, (\d+) sin cambios", wr2.out)
    ctx.add(Step("D9", "Reescribir no cambia nada", OK if m and m[1] == "0" else FALLA,
                 tail(wr2.out, 1)))


def _gcp_project(ctx: Ctx) -> str | None:
    if os.environ.get("GCP_PROJECT"):
        return os.environ["GCP_PROJECT"]
    print("    (leyendo terraform output project_id de infra/gcp; en /mnt/c puede tardar; "
          "exporte GCP_PROJECT para saltarlo)", flush=True)
    res = ctx.sh(["terraform", "-chdir=infra/gcp/environments/dev", "output", "-raw", "project_id"])
    return res.out.strip() if res.rc == 0 and res.out.strip() else None


def part_load(ctx: Ctx) -> None:
    root, seed = ctx.args.root, str(ctx.args.seed)
    project = _gcp_project(ctx)
    adc = ctx.sh(["gcloud", "auth", "application-default", "print-access-token"])
    ok = bool(project) and adc.rc == 0
    ctx.add(Step("E1", "GCP (proyecto y ADC)", OK if ok else FALLA,
                 f"{project} · credenciales OK" if ok else
                 "falta GCP_PROJECT o ADC: gcloud auth application-default login"))
    if not ok:
        return
    env = {"GCP_PROJECT": project}
    count = ctx.sh(ctx.py("-c", (
        "import sys, json; sys.path.insert(0, 'src'); from historical import bq; "
        f"print(json.dumps(bq.run_query(bq.default_table('{project}'), [])))")), env=env)
    data = parse_json_tail(count.out) or {}
    n_rows = data.get("n_rows")
    ctx.add(Step("E2", "posts_enriched del stream", OK if (n_rows or 0) >= 100 else AVISO,
                 f"{n_rows} filas de stream en 90 días, {data.get('stream_users')} usuarios, "
                 f"versiones NLP {data.get('versions')}" if count.rc == 0 else
                 f"no se pudo consultar BigQuery: {tail(count.out, 3)}", data))
    gen_cmd = _hist("generate", *_stage_args(root, seed), "--key-source", "secret",
                    "--gcp-project", project, "--json-out", "evidencia/historical_generate.json")
    if ctx.args.n:
        gen_cmd += ["--n", str(ctx.args.n)]
    gen = ctx.sh(gen_cmd, env=env, live=True)
    gen_json = ctx.project / "evidencia" / "historical_generate.json"
    summary = json.loads(gen_json.read_text(encoding="utf-8")) if gen_json.exists() else {}
    kc = summary.get("key_check") or {}
    ctx.add(Step("E3", "generate (llave del secreto, cruce con el stream)",
                 OK if gen.rc == 0 and kc.get("status") in ("OK", None) else
                 (AVISO if gen.rc == 0 else FALLA),
                 (f"{summary.get('rows')} filas, digest {str(summary.get('digest'))[:16]}…, llave: "
                  f"{kc.get('detail', 'etapa ya generada')}") if gen.rc == 0 else tail(gen.out, 8),
                 {"digest": summary.get("digest"), "key_check": kc}))
    if gen.rc != 0:
        return
    if not _run_nlp(ctx, "E4", root, seed):
        return
    wr = ctx.sh(_hist("write", *_stage_args(root, seed), "--json-out", "evidencia/historical_write.json"),
                live=True)
    ctx.add(Step("E5", "write", OK if wr.rc == 0 else FALLA, tail(wr.out, 1)))
    if wr.rc != 0:
        return
    _verify_step(ctx, "E6", root, seed, ["--bq"], "evidencia/historical_verify.json")
    again = ctx.sh(_hist("generate", *_stage_args(root, seed), "--digest-only",
                         *(["--n", str(ctx.args.n)] if ctx.args.n else [])), env=env)
    ctx.add(Step("E7", "Reproducible (digest)", OK if again.rc == 0 else FALLA, tail(again.out, 1)))


# ==================================================================== parte F

ROOT_NOTE = ("es la cuenta raíz: AWS no le deja asumir roles, así que F sube con el perfil "
             "(--no-role) y prueba el rol con iam simulate-principal-policy. Recomendado: un "
             "usuario IAM o SSO para el día a día")


def is_root_arn(arn: str | None) -> bool:
    return bool(arn) and str(arn).endswith(":root")


def _caller(ctx: Ctx) -> dict[str, Any]:
    prof = ["--profile", ctx.args.profile] if ctx.args.profile else []
    res = ctx.sh(["aws", "sts", "get-caller-identity", "--output", "json", *prof])
    return (parse_json_tail(res.out) or {}) if res.rc == 0 else {}


def simulate_role(ctx: Ctx, role_arn: str, bucket: str) -> dict[str, dict[str, str]]:
    """Evalúa la política del rol sin asumirlo (``iam simulate-principal-policy``)."""
    obj = f"arn:aws:s3:::{bucket}"
    cases = [
        ("put raw/origin=historical/", "s3:PutObject", f"{obj}/raw/origin=historical/_ta_probe", None, "PERMITIDO"),
        ("put raw/origin=stream/", "s3:PutObject", f"{obj}/raw/origin=stream/_ta_probe", None, "DENEGADO"),
        ("get raw/origin=historical/", "s3:GetObject", f"{obj}/raw/origin=historical/_ta_probe", None, "DENEGADO"),
        ("delete raw/origin=historical/", "s3:DeleteObject", f"{obj}/raw/origin=historical/_ta_probe", None, "DENEGADO"),
        ("list raw/origin=historical/", "s3:ListBucket", obj, "raw/origin=historical/", "PERMITIDO"),
        ("list trusted/", "s3:ListBucket", obj, "trusted/", "DENEGADO"),
    ]
    prof = ["--profile", ctx.args.profile] if ctx.args.profile else []
    out: dict[str, dict[str, str]] = {}
    for name, action, resource, prefix, expected in cases:
        cmd = ["aws", "iam", "simulate-principal-policy", "--policy-source-arn", role_arn,
               "--action-names", action, "--resource-arns", resource, "--output", "json", *prof]
        if prefix:
            cmd += ["--context-entries",
                    f"ContextKeyName=s3:prefix,ContextKeyValues={prefix},ContextKeyType=string"]
        res = ctx.sh(cmd)
        data = parse_json_tail(res.out) or {}
        results = data.get("EvaluationResults") or []
        decision = results[0].get("EvalDecision") if results else None
        got = ("PERMITIDO" if decision == "allowed" else
               "DENEGADO" if decision in ("implicitDeny", "explicitDeny") else
               f"ERROR {tail(res.out, 1)[:80]}")
        out[name] = {"got": got, "expected": expected}
    return out


def part_upload(ctx: Ctx) -> None:
    root, seed = ctx.args.root, str(ctx.args.seed)
    outs = aws_outputs(ctx)
    need = ("lake_bucket", "kms_key_arn", "historical_loader_role_arn")
    missing = [k for k in need if not outs.get(k)]
    ctx.add(Step("F1", "Outputs de AWS", FALLA if missing else OK,
                 f"faltan {missing}: aplique la parte C" if missing else
                 f"s3://{outs['lake_bucket']} · {outs['historical_loader_role_arn']}"))
    if missing:
        return
    role = outs["historical_loader_role_arn"]
    caller = _caller(ctx)
    as_root = is_root_arn(caller.get("Arn"))
    aws = ["--bucket", outs["lake_bucket"], "--kms-key-arn", outs["kms_key_arn"],
           "--region", ctx.args.region] + (["--profile", ctx.args.profile] if ctx.args.profile else [])
    how = ["--no-role"] if as_root else ["--role-arn", role, "--probe"]
    up = ctx.sh(_hist("upload", *_stage_args(root, seed), *aws, *how, "--json-out",
                      "evidencia/historical_upload.json"), live=True)
    path = ctx.project / "evidencia" / "historical_upload.json"
    res = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    ok = up.rc == 0 and res.get("n_errors") == 0
    detail = (f"{res.get('uploaded')} subidos, {res.get('skipped_same_size')} ya estaban, "
              f"{res.get('n_stale', 0)} sobrantes de la semilla, {res.get('seconds')} s")
    if ok and as_root:
        detail += f" · con la cuenta raíz ({ROOT_NOTE})"
    ctx.add(Step("F2", "Subida a S3" + ("" if as_root else " con ta-historical-loader"),
                 (AVISO if as_root else OK) if ok else FALLA,
                 detail if ok else tail(up.out, 8), res))
    sim = simulate_role(ctx, role, outs["lake_bucket"])
    live = res.get("permission_probe") or {}
    bad = [f"{k}: {v['got']} (esperado {v['expected']})" for k, v in sim.items()
           if v["got"] != v["expected"]]
    bad += [f"sonda real {k}: {v}" for k, v in live.items() if v != "DENEGADO"]
    ctx.add(Step("F3", "Mínimo privilegio del rol", FALLA if bad else OK,
                 "; ".join(bad) or (f"política del rol: {len(sim)} casos como se esperaba"
                                    + (f"; sonda real: {', '.join(f'{k} {v}' for k, v in live.items())}"
                                       if live else " (simulada; la cuenta raíz no asume roles)")),
                 {"simulation": sim, "live_probe": live}))
    if not ok:
        return
    _verify_step(ctx, "F4", root, seed, ["--s3", *aws], "evidencia/historical_s3_check.json")
    how2 = ["--no-role"] if as_root else ["--role-arn", role]
    up2 = ctx.sh(_hist("upload", *_stage_args(root, seed), *aws, *how2))
    data = parse_json_tail(up2.out) or {}
    idem = up2.rc == 0 and data.get("uploaded") == 0 and data.get("skipped_same_size") == data.get("files")
    ctx.add(Step("F5", "Segunda subida idempotente", OK if idem else FALLA,
                 f"{data.get('uploaded')} subidos, {data.get('skipped_same_size')} omitidos de "
                 f"{data.get('files')}" if data else tail(up2.out, 6), data))


# ======================================================================= main

PARTS = {"preflight": ("A", part_preflight), "tests": ("B", part_tests),
         "terraform": ("C", part_terraform), "local": ("D", part_local),
         "load": ("E", part_load), "upload": ("F", part_upload)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="validate_3_1.py", description="Validación del Lote 3.1")
    p.add_argument("part", choices=list(PARTS))
    p.add_argument("--apply", action="store_true", help="C: aplica el plan (pide confirmación)")
    p.add_argument("--yes", action="store_true", help="no pide confirmación")
    p.add_argument("--build", action="store_true", help="D: construye la imagen de NLP si falta")
    p.add_argument("--root", default=".local-s3", help="E/F: espejo local del bucket")
    p.add_argument("--seed", type=int, default=int(os.environ.get("HIST_SEED", "3101")))
    p.add_argument("--n", type=int, default=None, help="E: filas (por defecto 50.000)")
    p.add_argument("--nlp-batch-size", type=int, default=None)
    p.add_argument("--nlp-runner", default=None, help=argparse.SUPPRESS)
    p.add_argument("--nlp-batch-seconds", type=float, default=None)
    p.add_argument("--nlp-workers", type=int, default=None,
                   help="D/E: procesos de NLP en paralelo (lo recomienda --nlp-bench)")
    p.add_argument("--allow-busy", action="store_true",
                   help="D/E: corre el NLP aunque el entorno local del stream esté encendido")
    p.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    return p


def main(argv: Sequence[str] | None = None, ctx: Ctx | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.nlp_runner not in (None, "bundle"):
        print(f"--nlp-runner {args.nlp_runner}: se quitó; el NLP en lote usa siempre el "
              "BundleBasedDirectRunner (el FnApiRunner pierde o atasca el último lote parcial). "
              "Para ir más rápido use --nlp-workers N (lo recomienda "
              "`bash project/scripts/historical_nlp.sh doctor --nlp-bench 160`).", file=sys.stderr)
        return 2
    ctx = ctx or Ctx(args=args)
    ctx.args = args
    letter, fn = PARTS[args.part]
    print(f"Lote 3.1 · parte {letter} ({args.part}) · {ctx.project}", flush=True)
    t0 = time.monotonic()
    fn(ctx)
    part = save_evidence(ctx, letter, {"minutes": round((time.monotonic() - t0) / 60, 1)})
    print(f"resultado parte {letter}: {part['status']} · evidencia en {ctx.evidence}")
    return 1 if part["status"] == FALLA else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
