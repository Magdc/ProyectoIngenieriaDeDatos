"""Plantilla Flex del pipeline de streaming (Lote 2.4, HU-005).

La plantilla se parametriza con los outputs de Terraform: nada de proyecto, buckets,
tablas o cuentas escritos a mano. Comandos (``scripts/dataflow_template.py``):

``params``  muestra lo que se tomaría de ``terraform output -json``.
``build``   construye la imagen ``dataflow-launcher`` en Cloud Build y publica la
            especificación en ``gs://{lake}/_dataflow/templates/ta-stream-{etiqueta}.json``
            y ``ta-stream.json`` (la última).
``run``     lanza el job ``ta-stream-{env}`` desde la plantilla, con las mismas
            características que ``dataflow_job.sh start`` (1 × e2-standard-2, Streaming
            Engine, ``sa-dataflow``). Se niega si ya hay un job activo con ese nombre.

``--dry-run`` imprime los comandos sin ejecutarlos.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_TF_DIR = PROJECT_DIR / "infra" / "gcp" / "environments" / "dev"
FLEX_DIR = Path(__file__).resolve().parent / "flex_template"
TEMPLATE_PREFIX = "_dataflow/templates"

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess]


class TemplateError(RuntimeError):
    """Error de configuración o de un comando externo."""


def default_runner(cmd: Sequence[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(list(cmd), cwd=cwd, capture_output=True, text=True, check=False)


@dataclass(frozen=True)
class TemplateEnv:
    project: str
    region: str
    env: str
    lake_bucket: str
    service_account: str
    artifact_registry_url: str
    template_spec: str
    launcher_image: str
    labels: Mapping[str, str] = field(default_factory=dict)
    posts_enriched_table: str | None = None
    trend_windows_table: str | None = None
    dlq_events_table: str | None = None
    dlq_subscription: str | None = None

    @property
    def job_name(self) -> str:
        return f"ta-stream-{self.env}"

    @property
    def lake_root(self) -> str:
        return f"gs://{self.lake_bucket}"


def _value(outputs: Mapping[str, Any], name: str, required: bool = True):
    entry = outputs.get(name)
    value = entry.get("value") if isinstance(entry, Mapping) and "value" in entry else entry
    if value in (None, "") and required:
        raise TemplateError(f"falta el output de Terraform {name!r} (¿se aplicaron los lotes 1.3, 2.2 y 2.4?)")
    return value


def _dataflow_account(value: Any) -> str:
    candidates: list[str] = []
    if isinstance(value, Mapping):
        candidates = [str(v) for v in value.values()]
    elif isinstance(value, (list, tuple)):
        candidates = [str(v) for v in value]
    elif isinstance(value, str):
        candidates = [value]
    for item in candidates:
        email = item.split(":", 1)[-1]
        if email.startswith("sa-dataflow@"):
            return email
    raise TemplateError("el output service_accounts no tiene sa-dataflow@…")


def env_from_outputs(outputs: Mapping[str, Any], environ: Mapping[str, str] | None = None) -> TemplateEnv:
    """Construye la configuración desde ``terraform output -json``."""
    environ = os.environ if environ is None else environ
    labels = _value(outputs, "labels", required=False) or {}
    lake = str(_value(outputs, "lake_bucket")).removeprefix("gs://").rstrip("/")
    registry = str(_value(outputs, "artifact_registry_url")).rstrip("/")
    spec = _value(outputs, "dataflow_template_spec", required=False) or f"gs://{lake}/{TEMPLATE_PREFIX}/ta-stream.json"
    launcher = _value(outputs, "dataflow_launcher_image", required=False) or f"{registry}/dataflow-launcher"
    return TemplateEnv(
        project=str(_value(outputs, "project_id")),
        region=str(_value(outputs, "region")),
        env=str(labels.get("env") or environ.get("ENV") or "dev"),
        lake_bucket=lake,
        service_account=_dataflow_account(_value(outputs, "service_accounts")),
        artifact_registry_url=registry,
        template_spec=str(spec),
        launcher_image=str(launcher),
        labels=dict(labels),
        posts_enriched_table=_value(outputs, "posts_enriched_table", required=False),
        trend_windows_table=_value(outputs, "trend_windows_table", required=False),
        dlq_events_table=_value(outputs, "dlq_events_table", required=False),
        dlq_subscription=_value(outputs, "dlq_df_subscription", required=False),
    )


def read_outputs(tf_dir: Path, runner: Runner | None = None) -> dict:
    runner = runner or default_runner
    proc = runner(["terraform", f"-chdir={tf_dir}", "output", "-json"])
    if proc.returncode != 0:
        raise TemplateError(f"terraform output falló: {proc.stderr.strip()[:500]}")
    try:
        return json.loads(proc.stdout or "{}")
    except ValueError as exc:
        raise TemplateError(f"terraform output no devolvió JSON: {exc}") from exc


def resolve_tag(explicit: str | None, runner: Runner | None = None,
                now: datetime | None = None) -> str:
    """Etiqueta de la imagen: la pedida, el commit corto o la fecha UTC (como el Lote 2.3)."""
    if explicit:
        return explicit
    runner = runner or default_runner
    try:
        proc = runner(["git", "-C", str(PROJECT_DIR), "rev-parse", "--short", "HEAD"])
        tag = (proc.stdout or "").strip()
        if proc.returncode == 0 and tag:
            return tag
    except OSError:
        pass
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%d-%H%M")


def versioned_spec(spec: str, tag: str) -> str:
    base = spec[:-5] if spec.endswith(".json") else spec
    return f"{base}-{tag}.json"


def template_parameters(env: TemplateEnv, nlp_tag: str = "latest",
                        extra: Mapping[str, str] | None = None) -> dict[str, str]:
    params = {
        "output_root": env.lake_root,
        "nlp_container_image": f"{env.artifact_registry_url}/dataflow-nlp:{nlp_tag}",
    }
    for name, value in (("posts_enriched_table", env.posts_enriched_table),
                        ("trend_windows_table", env.trend_windows_table),
                        ("dlq_events_table", env.dlq_events_table),
                        ("dlq_df_subscription", env.dlq_subscription)):
        if value:
            params[name] = str(value)
    for name, value in (extra or {}).items():
        params[name] = str(value)
    bad = [k for k, v in params.items() if "," in v]
    if bad:
        raise TemplateError(f"parámetros con comas no admitidos: {bad}")
    return params


def job_labels(env: TemplateEnv) -> dict[str, str]:
    labels = {k: str(v) for k, v in env.labels.items()}
    labels.setdefault("project", "trend-analyzer")
    labels.setdefault("env", env.env)
    labels["component"] = "streaming"
    return labels


def build_commands(env: TemplateEnv, tag: str, context_dir: Path, metadata_file: Path) -> list[list[str]]:
    image = f"{env.launcher_image}:{tag}"
    spec_tag = versioned_spec(env.template_spec, tag)
    return [
        ["gcloud", "builds", "submit", str(context_dir), f"--project={env.project}",
         f"--tag={image}", "--timeout=1800s"],
        ["gcloud", "artifacts", "docker", "tags", "add", image, f"{env.launcher_image}:latest",
         f"--project={env.project}"],
        ["gcloud", "dataflow", "flex-template", "build", spec_tag, f"--image={image}",
         "--sdk-language=PYTHON", f"--metadata-file={metadata_file}", f"--project={env.project}"],
        ["gcloud", "storage", "cp", spec_tag, env.template_spec, f"--project={env.project}"],
    ]


def run_command(env: TemplateEnv, params: Mapping[str, str], *, template_spec: str | None = None,
                job_name: str | None = None, machine_type: str = "e2-standard-2",
                workers: int = 1) -> list[str]:
    labels = ",".join(f"{k}={v}" for k, v in job_labels(env).items())
    parameters = ",".join(f"{k}={v}" for k, v in params.items())
    return [
        "gcloud", "dataflow", "flex-template", "run", job_name or env.job_name,
        f"--template-file-gcs-location={template_spec or env.template_spec}",
        f"--project={env.project}", f"--region={env.region}",
        f"--service-account-email={env.service_account}",
        f"--temp-location={env.lake_root}/_dataflow/temp",
        f"--staging-location={env.lake_root}/_dataflow/staging",
        f"--worker-machine-type={machine_type}", f"--num-workers={workers}", f"--max-workers={workers}",
        "--enable-streaming-engine",
        f"--additional-user-labels={labels}",
        f"--parameters={parameters}",
    ]


def active_jobs(env: TemplateEnv, name: str, runner: Runner | None = None) -> list[dict]:
    runner = runner or default_runner
    proc = runner(["gcloud", "dataflow", "jobs", "list", f"--project={env.project}", f"--region={env.region}",
                   "--status=active", f"--filter=name={name}", "--format=json"])
    if proc.returncode != 0:
        raise TemplateError(f"gcloud dataflow jobs list falló: {proc.stderr.strip()[:500]}")
    try:
        jobs = json.loads(proc.stdout or "[]")
    except ValueError:
        jobs = []
    return [j for j in jobs if j.get("name") == name]


_SKIP_DIRS = {"tests", "__pycache__", ".pytest_cache"}


def stage_context(project_dir: Path, dest: Path) -> Path:
    """Contexto de Cloud Build: Dockerfile, requirements.txt, project/src (sin pruebas) y project/contracts."""
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(FLEX_DIR / "Dockerfile", dest / "Dockerfile")
    requirements = project_dir / "src" / "streaming" / "requirements.txt"
    if not requirements.exists():
        raise TemplateError(f"no existe {requirements} (Lote 2.1)")
    shutil.copy2(requirements, dest / "requirements.txt")

    def ignore(directory, names):
        return [n for n in names if n in _SKIP_DIRS or n.endswith(".egg-info") or n.endswith(".pyc")]

    shutil.copytree(project_dir / "src", dest / "project" / "src", ignore=ignore)
    contracts = project_dir / "contracts"
    if contracts.exists():
        shutil.copytree(contracts, dest / "project" / "contracts", ignore=ignore)
    (dest / ".gcloudignore").write_text("# contexto preparado por template_tools.stage_context\n", encoding="utf-8")
    return dest


def _run_checked(cmd: Sequence[str], runner: Runner, dry_run: bool) -> None:
    print("$ " + " ".join(cmd), flush=True)
    if dry_run:
        return
    proc = runner(cmd)
    if proc.stdout:
        print(proc.stdout.rstrip(), flush=True)
    if proc.returncode != 0:
        raise TemplateError(f"falló ({proc.returncode}): {' '.join(cmd[:4])}…\n{(proc.stderr or '').strip()[-2000:]}")


def cmd_params(env: TemplateEnv, args) -> dict:
    params = template_parameters(env, args.nlp_tag)
    report = {"env": asdict(env), "job_name": args.job_name or env.job_name, "parameters": params,
              "run_command": run_command(env, params, job_name=args.job_name)}
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def cmd_build(env: TemplateEnv, args, runner: Runner) -> dict:
    tag = resolve_tag(args.tag, runner)
    with tempfile.TemporaryDirectory(prefix="ta-flex-") as tmp:
        context = stage_context(PROJECT_DIR, Path(tmp) / "context")
        commands = build_commands(env, tag, context, FLEX_DIR / "metadata.json")
        for cmd in commands:
            _run_checked(cmd, runner, args.dry_run)
    report = {"tag": tag, "image": f"{env.launcher_image}:{tag}",
              "template_spec": env.template_spec, "template_spec_tag": versioned_spec(env.template_spec, tag),
              "dry_run": args.dry_run}
    print(json.dumps({"event": "flex_template_built", **report}, ensure_ascii=False))
    return report


def cmd_run(env: TemplateEnv, args, runner: Runner) -> dict:
    name = args.job_name or env.job_name
    if not args.dry_run:
        active = active_jobs(env, name, runner)
        if active:
            raise TemplateError(
                f"ya hay un job activo llamado {name} ({active[0].get('id')}, {active[0].get('state')}): "
                "deténgalo con 'bash scripts/dataflow_job.sh drain' y espere a que termine.")
    spec = versioned_spec(env.template_spec, args.template_tag) if args.template_tag != "latest" else env.template_spec
    params = template_parameters(env, args.nlp_tag)
    cmd = run_command(env, params, template_spec=spec, job_name=name)
    _run_checked(cmd, runner, args.dry_run)
    report = {"job_name": name, "template_spec": spec, "parameters": params, "dry_run": args.dry_run}
    print(json.dumps({"event": "flex_template_run", **report}, ensure_ascii=False))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dataflow_template.py", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tf-dir", type=Path, default=DEFAULT_TF_DIR, help="Entorno de Terraform (outputs).")
    parser.add_argument("--json-out", type=Path, help="Guarda el resultado en JSON (evidencia).")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("params", help="Muestra los parámetros tomados de Terraform.")
    p.add_argument("--nlp-tag", default="latest")
    p.add_argument("--job-name")
    b = sub.add_parser("build", help="Construye la imagen del lanzador y publica la plantilla.")
    b.add_argument("--tag", help="Etiqueta (por defecto, el commit corto o la fecha UTC).")
    b.add_argument("--dry-run", action="store_true")
    r = sub.add_parser("run", help="Lanza el job desde la plantilla.")
    r.add_argument("--template-tag", default="latest", help="latest o la etiqueta de un build.")
    r.add_argument("--nlp-tag", default="latest", help="Etiqueta de dataflow-nlp para los workers.")
    r.add_argument("--job-name")
    r.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None, runner: Runner | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = runner or default_runner
    try:
        env = env_from_outputs(read_outputs(args.tf_dir, runner))
        if args.command == "params":
            report = cmd_params(env, args)
        elif args.command == "build":
            report = cmd_build(env, args, runner)
        else:
            report = cmd_run(env, args, runner)
    except TemplateError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return 0
