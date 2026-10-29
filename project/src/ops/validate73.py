"""Validación automática del Lote 7.3 (HU-043, control de costos): ``docs/validation/validation-7-3.md``.

    python project/scripts/validate_7_3.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes, en el orden de uso: ``preflight`` (A), ``tests`` (B), ``local`` (L: Dataflow con 1 worker, scripts de
encendido y apagado, etiquetas por componente y ciclos de vida en el código, sin nube), ``terraform`` (C: plan; con
``--apply`` crea las dos alertas de gasto y pone las etiquetas; después, plan limpio), ``cloud`` (K: lo que hay en la
nube: presupuestos de GCP y AWS, ciclos de vida de GCS, BigQuery y S3, etiquetas `component` y las alertas de gasto),
``fire`` (F: con Dataflow y el simulador encendidos, baja los umbrales a minutos, espera la notificación de cada alerta
en ``ta-alerts-incidents`` y los devuelve a 4 h y 8 h) y ``status``. Cada parte imprime un paso por línea y guarda su
resumen en ``project/evidencia/validacion_7_3.json``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from alerts import check
from ops import services as sv
from ops import validate62 as v62
from ops.validate32 import AVISO, FALLA, OK, OMITIDO, WSL_HINT, Part, Runner, on_windows, parse_json, pytest_counts, summarize_plan

REPO = v62.REPO
PROJECT = v62.PROJECT
EVIDENCE = PROJECT / "evidencia" / "validacion_7_3.json"
GCP_ENV = v62.GCP_ENV
AWS_ENV = PROJECT / "infra" / "aws" / "environments" / "dev"
AWS_ACCOUNT = "648617469176"
LAKE_BUCKET = f"ta-dev-{AWS_ACCOUNT}-lake"
POLICIES = ("ta-gasto-dataflow-encendido", "ta-gasto-simulador-encendido")
#: §5.12 «Retención» y HU-043: prefijo o tabla → días.
RETENTION = {"gcs raw/": 30, "gcs dlq/": 30, "bigquery posts_enriched": 90, "s3 athena-results/": 7}
COMPONENTS_AWS = {"lake", "historical", "exporter", "lakehouse", "serving", "observability"}

LOT_FILES = [
    "project/infra/gcp/modules/cost_guard/main.tf",
    "project/infra/gcp/environments/dev/costs.tf",
    "project/infra/aws/environments/dev/components.tf",
    "project/src/ops/validate73.py",
    "project/src/ops/tests/test_ops_validate73.py",
    "project/scripts/validate_7_3.py",
]
B1_EXPECTED = 7
B2_EXPECTED = 35


@dataclass
class Ctx(v62.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "7.3", "hu": ["HU-043"], "parts": {}}

    def awsj(self, *args: str, timeout: float = 180) -> Any:
        res = self.aws(*args, timeout=timeout)
        return parse_json(res.out) if res.ok else None

    def bq_show(self, table: str) -> Any:
        res = self.run(["bq", f"--project_id={self.project}", "show", "--format=json", table], timeout=180)
        return parse_json(res.out) if res.ok else None


# --- utilidades puras ----------------------------------------------------------------------------


def dataflow_workers(script: str) -> dict[str, str]:
    """Las opciones de workers de ``dataflow_job.sh`` (HU-043: maxNumWorkers de 1 a 2)."""
    out = {}
    for flag in ("num_workers", "max_num_workers", "autoscaling_algorithm"):
        m = re.search(rf"--{flag}[ =]+(\S+)", script)
        if m:
            out[flag] = m.group(1)
    return out


def module_providers(tf: str) -> dict[str, str | None]:
    """``module "x" { … providers = { aws = aws.y } … }`` → {x: y o None}."""
    out = {}
    for m in re.finditer(r'module "([^"]+)" \{(.*?)\n\}', tf, flags=re.S):
        p = re.search(r"providers\s*=\s*\{\s*aws\s*=\s*aws\.(\w+)\s*\}", m.group(2))
        out[m.group(1)] = p.group(1) if p else None
    return out


def gcp_label_blocks(tf: str) -> list[str]:
    """Líneas ``labels = …`` de una raíz de GCP que no ponen ``component`` (ni usan un local que lo ponga)."""
    out = []
    for line in tf.splitlines():
        m = re.match(r"\s*labels\s*=\s*(.+)$", line)
        if not m:
            continue
        value = m.group(1)
        if "component" in value or re.search(r"local\.(ta_stream|trends)_labels", value):
            continue
        out.append(line.strip())
    return out


def gcs_rules(lifecycle: Any) -> dict[str, int]:
    """Reglas Delete de un bucket de GCS → {prefijo: días}."""
    out = {}
    for rule in (lifecycle or {}).get("rule") or []:
        if (rule.get("action") or {}).get("type") != "Delete":
            continue
        cond = rule.get("condition") or {}
        for prefix in cond.get("matchesPrefix") or []:
            if cond.get("age") is not None:
                out[prefix] = int(cond["age"])
    return out


def s3_rules(cfg: Any) -> dict[str, int]:
    out = {}
    for rule in (cfg or {}).get("Rules") or []:
        if rule.get("Status") != "Enabled" or "Expiration" not in rule:
            continue
        flt = rule.get("Filter") or {}
        prefix = flt.get("Prefix") or (flt.get("And") or {}).get("Prefix") or rule.get("Prefix") or ""
        out[prefix] = int(rule["Expiration"].get("Days") or 0)
    return out


def tag_components(resources: list[dict[str, Any]]) -> Counter:
    c: Counter = Counter()
    for r in resources:
        tags = {t.get("Key"): t.get("Value") for t in r.get("Tags") or []}
        c[tags.get("component") or "(sin component)"] += 1
    return c


def policy_problems(policy: dict[str, Any], channels: int) -> list[str]:
    out = []
    if not policy.get("enabled", True):
        out.append("deshabilitada")
    if len(policy.get("notificationChannels") or []) < channels:
        out.append(f"{len(policy.get('notificationChannels') or [])} canales (esperado ≥ {channels})")
    return out


# --- Parte A · preflight -----------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    absent = [f for f in LOT_FILES if not (REPO / f).exists()]
    part.add("A1", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK, ", ".join(absent) or "presentes")
    tools = {t: ctx.runner.which(t) for t in ("gcloud", "aws", "terraform", "bq")}
    need = [t for t in tools if not tools[t]]
    part.add("A2", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {need}" if need else ""))
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs, sim = sv.active_jobs(run, s), sv.simulator_service(run, s)
    part.add("A3", "Dataflow y simulador", OK, f"Dataflow {'ENCENDIDO' if jobs else 'apagado'} · simulador {'ENCENDIDO' if sim else 'apagado'}",
             dataflow=bool(jobs), simulator=bool(sim))
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    for sid, title, path, expected in (("B1", "test_ops_validate73.py", "src/ops/tests/test_ops_validate73.py", B1_EXPECTED),
                                        ("B2", "test_ops_services.py (on.py, off.py, status.py)", "src/ops/tests/test_ops_services.py", B2_EXPECTED)):
        res = ctx.pytest(path, "-q", "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and not c["failed"]
        part.add(sid, title, OK if good else FALLA, f"{c['passed']} passed, {c['failed']} failed (esperado {expected})"
                 + ("" if good else "\n" + res.tail(8)), **c)
    return part.finish()


# --- Parte L · local ----------------------------------------------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Workers, encendido y apagado, etiquetas y ciclos de vida en el código")
    workers = dataflow_workers((PROJECT / "scripts" / "dataflow_job.sh").read_text(encoding="utf-8"))
    good = workers.get("max_num_workers") in ("1", "2") and workers.get("num_workers") == "1"
    part.add("L1", "Dataflow: 1 worker, maxNumWorkers entre 1 y 2 (dataflow_job.sh)", OK if good else FALLA, json.dumps(workers), **workers)
    scripts = [f for f in ("on.py", "off.py", "status.py") if (PROJECT / "scripts" / f).exists()]
    part.add("L2", "scripts de encendido y apagado (Dataflow, simulador, exportador, lakehouse)", OK if len(scripts) == 3 else FALLA,
             ", ".join(scripts) + " (lógica en src/ops/services.py; B2 los prueba)")
    aws_tf = "\n".join(p.read_text(encoding="utf-8") for p in sorted(AWS_ENV.glob("*.tf")))
    providers = module_providers(aws_tf)
    missing = [m for m, p in providers.items() if p is None]
    part.add("L3", "AWS: cada módulo con el proveedor de su componente (default_tags con `component`)", FALLA if missing else OK,
             f"sin proveedor de componente: {missing}" if missing else
             ", ".join(f"{m}→{p}" for m, p in sorted(providers.items())), providers=providers)
    gcp_tf = "\n".join(p.read_text(encoding="utf-8") for p in sorted(GCP_ENV.glob("*.tf")))
    loose = [ln for ln in gcp_label_blocks(gcp_tf) if "local.platform_labels" in ln and "pubsub" not in ln]
    part.add("L4", "GCP: etiquetas con `component` en la raíz", OK if len(loose) <= 1 else AVISO,
             "todas con component (Pub/Sub lo pone su módulo: ingest)" if len(loose) <= 1 else "\n".join(loose))
    platform = (GCP_ENV / "platform.tf").read_text(encoding="utf-8")
    code = {"gcs raw/": int(re.search(r'"raw/"\s*=\s*(\d+)', platform).group(1)),
            "gcs dlq/": int(re.search(r'"dlq/"\s*=\s*(\d+)', platform).group(1))}
    bq = (PROJECT / "infra/gcp/modules/bigquery_stream/main.tf").read_text(encoding="utf-8")
    m = re.search(r'variable "posts_enriched_partition_expiration_days".*?default\s*=\s*(\d+)', bq, flags=re.S)
    code["bigquery posts_enriched"] = int(m.group(1)) if m else -1
    lake = (PROJECT / "infra/aws/modules/s3_lake/main.tf").read_text(encoding="utf-8")
    m = re.search(r'variable "athena_results_expiration_days".*?default\s*=\s*(\d+)', lake, flags=re.S)
    code["s3 athena-results/"] = int(m.group(1)) if m else -1
    part.add("L5", "ciclos de vida en el código = §5.12 (30, 30, 90 y 7 días)", OK if code == RETENTION else FALLA,
             json.dumps(code, ensure_ascii=False), **{k.replace(" ", "_"): v for k, v in code.items()})
    res = ctx.run(["bash", "project/infra/validate.sh"], cwd=REPO, timeout=900)
    part.add("L6", "terraform fmt y validate de las 4 raíces", OK if res.ok and "Warning" not in res.out else FALLA,
             "OK, sin avisos" if res.ok and "Warning" not in res.out else res.tail(10))
    return part.finish()


# --- Parte C · Terraform --------------------------------------------------------------------------------------


def plan(ctx: Ctx, root: Path, name: str, extra: tuple[str, ...] = ()) -> tuple[Any, dict[str, list[str]]]:
    plan_path = Path(f"/tmp/validate73/tfplan_{name}")
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    res = ctx.tf(root, "plan", "-input=false", "-lock-timeout=60s", f"-out={plan_path}", *extra, timeout=1500)
    if not res.ok:
        return res, {}
    show = ctx.tf(root, "show", "-json", str(plan_path), timeout=600)
    return res, summarize_plan(parse_json(show.out) or {})


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform: alertas de gasto y etiquetas (sin borrar nada)")
    apply = bool(getattr(ctx.options, "apply", False))
    for sid, root, name in (("C1", GCP_ENV, "gcp"), ("C2", AWS_ENV, "aws")):
        res, summary = plan(ctx, root, name)
        if not res.ok:
            part.add(sid, f"plan de infra/{name}", FALLA, res.tail(10))
            continue
        bad = summary.get("delete", []) + summary.get("replace", [])
        n = {k: len(v) for k, v in summary.items()}
        part.add(sid, f"plan de infra/{name}: {n['create']} a crear, {n['update']} a cambiar, 0 a borrar", FALLA if bad else OK,
                 f"borra o reemplaza: {bad}" if bad else f"crea {summary['create']} · cambia {n['update']} (etiquetas)", **n)
        if bad or not apply or not sum(n.values()):
            continue
        if not ctx.ask(f"¿Aplicar el plan de infra/{name} ({n['create']} a crear, {n['update']} a cambiar)?"):
            part.add(f"{sid}a", f"apply de infra/{name}", AVISO, "no confirmado")
            continue
        res = ctx.tf(root, "apply", "-input=false", "-lock-timeout=60s", f"/tmp/validate73/tfplan_{name}", timeout=1800)
        part.add(f"{sid}a", f"apply de infra/{name}", OK if res.ok else FALLA, (re.findall(r"Apply complete!.*", res.out) or [res.tail(6)])[-1])
        res, summary = plan(ctx, root, name)
        left = sum(len(v) for v in summary.values())
        part.add(f"{sid}b", f"plan de infra/{name} después del apply", OK if res.ok and not left else FALLA,
                 "sin cambios" if res.ok and not left else json.dumps(summary))
    return part.finish()


# --- Parte K · la nube ------------------------------------------------------------------------------------


def billing_account(ctx: Ctx) -> str | None:
    res = ctx.gcloud("billing", "projects", "describe", ctx.project, "--format=value(billingAccountName)", timeout=120)
    name = res.out.strip() if res.ok else ""
    return name.split("/")[-1] or None


def cost_policies(ctx: Ctx) -> dict[str, dict[str, Any]]:
    data = ctx.gjson("monitoring", "policies", "list", "--filter", 'displayName:"ta-gasto-"') or []
    return {p.get("displayName"): p for p in data}


def part_cloud(ctx: Ctx) -> int:
    part = Part(ctx, "K", "Lo que hay en la nube: presupuestos, ciclos de vida, etiquetas y alertas de gasto")
    account = billing_account(ctx)
    budgets = ctx.gjson("billing", "budgets", "list", f"--billing-account={account}") if account else None
    gcp_b = [b for b in (budgets or []) if "ta" in str(b.get("displayName", "")).lower() or "trend" in str(b.get("displayName", "")).lower()]
    rules = [round(float(t.get("thresholdPercent", 0)), 2) for b in gcp_b for t in b.get("thresholdRules") or []]
    part.add("K1", "presupuesto de GCP con avisos", OK if gcp_b and rules else FALLA,
             f"{', '.join(b.get('displayName', '?') for b in gcp_b)} · umbrales {rules}" if gcp_b else f"sin presupuesto (cuenta {account or '?'})",
             thresholds=rules)
    aws_b = (ctx.awsj("budgets", "describe-budgets", "--account-id", AWS_ACCOUNT) or {}).get("Budgets") or []
    mine = [b for b in aws_b if str(b.get("BudgetName", "")).startswith("ta-")]
    part.add("K2", "presupuesto de AWS", OK if mine else FALLA,
             ", ".join(f"{b['BudgetName']} {b.get('BudgetLimit', {}).get('Amount')} {b.get('BudgetLimit', {}).get('Unit')}" for b in mine)
             or "sin presupuesto ta-*")
    seen: dict[str, int] = {}
    lake = ctx.gjson("storage", "buckets", "describe", f"gs://{ctx.project}-ta-lake")
    for prefix, days in gcs_rules((lake or {}).get("lifecycle_config") or (lake or {}).get("lifecycle")).items():
        seen[f"gcs {prefix}"] = days
    table = ctx.bq_show(f"{ctx.project}:ta_stream.posts_enriched")
    exp = ((table or {}).get("timePartitioning") or {}).get("expirationMs")
    if exp:
        seen["bigquery posts_enriched"] = int(int(exp) / 86_400_000)
    s3 = ctx.awsj("s3api", "get-bucket-lifecycle-configuration", "--bucket", LAKE_BUCKET)
    for prefix, days in s3_rules(s3).items():
        seen[f"s3 {prefix}"] = days
    want = {k: seen.get(k) for k in RETENTION}
    part.add("K3", "ciclos de vida aplicados = §5.12", OK if want == RETENTION else FALLA, json.dumps(want, ensure_ascii=False), **{
        k.replace(" ", "_"): v for k, v in want.items()})
    res = ctx.awsj("resourcegroupstaggingapi", "get-resources", "--tag-filters", "Key=project,Values=trend-analyzer")
    comps = tag_components((res or {}).get("ResourceTagMappingList") or [])
    missing = sorted(COMPONENTS_AWS - set(comps))
    part.add("K4", "AWS: recursos del proyecto por `component`", OK if not missing and "(sin component)" not in comps else AVISO,
             ", ".join(f"{k} {v}" for k, v in sorted(comps.items())) + (f" · faltan {missing}" if missing else ""), components=dict(comps))
    found = 0
    labels: dict[str, str] = {}
    for kind, args in (("lake", ["storage", "buckets", "describe", f"gs://{ctx.project}-ta-lake"]),
                       ("images", ["artifacts", "repositories", "describe", "ta-images", f"--location={v62.GCP_REGION}"]),
                       ("ingest", ["pubsub", "topics", "describe", "ta-raw-reddit"])):
        data = ctx.gjson(*args) or {}
        value = (data.get("labels") or {}).get("component")
        labels[kind] = value
        found += value == kind
    part.add("K5", "GCP: etiqueta `component` (muestras)", OK if found == 3 else AVISO, json.dumps(labels), labels=labels)
    policies = cost_policies(ctx)
    channels = int(v62.tf_outputs(ctx).get("alert_email_channels") or 0) + 1  # correos + copia en Pub/Sub
    problems = {name: policy_problems(policies[name], channels) if name in policies else ["no existe"] for name in POLICIES}
    bad = {k: v for k, v in problems.items() if v}
    part.add("K6", "alertas de gasto en Cloud Monitoring", FALLA if bad else OK,
             json.dumps(bad, ensure_ascii=False) if bad else ", ".join(f"{n} ({len(policies[n].get('notificationChannels') or [])} canales)" for n in POLICIES))
    return part.finish()


# --- Parte F · disparar las alertas de gasto ----------------------------------------------------------------------


def part_fire(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Alertas de gasto de verdad: umbrales en minutos → notificación → umbrales normales")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs, sim = sv.active_jobs(run, s), sv.simulator_service(run, s)
    if not jobs or not sim:
        part.add("F0", "Dataflow y simulador encendidos", OMITIDO, "hace falta tenerlos encendidos (validate_7_2.py prepare)")
        return part.finish()
    if not ctx.ask("¿Bajar los umbrales de gasto a 0,1 h, esperar las notificaciones y devolverlos a 4 h y 8 h (2 applies)?"):
        part.add("F0", "umbrales", AVISO, "no confirmado")
        return part.finish()
    sub = f"ta-verify-incidents-73-{uuid.uuid4().hex[:6]}"
    v62.create_sub(ctx, sub, v62.INCIDENTS_TOPIC)
    try:
        low = ("-target=module.cost_guard", "-var=cost_dataflow_max_hours=0.1", "-var=cost_simulator_max_hours=0.1")
        res, summary = plan(ctx, GCP_ENV, "gcp_low", low)
        res = ctx.tf(GCP_ENV, "apply", "-input=false", "-lock-timeout=60s", "/tmp/validate73/tfplan_gcp_low", timeout=900) if res.ok else res
        lowered_at = ctx.now()
        part.add("F1", "umbrales a 0,1 h (6 min)", OK if res.ok else FALLA, f"{len(summary.get('update', []))} políticas" if res.ok else res.tail(6))
        seen: dict[str, str] = {}
        if res.ok:
            wait = float(getattr(ctx.options, "watch_minutes", 25) or 25) * 60
            end = time.monotonic() + wait
            while time.monotonic() < end and len(seen) < 2:
                for inc in v62.incidents_from(v62.pull(ctx, sub)):
                    for name in POLICIES:
                        if name in json.dumps(inc) and name not in seen:
                            seen[name] = inc.get("published_at")
                ctx.sleep(20)
        for i, name in enumerate(POLICIES, start=2):
            at = seen.get(name)
            part.add(f"F{i}", f"notificación de {name}", OK if at else FALLA,
                     f"a los {check.seconds_between(lowered_at.strftime('%Y-%m-%dT%H:%M:%S.000Z'), at)} s del cambio" if at else "no llegó en la observación")
    finally:
        res, _ = plan(ctx, GCP_ENV, "gcp_back", ("-target=module.cost_guard",))
        res = ctx.tf(GCP_ENV, "apply", "-input=false", "-lock-timeout=60s", "/tmp/validate73/tfplan_gcp_back", timeout=900) if res.ok else res
        part.add("F4", "umbrales de vuelta a 4 h y 8 h", OK if res.ok else FALLA, "aplicado" if res.ok else res.tail(6))
        v62.delete_sub(ctx, sub)
    return part.finish()


# --- status ---------------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs, sim = sv.active_jobs(run, s), sv.simulator_service(run, s)
    part.add("S1", "Dataflow y simulador", AVISO if jobs or sim else OK, "ENCENDIDOS: off.py" if jobs or sim else "apagados")
    policies = cost_policies(ctx)
    part.add("S2", "alertas de gasto", OK if all(p in policies for p in POLICIES) else AVISO, ", ".join(sorted(policies)) or "no existen")
    return part.finish()


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight, "tests": part_tests, "local": part_local, "terraform": part_terraform,
    "cloud": part_cloud, "fire": part_fire, "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_7_3.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--apply", action="store_true", help="C: aplicar los planes")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--watch-minutes", type=float, default=25, help="F: cuánto se esperan las notificaciones")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm, sleep=sleep or time.sleep,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 7.3 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except sv.OpsError as exc:
        printer(f"error: {exc}")
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
