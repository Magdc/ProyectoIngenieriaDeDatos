"""Validación automática del Lote 6.2 (HU-039, alertas de tendencia): ``docs/validation/validation-6-2.md``.

    python project/scripts/validate_6_2.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes, en el orden de uso: ``preflight`` (A), ``tests`` (B), ``local`` (L: mensaje, ola de prueba, parámetros y
contexto de la imagen, sin nube), ``image`` (D: construye ``alert-notifier`` en Cloud Build y escribe
``alerts.auto.tfvars``), ``terraform`` (C: ``sa-alerts``, notificador, push, canales y política; ``--apply``),
``notify`` (E: alerta sintética en ``ta-alerts`` → log del notificador → notificación de la política), ``prepare``
(P: descarta el atraso de ``ta-raw-*-df``, enciende el simulador y Dataflow con la rama de alertas), ``flow`` (F:
espera el baseline, publica una ola de prueba y mide alerta real, detección < 5 min y *cooldown*), ``email`` (M:
registra que el correo llegó, lo confirma el usuario), ``off`` (O: drena Dataflow y apaga el simulador) y ``status``.
Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su resumen en
``project/evidencia/validacion_6_2.json`` (sin direcciones de correo).

Reutiliza de ``ops/services.py`` el encendido y apagado (``cloud_on``, ``cloud_off``, ``active_jobs``,
``simulator_service``), de ``ops/validate41.py`` ``Ctx`` y de ``ops/validate32.py`` ``Part``, ``Runner``,
``tf_plan``, ``tf_apply``, ``render_tfvars`` y ``pytest_counts``.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from alerts import check
from common import alert_message as am
from ops import services as sv
from ops import validate41 as v41
from ops.validate32 import (
    AVISO,
    FALLA,
    OK,
    OMITIDO,
    WSL_HINT,
    Part,
    Runner,
    diff_text,
    on_windows,
    parse_json,
    plan_line,
    pytest_counts,
    render_tfvars,
    tf_apply,
    tf_plan,
)

REPO = v41.REPO
PROJECT = v41.PROJECT
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_6_2.json"
GCP_ENV = PROJECT / "infra" / "gcp" / "environments" / "dev"
TFVARS = GCP_ENV / "terraform.tfvars"
ALERTS_AUTO = GCP_ENV / "alerts.auto.tfvars"
GCP_REGION = "us-central1"
SERVICE = "ta-alert-notifier"
PUSH_SUBSCRIPTION = "ta-alerts-notifier"
ALERTS_TOPIC = "ta-alerts"
INCIDENTS_TOPIC = "ta-alerts-incidents"
POLICY = "ta-alerta-tendencia"
SUBSCRIPTIONS = tuple(f"ta-raw-{s}-df" for s in ("reddit", "facebook", "instagram"))
ADC = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
TERRAFORM_TARGETS = ["module.sa_alerts", "module.alerts", "google_pubsub_topic_iam_member.dataflow_alerts_publisher"]
#: HU-039: detección en menos de 5 min (fin de la ventana → notificación).
DETECTION_KPI_S = 300
#: §5.12: baseline de 60 ventanas de 1 min tras la primera ventana completa (5 min) + 1 min de margen.
WARMUP = timedelta(minutes=66)
DATAFLOW_USD_PER_HOUR = 0.25
SIMULATOR_USD_PER_HOUR = 0.10

LOT_FILES = [
    "src/common/alert_message.py",
    "src/streaming/alerts.py",
    "src/streaming/tests/test_alerts_pipeline.py",
    "src/alerts/__init__.py",
    "src/alerts/notifier.py",
    "src/alerts/check.py",
    "src/alerts/cli.py",
    "src/alerts/Dockerfile",
    "src/alerts/cloudbuild.yaml",
    "src/alerts/ALERTS.md",
    "src/alerts/tests/__init__.py",
    "src/alerts/tests/test_notifier.py",
    "src/alerts/tests/test_alerts_check.py",
    "src/alerts/tests/test_alerts_terraform.py",
    "scripts/alerts_check.py",
    "scripts/build_alerts_image.sh",
    "infra/gcp/modules/alert_notifier/main.tf",
    "infra/gcp/environments/dev/alerts.tf",
    "infra/gcp/environments/dev/variables_alerts.tf",
    "infra/gcp/environments/dev/outputs_alerts.tf",
    "src/ops/validate62.py",
    "src/ops/tests/test_ops_validate62.py",
    "scripts/validate_6_2.py",
]
PREVIOUS_FILES = ["src/streaming/trends.py", "src/streaming/trends_core.py", "src/ops/services.py", "src/simulator/factory.py",
                  "src/smoke/e2e.py", "src/lakehouse/glue_jobs/raw_to_trusted.py", "scripts/dataflow_job.sh"]

B1_EXPECTED = 20
B2_EXPECTED = 6
B3_EXPECTED = 151
B4_EXPECTED = 12


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "6.2", "hu": ["HU-039"], "parts": {}}

    def gcloud(self, *args: str, timeout: float = 180) -> Any:
        return self.run(["gcloud", *args, "--project", self.project], timeout=timeout)

    def gjson(self, *args: str, timeout: float = 180) -> Any:
        """``gcloud … --format=json`` ya interpretado (``None`` si falla)."""
        res = self.gcloud(*args, "--format=json", timeout=timeout)
        return parse_json(res.out) if res.ok else None

    def bq(self, sql: str) -> list[dict[str, Any]] | None:
        res = self.run(["bq", f"--project_id={self.project}", "query", "--use_legacy_sql=false", "--format=json",
                        "--max_rows=10000", sql], timeout=300)
        data = parse_json(res.out) if res.ok else None
        return data if isinstance(data, list) else None

    def alerts_cli(self, *args: str, timeout: float = 300) -> list[dict[str, Any]]:
        res = self.run([sys.executable, "scripts/alerts_check.py", *args, "--project", self.project], cwd=PROJECT,
                       timeout=timeout)
        self.cache["cli_tail"] = res.tail(6)
        return events(res.out)


def sv_runner(ctx: Ctx) -> Callable[..., sv.Result]:
    """Adaptador de ``ctx.run`` a la firma de ``ops.services`` (como en ``validate54``)."""

    def call(args: list[str], *, capture: bool = True, env: dict | None = None, check: bool = False) -> sv.Result:
        res = ctx.run(list(args), cwd=PROJECT, env=env, timeout=1800, stream=not capture)
        out = sv.Result(res.code, res.out, res.err)
        if check and not out.ok:
            raise sv.OpsError(f"falló: {' '.join(args)}")
        return out

    return call


def settings(ctx: Ctx) -> sv.Settings:
    return sv.Settings(project=ctx.project, yes=True, force=bool(getattr(ctx.options, "force", False)))


# --- utilidades puras ----------------------------------------------------------------------------


def events(text: str) -> list[dict[str, Any]]:
    return [e for e in (parse_json(ln) for ln in (text or "").splitlines() if ln.strip().startswith("{")) if isinstance(e, dict)]


def last_event(evs: list[dict[str, Any]], name: str) -> dict[str, Any]:
    found = [e for e in evs if e.get("event") == name]
    return found[-1] if found else {}


_EMAILS_RE = re.compile(r'^\s*alert_emails\s*=\s*\[(?P<body>[^\]]*)\]', re.M)


def read_alert_emails(text: str) -> list[str]:
    m = _EMAILS_RE.search(text or "")
    return re.findall(r'"([^"]+)"', m.group("body")) if m else []


def set_alert_emails(text: str, emails: list[str]) -> str:
    line = "alert_emails = [" + ", ".join(json.dumps(e) for e in emails) + "]"
    if _EMAILS_RE.search(text):
        return _EMAILS_RE.sub(line, text, count=1)
    sep = "" if text.endswith("\n") or not text else "\n"
    return f"{text}{sep}\n# Lote 6.2 · destinatarios de las alertas (§5.12); no se versiona.\n{line}\n"


def budget_emails(channels: list[dict[str, Any]] | None) -> list[str]:
    """Direcciones de los canales de correo del presupuesto (``ta-budget-*``, Lote 1.0): el destinatario acordado."""
    out = []
    for c in channels or []:
        if c.get("type") == "email" and str(c.get("displayName", "")).startswith("ta-budget-"):
            addr = (c.get("labels") or {}).get("email_address")
            if addr and addr not in out:
                out.append(addr)
    return out


def expected_creates(n_emails: int, with_image: bool) -> set[str]:
    out = {
        "module.sa_alerts.google_service_account.this",
        "google_pubsub_topic_iam_member.dataflow_alerts_publisher",
        "module.alerts.google_pubsub_topic.incidents",
        "module.alerts.google_monitoring_notification_channel.incidents",
        "module.alerts.google_pubsub_topic_iam_member.incidents_publisher",
        "module.alerts.google_monitoring_alert_policy.trend",
    }
    out |= {f'module.alerts.google_monitoring_notification_channel.email["{i}"]' for i in range(n_emails)}
    if with_image:
        out |= {"module.alerts.google_cloud_run_v2_service.this[0]", "module.alerts.google_cloud_run_v2_service_iam_member.invoker[0]",
                "module.alerts.google_pubsub_subscription.push[0]"}
    return out


def image_from_output(text: str) -> str | None:
    found = re.findall(r'^alerts_notifier_image = "([^"]+)"', text or "", flags=re.M)
    return found[-1] if found else None


def pulled(text: str) -> list[dict[str, Any]]:
    """Salida de ``gcloud pubsub subscriptions pull --format=json`` → ``[{data, attributes, publish_time, message_id}]``."""
    out = []
    for item in parse_json(text) or []:
        msg = (item or {}).get("message") or {}
        try:
            data = base64.b64decode(msg.get("data") or "")
        except (ValueError, TypeError):
            data = b""
        out.append({"data": data, "attributes": msg.get("attributes") or {}, "publish_time": msg.get("publishTime"),
                    "message_id": msg.get("messageId")})
    return out


def iso_ms(text: str | None) -> str | None:
    """``2026-10-27T21:30:15.123456Z`` (Pub/Sub, Logging) → ``2026-10-27T21:30:15.123Z``."""
    if not text:
        return None
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z$", text)
    if not m:
        return None
    return f"{m.group(1)}.{(m.group(2) or '0')[:3].ljust(3, '0')}Z"


def payloads(entries: Any) -> list[dict[str, Any]]:
    """``jsonPayload`` de las entradas de ``gcloud logging read --format=json`` (más su ``timestamp`` y ``severity``).

    Cloud Logging saca ``severity`` del JSON que escribe el servicio y la pone en la entrada (visto en la validación):
    por eso se lee de la entrada (``_severity``), no del ``jsonPayload``.
    """
    out = []
    for e in entries or []:
        p = (e or {}).get("jsonPayload")
        if isinstance(p, dict):
            out.append({**p, "_timestamp": iso_ms(e.get("timestamp")) or e.get("timestamp"), "_severity": e.get("severity")})
    return out


def rendered_problems(text: str, alert: dict[str, Any]) -> list[str]:
    """Lo que falta en el texto de la notificación (marca, fuente, tipo, alerta) o variables sin reemplazar."""
    out = [f"falta {k} ({alert[k]})" for k in ("brand", "source", "alert_type", "alert_id") if str(alert[k]) not in (text or "")]
    if "${" in (text or ""):
        out.append("quedaron variables sin reemplazar (${…})")
    return out


def warm_at(first_window: str) -> datetime:
    return am.parse_ts(first_window) + WARMUP


def metric_windows(rows: list[dict[str, Any]], metric: str, threshold: float = 3.0) -> list[dict[str, Any]]:
    col = "zscore_volume" if metric == "volume" else "zscore_sentiment"
    out = []
    for r in rows:
        z = r.get(col)
        if str(r.get("is_alert")).lower() == "true" and z not in (None, "") and abs(float(z)) >= threshold:
            out.append(r)
    return out


def tfvars_ok(text: str) -> bool:
    return bool(read_alert_emails(text))


# --- consultas a la nube ------------------------------------------------------------------------------


def tf_outputs(ctx: Ctx) -> dict[str, Any]:
    if "outputs" not in ctx.cache:
        res = ctx.tf(GCP_ENV, "output", "-json", timeout=300)
        data = parse_json(res.out) if res.ok else None
        ctx.cache["outputs"] = {k: (v or {}).get("value") for k, v in (data or {}).items()}
    return ctx.cache["outputs"]


def notifier_logs(ctx: Ctx, extra: str, freshness: str = "30m", limit: int = 50) -> list[dict[str, Any]]:
    flt = (f'resource.type="cloud_run_revision" AND resource.labels.service_name="{SERVICE}" '
           f'AND jsonPayload.alert="trend" AND {extra}')
    return payloads(ctx.gjson("logging", "read", flt, f"--freshness={freshness}", f"--limit={limit}", timeout=120))


def create_sub(ctx: Ctx, name: str, topic: str) -> bool:
    res = ctx.gcloud("pubsub", "subscriptions", "create", name, f"--topic={topic}", "--expiration-period=1d",
                     "--message-retention-duration=1h", "--ack-deadline=30", timeout=120)
    return res.ok


def pull(ctx: Ctx, sub: str, limit: int = 100) -> list[dict[str, Any]]:
    res = ctx.gcloud("pubsub", "subscriptions", "pull", sub, "--auto-ack", f"--limit={limit}", "--format=json", timeout=120)
    return pulled(res.out) if res.ok else []


def delete_sub(ctx: Ctx, name: str) -> None:
    ctx.gcloud("pubsub", "subscriptions", "delete", name, "--quiet", timeout=120)


def wait_for(ctx: Ctx, fn: Callable[[], Any], timeout_s: float, poll_s: float) -> Any:
    """Llama a ``fn`` cada ``poll_s`` hasta que devuelva algo o pasen ``timeout_s`` (cuenta las esperas de ``ctx.sleep``)."""
    got = fn()
    for _ in range(int(timeout_s // poll_s)):
        if got:
            return got
        ctx.sleep(poll_s)
        got = fn()
    return got


def incidents_from(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for m in messages:
        inc = check.parse_incident(m["data"])
        if inc:
            out.append({**inc, "published_at": iso_ms(m.get("publish_time"))})
    return out


# --- Parte A · preflight -----------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código, de GCP y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    missing = []
    for mod in ("apache_beam", "google.cloud.pubsub_v1", "jsonschema"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    part.add("A1", "Python y dependencias", FALLA if missing or (py.major, py.minor) != (3, 11) else OK,
             f"Python {py.major}.{py.minor}.{py.micro}" + (f"; faltan: {', '.join(missing)}" if missing else "; Beam, Pub/Sub y jsonschema"))
    absent = [f for f in LOT_FILES + PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote y {len(PREVIOUS_FILES)} que reutiliza", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else "presentes")
    tools = {t: ctx.runner.which(t) for t in ("gcloud", "bq", "terraform", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A3", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))
    part.add("A4", "proyecto de GCP y ADC", OK if ADC.exists() and ctx.project else FALLA,
             f"{ctx.project} · ADC {'presente' if ADC.exists() else 'FALTA: gcloud auth application-default login'}")
    run, s = sv_runner(ctx), settings(ctx)
    try:
        jobs = sv.active_jobs(run, s)
        sim = sv.simulator_service(run, s)
    except sv.OpsError as exc:
        part.add("A5", "Dataflow y simulador", FALLA, str(exc))
        return part.finish()
    topic = ctx.gjson("pubsub", "topics", "describe", ALERTS_TOPIC)
    part.add("A5", "Dataflow, simulador y topic ta-alerts", OK if topic else FALLA,
             f"Dataflow: {', '.join(j.get('name', '?') for j in jobs) or 'apagado'} · simulador: "
             f"{'ENCENDIDO' if sim else 'apagado'} · {ALERTS_TOPIC}: {'existe (Lote 1.3)' if topic else 'FALTA'}",
             dataflow=[j.get("name") for j in jobs], simulator=bool(sim))
    try:
        emails = read_alert_emails(TFVARS.read_text(encoding="utf-8"))
    except OSError:
        emails = []
    part.add("A6", "alert_emails en terraform.tfvars (no se versiona)", OK if emails else AVISO,
             f"{len(emails)} dirección(es)" if emails else "vacío: la parte C lo completa con el destinatario del presupuesto (§5.12)",
             emails=len(emails))
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/alerts (notificador, contrato, comprobaciones, Terraform)", ["src/alerts", "-q"], B1_EXPECTED),
        ("B2", "pytest test_alerts_pipeline.py (rama de alertas con TestStream)",
         ["src/streaming/tests/test_alerts_pipeline.py", "-q", "-W", "ignore::FutureWarning"], B2_EXPECTED),
        ("B3", "regresión: pytest src/streaming -k trends (Lote 2.4)",
         ["src/streaming/tests", "-q", "-k", "trends", "-W", "ignore::FutureWarning"], B3_EXPECTED),
        ("B4", "pytest test_ops_validate62.py", ["src/ops/tests/test_ops_validate62.py", "-q"], B4_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · local -----------------------------------------------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Mensaje, ola de prueba, parámetros e imagen (sin nube)")
    res = ctx.run([sys.executable, "scripts/alerts_check.py", "plan"], cwd=PROJECT, timeout=300)
    plan = last_event(events(res.out), "alerts_plan")
    if not plan:
        part.add("L1", "alerts_check.py plan", FALLA, res.tail(8))
        return part.finish()
    burst = plan.get("burst") or {}
    good = res.ok and not plan.get("sample_problems") and not burst.get("contract_problems") and not burst.get("problems")
    part.add("L1", "alerta de ejemplo y ola de prueba frente a los contratos", OK if good else FALLA,
             f"ta-alert {am.SCHEMA_VERSION}: {plan.get('bytes')} bytes, atributos {sorted(plan.get('attributes') or {})} · ola: "
             f"{burst.get('count')} comentarios {burst.get('generator')} de {burst.get('brand')} en {burst.get('source')} "
             f"(escenario {burst.get('scenario')}, envelope v1.0 sin errores)" if good else
             json.dumps({"sample": plan.get("sample_problems"), "burst": burst}, ensure_ascii=False)[:600], text=plan.get("text"))
    sample = plan.get("sample") or {}
    hu = {"marca": sample.get("brand"), "fuente": sample.get("source"), "ventana": sample.get("window_start"),
          "magnitud": sample.get("zscore") is not None and sample.get("change_pct") is not None}
    part.add("L2", "la alerta lleva marca, fuente, ventana y magnitud (HU-039)", OK if all(hu.values()) else FALLA,
             plan.get("text") or "", aviso=plan.get("aviso"))
    from streaming.alerts import DEFAULT_COOLDOWN_SECONDS
    from streaming.trends_core import TrendConfig

    cfg = TrendConfig()
    module = (PROJECT / "infra/gcp/modules/alert_notifier/main.tf").read_text(encoding="utf-8")
    rate = re.search(r'variable "rate_limit_seconds" \{.*?default\s*=\s*(\d+)', module, flags=re.S)
    params = {"cooldown": DEFAULT_COOLDOWN_SECONDS, "z": cfg.z_threshold, "min_posts": cfg.min_posts,
              "baseline": cfg.baseline_windows, "rate_limit": int(rate.group(1)) if rate else None}
    want = {"cooldown": 900, "z": 3.0, "min_posts": 5, "baseline": 60, "rate_limit": 300}
    part.add("L3", "parámetros = §5.12", OK if params == want else FALLA,
             f"cooldown {params['cooldown']} s por marca × fuente × tipo · |z| ≥ {params['z']} · n_posts ≥ {params['min_posts']} · "
             f"baseline {params['baseline']} ventanas · como máximo un correo cada {params['rate_limit']} s" if params == want else
             f"código {params} ≠ §5.12 {want}", params=params)
    res = ctx.run(["bash", "scripts/build_alerts_image.sh", "--dry-run"], cwd=PROJECT, timeout=120,
                  env={"GCP_PROJECT": ctx.project})
    image = image_from_output(res.out)
    files = re.search(r"contexto:\s*(\d+) archivos", res.out or "")
    good = res.ok and image and files and int(files.group(1)) == 8
    part.add("L4", "contexto de la imagen alert-notifier (dry-run)", OK if good else FALLA,
             f"{image} · {files.group(1) if files else '?'} archivos (notificador, su prueba, common/alert_message.py, Dockerfile y "
             f"cloudbuild.yaml)" if good else res.tail(6), image=image)
    return part.finish()


# --- Parte D · imagen --------------------------------------------------------------------------------------


def write_alerts_auto(ctx: Ctx, image: str) -> Path:
    header = "Lote 6.2 · generado por validate_6_2.py (parte D). No se versiona.\nImagen del notificador de alertas."
    ALERTS_AUTO.write_text(render_tfvars(header, {"alerts_notifier_image": image}), encoding="utf-8")
    ctx.run(["terraform", "fmt", str(ALERTS_AUTO)], timeout=60)
    return ALERTS_AUTO


def part_image(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Imagen del notificador (Cloud Build)")
    env = {"GCP_PROJECT": ctx.project, "GCP_REGION": GCP_REGION}
    res = ctx.run(["bash", "scripts/build_alerts_image.sh", "--dry-run"], cwd=PROJECT, timeout=120, env=env)
    image = image_from_output(res.out)
    if not image:
        part.add("D1", "etiqueta de la imagen", FALLA, res.tail(6))
        return part.finish()
    exists = ctx.gcloud("artifacts", "docker", "images", "describe", image, "--format=value(image_summary.digest)", timeout=120)
    if exists.ok and exists.out.strip():
        part.add("D1", "imagen alert-notifier", OK, f"{image} ya existe (mismo contenido): no se reconstruye", image=image)
    elif not ctx.ask(f"¿Construir {image} en Cloud Build (≈ 1–2 min)?"):
        part.add("D1", "imagen alert-notifier", AVISO, "no confirmado")
        return part.finish()
    else:
        part.note("Cloud Build: pruebas del notificador dentro de la imagen y publicación…")
        res = ctx.run(["bash", "scripts/build_alerts_image.sh", "cloudbuild"], cwd=PROJECT, timeout=1200, env=env, stream=True)
        built = image_from_output(res.out)
        passed = re.search(r"(\d+) passed", res.out or "")
        part.add("D1", "imagen alert-notifier", OK if res.ok and built == image else FALLA,
                 f"{image} · {passed.group(1) if passed else '?'} pruebas en la imagen · {res.seconds:.0f} s" if res.ok else res.tail(10),
                 image=image, seconds=round(res.seconds, 1))
        if not res.ok:
            return part.finish()
    write_alerts_auto(ctx, image)
    part.add("D2", "alerts.auto.tfvars", OK, f"infra/gcp/environments/dev/{ALERTS_AUTO.name} con alerts_notifier_image (no se versiona)")
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------------------


def check_resources(ctx: Ctx, outputs: dict[str, Any], n_emails: int) -> list[str]:
    problems = []
    svc = ctx.gjson("run", "services", "describe", SERVICE, "--region", GCP_REGION) or {}
    conditions = {c.get("type"): c.get("status") for c in (svc.get("status") or {}).get("conditions") or []}
    if conditions.get("Ready") != "True":
        problems.append(f"{SERVICE} no está listo: {conditions}")
    policy = ctx.gjson("run", "services", "get-iam-policy", SERVICE, "--region", GCP_REGION) or {}
    members = sorted(m for b in policy.get("bindings") or [] for m in b.get("members") or [])
    if members != [f"serviceAccount:sa-alerts@{ctx.project}.iam.gserviceaccount.com"]:
        problems.append(f"invocadores de {SERVICE}: {members} (solo sa-alerts)")
    sub = ctx.gjson("pubsub", "subscriptions", "describe", PUSH_SUBSCRIPTION) or {}
    push = sub.get("pushConfig") or {}
    if push.get("pushEndpoint") != outputs.get("alerts_notifier_url") or not (push.get("oidcToken") or {}).get("serviceAccountEmail", "").startswith("sa-alerts@"):
        problems.append(f"push de {PUSH_SUBSCRIPTION}: {push}")
    topic_policy = ctx.gjson("pubsub", "topics", "get-iam-policy", ALERTS_TOPIC) or {}
    pubs = [m for b in topic_policy.get("bindings") or [] if b.get("role") == "roles/pubsub.publisher" for m in b.get("members") or []]
    if f"serviceAccount:sa-dataflow@{ctx.project}.iam.gserviceaccount.com" not in pubs:
        problems.append(f"sa-dataflow no publica en {ALERTS_TOPIC}: {pubs}")
    pols = ctx.gjson("monitoring", "policies", "list", f"--filter=displayName={POLICY}") or []
    if len(pols) != 1:
        problems.append(f"políticas {POLICY}: {len(pols)}")
    else:
        p = pols[0]
        strat = p.get("alertStrategy") or {}
        if not p.get("enabled", True) or len(p.get("notificationChannels") or []) != n_emails + 1:
            problems.append(f"{POLICY}: enabled={p.get('enabled')} canales={len(p.get('notificationChannels') or [])}")
        if (strat.get("notificationRateLimit") or {}).get("period") != "300s":
            problems.append(f"{POLICY}: rate limit {strat.get('notificationRateLimit')}")
    return problems


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (sa-alerts, notificador, push, canales y política)")
    apply = bool(getattr(ctx.options, "apply", False))
    try:
        text = TFVARS.read_text(encoding="utf-8")
    except OSError:
        part.add("C0", "terraform.tfvars", FALLA, f"no existe {TFVARS.relative_to(PROJECT)}")
        return part.finish()
    emails = read_alert_emails(text)
    if not emails:
        emails = budget_emails(ctx.gjson("beta", "monitoring", "channels", "list"))
        if emails:
            TFVARS.write_text(set_alert_emails(text, emails), encoding="utf-8")
    part.add("C0", "alert_emails (terraform.tfvars, no se versiona)", OK if emails else FALLA,
             f"{len(emails)} dirección(es) (el destinatario del presupuesto, §5.12)" if emails else
             "vacío y sin canal ta-budget-*: agregue alert_emails = [\"…\"] a terraform.tfvars", emails=len(emails))
    if not emails:
        return part.finish()
    with_image = bool(v41_tfvars_image())
    part.add("C0b", "imagen del notificador", OK if with_image else AVISO,
             "alerts.auto.tfvars presente" if with_image else "sin imagen (corra la parte D): se crea todo menos el servicio y la push")
    # Cloud Monitoring no crea su agente de notificaciones al crear el canal de Pub/Sub por API (visto en la
    # validación: «Service account service-…@gcp-sa-monitoring-notification… does not exist»). Es idempotente.
    res = ctx.gcloud("beta", "services", "identity", "create", "--service=monitoring.googleapis.com", timeout=180)
    agent = re.search(r"service-\d+@gcp-sa-monitoring-notification\.iam\.gserviceaccount\.com", res.text or "")
    part.add("C0c", "agente de notificaciones de Cloud Monitoring", OK if res.ok and agent else FALLA,
             f"{agent.group(0)} (publica en {INCIDENTS_TOPIC})" if agent else res.tail(3))
    if not (res.ok and agent):
        return part.finish()
    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()
    res = ctx.tf(GCP_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
    part.add("C2a", "terraform init (instala alert_notifier)", OK if res.ok else FALLA, res.tail(3))
    if not res.ok:
        return part.finish()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / "tfplan_6_2"
    title = "plan (-target de los recursos del lote)"
    res, summary = tf_plan(ctx, GCP_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", title, FALLA, res.err or res.tail(8))
        return part.finish()
    expected = expected_creates(len(emails), with_image)
    creates = set(summary["create"])
    extra = (creates - expected) | set(summary["delete"] + summary["replace"])
    # Una actualización solo se admite en el servicio (imagen nueva).
    extra |= {a for a in summary["update"] if a != "module.alerts.google_cloud_run_v2_service.this[0]"}
    if extra:
        part.add("C2", title, FALLA, f"{plan_line(summary)}; no aplique: toca {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    changes = summary["create"] + summary["update"]
    part.add("C2", title, OK, plan_line(summary) + (" (ya aplicado)" if not changes else
             f" (esperado: hasta {len(expected)} a crear; nada a destruir)"), **summary)
    if changes:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
            return part.finish()
        if not ctx.ask(f"¿Aplicar el plan de infra/gcp ({plan_line(summary)})?"):
            part.add("C3", "apply", AVISO, "no confirmado")
            return part.finish()
        res = tf_apply(ctx, GCP_ENV, plan_path)
        good = res.ok and "Apply complete!" in res.out
        part.add("C3", "apply", OK if good else FALLA, res.tail(4) if res.ok else res.err)
        if not res.ok:
            return part.finish()
        res, summary = tf_plan(ctx, GCP_ENV, TERRAFORM_TARGETS, plan_path)
        clean = summary is not None and not any(summary[k] for k in ("create", "update", "delete", "replace"))
        part.add("C4", "plan después del apply", OK if clean else FALLA,
                 "sin cambios" if clean else (plan_line(summary) if summary else res.err), **(summary or {}))
    ctx.cache.pop("outputs", None)
    outputs = tf_outputs(ctx)
    if not with_image or not outputs.get("alerts_notifier_url"):
        part.add("C5", "recursos reales", OMITIDO, "sin servicio todavía (parte D y C otra vez)")
        return part.finish()
    problems = check_resources(ctx, outputs, len(emails))
    part.add("C5", "recursos reales (servicio, invocadores, push, permisos y política)", FALLA if problems else OK,
             "\n".join(problems) if problems else
             f"{SERVICE} listo y privado (solo sa-alerts invoca) · push {PUSH_SUBSCRIPTION} con ID token de sa-alerts · "
             f"sa-dataflow publica en {ALERTS_TOPIC} · {POLICY} con {len(emails)} correo(s) + Pub/Sub y 1 correo cada 5 min como máximo",
             notifier_url=outputs.get("alerts_notifier_url"), image=outputs.get("alerts_notifier_image"))
    return part.finish()


def v41_tfvars_image() -> str | None:
    try:
        m = re.search(r'^\s*alerts_notifier_image\s*=\s*"([^"]+)"', ALERTS_AUTO.read_text(encoding="utf-8"), flags=re.M)
    except OSError:
        return None
    return m.group(1) if m else None


# --- Parte E · alerta sintética ---------------------------------------------------------------------------


def part_notify(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Alerta sintética: ta-alerts → notificador → log → política → notificación")
    outputs = tf_outputs(ctx)
    if not outputs.get("alerts_notifier_url") or not outputs.get("alerts_incidents_topic"):
        part.add("E0", "notificador desplegado", FALLA, "faltan alerts_notifier_url o alerts_incidents_topic: corra D y C con --apply")
        return part.finish()
    if not ctx.ask("¿Publicar una alerta de prueba en ta-alerts (llega un correo marcado [prueba])?"):
        part.add("E0", "alerta sintética", AVISO, "no confirmado")
        return part.finish()
    run_id = uuid.uuid4().hex[:8]
    sub = f"ta-verify-incidents-{run_id}"
    if not create_sub(ctx, sub, INCIDENTS_TOPIC):
        part.add("E1", "suscripción temporal a ta-alerts-incidents", FALLA, sub)
        return part.finish()
    part.add("E1", "suscripción temporal a ta-alerts-incidents", OK, f"{sub} (expira en 1 día; se borra al terminar)")
    try:
        pub = last_event(ctx.alerts_cli("publish-test", *brand_args(ctx)), "alert_test_published")
        if not pub:
            part.add("E2", "publish-test", FALLA, ctx.cache.get("cli_tail", ""))
            return part.finish()
        part.add("E2", "alerta publicada en ta-alerts", OK, f"{pub['alert_id']} · {pub['text']}", **pub)
        logs = wait_for(ctx, lambda: notifier_logs(ctx, f'jsonPayload.alert_id="{pub["alert_id"]}"', "15m"), 240, 10)
        rec = logs[0] if logs else {}
        lag = check.seconds_between(pub["published_at"], rec.get("received_at"))
        good = rec.get("_severity") == "WARNING" and rec.get("aviso", {}).get("alerta_id") == pub["alert_id"] and rec.get("message") == pub["text"]
        part.add("E3", "log del notificador (WARNING, alert=trend)", OK if good else FALLA,
                 f"{lag} s de la publicación al notificador (incluye el arranque en frío) · severidad {rec.get('_severity')} · "
                 f"aviso {sorted(rec.get('aviso') or {})}" if good else
                 f"no apareció en 4 min o no coincide (severidad {rec.get('_severity')}): {json.dumps(rec, ensure_ascii=False)[:300]}",
                 publish_to_log_s=lag, received_at=rec.get("received_at"))
        seen: list[dict[str, Any]] = []

        def found():
            seen.extend(incidents_from(pull(ctx, sub)))
            return check.incident_for(seen, pub["alert_id"])

        inc = wait_for(ctx, found, 420, 15)
        lag2 = check.seconds_between(rec.get("received_at"), (inc or {}).get("published_at"))
        part.add("E4", f"notificación de {POLICY} (canal Pub/Sub)", OK if inc else FALLA,
                 f"incidente {inc.get('incident_id')} · {lag2} s del log a la notificación · "
                 f"{check.seconds_between(pub['published_at'], inc.get('published_at'))} s desde la publicación" if inc else
                 f"no llegó en 7 min ({len(seen)} notificaciones de otras alertas)",
                 log_to_notification_s=lag2, incident=(inc or {}).get("incident_id"), notified_at=(inc or {}).get("published_at"))
        if inc:
            alert = check.synthetic_alert()  # solo para los nombres de los campos
            alert.update({k: pub[k] for k in ("brand", "source", "alert_type", "alert_id")})
            problems = rendered_problems(f"{inc.get('subject')} {inc.get('content')}", alert)
            part.add("E5", "texto del correo (asunto y cuerpo de la política)", FALLA if problems else OK,
                     "\n".join(problems) if problems else f"asunto: {inc.get('subject')}", subject=inc.get("subject"))
        part.add("E6", "correo en el buzón", AVISO, "compruébelo a mano (asunto «TrendAnalyzer: volume_up de …», [prueba]) y "
                 "regístrelo con: python project/scripts/validate_6_2.py email --note \"…\"")
    finally:
        delete_sub(ctx, sub)
    return part.finish()


def brand_args(ctx: Ctx) -> list[str]:
    return ["--brand", getattr(ctx.options, "brand", None) or check.DEFAULT_BRAND,
            "--source", getattr(ctx.options, "source", None) or check.DEFAULT_SOURCE]


# --- Parte P · preparar la nube --------------------------------------------------------------------------------


def part_prepare(ctx: Ctx) -> int:
    part = Part(ctx, "P", "Preparar: descartar el atraso, encender el simulador y Dataflow con la rama de alertas")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    if jobs:
        part.add("P1", "seek de ta-raw-*-df", OMITIDO, f"Dataflow ya está encendido ({jobs[0].get('name')}): si se lanzó antes del "
                 "código del 6.2 no tiene la rama de alertas (off y prepare otra vez)")
    elif ctx.ask(f"¿Descartar con seek lo pendiente en ta-raw-*-df (irreversible) y encender el simulador y Dataflow con NLP "
                 f"(≈ {DATAFLOW_USD_PER_HOUR + SIMULATOR_USD_PER_HOUR:.2f} USD/h hasta el off)?"):
        when = ctx.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        bad = [sub for sub in SUBSCRIPTIONS if not ctx.gcloud("pubsub", "subscriptions", "seek", sub, f"--time={when}", timeout=120).ok]
        part.add("P1", "seek de ta-raw-*-df a ahora", FALLA if bad else OK,
                 f"fallaron: {bad}" if bad else f"{len(SUBSCRIPTIONS)} suscripciones movidas a {when}", seek_time=when)
        if bad:
            return part.finish()
    else:
        part.add("P1", "seek, simulador y Dataflow", AVISO, "no confirmado")
        return part.finish()
    try:
        sv.cloud_on(run, s, part.note)
    except sv.OpsError as exc:
        part.add("P2", "simulador y Dataflow (ops.services.cloud_on)", FALLA, str(exc)[-800:])
        return part.finish()
    jobs = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    running = [j for j in jobs if str(j.get("state") or j.get("currentState")).lower().endswith("running")]
    part.add("P2", "simulador y Dataflow en marcha", OK if running and sim else FALLA,
             f"simulador {sim or 'APAGADO'} · {running[0].get('name')} ({running[0].get('id')})" if running else
             f"{[(j.get('name'), j.get('state')) for j in jobs] or 'ningún job'}",
             job=(running[0].get("id") if running else None), started_at=ctx.now().isoformat())
    if running:
        part.note(f"el baseline se completa ≈ {WARMUP.seconds // 60} min después de los primeros datos: luego, la parte flow")
    return part.finish()


# --- Parte F · alerta real ----------------------------------------------------------------------------------


def first_window(ctx: Ctx, brand: str, source: str, since: datetime) -> tuple[str | None, int]:
    rows = ctx.bq(
        f"SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', MIN(window_start)) AS first, COUNT(*) AS n "
        f"FROM `{ctx.project}.ta_stream.trend_windows` WHERE brand = '{brand}' AND source = '{source}' "
        f"AND window_start >= TIMESTAMP('{(since - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')}')") or [{}]
    return rows[0].get("first"), int(rows[0].get("n") or 0)


def key_windows(ctx: Ctx, brand: str, source: str, since: str) -> list[dict[str, Any]]:
    return ctx.bq(
        f"SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', window_start) AS window_start, "
        f"FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', window_end) AS window_end, n_posts, avg_sentiment, zscore_volume, "
        f"zscore_sentiment, is_alert FROM `{ctx.project}.ta_stream.trend_windows_latest` WHERE brand = '{brand}' "
        f"AND source = '{source}' AND window_end >= TIMESTAMP('{since}') ORDER BY window_start") or []


def part_flow(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Alerta real: ola de prueba → trend_windows → ta-alerts → notificador → notificación")
    brand, source = brand_args(ctx)[1], brand_args(ctx)[3]
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    outputs = tf_outputs(ctx)
    started = ctx.step_data("P", "P2").get("started_at")
    if not jobs or not outputs.get("alerts_notifier_url") or not started:
        part.add("F0", "Dataflow con alertas y notificador", FALLA,
                 f"Dataflow {'encendido' if jobs else 'APAGADO'} · notificador {outputs.get('alerts_notifier_url') or 'FALTA'} · "
                 f"parte P {'hecha' if started else 'sin correr'}: corra prepare (y D/C)")
        return part.finish()
    since = datetime.fromisoformat(started)
    part.add("F0", "Dataflow con alertas y notificador", OK, f"{jobs[0].get('name')} desde {started[:19]} UTC · {outputs['alerts_notifier_url']}")
    max_wait = float(getattr(ctx.options, "max_wait_minutes", 90) or 0) * 60
    waited = 0.0
    while True:
        first, n = first_window(ctx, brand, source, since)
        ready = warm_at(first) if first else None
        left = (ready - ctx.now()).total_seconds() if ready else None
        if ready and left <= 0:
            break
        if waited >= max_wait:
            part.add("F1", "baseline completo (60 ventanas)", AVISO,
                     f"todavía no: primera ventana {first or 'sin datos'}; listo ≈ {ready:%H:%M} UTC" if ready else
                     f"sin ventanas de {brand}/{source} desde {started[:19]}: ¿corre el simulador?")
            return part.finish()
        part.note(f"esperando el baseline de {brand}/{source}: {n} ventanas, listo ≈ "
                  f"{ready:%H:%M:%S} UTC ({left / 60:.0f} min)" if ready else "esperando las primeras ventanas…")
        pause = min(300.0, max(30.0, left or 60.0))
        ctx.sleep(pause)
        waited += pause
    part.add("F1", "baseline completo (60 ventanas)", OK, f"primera ventana de {brand}/{source}: {first} · listo desde {ready:%H:%M} UTC",
             first_window=first)
    run_id = uuid.uuid4().hex[:8]
    subs = {"alerts": f"ta-verify-alerts-{run_id}", "incidents": f"ta-verify-incidents-{run_id}"}
    if not (create_sub(ctx, subs["alerts"], ALERTS_TOPIC) and create_sub(ctx, subs["incidents"], INCIDENTS_TOPIC)):
        part.add("F2", "suscripciones temporales", FALLA, str(subs))
        for name in subs.values():
            delete_sub(ctx, name)
        return part.finish()
    part.add("F2", "suscripciones temporales a ta-alerts y ta-alerts-incidents", OK, ", ".join(subs.values()))
    try:
        count = str(getattr(ctx.options, "count", None) or check.DEFAULT_COUNT)
        burst = last_event(ctx.alerts_cli("burst", *brand_args(ctx), "--count", count, "--run-id", run_id), "burst_published")
        if not burst:
            part.add("F3", "ola de prueba", FALLA, ctx.cache.get("cli_tail", ""))
            return part.finish()
        part.add("F3", "ola de prueba publicada", OK, f"{burst['count']} comentarios {burst['generator']} de {brand} en {source} "
                 f"entre {burst['published_from'][11:19]} y {burst['published_to'][11:19]} UTC (escenario {burst['scenario']})", burst=burst)
        watch = float(getattr(ctx.options, "watch_minutes", 12) or 12) * 60
        messages: list[dict[str, Any]] = []
        incidents: list[dict[str, Any]] = []
        part.note(f"observando ta-alerts y las notificaciones {watch / 60:.0f} min…")
        for _ in range(int(watch // 20)):
            messages += pull(ctx, subs["alerts"])
            incidents += incidents_from(pull(ctx, subs["incidents"]))
            ctx.sleep(20)
        messages += pull(ctx, subs["alerts"])
        incidents += incidents_from(pull(ctx, subs["incidents"]))
    finally:
        for name in subs.values():
            delete_sub(ctx, name)
    flow_report(ctx, part, brand, source, burst, messages, incidents)
    return part.finish()


def flow_report(ctx: Ctx, part: Part, brand: str, source: str, burst: dict[str, Any], messages: list[dict[str, Any]],
                incidents: list[dict[str, Any]]) -> None:
    alerts, bad = [], []
    for m in messages:
        try:
            alerts.append({**am.decode(m["data"]), "_published_at": iso_ms(m.get("publish_time"))})
        except ValueError as exc:
            bad.append(str(exc))
    burst_start = am.to_ms(burst["first_event_time"])
    mine = sorted((a for a in alerts if a["brand"] == brand and a["source"] == source and a["origin"] == "dataflow"
                   and am.to_ms(a["window_end"]) > burst_start), key=lambda a: a["window_end"])
    since = am.format_ts(burst_start)
    rows = key_windows(ctx, brand, source, since)
    windows = {m: metric_windows(rows, m) for m in am.METRICS}
    part.add("F4", f"trend_windows: ventanas con is_alert de {brand}/{source}", OK if windows["volume"] else FALLA,
             f"volumen: {len(windows['volume'])} ventanas (z máx {max((float(r['zscore_volume']) for r in windows['volume']), default=0):.1f}) · "
             f"sentimiento: {len(windows['sentiment'])} · {len(rows)} ventanas desde la ola",
             alert_windows={k: [r["window_end"] for r in v] for k, v in windows.items()})
    part.add("F5", "mensajes de Dataflow en ta-alerts", OK if mine and not bad else FALLA,
             (f"{len(mine)} de la ola ({', '.join(a['alert_type'] for a in mine)}) y {len(alerts) - len(mine)} de otras claves; "
              f"{len(bad)} inválidos") if mine else f"ninguno de {brand}/{source} en {len(alerts)} mensajes; inválidos: {bad[:3]}",
             types=[a["alert_type"] for a in mine], others=len(alerts) - len(mine), invalid=len(bad))
    if not mine:
        return
    first = mine[0]
    body = {k: first[k] for k in am.FIELDS}
    problems = am.alert_problems(body)
    part.add("F6", "contenido: marca, fuente, ventana y magnitud", OK if not problems else FALLA,
             am.alert_text(body) if not problems else "\n".join(problems), alert=body)
    logs = wait_for(ctx, lambda: notifier_logs(ctx, f'jsonPayload.alert_id="{first["alert_id"]}"', "1h"), 120, 10)
    rec = logs[0] if logs else {}
    det_log = check.seconds_between(first["window_end"], rec.get("received_at"))
    inc = check.incident_for(incidents, first["alert_id"])
    det_inc = check.seconds_between(first["window_end"], (inc or {}).get("published_at"))
    good = det_log is not None and det_log < DETECTION_KPI_S and det_inc is not None and det_inc < DETECTION_KPI_S
    part.add("F7", "detección < 5 min (fin de la ventana → notificación)",
             OK if good else (AVISO if det_log is not None and det_log < DETECTION_KPI_S and inc is None else FALLA),
             f"fin de la ventana {first['window_end'][11:19]} → Dataflow {check.seconds_between(first['window_end'], first['detected_at'])} s → "
             f"ta-alerts {check.seconds_between(first['window_end'], first['_published_at'])} s → notificador {det_log} s → "
             f"notificación {det_inc if det_inc is not None else 'no vista (¿límite de 1 cada 5 min por otra alerta?)'} s",
             detected_s=check.seconds_between(first["window_end"], first["detected_at"]),
             published_s=check.seconds_between(first["window_end"], first["_published_at"]), notifier_s=det_log,
             notification_s=det_inc, incident=(inc or {}).get("incident_id"))
    per_type = {t: sum(1 for a in mine if a["alert_type"] == t) for t in {a["alert_type"] for a in mine}}
    vol_windows = len(windows["volume"])
    violations = check.cooldown_violations(alerts)
    good = all(n == 1 for n in per_type.values()) and not violations
    part.add("F8", "cooldown de 15 min por marca × fuente × tipo", (OK if vol_windows >= 2 else AVISO) if good else FALLA,
             f"{vol_windows} ventanas con alerta de volumen → {per_type.get('volume_up', 0)} mensaje(s) volume_up; "
             f"mensajes por tipo {per_type}; sin pares a menos de 15 min en los {len(alerts)} mensajes vistos"
             + ("" if vol_windows >= 2 else " (solo una ventana alertó: la supresión no se vio con esta ola)")
             if good else "\n".join(violations) or f"mensajes por tipo {per_type}", per_type=per_type, violations=violations)
    others = sorted({f"{a['brand']}/{a['source']}/{a['alert_type']}" for a in alerts if a not in mine})
    part.add("F9", "otras alertas durante la observación (tráfico normal del simulador)", OK,
             f"{len(alerts) - len(mine)} mensajes: {', '.join(others) or 'ninguno'} · {len(incidents)} notificaciones de la política",
             others=others, notifications=len(incidents))


# --- Parte M · correo ---------------------------------------------------------------------------------------


def part_email(ctx: Ctx) -> int:
    part = Part(ctx, "M", "Correo recibido (lo confirma el usuario)")
    note = (getattr(ctx.options, "note", None) or "").strip()
    if re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", note):
        part.add("M1", "nota", FALLA, "la nota trae una dirección de correo: no se guarda (repita sin ella)")
        return part.finish()
    part.add("M1", "correo de la política en el buzón", OK if note else AVISO,
             note or "sin --note: describa qué llegó (asunto, hora), sin la dirección", note=note or None)
    return part.finish()


# --- Parte O · apagado ----------------------------------------------------------------------------------------


def part_off(ctx: Ctx) -> int:
    part = Part(ctx, "O", "Apagado: Dataflow drenado y simulador apagado (el notificador queda, escala a cero)")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    started = ctx.step_data("P", "P2").get("started_at")
    if (jobs or sim) and not ctx.ask(f"¿Drenar {', '.join(j.get('name', '?') for j in jobs) or 'Dataflow'} y apagar el simulador?"):
        part.add("O1", "apagado", AVISO, "no confirmado: sigue cobrando")
        return part.finish()
    try:
        sv.cloud_off(run, s, part.note)
    except sv.OpsError as exc:
        part.add("O1", "drenado y simulador (ops.services.cloud_off)", FALLA, str(exc)[-800:])
        return part.finish()
    left = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    hours = None
    if started:
        try:
            hours = round((ctx.now() - datetime.fromisoformat(started)).total_seconds() / 3600, 2)
        except ValueError:
            hours = None
    part.add("O1", "Dataflow y simulador apagados", FALLA if left or sim else OK,
             (f"sigue: {[j.get('name') for j in left]} {'simulador' if sim else ''} (repita con --force)" if left or sim else
              "ningún job activo y simulador apagado")
             + (f" · encendidos ≈ {hours} h ≈ {round(hours * (DATAFLOW_USD_PER_HOUR + SIMULATOR_USD_PER_HOUR), 2)} USD" if hours else ""),
             hours=hours)
    subs = sv.verify_subscriptions(run, s)
    part.add("O2", "suscripciones temporales ta-verify-*", AVISO if subs else OK, ", ".join(subs) if subs else "ninguna")
    return part.finish()


# --- status ---------------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    part.add("S1", "Dataflow y simulador", AVISO if jobs or sim else OK,
             f"ENCENDIDOS: {[j.get('name') for j in jobs]} {'simulador' if sim else ''} (≈ "
             f"{DATAFLOW_USD_PER_HOUR + SIMULATOR_USD_PER_HOUR:.2f} USD/h; validate_6_2.py off)" if jobs or sim else "apagados")
    outputs = tf_outputs(ctx)
    part.add("S2", "notificador y política", OK if outputs.get("alerts_notifier_url") and outputs.get("alert_policy_id") else AVISO,
             f"{outputs.get('alerts_notifier_url') or 'sin servicio'} · {outputs.get('alert_policy_name') or 'sin política'} · "
             f"{outputs.get('alert_email_channels')} canal(es) de correo (escala a cero: sin costo en reposo)")
    f7 = ctx.step_data("F", "F7")
    part.add("S3", "última alerta real medida", OK if f7 else AVISO,
             f"notificador {f7.get('notifier_s')} s · notificación {f7.get('notification_s')} s tras el fin de la ventana" if f7 else
             "todavía no se corrió la parte flow")
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "image": part_image,
    "terraform": part_terraform,
    "notify": part_notify,
    "prepare": part_prepare,
    "flow": part_flow,
    "email": part_email,
    "off": part_off,
    "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_6_2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--brand", default=None, help=f"E/F: marca (por defecto {check.DEFAULT_BRAND})")
    parser.add_argument("--source", default=None, choices=am.SOURCES, help=f"E/F: red (por defecto {check.DEFAULT_SOURCE})")
    parser.add_argument("--count", type=int, default=None, help=f"F: comentarios de la ola (por defecto {check.DEFAULT_COUNT})")
    parser.add_argument("--max-wait-minutes", type=float, default=90, help="F: espera máxima del baseline")
    parser.add_argument("--watch-minutes", type=float, default=12, help="F: cuánto se observa después de la ola")
    parser.add_argument("--note", default=None, help="M: qué correo llegó (sin la dirección)")
    parser.add_argument("--force", action="store_true", help="O: cancelar Dataflow si sigue drenando a los 15 min")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 6.2 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except sv.OpsError as exc:
        printer(f"error: {exc}")
        return 1
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si Dataflow o el simulador quedaron encendidos: python project/scripts/validate_6_2.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
