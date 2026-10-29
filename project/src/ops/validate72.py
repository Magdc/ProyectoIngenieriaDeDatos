"""Validación automática del Lote 7.2 (HU-042, observabilidad): ``docs/validation/validation-7-2.md``.

    python project/scripts/validate_7_2.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes, en el orden de uso: ``preflight`` (A), ``tests`` (B), ``local`` (L: tablero, alertas y logs JSON en el código,
sin nube), ``terraform`` (C: plan; ``--apply`` crea el tablero, las alertas de operación y lo de AWS), ``cloud`` (K: lo
creado en la nube y los logs JSON de los servicios), ``prepare`` (P: la del 6.2, enciende el simulador y Dataflow),
``metrics`` (M: con Dataflow encendido, cada gráfico del tablero tiene datos: tasa, atraso, latencia p95, marca de agua,
DLQ), ``fire`` (F: dispara de verdad la alerta de DLQ creciente con mensajes malformados, la de latencia con el umbral a
1 s y la alarma de Glue con una corrida que falla a propósito), ``off`` (O: la del 6.2) y ``status``. Evidencia en
``project/evidencia/validacion_7_2.json``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ops import services as sv
from ops import validate62 as v62
from ops import validate73 as v73
from ops.validate32 import AVISO, FALLA, OK, OMITIDO, WSL_HINT, Part, Runner, on_windows, pytest_counts

REPO = v62.REPO
PROJECT = v62.PROJECT
EVIDENCE = PROJECT / "evidencia" / "validacion_7_2.json"
GCP_ENV = v62.GCP_ENV
AWS_ENV = v73.AWS_ENV
DASHBOARD = "TrendAnalyzer · operación"
OPS_POLICIES = ("ta-op-dataflow-fallido", "ta-op-latencia", "ta-op-dlq-creciente", "ta-op-exportador-fallido")
AWS_DASHBOARD = "ta-lakehouse"
GLUE_ALARM = "ta-glue-job-fallido"
GLUE_JOB = "ta-job-trusted-to-curated"
#: Gráficos del tablero que deben tener datos con Dataflow encendido: clave → (métrica, filtro, alineación).
SERIES = {
    "tasa ta-raw-* (msg/min)": ("pubsub.googleapis.com/topic/send_message_operation_count",
                                'resource.type = "pubsub_topic" AND resource.label.topic_id = starts_with("ta-raw-")', "rate"),
    "atraso ta-raw-*-df (mensajes)": ("pubsub.googleapis.com/subscription/num_undelivered_messages",
                                      'resource.type = "pubsub_subscription" AND resource.label.subscription_id = ends_with("-df")', "max"),
    "latencia p95 de lectura (ms)": ("pubsub.googleapis.com/subscription/ack_latencies",
                                     'resource.type = "pubsub_subscription" AND resource.label.subscription_id = ends_with("-df")', "p95"),
    "marca de agua de Dataflow (s)": ("dataflow.googleapis.com/job/data_watermark_age",
                                      'resource.type = "dataflow_job" AND resource.label.job_name = starts_with("ta-stream-")', "max"),
    "retraso del sistema (s)": ("dataflow.googleapis.com/job/system_lag",
                                'resource.type = "dataflow_job" AND resource.label.job_name = starts_with("ta-stream-")', "max"),
}
#: Componentes que escriben logs JSON a stdout (HU-042): archivo → lo que lo muestra.
JSON_LOGGERS = {
    "src/simulator/publisher.py": "json.dumps(rec",
    "src/alerts/notifier.py": "json.dumps(",
    "src/exporter/jsonlog.py": "json.dumps(",
    "src/streaming/enriched.py": "json.dumps(",
    "src/lakehouse/glue_jobs/raw_to_trusted.py": "json.dumps(",
    "src/lakehouse/glue_jobs/trusted_to_curated.py": "json.dumps(",
}
LOG_SERVICES = {"ta-stream-simulator": "cloud_run_revision", "ta-alert-notifier": "cloud_run_revision", "ta-gcs-to-s3": "cloud_run_job"}

LOT_FILES = [
    "project/infra/gcp/modules/observability/main.tf",
    "project/infra/gcp/environments/dev/observability.tf",
    "project/infra/aws/modules/lakehouse_observability/main.tf",
    "project/infra/aws/environments/dev/observability.tf",
    "project/src/ops/validate72.py",
    "project/src/ops/tests/test_ops_validate72.py",
    "project/scripts/validate_7_2.py",
]
B1_EXPECTED = 6


@dataclass
class Ctx(v73.Ctx):
    evidence_path: Path = EVIDENCE
    monitoring: Callable[[str, str, str, int], list[float]] | None = None
    publish: Callable[[str, int], int] | None = None

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "7.2", "hu": ["HU-042"], "parts": {}}


# --- utilidades puras ----------------------------------------------------------------------------


def module_text(rel: str) -> str:
    return (PROJECT / rel).read_text(encoding="utf-8")


def gcp_module_problems(tf: str) -> list[str]:
    """Lo que HU-042 pide a Cloud Monitoring, en el módulo: métricas del tablero y las cuatro alertas."""
    out = []
    for metric, why in (("topic/send_message_operation_count", "tasa de mensajes"), ("subscription/ack_latencies", "latencia p95"),
                        ("REDUCE_PERCENTILE_95", "agregación p95"), ("job/data_watermark_age", "marca de agua"),
                        ('topic_id = \\"ta-dlq\\"', "tamaño de la DLQ")):
        if metric not in tf:
            out.append(f"el tablero no tiene {why} ({metric})")
    out += [f"falta la alerta {p}" for p in OPS_POLICIES if f'"{p}"' not in tf]
    if tf.count("notification_channels = var.notification_channels") < len(OPS_POLICIES):
        out.append("alguna alerta sin canales de notificación")
    return out


def aws_module_problems(tf: str) -> list[str]:
    out = []
    for marker, why in (("Glue Job State Change", "regla de estados de Glue"), ("aws_cloudwatch_dashboard", "tablero de CloudWatch"),
                        ("aws_cloudwatch_metric_alarm", "alarma"), ("aws_sns_topic_subscription", "aviso por correo"),
                        ("detail.state", "estado de cada job en el tablero")):
        if marker not in tf:
            out.append(f"falta {why}")
    return out


def json_logger_problems(root: Path = PROJECT) -> list[str]:
    """Componentes cuyo log no es una línea JSON. En los workers de Dataflow el JSON va por ``logging`` (el
    worker lo manda a Cloud Logging); en Cloud Run y Glue, a stdout."""
    out = []
    for rel, marker in JSON_LOGGERS.items():
        text = (root / rel).read_text(encoding="utf-8")
        if marker not in text or not re.search(r"sys\.stdout|print\(|LOG\.info\(json\.dumps", text):
            out.append(rel)
    return out


def log_kinds(entries: list[dict[str, Any]]) -> dict[str, int]:
    out = {"json": 0, "texto": 0}
    for e in entries or []:
        out["json" if isinstance(e.get("jsonPayload"), dict) else "texto"] += 1
    return out


def aggregation(kind: str) -> dict[str, Any]:
    if kind == "rate":
        return {"alignment_period": 60, "per_series_aligner": "ALIGN_RATE", "cross_series_reducer": "REDUCE_SUM"}
    if kind == "p95":
        return {"alignment_period": 300, "per_series_aligner": "ALIGN_DELTA", "cross_series_reducer": "REDUCE_PERCENTILE_95"}
    return {"alignment_period": 60, "per_series_aligner": "ALIGN_MAX", "cross_series_reducer": "REDUCE_MAX"}


def query_series(project: str, metric: str, flt: str, kind: str, minutes: int) -> list[float]:  # pragma: no cover - red
    """Puntos de una serie de Cloud Monitoring (google-cloud-monitoring, ADC), ya agregados como el tablero."""
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    now = time.time()
    interval = monitoring_v3.TimeInterval({"end_time": {"seconds": int(now)}, "start_time": {"seconds": int(now - minutes * 60)}})
    agg = aggregation(kind)
    req = {
        "name": f"projects/{project}",
        "filter": f'metric.type = "{metric}" AND {flt}',
        "interval": interval,
        "aggregation": monitoring_v3.Aggregation({
            "alignment_period": {"seconds": agg["alignment_period"]},
            "per_series_aligner": getattr(monitoring_v3.Aggregation.Aligner, agg["per_series_aligner"]),
            "cross_series_reducer": getattr(monitoring_v3.Aggregation.Reducer, agg["cross_series_reducer"]),
        }),
        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    }
    out = []
    for ts in client.list_time_series(request=req):
        for p in ts.points:
            v = p.value
            out.append(float(v.double_value or v.int64_value or 0))
    return out


def summarize(points: list[float]) -> dict[str, float | int]:
    if not points:
        return {"n": 0}
    return {"n": len(points), "max": round(max(points), 2), "last": round(points[0], 2)}


# --- Parte A · preflight ---------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    absent = [f for f in LOT_FILES if not (REPO / f).exists()]
    part.add("A1", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK, ", ".join(absent) or "presentes")
    tools = {t: ctx.runner.which(t) for t in ("gcloud", "aws", "terraform")}
    need = [t for t in tools if not tools[t]]
    try:
        __import__("google.cloud.monitoring_v3")
        lib = True
    except ImportError:
        lib = False
    part.add("A2", "herramientas y google-cloud-monitoring", FALLA if need or not lib else OK,
             ", ".join(t for t in tools if tools[t]) + ("" if lib else "; falta google-cloud-monitoring"))
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs, sim = sv.active_jobs(run, s), sv.simulator_service(run, s)
    part.add("A3", "Dataflow y simulador", OK, f"Dataflow {'ENCENDIDO' if jobs else 'apagado'} · simulador {'ENCENDIDO' if sim else 'apagado'}")
    return part.finish()


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    res = ctx.pytest("src/ops/tests/test_ops_validate72.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    good = res.ok and c["passed"] == B1_EXPECTED and not c["failed"]
    part.add("B1", "test_ops_validate72.py", OK if good else FALLA, f"{c['passed']} passed, {c['failed']} failed (esperado {B1_EXPECTED})"
             + ("" if good else "\n" + res.tail(8)), **c)
    return part.finish()


# --- Parte L · local ----------------------------------------------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Tablero, alertas y logs JSON en el código")
    problems = gcp_module_problems(module_text("infra/gcp/modules/observability/main.tf"))
    part.add("L1", "Cloud Monitoring: tasa de mensajes, latencia p95, DLQ y las 4 alertas de operación", FALLA if problems else OK,
             "\n".join(problems) or "8 gráficos y 4 alertas con los canales del 6.2")
    problems = aws_module_problems(module_text("infra/aws/modules/lakehouse_observability/main.tf"))
    part.add("L2", "CloudWatch: estado de los jobs de Glue, alarma y correo", FALLA if problems else OK,
             "\n".join(problems) or "regla de estados, tablero ta-lakehouse, alarma ta-glue-job-fallido y SNS")
    bad = json_logger_problems()
    part.add("L3", "logs JSON a stdout en cada componente", FALLA if bad else OK, f"sin JSON: {bad}" if bad else
             ", ".join(Path(p).parent.name for p in JSON_LOGGERS))
    res = ctx.run(["bash", "project/infra/validate.sh"], cwd=REPO, timeout=900)
    part.add("L4", "terraform fmt y validate", OK if res.ok and "Warning" not in res.out else FALLA, "OK" if res.ok else res.tail(8))
    return part.finish()


# --- Parte C · Terraform --------------------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    return v73.part_terraform(ctx)


# --- Parte K · la nube ------------------------------------------------------------------------------------


def part_cloud(ctx: Ctx) -> int:
    part = Part(ctx, "K", "Tablero, alertas y logs en la nube")
    dashboards = ctx.gjson("monitoring", "dashboards", "list", f"--filter=displayName=\"{DASHBOARD}\"") or []
    tiles = len(((dashboards[0] if dashboards else {}).get("mosaicLayout") or {}).get("tiles") or [])
    part.add("K1", "tablero de Cloud Monitoring", OK if dashboards and tiles == 8 else FALLA,
             f"«{DASHBOARD}» con {tiles} gráficos" if dashboards else "no existe")
    policies = {p.get("displayName"): p for p in (ctx.gjson("monitoring", "policies", "list", "--filter", 'displayName:"ta-op-"') or [])}
    channels = int(v62.tf_outputs(ctx).get("alert_email_channels") or 0) + 1
    problems = {n: v73.policy_problems(policies[n], channels) if n in policies else ["no existe"] for n in OPS_POLICIES}
    bad = {k: v for k, v in problems.items() if v}
    part.add("K2", "las 4 alertas de operación", FALLA if bad else OK, json.dumps(bad, ensure_ascii=False) if bad else ", ".join(OPS_POLICIES))
    dash = ctx.awsj("cloudwatch", "get-dashboard", "--dashboard-name", AWS_DASHBOARD)
    widgets = len((json.loads((dash or {}).get("DashboardBody") or "{}")).get("widgets") or [])
    part.add("K3", "tablero de CloudWatch ta-lakehouse", OK if widgets == 4 else FALLA, f"{widgets} widgets" if dash else "no existe")
    alarms = (ctx.awsj("cloudwatch", "describe-alarms", "--alarm-names", GLUE_ALARM) or {}).get("MetricAlarms") or []
    alarm = alarms[0] if alarms else {}
    topic = (alarm.get("AlarmActions") or [None])[0]
    subs = (ctx.awsj("sns", "list-subscriptions-by-topic", "--topic-arn", topic) or {}).get("Subscriptions") or [] if topic else []
    pending = sum(1 for s in subs if s.get("SubscriptionArn") == "PendingConfirmation")
    part.add("K4", "alarma de Glue con aviso por SNS", OK if alarm and topic else FALLA,
             f"{GLUE_ALARM} en {alarm.get('StateValue')} → {str(topic).split(':')[-1]} · {len(subs)} suscripción(es) de correo"
             + (f", {pending} sin confirmar (el usuario confirma en el correo de AWS)" if pending else "") if alarm else "no existe",
             subscriptions=len(subs), pending=pending)
    rule = ctx.awsj("events", "describe-rule", "--name", "ta-glue-job-estados") or {}
    part.add("K5", "regla de estados de Glue", OK if rule.get("State") == "ENABLED" else FALLA, rule.get("State") or "no existe")
    kinds = {}
    for name, rtype in LOG_SERVICES.items():
        label = "service_name" if rtype == "cloud_run_revision" else "job_name"
        # Solo lo que escribe el servicio (stdout): las líneas de la plataforma (arranque, peticiones) son texto.
        flt = f'resource.type="{rtype}" AND resource.labels.{label}="{name}" AND logName:"run.googleapis.com%2Fstdout"'
        entries = ctx.gjson("logging", "read", flt, "--freshness=7d", "--limit=50", timeout=120) or []
        kinds[name] = log_kinds(entries)
    texts = {k: v for k, v in kinds.items() if v["texto"] or not v["json"]}
    part.add("K6", "stdout de los servicios en JSON (últimos 7 días)", OK if not texts else AVISO,
             " · ".join(f"{k}: {v['json']} JSON, {v['texto']} texto" for k, v in kinds.items()), kinds=kinds)
    return part.finish()


# --- Parte M · métricas con datos ----------------------------------------------------------------------------


def part_metrics(ctx: Ctx) -> int:
    part = Part(ctx, "M", "Los gráficos del tablero tienen datos (Dataflow encendido)")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    if not sv.active_jobs(run, s):
        part.add("M0", "Dataflow encendido", OMITIDO, "corra antes: validate_7_2.py prepare")
        return part.finish()
    query = ctx.monitoring or query_series
    minutes = int(getattr(ctx.options, "minutes", 30) or 30)
    for i, (title, (metric, flt, kind)) in enumerate(SERIES.items(), start=1):
        try:
            points = query(ctx.project, metric, flt, kind, minutes)
        except Exception as exc:  # noqa: BLE001
            part.add(f"M{i}", title, FALLA, str(exc)[:300])
            continue
        sm = summarize(points)
        part.add(f"M{i}", title, OK if sm["n"] else FALLA, f"{sm['n']} puntos en {minutes} min · máx {sm.get('max')} · último {sm.get('last')}"
                 if sm["n"] else f"sin datos en {minutes} min", **sm)
    return part.finish()


# --- Parte F · disparar las alertas --------------------------------------------------------------------------


def publish_malformed(project: str, count: int) -> int:  # pragma: no cover - red
    from google.cloud import pubsub_v1

    client = pubsub_v1.PublisherClient()
    topic = client.topic_path(project, "ta-raw-reddit")
    futures = [client.publish(topic, b'{"event_id": "ta-verify-7.2", "roto', origin="stream", source="reddit") for _ in range(count)]
    return sum(1 for f in futures if f.result(timeout=60))


def wait_incident(ctx: Ctx, sub: str, name: str, minutes: float) -> str | None:
    end = time.monotonic() + minutes * 60
    while time.monotonic() < end:
        for inc in v62.incidents_from(v62.pull(ctx, sub)):
            if name in json.dumps(inc):
                return inc.get("published_at")
        ctx.sleep(20)
    return None


def part_fire(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Alertas de verdad: DLQ creciente, latencia y fallo de Glue")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    if not sv.active_jobs(run, s):
        part.add("F0", "Dataflow encendido", OMITIDO, "corra antes: validate_7_2.py prepare")
        return part.finish()
    if not ctx.ask("¿Publicar 40 mensajes malformados en ta-raw-reddit, bajar el umbral de latencia a 1 s un rato y correr "
                   f"{GLUE_JOB} con un bucket inexistente para que falle (≈ 0,001 USD)?"):
        part.add("F0", "disparos", AVISO, "no confirmado")
        return part.finish()
    sub = f"ta-verify-incidents-72-{uuid.uuid4().hex[:6]}"
    v62.create_sub(ctx, sub, v62.INCIDENTS_TOPIC)
    try:
        sent_at = ctx.now()
        n = (ctx.publish or publish_malformed)(ctx.project, 40)
        part.add("F1", "40 mensajes malformados a ta-raw-reddit", OK if n == 40 else FALLA, f"{n} publicados a las {sent_at:%H:%M:%S} UTC")
        at = wait_incident(ctx, sub, "ta-op-dlq-creciente", float(getattr(ctx.options, "watch_minutes", 15) or 15))
        part.add("F2", "notificación de ta-op-dlq-creciente", OK if at else FALLA,
                 f"{v62.check.seconds_between(sent_at.strftime('%Y-%m-%dT%H:%M:%S.000Z'), at)} s después de publicar" if at else "no llegó")
        res, _ = v73.plan(ctx, GCP_ENV, "lat_low", ("-target=module.observability", "-var=ops_latency_threshold_seconds=1"))
        res = ctx.tf(GCP_ENV, "apply", "-input=false", "-lock-timeout=60s", "/tmp/validate73/tfplan_lat_low", timeout=900) if res.ok else res
        lowered = ctx.now()
        at = wait_incident(ctx, sub, "ta-op-latencia", float(getattr(ctx.options, "watch_minutes", 15) or 15)) if res.ok else None
        part.add("F3", "notificación de ta-op-latencia (umbral a 1 s)", OK if at else FALLA,
                 f"{v62.check.seconds_between(lowered.strftime('%Y-%m-%dT%H:%M:%S.000Z'), at)} s después de bajar el umbral" if at else
                 ("no llegó" if res.ok else res.tail(6)))
    finally:
        res, _ = v73.plan(ctx, GCP_ENV, "lat_back", ("-target=module.observability",))
        res = ctx.tf(GCP_ENV, "apply", "-input=false", "-lock-timeout=60s", "/tmp/validate73/tfplan_lat_back", timeout=900) if res.ok else res
        part.add("F4", "umbral de latencia de vuelta a 300 s", OK if res.ok else FALLA, "aplicado" if res.ok else res.tail(6))
        v62.delete_sub(ctx, sub)
    run_id = (ctx.awsj("glue", "start-job-run", "--job-name", GLUE_JOB, "--arguments",
                       json.dumps({"--lake_bucket": "ta-verify-7-2-no-existe"})) or {}).get("JobRunId")
    state, waited = None, 0
    while run_id and waited < 900:
        ctx.sleep(30)
        waited += 30
        state = ((ctx.awsj("glue", "get-job-run", "--job-name", GLUE_JOB, "--run-id", run_id) or {}).get("JobRun") or {}).get("JobRunState")
        if state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED", "SUCCEEDED"):
            break
    part.add("F5", f"corrida de {GLUE_JOB} que falla a propósito", OK if state in ("FAILED", "ERROR") else FALLA, f"{run_id} → {state}")
    alarm_state, waited = None, 0
    while state in ("FAILED", "ERROR") and waited < 900:
        ctx.sleep(30)
        waited += 30
        alarm = ((ctx.awsj("cloudwatch", "describe-alarms", "--alarm-names", GLUE_ALARM) or {}).get("MetricAlarms") or [{}])[0]
        alarm_state = alarm.get("StateValue")
        if alarm_state == "ALARM":
            break
    part.add("F6", f"alarma {GLUE_ALARM} en ALARM (y aviso a SNS)", OK if alarm_state == "ALARM" else FALLA,
             f"{alarm_state} a los {waited} s del fallo" if alarm_state else "sin cambio de estado")
    return part.finish()


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight, "tests": part_tests, "local": part_local, "terraform": part_terraform,
    "cloud": part_cloud, "prepare": v62.part_prepare, "metrics": part_metrics, "fire": part_fire, "off": v62.part_off,
    "status": v73.part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_7_2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--apply", action="store_true", help="C: aplicar los planes")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--minutes", type=int, default=30, help="M: ventana de las series")
    parser.add_argument("--watch-minutes", type=float, default=15, help="F: espera de cada notificación")
    parser.add_argument("--force", action="store_true", help="O: cancelar Dataflow si sigue drenando a los 15 min")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None,
         monitoring: Callable[[str, str, str, int], list[float]] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm, sleep=sleep or time.sleep,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE, monitoring=monitoring)
    printer(f"Lote 7.2 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
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
