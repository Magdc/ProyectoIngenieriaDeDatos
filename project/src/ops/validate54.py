"""Validación automática del Lote 5.4 (HU-048, prueba de humo de extremo a extremo): ``docs/validation/validation-5-4.md``.

    python project/scripts/validate_5_4.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: los eventos de la prueba frente al contrato, sin nube),
``prepare`` (P: descarta con *seek* el atraso viejo de ``ta-raw-*-df`` y enciende Dataflow con NLP), ``smoke`` (E:
``scripts/smoke_e2e.py run`` y su informe), ``off`` (O: drena Dataflow y deja el exportador en pausa y el horario del
lakehouse encendido) y ``status``. Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su
resumen en ``project/evidencia/validacion_5_4.json``.

Lo que cambia la nube (``prepare``, ``smoke``, ``off``) pide confirmación salvo con ``--yes``. Reutiliza
``ops/services.py`` (Dataflow: ``nlp_env``, ``active_jobs``, ``wait_running``, ``drain_jobs``, ``simulator_service``),
de ``ops/validate41.py`` ``Ctx``, ``get_schedule`` y ``services_status``, de ``ops/validate53.py`` ``leak_problems`` y
de ``ops/validate32.py`` ``Part``, ``Runner`` y ``pytest_counts``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ops import services as sv
from ops import validate41 as v41
from ops import validate53 as v53
from ops.validate32 import AVISO, FALLA, OK, OMITIDO, WSL_HINT, Part, Runner, on_windows, parse_json, pytest_counts

REPO = v41.REPO
PROJECT = v41.PROJECT
SRC = v41.SRC
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_5_4.json"
REPORT_JSON = EVIDENCE_DIR / "smoke_e2e_5_4.json"
REPORT_MD = EVIDENCE_DIR / "smoke_e2e_5_4.md"
SCHEDULE = v41.SCHEDULE
EXPORTER_SCHEDULER = "ta-gcs-to-s3-cada-15-min"
GCP_REGION = "us-central1"
SUBSCRIPTIONS = tuple(f"ta-raw-{s}-df" for s in ("reddit", "facebook", "instagram"))
ADC = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"

LOT_FILES = [
    "src/smoke/__init__.py",
    "src/smoke/e2e.py",
    "src/smoke/clouds.py",
    "src/smoke/cli.py",
    "src/smoke/tests/test_smoke_e2e.py",
    "scripts/smoke_e2e.py",
    "src/ops/validate54.py",
    "src/ops/tests/test_ops_validate54.py",
    "scripts/validate_5_4.py",
]
PREVIOUS_FILES = ["src/ops/services.py", "src/ops/validate41.py", "src/ops/validate53.py", "src/streaming/inject.py",
                  "src/simulator/factory.py", "src/lakehouse/glue_jobs/raw_to_trusted.py", "scripts/dataflow_job.sh"]

B1_EXPECTED = 11
B2_EXPECTED = 14
B3_EXPECTED = 18
B4_EXPECTED = 20
#: Objetivos de §5.12 (segundos); L2 exige que el código diga lo mismo.
OBJECTIVES = {"bigquery": 60, "gcs_enriched": 900, "s3_raw": 300, "trusted": 900, "curated": 600}
#: Costo estimado de Dataflow encendido (ACTIVE_SERVICES.md §0).
DATAFLOW_USD_PER_HOUR = 0.25


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "5.4", "hu": ["HU-048"], "parts": {}}


def sv_runner(ctx: Ctx) -> Callable[..., sv.Result]:
    """Adaptador de ``ctx.run`` a la firma de ``ops.services`` (como ``v41.services_status``)."""

    def call(args: list[str], *, capture: bool = True, env: dict | None = None, check: bool = False) -> sv.Result:
        res = ctx.run(list(args), cwd=PROJECT, env=env, timeout=1800, stream=not capture)
        out = sv.Result(res.code, res.out, res.err)
        if check and not out.ok:
            raise sv.OpsError(f"falló: {' '.join(args)}")
        return out

    return call


def settings(ctx: Ctx) -> sv.Settings:
    return sv.Settings(project=ctx.project, yes=True, force=bool(getattr(ctx.options, "force", False)))


def say(part: Part) -> Callable[[str], None]:
    return part.note


def exporter_state(ctx: Ctx) -> str | None:
    res = ctx.run(["gcloud", "scheduler", "jobs", "describe", EXPORTER_SCHEDULER, "--location", GCP_REGION,
                   "--project", ctx.project, "--format=value(state)"], timeout=120)
    return (res.out.strip() or None) if res.ok else None


def smoke_events(text: str) -> list[dict[str, Any]]:
    return [e for e in (parse_json(ln) for ln in text.splitlines() if ln.strip().startswith("{")) if isinstance(e, dict)]


def stage_line(s: dict[str, Any]) -> str:
    return (f"{s.get('latency_s')} s (objetivo {s.get('objective_s')} s, desde {s.get('from')}); "
            f"{s.get('since_publish_s')} s desde la publicación · {s.get('detail')}")


# --- Parte A · preflight --------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    missing = []
    for mod in ("google.cloud.pubsub_v1", "google.cloud.bigquery", "google.cloud.storage", "boto3", "pyarrow", "jsonschema",
                "apache_beam"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    part.add("A1", "Python y dependencias", FALLA if missing or (py.major, py.minor) != (3, 11) else OK,
             f"Python {py.major}.{py.minor}.{py.micro}" + (f"; faltan: {', '.join(missing)}" if missing else
                                                           "; Pub/Sub, BigQuery, GCS, boto3, pyarrow, jsonschema y Beam"))
    absent = [f for f in LOT_FILES + PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote y {len(PREVIOUS_FILES)} que reutiliza", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else "presentes")
    tools = {t: ctx.runner.which(t) for t in ("gcloud", "aws", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A3", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))
    part.add("A4", "proyecto de GCP y ADC", OK if ADC.exists() and ctx.project else FALLA,
             f"{ctx.project} · ADC {'presente' if ADC.exists() else 'FALTA: gcloud auth application-default login'}")
    run, s = sv_runner(ctx), settings(ctx)
    try:
        jobs = sv.active_jobs(run, s)
        sim = sv.simulator_service(run, s)
        image = sv.latest_nlp_image(run, s)
    except sv.OpsError as exc:
        part.add("A5", "GCP: Dataflow, simulador e imagen de NLP", FALLA, str(exc))
        return part.finish()
    part.add("A5", "GCP: Dataflow, simulador e imagen de NLP", OK if image and not sim else AVISO,
             f"Dataflow: {', '.join(j.get('name', '?') for j in jobs) or 'apagado'} · simulador: "
             f"{'ENCENDIDO (este lote no lo necesita: off.py)' if sim else 'apagado'} · imagen NLP: {image or 'FALTA'}",
             dataflow=[j.get("name") for j in jobs], simulator=bool(sim), nlp_image=image)
    state = exporter_state(ctx)
    part.add("A6", f"exportador {EXPORTER_SCHEDULER}", OK if state == "PAUSED" else AVISO,
             f"{state} (estado normal: PAUSED; la prueba ejecuta el job a mano, sin el Scheduler)")
    sched, data = v41.get_schedule(ctx)
    part.add("A7", f"horario {SCHEDULE}", OK if sched == "ENABLED" else AVISO,
             f"{sched or data.get('error')} (estado normal: ENABLED; la prueba arranca una corrida a mano y espera si hay otra)")
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/smoke (pipeline falso)", ["src/smoke", "-q", "-W", "ignore::FutureWarning"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate54.py", ["src/ops/tests/test_ops_validate54.py", "-q"], B2_EXPECTED),
        ("B3", "pytest contracts/tests", ["contracts/tests", "-q"], B3_EXPECTED),
        ("B4", "regresión: test_ops_validate53.py (leak_problems)", ["src/ops/tests/test_ops_validate53.py", "-q"], B4_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · local -------------------------------------------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Los eventos de la prueba frente al contrato (sin nube)")
    res = ctx.run([sys.executable, "scripts/smoke_e2e.py", "plan"], cwd=PROJECT, timeout=300)
    events = [e for e in smoke_events(res.out) if e.get("event") == "smoke_plan"]
    plan = events[-1] if events else {}
    from lakehouse.glue_jobs.raw_to_trusted import SENTINEL_PREFIX

    good = (res.ok and plan.get("contract") == "ok" and plan.get("warmup_contract") == "ok"
            and plan.get("generator") == "comment-gen@1.0" and str(plan.get("warmup_generator", "")).startswith(SENTINEL_PREFIX)
            and plan.get("messages") == 1)
    part.add("L1", "centinela y calentamiento cumplen el contrato", OK if good else FALLA,
             (f"centinela {plan.get('event_id')} ({plan.get('generator')}, {plan.get('source')}): contrato {plan.get('contract')}, "
              f"1 mensaje con atributos {plan.get('attributes')} · calentamiento {plan.get('warmup_generator')} (Trusted descarta "
              f"'{SENTINEL_PREFIX}*'): contrato {plan.get('warmup_contract')}") if plan else res.tail(8), plan=plan)
    from smoke import e2e

    diff = {k: (e2e.OBJECTIVES.get(k), v) for k, v in OBJECTIVES.items() if e2e.OBJECTIVES.get(k) != v}
    part.add("L2", "objetivos de latencia = §5.12", FALLA if diff else OK,
             f"distintos (código, §5.12): {diff}" if diff else
             "; ".join(f"{k} ≤ {v} s desde {e2e.FROM[k]}" for k, v in OBJECTIVES.items()))
    return part.finish()


# --- Parte P · preparar la nube ---------------------------------------------------------------------------


def part_prepare(ctx: Ctx) -> int:
    part = Part(ctx, "P", "Preparar: descartar el atraso de ta-raw-*-df y encender Dataflow con NLP")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    if jobs:
        part.add("P1", "seek de ta-raw-*-df", OMITIDO, f"Dataflow ya está encendido ({jobs[0].get('name')}): no se toca el atraso")
    elif ctx.ask(f"¿Descartar con seek todo lo pendiente en {', '.join(SUBSCRIPTIONS)} (irreversible) y encender Dataflow con "
                 f"NLP (≈ {DATAFLOW_USD_PER_HOUR} USD/h hasta el off)?"):
        when = ctx.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        bad = []
        for sub in SUBSCRIPTIONS:
            r = ctx.run(["gcloud", "pubsub", "subscriptions", "seek", sub, f"--time={when}", "--project", ctx.project], timeout=120)
            if not r.ok:
                bad.append(f"{sub}: {r.tail(2)}")
        part.add("P1", "seek de ta-raw-*-df a ahora", FALLA if bad else OK,
                 "\n".join(bad) if bad else f"{len(SUBSCRIPTIONS)} suscripciones movidas a {when}: lo publicado antes queda descartado",
                 seek_time=when)
        if bad:
            return part.finish()
    else:
        part.add("P1", "seek y Dataflow", AVISO, "no confirmado")
        return part.finish()
    if not jobs:
        env = sv.nlp_env(run, s, say(part))
        env.update({"GCP_PROJECT": s.project, "GCP_REGION": s.region, "ENV": s.env})
        res = run(["bash", "scripts/dataflow_job.sh", "start"], capture=False, env=env)
        if not res.ok:
            part.add("P2", "dataflow_job.sh start", FALLA, (res.err or res.out)[-600:])
            return part.finish()
        part.add("P2", "dataflow_job.sh start", OK, f"lanzado con {env.get('TA_NLP_IMAGE') or 'NLP apagado'}",
                 nlp_image=env.get("TA_NLP_IMAGE"))
        sv.wait_running(run, s, say(part))
        jobs = sv.active_jobs(run, s)
    running = [j for j in jobs if str(j.get("state") or j.get("currentState")).lower().endswith("running")]
    part.add("P3", "Dataflow en Running", OK if running else FALLA,
             f"{running[0].get('name')} ({running[0].get('id')})" if running else
             f"{[(j.get('name'), j.get('state')) for j in jobs] or 'ningún job'}: revise bash scripts/dataflow_job.sh status",
             job=(running[0].get("id") if running else None), started_at=ctx.now().isoformat())
    return part.finish()


# --- Parte E · la prueba de humo --------------------------------------------------------------------------


def part_smoke(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Prueba de humo de extremo a extremo (scripts/smoke_e2e.py run)")
    if not ctx.ask("¿Publicar el calentamiento y el centinela, ejecutar el exportador una vez y arrancar una corrida del "
                   "workflow de Glue?"):
        part.add("E0", "prueba de humo", AVISO, "no confirmado")
        return part.finish()
    cmd = [sys.executable, "scripts/smoke_e2e.py", "run", "--out", str(EVIDENCE_DIR)]
    if getattr(ctx.options, "source", None):
        cmd += ["--source", ctx.options.source]
    part.note("E: publica, sigue el centinela y escribe el informe (≈ 15–30 min; una línea JSON por etapa)…")
    res = ctx.run(cmd, cwd=PROJECT, timeout=3 * 3600, stream=True)
    try:
        report = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        report = {}
    if not report:
        part.add("E0", "informe", FALLA, res.tail(10))
        return part.finish()
    w = report.get("warmup") or {}
    part.add("E1", "calentamiento (ta-verify@2.1) hasta BigQuery", OK if w.get("ok") else (OMITIDO if not w else FALLA),
             f"{w.get('latency_s')} s (incluye la carga de los modelos de NLP; Trusted lo descarta)" if w else
             report.get("reason", "sin calentamiento"), warmup=w)
    stages = {s["stage"]: s for s in report.get("stages", [])}
    for sid, name, title in (("E2", "bigquery", "BigQuery posts_enriched"), ("E3", "gcs_enriched", "GCS enriched/"),
                             ("E4", "s3_raw", "S3 raw/origin=stream/"), ("E5", "trusted", "ta_trusted.posts (Athena)"),
                             ("E6", "curated", "ta_curated.fact_posts (Athena)")):
        st = stages.get(name)
        part.add(sid, title, (OK if st["ok"] else FALLA) if st else FALLA,
                 stage_line(st) if st else f"no llegó a medirse ({report.get('reason')})", stage=st)
    ident = report.get("identity") or {}
    good = not ident.get("problems") and ident.get("same_user_hash") and len(ident.get("checked") or []) == 5
    part.add("E7", "sin user_id ni handle después de Dataflow; mismo user_hash", OK if good else FALLA,
             f"revisadas: {', '.join(ident.get('checked') or [])}; problemas: {ident.get('problems') or 'ninguno'}; "
             f"user_hash igual en {ident.get('stages_with_user_hash')} etapas: {ident.get('same_user_hash')}")
    texts = {}
    for p in (REPORT_JSON, REPORT_MD):
        try:
            texts[p.name] = p.read_text(encoding="utf-8")
        except OSError:
            texts[p.name] = ""
    leaks = v53.leak_problems(texts)
    part.add("E8", "informe para HU-044/HU-045 sin datos personales", FALLA if leaks or not all(texts.values()) else OK,
             "\n".join(leaks) if leaks else f"{REPORT_JSON.name} y {REPORT_MD.name} · centinela {report.get('event_id')} · "
             f"total {report.get('total_s')} s · resultado {'OK' if report.get('ok') else 'FALLA'}",
             event_id=report.get("event_id"), total_s=report.get("total_s"), ok=report.get("ok"))
    return part.finish()


# --- Parte O · apagado -------------------------------------------------------------------------------------


def part_off(ctx: Ctx) -> int:
    part = Part(ctx, "O", "Apagado: Dataflow drenado, exportador en pausa, horario encendido")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    started = ctx.step_data("P", "P3").get("started_at")
    if jobs and not ctx.ask(f"¿Drenar {', '.join(j.get('name', '?') for j in jobs)}?"):
        part.add("O1", "drenar Dataflow", AVISO, "no confirmado: sigue cobrando")
    else:
        if jobs:
            sv.drain_jobs(run, s, say(part))
        left = sv.active_jobs(run, s)
        hours = None
        if started:
            try:
                hours = round((ctx.now() - datetime.fromisoformat(started)).total_seconds() / 3600, 2)
            except ValueError:
                hours = None
        part.add("O1", "Dataflow apagado", FALLA if left else OK,
                 (f"sigue activo: {[j.get('name') for j in left]} (repita con --force para cancelarlo)" if left else "ningún job activo")
                 + (f" · encendido ≈ {hours} h ≈ {round(hours * DATAFLOW_USD_PER_HOUR, 2)} USD" if hours is not None else ""),
                 hours=hours)
    state = exporter_state(ctx)
    if state == "ENABLED":
        ctx.run(["gcloud", "scheduler", "jobs", "pause", EXPORTER_SCHEDULER, "--location", GCP_REGION, "--project", ctx.project], timeout=120)
        state = exporter_state(ctx)
    part.add("O2", f"exportador {EXPORTER_SCHEDULER}", OK if state == "PAUSED" else FALLA, f"{state}")
    sim = sv.simulator_service(run, s)
    part.add("O3", "simulador", AVISO if sim else OK, "ENCENDIDO: python project/scripts/off.py" if sim else "apagado")
    sched, data = v41.get_schedule(ctx)
    part.add("O4", f"horario {SCHEDULE}", OK if sched == "ENABLED" else AVISO,
             f"{sched or data.get('error')}" + ("" if sched == "ENABLED" else " (encenderlo: python project/scripts/validate_4_4.py on --yes)"))
    subs = sv.verify_subscriptions(run, s)
    part.add("O5", "suscripciones temporales ta-verify-*", AVISO if subs else OK, ", ".join(subs) if subs else "ninguna")
    return part.finish()


# --- status ----------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    run, s = sv_runner(ctx), settings(ctx)
    jobs = sv.active_jobs(run, s)
    part.add("S1", "Dataflow", AVISO if jobs else OK,
             f"ENCENDIDO: {[j.get('name') for j in jobs]} (≈ {DATAFLOW_USD_PER_HOUR} USD/h; validate_5_4.py off)" if jobs else "apagado")
    state = exporter_state(ctx)
    sched, _ = v41.get_schedule(ctx)
    part.add("S2", "exportador y horario del lakehouse", OK if state == "PAUSED" and sched == "ENABLED" else AVISO,
             f"exportador {state} (normal: PAUSED) · horario {sched} (normal: ENABLED)")
    try:
        report = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        report = {}
    part.add("S3", "última prueba de humo", (OK if report.get("ok") else AVISO) if report else AVISO,
             f"{report.get('run_id')}: {'OK' if report.get('ok') else 'FALLA'} · centinela {report.get('event_id')} · total "
             f"{report.get('total_s')} s" if report else "todavía no hay informe")
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "prepare": part_prepare,
    "smoke": part_smoke,
    "off": part_off,
    "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_5_4.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--source", default=None, choices=("reddit", "facebook", "instagram"), help="E: red del centinela")
    parser.add_argument("--force", action="store_true", help="O: cancelar Dataflow si sigue drenando a los 15 min")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 5.4 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except sv.OpsError as exc:
        printer(f"error: {exc}")
        return 1
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si Dataflow quedó encendido: python project/scripts/validate_5_4.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
