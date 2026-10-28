"""Validación automática del Lote 6.3 (HU-040, escenario de demo): ``docs/validation/validation-6-3.md``.

    python project/scripts/validate_6_3.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes, en el orden de uso: ``preflight`` (A), ``tests`` (B), ``local`` (L: ola en tiempo virtual y API con el
servidor real, sin nube), ``image`` (D: construye ``stream-simulator`` en Cloud Build y lo pone en
``terraform.tfvars``), ``prepare`` (P: la del 6.2: *seek*, simulador con la imagen nueva y Dataflow), ``auth`` (S:
imagen desplegada, servicio privado, 403 sin token, API con token), ``flow`` (F: espera el baseline, lanza el
escenario con ``demo_scenario.py start`` y mide la marca en ``sim.scenario``, la cadencia y la alerta que dispara),
``off`` (O: la del 6.2) y ``status``. Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su
resumen en ``project/evidencia/validacion_6_3.json``.

Reutiliza de ``ops/validate62.py`` ``Ctx``, ``part_prepare``, ``part_off``, ``first_window``, ``warm_at``,
``flow_report`` y las suscripciones temporales, y de ``demo/`` el cliente y la comprobación local.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from common import alert_message as am
from demo import client
from ops import services as sv
from ops import validate62 as v62
from ops.validate32 import AVISO, FALLA, OK, WSL_HINT, Part, Runner, on_windows, pytest_counts

PROJECT = v62.PROJECT
EVIDENCE = PROJECT / "evidencia" / "validacion_6_3.json"
TFVARS = v62.TFVARS
SIM_RE = re.compile(r'^(?P<lead>\s*simulator_image\s*=\s*)"(?P<img>[^"]*)"', re.M)
IMAGE_REPO = "{region}-docker.pkg.dev/{project}/ta-images/stream-simulator"
GENERATOR = "comment-gen@1.0"
#: Lo que entra a la imagen del simulador (lista de permitidos de ``project/.dockerignore``).
IMAGE_INPUTS = ("pytest.ini", "contracts", "src/conftest.py", "src/common", "src/simulator", "scripts/generate_comments.py")
#: Antes de lanzar el escenario se mide la cadencia normal durante este tiempo.
BEFORE_S = 120

LOT_FILES = [
    "src/simulator/scenario.py",
    "src/simulator/tests/test_scenario.py",
    "src/demo/__init__.py",
    "src/demo/client.py",
    "src/demo/local.py",
    "src/demo/cli.py",
    "src/demo/tests/__init__.py",
    "src/demo/tests/test_demo.py",
    "scripts/demo_scenario.py",
    "src/ops/validate63.py",
    "src/ops/tests/test_ops_validate63.py",
    "scripts/validate_6_3.py",
]
PREVIOUS_FILES = ["src/simulator/app.py", "src/simulator/publisher.py", "src/simulator/factory.py", "src/ops/validate62.py",
                  "src/ops/services.py", "scripts/build_simulator_image.sh", "scripts/alerts_check.py"]

B1_EXPECTED = 137
B2_EXPECTED = 9
B3_EXPECTED = 10


@dataclass
class Ctx(v62.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "6.3", "hu": ["HU-040"], "parts": {}}

    def demo(self, *args: str, timeout: float = 120) -> dict[str, Any]:
        """``demo_scenario.py`` (su última línea JSON; ``{}`` si no hubo)."""
        res = self.run([sys.executable, "scripts/demo_scenario.py", *args, "--project", self.project], cwd=PROJECT,
                       timeout=timeout)
        self.cache["demo_tail"] = res.tail(6)
        evs = v62.events(res.out)
        return evs[-1] if evs else {}


# --- utilidades puras ----------------------------------------------------------------------------


def read_image(text: str) -> str | None:
    m = SIM_RE.search(text or "")
    return m.group("img") if m else None


def set_image(text: str, image: str) -> str:
    if SIM_RE.search(text):
        return SIM_RE.sub(lambda m: f'{m.group("lead")}"{image}"', text, count=1)
    sep = "" if text.endswith("\n") or not text else "\n"
    return f'{text}{sep}simulator_image = "{image}"\n'


def image_files(root: Path = PROJECT) -> list[Path]:
    """Los archivos que entran a la imagen (``project/.dockerignore``), sin cachés."""
    out = []
    for rel in IMAGE_INPUTS:
        base = root / rel
        for p in [base] if base.is_file() else sorted(base.rglob("*")):
            parts = set(p.relative_to(root).parts)
            if p.is_file() and not parts & {"__pycache__", ".pytest_cache", ".sim-state"} and p.suffix != ".pyc":
                out.append(p)
    return out


def image_tag(root: Path = PROJECT) -> str:
    """``h-{huella}``: sha256 de las rutas y el contenido (con LF) de la imagen, 12 hex. Mismo código ⇒ misma etiqueta.

    No sale de git: el git de WSL no lee el checkout de Windows («dubious ownership») y ve todo cambiado por CRLF."""
    h = hashlib.sha256()
    for p in image_files(root):
        h.update(p.relative_to(root).as_posix().encode() + b"\0" + p.read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return f"h-{h.hexdigest()[:12]}"


def raw_envelopes(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Envelopes de ``ta-raw-*`` (sin los malformados del caos y sin repetidos por ``event_id``)."""
    seen, out = set(), []
    for m in messages:
        try:
            env = json.loads(m["data"])
        except (ValueError, TypeError):
            continue
        if not isinstance(env, dict) or not isinstance(env.get("sim"), dict) or not env.get("ingest_time"):
            continue
        if env.get("event_id") in seen:
            continue
        seen.add(env.get("event_id"))
        out.append(env)
    return out


def _ts(text: str) -> datetime:
    return am.parse_ts(text)


def wave_stats(envs: list[dict[str, Any]], scenario: dict[str, Any], before_from: datetime,
               until: datetime) -> dict[str, Any]:
    """Cadencia y marca por tramo (antes, durante, después), por ``ingest_time`` (la hora del simulador)."""
    start, end = _ts(scenario["started_at"]), _ts(scenario["ends_at"])
    phases = {}
    for key, lo, hi in (("before", before_from, start), ("during", start, end), ("after", end, until)):
        evs = [e for e in envs if lo <= _ts(e["ingest_time"]) < hi]
        marked = [e for e in evs if e["sim"].get("scenario")]
        minutes = max((hi - lo).total_seconds(), 1.0) / 60
        phases[key] = {
            "minutes": round(minutes, 2),
            "events": len(evs),
            "per_min": round(len(evs) / minutes, 2),
            "marked": len(marked),
            "marked_share": round(len(marked) / len(evs), 4) if evs else 0.0,
            "normal_per_min": round((len(evs) - len(marked)) / minutes, 2),
            "wrong_marked": [e["event_id"] for e in marked
                             if (e.get("brands") or [None])[0] != scenario["brand"]
                             or e["sim"].get("label_sentiment") != scenario["sentiment"]
                             or e["sim"].get("scenario") != scenario["name"]
                             or e.get("generator") != GENERATOR][:5],
        }
    before, during = phases["before"]["per_min"], phases["during"]["per_min"]
    return {"phases": phases, "rate_ratio": round(during / before, 2) if before else None,
            "expected_share": round(1 - 1 / float(scenario["multiplier"]), 4)}


def wave_problems(stats: dict[str, Any], multiplier: float) -> list[str]:
    p = stats["phases"]
    out = []
    ratio = stats["rate_ratio"] or 0
    if not 0.7 * multiplier <= ratio <= 1.3 * multiplier:
        out.append(f"cadencia durante/antes = {ratio} (esperado ≈ {multiplier:g})")
    if abs(p["during"]["marked_share"] - stats["expected_share"]) > 0.1:
        out.append(f"fracción marcada {p['during']['marked_share']} (esperado ≈ {stats['expected_share']})")
    if p["before"]["marked"] or p["after"]["marked"]:
        out.append(f"eventos marcados fuera del escenario: antes {p['before']['marked']}, después {p['after']['marked']}")
    base = p["before"]["normal_per_min"]
    if base and not 0.6 <= p["during"]["normal_per_min"] / base <= 1.4:
        out.append(f"el tráfico normal cambió: {base}/min antes y {p['during']['normal_per_min']}/min durante")
    if p["during"]["wrong_marked"]:
        out.append(f"marcados con otra marca, sentimiento, nombre o generador: {p['during']['wrong_marked']}")
    if not p["during"]["marked"]:
        out.append("ningún evento marcado durante el escenario")
    return out


def public_members(policy: dict[str, Any] | None) -> list[str]:
    return sorted({m for b in (policy or {}).get("bindings") or [] for m in b.get("members") or []
                   if m in ("allUsers", "allAuthenticatedUsers")})


def scenario_args(ctx: Ctx) -> list[str]:
    o = ctx.options
    return ["--brand", o.brand, "--sentiment", o.sentiment, "--multiplier", f"{o.multiplier:g}",
            "--duration", str(o.duration), "--source", o.source]


# --- consultas a la nube ------------------------------------------------------------------------------


def deployed_image(ctx: Ctx) -> str | None:
    res = ctx.gcloud("run", "services", "describe", client.SERVICE, f"--region={v62.GCP_REGION}",
                     "--format=value(spec.template.spec.containers[0].image)", timeout=120)
    return res.out.strip() or None if res.ok else None


def simulator_logs(ctx: Ctx, name: str) -> list[dict[str, Any]]:
    flt = (f'resource.type="cloud_run_revision" AND resource.labels.service_name="{client.SERVICE}" '
           f'AND jsonPayload.name="{name}"')
    return v62.payloads(ctx.gjson("logging", "read", flt, "--freshness=2h", "--limit=20", timeout=120))


# --- Parte A · preflight -----------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código, de GCP y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    missing = []
    for mod in ("apache_beam", "google.cloud.pubsub_v1", "jsonschema", "yaml"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    part.add("A1", "Python y dependencias", FALLA if missing or (py.major, py.minor) != (3, 11) else OK,
             f"Python {py.major}.{py.minor}.{py.micro}"
             + (f"; faltan: {', '.join(missing)}" if missing else "; Beam, Pub/Sub, jsonschema y PyYAML"))
    absent = [f for f in LOT_FILES + PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote y {len(PREVIOUS_FILES)} que reutiliza", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else "presentes")
    tools = {t: ctx.runner.which(t) for t in ("gcloud", "bq", "terraform", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A3", "herramientas", FALLA if need else OK,
             ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))
    part.add("A4", "proyecto de GCP y ADC", OK if v62.ADC.exists() and ctx.project else FALLA,
             f"{ctx.project} · ADC {'presente' if v62.ADC.exists() else 'FALTA: gcloud auth application-default login'}")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    try:
        jobs = sv.active_jobs(run, s)
        sim = sv.simulator_service(run, s)
    except sv.OpsError as exc:
        part.add("A5", "Dataflow y simulador", FALLA, str(exc))
        return part.finish()
    try:
        image = read_image(TFVARS.read_text(encoding="utf-8"))
    except OSError:
        image = None
    part.add("A5", "Dataflow, simulador e imagen en terraform.tfvars", OK if image else FALLA,
             f"Dataflow: {', '.join(j.get('name', '?') for j in jobs) or 'apagado'} · simulador: "
             f"{'ENCENDIDO' if sim else 'apagado'} · simulator_image: {(image or 'FALTA').rsplit('/', 1)[-1]}",
             dataflow=[j.get("name") for j in jobs], simulator=bool(sim), image=image)
    outputs = v62.tf_outputs(ctx)
    ready = outputs.get("alerts_notifier_url") and outputs.get("alert_policy_id")
    part.add("A6", "alertas del 6.2 (notificador y política)", OK if ready else AVISO,
             f"{outputs.get('alerts_notifier_url') or 'sin notificador'} · {outputs.get('alert_policy_name') or 'sin política'}"
             + ("" if outputs.get("alerts_notifier_url") else ": la parte flow no podrá medir la notificación"))
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/common src/simulator (lo que corre la imagen; escenario incluido)",
         ["src/common", "src/simulator", "-q"], B1_EXPECTED),
        ("B2", "pytest src/demo (cliente, CLI, ola en tiempo virtual y API)", ["src/demo", "-q"], B2_EXPECTED),
        ("B3", "pytest test_ops_validate63.py", ["src/ops/tests/test_ops_validate63.py", "-q"], B3_EXPECTED),
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
    part = Part(ctx, "L", "Ola en tiempo virtual y API con el servidor real (sin nube)")
    res = ctx.run([sys.executable, "scripts/demo_scenario.py", "check", *scenario_args(ctx)], cwd=PROJECT, timeout=300)
    out = v62.last_event(v62.events(res.out), "scenario_check")
    if not out:
        part.add("L1", "demo_scenario.py check", FALLA, res.tail(8))
        return part.finish()
    t, p = out["timeline"], out["timeline"]["phases"]
    part.add("L1", "ola: cadencia ×m, fracción marcada 1 − 1/m, el resto igual, envelope v1.0", OK if not out["problems"] else FALLA,
             f"{t['events']} eventos · {p['before']['per_min']}/min antes → {p['during']['per_min']}/min durante "
             f"(×{t['rate_ratio']}) → {p['after']['per_min']}/min después · marcados {p['during']['marked']} "
             f"({p['during']['marked_share']:.0%}, esperado {t['expected_share']:.0%}) · normal {p['before']['normal_per_min']} → "
             f"{p['during']['normal_per_min']}/min · {len(t['contract_problems'])} errores de contrato · terminó por "
             f"{(t['status_after'].get('last') or {}).get('reason')}" + ("".join(f"\n{x}" for x in out["problems"])),
             rate_ratio=t["rate_ratio"], phases=p, scenario=t["scenario"].get("name"))
    bad = out["api_problems"]
    part.add("L2", "API /health y /control/scenario con el servidor HTTP del simulador", OK if not bad else FALLA,
             " · ".join(f"{s['step']} {s['status']}" for s in out["api"]) + (f"\nfallan: {bad}" if bad else ""),
             api=out["api"], logs=out.get("simulator_logs"))
    return part.finish()


# --- Parte D · imagen ----------------------------------------------------------------------------------------


def part_image(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Imagen stream-simulator con el escenario")
    tag = image_tag()
    image = f"{IMAGE_REPO.format(region=v62.GCP_REGION, project=ctx.project)}:{tag}"
    part.add("D1", "etiqueta por huella del contenido de la imagen", OK, f"{tag} ({len(image_files())} archivos)", tag=tag)
    exists = ctx.gcloud("artifacts", "docker", "images", "describe", image, "--format=value(image_summary.digest)", timeout=120)
    if exists.ok and exists.out.strip():
        part.add("D2", "imagen en Artifact Registry", OK, f"ya existe ({exists.out.strip()[:19]}…): no se reconstruye", image=image)
    elif ctx.ask(f"¿Construir {tag} en Cloud Build (corre las pruebas del simulador dentro de la imagen)?"):
        res = ctx.run(["bash", "scripts/build_simulator_image.sh", "cloudbuild", tag], cwd=PROJECT, timeout=1800, stream=True)
        part.add("D2", "Cloud Build (pruebas en la imagen + push)", OK if res.ok else FALLA,
                 f"{image} en {res.seconds:.0f} s" if res.ok else res.tail(12), image=image, seconds=round(res.seconds))
        if not res.ok:
            return part.finish()
    else:
        part.add("D2", "imagen", AVISO, "no confirmado")
        return part.finish()
    text = TFVARS.read_text(encoding="utf-8")
    before = read_image(text)
    if before != image:
        TFVARS.write_text(set_image(text, image), encoding="utf-8")
    part.add("D3", "simulator_image en terraform.tfvars (no se versiona)", OK,
             f"{(before or 'vacío').rsplit(':', 1)[-1]} → {tag}" if before != image else f"ya era {tag}",
             previous=before, image=image)
    return part.finish()


# --- Parte S · acceso ---------------------------------------------------------------------------------------


def part_auth(ctx: Ctx) -> int:
    part = Part(ctx, "S", "Simulador desplegado y /control/scenario protegido con ID token")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    url = sv.simulator_service(run, s)
    if not url:
        part.add("S1", "simulador en Cloud Run", FALLA, "apagado: corra prepare")
        return part.finish()
    want = read_image(TFVARS.read_text(encoding="utf-8"))
    got = deployed_image(ctx)
    part.add("S1", "imagen desplegada = simulator_image", OK if got and got == want else FALLA,
             f"{url} · {(got or '?').rsplit('/', 1)[-1]}"
             + ("" if got == want else f" (terraform.tfvars: {(want or '?').rsplit('/', 1)[-1]})"),
             url=url, image=got)
    policy = ctx.gjson("run", "services", "get-iam-policy", client.SERVICE, f"--region={v62.GCP_REGION}")
    public = public_members(policy)
    part.add("S2", "IAM del servicio sin allUsers ni allAuthenticatedUsers", FALLA if public or policy is None else OK,
             f"públicos: {public}" if public else ("no se pudo leer la política" if policy is None else
             f"{len((policy or {}).get('bindings') or [])} enlace(s), ninguno público"))
    anon = ctx.demo("status", "--no-token")
    part.add("S3", "sin token: Cloud Run responde 401/403 antes del contenedor", OK if anon.get("status") in (401, 403) else FALLA,
             f"{anon.get('status')} {str(anon.get('error') or '')[:60]}" if anon else ctx.cache.get("demo_tail", ""),
             http_status=anon.get("status"))
    health = ctx.demo("status")
    part.add("S4", "con token: GET /control/scenario", OK if health.get("event") == "scenario_status" and "active" in health else FALLA,
             f"{health.get('status')} · activo: {(health.get('active') or {}).get('name')} · "
             f"último: {(health.get('last') or {}).get('name')}"
             if health else ctx.cache.get("demo_tail", ""), active=health.get("active"))
    bad = ctx.demo("start", "--brand", "cocacola", "--multiplier", "50")
    rejected = bad.get("status") == 400 and len(bad.get("problems") or []) == 2
    part.add("S5", "con token: POST inválido → 400 con los problemas", OK if rejected else FALLA,
             f"{bad.get('status')} · {bad.get('problems')}" if bad else ctx.cache.get("demo_tail", ""))
    return part.finish()


# --- Parte F · ola real -----------------------------------------------------------------------------------


def wait_baseline(ctx: Ctx, part: Part, brand: str, source: str, since: datetime) -> bool:
    max_wait = float(getattr(ctx.options, "max_wait_minutes", 90) or 0) * 60
    waited = 0.0
    while True:
        first, n = v62.first_window(ctx, brand, source, since)
        ready = v62.warm_at(first) if first else None
        left = (ready - ctx.now()).total_seconds() if ready else None
        if ready and left <= 0:
            part.add("F1", "baseline completo (60 ventanas)", OK,
                     f"primera ventana de {brand}/{source}: {first} · listo desde {ready:%H:%M} UTC",
                     first_window=first)
            return True
        if waited >= max_wait:
            part.add("F1", "baseline completo (60 ventanas)", AVISO,
                     f"todavía no: primera ventana {first or 'sin datos'}; listo ≈ {ready:%H:%M} UTC" if ready else
                     f"sin ventanas de {brand}/{source}: ¿corre el simulador?")
            return False
        part.note(f"esperando el baseline de {brand}/{source}: {n} ventanas, listo ≈ {ready:%H:%M:%S} UTC ({left / 60:.0f} min)"
                  if ready else "esperando las primeras ventanas…")
        pause = min(300.0, max(30.0, left or 60.0))
        ctx.sleep(pause)
        waited += pause


def part_flow(ctx: Ctx) -> int:
    part = Part(ctx, "F", "Escenario real: POST /control/scenario → ola marcada → trend_windows → alerta → notificación")
    o = ctx.options
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    started = ctx.step_data("P", "P2").get("started_at")
    if not jobs or not sim or not started:
        part.add("F0", "Dataflow y simulador", FALLA, f"Dataflow {'encendido' if jobs else 'APAGADO'} · simulador "
                 f"{'encendido' if sim else 'APAGADO'} · parte P {'hecha' if started else 'sin correr'}: corra prepare")
        return part.finish()
    part.add("F0", "Dataflow y simulador", OK, f"{jobs[0].get('name')} y {sim} desde {started[:19]} UTC")
    if not wait_baseline(ctx, part, o.brand, o.source, datetime.fromisoformat(started)):
        return part.finish()
    run_id = uuid.uuid4().hex[:8]
    subs = {"raw": (f"ta-verify-raw-{run_id}", f"ta-raw-{o.source}"), "alerts": (f"ta-verify-alerts-{run_id}", v62.ALERTS_TOPIC),
            "incidents": (f"ta-verify-incidents-{run_id}", v62.INCIDENTS_TOPIC)}
    if not all(v62.create_sub(ctx, name, topic) for name, topic in subs.values()):
        part.add("F2", "suscripciones temporales", FALLA, str(subs))
        for name, _t in subs.values():
            v62.delete_sub(ctx, name)
        return part.finish()
    before_from = ctx.now() + timedelta(seconds=5)
    part.add("F2", "suscripciones temporales a ta-raw, ta-alerts y ta-alerts-incidents", OK, ", ".join(n for n, _t in subs.values()))
    raw: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []
    incidents: list[dict[str, Any]] = []

    def observe(seconds: float) -> None:
        end = time.monotonic() + seconds
        for _ in range(int(seconds // 20) + 1):  # un pull vacío espera ~20 s: manda el reloj (v2.50)
            if time.monotonic() >= end:
                break
            raw.extend(v62.pull(ctx, subs["raw"][0], 1000))
            messages.extend(v62.pull(ctx, subs["alerts"][0]))
            incidents.extend(v62.incidents_from(v62.pull(ctx, subs["incidents"][0])))
            ctx.sleep(20)

    try:
        part.note(f"midiendo la cadencia normal de {o.source} {BEFORE_S} s…")
        observe(BEFORE_S)
        out = ctx.demo("start", *scenario_args(ctx))
        scenario = out.get("started") or {}
        part.add("F3", "POST /control/scenario (con ID token)", OK if out.get("status") == 201 and scenario else FALLA,
                 f"{scenario.get('name')}: {scenario.get('brand')}/{scenario.get('sentiment')} ×{scenario.get('multiplier')} "
                 f"en {scenario.get('source') or 'las tres'} de {scenario.get('started_at', '')[11:19]} a "
                 f"{scenario.get('ends_at', '')[11:19]} UTC" if scenario else f"{out or ctx.cache.get('demo_tail', '')}",
                 scenario=scenario)
        if not scenario:
            return part.finish()
        watch = float(o.duration) + float(getattr(o, "watch_minutes", 8) or 8) * 60
        part.note(f"observando la ola, ta-alerts y las notificaciones {watch / 60:.0f} min…")
        observe(watch)
        for name, items in (("raw", raw), ("alerts", messages)):
            pulled = v62.pull(ctx, subs[name][0], 1000)
            items.extend(pulled)
        incidents.extend(v62.incidents_from(v62.pull(ctx, subs["incidents"][0])))
    finally:
        for name, _t in subs.values():
            v62.delete_sub(ctx, name)
    # F4–F9 como en el 6.2: trend_windows, mensajes de Dataflow, contenido, detección < 5 min, cooldown y otras alertas.
    v62.flow_report(ctx, part, o.brand, o.source, {"first_event_time": scenario["started_at"]}, messages, incidents)
    after = ctx.demo("status")
    last = after.get("last") or {}
    ended = last.get("name") == scenario["name"] and last.get("reason") == "expired" and after.get("active") is None
    part.add("F10", "el escenario terminó solo al cumplir duration_s", OK if ended else FALLA,
             f"{last.get('reason')} a las {str(last.get('ended_at'))[11:19]} UTC · eventos forzados {last.get('events')}"
             if last else str(after or ctx.cache.get("demo_tail", "")), last=last)
    envs = raw_envelopes(raw)
    stats = wave_stats(envs, scenario, before_from, _ts(scenario["ends_at"]) + timedelta(minutes=3))
    p = stats["phases"]
    problems = wave_problems(stats, float(scenario["multiplier"]))
    part.add("F11", f"ola en ta-raw-{o.source}: cadencia ×m, marca en sim.scenario y el resto igual", OK if not problems else FALLA,
             f"{len(envs)} eventos · {p['before']['per_min']}/min antes → {p['during']['per_min']}/min durante (×{stats['rate_ratio']}) → "
             f"{p['after']['per_min']}/min después · marcados {p['during']['marked']} ({p['during']['marked_share']:.0%}, esperado "
             f"{stats['expected_share']:.0%}), todos {scenario['brand']}/{scenario['sentiment']}/{GENERATOR} · normal "
             f"{p['before']['normal_per_min']} → {p['during']['normal_per_min']}/min" + "".join(f"\n{x}" for x in problems),
             **stats)
    logs = {name: len([x for x in simulator_logs(ctx, scenario["name"]) if x.get("event") == name])
            for name in ("scenario_start", "scenario_end")}
    part.add("F12", "logs del simulador en Cloud Logging", OK if logs == {"scenario_start": 1, "scenario_end": 1} else AVISO,
             f"scenario_start {logs['scenario_start']} · scenario_end {logs['scenario_end']}", logs=logs)
    return part.finish()


# --- status ---------------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    run, s = v62.sv_runner(ctx), v62.settings(ctx)
    jobs = sv.active_jobs(run, s)
    sim = sv.simulator_service(run, s)
    part.add("S1", "Dataflow y simulador", AVISO if jobs or sim else OK,
             f"ENCENDIDOS: {[j.get('name') for j in jobs]} {'simulador' if sim else ''} (≈ "
             f"{v62.DATAFLOW_USD_PER_HOUR + v62.SIMULATOR_USD_PER_HOUR:.2f} USD/h; validate_6_3.py off)" if jobs or sim else "apagados")
    image = read_image(TFVARS.read_text(encoding="utf-8")) if TFVARS.exists() else None
    current = bool(image) and image.endswith(":" + image_tag())
    part.add("S2", "simulator_image en terraform.tfvars = el código actual", OK if current else AVISO,
             (image or "vacío").rsplit("/", 1)[-1] + ("" if current else f": el código actual es {image_tag()} (parte image)"))
    f7, f11 = ctx.step_data("F", "F7"), ctx.step_data("F", "F11")
    part.add("S3", "última ola medida", OK if f7 else AVISO,
             f"×{f11.get('rate_ratio')} · notificación {f7.get('notification_s')} s tras el fin de la ventana" if f7 else
             "todavía no se corrió la parte flow")
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "image": part_image,
    "prepare": v62.part_prepare,
    "auth": part_auth,
    "flow": part_flow,
    "off": v62.part_off,
    "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    d = client.DEFAULTS
    parser = argparse.ArgumentParser(prog="validate_6_3.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (si no, GCP_PROJECT o el de §5.9)")
    parser.add_argument("--brand", default=d["brand"])
    parser.add_argument("--sentiment", default=d["sentiment"])
    parser.add_argument("--multiplier", type=float, default=d["multiplier"])
    parser.add_argument("--duration", type=int, default=d["duration_s"], help="segundos del escenario")
    parser.add_argument("--source", default=d["source"], choices=am.SOURCES)
    parser.add_argument("--max-wait-minutes", type=float, default=90, help="F: espera máxima del baseline")
    parser.add_argument("--watch-minutes", type=float, default=8, help="F: cuánto se observa después del escenario")
    parser.add_argument("--force", action="store_true", help="O: cancelar Dataflow si sigue drenando a los 15 min")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 6.3 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except sv.OpsError as exc:
        printer(f"error: {exc}")
        return 1
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido. Si Dataflow o el simulador quedaron encendidos: python project/scripts/validate_6_3.py off")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
