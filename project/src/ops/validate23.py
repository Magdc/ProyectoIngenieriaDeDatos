"""Validación automática del Lote 2.3 (NLP), siguiendo ``docs/validation/validation-2-3.md``.

Partes:

* **tests**  (B, C): dependencias del preprocesado, pruebas de los lotes 2.2 y 2.3, regresión
  completa de ``streaming/``, ``contracts/`` y ``validate.sh``. Gratis.
* **local**  (D): imagen local con los modelos (se construye solo si falta), prueba de humo,
  ``dataflow-local`` con NLP, ``verify_enriched.py --local --expect-nlp`` y regresión del 2.1.
  Gratis.
* **cloud**  (E): permiso de Artifact Registry (``apply`` vigilado: solo ese recurso), imagen
  ``dataflow-nlp`` (se construye solo si falta), simulador y job con NLP, verificador con
  BigQuery, consultas de E8 (versión, filas sin NLP, latencia de emisión a BigQuery < 60 s) y
  centinela de E9. Cobra mientras corre.

Deja el resumen en pantalla y la evidencia en ``evidencia/validacion_2_3.json``. Salida: 0 si
todo pasó, 1 si algo falló, 2 si no se pudo correr.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ops import services as sv

PROJECT_DIR = sv.PROJECT_DIR
EVIDENCE_DIR = PROJECT_DIR / "evidencia"
TABLE = "{project}.ta_stream.posts_enriched"

OK, FAIL, SKIP, WARN = "OK", "FALLA", "OMITIDO", "AVISO"


@dataclass
class Step:
    id: str
    name: str
    status: str = SKIP
    detail: str = ""
    data: dict = field(default_factory=dict)


class Validation:
    def __init__(self, run: sv.Runner):
        self.run = run
        self.steps: list[Step] = []
        self.started = datetime.now(timezone.utc)

    def step(self, sid: str, name: str) -> Step:
        st = Step(sid, name)
        self.steps.append(st)
        print(f"\n▶ {sid} · {name}", flush=True)
        return st

    def done(self, st: Step, status: str, detail: str = "", **data) -> Step:
        st.status, st.detail = status, detail
        st.data.update(data)
        print(f"  {status}: {detail}", flush=True)
        return st

    @property
    def failed(self) -> bool:
        return any(s.status == FAIL for s in self.steps)

    def summary(self) -> str:
        width = max(len(s.name) for s in self.steps) if self.steps else 10
        lines = ["", "Resumen del Lote 2.3:"]
        for s in self.steps:
            lines.append(f"  {s.status:<8} {s.id:<4} {s.name:<{width}}  {s.detail}")
        lines.append(f"Veredicto: {'FALLA' if self.failed else 'OK'}")
        return "\n".join(lines)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "lote": "2.3",
            "started_at": self.started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "verdict": "fail" if self.failed else "ok",
            "steps": [asdict(s) for s in self.steps],
        }, ensure_ascii=False, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Utilidades puras (con pruebas)
# --------------------------------------------------------------------------- #


def pytest_counts(output: str) -> dict[str, int]:
    """``{'passed': n, 'failed': n, 'skipped': n, 'errors': n}`` de la última línea de pytest."""
    counts = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
    for n, word in re.findall(r"(\d+) (passed|failed|skipped|errors?|deselected)", output):
        key = "errors" if word.startswith("error") else word
        if key in counts:
            counts[key] = int(n)
    return counts


def failed_tests(output: str) -> list[str]:
    """Líneas ``FAILED ruta::prueba - motivo`` del resumen de pytest (``-rf``)."""
    return [ln[len("FAILED "):].strip()[:300] for ln in output.splitlines() if ln.startswith("FAILED ")]


def ar_plan_ok(changes: list[tuple[str, str, list[str]]]) -> list[str]:
    """Cambios no esperados en el plan del Lote 2.3 (solo se admite el permiso de AR)."""
    bad = []
    for address, rtype, actions in changes:
        if rtype == "google_artifact_registry_repository_iam_member" and actions == ["create"]:
            continue
        if rtype.startswith("google_cloud_run") and actions == ["update"]:
            continue  # deriva del simulador (imagen o variables): se actualiza en sitio
        bad.append(f"{address} ({'/'.join(actions)})")
    return bad


def ar_plan_detail(changes: list[tuple[str, str, list[str]]]) -> str:
    """Qué aplicó E2: el permiso de AR (si faltaba) y la deriva del simulador (si la había)."""
    ar_type = "google_artifact_registry_repository_iam_member"
    ar = [a for a, t, _x in changes if t == ar_type]
    other = [f"{a} ({'/'.join(x)})" for a, t, x in changes if t != ar_type]
    parts = ["permiso creado ahora" if ar else "el permiso ya existía"]
    if other:
        parts.append("además se actualizó en sitio: " + ", ".join(other))
    return "; ".join(parts)


_NLP_IMAGE_REF = re.compile(r"[\w.\-/]*dataflow-nlp:[\w.\-]+")


def job_images(job_text: str) -> list[str]:
    """Referencias a la imagen ``dataflow-nlp`` en la descripción del job, en cualquier campo.

    ``gcloud dataflow jobs describe --full`` no trae un campo ``sdkContainerImage``: la imagen
    aparece en las opciones del pipeline y en los contenedores del worker.
    """
    return sorted(set(_NLP_IMAGE_REF.findall(job_text)))


def image_in_job(job_text: str, image: str) -> bool:
    name = image.rsplit("/", 1)[-1]
    return any(ref == image or ref.rsplit("/", 1)[-1] == name for ref in job_images(job_text))


def coverage_wait_s(job_started: float, now: float, window_s: int = 300, settle_s: int = 600, margin_s: int = 30) -> int:
    """Segundos que faltan para medir la cobertura ``raw → enriched`` del job nuevo.

    El verificador solo cuenta ventanas de ``raw/`` que empiezan dentro de la ventana revisada
    (desde el arranque del job) y que cerraron hace al menos ``settle_s``. Hace falta que entre
    el arranque y ``ahora − settle_s`` quepa una ventana entera: ``settle_s + window_s``.
    """
    return max(0, int(job_started + settle_s + window_s + margin_s - now))


def sentinel_from(check_json: dict) -> str | None:
    plan = check_json.get("plan") or {}
    return plan.get("sentinel_event_id")


def wait(seconds: int, label: str) -> None:
    end = time.time() + seconds
    while True:
        left = int(end - time.time())
        if left <= 0:
            break
        print(f"  … {label}: faltan {left // 60} min {left % 60:02d} s", end="\r", flush=True)
        time.sleep(min(15, left))
    print(" " * 70, end="\r")


# --------------------------------------------------------------------------- #
# Partes
# --------------------------------------------------------------------------- #


def part_tests(v: Validation, py: str) -> None:
    st = v.step("B1", "preprocesado de pysentimiento en el entorno de pruebas")
    r1 = v.run([py, "-m", "pip", "install", "-q", "--no-deps", "pysentimiento==0.7.3"])
    r2 = v.run([py, "-m", "pip", "install", "-q", "emoji==2.14.0"])
    v.done(st, OK if r1.ok and r2.ok else FAIL, "pysentimiento 0.7.3 y emoji 2.14.0" if r1.ok and r2.ok else (r1.err or r2.err)[-300:])

    st = v.step("B2", "pruebas de los lotes 2.2 y 2.3")
    r = v.run([py, "-m", "pytest", "-c", "pytest.ini", "src/streaming", "-k", "enriched or nlp", "-q", "-rfs", "-p", "no:cacheprovider"])
    c = pytest_counts(r.out[-3000:])
    good = r.ok and c["failed"] == 0 and c["skipped"] == 0 and c["passed"] >= 89
    failed = failed_tests(r.out)
    v.done(st, OK if good else FAIL, f"{c['passed']} passed, {c['failed']} failed, {c['skipped']} skipped (esperado ≥ 89, 0 omitidas)", **c,
           failed_tests=failed, tail=r.out[-6000:] if not good else "")
    for line in failed:
        print(f"    ✗ {line}")

    st = v.step("B3", "regresión de streaming/ y contracts/")
    r = v.run([py, "-m", "pytest", "-c", "pytest.ini", "src/streaming", "-q", "-p", "no:cacheprovider"])
    c = pytest_counts(r.out[-3000:])
    r2 = v.run([py, "-m", "pytest", "-c", "pytest.ini", "contracts/tests", "-q", "-p", "no:cacheprovider"])
    c2 = pytest_counts(r2.out[-2000:])
    good = r.ok and r2.ok and c["failed"] == 0 and c2["passed"] == 18
    v.done(st, OK if good else FAIL, f"streaming {c['passed']} passed / {c['failed']} failed; contracts {c2['passed']} passed",
           streaming=c, contracts=c2, tail=(r.out + r2.out)[-1500:] if not good else "")

    st = v.step("C", "Terraform sin credenciales (validate.sh)")
    r = v.run(["bash", "infra/validate.sh"])
    v.done(st, OK if r.ok else FAIL, "fmt y validate en verde" if r.ok else (r.out + r.err)[-600:])


def part_local(v: Validation, py: str, rebuild: bool, wait_s: int) -> None:
    st = v.step("D1", "imagen local con los modelos")
    has = v.run(["docker", "image", "inspect", sv.LOCAL_NLP_IMAGE]).ok
    if has and not rebuild:
        v.done(st, OK, f"{sv.LOCAL_NLP_IMAGE} ya existe (use --rebuild-local para reconstruirla)")
    else:
        print("  construyendo (10 a 15 min la primera vez)…", flush=True)
        r = v.run(["bash", "scripts/local_nlp.sh", "build"], capture=False)
        v.done(st, OK if r.ok else FAIL, "construida" if r.ok else "falló la construcción (ver arriba)")
        if not r.ok:
            return

    st = v.step("D2", "prueba de humo de la imagen")
    r = v.run(["bash", "scripts/local_nlp.sh", "smoke"])
    labels = re.findall(r"^(sentiment|emotion): (\S+) etiquetas (\[.*\])$", r.out, re.M)
    v.done(st, OK if r.ok and "Prueba de humo OK" in r.out else FAIL,
           "; ".join(f"{t} {rev}" for t, rev, _ in labels) or (r.out + r.err)[-400:], output=r.out[-2000:])

    st = v.step("D3", "dataflow-local con NLP")
    up = v.run(["bash", "scripts/local_dataflow.sh", "up"], capture=False)
    nlp = v.run(["bash", "scripts/local_nlp.sh", "up"], capture=False)
    if not (up.ok and nlp.ok):
        v.done(st, FAIL, "no arrancó (ver arriba)")
        return
    wait(wait_s, "ventanas de 60 s con NLP")
    logs = v.run(["docker", "compose", *sv.COMPOSE_BASE, "-f", "docker-compose.nlp.yml", "--profile", "dataflow",
                  "logs", "--no-color", "--since", f"{wait_s + 120}s", "dataflow-local"])
    loaded = logs.out.count("nlp_model_loaded")
    cfg_on = '"enabled": true' in logs.out and "nlp_config" in logs.out
    v.done(st, OK if loaded >= 2 or cfg_on else WARN, f"nlp_model_loaded × {loaded}; nlp_config activo: {cfg_on}")

    st = v.step("D4", "verify_enriched --local --expect-nlp")
    out = EVIDENCE_DIR / "local_nlp_report.json"
    r = None
    for attempt in range(1, 5):  # hasta 3 reintentos de 90 s si aún no hay filas
        hours = max(0.05, (wait_s - 60 + (attempt - 1) * 90) / 3600)
        r = v.run([py, "scripts/verify_enriched.py", "report", "--local", "--hours", f"{hours:.3f}", "--expect-nlp",
                   "--settle-seconds", "120", "--json-out", str(out)])
        if r.ok or "filas recientes: 0 filas" not in r.out:
            break
        print(f"  … aún sin filas en enriched/ (intento {attempt}/4); espero 90 s", flush=True)
        time.sleep(90)
    diag = local_diagnosis(v, wait_s + 600) if not r.ok else {}
    v.done(st, OK if r.ok else FAIL, _verdict_line(r.out), output=r.out[-3000:], diagnosis=diag)
    if diag:
        print(f"    contenedor: {diag['ps'] or '?'}")
        print(f"    eventos: {diag['events']}")
        print(f"    archivos en .local-lake/enriched: {diag['enriched_files']} (último: {diag['last_enriched']})")
        for e in diag["errors"][:8]:
            print(f"    ! {e}")

    st = v.step("D6", "regresión del Lote 2.1 en local (check)")
    r = v.run(["bash", "scripts/local_dataflow.sh", "check"])
    v.done(st, OK if r.ok else FAIL, "check OK" if r.ok else clean_output(r.out + "\n" + r.err), output=clean_output(r.out + "\n" + r.err, 4000))


NLP_COMPOSE = [*sv.COMPOSE_BASE, "-f", "docker-compose.nlp.yml", "--profile", "dataflow"]
_NOISE = re.compile(r"^\s*(Container |\[\+\]|✔|=>|#\d)")


def clean_output(text: str, limit: int = 600) -> str:
    """Quita el ruido de docker compose (Container …, ✔, =>) y deja las últimas líneas útiles."""
    lines = [ln for ln in text.splitlines() if ln.strip() and not _NOISE.match(ln)]
    return "\n".join(lines)[-limit:]


def local_diagnosis(v: Validation, since_s: int) -> dict:
    """Errores, eventos de la rama y archivos de enriched/ del contenedor dataflow-local."""
    logs = v.run(["docker", "compose", *NLP_COMPOSE, "logs", "--no-color", "--since", f"{since_s}s", "dataflow-local"])
    text = logs.out
    errors = [ln.split("|", 1)[-1].strip()[:400] for ln in text.splitlines()
              if re.search(r"Traceback|Error|ERROR|Exception|nlp_inference_failed|enrich_error", ln)][:15]
    events = {e: text.count(f'"event": "{e}"') for e in
              ("nlp_config", "nlp_model_loaded", "hmac_key_loaded", "enriched_file_written", "nlp_inference_failed", "enrich_error", "file_written")}
    files = sorted((PROJECT_DIR / ".local-lake" / "enriched").rglob("part-*.parquet")) if (PROJECT_DIR / ".local-lake" / "enriched").exists() else []
    ps = v.run(["docker", "compose", *NLP_COMPOSE, "ps", "--format", "{{.Service}} {{.Status}}"])
    return {"events": events, "errors": errors, "enriched_files": len(files),
            "last_enriched": str(files[-1].relative_to(PROJECT_DIR)) if files else None, "ps": ps.out.strip()}


def _verdict_line(text: str) -> str:
    m = re.search(r"Veredicto: (\w+)", text)
    fails = [ln.strip() for ln in text.splitlines() if ln.startswith("FALLA")]
    return (f"Veredicto {m.group(1)}" if m else "sin veredicto") + (f" · {fails[0]}" if fails else "")


def _bq(v: Validation, sql: str) -> list[dict]:
    r = v.run(["bq", "query", "--use_legacy_sql=false", "--format=json", "--quiet", sql])
    if not r.ok:
        raise sv.OpsError("bq query falló: " + (r.err or r.out)[-400:])
    text = r.out.strip()
    return json.loads(text) if text else []


def part_cloud(v: Validation, py: str, s: sv.Settings, rebuild_image: bool, wait_s: int, image_mode: str = "docker") -> None:
    say = sv.Report()

    st = v.step("E2", "permiso de Artifact Registry para sa-dataflow")
    if not sv.terraform_ready():
        v.done(st, FAIL, sv.INIT_HINT)
        return
    with tempfile.TemporaryDirectory() as tmp:
        plan_file = os.path.join(tmp, "tfplan")
        r = v.run(["terraform", f"-chdir={sv.TF_DIR}", "plan", "-input=false", "-no-color", "-lock-timeout=60s", f"-out={plan_file}"])
        if not r.ok:
            v.done(st, FAIL, "terraform plan falló: " + (r.err or r.out)[-400:])
            return
        changes = sv.plan_changes(json.loads(v.run(["terraform", f"-chdir={sv.TF_DIR}", "show", "-json", plan_file], check=True).out))
        bad = ar_plan_ok(changes)
        if bad:
            v.done(st, FAIL, "el plan cambia más que el permiso de AR; no se aplica nada: " + "; ".join(bad))
            return
        if changes:
            r = v.run(["terraform", f"-chdir={sv.TF_DIR}", "apply", "-input=false", "-no-color", plan_file])
            if not r.ok:
                v.done(st, FAIL, "apply falló: " + (r.err or r.out)[-400:])
                return
        v.done(st, OK, ar_plan_detail(changes) if changes else "el permiso ya existía; sin cambios",
               changes=[f"{a} ({'/'.join(x)})" for a, _t, x in changes])

    st = v.step("E3", "imagen dataflow-nlp publicada")
    image = None if rebuild_image else sv.latest_nlp_image(v.run, s)
    if image:
        v.done(st, OK, f"{image} (use --rebuild-image para reconstruirla)")
    else:
        if image_mode == "docker":
            if not sv.docker_ok(v.run):
                v.done(st, FAIL, "Docker no responde: abra Docker Desktop (el modo docker construye y sube desde su máquina)")
                return
            v.run(["gcloud", "auth", "configure-docker", f"{s.region}-docker.pkg.dev", "--quiet"])
            print("  construyendo con su Docker y subiendo a Artifact Registry (~2,5 GB; 10 a 30 min según la conexión)…", flush=True)
        else:
            print("  construyendo en Cloud Build (15 a 25 min)…", flush=True)
        r = v.run(["bash", "scripts/build_nlp_image.sh", image_mode], capture=False,
                  env={"GCP_PROJECT": s.project, "GCP_REGION": s.region})
        image = sv.latest_nlp_image(v.run, s) if r.ok else None
        v.done(st, OK if image else FAIL, image or "falló la construcción (ver arriba)")
        if not image:
            return

    st = v.step("E4", "simulador y job de Dataflow con NLP")
    try:
        s.nlp = "on"
        os.environ["TA_NLP_IMAGE"] = image
        sv.cloud_on(v.run, s, say, restart_job=True)
    except sv.OpsError as exc:
        v.done(st, FAIL, str(exc))
        return
    job_started = time.time()  # cloud_on ya drenó el job anterior y esperó a Running
    jobs = sv.active_jobs(v.run, s)
    jid = jobs[0].get("id") if jobs else None
    desc = v.run(["gcloud", "dataflow", "jobs", "describe", jid or "-", "--region", s.region, "--project", s.project, "--full", "--format=json"])
    refs = job_images(desc.out)
    v.done(st, OK if image_in_job(desc.out, image) else WARN,
           f"job {jid} · imagen en el job: {', '.join(refs) or 'no aparece'}", job_id=jid, images=refs)

    wait(wait_s, "ventanas de 300 s con NLP en Dataflow")

    st = v.step("E5", "logs de los workers")
    q = 'resource.type="dataflow_step" AND resource.labels.job_id="%s"' % jid
    loaded = v.run(["gcloud", "logging", "read", q + ' AND "nlp_model_loaded"', "--freshness=1h", "--limit=10", "--format=value(timestamp)", "--project", s.project])
    errors = v.run(["gcloud", "logging", "read", q + ' AND ("nlp_inference_failed" OR "enrich_error" OR "bq_insert_failed")', "--freshness=1h", "--limit=5", "--format=value(timestamp)", "--project", s.project])
    n_loaded, n_err = len(loaded.out.split()), len(errors.out.split())
    v.done(st, OK if n_loaded >= 2 and n_err == 0 else (WARN if n_err == 0 else FAIL), f"nlp_model_loaded × {n_loaded}; errores × {n_err}")

    st = v.step("E7", "verify_enriched --expect-nlp (lake en GCS + BigQuery)")
    extra = coverage_wait_s(job_started, time.time())
    if extra:
        wait(extra, "una ventana completa de raw/ para medir la cobertura")
    # Desde el arranque del job nuevo: así no entran filas del job anterior (sin NLP) y la
    # cobertura raw → enriched tiene ventanas cerradas que contar.
    hours = max(0.05, (time.time() - job_started) / 3600)
    r = v.run([py, "scripts/verify_enriched.py", "report", "--hours", f"{hours:.3f}", "--expect-nlp", "--settle-seconds", "600",
               "--json-out", str(EVIDENCE_DIR / "nlp_report.json")],
              env={"GCP_PROJECT": s.project})
    v.done(st, OK if r.ok else FAIL, _verdict_line(r.out), output=r.out[-3000:])

    st = v.step("E8", "BigQuery: versión única, sin NLP nulo y emisión → BigQuery < 60 s")
    table = TABLE.format(project=s.project)
    try:
        rows = _bq(v, f"""
SELECT nlp_model_version AS version, COUNT(*) AS n,
       COUNTIF(sentiment_label IS NULL OR emotion_label IS NULL) AS sin_nlp,
       APPROX_QUANTILES(TIMESTAMP_DIFF(process_time, ingest_time, MILLISECOND), 100)[OFFSET(95)] AS p95_ms
FROM `{table}`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {max(5, wait_s // 60 - 2)} MINUTE)
GROUP BY 1""")
        with_v = [r for r in rows if r.get("version")]
        p95 = max((int(r["p95_ms"]) for r in with_v), default=None)
        # filas sin versión = sin NLP; más las que tienen versión pero alguna etiqueta nula
        sin = sum(int(r["n"]) for r in rows if not r.get("version")) + sum(int(r["sin_nlp"]) for r in with_v)
        good = len(with_v) == 1 and sin == 0 and p95 is not None and p95 < 60000
        v.done(st, OK if good else FAIL,
               f"versiones {[r['version'] for r in with_v]}; sin NLP {sin}; p95 emisión → BigQuery {p95} ms", rows=rows)
    except sv.OpsError as exc:
        v.done(st, FAIL, str(exc))

    st = v.step("E9", "centinela de punta a punta (check --allow-cloud)")
    out = EVIDENCE_DIR / "streaming_check_2_3.json"
    r = v.run([py, "scripts/verify_streaming.py", "check", "--allow-cloud", "--json-out", str(out)], env={"GCP_PROJECT": s.project})
    sentinel = sentinel_from(json.loads(out.read_text(encoding="utf-8"))) if out.exists() else None
    detail = "check OK" if r.ok else "check FALLA"
    status = OK if r.ok else FAIL
    if sentinel:
        time.sleep(30)
        try:
            rows = _bq(v, f"""
SELECT sentiment_label, emotion_label, nlp_model_version, TIMESTAMP_DIFF(process_time, ingest_time, SECOND) AS segundos
FROM `{table}` WHERE event_id = "{sentinel}" AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)""")
            if rows and rows[0].get("emotion_label") and int(rows[0]["segundos"]) < 60:
                detail += f" · centinela en BigQuery con NLP en {rows[0]['segundos']} s"
            else:
                status, detail = FAIL, detail + f" · centinela en BigQuery: {rows}"
        except sv.OpsError as exc:
            status, detail = FAIL, f"{detail} · {exc}"
    v.done(st, status, detail, sentinel=sentinel)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None, run: sv.Runner | None = None) -> int:
    p = argparse.ArgumentParser(prog="validate_2_3.py", description="Valida el Lote 2.3 (NLP) siguiendo docs/validation/validation-2-3.md.")
    p.add_argument("parts", nargs="*", metavar="{tests,local,cloud}",
                   help="partes a correr (por defecto: tests local cloud)")
    p.add_argument("--rebuild-local", action="store_true", help="reconstruye la imagen local aunque exista")
    p.add_argument("--rebuild-image", action="store_true", help="reconstruye la imagen de Cloud Build aunque exista")
    p.add_argument("--image-mode", choices=("docker", "cloudbuild"), default="docker",
                   help="cómo construir dataflow-nlp si falta: docker (su máquina; por defecto) o cloudbuild")
    p.add_argument("--wait-local", type=int, default=300, help="segundos de espera con NLP en local (300)")
    p.add_argument("--wait-cloud", type=int, default=720, help="segundos de espera con NLP en Dataflow (720)")
    p.add_argument("--off-after", action="store_true", help="al terminar, drena Dataflow (deja el simulador)")
    p.add_argument("--continue-on-fail", action="store_true", help="sigue con local/cloud aunque fallen las pruebas")
    p.add_argument("--yes", "-y", action="store_true", help="no pedir confirmación")
    args = p.parse_args(argv)
    bad = [x for x in args.parts if x not in ("tests", "local", "cloud")]
    if bad:
        p.error(f"partes desconocidas: {bad} (use tests, local o cloud)")
    args.parts = args.parts or ["tests", "local", "cloud"]
    run = run or sv.Runner()
    py = sys.executable
    v = Validation(run)
    try:
        sv.check_line_endings()
        if not (PROJECT_DIR / "src" / "streaming" / "nlp.py").exists():
            raise sv.OpsError("no está el código del Lote 2.3 (src/streaming/nlp.py): copie out-2-3.zip primero")
        if "cloud" in args.parts or "local" in args.parts:
            if not run([py, "-c", "import apache_beam, pyarrow"]).ok:
                raise sv.OpsError("este Python no tiene Beam: active ~/.venvs/ta-streaming")
        s = None
        print("Validación del Lote 2.3. Partes: " + ", ".join(args.parts))
        if "local" in args.parts:
            print("  - local: puede construir la imagen con los modelos (~2,5 GB, 10–15 min)")
        if "cloud" in args.parts:
            s = sv.Settings(project=sv.detect_project(run), region=os.environ.get("GCP_REGION", "us-central1"),
                            env=os.environ.get("ENV", "dev"), yes=True, wait=True)
            print(f"  - cloud ({s.project}): Terraform vigilado, Cloud Build si falta la imagen, simulador y job "
                  f"con NLP (cobra; ~{args.wait_cloud // 60 + 15} min)")
        if not sv.confirm("¿Continuar?", sv.Settings(project="", yes=args.yes)):
            print("Cancelado.")
            return 1
        if "tests" in args.parts:
            part_tests(v, py)
            if v.failed and not args.continue_on_fail:
                print("\nLas pruebas fallaron: no se sigue con local ni cloud (use --continue-on-fail para forzarlo).")
                print("Detalle de los fallos: python -m pytest -c project/pytest.ini project/src/streaming -k \"enriched or nlp\" -q -rf")
                args.parts = ["tests"]
        if "local" in args.parts:
            if not sv.docker_ok(run):
                raise sv.OpsError("Docker no responde: abra Docker Desktop y repita")
            part_local(v, py, args.rebuild_local, args.wait_local)
        if "cloud" in args.parts:
            part_cloud(v, py, s, args.rebuild_image, args.wait_cloud, args.image_mode)
            if args.off_after:
                sv.drain_jobs(run, s, sv.Report())
    except sv.OpsError as exc:
        if v.steps:
            print(v.summary())
        print(f"\nERROR: {exc}", file=sys.stderr)
        v.save(EVIDENCE_DIR / "validacion_2_3.json")
        return 2
    except KeyboardInterrupt:
        print("\nInterrumpido.", file=sys.stderr)
        v.save(EVIDENCE_DIR / "validacion_2_3.json")
        return 130
    print(v.summary())
    v.save(EVIDENCE_DIR / "validacion_2_3.json")
    print(f"Evidencia: {EVIDENCE_DIR / 'validacion_2_3.json'}")
    if "cloud" in args.parts and not args.off_after:
        print("Recuerde apagar al terminar: python project/scripts/off.py --keep-simulator")
    return 1 if v.failed else 0
