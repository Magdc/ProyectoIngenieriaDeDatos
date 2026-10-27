"""Sigue un comentario centinela de Pub/Sub a ``ta_curated.fact_posts`` y mide cada etapa (HU-048, §3.26.1).

El centinela es un **comentario simulado normal** (``generator = comment-gen@1.0``, el ``EventFactory`` del simulador
con la semilla reservada ``SMOKE_SEED``), decisión del usuario del 27-oct-2026: así atraviesa Trusted (que descarta
los ``ta-verify@*``) y llega a Curated sin tocar ningún job; deja una fila sintética más por corrida y su
``event_id`` queda en la evidencia. Antes, para que Dataflow cargue los modelos de NLP, se publica un
**calentamiento**: el centinela del verificador del 2.1 (``ta-verify@2.1``), que Trusted descarta.

Etapas (desde → hasta; objetivo en segundos, ``OBJECTIVES``): ``bigquery`` (publicación → fila en
``posts_enriched``), ``gcs_enriched`` (publicación → Parquet en ``enriched/``), ``s3_raw`` (``enriched/`` →
``raw/origin=stream/``, disparando el exportador), ``trusted`` (S3 → ``ta_trusted.posts``, disparando el workflow)
y ``curated`` (Trusted → ``ta_curated.fact_posts``). En cada una comprueba que no haya ``user_id`` ni ``handle``
(columnas ni claves de ``payload``) y que ``user_hash`` sea el mismo hex de 64 en todas.

Todo lo que toca la nube pasa por un objeto ``clients`` (``smoke.clouds.Clouds`` en producción, falsos en las
pruebas) y el tiempo por ``now``/``sleep``.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Protocol

SMOKE_SEED = 990048
SMOKE_GENERATOR = "comment-gen@1.0"
WARMUP_GENERATOR = "ta-verify@2.1"
SOURCES = ("reddit", "facebook", "instagram")
IDENTITY_KEYS = ("user_id", "handle")
STAGES = ("bigquery", "gcs_enriched", "s3_raw", "trusted", "curated")
#: Objetivo de cada etapa en segundos, medido desde la etapa de ``FROM`` (§5.12).
OBJECTIVES = {"bigquery": 60, "gcs_enriched": 900, "s3_raw": 300, "trusted": 900, "curated": 600}
FROM = {"bigquery": "published", "gcs_enriched": "published", "s3_raw": "gcs_enriched", "trusted": "s3_raw",
        "curated": "trusted"}
#: Cuánto se espera como máximo (para medir aunque se pase del objetivo).
TIMEOUTS = {"warmup": 1200, "bigquery": 600, "gcs_enriched": 1800, "s3_raw": 900, "trusted": 1800, "curated": 1200}
POLL = {"warmup": 5, "bigquery": 2, "gcs_enriched": 15, "s3_raw": 10, "workflow": 20}
WORKFLOW = "ta-wf-raw-to-curated"
JOB_TRUSTED = "ta-job-raw-to-trusted"
JOB_CURATED = "ta-job-trusted-to-curated"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class Busy(RuntimeError):
    """El workflow ya tiene una corrida en curso (``ConcurrentRunsExceededException``)."""


class Clients(Protocol):  # pragma: no cover - contrato
    def dataflow_active(self) -> list[str]: ...
    def publish(self, topic: str, data: bytes, attributes: dict[str, str]) -> str: ...
    def bq_rows(self, event_id: str, day: str) -> list[dict[str, Any]]: ...
    def gcs_list(self, prefix: str) -> list[dict[str, Any]]: ...
    def gcs_get(self, name: str) -> bytes: ...
    def run_exporter(self) -> dict[str, Any]: ...
    def s3_head(self, key: str) -> dict[str, Any] | None: ...
    def s3_get(self, key: str) -> bytes: ...
    def workflow_runs(self) -> list[dict[str, Any]]: ...
    def start_workflow(self) -> str: ...
    def workflow_run(self, run_id: str) -> dict[str, Any]: ...
    def athena(self, sql: str, database: str) -> tuple[list[str], list[dict[str, Any]]]: ...


@dataclass
class Config:
    source: str = "reddit"
    warmup: bool = True
    trigger: bool = True
    seq: int | None = None
    timeouts: dict[str, float] = field(default_factory=lambda: dict(TIMEOUTS))
    objectives: dict[str, float] = field(default_factory=lambda: dict(OBJECTIVES))


# --- eventos ---------------------------------------------------------------------------------------


def sentinel_emission(source: str, seq: int, now: datetime):
    """El comentario centinela: ``EventFactory`` del simulador con ``SMOKE_SEED`` y el caos apagado."""
    from simulator.factory import EventFactory
    from simulator.sim_config import load_config

    config = load_config(env={"SIM_SEED": str(SMOKE_SEED), "SIM_CHAOS_ENABLED": "false"})
    return EventFactory(config).make(source, seq, now)


def warmup_envelope(run_id: str, seq: int, now: float) -> dict[str, Any]:
    """El centinela del verificador del 2.1 (``ta-verify@2.1``, Reddit): Dataflow lo procesa y Trusted lo descarta."""
    from streaming.inject import sentinel_envelope

    return sentinel_envelope(run_id, seq, now)


def envelope_problems(envelope: dict[str, Any]) -> str | None:
    """Primer error del envelope frente a ``contracts/envelope.v1.schema.json`` (``None`` = válido)."""
    from streaming.schemas import ENVELOPE_SCHEMA, build_validator, describe_error, load_bundle

    return describe_error(build_validator(load_bundle(), ENVELOPE_SCHEMA), envelope)


# --- comprobaciones puras ------------------------------------------------------------------------------


def _keys(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for k, v in value.items():
            yield str(k)
            yield from _keys(v)
    elif isinstance(value, list):
        for v in value:
            yield from _keys(v)


def identity_problems(columns: Iterable[str], row: dict[str, Any]) -> list[str]:
    """Identidad en claro en una etapa: columnas, claves de ``payload`` y forma de ``user_hash``."""
    out = [f"columna {c}" for c in columns if c in IDENTITY_KEYS]
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    out += [f"payload.{k}" for k in sorted(set(_keys(payload))) if k in IDENTITY_KEYS]
    if not _HEX64.match(str(row.get("user_hash") or "")):
        out.append("user_hash no es hex de 64")
    return out


def find_in_parquet(data: bytes, event_id: str) -> tuple[list[str], dict[str, Any] | None]:
    """Columnas del Parquet y la fila de ``event_id`` (o ``None``)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(data))
    rows = table.filter(pc.equal(table["event_id"], event_id)).to_pylist() if "event_id" in table.column_names else []
    return table.column_names, (rows[0] if rows else None)


def hour_prefixes(base: str, start: datetime, end: datetime) -> list[str]:
    """``{base}dt={AAAA-MM-DD}/hr={HH}/`` de cada hora UTC entre ``start`` y ``end``."""
    out, t = [], start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    while t <= end:
        out.append(f"{base}dt={t:%Y-%m-%d}/hr={t:%H}/")
        t += timedelta(hours=1)
    return out


def iso(t: datetime | None) -> str | None:
    return t.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z") if t else None


def as_dt(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value.astimezone(timezone.utc) if isinstance(value, datetime) else None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


# --- la corrida -----------------------------------------------------------------------------------


class Smoke:
    def __init__(self, clients: Clients, config: Config | None = None, *, now: Callable[[], datetime] | None = None,
                 sleep: Callable[[float], None] = time.sleep, log: Callable[[dict[str, Any]], None] | None = None):
        self.c = clients
        self.cfg = config or Config()
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep
        self.log = log or (lambda event: print(json.dumps(event, ensure_ascii=False, default=str), flush=True))
        self.times: dict[str, datetime] = {}
        self.stages: list[dict[str, Any]] = []
        self.identity: dict[str, list[str]] = {}
        self.hashes: dict[str, str] = {}

    # utilidades
    def _wait(self, name: str, probe: Callable[[], Any], poll: float, timeout: float) -> Any:
        deadline = self.now() + timedelta(seconds=timeout)
        while True:
            got = probe()
            if got:
                return got
            if self.now() >= deadline:
                return None
            self.sleep(poll)

    def _stage(self, name: str, available: datetime | None, detail: str, **data: Any) -> bool:
        start = self.times.get(FROM[name])
        objective = self.cfg.objectives[name]
        latency = (available - start).total_seconds() if available and start else None
        ok = latency is not None and latency <= objective
        if available:
            self.times[name] = available
        rec = {"stage": name, "from": FROM[name], "objective_s": objective, "available_at": iso(available),
               "detected_at": iso(self.now()), "latency_s": round(latency, 1) if latency is not None else None,
               "since_publish_s": round((available - self.times["published"]).total_seconds(), 1) if available else None,
               "ok": ok, "detail": detail, **data}
        self.stages.append(rec)
        self.log({"event": "smoke_stage", **rec})
        return available is not None

    def _check(self, stage: str, columns: Iterable[str], row: dict[str, Any]) -> None:
        self.identity[stage] = identity_problems(columns, row)
        if row.get("user_hash"):
            self.hashes[stage] = str(row["user_hash"])

    # etapas
    def run(self) -> dict[str, Any]:
        cfg = self.cfg
        started = self.now()
        seq = cfg.seq if cfg.seq is not None else int(started.timestamp())
        run_id = f"smoke-{started:%Y%m%dT%H%M%SZ}"
        report: dict[str, Any] = {"run_id": run_id, "source": cfg.source, "seq": seq, "started_at": iso(started),
                                  "objectives_s": dict(cfg.objectives), "trigger": cfg.trigger}
        jobs = self.c.dataflow_active()
        if not jobs:
            return self._finish(report, False, "Dataflow apagado: no hay un job ta-stream-* activo", code=2)
        report["dataflow"] = jobs
        if cfg.warmup:
            report["warmup"] = self._warmup(run_id, seq)
            if not report["warmup"].get("ok"):
                return self._finish(report, False, "el calentamiento no llegó a BigQuery")
        emission = sentinel_emission(cfg.source, seq, self.now())
        env = emission.envelope
        problem = envelope_problems(env)
        if problem:
            return self._finish(report, False, f"centinela inválido: {problem}")
        report.update(event_id=env["event_id"], generator=env["generator"], event_time=env["event_time"])
        msg = emission.messages[0]
        self.c.publish(f"ta-raw-{cfg.source}", msg.data, dict(msg.attributes))
        self.times["published"] = self.now()
        report["published_at"] = iso(self.times["published"])
        self.log({"event": "smoke_published", "event_id": env["event_id"], "source": cfg.source,
                  "published_at": report["published_at"]})
        day = env["event_time"][:10]
        ok = (self._bigquery(env["event_id"], day) and self._gcs(env["event_id"]) and self._s3(env["event_id"])
              and self._lakehouse(env["event_id"], day))
        return self._finish(report, ok and all(s["ok"] for s in self.stages), None if ok else "una etapa no llegó")

    def _warmup(self, run_id: str, seq: int) -> dict[str, Any]:
        from streaming.inject import attributes_for, dumps

        env = warmup_envelope(run_id, seq, self.now().timestamp())
        self.c.publish("ta-raw-reddit", dumps(env), attributes_for(env))
        t0 = self.now()
        found = self._wait("warmup", lambda: self.c.bq_rows(env["event_id"], env["event_time"][:10]), POLL["warmup"],
                           self.cfg.timeouts["warmup"])
        out = {"event_id": env["event_id"], "generator": env["generator"], "ok": bool(found),
               "latency_s": round((self.now() - t0).total_seconds(), 1) if found else None}
        self.log({"event": "smoke_warmup", **out})
        return out

    def _bigquery(self, event_id: str, day: str) -> bool:
        rows = self._wait("bigquery", lambda: self.c.bq_rows(event_id, day), POLL["bigquery"], self.cfg.timeouts["bigquery"])
        if not rows:
            return self._stage("bigquery", None, "sin fila en posts_enriched")
        self._check("bigquery", rows[0].keys(), rows[0])
        return self._stage("bigquery", self.now(), f"{len(rows)} fila(s) en posts_enriched", rows=len(rows))

    def _gcs(self, event_id: str) -> bool:
        base = f"enriched/source={self.cfg.source}/"
        seen: set[str] = set()
        floor = self.times["published"] - timedelta(minutes=10)

        def probe():
            for prefix in hour_prefixes(base, floor, self.now()):
                for blob in self.c.gcs_list(prefix):
                    name = blob["name"]
                    created = as_dt(blob.get("created"))
                    if name in seen or not name.endswith(".parquet") or (created and created < floor):
                        continue
                    seen.add(name)
                    data = self.c.gcs_get(name)
                    cols, row = find_in_parquet(data, event_id)
                    if row:
                        return name, created, data, cols, row
            return None

        got = self._wait("gcs_enriched", probe, POLL["gcs_enriched"], self.cfg.timeouts["gcs_enriched"])
        if not got:
            return self._stage("gcs_enriched", None, f"sin el event_id en enriched/ ({len(seen)} archivos revisados)")
        name, created, data, cols, row = got
        self._check("gcs_enriched", cols, row)
        self.gcs = {"name": name, "rel": name[len("enriched/"):], "sha256": hashlib.sha256(data).hexdigest()}
        return self._stage("gcs_enriched", created or self.now(), name, object=name, bytes=len(data))

    def _s3(self, event_id: str) -> bool:
        key = f"raw/origin=stream/{self.gcs['rel']}"
        runs = []
        if self.cfg.trigger:
            runs.append(self.c.run_exporter())
        head = self._wait("s3_raw", lambda: self.c.s3_head(key), POLL["s3_raw"], 120 if self.cfg.trigger else self.cfg.timeouts["s3_raw"])
        if not head and self.cfg.trigger:
            runs.append(self.c.run_exporter())
            head = self._wait("s3_raw", lambda: self.c.s3_head(key), POLL["s3_raw"], self.cfg.timeouts["s3_raw"])
        if not head:
            return self._stage("s3_raw", None, f"no apareció {key}", exporter_runs=runs)
        data = self.c.s3_get(key)
        same = hashlib.sha256(data).hexdigest() == self.gcs["sha256"]
        try:
            cols, row = find_in_parquet(data, event_id)
        except Exception:  # noqa: BLE001 - un objeto que no es Parquet
            cols, row = [], None
        if row:
            self._check("s3_raw", cols, row)
        else:
            self.identity["s3_raw"] = ["el event_id no está en el objeto de S3"]
        return self._stage("s3_raw", as_dt(head.get("LastModified")) or self.now(),
                           f"{key} ({'mismos bytes que enriched/' if same else 'BYTES DISTINTOS de enriched/'})",
                           object=key, same_bytes_as_gcs=same, exporter_runs=runs) and bool(row) and same

    def _lakehouse(self, event_id: str, day: str) -> bool:
        run_id = self._workflow_run()
        if not run_id:
            self._stage("trusted", None, "no se pudo arrancar ni encontrar una corrida del workflow")
            return False
        def finished():
            r = self.c.workflow_run(run_id)
            return r if r.get("Status") in ("COMPLETED", "STOPPED", "ERROR") else None

        final = self._wait("trusted", finished, POLL["workflow"], self.cfg.timeouts["trusted"] + self.cfg.timeouts["curated"]) or {}
        jobs = final.get("jobs") or {}
        ok = True
        for stage, job, db, sql in (
            ("trusted", JOB_TRUSTED, "ta_trusted",
             f"SELECT * FROM ta_trusted.posts WHERE source = '{self.cfg.source}' AND dt = '{day}' AND event_id = '{event_id}'"),
            ("curated", JOB_CURATED, "ta_curated",
             f"SELECT * FROM ta_curated.fact_posts WHERE dt = '{day}' AND event_id = '{event_id}'"),
        ):
            info = jobs.get(job) or {}
            if info.get("state") != "SUCCEEDED":
                self._stage(stage, None, f"{job}: {info.get('state') or final.get('Status') or 'sin terminar'} (corrida {run_id})",
                            workflow_run=run_id)
                return False
            cols, rows = self.c.athena(sql, db)
            if not rows:
                self._stage(stage, None, f"{job} terminó pero Athena no ve el event_id", workflow_run=run_id)
                return False
            self._check(stage, cols, rows[0])
            ok = self._stage(stage, as_dt(info.get("completed")) or self.now(), f"{job} SUCCEEDED; Athena ve la fila",
                             workflow_run=run_id) and ok
        return ok

    def _workflow_run(self) -> str | None:
        """Una corrida que empezó **después** de que el objeto llegó a S3: la propia (si ``trigger``) o la del horario."""
        s3_at = self.times["s3_raw"]
        deadline = self.now() + timedelta(seconds=self.cfg.timeouts["trusted"])
        while self.now() < deadline:
            runs = self.c.workflow_runs()
            after = [r for r in runs if (as_dt(r.get("StartedOn")) or s3_at) >= s3_at]
            if after:
                return after[0]["RunId"]
            running = [r for r in runs if r.get("Status") in ("RUNNING", "STOPPING")]
            if self.cfg.trigger and not running:
                try:
                    return self.c.start_workflow()
                except Busy:
                    pass
            self.sleep(POLL["workflow"])
        return None

    def _finish(self, report: dict[str, Any], ok: bool, reason: str | None, code: int | None = None) -> dict[str, Any]:
        hashes = set(self.hashes.values())
        identity_bad = {k: v for k, v in self.identity.items() if v}
        same_user = len(hashes) <= 1
        report.update(
            stages=self.stages,
            identity={"checked": sorted(self.identity), "problems": identity_bad, "same_user_hash": same_user,
                      "stages_with_user_hash": len(self.hashes)},
            total_s=round((self.times[self.stages[-1]["stage"]] - self.times["published"]).total_seconds(), 1)
            if self.stages and self.stages[-1]["stage"] in self.times and "published" in self.times else None,
            finished_at=iso(self.now()),
        )
        ok = ok and not identity_bad and same_user and len(self.stages) == len(STAGES)
        report["ok"] = ok
        report["exit_code"] = 0 if ok else (code or 1)
        if reason and not ok:
            report["reason"] = reason
        self.log({"event": "smoke_done", "ok": ok, "event_id": report.get("event_id"), "total_s": report["total_s"],
                  "reason": report.get("reason")})
        return report


def render_md(report: dict[str, Any]) -> str:
    """Resumen en Markdown para el informe (sin ``user_hash`` ni textos)."""
    lines = [f"# Prueba de humo E2E · {report.get('run_id')}", "",
             f"- Resultado: **{'OK' if report.get('ok') else 'FALLA'}**" + (f" ({report['reason']})" if report.get("reason") else ""),
             f"- Centinela: `{report.get('event_id')}` ({report.get('source')}, `{report.get('generator')}`), publicado "
             f"{report.get('published_at')}",
             f"- Total publicación → `fact_posts`: {report.get('total_s')} s",
             f"- Identidad: sin `user_id`/`handle` en {', '.join(report.get('identity', {}).get('checked', []))}; mismo "
             f"`user_hash` en {report.get('identity', {}).get('stages_with_user_hash')} etapas: "
             f"{'sí' if report.get('identity', {}).get('same_user_hash') else 'NO'}", ""]
    if report.get("warmup"):
        w = report["warmup"]
        lines += [f"- Calentamiento (`{w.get('generator')}`, Trusted lo descarta): {w.get('latency_s')} s hasta BigQuery", ""]
    lines += ["| Etapa | Desde | Latencia (s) | Objetivo (s) | Desde la publicación (s) | OK |", "|---|---|---|---|---|---|"]
    for s in report.get("stages", []):
        lines.append(f"| {s['stage']} | {s['from']} | {s['latency_s']} | {s['objective_s']} | {s['since_publish_s']} | "
                     f"{'✅' if s['ok'] else '❌'} |")
    return "\n".join(lines) + "\n"
