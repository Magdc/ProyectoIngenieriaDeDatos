"""Prueba de humo (Lote 5.4) contra un pipeline falso: Pub/Sub, BigQuery, GCS, exportador, S3, Glue y Athena en memoria."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from smoke import cli, e2e

T0 = datetime(2026, 10, 27, 15, 0, 0, tzinfo=timezone.utc)
HASH = "ab" * 32


class Clock:
    def __init__(self):
        self.t = T0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)


def parquet(rows, extra_cols=()):
    cols = {"event_id": [r["event_id"] for r in rows], "user_hash": [HASH] * len(rows),
            "payload": [json.dumps({"kind": "comment"})] * len(rows)}
    for c in extra_cols:
        cols[c] = ["x"] * len(rows)
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


class FakePipeline:
    """Cada etapa aparece ``delays[etapa]`` segundos después de la anterior (``None`` = nunca)."""

    def __init__(self, clock, *, delays=None, active=True, leak=None, s3_bytes_differ=False, busy_once=False, job_state="SUCCEEDED"):
        self.clock, self.active, self.leak = clock, active, leak
        self.delays = {"bigquery": 5, "gcs": 400, "s3": 30, "trusted": 150, "curated": 60, **(delays or {})}
        self.s3_bytes_differ, self.busy, self.job_state = s3_bytes_differ, busy_once, job_state
        self.published, self.exporter_runs, self.started = {}, 0, None
        self.gcs_name = None

    def dataflow_active(self):
        return ["ta-stream-dev-20261027"] if self.active else []

    def publish(self, topic, data, attributes):
        env = json.loads(data)
        assert attributes["event_id"] == env["event_id"] and topic == f"ta-raw-{env['source']}"
        self.published[env["event_id"]] = (self.clock.now(), env)
        return f"m{len(self.published)}"

    def _since(self, eid):
        return (self.clock.now() - self.published[eid][0]).total_seconds()

    def bq_rows(self, event_id, day):
        d = self.delays["bigquery"]
        if event_id not in self.published or d is None or self._since(event_id) < d:
            return []
        cols = {"event_id": event_id, "user_hash": HASH, "payload": {"kind": "comment"}, "source": "reddit"}
        if self.leak == "bigquery":
            cols["handle"] = "u/x"
        return [cols]

    def _smoke_id(self):
        return next(eid for eid, (_, env) in self.published.items() if env["generator"] == e2e.SMOKE_GENERATOR)

    def _gcs_at(self):
        return self.published[self._smoke_id()][0] + timedelta(seconds=self.delays["gcs"])

    def gcs_list(self, prefix):
        if self.delays["gcs"] is None or self.clock.now() < self._gcs_at():
            return []
        at = self._gcs_at()
        name = f"enriched/source=reddit/dt={at:%Y-%m-%d}/hr={at:%H}/part-{at:%Y%m%dT%H%M%S}Z-p000.parquet"
        self.gcs_name = name
        other = f"enriched/source=reddit/dt={at:%Y-%m-%d}/hr={at:%H}/vacio.parquet"
        return [b for b in ({"name": other, "created": at}, {"name": name, "created": at}) if b["name"].startswith(prefix)]

    def gcs_get(self, name):
        if name.endswith("vacio.parquet"):
            return parquet([{"event_id": "otro"}])
        return parquet([{"event_id": self._smoke_id()}], extra_cols=("user_id",) if self.leak == "gcs" else ())

    def run_exporter(self):
        self.exporter_runs += 1
        self.clock.sleep(60)
        self.exported_at = self.clock.now() - timedelta(seconds=60) + timedelta(seconds=self.delays["s3"] or 0)
        return {"ok": True, "seconds": 60}

    def s3_head(self, key):
        if self.delays["s3"] is None or not self.exporter_runs or key != "raw/origin=stream/" + self.gcs_name[len("enriched/"):]:
            return None
        return {"LastModified": self.exported_at}

    def s3_get(self, key):
        data = self.gcs_get(self.gcs_name)
        return data + b"x" if self.s3_bytes_differ else data

    def workflow_runs(self):
        return [{"RunId": "wr_viejo", "Status": "COMPLETED", "StartedOn": T0 - timedelta(hours=1)}] + (
            [{"RunId": "wr_1", "Status": "RUNNING", "StartedOn": self.started}] if self.started else [])

    def start_workflow(self):
        if self.busy:
            self.busy = False
            raise e2e.Busy("ConcurrentRunsExceededException")
        self.started = self.clock.now()
        return "wr_1"

    def workflow_run(self, run_id):
        el = (self.clock.now() - self.started).total_seconds()
        t_done = self.started + timedelta(seconds=self.delays["trusted"])
        c_done = t_done + timedelta(seconds=self.delays["curated"])
        done = el >= self.delays["trusted"] + self.delays["curated"]
        return {"RunId": run_id, "Status": "COMPLETED" if done else "RUNNING",
                "jobs": {e2e.JOB_TRUSTED: {"state": self.job_state if done else "RUNNING", "completed": t_done},
                         e2e.JOB_CURATED: {"state": "SUCCEEDED" if done else "RUNNING", "completed": c_done}}}

    def athena(self, sql, database):
        eid = self._smoke_id()
        cols = ["event_id", "user_hash"] + (["text", "payload"] if database == "ta_trusted" else ["date_key"])
        return cols, [{"event_id": eid, "user_hash": HASH, "payload": json.dumps({"kind": "comment"})}]


def run_smoke(clock, pipe, **cfg):
    events = []
    report = e2e.Smoke(pipe, e2e.Config(seq=1761577200, **cfg), now=clock.now, sleep=clock.sleep, log=events.append).run()
    return report, events


def test_eventos_cumplen_el_contrato_y_el_calentamiento_lo_descarta_trusted():
    from lakehouse.glue_jobs.raw_to_trusted import SENTINEL_PREFIX

    em = e2e.sentinel_emission("reddit", 1761577200, T0)
    env = em.envelope
    assert e2e.envelope_problems(env) is None and env["generator"] == e2e.SMOKE_GENERATOR
    assert not env["generator"].startswith(SENTINEL_PREFIX) and len(em.messages) == 1
    assert env["event_id"] == e2e.sentinel_emission("reddit", 1761577200, T0).envelope["event_id"]  # determinista
    warm = e2e.warmup_envelope("smoke-x", 1761577200, T0.timestamp())
    assert e2e.envelope_problems(warm) is None and warm["generator"].startswith(SENTINEL_PREFIX)
    assert warm["event_id"] != env["event_id"]


def test_corrida_completa_ok():
    clock = Clock()
    pipe = FakePipeline(clock)
    report, events = run_smoke(clock, pipe)
    assert report["ok"] and report["exit_code"] == 0, report
    assert [s["stage"] for s in report["stages"]] == list(e2e.STAGES)
    lat = {s["stage"]: s["latency_s"] for s in report["stages"]}
    # trusted cuenta desde que el objeto llegó a S3 (30 s después de arrancar el exportador): + 30 s hasta que el
    # exportador termina y arranca el workflow, + 150 s del job.
    assert lat["bigquery"] <= 7 and lat["gcs_enriched"] == 400 and lat["trusted"] == 180 and lat["curated"] == 60
    assert report["identity"] == {"checked": ["bigquery", "curated", "gcs_enriched", "s3_raw", "trusted"], "problems": {},
                                  "same_user_hash": True, "stages_with_user_hash": 5}
    assert report["warmup"]["ok"] and report["warmup"]["generator"] == e2e.WARMUP_GENERATOR
    assert pipe.exporter_runs == 1 and len(pipe.published) == 2
    assert {e["event"] for e in events} >= {"smoke_warmup", "smoke_published", "smoke_stage", "smoke_done"}
    assert HASH not in json.dumps(report) and HASH not in e2e.render_md(report)


def test_falla_si_una_etapa_pasa_su_objetivo():
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, delays={"bigquery": 90}))
    bq = report["stages"][0]
    assert not report["ok"] and bq["stage"] == "bigquery" and not bq["ok"] and bq["latency_s"] >= 90
    assert len(report["stages"]) == 5  # sigue midiendo las demás


def test_falla_si_el_evento_no_llega_a_gcs():
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, delays={"gcs": None}), warmup=False)
    assert not report["ok"] and report["stages"][-1]["stage"] == "gcs_enriched" and report["stages"][-1]["available_at"] is None
    assert report["reason"] == "una etapa no llegó"


@pytest.mark.parametrize("leak,stage", [("bigquery", "bigquery"), ("gcs", "gcs_enriched")])
def test_detecta_identidad_en_claro(leak, stage):
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, leak=leak), warmup=False)
    assert not report["ok"] and stage in report["identity"]["problems"]


def test_bytes_distintos_en_s3_y_workflow_ocupado():
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, s3_bytes_differ=True), warmup=False)
    s3 = [s for s in report["stages"] if s["stage"] == "s3_raw"][0]
    assert not report["ok"] and s3["same_bytes_as_gcs"] is False
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, busy_once=True), warmup=False)
    assert report["ok"], report


def test_job_de_trusted_fallido():
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, job_state="FAILED"), warmup=False)
    assert not report["ok"] and "FAILED" in report["stages"][-1]["detail"]


def test_sin_dataflow_sale_con_2():
    clock = Clock()
    report, _ = run_smoke(clock, FakePipeline(clock, active=False))
    assert report["exit_code"] == 2 and "Dataflow apagado" in report["reason"] and report["stages"] == []


def test_utilidades():
    assert e2e.identity_problems(["event_id", "user_id"], {"user_hash": "x", "payload": '{"a": {"handle": 1}}'}) == [
        "columna user_id", "payload.handle", "user_hash no es hex de 64"]
    assert e2e.identity_problems(["user_hash"], {"user_hash": HASH, "payload": {"kind": "comment"}}) == []
    assert e2e.hour_prefixes("enriched/source=reddit/", T0 - timedelta(minutes=10), T0 + timedelta(minutes=70)) == [
        "enriched/source=reddit/dt=2026-10-27/hr=14/", "enriched/source=reddit/dt=2026-10-27/hr=15/",
        "enriched/source=reddit/dt=2026-10-27/hr=16/"]
    cols, row = e2e.find_in_parquet(parquet([{"event_id": "a"}, {"event_id": "b"}]), "b")
    assert row["event_id"] == "b" and "user_hash" in cols
    assert e2e.find_in_parquet(parquet([{"event_id": "a"}]), "z")[1] is None


def test_cli_plan_y_run_con_informe(tmp_path, capsys):
    assert cli.main(["plan", "--seq", "1761577200"]) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["contract"] == "ok" and out["warmup_contract"] == "ok" and out["generator"] == e2e.SMOKE_GENERATOR
    clock = Clock()
    pipe = FakePipeline(clock)
    code = cli.main(["run", "--seq", "1761577200", "--gcp-project", "p", "--out", str(tmp_path)],
                    clients_factory=lambda *a, **k: pipe, now=clock.now, sleep=clock.sleep)
    assert code == 0
    report = json.loads((tmp_path / "smoke_e2e_5_4.json").read_text(encoding="utf-8"))
    md = (tmp_path / "smoke_e2e_5_4.md").read_text(encoding="utf-8")
    assert report["ok"] and report["event_id"] in md and "| curated |" in md
    digest = hashlib.sha256(json.dumps(report).encode()).hexdigest()
    assert len(digest) == 64
