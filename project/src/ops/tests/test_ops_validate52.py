"""Pruebas del validador del Lote 5.2 con un ``run_dq.py``, un S3 y un BigQuery falsos (sin red ni Great Expectations)."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ops import validate52 as v

NOW = datetime(2026, 10, 27, 15, 0, 0, tzinfo=timezone.utc)
BUCKET = v.DEFAULT_BUCKET
RUN_ID = "dq-20261027T150000Z-abc123"
KPIS = [{"layer": "trusted", "origin": "stream", "kpi": k, "rule": r, "observed": o, "target": t, "passed": True}
        for k, r, o, t in (("completitud", "DQ-05", 1.0, "≥ 0.95"), ("unicidad", "DQ-49", 1.0, "≥ 0.99"),
                           ("duplicados", "DQ-48", 0.0, "< 0.01"), ("anonimizacion", "DQ-02", 1.0, "= 1"))]


def done_event(folder, **kw):
    base = {"event": "dq_done", "folder": str(folder), "run_id": RUN_ID, "ok": True, "rows": 239, "passed": 238,
            "errors_failed": [], "warnings_failed": ["DQ-63[origin=stream] ta_raw.posts_raw"], "kpis": KPIS,
            "suites": {"ge_raw_posts": {"checks": 96}}, "duration_s": 81.4, "great_expectations": "1.24.1", "skipped": [],
            "nlp": {"evaluated": 109365, "accuracy": 0.9197, "macro_f1": 0.9167},
            "inputs": {"raw": {"rows": {"stream": 171637, "historical": 50000}}, "trusted": {"rows": {"stream": 171629, "historical": 50000}},
                       "curated": {"rows": {"fact_posts": 221629}}}}
    base.update(kw)
    return json.dumps(base)


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err)


class Fake:
    """``run_dq.py``, ``aws`` y ``uv`` falsos."""

    def __init__(self, folder, *, done=None, sync_down=(5, 0), pytest_out=None, gx="1.24.1", published=True, s3_keys=None):
        self.folder, self.done, self.sync_down, self.pytest_out, self.gx = folder, done, list(sync_down), pytest_out or {}, gx
        self.published, self.s3_keys = published, s3_keys
        self.calls = []
        self.latest = {"run_id": RUN_ID, "ok": True, "rows": 239, "passed": 238, "errors_failed": [], "warnings_failed": []}

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        text = " ".join(cmd)
        if "-c" in cmd and "great_expectations" in text:
            if "json.dumps" in text:
                return R(0, json.dumps({"gx": self.gx, "pandas": "3.1.0", "pyarrow": "25.0.1"}))
            return R(0, self.gx + "\n")
        if cmd[0].endswith("uv"):
            return R(0, "ok")
        if "run_dq.py" in text and " sync" in text:
            down = self.sync_down.pop(0) if self.sync_down else 0
            return R(0, "\n".join(json.dumps({"event": "dq_sync", "layer": lay, "objects": 10, "downloaded": down, "failed": 0})
                                  for lay in ("raw", "trusted", "curated")))
        if "run_dq.py" in text and " run" in text:
            out = [self.done or done_event(self.folder)]
            if "--publish" in cmd and self.published:
                out += [json.dumps({"event": "dq_published", "target": "s3", "objects": 21}),
                        json.dumps({"event": "dq_published", "target": "bigquery", "rows": 239, "errors": []})]
            return R(0, "\n".join(out))
        if "pytest" in text:
            target = next((c for c in cmd if c.startswith(("src/", "contracts/"))), "")
            default = {"src/dq": v.B1_EXPECTED if cmd[0].endswith("ta-gx/bin/python") else v.B3_EXPECTED,
                       "src/ops/tests/test_ops_validate52.py": v.B2_EXPECTED, "contracts/tests": 18, "src/quality": v.B5_EXPECTED}
            return R(0, self.pytest_out.get(target, f"{default.get(target, 1)} passed in 1s"))
        if cmd[:3] == ["aws", "s3api", "list-objects-v2"]:
            prefix = cmd[cmd.index("--prefix") + 1]
            n = self.s3_keys if self.s3_keys is not None else 20
            return R(0, json.dumps({"Contents": [{"Key": f"{prefix}f{i}.json", "Size": 1} for i in range(n)]}))
        if cmd[:3] == ["aws", "s3", "cp"]:
            return R(0, json.dumps(self.latest)) if self.latest else R(1, "", "404")
        return R(0, "")

    def count(self, text):
        return sum(1 for c in self.calls if text in " ".join(c))


def bq_fake(rows=None, fail=False):
    def query(sql, params):
        if fail:
            raise RuntimeError("sin credenciales")
        if "GROUP BY layer" in sql:
            assert params["run_id"] == RUN_ID and "dq_results" in sql
            return rows if rows is not None else [{"layer": "curated", "n": 31, "ok": 31, "kpi_rows": 5},
                                                  {"layer": "raw", "n": 109, "ok": 108, "kpi_rows": 30},
                                                  {"layer": "trusted", "n": 99, "ok": 99, "kpi_rows": 28}]
        return [{"run_id": RUN_ID, "n": 239, "ok": 238}]
    return query


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    monkeypatch.setattr(v, "EVIDENCE_DIR", tmp_path / "evidencia")
    monkeypatch.setattr(v, "NLP_EVIDENCE", tmp_path / "evidencia" / "precision_nlp_5_2.md")
    folder = tmp_path / "dq_runs" / RUN_ID
    (folder / "ge").mkdir(parents=True)
    (folder / "summary.json").write_text(json.dumps({"run_id": RUN_ID, "ok": True}))
    (folder / "ge" / "ge_raw_posts__stream.json").write_text(json.dumps({"results": [{"kwargs": {"regex": "^[0-9a-f]{64}$"}}]}))
    (folder / "nlp_precision.md").write_text("# Precisión del NLP\n\n**Accuracy: 92.0 %**\n")
    return tmp_path, folder


def run_part(fake, *argv, sandbox_dir, confirm=None, bq=None):
    lines, state = [], {"t": NOW}

    def sleep(s):
        state["t"] += timedelta(seconds=s)

    path = sandbox_dir / "evidencia" / "validacion_5_2.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    args = list(argv) + ["--cache", str(sandbox_dir / "cache"), "--gx-python", "/root/.venvs/ta-gx/bin/python"]
    code = v.main(args, runner=fake, printer=lines.append, confirm=confirm, sleep=sleep, now=lambda: state["t"],
                  evidence_path=path, bq=bq or bq_fake())
    return code, lines, json.loads(path.read_text())


def steps(ev, part):
    return {s["id"]: s for s in ev["parts"][part]["steps"]}


# --- utilidades -------------------------------------------------------------------------------------


def test_eventos_privacidad_y_resumen(sandbox):
    tmp, folder = sandbox
    assert v.events('x\n{"event": "dq_sync", "layer": "raw"}\n{"event": "otro"}', "dq_sync") == [{"event": "dq_sync", "layer": "raw"}]
    assert v.privacy_problems(folder) == []  # el patrón ^[0-9a-f]{64}$ no es un user_hash
    (folder / "x.json").write_text('{"u": "' + "a" * 64 + '", "e": "12345678-1234-1234-1234-123456789abc"}')
    probs = v.privacy_problems(folder)
    assert len(probs) == 2 and "hex de 64" in probs[0] and "UUID" in probs[1]
    assert "239 comprobaciones, 238 OK" in v.summarize_done(json.loads(done_event(folder)))


def test_sql_de_bigquery_usa_la_particion_y_el_run_id():
    assert "@run_id" in v.COUNT_SQL and "DATE(run_ts) = PARSE_DATE('%Y-%m-%d', @day)" in v.COUNT_SQL and "{table}" in v.COUNT_SQL
    assert "STARTS_WITH(run_id, 'dq-')" in v.LAST_RUNS_SQL


def test_gcp_project_de_la_opcion_o_de_tfvars(sandbox):
    ctx = v.Ctx(runner=Fake(sandbox[1]), options=v.build_parser().parse_args(["status", "--gcp-project", "p-1"]), printer=print)
    assert v.gcp_project(ctx) == "p-1"


# --- A, V y B ----------------------------------------------------------------------------------------


def test_preflight_ok_y_venv_viejo(sandbox):
    tmp, folder = sandbox
    for lay in ("raw", "trusted/posts", "curated"):
        (tmp / "cache" / "s3" / BUCKET / lay).mkdir(parents=True)
    code, _, ev = run_part(Fake(folder), "preflight", sandbox_dir=tmp)
    s = steps(ev, "A")
    assert code == 0 and s["A4"]["status"] == "OK" and "1.24.1" in s["A4"]["detail"] and s["A5"]["status"] == "OK"
    assert ev["lote"] == "5.2" and ev["hu"] == ["HU-032", "HU-033"]
    code, _, ev = run_part(Fake(folder, gx="1.10.0"), "preflight", sandbox_dir=tmp)
    assert code == 1 and "validate_5_2.py venv" in steps(ev, "A")["A4"]["detail"]


def test_preflight_sin_cache_avisa(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder), "preflight", sandbox_dir=tmp)
    assert code == 0 and steps(ev, "A")["A5"]["status"] == "AVISO" and "sync" in steps(ev, "A")["A5"]["detail"]


def test_venv_instala_los_requisitos(sandbox):
    tmp, folder = sandbox
    fake = Fake(folder)
    code, _, ev = run_part(fake, "venv", sandbox_dir=tmp)
    s = steps(ev, "V")
    assert code == 0 and s["V2"]["status"] == "OK" and s["V3"]["status"] == "OK"
    assert fake.count("pip install --python") == 1 and fake.count("requirements.txt") == 1


def test_tests_ok_y_conteo_distinto(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder), "tests", sandbox_dir=tmp)
    assert code == 0 and len(steps(ev, "B")) == 5 and all(x["status"] == "OK" for x in steps(ev, "B").values())
    code, _, ev = run_part(Fake(folder, pytest_out={"contracts/tests": "17 passed, 1 skipped"}), "tests", sandbox_dir=tmp)
    assert code == 1 and steps(ev, "B")["B4"]["status"] == "FALLA"


# --- S, L y P ----------------------------------------------------------------------------------------


def test_sync_y_segundo_sync_sin_descargas(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder), "sync", sandbox_dir=tmp)
    assert code == 0 and steps(ev, "S")["S2"]["status"] == "OK" and "curated: 10 objetos" in steps(ev, "S")["S1"]["detail"]
    code, _, ev = run_part(Fake(folder, sync_down=(5, 3)), "sync", sandbox_dir=tmp)
    assert code == 0 and steps(ev, "S")["S2"]["status"] == "AVISO"


def test_local_ok_copia_el_informe_del_nlp(sandbox):
    tmp, folder = sandbox
    code, lines, ev = run_part(Fake(folder), "local", sandbox_dir=tmp)
    s = steps(ev, "L")
    assert code == 0, "\n".join(lines)
    assert [s[k]["status"] for k in ("L1", "L2", "L3", "L4", "L5", "L6")] == ["OK"] * 6
    assert "accuracy 0.9197" in s["L5"]["detail"] and v.NLP_EVIDENCE.read_text().startswith("# Precisión")
    assert "221637 filas de Raw" in s["L6"]["detail"]


def test_local_con_errores_kpi_y_avisos_nuevos(sandbox):
    tmp, folder = sandbox
    bad = done_event(folder, ok=False, errors_failed=["DQ-49[origin=stream] ta_trusted.posts"],
                     warnings_failed=["DQ-63[origin=stream] ta_raw.posts_raw", "DQ-60[origin=stream] ta_trusted.posts"],
                     kpis=[dict(KPIS[1], observed=0.98, passed=False)])
    code, _, ev = run_part(Fake(folder, done=bad), "local", sandbox_dir=tmp)
    s = steps(ev, "L")
    assert code == 1 and s["L1"]["status"] == "FALLA" and s["L2"]["status"] == "FALLA" and s["L3"]["status"] == "AVISO"
    assert s["L3"]["data"]["unexpected"] == ["DQ-60[origin=stream] ta_trusted.posts"]


def test_local_detecta_datos_de_filas_en_la_salida(sandbox):
    tmp, folder = sandbox
    (folder / "dq_results.json").write_text('[{"details": {"x": "' + "b" * 64 + '"}}]')
    code, _, ev = run_part(Fake(folder), "local", sandbox_dir=tmp)
    assert code == 1 and steps(ev, "L")["L4"]["status"] == "FALLA"


def test_local_sin_cache_falla(sandbox):
    tmp, folder = sandbox
    err = json.dumps({"event": "dq_error", "error": "faltan en la caché …: ['raw'] (corra antes: run_dq.py sync)"})
    code, _, ev = run_part(Fake(folder, done=err), "local", sandbox_dir=tmp)
    assert code == 1 and "run_dq.py sync" in steps(ev, "L")["L1"]["detail"]


def test_local_cifras_distintas_avisa(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder, done=done_event(folder, rows=250)), "local", sandbox_dir=tmp)
    assert code == 0 and steps(ev, "L")["L6"]["status"] == "AVISO" and "rows 250" in steps(ev, "L")["L6"]["detail"]


def test_publish_s3_y_bigquery(sandbox):
    tmp, folder = sandbox
    fake = Fake(folder)
    code, lines, ev = run_part(fake, "publish", "--yes", sandbox_dir=tmp)
    s = steps(ev, "P")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert "quality/2026-10-27/ge/" + RUN_ID in s["P4"]["detail"] and "consultadas 239" in s["P5"]["detail"]
    cmd = next(c for c in fake.calls if "run_dq.py" in " ".join(c))
    assert cmd[cmd.index("--publish") + 1] == "s3,bq" and "--gcp-project" in cmd


def test_publish_no_confirmado_y_filas_que_no_cuadran(sandbox):
    tmp, folder = sandbox
    fake = Fake(folder)
    code, _, ev = run_part(fake, "publish", sandbox_dir=tmp, confirm=lambda q: False)
    assert code == 0 and steps(ev, "P")["P1"]["status"] == "AVISO" and fake.count("run_dq.py") == 0
    code, _, ev = run_part(Fake(folder), "publish", "--yes", sandbox_dir=tmp, bq=bq_fake(rows=[{"layer": "raw", "n": 100}]))
    assert code == 1 and steps(ev, "P")["P5"]["status"] == "FALLA"
    code, _, ev = run_part(Fake(folder, s3_keys=3), "publish", "--yes", sandbox_dir=tmp)
    assert code == 1 and steps(ev, "P")["P4"]["status"] == "FALLA"


def test_publish_sin_credenciales_de_bigquery(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder), "publish", "--yes", sandbox_dir=tmp, bq=bq_fake(fail=True))
    assert code == 1 and "sin credenciales" in steps(ev, "P")["P5"]["detail"]


def test_publish_sin_proyecto_de_gcp(sandbox, monkeypatch):
    tmp, folder = sandbox
    monkeypatch.setattr(v, "gcp_project", lambda ctx: None)
    code, _, ev = run_part(Fake(folder), "publish", "--yes", sandbox_dir=tmp)
    assert code == 1 and steps(ev, "P")["P0"]["status"] == "FALLA"


# --- status y CLI ------------------------------------------------------------------------------------


def test_status_con_y_sin_corridas(sandbox):
    tmp, folder = sandbox
    code, _, ev = run_part(Fake(folder), "status", sandbox_dir=tmp)
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "OK" and RUN_ID in s["S2"]["detail"]
    fake = Fake(folder)
    fake.latest = None
    code, _, ev = run_part(fake, "status", sandbox_dir=tmp, bq=bq_fake(fail=True))
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "AVISO" and "publish" in s["S1"]["detail"] and s["S2"]["status"] == "AVISO"


def test_fuera_de_wsl_sale_con_2(sandbox, monkeypatch):
    tmp, folder = sandbox
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    code = v.main(["local"], runner=Fake(folder), printer=lines.append, evidence_path=tmp / "x.json")
    assert code == 2 and "Windows" in lines[-1]


def test_main_por_script():
    path = v.PROJECT / "scripts" / "validate_5_2.py"
    spec = importlib.util.spec_from_file_location("validate_5_2_script", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main is v.main and sys.modules["ops.validate52"] is v
    assert Path(v.RUN_DQ).exists() and Path(v.REQUIREMENTS).read_text().count("great_expectations==" + v.GX_VERSION) == 1
