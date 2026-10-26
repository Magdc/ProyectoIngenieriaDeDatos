"""Pruebas del validador del Lote 5.1 con un ejecutor falso (sin red, Docker ni nube).

Los informes que "devuelve" ``profile_data.py`` son reales: salen del profiler sobre un lake sintético
pequeño, así que las comprobaciones de C y D se prueban contra la forma verdadera del JSON.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ops import services
from ops import validate51 as v
from quality import layers as L
from quality.profiler import profile_layer
from quality.testing import build_trusted, make_rows, write_raw_layer

AS_OF = datetime(2026, 10, 26, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc)


# --- ejecutor falso --------------------------------------------------------------------------


class FakeRunner:
    def __init__(self, tools=("aws",)):
        self.handlers: list[tuple] = []
        self.calls: list[list[str]] = []
        self.tools = set(tools)

    def on(self, pattern, out="", code=0):
        self.handlers.append((pattern, out if callable(out) else v.Result(code, out, "")))
        return self

    def which(self, name):
        return f"/usr/bin/{name}" if name in self.tools else None

    def run(self, cmd, *, cwd=None, env=None, timeout=900, stream=False):
        self.calls.append([str(c) for c in cmd])
        joined = " ".join(str(c) for c in cmd)
        for pattern, result in self.handlers:
            if (callable(pattern) and pattern(cmd)) or (isinstance(pattern, str) and pattern in joined):
                return result(cmd) if callable(result) else result
        return v.Result(0, "", "")

    def joined(self):
        return [" ".join(c) for c in self.calls]


def arg(cmd, name):
    cmd = [str(c) for c in cmd]
    return cmd[cmd.index(name) + 1] if name in cmd else None


def writes(data, out="ok", code=0, flag="--json-out"):
    def handler(cmd):
        path = arg(cmd, flag)
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(data(cmd) if callable(data) else data, default=str), encoding="utf-8")
        md = arg(cmd, "--md-out")
        if md:
            Path(md).write_text("# informe\n", encoding="utf-8")
        return v.Result(code, out, "")
    return handler


def pytest_out(passed, skipped=0, failed=0):
    parts = [f"{passed} passed"] + ([f"{skipped} skipped"] if skipped else []) + ([f"{failed} failed"] if failed else [])
    return ", ".join(parts) + " in 1.00s"


class FakeServices:
    """Ejecutor con la firma de ``ops.services.Runner`` (lo usa ``services.status``)."""

    def __init__(self, simulator_url=""):
        self.simulator_url = simulator_url
        self.calls = []

    def __call__(self, args, *, capture=True, env=None, check=False):
        self.calls.append(list(args))
        joined = " ".join(args)
        if joined.startswith("docker info"):
            return services.Result(1, "", "no docker")
        if "config get-value project" in joined:
            return services.Result(0, "proyecto-x\n")
        if "run services describe" in joined:
            return services.Result(0 if self.simulator_url else 1, self.simulator_url)
        if "dataflow jobs list" in joined:
            return services.Result(0, "[]")
        return services.Result(0, "")


# --- datos: informes reales de un lake sintético pequeño ----------------------------------------


@pytest.fixture(scope="module")
def reports(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    rows = []
    for s in L.SOURCES:
        rows += make_rows("historical", s, 10, datetime(2026, 6, 1, tzinfo=timezone.utc), step=timedelta(hours=3))
        rows += make_rows("stream", s, 10, datetime(2026, 10, 23, 5, tzinfo=timezone.utc), step=timedelta(minutes=2))
    write_raw_layer(root, rows)
    raw = profile_layer(root, "raw", as_of=AS_OF, source_uri="s3://lake/raw/ (caché local)")
    t_out = tmp_path_factory.mktemp("trusted")
    t_info = build_trusted(root, t_out)
    trusted = profile_layer(t_out, "trusted", as_of=AS_OF, raw_report=raw)
    d_out = tmp_path_factory.mktemp("defects")
    d_info = build_trusted(root, d_out, defects=True)
    defects = profile_layer(d_out, "trusted", as_of=AS_OF)
    t_info["out_root"], d_info["out_root"] = str(t_out), str(d_out)
    return {"raw": raw, "trusted": trusted, "defects": defects, "t_info": t_info, "d_info": d_info}


SYNC = {"bucket": "lake", "objects": 60, "bytes": 600_000, "downloaded": 60, "skipped_same_size": 0, "failed": 0, "to_download": 60,
        "by_prefix": {f"origin={o}/source={s}": {"objects": 30 if o == "historical" else 30, "bytes": 1}
                      for o in ("historical", "stream") for s in ("reddit", "facebook", "instagram")}}


def ctx_main(argv, tmp_path, runner, **kw):
    lines: list[str] = []
    code = v.main(argv + ["--cache", str(tmp_path / "cache")], runner=runner, printer=lines.append, now=lambda: NOW,
                  evidence_path=tmp_path / "ev" / "validacion_5_1.json", services_runner=kw.pop("services_runner", FakeServices()),
                  **kw)
    ev = json.loads((tmp_path / "ev" / "validacion_5_1.json").read_text(encoding="utf-8"))
    return code, "\n".join(lines), ev


def steps(ev, part):
    return {s["id"]: s for s in ev["parts"][part]["steps"]}


@pytest.fixture(autouse=True)
def linux(monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: False)
    monkeypatch.delenv("TA_LAKE_BUCKET", raising=False)
    monkeypatch.delenv("GCP_PROJECT", raising=False)


def raw_runner(reports, *, sync_code=0, raw_report=None, pending="0"):
    r = FakeRunner()
    r.on("sts get-caller-identity", json.dumps({"Account": "648617469176", "Arn": "arn:aws:iam::648617469176:root"}))
    r.on(lambda c: "sync" in c and "--dry-run" in c and "raw" in c, f"s3://lake/raw/: 60 objetos (0.6 MB); por bajar {pending}, bajados 0")
    r.on(lambda c: "sync" in c and "raw" in c, writes(SYNC, code=sync_code))
    r.on(lambda c: "profile" in c and "raw" in c, writes(raw_report or reports["raw"]))
    return r


# --- A · preflight ------------------------------------------------------------------------------


def test_preflight_todo_presente(tmp_path):
    code, out, ev = ctx_main(["preflight"], tmp_path, FakeRunner())
    s = steps(ev, "A")
    assert s["A2"]["status"] == v.OK and s["A3"]["status"] == v.OK
    assert s["A4"]["status"] == v.OK, s["A4"]["detail"]
    assert s["A5"]["status"] == v.OK
    assert s["A6"]["status"] in (v.OK, v.AVISO) and s["A6"]["data"]["cache"].endswith("cache")
    assert s["A7"]["status"] == v.OK and "GCP simulador: apagado" in s["A7"]["detail"]
    assert "A0" not in s


def test_preflight_avisa_si_algo_cobra_y_sin_aws(tmp_path):
    code, out, ev = ctx_main(["preflight"], tmp_path, FakeRunner(tools=()), services_runner=FakeServices("https://sim.run.app"))
    s = steps(ev, "A")
    assert s["A7"]["status"] == v.AVISO and "off.py" in s["A7"]["detail"]
    assert s["A5"]["status"] == v.AVISO


def test_preflight_cache_dentro_del_repo_falla(tmp_path):
    lines = []
    v.main(["preflight", "--cache", str(v.PROJECT / ".cache-x")], runner=FakeRunner(), printer=lines.append,
           evidence_path=tmp_path / "ev.json", services_runner=FakeServices())
    ev = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    assert steps(ev, "A")["A6"]["status"] == v.FALLA


def test_windows_solo_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    assert v.main(["raw"], runner=FakeRunner(), printer=lines.append, evidence_path=tmp_path / "e.json") == 2
    assert "WSL" in "\n".join(lines)


# --- B · pruebas --------------------------------------------------------------------------------


def test_tests_cuenta_y_exige_cero_omitidas(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "B1_EXPECTED", 5)
    monkeypatch.setattr(v, "B2_EXPECTED", 3)
    r = FakeRunner()
    r.on("pytest src/quality", pytest_out(5)).on("test_ops_validate51.py", pytest_out(3))
    r.on("contracts/tests", pytest_out(18)).on("test_ops_services.py", pytest_out(9))
    code, out, ev = ctx_main(["tests"], tmp_path, r)
    assert code == 0 and ev["parts"]["B"]["status"] == v.OK
    r2 = FakeRunner()
    r2.on("pytest src/quality", pytest_out(4, skipped=1)).on("test_ops_validate51.py", pytest_out(3))
    r2.on("contracts/tests", pytest_out(18)).on("test_ops_services.py", pytest_out(9))
    code, out, ev = ctx_main(["tests"], tmp_path, r2)
    assert code == 1 and steps(ev, "B")["B1"]["status"] == v.FALLA


# --- C · Raw --------------------------------------------------------------------------------------


def test_raw_ok(tmp_path, reports, monkeypatch):
    monkeypatch.setattr(v, "HISTORICAL_ROWS", 30)
    monkeypatch.setattr(v, "HISTORICAL_OBJECTS", 90)
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports))
    s = steps(ev, "C")
    assert s["C1"]["status"] == v.OK and "cuenta raíz" in s["C1"]["detail"]
    assert s["C2"]["status"] == v.OK, s["C2"]["detail"]
    assert s["C3"]["status"] == v.OK
    assert s["C4"]["status"] == v.OK
    assert s["C5"]["status"] == v.OK, s["C5"]["detail"]
    assert s["C6"]["status"] == v.OK, s["C6"]["detail"]
    assert s["C7"]["status"] == v.OK, s["C7"]["detail"]
    assert s["C8"]["status"] == v.OK, s["C8"]["detail"]
    assert s["C9"]["status"] == v.OK, s["C9"]["detail"]
    assert code == 0


def test_raw_usa_el_bucket_derivado_de_la_cuenta(tmp_path, reports, monkeypatch):
    monkeypatch.setattr(v, "HISTORICAL_ROWS", 30)
    r = raw_runner(reports)
    ctx_main(["raw"], tmp_path, r)
    syncs = [j for j in r.joined() if " sync " in f" {j} "]
    assert syncs and all("--bucket ta-dev-648617469176-lake" in j for j in syncs)


def test_raw_falla_si_el_sync_falla_o_no_hay_stream(tmp_path, reports):
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports, sync_code=1))
    assert code == 1 and steps(ev, "C")["C2"]["status"] == v.FALLA


def test_raw_avisa_si_la_cache_no_quedo_completa(tmp_path, reports, monkeypatch):
    monkeypatch.setattr(v, "HISTORICAL_ROWS", 30)
    monkeypatch.setattr(v, "HISTORICAL_OBJECTS", 90)
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports, pending="3"))
    assert steps(ev, "C")["C3"]["status"] == v.AVISO


def test_raw_c6_avisa_con_otro_volumen_y_c8_detecta_user_hash(tmp_path, reports):
    leaky = json.loads(json.dumps(reports["raw"], default=str))
    leaky["leak"] = "a" * 64
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports, raw_report=leaky))
    s = steps(ev, "C")
    assert s["C6"]["status"] == v.AVISO  # 30 filas de histórico, no 50.000
    assert s["C8"]["status"] == v.FALLA and "hex de 64" in s["C8"]["detail"]
    assert code == 1


def test_raw_c5_falla_si_falta_una_columna(tmp_path, reports, monkeypatch):
    monkeypatch.setattr(v, "HISTORICAL_ROWS", 30)
    broken = json.loads(json.dumps(reports["raw"], default=str))
    for g in broken["groups"]:
        g["columns"].pop("followers", None)
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports, raw_report=broken))
    assert steps(ev, "C")["C5"]["status"] == v.FALLA


def test_raw_c6_falla_sin_anonimizacion(tmp_path, reports):
    bad = json.loads(json.dumps(reports["raw"], default=str))
    bad["summary"]["kpi"]["stream"]["anonimizacion"] = 0.99
    code, out, ev = ctx_main(["raw"], tmp_path, raw_runner(reports, raw_report=bad))
    assert steps(ev, "C")["C6"]["status"] == v.FALLA


# --- D · Trusted ----------------------------------------------------------------------------------


def trusted_runner(reports, *, objects=0, defects_report=None):
    r = raw_runner(reports)
    r.on(lambda c: "sync" in c and "trusted" in c and "--dry-run" in c, f"s3://lake/trusted/posts/: {objects} objetos (0.0 MB); por bajar {objects}")
    r.on(lambda c: "sync" in c and "trusted" in c, "ok")
    r.on(lambda c: "synth-trusted" in c and "--defects" in c, writes(reports["d_info"]))
    r.on(lambda c: "synth-trusted" in c, writes(reports["t_info"]))
    r.on(lambda c: "profile" in c and "trusted" in c and reports["d_info"]["out_root"] in c, writes(defects_report or reports["defects"]))
    r.on(lambda c: "profile" in c and "trusted" in c, writes(reports["trusted"]))
    return r


def prepare_raw_profile(tmp_path, reports):
    path = tmp_path / "ev" / v.RAW_JSON
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(reports["raw"], default=str), encoding="utf-8")


def test_trusted_simulado_mientras_no_exista(tmp_path, reports):
    prepare_raw_profile(tmp_path, reports)
    code, out, ev = ctx_main(["trusted"], tmp_path, trusted_runner(reports))
    s = steps(ev, "D")
    assert s["D1"]["status"] == v.AVISO and "Lote 4.2" in s["D1"]["detail"]
    for sid in ("D2", "D3", "D4", "D5", "D6"):
        assert s[sid]["status"] == v.OK, (sid, s[sid]["detail"])
    assert ev["parts"]["D"]["status"] == v.AVISO and code == 0
    assert all(s["D5"]["data"]["detected"].values())


def test_trusted_falla_si_un_defecto_no_se_detecta(tmp_path, reports):
    prepare_raw_profile(tmp_path, reports)
    blind = json.loads(json.dumps(reports["defects"], default=str))
    blind["duplicates"]["other_file_same_origin"] = 0
    code, out, ev = ctx_main(["trusted"], tmp_path, trusted_runner(reports, defects_report=blind))
    s = steps(ev, "D")
    assert s["D5"]["status"] == v.FALLA and "duplicate_event_id: NO detectado" in s["D5"]["detail"]
    assert code == 1


def test_trusted_sin_profiling_de_raw(tmp_path, reports):
    code, out, ev = ctx_main(["trusted"], tmp_path, trusted_runner(reports))
    assert steps(ev, "D")["D2"]["status"] == v.FALLA


def test_trusted_real_si_existe(tmp_path, reports):
    prepare_raw_profile(tmp_path, reports)
    r = trusted_runner(reports, objects=5)
    code, out, ev = ctx_main(["trusted"], tmp_path, r)
    s = steps(ev, "D")
    assert s["D1"]["status"] == v.OK and s["D2"]["status"] == v.OK and s["D3"]["status"] == v.OK
    assert s["D4"]["status"] == v.OK, s["D4"]["detail"]
    assert code == 0 and ev["parts"]["D"]["status"] == v.OK
    assert any("--raw-profile" in j for j in r.joined())


# --- clean y all ------------------------------------------------------------------------------


def test_clean_pide_confirmacion(tmp_path):
    cache = tmp_path / "cache"
    (cache / "s3").mkdir(parents=True)
    (cache / "s3" / "x.parquet").write_bytes(b"1234")
    code, out, ev = ctx_main(["clean"], tmp_path, FakeRunner(), confirm=lambda q: False)
    assert steps(ev, "clean")["L1"]["status"] == v.OMITIDO and cache.exists()
    code, out, ev = ctx_main(["clean", "--yes"], tmp_path, FakeRunner())
    assert steps(ev, "clean")["L1"]["status"] == v.OK and not cache.exists()


def test_all_corre_las_cuatro_partes(tmp_path, reports, monkeypatch):
    monkeypatch.setattr(v, "B1_EXPECTED", 1)
    monkeypatch.setattr(v, "B2_EXPECTED", 1)
    monkeypatch.setattr(v, "HISTORICAL_ROWS", 30)
    monkeypatch.setattr(v, "HISTORICAL_OBJECTS", 90)
    r = trusted_runner(reports)
    r.on(lambda c: "pytest" in c and "src/quality" in c, pytest_out(1))
    r.on(lambda c: "pytest" in c and "test_ops_validate51.py" in c, pytest_out(1))
    r.on(lambda c: "pytest" in c and "contracts/tests" in c, pytest_out(18))
    r.on(lambda c: "pytest" in c, pytest_out(9))
    code, out, ev = ctx_main(["all"], tmp_path, r)
    assert set(ev["parts"]) == {"A", "B", "C", "D"}
    assert ev["parts"]["C"]["status"] == v.OK and ev["parts"]["D"]["status"] == v.AVISO


# --- funciones puras ------------------------------------------------------------------------------


def test_ddl_columns_de_contracts():
    cols, parts = v.ddl_columns((v.PROJECT / "contracts/athena/posts.sql").read_text(encoding="utf-8"))
    assert cols[0] == "event_id" and "text_clean" in cols and parts == ["source", "dt"]


def test_doc_rule_ids():
    text = "### 5.10 Reglas\n| DQ-01 | a |\n| DQ-02 | b |\n| DQ-01 | c |\n### 5.11 X\n| DQ-99 | z |\n"
    assert v.doc_rule_ids(text) == ["DQ-01", "DQ-02"]
    assert v.doc_rule_ids("sin sección") == []


def test_evidence_privacy(tmp_path):
    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps({"nlp": "robertuito-sentiment@a2cc0f67ebd7", "n": 1}), encoding="utf-8")
    bad = tmp_path / "bad.md"
    bad.write_text("usuario u_000123 y " + "f" * 64, encoding="utf-8")
    problems, sizes = v.evidence_privacy([ok, bad, tmp_path / "no.json"])
    assert len(problems) == 2 and set(sizes) == {"ok.json", "bad.md"}


def test_pytest_counts_y_parse_json():
    assert v.pytest_counts("..\n12 passed, 2 skipped in 3s") == {"passed": 12, "failed": 0, "skipped": 2, "error": 0}
    assert v.parse_json("ruido\n{\"a\": 1}") == {"a": 1}
    assert v.parse_json("") is None


def test_coverage_problems_detecta_grupos_faltantes():
    problems = v.coverage_problems({"groups": [], "files": {}}, ["event_id"], ("historical",))
    assert any("historical/*" in p for p in problems) and any("tipos físicos" in p for p in problems)


def test_detected_defects_vacio():
    assert not any(v.detected_defects({}).values())
