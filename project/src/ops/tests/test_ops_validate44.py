"""Pruebas del validador del Lote 4.4 con un AWS, un Terraform y un job falsos (sin red, Docker ni nube).

Reutiliza el AWS falso de ``test_ops_validate43.py`` (``World``, ``FakeRunner``, ``plan``, ``table_parquet``)
y le suma las aristas: nueve tablas, el reporte de la versión 1.1 y las consultas de la parte E."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse import catalog_spec
from ops import validate41 as v41
from ops import validate43 as v43
from ops import validate44 as v

_spec = importlib.util.spec_from_file_location("t43_para_44", Path(__file__).with_name("test_ops_validate43.py"))
t43 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(t43)

BUCKET = t43.BUCKET
NOW = t43.NOW
JOB = t43.JOB  # el job real (versión 1.1): sus constantes y sus nombres de comprobaciones
EDGE_ROWS = {"edges_user_brand": 15395, "edges_hashtag_cooccurrence": 136}
TABLE_ROWS = dict(t43.TABLE_ROWS, **EDGE_ROWS)
UPDATES = sorted(v.EXPECTED_UPDATES)
_ACC = JOB.Accumulator()
CHECK_NAMES = list(JOB.conciliation_checks(_ACC, 0, JOB.build_dimensions_and_aggregates(_ACC)[0]))
COUNTS = {"fact_rows": 221629, "nlp_missing": 65263, "with_sentiment": 156366, "with_emotion": 156366, "mentions": 233395,
          "mentions_by_brand": {"pepsi": 69434, "postobon": 103436, "sprite": 60525}, "rows_without_brands": 0,
          "hashtags_distinct": 25, "rows_by_hashtag_count": {"0": 132578, "1": 55725, "2": 27700, "3": 5626},
          "cooccurrences": 44578}


def curated_keys():
    return t43.curated_keys() + [f"curated/{t}/part-00000.parquet" for t in v.EDGE_TABLES]


def make_report(run_id, wf_run_id=None, *, changed=None, outside=0, sha="a", bad_edge=False, version=v.JOB_VERSION,
                n_checks=16, trusted_expected=None):
    """Reporte del job 1.1. ``changed=None``: la primera corrida tras el 4.3 (cambian las aristas y ``outside`` más)."""
    keys = curated_keys()
    files = [{"key": k, "table": v.table_of(k), "rows": 1, "bytes": 100, "sha256": hashlib.sha256(f"{sha}{k}".encode()).hexdigest()}
             for k in keys]
    checks = {n: {"expected": 0, "observed": 0, "ok": True} for n in CHECK_NAMES[:n_checks]}
    checks["fact_rows_eq_trusted_rows"] = {"expected": 221629, "observed": 221629, "ok": True}
    if n_checks == 16:
        checks["edges_user_brand_weight_eq_mentions"] = {"expected": 233395, "observed": 233395, "ok": True}
        checks["edges_hashtag_weight_eq_pairs"] = {"expected": 44578, "observed": 44578, "ok": True}
    if bad_edge:
        checks["edges_hashtag_weight_eq_pairs"] = {"expected": 44578, "observed": 44000, "ok": False}
    edges = [k for k in keys if k.startswith(v.EDGE_PREFIX)]
    changed_keys = (edges + [k for k in keys if k not in edges][:outside]) if changed is None else keys[:changed]
    n = len(changed_keys)
    tables = {t: {"rows": rows, "files": 3 if t == "fact_posts" else 1} for t, rows in TABLE_ROWS.items()}
    return {
        "schema": "ta-trusted-to-curated-report@1", "version": version, "run_id": run_id, "workflow_run_id": wf_run_id,
        "report_key": f"quality/2026-10-27/trusted_to_curated/{run_id}.json",
        "input": {"objects": 543, "rows": 221629, "dt_min": "2026-04-27", "dt_max": "2026-10-24",
                  "trusted_rows_expected": trusted_expected, "trusted_rows_match": None if trusted_expected is None else True},
        "counts": dict(COUNTS), "tables": tables, "checks": checks, "conciliation": {"ok": not bad_edge},
        "output": {"objects": len(keys), "objects_written": n, "objects_unchanged": len(keys) - n, "objects_deleted": 0,
                   "files_changed": n, "changed_keys": changed_keys, "bytes": 18_480_344, "files": files},
    }


def done_fields(**kw):
    base = {"event": "glue_job_done", "rows_read": 221629, "fact_rows": 221629, "fact_partitions": 181, "tables": TABLE_ROWS,
            "mentions": 233395, "cooccurrences": 44578, "hashtags_distinct": 25, "nlp_missing": 65263, "objects_written": 2,
            "objects_unchanged": 187, "files_changed": 2, "objects_deleted": 0, "conciliation_ok": True, "duration_s": 4.9,
            "cpu_seconds": 7.8, "read_seconds": 3.0, "peak_rss_mb": 431.7,
            "environment": {"python": "3.9.25", "pyarrow": "7.0.0", "boto3": "1.21.21"}}
    base.update(kw)
    return base


class World(t43.World):
    """El AWS falso del 4.3 con el job 1.1: escribe las nueve tablas y responde el SQL de las aristas."""

    def __init__(self):
        super().__init__()
        self.objects.pop("curated/edges_user_brand/part-00000.parquet", None)  # antes del 4.4: aristas vacías
        self.sql.update({
            "ub": {"claves_distintas": 0, "pesos_distintos": 0, "pares": 15395, "peso_total": 233395, "menciones": 233395,
                   "marcas_en_brands": 233395},
            "links44": {k: 0 for k in ("sin_usuario", "sin_marca", "nulos_usuario_marca", "pares_repetidos_ub", "nulos_hashtags",
                                       "fuera_de_orden", "no_canonicos", "pares_repetidos_ht")},
            "ht": {"claves_distintas": 0, "pesos_distintos": 0, "pares": 136, "peso_total": 44578, "coocurrencias": 44578,
                   "pares_por_fila": 44578, "hashtags": 25},
            "count44": {"fact": 221629, "pares_ub": 15395, "peso_ub": 233395, "pares_ht": 136, "peso_ht": 44578},
        })

    def run_curated(self, job_run_id, wf_run_id=None):
        first = v.LATEST_REPORT not in self.objects
        report = make_report(wf_run_id or f"manual-{job_run_id}", wf_run_id, changed=None if first else self.rerun_changed,
                             trusted_expected=221629 if wf_run_id else None, **self.report_overrides)
        for k in curated_keys():
            self.objects[k] = "PAR1"
        if self.extra_key:
            self.objects[self.extra_key] = "PAR1"
        if self.add_placeholder_on_run:
            self.objects[f"curated/_ta_placeholder/{v.JOB}/x.json"] = "{}"
        text = json.dumps(report)
        self.objects[report["report_key"]] = text
        self.objects[v.LATEST_REPORT] = text
        if wf_run_id:
            self.props.setdefault(wf_run_id, {}).update({
                "ta_curated_report": report["report_key"], "ta_curated_rows": "221629",
                "ta_curated_files_changed": str(report["output"]["files_changed"])})
        done = done_fields(report=report["report_key"], peak_rss_mb=self.peak)
        self.logs.append((v.GLUE_OUTPUT_LOG, job_run_id, json.dumps(done)))

    def handle(self, cmd):
        if cmd[1:3] == ["glue", "start-job-run"]:
            assert t43.arg(cmd, "--job-name") == v.JOB
            if self.busy_once:
                self.busy_once = False
                return t43.err("ConcurrentRunsExceededException")
            assert json.loads(t43.arg(cmd, "--arguments")) == {"--ta_validacion": "4.4"}
            self.n += 1
            jid = f"jr_d{self.n}"
            self.job_runs[jid] = {"Id": jid, "JobRunState": self.job_state, "ExecutionTime": 22, "MaxCapacity": self.capacity,
                                  "ErrorMessage": "MemoryError" if self.job_state != "SUCCEEDED" else None}
            if self.job_state == "SUCCEEDED":
                self.run_curated(jid)
            return t43.ok({"JobRunId": jid})
        return super().handle(cmd)

    def query_result(self, sql):
        for name, needle in (("ub", "marcas_en_brands"), ("links44", "pares_repetidos_ht"), ("ht", "pares_por_fila"),
                             ("count44", "peso_ht")):
            if needle in sql:
                return list(self.sql[name]), [list(self.sql[name].values())]
        if "ORDER BY weight DESC" in sql:
            return ["hashtag_a", "hashtag_b", "weight"], [["colombia", "gaseosa", 3898], ["bebidas", "gaseosa", 3448]]
        if "n_marcas" in sql:
            return ["n_marcas", "usuarios", "menciones"], [[1, 479, 1000], [2, 1455, 50000], [3, 4002, 182395]]
        return super().query_result(sql)


class FakeRunner(t43.FakeRunner):
    def __init__(self, world, **kw):
        super().__init__(world, **kw)
        self.apply_text = "Apply complete! Resources: 0 added, 2 changed, 0 destroyed."

    def run(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        if "pytest" in cmd:
            self.calls.append(cmd)
            target = next(c for c in cmd if c.startswith(("src/", "contracts/")))
            default = {"src/lakehouse": v.B1_EXPECTED, "src/ops/tests/test_ops_validate44.py": v.B2_EXPECTED, "contracts/tests": 18,
                       "src/ops/tests/test_ops_validate43.py": v.B4_EXPECTED, "src/ops/tests/test_ops_validate42.py": v.B5_EXPECTED,
                       "src/ops/tests/test_ops_validate41.py": v.B6_EXPECTED}
            return v43.Result(0, self.pytest_out.get(target, f"{default.get(target, 1)} passed in 1s"))
        return super().run(cmd, **kw)


def s3_get_fake(wrong_edge=False):
    def get(bucket, key):
        table = v.table_of(key)
        if wrong_edge and table == "edges_user_brand":  # weight int32 en vez de bigint
            t = pa.table({"user_hash": ["u"], "brand": ["pepsi"], "weight": pa.array([1], pa.int32())})
            buf = pa.BufferOutputStream()
            pq.write_table(t, buf)
            return buf.getvalue().to_pybytes()
        return t43.table_parquet(table)
    return get


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    aws, ev = tmp_path / "aws", tmp_path / "evidencia"
    aws.mkdir()
    ev.mkdir()
    (aws / "backend.hcl").write_text('bucket = "x"\n')
    (aws / "terraform.tfvars").write_text(f'aws_account_id = "{t43.ACCOUNT}"\n')
    for module in (v, v43, v41):
        monkeypatch.setattr(module, "AWS_ENV", aws)
    monkeypatch.setattr(v, "EVIDENCE_DIR", ev)
    monkeypatch.setattr(v, "on_windows", lambda: False)
    return tmp_path


def run_part(runner, *argv, sandbox_dir, confirm=None, s3_rows=None, s3_get=None):
    lines = []
    state = {"t": NOW}

    def sleep(s):
        state["t"] += timedelta(seconds=s)

    path = sandbox_dir / "evidencia" / "validacion_4_4.json"
    code = v.main(list(argv), runner=runner, printer=lines.append, confirm=confirm, sleep=sleep, now=lambda: state["t"],
                  evidence_path=path, s3_rows=s3_rows or t43.s3_rows_fake(tables=EDGE_ROWS), s3_get=s3_get or s3_get_fake())
    return code, lines, json.loads(path.read_text())


steps = t43.steps


# --- utilidades puras ---------------------------------------------------------------------------


def test_table_of_y_layout_problems():
    assert v.table_of("curated/edges_user_brand/part-00000.parquet") == "edges_user_brand"
    assert v.table_of("curated/dim_user/part-00000.parquet") == "dim_user" and v.table_of("curated/_ta_placeholder/x") is None
    assert v.layout_problems(curated_keys() + ["curated/edges_user_brand/"]) == []
    bad = v.layout_problems(curated_keys() + ["curated/edges_hashtag_cooccurrence/part-00001.parquet"])
    assert bad == ["archivo de más: curated/edges_hashtag_cooccurrence/part-00001.parquet"]
    missing = v.layout_problems([k for k in curated_keys() if "edges_user_brand" not in k])
    assert missing == ["falta la tabla edges_user_brand"]


def test_constantes_iguales_al_job():
    assert v.TABLES == JOB.TABLE_NAMES and v.EDGE_TABLES == JOB.EDGE_TABLES and v.JOB_VERSION == JOB.JOB_VERSION
    assert len(CHECK_NAMES) == v.N_CHECKS and set(t43.make_report("r")["checks"]) <= set(CHECK_NAMES[:11])
    assert all("edge" in n for n in CHECK_NAMES[11:]) and not any("edge" in n for n in CHECK_NAMES[:11])
    assert {"edges_user_brand", "edges_hashtag_cooccurrence"} <= {
        s.name for s in catalog_spec.load_contract_tables() if s.database == "ta_curated"}
    for table in v.TABLES:
        assert v43.curated_schema_problems(t43.table_parquet(table), table) == ([], 1), table


def write_trusted(root, rows):
    path = root / "trusted" / "posts" / "source=reddit" / "dt=2026-10-24" / "part-00000.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    lst = pa.list_(pa.field("element", pa.string()))
    pq.write_table(pa.table({"user_hash": pa.array([r[0] for r in rows], pa.string()), "brands": pa.array([r[1] for r in rows], lst),
                             "hashtags": pa.array([r[2] for r in rows], lst)}), path)


ROWS = [("u1", ["pepsi", "postobon"], ["a", "b", "a"]), ("u1", ["pepsi", "pepsi"], ["b", "a"]), (None, ["sprite"], ["a", "c"]),
        ("u2", ["sprite"], []), ("u2", None, None)]
WANT_UB = {("u1", "pepsi"): 2, ("u1", "postobon"): 1, ("u2", "sprite"): 1}
WANT_HT = {("a", "b"): 2, ("a", "c"): 1}


def test_edges_from_trusted_igual_al_acumulador_del_job(tmp_path):
    write_trusted(tmp_path, ROWS)
    got = v.edges_from_trusted(tmp_path)
    assert got["user_brand"] == WANT_UB and got["pairs"] == WANT_HT
    assert (got["mentions"], got["cooccurrences"], got["hashtags"], got["rows"]) == (4, 3, 3, 5)
    acc = JOB.Accumulator()
    for user, marks, tags in ROWS:
        acc.add_edges(user, marks, tags)
    assert acc.mentions == got["user_brand"] and acc.cooc == got["pairs"] and acc.mention_total == got["mentions"]
    assert v.edges_from_trusted(tmp_path / "vacio")["rows"] == 0


def edge_parquets(ub, ht):
    a = JOB.parquet_bytes(JOB.table_from_columns("edges_user_brand", {
        "user_hash": [k[0] for k in sorted(ub)], "brand": [k[1] for k in sorted(ub)], "weight": [ub[k] for k in sorted(ub)]}))
    b = JOB.parquet_bytes(JOB.table_from_columns("edges_hashtag_cooccurrence", {
        "hashtag_a": [k[0] for k in sorted(ht)], "hashtag_b": [k[1] for k in sorted(ht)], "weight": [ht[k] for k in sorted(ht)]}))
    return a, b


def test_edges_from_parquet_y_edge_diff_sin_user_hash():
    ub, ht = v.edges_from_parquet(*edge_parquets(WANT_UB, WANT_HT))
    assert ub == WANT_UB and ht == WANT_HT and v.edge_diff(WANT_UB, ub) == []
    diff = v.edge_diff(WANT_UB, {("u1", "pepsi"): 3, ("u9", "sprite"): 1})
    assert diff == ["2 faltan", "1 sobran", "1 con otro peso"] and not any("u1" in d or "u9" in d for d in diff)


def cte(sql, name):
    start = sql.index(f"{name} AS (") + len(f"{name} AS (")
    depth, i = 1, start
    while depth:
        depth += {"(": 1, ")": -1}.get(sql[i], 0)
        i += 1
    return sql[start:i - 1]


def test_sql_de_aristas_lee_columnas_que_existen():
    fact = set(catalog_spec.table("ta_curated.fact_posts").column_names)
    trusted = set(catalog_spec.table("ta_trusted.posts").column_names)
    assert "FROM ta_curated.fact_posts" in cte(v.UB_SQL, "WITH m") and {"user_hash", "brands"} <= fact
    assert "FROM ta_trusted.posts" in cte(v.HT_SQL, "WITH t") and {"event_id", "hashtags"} <= trusted
    assert "hashtags" not in fact  # por eso las coocurrencias se recalculan desde Trusted (§3.22.1, v2.38)
    for sql in (v.UB_SQL, v.LINKS_SQL, v.HT_SQL, v.COUNT_SQL, *v.EXAMPLE_SQL.values()):
        assert ";" not in sql and sql.strip().upper().startswith(("SELECT", "WITH"))
    for sql in v.EXAMPLE_SQL.values():  # las consultas de ejemplo no muestran usuarios
        assert "user_hash" not in re.sub(r"\(SELECT user_hash.*?GROUP BY user_hash\)", "", sql, flags=re.S)


# --- A y B -------------------------------------------------------------------------------------


def test_preflight_ok_sin_trusted_local(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "preflight", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    s = steps(ev, "A")
    assert code == 0 and s["A2"]["status"] == "OK" and s["A4"]["status"] == "OK" and s["A6"]["status"] == "OK"
    assert s["A8"]["status"] == "AVISO" and ev["lote"] == "4.4" and ev["hu"] == ["HU-028"]


def test_preflight_horario_encendido_avisa_con_off_y_on(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "preflight", sandbox_dir=sandbox)
    a6 = steps(ev, "A")["A6"]
    assert code == 0 and a6["status"] == "AVISO" and "validate_4_4.py off" in a6["detail"] and "on --yes" in a6["detail"]


def test_tests_ok_y_falla_si_cambia_el_conteo(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "tests", sandbox_dir=sandbox)
    assert code == 0 and all(s["status"] == "OK" for s in steps(ev, "B").values()) and len(steps(ev, "B")) == 6
    runner = FakeRunner(World(), pytest_out={"src/ops/tests/test_ops_validate43.py": "34 passed in 1s"})
    code, _, ev = run_part(runner, "tests", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "B")["B4"]["status"] == "FALLA"


# --- L (local) ---------------------------------------------------------------------------------


def done_line(**kw):
    return json.dumps({"event": "glue_job_start"}) + "\n" + json.dumps(done_fields(**kw)) + "\n"


def seed_local(sandbox, *, ub=WANT_UB, ht=WANT_HT, report=None):
    write_trusted(sandbox / "cache" / "s3" / BUCKET, ROWS)
    dst = sandbox / "cache" / "trusted_to_curated_local"
    for key in t43.curated_keys():
        (dst / key).parent.mkdir(parents=True, exist_ok=True)
        (dst / key).write_bytes(t43.table_parquet(v43.table_of(key)))
    for table, data in zip(v.EDGE_TABLES, edge_parquets(ub, ht)):
        (dst / "curated" / table).mkdir(parents=True, exist_ok=True)
        (dst / "curated" / table / "part-00000.parquet").write_bytes(data)
    (dst / "quality" / "_latest").mkdir(parents=True, exist_ok=True)
    (dst / "quality" / "_latest" / "trusted_to_curated.json").write_text(json.dumps(report or make_report("local")))


def local(runner, sandbox):
    return run_part(runner, "local", "--cache", str(sandbox / "cache"), "--python", "/py39/bin/python", sandbox_dir=sandbox)


def test_local_sin_trusted_avisa(sandbox):
    code, _, ev = run_part(FakeRunner(World()), "local", "--cache", str(sandbox / "cache"), sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "L")["L0"]["status"] == "AVISO"


def test_local_aristas_conciliadas_idempotentes_y_recalculadas(sandbox):
    seed_local(sandbox)
    runner = FakeRunner(World(), local_out=[done_line(), done_line(objects_written=0, objects_unchanged=189, files_changed=0)])
    code, lines, ev = local(runner, sandbox)
    s = steps(ev, "L")
    assert code == 0, "\n".join(lines)
    assert [s[k]["status"] for k in ("L0", "L1", "L2", "L3", "L4")] == ["OK"] * 5, {k: x["detail"] for k, x in s.items()}
    assert "233395 menciones" in s["L1"]["detail"] and "16 comprobaciones (todas OK)" in s["L1"]["detail"]
    assert "3 pares usuario–marca" in s["L4"]["detail"] and s["L4"]["data"]["recomputed"]["mentions"] == 4
    assert set(s["L1"]["data"]["edge_checks"]) == {n for n in CHECK_NAMES if "edge" in n}


def test_local_aristas_distintas_o_script_viejo_falla(sandbox):
    seed_local(sandbox, ub={**WANT_UB, ("u1", "pepsi"): 5})
    runner = FakeRunner(World(), local_out=[done_line(), done_line(objects_written=0, files_changed=0)])
    code, _, ev = local(runner, sandbox)
    l4 = steps(ev, "L")["L4"]
    assert code == 1 and l4["status"] == "FALLA" and "1 con otro peso" in l4["detail"] and "u1" not in l4["detail"]
    seed_local(sandbox, report=make_report("viejo", version="trusted-to-curated@1.0", n_checks=11))
    runner = FakeRunner(World(), local_out=[done_line(mentions=None), done_line(objects_written=0, files_changed=0)])
    code, _, ev = local(runner, sandbox)
    assert code == 1 and steps(ev, "L")["L1"]["status"] == "FALLA" and "11 comprobaciones" in steps(ev, "L")["L1"]["detail"]


# --- C (Terraform) -----------------------------------------------------------------------------


def test_terraform_aplica_solo_el_script_y_la_descripcion(sandbox):
    runner = FakeRunner(World(), plans=[t43.plan(UPDATES), t43.plan()])
    code, lines, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "C")
    assert code == 0, "\n".join(lines)
    assert s["C2"]["status"] == "OK" and s["C3"]["status"] == "OK" and s["C3b"]["status"] == "OK"
    assert runner.count("-target=module.glue_workflow") == 2 and runner.count("module.athena") == 0 and runner.count(" apply ") == 1


def test_terraform_no_aplica_si_crea_o_cambia_otra_cosa(sandbox):
    for p in (t43.plan(UPDATES, ['module.athena.aws_athena_named_query.this["x"]']),
              t43.plan(UPDATES + ['module.glue_workflow.aws_glue_job.this["raw_to_trusted"]']),
              t43.plan(UPDATES, job_diff=("description", "max_capacity"))):
        runner = FakeRunner(World(), plans=[p])
        code, _, ev = run_part(runner, "terraform", "--apply", "--yes", sandbox_dir=sandbox)
        assert code == 1 and steps(ev, "C")["C2"]["status"] == "FALLA" and runner.count(" apply ") == 0


def test_terraform_ya_aplicado_y_sin_apply(sandbox):
    code, _, ev = run_part(FakeRunner(World(), plans=[t43.plan(), t43.plan()]), "terraform", sandbox_dir=sandbox)
    assert code == 0 and "ya aplicado" in steps(ev, "C")["C2"]["detail"] and steps(ev, "C")["C3b"]["status"] == "OK"
    code, _, ev = run_part(FakeRunner(World(), plans=[t43.plan(UPDATES)]), "terraform", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "C")["C3"]["status"] == "AVISO" and steps(ev, "C")["C3b"]["status"] == "OMITIDO"


# --- D (corrida real) --------------------------------------------------------------------------


def test_run_completo_solo_cambian_las_aristas(sandbox):
    runner = FakeRunner(World())
    code, lines, ev = run_part(runner, "run", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 0, "\n".join(lines)
    assert list(s) == ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"]
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert "233395 menciones" in s["D4"]["detail"] and "16 comprobaciones; 5 de aristas" in ev["parts"]["D"]["steps"][5]["title"]
    assert "solo aristas" in s["D7"]["detail"] and s["D7"]["data"]["files_changed"] == 2 and "9 tablas" in s["D8"]["detail"]
    assert runner.count("start-job-run") == 1 and "files" not in s["D5"]["data"]["report"]["output"]


def test_run_detecta_cambios_fuera_de_las_aristas_y_script_viejo(sandbox):
    world = World()
    world.report_overrides = {"outside": 1}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    d7 = steps(ev, "D")["D7"]
    assert code == 1 and d7["status"] == "FALLA" and "cambiaron tablas del 4.3" in d7["detail"]
    world = World()
    world.report_overrides = {"version": "trusted-to-curated@1.0", "n_checks": 11}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "D")
    assert code == 1 and s["D5"]["status"] == "FALLA" and s["D6"]["status"] == "FALLA" and "¿script viejo?" in s["D6"]["detail"]


def test_run_conciliacion_de_aristas_que_no_cuadra(sandbox):
    world = World()
    world.report_overrides = {"bad_edge": True}
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    d6 = steps(ev, "D")["D6"]
    assert code == 1 and d6["status"] == "FALLA" and "edges_hashtag_weight_eq_pairs" in d6["detail"]


def test_run_no_confirmado_job_fallido_y_esquema_de_arista(sandbox):
    runner = FakeRunner(World())
    code, _, ev = run_part(runner, "run", sandbox_dir=sandbox, confirm=lambda q: False)
    assert code == 0 and steps(ev, "D")["D3"]["status"] == "AVISO" and runner.count("start-job-run") == 0
    world = World()
    world.job_state = "FAILED"
    code, _, ev = run_part(FakeRunner(world), "run", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "D")["D3"]["status"] == "FALLA" and "D4" not in steps(ev, "D")
    code, _, ev = run_part(FakeRunner(World()), "run", "--yes", sandbox_dir=sandbox, s3_get=s3_get_fake(wrong_edge=True))
    d8 = steps(ev, "D")["D8"]
    assert code == 1 and d8["status"] == "FALLA" and "edges_user_brand.weight" in d8["detail"]


# --- E (Athena) ----------------------------------------------------------------------------------


def with_report(world):
    world.run_curated("jr_prev")
    return world


def test_athena_ok(sandbox):
    code, lines, ev = run_part(FakeRunner(with_report(World())), "athena", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert "Σ weight 233395 = menciones 233395" in s["E1"]["detail"] and "Σ C(k, 2) por fila 44578" in s["E3"]["detail"]
    assert "colombia + gaseosa (3898)" in s["E4"]["detail"] and s["E5"]["data"]["queries"] == 5


def test_athena_detecta_diferencias(sandbox):
    world = with_report(World())
    world.sql["ub"]["pesos_distintos"] = 4
    world.sql["links44"]["fuera_de_orden"] = 1
    world.sql["ht"]["pares_por_fila"] = 44000
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    s = steps(ev, "E")
    assert code == 1 and [s[k]["status"] for k in ("E1", "E2", "E3", "E4")] == ["FALLA", "FALLA", "FALLA", "OK"]
    world = with_report(World())
    world.sql["ub"]["peso_total"] = world.sql["ub"]["menciones"] = world.sql["ub"]["marcas_en_brands"] = 221629
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert code == 1 and "menciones del reporte 233395" in steps(ev, "E")["E1"]["detail"]


def test_athena_latencia_alta_avisa(sandbox):
    world = with_report(World())
    world.query_ms = 4200
    code, _, ev = run_part(FakeRunner(world), "athena", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "E")["E6"]["status"] == "AVISO" and "SPICE" in steps(ev, "E")["E6"]["detail"]


# --- F (segunda corrida) -------------------------------------------------------------------------


def test_rerun_idempotente_y_costo(sandbox):
    world = with_report(World())
    code, lines, ev = run_part(FakeRunner(world), "rerun", "--yes", sandbox_dir=sandbox)
    s = steps(ev, "F")
    assert code == 0, "\n".join(lines)
    assert all(x["status"] == "OK" for x in s.values()), {k: x["detail"] for k, x in s.items()}
    assert "2 de aristas" in s["F3"]["detail"] and "validate_4_4.py on --yes" in s["F7"]["detail"]
    f8 = s["F8"]["data"]
    assert f8["usd_per_day"] == pytest.approx(f8["usd_per_run"] * 24, rel=1e-3) and f8["curated_usd"] < f8["trusted_usd"]


def test_rerun_detecta_cambios_y_sin_aristas_previas(sandbox):
    world = with_report(World())
    world.rerun_changed = 1
    code, _, ev = run_part(FakeRunner(world), "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F3"]["status"] == "FALLA"
    world = with_report(World())
    world.objects = {k: o for k, o in world.objects.items() if not k.startswith(v.EDGE_PREFIX)}
    runner = FakeRunner(world)
    code, _, ev = run_part(runner, "rerun", "--yes", sandbox_dir=sandbox)
    assert code == 1 and steps(ev, "F")["F1"]["status"] == "FALLA" and runner.count("start-workflow-run") == 0


# --- status, off, on y CLI -----------------------------------------------------------------------


def test_status_con_aristas_y_encendido(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    assert code == 0 and steps(ev, "status")["S3"]["status"] == "AVISO" and "sin aristas" in steps(ev, "status")["S3"]["detail"]
    with_report(world)
    code, _, ev = run_part(FakeRunner(world), "status", sandbox_dir=sandbox)
    s = steps(ev, "status")
    assert code == 0 and s["S1"]["status"] == "OK" and s["S3"]["status"] == "OK" and "11 objetos" in s["S3"]["detail"]
    assert "menciones 233395" in s["S3"]["detail"] and "trusted-to-curated@1.1" in s["S3"]["detail"]


def test_off_y_on_dejan_el_horario_encendido(sandbox):
    world = World()
    world.schedule["State"] = "ENABLED"
    code, _, ev = run_part(FakeRunner(world), "off", sandbox_dir=sandbox)
    assert code == 0 and world.schedule["State"] == "DISABLED" and ev["lote"] == "4.4"
    questions = []
    code, _, ev = run_part(FakeRunner(world), "on", sandbox_dir=sandbox, confirm=lambda q: questions.append(q) or True)
    s = steps(ev, "on")
    assert code == 0 and world.schedule["State"] == "ENABLED" and s["N1"]["status"] == "OK" and s["N2"]["status"] == "OK"
    assert "estimado" in questions[0] and "validate_4_4.py off" in s["N1"]["detail"]


def test_fuera_de_wsl_sale_con_2(sandbox, monkeypatch):
    monkeypatch.setattr(v, "on_windows", lambda: True)
    lines = []
    code = v.main(["run"], runner=FakeRunner(World()), printer=lines.append, evidence_path=sandbox / "evidencia" / "x.json")
    assert code == 2 and "Windows" in lines[-1]


def test_main_por_script(sandbox):
    spec = importlib.util.spec_from_file_location("validate_4_4_script", v.PROJECT / "scripts" / "validate_4_4.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main is v.main and sys.modules["ops.validate44"] is v
