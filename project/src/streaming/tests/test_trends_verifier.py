"""Verificador del Lote 2.4 (``scripts/verify_trends.py``) con lake local y BigQuery falsos. Sin red."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.stream_tables import TABLES
from streaming import trends_cli
from streaming.dlq_bq import dlq_row
from streaming.trends_check import (
    AVISO,
    FALLA,
    OK,
    check_cold_start,
    check_reconciliation,
    check_switch_alerts,
    check_table_schemas,
    compare_windows,
    detection_latencies,
    load_local_trends,
    percentiles,
)
from streaming.trends_core import (
    TrendConfig,
    expected_windows,
    feature_from_row,
    format_ts,
    score_window,
    to_epoch_ms,
    trend_row,
)

CFG = TrendConfig()
T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)
BRANDS = ("postobon", "pepsi", "sprite")
SOURCES = ("reddit", "facebook", "instagram")


def _posts(start_ms: int, minutes: int, per_minute: int = 2) -> list[dict]:
    posts = []
    labels = ("positive", "negative", "neutral")
    emotions = ("joy", "anger", "neutral", None)
    i = 0
    for minute in range(minutes):
        for brand in BRANDS:
            for source in SOURCES:
                for k in range(per_minute):
                    i += 1
                    ms = start_ms + minute * 60_000 + k * 20_000 + 1500
                    posts.append({"event_id": f"e{i}", "event_time": format_ts(ms), "brand_primary": brand,
                                  "source": source, "sentiment_label": labels[i % 3], "sentiment_score": 0.8,
                                  "emotion_label": emotions[i % 4], "process_time": format_ts(ms + 8000),
                                  "ingest_time": format_ts(ms + 2000)})
    return posts


def _rows_for(features) -> list[dict]:
    rows, states = [], {}
    for (_start, brand, source), agg in sorted(expected_windows(features, CFG).items()):
        states[(brand, source)], score = score_window(states.get((brand, source)), agg, CFG)
        rows.append(trend_row(agg, score))
    return rows


def _trend_rows(posts: list[dict], late_panes: bool = True) -> list[dict]:
    """Filas como las escribiría la rama: la versión final de cada ventana y, con
    ``late_panes``, una versión anterior con menos posts (como un panel a tiempo
    seguido de uno tardío)."""
    features = [feature_from_row(p)[0] for p in posts]
    rows = _rows_for(features)
    if late_panes:
        rows = _rows_for(features[::2]) + rows
    return rows


# --------------------------------------------------------------------------- comprobaciones

def test_percentiles():
    assert percentiles([]) == {"n": 0, "p50": None, "p95": None, "max": None}
    stats = percentiles(list(range(1, 101)))
    assert (stats["p50"], stats["p95"], stats["max"]) == (50, 95, 100)


def test_conciliacion_ok_con_paneles_tardios():
    posts = _posts(T0, 15)
    rows = _trend_rows(posts)
    result = compare_windows(rows, posts, CFG, T0, T0 + 15 * 60_000)
    assert result["compared"] > 0 and result["matched"] == result["compared"]
    assert check_reconciliation(result).status == OK


def test_conciliacion_detecta_diferencias():
    posts = _posts(T0, 15)
    rows = _trend_rows(posts, late_panes=False)
    rows[10] = dict(rows[10], n_posts=rows[10]["n_posts"] + 5)
    rows.pop(20)
    result = compare_windows(rows, posts, CFG, T0 - 300_000, T0 + 15 * 60_000)
    assert result["n_mismatches"] >= 1 and result["n_missing"] == 1
    assert check_reconciliation(result, min_ok=1.0, min_warn=0.999).status == FALLA


def test_conciliacion_ignora_copias_de_posts():
    posts = _posts(T0, 10)
    rows = _trend_rows(posts)
    result = compare_windows(rows, posts + posts[:5], CFG, T0, T0 + 10 * 60_000)
    assert result["matched"] == result["compared"]


def test_conciliacion_sin_ventanas_es_aviso():
    assert check_reconciliation(compare_windows([], [], CFG, T0, T0)).status == AVISO


def test_arranque_en_frio():
    posts = _posts(T0, 10)
    rows = _trend_rows(posts)
    assert check_cold_start(rows, CFG, T0).status == OK
    index = next(i for i, r in enumerate(rows) if to_epoch_ms(r["window_start"]) >= T0 + 60_000)
    rows[index] = dict(rows[index], is_alert=True)
    assert check_cold_start(rows, CFG, T0).status == FALLA


def test_esquemas_de_tablas():
    schemas = {t: [(c.name, c.type) for c in cols] for t, cols in TABLES.items()}
    schemas["trend_windows_latest"] = schemas["trend_windows"]
    assert check_table_schemas(schemas).status == OK
    schemas["dq_results"] = schemas["dq_results"][:-1]
    assert check_table_schemas(schemas).status == FALLA
    assert check_table_schemas({}).status == FALLA


def test_latencia_de_deteccion():
    first_seen = {(T0, "p", "r"): T0 + 300_000 + 45_000, (T0 + 60_000, "p", "r"): T0 + 360_000 + 75_000}
    stats = detection_latencies(first_seen)
    assert stats["n"] == 2 and stats["max"] == 75
    assert [w["latency_first_s"] for w in stats["windows"]] == [45.0, 75.0]


def test_deteccion_por_ventana_distingue_una_ventana_atrasada():
    first_seen = {}
    for w in range(12):
        delay = 310_500 if w == 0 else 52_600
        for brand in ("a", "b", "c"):
            for source in ("reddit", "facebook", "instagram"):
                first_seen[(T0 + w * 60_000, brand, source)] = T0 + w * 60_000 + 300_000 + delay
    stats = detection_latencies(first_seen)
    assert stats["p95"] == 310.5 and stats["p50"] == 52.6  # una ventana de 12 basta para tumbar el p95
    assert [w["keys"] for w in stats["windows"]] == [9] * 12
    assert stats["windows"][0]["latency_first_s"] == 310.5 and stats["windows"][1]["latency_first_s"] == 52.6


def test_alertas_en_las_ventanas_del_cambio_de_job():
    rows = _trend_rows(_posts(T0 - 600_000, 20), late_panes=False)
    assert check_switch_alerts(rows, CFG, T0).status == OK
    index = next(i for i, r in enumerate(rows) if T0 - 300_000 <= to_epoch_ms(r["window_start"]) < T0)
    rows[index] = dict(rows[index], is_alert=True)
    check = check_switch_alerts(rows, CFG, T0)
    assert check.status == AVISO and len(check.data["alerts"]) == 1
    assert check_cold_start(rows, CFG, T0).status == OK  # el arranque en frío no las cuenta


# --------------------------------------------------------------------------- local

def _write_lake(lake: Path, posts, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    folder = lake / "enriched" / "source=reddit" / f"dt={format_ts(T0)[:10]}" / f"hr={format_ts(T0)[11:13]}"
    folder.mkdir(parents=True)
    columns = ["event_id", "event_time", "brand_primary", "source", "sentiment_label", "sentiment_score",
               "emotion_label", "process_time", "ingest_time"]
    data = {c: [p[c] for p in posts] for c in columns}
    for name in ("event_time", "process_time", "ingest_time"):
        data[name] = pa.array([to_epoch_ms(v) for v in data[name]], type=pa.timestamp("ms", tz="UTC"))
    pq.write_table(pa.table(data), folder / "part-x.parquet")
    trends = lake / "trend_windows" / "dt=x" / "hr=y"
    trends.mkdir(parents=True)
    (trends / "part.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_report_local_completo(tmp_path):
    posts = _posts(T0, 20)
    rows = _trend_rows(posts)
    _write_lake(tmp_path, posts, rows)
    out = tmp_path / "rep.json"
    code = trends_cli.main(["--json-out", str(out), "report", "--local", "--lake", str(tmp_path),
                            "--since", format_ts(T0), "--warmup-minutes", "0"],
                           clock=lambda: (T0 + 21 * 60_000) / 1000)
    report = json.loads(out.read_text())
    statuses = {c["id"]: c["status"] for c in report["checks"]}
    assert code == 0, report
    assert statuses["T2"] == OK and statuses["T3"] == OK and statuses["T4"] == OK
    assert statuses["T5"] == OK and statuses["T7"] == OK
    assert report["versions"]["keys_with_late_panes"] > 0


def test_report_local_sin_datos_falla(tmp_path):
    code = trends_cli.main(["report", "--local", "--lake", str(tmp_path), "--since", format_ts(T0)],
                           clock=lambda: (T0 + 600_000) / 1000)
    assert code == 1


def test_lineas_invalidas_en_el_lake(tmp_path):
    folder = tmp_path / "trend_windows" / "dt=1"
    folder.mkdir(parents=True)
    (folder / "p.jsonl").write_text('{"a": 1}\nno json\n')
    rows = load_local_trends(tmp_path)
    assert rows[1] == {"_invalid_line": "no json"}


# --------------------------------------------------------------------------- nube con un doble

class FakeBackend:
    project = "proj-x"
    dataset = "ta_stream"

    def __init__(self, posts=(), rows=(), clock=None, visible_after_s=None):
        self.visible_after_s = visible_after_s or (lambda _now_ms: 40)
        self.posts_data = list(posts)
        self.rows = list(rows)
        self.published = []
        self.dq = []
        self.clock = clock
        self.visible_from = {}

    def table_columns(self, table):
        if table == "trend_windows_latest":
            table = "trend_windows"
        return [(c.name, c.type) for c in TABLES[table]]

    def trend_rows(self, lo, hi):
        return [r for r in self.rows if lo <= to_epoch_ms(r["window_start"]) < hi]

    def window_keys(self, from_ms):
        now = int(self.clock() * 1000)
        delay_ms = int(self.visible_after_s(now) * 1000)  # por defecto, aparece 40 s después de cerrar
        keys = set()
        for r in self.rows:
            start = to_epoch_ms(r["window_start"])
            if start >= from_ms and start + 300_000 + delay_ms <= now:
                keys.add((start, r["brand"], r["source"]))
        return sorted(keys)

    def posts(self, lo, hi):
        return [p for p in self.posts_data if lo <= to_epoch_ms(p["event_time"]) < hi]

    def latency(self, since):
        return {"n": 100, "p50": 8.0, "p95": 25.0, "max": 40.0}

    def dlq_rows(self, since, marker):
        rows = []
        for data, attrs in self.published:
            if marker.encode() in data:
                row, _ = dlq_row(data, {**attrs, "dlq_error_type": _type_of(data), "dlq_source": "reddit",
                                        "dlq_received_at": format_ts(T0)}, T0)
                rows.append(row)
        return rows

    def insert_dq(self, rows):
        self.dq.extend(rows)
        return []

    def publish(self, topic, data, attributes):
        assert topic == "ta-raw-reddit"
        self.published.append((data, dict(attributes)))
        return str(len(self.published))


def _type_of(data: bytes) -> str:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return "encoding"
    try:
        json.loads(text)
    except ValueError:
        return "json"
    return "schema"


class FakeClock:
    def __init__(self, start_s):
        self.now = start_s

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_report_nube_con_watch_y_dq(tmp_path):
    posts = _posts(T0, 30)
    rows = _trend_rows(posts)
    clock = FakeClock((T0 + 20 * 60_000) / 1000)
    backend = FakeBackend(posts, rows, clock)
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "proj-x", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "7", "--poll-seconds", "20", "--write-dq"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    statuses = {c["id"]: c["status"] for c in report["checks"]}
    assert code == 0, report
    assert statuses == {"T1": OK, "T2": OK, "T3": OK, "T4": OK, "T5": OK, "T5b": OK, "T6": OK, "T7": OK,
                        "T8": OK, "T9": OK}
    assert report["detection"]["n"] > 0 and report["detection"]["p95"] <= 60
    assert report["caught_up"] is None and report["detection"]["windows"]
    assert {r["check_name"] for r in backend.dq} >= {"latency_event_to_process_p95", "trend_detection_latency_p95"}
    assert all(r["layer"] == "stream" and isinstance(r["details"], str) for r in backend.dq)


def _catch_up_backend(posts, rows, clock, start_s, backlog_s):
    """La marca de agua arranca atrasada ``backlog_s`` y se pone al día a razón de 1 s por segundo."""
    return FakeBackend(posts, rows, clock,
                       visible_after_s=lambda now_ms: max(40, backlog_s - (now_ms / 1000 - start_s)))


def test_report_espera_a_que_el_job_se_ponga_al_dia(tmp_path):
    posts = _posts(T0, 40)
    rows = _trend_rows(posts)
    start = (T0 + 20 * 60_000) / 1000
    clock = FakeClock(start)
    backend = _catch_up_backend(posts, rows, clock, start, backlog_s=400)
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "p", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "7", "--wait-caught-up-minutes", "15"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    assert code == 0, report
    assert report["caught_up"]["caught_up"] and report["caught_up"]["waited_s"] >= 200
    assert report["detection"]["p95"] <= 80
    assert report["steady_from"] == format_ts(report["caught_up"]["at_ms"])


def test_sin_esperar_el_backlog_tumba_t8(tmp_path):
    posts = _posts(T0, 40)
    rows = _trend_rows(posts)
    start = (T0 + 20 * 60_000) / 1000
    clock = FakeClock(start)
    backend = _catch_up_backend(posts, rows, clock, start, backlog_s=900)
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "p", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "8"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    statuses = {c["id"]: c["status"] for c in report["checks"]}
    assert code == 1 and statuses["T8"] == FALLA
    assert report["detection"]["censored"] > 0 and report["detection"]["p95"] >= 300


def test_observacion_sin_ventanas_nuevas_es_falla(tmp_path):
    posts = _posts(T0, 40)
    rows = _trend_rows(posts)
    clock = FakeClock((T0 + 20 * 60_000) / 1000)
    backend = FakeBackend(posts, rows, clock, visible_after_s=lambda _now: 3600)  # nada aparece
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "p", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "7"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    t8 = next(c for c in json.loads(out.read_text())["checks"] if c["id"] == "T8")
    assert code == 1 and t8["status"] == FALLA and "ninguna ventana" in t8["detail"]


def test_job_que_no_se_pone_al_dia(tmp_path):
    posts = _posts(T0, 40)
    rows = _trend_rows(posts)
    clock = FakeClock((T0 + 20 * 60_000) / 1000)
    backend = FakeBackend(posts, rows, clock, visible_after_s=lambda _now: 900)
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "p", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "7", "--wait-caught-up-minutes", "2"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    t8 = next(c for c in report["checks"] if c["id"] == "T8")
    assert code == 1 and t8["status"] == FALLA and "no se puso al día" in t8["detail"]
    assert report["detection"] is None


def test_job_detenido_falla_aunque_hubiera_ventanas_antes(tmp_path):
    posts = _posts(T0, 40)
    rows = [r for r in _trend_rows(posts) if to_epoch_ms(r["window_start"]) < T0 + 14 * 60_000]  # se detuvo
    clock = FakeClock((T0 + 20 * 60_000) / 1000)
    backend = FakeBackend(posts, rows, clock)
    out = tmp_path / "r.json"
    code = trends_cli.main(["--project", "p", "--json-out", str(out), "report", "--since", format_ts(T0),
                            "--watch-minutes", "7"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    t8 = next(c for c in report["checks"] if c["id"] == "T8")
    assert code == 1 and t8["status"] == FALLA
    assert report["detection"]["censored"] > 0 and report["detection"]["p95"] >= 300


def test_observacion_corta_es_error_de_uso(capsys):
    assert trends_cli.main(["--project", "p", "report", "--watch-minutes", "3"],
                           backend_factory=lambda p, d: FakeBackend()) == 2
    assert "--watch-minutes" in capsys.readouterr().err


def test_report_nube_tabla_faltante():
    backend = FakeBackend()
    backend.table_columns = lambda t: None
    code = trends_cli.main(["--project", "p", "report"], backend_factory=lambda p, d: backend)
    assert code == 1


def test_dlq_check(tmp_path):
    clock = FakeClock(T0 / 1000)
    backend = FakeBackend(clock=clock)
    out = tmp_path / "d.json"
    code = trends_cli.main(["--project", "proj-x", "--json-out", str(out), "dlq-check", "--allow-cloud"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    report = json.loads(out.read_text())
    assert code == 0, report
    assert len(backend.published) == 3
    assert all(a["source"] == "reddit" and a["schema_version"] == "1.0" for _, a in backend.published)
    assert {c["id"]: c["status"] for c in report["checks"]} == {"D1": OK, "D2": OK, "D3": OK}


def test_dlq_check_exige_allow_cloud(capsys):
    assert trends_cli.main(["--project", "p", "dlq-check"], backend_factory=lambda p, d: FakeBackend()) == 2
    assert "--allow-cloud" in capsys.readouterr().err


def test_dlq_check_tiempo_agotado():
    clock = FakeClock(T0 / 1000)
    backend = FakeBackend(clock=clock)
    backend.dlq_rows = lambda since, marker: []
    code = trends_cli.main(["--project", "p", "dlq-check", "--allow-cloud", "--timeout", "60"],
                           backend_factory=lambda p, d: backend, clock=clock, sleep=clock.sleep)
    assert code == 1


def test_sin_proyecto(monkeypatch, capsys):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert trends_cli.main(["tables"]) == 2
    assert "GCP_PROJECT" in capsys.readouterr().err


@pytest.mark.parametrize("since", ["ayer", "2026-13-01"])
def test_since_invalido(since, tmp_path):
    assert trends_cli.main(["report", "--local", "--lake", str(tmp_path), "--since", since]) == 2
