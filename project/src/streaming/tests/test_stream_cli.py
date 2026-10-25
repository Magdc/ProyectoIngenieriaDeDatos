from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone

import pytest

from streaming import cli
from streaming.inject import build_plan
from streaming.lake_report import GcsLake, LocalLake, hours_back, scan
from streaming.records import DlqRecord

NOW = datetime(2027, 1, 15, 8, 30, tzinfo=timezone.utc)
SOURCES = ["reddit", "facebook", "instagram"]


def _put(root, rel, lines):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))


def _event(eid, source="reddit"):
    return json.dumps({"event_id": eid, "source": source})


def test_hours_back_incluye_la_hora_actual():
    slots = hours_back(NOW, 2)
    assert [f"{s:%H}" for s in slots] == ["06", "07", "08"]


def test_scan_cuenta_lineas_ids_y_dlq(tmp_path):
    _put(tmp_path, "raw/source=reddit/dt=2027-01-15/hr=08/part-a.jsonl.gz", [_event("a"), _event("b")])
    _put(tmp_path, "raw/source=reddit/dt=2027-01-15/hr=07/part-b.jsonl.gz", [_event("a"), "no-json"])
    _put(tmp_path, "raw/source=facebook/dt=2027-01-15/hr=08/part-c.jsonl.gz", [_event("c", "reddit")])
    _put(tmp_path, "raw/source=reddit/dt=2027-01-14/hr=08/part-old.jsonl.gz", [_event("z")])  # fuera de rango
    _put(tmp_path, "dlq/dt=2027-01-15/part-d.jsonl.gz",
         [json.dumps({"error_type": "json", "source": "reddit", "raw_message": "x"})])
    result = scan(LocalLake(str(tmp_path)), SOURCES, hours=2, now=NOW)
    s = result.summary()
    assert s["raw"]["reddit"] == {"files": 2, "lines": 4, "unique_event_ids": 2,
                                  "duplicate_event_ids": 1, "extra_copies": 1, "bad_lines": 1}
    assert s["raw"]["facebook"]["bad_lines"] == 1  # fuente distinta de la partición
    assert s["dlq"] == {"files": 1, "records": 1, "by_error_type": {"json": 1},
                        "by_source": {"reddit": 1}}
    checks = {c["name"]: c["status"] for c in cli.traffic_checks(result, 0)}
    assert checks == {"raw_no_duplicates": "fail", "raw_lines_valid": "fail"}
    assert cli.traffic_checks(result, 5)[0]["status"] == "ok"


class _Blob:
    def __init__(self, name, data):
        self.name, self._data = name, data

    def download_as_bytes(self):
        return self._data


class _Bucket:
    def __init__(self, objects):
        self._objects = objects

    def blob(self, name):
        return _Blob(name, self._objects[name])


class _Client:
    def __init__(self, objects):
        self.objects = objects

    def bucket(self, name):
        assert name == "p1-ta-lake"
        return _Bucket(self.objects)

    def list_blobs(self, bucket, prefix):
        return [_Blob(k, v) for k, v in sorted(self.objects.items()) if k.startswith(prefix)]


def test_gcs_lake_con_cliente_falso():
    data = gzip.compress((_event("a") + "\n").encode())
    client = _Client({
        "raw/source=reddit/dt=2027-01-15/hr=08/part-1.jsonl.gz": data,
        "raw/source=reddit/dt=2027-01-15/hr=08/otro.txt": b"",
        "raw/source=reddit/dt=2027-01-15/hr=080/part-x.jsonl.gz": data,  # prefijo parecido
    })
    lake = GcsLake("gs://p1-ta-lake", client=client)
    assert lake.list("raw/source=reddit/dt=2027-01-15/hr=08") == [
        "raw/source=reddit/dt=2027-01-15/hr=08/part-1.jsonl.gz"]
    result = scan(lake, ["reddit"], hours=0, now=NOW)
    assert result.summary()["raw"]["reddit"]["lines"] == 1


def _dlq_row(msg, error_type=None):
    return {"error_type": error_type or msg.expect, "source": msg.topic_source,
            "raw_message": msg.data.decode("utf-8", errors="backslashreplace")}


def _fake_scan(plan, raw_ids, dlq_rows):
    class FakeScan:
        def __init__(self):
            self.dlq_rows = dlq_rows

        def raw_lines_with(self, needle):
            return raw_ids.count(needle)

    return FakeScan()


def test_evaluate_todo_correcto():
    plan = build_plan("ev")
    bad = [m for m in plan.messages if m.expect != "raw"]
    topic = [{"attributes": {"event_id": m.event_id, "dlq_error_type": m.expect}} for m in bad]
    checks = cli.evaluate(plan, _fake_scan(plan, [plan.sentinel_id], [_dlq_row(m) for m in bad]), topic)
    assert all(c["status"] == "ok" for c in checks), checks
    assert len(checks) == 1 + 2 * len(bad)


def test_evaluate_detecta_duplicado_falta_y_tipo_equivocado():
    plan = build_plan("ev2")
    bad = [m for m in plan.messages if m.expect != "raw"]
    rows = [_dlq_row(m) for m in bad[1:]] + [_dlq_row(bad[1], "json")]  # falta uno, otro doble
    raw = [plan.sentinel_id, plan.sentinel_id, bad[2].event_id]
    failed = {c["name"] for c in cli.evaluate(plan, _fake_scan(plan, raw, rows), [])
              if c["status"] == "fail"}
    assert "sentinel_in_raw_once" in failed
    assert f"dlq_file:{bad[0].name}" in failed
    assert f"dlq_file:{bad[1].name}" in failed
    assert f"not_in_raw:{bad[2].name}" in failed
    assert f"dlq_topic:{bad[0].name}" in failed


def test_report_local(tmp_path, capsys):
    hour = datetime.now(timezone.utc)
    _put(tmp_path, f"raw/source=reddit/dt={hour:%Y-%m-%d}/hr={hour:%H}/part-a.jsonl.gz", [_event("a")])
    out = tmp_path / "ev" / "rep.json"
    assert cli.main(["report", "--root", str(tmp_path), "--json-out", str(out)]) == cli.PASS
    assert json.loads(out.read_text())["verdict"] == "ok"
    assert "raw/reddit" in capsys.readouterr().out


def test_check_se_niega_a_publicar_en_la_nube_sin_permiso(monkeypatch, capsys):
    monkeypatch.delenv("PUBSUB_EMULATOR_HOST", raising=False)
    monkeypatch.setenv("GCP_PROJECT", "p1")
    assert cli.main(["check", "--root", "/tmp/x"]) == cli.ERROR
    assert "--allow-cloud" in capsys.readouterr().err


def test_check_flujo_completo_con_dobles(monkeypatch, tmp_path):
    """check: publica, espera, lee el lake y el topic, y borra la suscripción temporal."""
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
    monkeypatch.setenv("GCP_PROJECT", "ta-local")
    published = {}
    deleted = []

    def fake_publish(plan, project, template):
        published["plan"] = plan
        hour = datetime.now(timezone.utc)
        _put(tmp_path, f"raw/source=reddit/dt={hour:%Y-%m-%d}/hr={hour:%H}/part-a.jsonl.gz",
             [json.dumps(plan.sentinel)])
        bad = [m for m in plan.messages if m.expect != "raw"]
        _put(tmp_path, f"dlq/dt={hour:%Y-%m-%d}/part-a.jsonl.gz",
             [json.dumps(_dlq_row(m)) for m in bad])
        return []

    class FakeWatcher:
        def __init__(self, project, topic, run_id):
            self.messages = []
            self.name = f"projects/{project}/subscriptions/ta-verify-dlq-{run_id}"

        def create(self):
            pass

        def drain(self):
            plan = published.get("plan")
            if plan:
                self.messages = [{"attributes": {"event_id": m.event_id, "dlq_error_type": m.expect}}
                                 for m in plan.messages if m.expect != "raw"]

        def delete(self):
            deleted.append(self.name)

    monkeypatch.setattr(cli, "publish", fake_publish)
    monkeypatch.setattr(cli, "DlqWatcher", FakeWatcher)
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    out = tmp_path / "check.json"
    code = cli.main(["check", "--root", str(tmp_path), "--timeout", "30", "--poll", "1",
                     "--json-out", str(out)])
    report = json.loads(out.read_text())
    assert code == cli.PASS, report["checks"]
    assert report["target"] == "emulator"
    assert report["plan"]["sentinel_event_id"] == published["plan"].sentinel_id
    assert deleted and deleted[0].startswith("projects/ta-local/subscriptions/ta-verify-dlq-")


def test_check_falla_por_tiempo(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
    monkeypatch.setenv("GCP_PROJECT", "ta-local")
    monkeypatch.setattr(cli, "publish", lambda *a: [])
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    clock = iter(range(0, 10_000, 50))
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(clock))
    code = cli.main(["check", "--root", str(tmp_path), "--timeout", "100", "--no-watch-dlq"])
    assert code == cli.FAIL


def test_dlq_record_es_lo_que_lee_el_verificador():
    """La línea de dlq/ que escribe el pipeline contiene el event_id del mensaje rechazado."""
    plan = build_plan("ln")
    msg = next(m for m in plan.messages if m.expect == "encoding")
    rec = DlqRecord(received_at="x", source="reddit", error_type="encoding", error_detail="d",
                    raw_message=msg.data.decode("utf-8", errors="backslashreplace"), process_ts=0)
    assert msg.event_id in json.loads(rec.line())["raw_message"]


@pytest.mark.parametrize("argv", [[], ["otro"]])
def test_uso_incorrecto(argv):
    with pytest.raises(SystemExit):
        cli.main(argv)
