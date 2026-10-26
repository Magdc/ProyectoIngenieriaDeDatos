"""CLI ``profile_data.py`` de punta a punta con S3 falso y carpetas temporales (sin red)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quality import cli, s3sync
from quality.report_md import SECTIONS, render
from quality.testing import make_rows, write_raw_layer

REPO = Path(__file__).resolve().parents[4]
HEX64 = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


@pytest.fixture()
def raw_root(tmp_path):
    rows = []
    for s in ("reddit", "facebook", "instagram"):
        rows += make_rows("historical", s, 12, datetime(2026, 6, 1, tzinfo=timezone.utc), step=timedelta(hours=2))
        rows += make_rows("stream", s, 12, datetime(2026, 10, 23, 5, tzinfo=timezone.utc), step=timedelta(minutes=1))
    write_raw_layer(tmp_path / "lake", rows)
    return tmp_path / "lake"


class DirS3:
    """S3 falso que sirve los archivos de una carpeta local."""

    def __init__(self, root: Path):
        self.root = root

    def list_objects(self, bucket, prefix):
        out = []
        for p in sorted(self.root.rglob("*")):
            key = p.relative_to(self.root).as_posix()
            if p.is_file() and key.startswith(prefix):
                out.append({"key": key, "size": p.stat().st_size, "etag": ""})
        return out

    def download(self, bucket, key, dest):
        dest.write_bytes((self.root / key).read_bytes())


def run(argv, **kw):
    lines: list[str] = []
    code = cli.main(argv, out=lines.append, env=kw.pop("env", {}), **kw)
    return code, "\n".join(lines)


def test_sync_y_profile_desde_la_cache(raw_root, tmp_path):
    cache = tmp_path / "cache"
    code, out = run(["sync", "--layer", "raw", "--bucket", "lake", "--cache", str(cache)], port=DirS3(raw_root))
    assert code == 0 and "por bajar" in out and "fallidos 0" in out
    js, md = tmp_path / "ev" / "p.json", tmp_path / "ev" / "p.md"
    code, out = run(["profile", "--layer", "raw", "--bucket", "lake", "--cache", str(cache), "--json-out", str(js),
                     "--md-out", str(md), "--as-of", "2026-10-26T00:00:00Z", "--quiet"])
    assert code == 0, out
    rep = json.loads(js.read_text(encoding="utf-8"))
    assert rep["source_uri"] == "s3://lake/raw/ (caché local)"
    assert rep["sync"]["objects"] == rep["files"]["listed"]
    assert not any(a["kind"] == "cache" for a in rep["anomalies"])
    text = md.read_text(encoding="utf-8")
    for i, title in enumerate(SECTIONS, start=1):
        assert f"## {i}. " in text
    assert not HEX64.search(text) and not HEX64.search(js.read_text(encoding="utf-8"))
    assert "historical/*" in out and "KPI stream" in out


def test_profile_con_root_y_fail_on(raw_root, tmp_path):
    bad = raw_root / "raw/origin=stream/source=reddit/dt=2026-10-23/hr=09/part-20261023T090000Z-p000.parquet"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"roto")
    code, _ = run(["profile", "--layer", "raw", "--root", str(raw_root), "--quiet"])
    assert code == 0
    code, _ = run(["profile", "--layer", "raw", "--root", str(raw_root), "--quiet", "--fail-on", "error"])
    assert code == 1


def test_errores_de_uso(tmp_path):
    code, out = run(["profile", "--layer", "raw", "--root", str(tmp_path / "vacio"), "--quiet"])
    assert code == 2 and "sync" in out
    code, out = run(["sync", "--layer", "raw", "--bucket", "b", "--cache", str(REPO / "project" / ".cache")], port=DirS3(tmp_path))
    assert code == 2 and "dentro del repositorio" in out
    code, out = run(["sync", "--layer", "trusted", "--origin", "stream", "--bucket", "b", "--cache", str(tmp_path / "c")],
                    port=DirS3(tmp_path))
    assert code == 2
    code, _ = run(["nada"])
    assert code == 2


def test_rules_catalogo_y_sobre_un_json(raw_root, tmp_path):
    js = tmp_path / "rules.json"
    code, out = run(["rules", "--catalog", "--json-out", str(js)])
    assert code == 0 and "DQ-01" in out
    assert len(json.loads(js.read_text(encoding="utf-8"))) >= 60
    prof = tmp_path / "p.json"
    assert run(["profile", "--layer", "raw", "--root", str(raw_root), "--json-out", str(prof), "--quiet"])[0] == 0
    code, out = run(["rules", "--from", str(prof)])
    assert code == 0 and "cumple" in out


def test_synth_trusted_y_su_marcador(raw_root, tmp_path):
    out_dir = tmp_path / "synth"
    code, out = run(["synth-trusted", "--from-root", str(raw_root), "--out", str(out_dir), "--cache", str(tmp_path / "c")])
    assert code == 0 and "Trusted simulado" in out
    assert (out_dir / cli.SYNTH_MARKER).exists()
    code, _ = run(["synth-trusted", "--from-root", str(raw_root), "--out", str(out_dir), "--cache", str(tmp_path / "c"), "--defects"])
    assert code == 0  # reemplaza la salida porque la creó synth-trusted
    other = tmp_path / "ajeno"
    (other / "trusted").mkdir(parents=True)
    code, out = run(["synth-trusted", "--from-root", str(raw_root), "--out", str(other), "--cache", str(tmp_path / "c")])
    assert code == 2 and "no lo creó" in out
    code, out = run(["profile", "--layer", "trusted", "--root", str(out_dir), "--quiet"])
    assert code == 0


class FakePut:
    def __init__(self):
        self.puts = []

    def put_object(self, **kw):
        self.puts.append(kw)
        return {"ServerSideEncryption": "aws:kms"}


def test_publish_apagado_por_defecto(raw_root, tmp_path):
    prof = tmp_path / "p.json"
    md = tmp_path / "p.md"
    run(["profile", "--layer", "raw", "--root", str(raw_root), "--json-out", str(prof), "--md-out", str(md), "--quiet"])
    client = FakePut()
    code, out = run(["publish", "--from", str(prof), "--md", str(md), "--bucket", "lake"], client=client)
    assert code == 0 and client.puts == [] and "sin --apply no sube" in out
    assert re.search(r"s3://lake/quality/\d{4}-\d{2}-\d{2}/profiling/raw/p\.json", out)
    code, out = run(["publish", "--from", str(prof), "--md", str(md), "--bucket", "lake", "--apply"], client=client,
                    env={"TA_LAKE_KMS_KEY_ARN": "arn:aws:kms:us-east-1:111111111111:key/abc"})
    assert code == 0 and len(client.puts) == 2
    assert all(p["ServerSideEncryption"] == "aws:kms" and p["BucketKeyEnabled"] for p in client.puts)


def test_render_sin_trusted_ni_stream():
    rep = {"layer": "raw", "table": "ta_raw.posts_raw", "profiler": "x", "generated_at": "t", "as_of": "t", "source_uri": "local",
           "summary": {"anomalies": {"error": 0, "warning": 0, "info": 0}, "rules": {}, "kpi": {}},
           "groups": [], "files": {"listed": 0, "recognized": 0, "unrecognized": 0, "read_error_count": 0, "auxiliary": 0,
                                   "schema_variants": [], "physical_types": {}},
           "duplicates": {"rows": 0, "distinct_event_ids": 0, "duplicate_rows": 0, "same_file": 0, "other_file_same_origin": 0,
                          "cross_origin": 0}, "comparison": {"available": False}, "anomalies": [], "rules": []}
    text = render(rep)
    assert "Solo hay un origen" in text


def test_resolve_cache_por_defecto_fuera_del_repo():
    assert not str(s3sync.resolve_cache(None, env={})).startswith(str(REPO))
