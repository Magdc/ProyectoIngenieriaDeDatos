"""verify: el dataset de dobles pasa y cada regla detecta su defecto."""

from __future__ import annotations

import io
import json

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from historical.testing import FakeEnvelopes  # noqa: E402
from historical.verify import (AVISO, FALLA, OK, Check, event_id_formula,  # noqa: E402
                               run_checks)


def _status(report, cid):
    return next(c for c in report["checks"] if c["id"] == cid)


def test_dataset_sano_pasa(fake_run):
    work, _, _, _ = fake_run
    report = run_checks(work, allow_fake_nlp=True)
    failed = [c for c in report["checks"] if c["status"] == FALLA]
    assert not failed, failed
    assert [c["id"] for c in report["checks"]] == [f"H{i}" for i in range(1, 13)]
    assert report["summary"]["rows"] == 900


def _candidates(envelopes):
    from historical.generate import _brand_label

    return lambda source, seq, when: _brand_label(envelopes.envelope(source, seq, when))


def test_dataset_grande_pasa_estacionalidad_y_episodios(tmp_path, cfg_factory, pipeline):
    cfg = cfg_factory(n=10_000, chunk_size=4000)
    work, _, _, _ = pipeline(tmp_path, cfg)
    report = run_checks(work, allow_fake_nlp=True)
    for cid in ("H5", "H6", "H7", "H8"):
        assert _status(report, cid)["status"] == OK, _status(report, cid)
    assert report["status"] == OK
    h7 = _status(run_checks(work, allow_fake_nlp=True, candidate=_candidates(FakeEnvelopes(cfg.seed))),
                 "H7")
    assert h7["status"] == OK and "simulador" in h7["detail"], h7
    pepsi = h7["data"]["pepsi"]
    assert pepsi["trend_ratio_expected"] > 1.2 and abs(pepsi["trend_ratio"] / pepsi["trend_ratio_expected"] - 1) < 0.1
    assert "tendencia aplicada frente a sprite" in h7["detail"]


class DriftEnvelopes(FakeEnvelopes):
    """Como el BrandSelector real (§3.10): la mezcla de marcas cambia con ``seq``."""

    def envelope(self, source, seq, now):
        self.brand_weights = {"postobon": 1.0, "pepsi": 1.0, "sprite": 0.5 + seq / 2000}
        return super().envelope(source, seq, now)


def test_h7_separa_la_deriva_del_simulador(tmp_path, cfg_factory, pipeline):
    """v2.28: con la mezcla del simulador moviéndose, suponerla constante da un falso aviso
    (sprite sube aunque el plan no le pone tendencia); contra los candidatos reales pasa."""
    cfg = cfg_factory(n=10_000, chunk_size=4000)
    work, _, _, _ = pipeline(tmp_path, cfg, envelopes=DriftEnvelopes(cfg.seed))
    uniform = _status(run_checks(work, allow_fake_nlp=True), "H7")
    assert uniform["status"] == AVISO and "sprite" in uniform["detail"] and "sin el simulador" in uniform["detail"]
    h7 = _status(run_checks(work, allow_fake_nlp=True, candidate=_candidates(DriftEnvelopes(cfg.seed))), "H7")
    assert h7["status"] == OK, h7
    sprite = h7["data"]["sprite"]
    assert sprite["simulator_last"] - sprite["simulator_first"] > 0.15 and sprite["change"] > 0.1
    assert h7["data"]["_method"]["generated"] > 0


def test_h7_detecta_tendencia_que_no_se_aplico(tmp_path, cfg_factory, pipeline):
    """El plan dice que Pepsi no cambia, pero los datos se generaron con +35 %: aviso."""
    cfg = cfg_factory(n=10_000, chunk_size=4000)
    work, _, _, _ = pipeline(tmp_path, cfg)
    gen = json.loads(work.generate_json.read_text())
    gen["config"]["brands"] = {b: 0.0 for b in gen["config"]["brands"]}
    work.generate_json.write_text(json.dumps(gen))
    h7 = _status(run_checks(work, allow_fake_nlp=True, candidate=_candidates(FakeEnvelopes(cfg.seed))), "H7")
    assert h7["status"] == AVISO and "pepsi" in h7["detail"], h7


def test_motor_falso_no_vale_para_el_lote(fake_run):
    work, _, _, _ = fake_run
    h11 = _status(run_checks(work), "H11")
    assert h11["status"] == FALLA and "beam" in h11["detail"]


def test_event_id_formula():
    assert event_id_formula("historical", "reddit", 3101, 0) != event_id_formula("stream", "reddit", 3101, 0)
    assert len(event_id_formula("historical", "reddit", 3101, 0)) == 36


def _rewrite(work, key, mutate):
    path = work.local_path(key)
    table = pq.read_table(path)
    rows = table.to_pylist()
    mutate(rows)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), buf)
    path.write_bytes(buf.getvalue())
    out = json.loads(work.output_json.read_text())
    import hashlib

    for f in out["files"]:
        if f["key"] == key:
            f["size"] = len(buf.getvalue())
            f["sha256"] = hashlib.sha256(buf.getvalue()).hexdigest()
    work.output_json.write_text(json.dumps(out))


@pytest.mark.parametrize("mutate, cid, text", [
    (lambda rows: rows[0].update(payload=json.dumps({"kind": "comment", "user_id": "u_1"})),
     "H9", "payload"),
    (lambda rows: rows[0].update(user_hash="XYZ"), "H9", "hex"),
    (lambda rows: rows[0].update(sentiment_label=None), "H11", "sin NLP"),
    (lambda rows: rows[0].update(emotion_label="happiness"), "H11", "enums"),
    (lambda rows: rows[0].update(nlp_model_version="otro@1"), "H11", "versiones"),
    (lambda rows: rows[0].update(brand_primary="coca"), "H12", "brand_primary"),
    (lambda rows: rows[0].update(hashtags=["#Postobon"]), "H12", "hashtags"),
    (lambda rows: rows[0].update(origin="stream"), "H3", "origin"),
    (lambda rows: rows[0].update(event_id="00000000-0000-0000-0000-000000000000"), "H4", "fórmula"),
])
def test_cada_regla_detecta_su_defecto(fake_run, mutate, cid, text):
    work, _, _, out = fake_run
    key = out["files"][0]["key"]
    _rewrite(work, key, mutate)
    check = _status(run_checks(work, allow_fake_nlp=True), cid)
    assert check["status"] == FALLA and text in check["detail"], check


def test_particion_equivocada(fake_run):
    work, _, _, out = fake_run
    src = work.local_path(out["files"][0]["key"])
    dst = work.local_path(out["files"][1]["key"])
    dst.write_bytes(src.read_bytes())
    report = run_checks(work, allow_fake_nlp=True)
    assert _status(report, "H1")["status"] == FALLA


def test_archivo_sobrante(fake_run):
    from historical.work import part_key

    work, _, _, _ = fake_run
    extra = work.local_path(part_key(work.seed, "reddit", "2026-01-01", "00"))
    extra.parent.mkdir(parents=True, exist_ok=True)
    extra.write_bytes(b"x")
    assert _status(run_checks(work, allow_fake_nlp=True), "H1")["status"] == FALLA


def test_columnas_de_referencia(fake_run):
    from historical.testing import COLUMNS

    work, _, _, _ = fake_run
    assert _status(run_checks(work, columns=list(COLUMNS), allow_fake_nlp=True), "H2")["status"] == OK
    wrong = list(COLUMNS)
    wrong[0], wrong[1] = wrong[1], wrong[0]
    assert _status(run_checks(work, columns=wrong, allow_fake_nlp=True), "H2")["status"] == FALLA


def test_chequeos_externos_se_suman(fake_run):
    work, _, _, _ = fake_run
    seen = {}

    def bq(hashes, versions):
        seen["bq"] = (len(hashes), versions)
        return Check("H13", "Cruce con el stream", OK, "ok")

    def s3(files):
        seen["s3"] = len(files)
        return Check("H14", "Objetos en S3", AVISO, "aviso")

    report = run_checks(work, allow_fake_nlp=True, bq_check=bq, s3_check=s3)
    assert [c["id"] for c in report["checks"]][-2:] == ["H13", "H14"]
    assert report["status"] == AVISO
    assert seen["bq"][0] <= 500 and seen["s3"] == len(json.loads(work.output_json.read_text())["files"])


def test_sin_etapas_falla(tmp_path):
    from historical.work import WorkDir

    report = run_checks(WorkDir(tmp_path, 1))
    assert report["status"] == FALLA and report["checks"][0]["id"] == "H0"
