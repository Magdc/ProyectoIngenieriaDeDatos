"""Etapa 3 (write): particiones por event_time, nombres deterministas e idempotencia."""

from __future__ import annotations

import pytest

pq = pytest.importorskip("pyarrow.parquet")

from historical.testing import FakeNlpEngine, fake_rows_to_parquet, fake_to_row  # noqa: E402
from historical.work import PART_RE, part_key  # noqa: E402
from historical.writer import WriteError, partition_of, write_output  # noqa: E402


def test_partition_of():
    assert partition_of("2026-05-01T13:02:03.045Z") == ("2026-05-01", "13")
    with pytest.raises(WriteError):
        partition_of("2026-05-01 13:02")


def test_part_key():
    key = part_key(3101, "reddit", "2026-05-01", "07")
    assert key == ("raw/origin=historical/source=reddit/dt=2026-05-01/hr=07/"
                   "part-historical-s3101-20260501T07.parquet")
    assert PART_RE.match(key)


def test_archivos_por_hora_del_evento(fake_run):
    work, gen, _, out = fake_run
    assert out["rows"] == 900 and sum(f["rows"] for f in out["files"]) == 900
    for f in out["files"][:50]:
        table = pq.read_table(work.local_path(f["key"]))
        rows = table.to_pylist()
        assert [r["event_id"] for r in rows] == sorted(r["event_id"] for r in rows)
        for r in rows:
            assert r["event_time"].strftime("%Y-%m-%d") == f["dt"]
            assert r["event_time"].strftime("%H") == f["hr"]
            assert r["source"] == f["source"]
            assert r["sentiment_label"] is not None


def test_reescribir_no_cambia_nada(fake_run):
    work, _, _, out = fake_run
    again = write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet)
    assert again["written"] == 0 and again["unchanged"] == out["n_files"]
    assert [f["sha256"] for f in again["files"]] == [f["sha256"] for f in out["files"]]


def test_borra_archivos_viejos_de_la_misma_semilla(fake_run):
    work, _, _, _ = fake_run
    stale = work.local_path(part_key(work.seed, "reddit", "2020-01-01", "00"))
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"viejo")
    other = work.local_path(part_key(work.seed + 1, "reddit", "2020-01-01", "00"))
    other.write_bytes(b"otra semilla")
    res = write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet)
    assert not stale.exists() and other.exists()
    assert len(res["removed"]) == 1


def test_se_niega_sin_nlp_completo(tmp_path, small_cfg, pipeline):
    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    victim = work.stage2_files()[0]
    victim.unlink()
    with pytest.raises(WriteError, match="faltan 1 archivos de NLP"):
        write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet)


def test_se_niega_con_filas_sin_nlp(tmp_path, small_cfg, pipeline):
    from historical.nlp_batch import run_nlp_stage

    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    for side in work.stage2.glob("*.json"):
        side.unlink()
    run_nlp_stage(work, FakeNlpEngine(fail_first=2), to_row=fake_to_row, retry_failed=False)
    with pytest.raises(WriteError, match="sin NLP"):
        write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet)
    res = write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet,
                       allow_missing_nlp=True)
    assert res["missing_nlp"] == 2


def test_valida_el_enriquecido(tmp_path, small_cfg, pipeline):
    work, _, _, _ = pipeline(tmp_path, small_cfg, write=False)
    with pytest.raises(WriteError, match="envelope.enriched.v1"):
        write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet,
                     validate_enriched=lambda e: ["likes: no es entero"])
