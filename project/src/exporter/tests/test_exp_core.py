from datetime import timedelta

import pytest

from exporter.core import run_export
from exporter.testing import T0, FakeGcs, FakeS3, gcs_key
from exporter.watermark import WATERMARK_SCHEMA, WatermarkError


def _lake(n_per_source=4, start=T0):
    gcs = FakeGcs()
    for i in range(n_per_source):
        when = start + timedelta(minutes=5 * i)
        for j, source in enumerate(("reddit", "facebook", "instagram")):
            gcs.add(gcs_key(source, when), f"{source}-{i}".encode() * (10 + j), when + timedelta(minutes=6))
    return gcs


def _clock(t):
    return lambda: t


def test_primera_corrida_sube_todo_y_escribe_el_watermark(cfg):
    gcs = _lake()
    gcs.add("enriched/source=reddit/dt=2026-10-24/hr=12/notas.txt", b"x", T0)
    s3 = FakeS3(kms_key_arn=cfg.kms_key_arn)
    rep = run_export(cfg, gcs, s3, now=_clock(T0 + timedelta(hours=1)))
    assert rep.ok and rep.uploaded == 12 and rep.listed == 13 and rep.unrecognized == 1
    assert sorted(s3.objects) == sorted(k.replace("enriched/", "raw/origin=stream/") for k in gcs.objects if k.endswith(".parquet"))
    put = s3.puts[0]
    assert put["crc32c"] and put["metadata"]["exporter"] == "gcs-to-s3@1.0" and "gcs-generation" in put["metadata"]
    state, _ = gcs.read_state(cfg.state_key)
    assert state["schema"] == WATERMARK_SCHEMA
    newest = max(o[1] for o in gcs.objects.values() if True)
    assert rep.watermark_after == state["cursor_time_created"]
    assert state["cursor_time_created"].startswith(f"{newest:%Y-%m-%dT%H:%M}")
    assert state["last_run"]["uploaded"] == 12
    assert rep.by_source["reddit"] == {"listed": 5, "candidates": 4, "uploaded": 4}


def test_segunda_corrida_no_sube_nada(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    run_export(cfg, gcs, s3)
    puts = len(s3.puts)
    rep = run_export(cfg, gcs, s3)
    assert rep.ok and rep.uploaded == 0 and rep.skipped_same_size == rep.candidates > 0
    assert len(s3.puts) == puts and rep.watermark_written


def test_un_objeto_nuevo_sube_solo_ese(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    run_export(cfg, gcs, s3)
    late = T0 + timedelta(minutes=40)
    name = gcs_key("instagram", late)
    gcs.add(name, b"nuevo", late + timedelta(minutes=6))
    rep = run_export(cfg, gcs, s3)
    assert rep.uploaded == 1 and rep.uploaded_keys == [name.replace("enriched/", "raw/origin=stream/")]


def test_el_solape_recupera_un_objeto_que_aparecio_tarde(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    first = run_export(cfg, gcs, s3)
    cursor_minus = T0 + timedelta(minutes=15)  # anterior al cursor pero dentro de 1 h
    name = gcs_key("reddit", T0 + timedelta(minutes=10), panel=1)
    gcs.add(name, b"panel tardio", cursor_minus)
    rep = run_export(cfg, gcs, s3)
    assert first.watermark_after > cursor_minus.isoformat()[:19]
    assert rep.uploaded == 1


def test_lo_anterior_al_solape_no_se_vuelve_a_mirar(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    run_export(cfg, gcs, s3)
    name = gcs_key("reddit", T0 - timedelta(hours=5))
    gcs.add(name, b"viejo", T0 - timedelta(hours=5))
    rep = run_export(cfg, gcs, s3)
    assert rep.uploaded == 0
    # y el listado empieza en la partición de cursor − solape − margen, no al principio
    offsets = [off for _, off in gcs.list_calls[-3:]]
    assert all(off and "/dt=2026-10-24/hr=09/" in off for off in offsets)


def test_un_panel_tardio_en_una_particion_anterior_dentro_del_margen(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    run_export(cfg, gcs, s3)
    # ventana de las 11:55 (partición hr=11) cuyo archivo se crea mucho después
    name = gcs_key("facebook", T0 - timedelta(minutes=5), panel=2)
    gcs.add(name, b"tardio", T0 + timedelta(minutes=50))
    rep = run_export(cfg, gcs, s3)
    assert rep.uploaded == 1 and "hr=11" in rep.uploaded_keys[0]


def test_una_falla_no_mueve_el_watermark_y_se_reintenta(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    bad = sorted(gcs.objects)[0]
    gcs.fail_download.add(bad)
    rep = run_export(cfg, gcs, s3)
    assert not rep.ok and len(rep.failed) == 1 and rep.failed[0]["name"] == bad
    assert rep.uploaded == 11 and not rep.watermark_written
    assert gcs.read_state(cfg.state_key) == (None, 0)
    gcs.fail_download.clear()
    rep2 = run_export(cfg, gcs, s3)
    assert rep2.ok and rep2.uploaded == 1 and rep2.skipped_same_size == 11


def test_cifrado_inesperado_es_una_falla(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn, wrong_sse=True)
    rep = run_export(cfg, gcs, s3)
    assert not rep.ok and len(rep.failed) == 3 and "aws:kms" in rep.failed[0]["error"]
    assert not rep.watermark_written


def test_llave_distinta_es_una_falla(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn="arn:aws:kms:us-east-1:648617469176:key/otra")
    rep = run_export(cfg, gcs, s3)
    assert not rep.ok and "llave del lake" in rep.failed[0]["error"]


def test_tamano_distinto_en_s3_se_sobrescribe(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn)
    name = sorted(gcs.objects)[0]
    s3.objects[name.replace("enriched/", "raw/origin=stream/")] = {"body": b"corto", "metadata": {}}
    rep = run_export(cfg, gcs, s3)
    assert rep.ok and rep.overwritten == 1 and rep.uploaded == 2


def test_corte_por_max_objects_sigue_en_la_proxima(cfg):
    cfg = cfg.with_overrides(max_objects=5)
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    rep = run_export(cfg, gcs, s3)
    assert rep.truncated and rep.uploaded == 5 and rep.pending == 7 and rep.watermark_written
    rep2 = run_export(cfg, gcs, s3)
    rep3 = run_export(cfg, gcs, s3)
    assert rep2.uploaded == 5 and rep3.uploaded == 2 and not rep3.truncated
    assert len(s3.objects) == 12


def test_plan_no_escribe_nada(cfg):
    gcs, s3 = _lake(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    rep = run_export(cfg, gcs, s3, dry_run=True)
    assert rep.dry_run and rep.to_upload == 12 and len(rep.planned_keys) == 12
    assert not s3.puts and gcs.read_state(cfg.state_key) == (None, 0) and not gcs.downloads


def test_s3_se_lista_solo_en_las_particiones_candidatas(cfg):
    gcs, s3 = _lake(2), FakeS3(kms_key_arn=cfg.kms_key_arn)
    rep = run_export(cfg, gcs, s3)
    assert rep.partitions_checked == 3
    assert sorted(s3.list_calls) == sorted(
        f"raw/origin=stream/source={s}/dt=2026-10-24/hr=12/" for s in ("reddit", "facebook", "instagram")
    )


def test_conflicto_de_watermark_se_queda_con_el_cursor_mayor(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn)
    gcs.conflicts_on_write = 1
    rep = run_export(cfg, gcs, s3)
    assert rep.ok and rep.watermark_conflicts == 1 and rep.watermark_written


def test_watermark_corrupto_detiene_la_corrida(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn)
    gcs.state[cfg.state_key] = ({"schema": "otro"}, 5)
    with pytest.raises(WatermarkError):
        run_export(cfg, gcs, s3)
    assert not s3.puts


def test_solo_lista_enriched(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn)
    gcs.add("raw/source=reddit/dt=2026-10-24/hr=12/part-x.jsonl.gz", b"handle", T0)
    run_export(cfg, gcs, s3)
    assert all(prefix.startswith("enriched/source=") for prefix, _ in gcs.list_calls)
    assert "raw/source=reddit/dt=2026-10-24/hr=12/part-x.jsonl.gz" not in gcs.downloads


def test_reporte_json_no_trae_secretos(cfg):
    gcs, s3 = _lake(1), FakeS3(kms_key_arn=cfg.kms_key_arn)
    data = run_export(cfg, gcs, s3).to_json()
    assert data["ok"] and data["uploaded"] == 3 and data["failed_count"] == 0
    text = str(data).lower()
    assert "secret" not in text and "token" not in text
