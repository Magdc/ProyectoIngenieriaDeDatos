from datetime import timedelta

from exporter.core import run_export
from exporter.probe import ALLOW, DENY, probe_aws, probe_gcs, summarize
from exporter.testing import T0, FakeExporterGcs, FakeGcs, FakeRoleS3Client, FakeS3, gcs_key
from exporter.verify import sample_keys, verify_export

ARN = "arn:aws:sts::648617469176:assumed-role/ta-gcp-exporter-role/ta-gcs-to-s3-x"


def test_sondas_de_aws_con_los_permisos_del_rol():
    client = FakeRoleS3Client()
    results = probe_aws(client, "b", "r1", ARN)
    by_id = {r.id: r for r in results}
    assert summarize(results)["ok"], [r for r in results if not r.ok]
    assert by_id["A2"].got == ALLOW and by_id["A3"].got == DENY and by_id["A6"].got == "sin objetos"
    # ninguna sonda escribe donde el rol puede escribir
    assert not any(op == "put" and key.startswith("raw/origin=stream/") for op, key in client.calls)


def test_sondas_de_aws_detectan_un_rol_demasiado_amplio():
    client = FakeRoleS3Client(allow_prefix="")
    results = {r.id: r for r in probe_aws(client, "b", "r1", ARN)}
    assert not results["A3"].ok and not results["A4"].ok and not results["A5"].ok
    other = {r.id: r for r in probe_aws(FakeRoleS3Client(), "b", "r1", "arn:aws:sts::1:assumed-role/otro/x")}
    assert not other["A1"].ok


STREAM_KEY = "raw/origin=stream/source=reddit/dt=2026-10-24/hr=12/part-20261024T120000Z-p000.parquet"


def test_a6_con_un_objeto_existente_exige_access_denied():
    results = {r.id: r for r in probe_aws(FakeRoleS3Client(existing=(STREAM_KEY,)), "b", "r1", ARN)}
    assert results["A6"].ok and results["A6"].got == DENY and STREAM_KEY in results["A6"].detail
    leaky = {r.id: r for r in probe_aws(FakeRoleS3Client(existing=(STREAM_KEY,), allow_get=True), "b", "r1", ARN)}
    assert not leaky["A6"].ok and leaky["A6"].got == ALLOW


def test_a6_sin_objetos_no_confunde_nosuchkey_con_lectura():
    """Visto en la validación: con ListBucket en el prefijo, S3 responde NoSuchKey a una clave
    que no existe aunque el rol no tenga GetObject."""
    results = {r.id: r for r in probe_aws(FakeRoleS3Client(), "b", "r1", ARN)}
    assert results["A6"].ok and results["A6"].got == "sin objetos" and "NoSuchKey" in results["A6"].detail
    assert [r.id for r in probe_aws(FakeRoleS3Client(), "b", "r1", ARN)] == ["A1", "A2", "A3", "A4", "A5", "A6", "A7"]


def test_sondas_de_gcs_con_los_permisos_de_sa_exporter():
    gcs = FakeExporterGcs()
    gcs.add(gcs_key("reddit", T0), b"pq", T0)
    gcs.add("raw/source=reddit/dt=2026-10-24/hr=12/part-x.jsonl.gz", b"raw", T0)
    results = probe_gcs(gcs, "r1")
    assert summarize(results)["ok"], [r for r in results if not r.ok]
    assert [r.id for r in results] == ["G1", "G2", "G3", "G4", "G5", "G6"]
    assert gcs.deleted == ["export-state/_probe/r1.json"]  # no deja nada


def test_sondas_de_gcs_detectan_lectura_de_raw():
    gcs = FakeExporterGcs(raw_read=True, enriched_write=True)
    gcs.add(gcs_key("reddit", T0), b"pq", T0)
    gcs.add("raw/source=reddit/dt=2026-10-24/hr=12/part-x.jsonl.gz", b"raw", T0)
    results = {r.id: r for r in probe_gcs(gcs, "r1")}
    assert not results["G3"].ok and not results["G4"].ok


def test_sondas_de_gcs_sin_raw_no_fallan():
    gcs = FakeExporterGcs()
    gcs.add(gcs_key("reddit", T0), b"pq", T0)
    results = {r.id: r for r in probe_gcs(gcs, "r1")}
    assert results["G3"].ok and "no se pudo probar" in results["G3"].detail


def _exported(cfg, n=2):
    gcs, s3 = FakeGcs(), FakeS3(kms_key_arn=cfg.kms_key_arn)
    for i in range(n):
        when = T0 + timedelta(minutes=5 * i)
        for source in ("reddit", "facebook", "instagram"):
            gcs.add(gcs_key(source, when), f"{source}{i}".encode() * 5, when + timedelta(minutes=6))
    run_export(cfg, gcs, s3)
    return gcs, s3


def _verify(cfg, gcs, s3, sample=10):
    objs = list(gcs.list_objects("enriched/"))
    data, _ = gcs.read_state(cfg.state_key)
    from exporter.watermark import Watermark

    cursor = Watermark.from_json(data).cursor
    index = s3.list_prefix("raw/origin=stream/")
    heads = {k: s3.head(k) for k in sample_keys(index, sample)}
    return verify_export(objs, index, cursor=cursor, heads=heads, kms_key_arn=cfg.kms_key_arn)


def test_verificador_ok_tras_exportar(cfg):
    gcs, s3 = _exported(cfg)
    result = _verify(cfg, gcs, s3)
    assert result["status"] == "OK", result["checks"]
    assert result["gcs_enriched"] == result["s3_stream"] == 6


def test_verificador_detecta_faltantes_tamano_y_claves_raras(cfg):
    gcs, s3 = _exported(cfg)
    keys = sorted(s3.objects)
    s3.objects.pop(keys[0])
    s3.objects[keys[1]]["body"] = b"otro tamano"
    s3.objects["raw/origin=stream/_probe/x.txt"] = {"body": b"x", "metadata": {}}
    checks = {c["id"]: c for c in _verify(cfg, gcs, s3)["checks"]}
    assert checks["V1"]["status"] == "FALLA" and checks["V1"]["missing"] == [keys[0]]
    assert checks["V1"]["size_mismatch"] == [keys[1]]
    assert checks["V3"]["status"] == "FALLA"


def test_verificador_detecta_checksum_distinto(cfg):
    gcs, s3 = _exported(cfg)
    key = sorted(s3.objects)[0]
    s3.objects[key]["crc32c"] = "AAAAAA=="
    s3.objects[key]["metadata"]["gcs-crc32c"] = "AAAAAA=="
    checks = {c["id"]: c for c in _verify(cfg, gcs, s3)["checks"]}
    assert checks["V4"]["status"] == "FALLA" and "CRC32C" in checks["V4"]["problems"][0]


def test_verificador_sin_watermark_falla():
    result = verify_export([], {}, cursor=None, heads={}, kms_key_arn=None)
    assert result["status"] == "FALLA"


def test_muestra_determinista_y_repartida():
    keys = [f"k{i:03d}" for i in range(100)]
    s = sample_keys(keys, 10)
    assert s == sample_keys(list(reversed(keys)), 10) and len(s) == 10 and s[0] == "k000" and s[-1] == "k090"
    assert sample_keys(keys[:3], 10) == keys[:3] and sample_keys(keys, 0) == []
