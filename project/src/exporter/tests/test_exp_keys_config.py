from datetime import datetime, timezone

import pytest

from exporter.config import ConfigError, load_config
from exporter.keys import Partition, parse_gcs_key, parse_s3_key, source_prefix, start_offset, to_s3_key


def test_mapea_enriched_a_raw_stream_con_el_mismo_nombre():
    key = "enriched/source=reddit/dt=2026-10-24/hr=13/part-20261024T130500Z-p000.parquet"
    assert to_s3_key(key) == "raw/origin=stream/source=reddit/dt=2026-10-24/hr=13/part-20261024T130500Z-p000.parquet"
    parsed = parse_gcs_key(key)
    assert parsed.partition == Partition("reddit", "2026-10-24", "13")
    assert parse_s3_key(parsed.s3_key).gcs_key == key


@pytest.mark.parametrize(
    "key",
    [
        "raw/source=reddit/dt=2026-10-24/hr=13/part-x.jsonl.gz",
        "enriched/source=twitter/dt=2026-10-24/hr=13/part-x.parquet",
        "enriched/source=reddit/dt=2026-13-24/hr=13/part-x.parquet",
        "enriched/source=reddit/dt=2026-10-24/hr=24/part-x.parquet",
        "enriched/source=reddit/dt=2026-10-24/hr=13/part-x.parquet.tmp",
        "enriched/source=reddit/dt=2026-10-24/hr=13/sub/part-x.parquet",
        "enriched/_probe/x.txt",
        "export-state/watermark.json",
    ],
)
def test_no_reconoce_lo_que_no_es_enriched(key):
    assert parse_gcs_key(key) is None
    with pytest.raises(ValueError):
        to_s3_key(key)


def test_start_offset_ordena_como_el_tiempo():
    t = datetime(2026, 10, 24, 9, 59, tzinfo=timezone.utc)
    off = start_offset("facebook", t)
    assert off == "enriched/source=facebook/dt=2026-10-24/hr=09/"
    later = "enriched/source=facebook/dt=2026-10-24/hr=10/part-a.parquet"
    earlier = "enriched/source=facebook/dt=2026-10-24/hr=08/part-a.parquet"
    assert later >= off and earlier < off
    with pytest.raises(ValueError):
        source_prefix("tiktok")


def test_config_por_defecto(env):
    cfg = load_config(env)
    assert cfg.gcs_bucket == "project-0568d9ba-848f-413c-88b-ta-lake"
    assert cfg.sa_email == "sa-exporter@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com"
    assert cfg.state_key == "export-state/watermark.json"
    assert cfg.audience == "ta-gcp-exporter-role"
    assert cfg.aws_region == "us-east-1"
    assert cfg.workers == 4 and cfg.max_objects == 5000 and cfg.overlap_seconds == 3600
    assert cfg.sources == ("reddit", "facebook", "instagram")


def test_config_exige_lo_necesario(env):
    env.pop("TA_LAKE_KMS_KEY_ARN")
    with pytest.raises(ConfigError, match="TA_LAKE_KMS_KEY_ARN"):
        load_config(env)
    assert load_config(env, require_upload=False).kms_key_arn is None
    env.pop("TA_LAKE_BUCKET")
    with pytest.raises(ConfigError, match="TA_LAKE_BUCKET"):
        load_config(env, require_upload=False)


@pytest.mark.parametrize(
    "name,value,match",
    [
        ("TA_LAKE_KMS_KEY_ARN", "alias/ta-dev-lake", "ARN de la llave"),
        ("TA_EXPORTER_ROLE_ARN", "ta-gcp-exporter-role", "ARN de rol"),
        ("TA_EXPORT_STATE_KEY", "enriched/wm.json", "export-state/"),
        ("TA_EXPORT_WORKERS", "0", "entre 1 y 10"),
        ("TA_EXPORT_WORKERS", "16", "entre 1 y 10"),
        ("TA_EXPORT_WORKERS", "muchos", "entero"),
        ("TA_EXPORT_SOURCES", "reddit,tiktok", "desconocidas"),
    ],
)
def test_config_rechaza_valores_invalidos(env, name, value, match):
    env[name] = value
    with pytest.raises(ConfigError, match=match):
        load_config(env)


def test_config_acepta_llaves_multirregion(env):
    env["TA_LAKE_KMS_KEY_ARN"] = "arn:aws:kms:us-east-1:648617469176:key/mrk-1234abcd12ab34cd56ef1234567890ab"
    assert load_config(env).kms_key_arn.endswith("mrk-1234abcd12ab34cd56ef1234567890ab")


def test_config_no_tiene_secretos(cfg):
    public = cfg.public()
    assert not any("secret" in k.lower() or "token" in k.lower() for k in public)
