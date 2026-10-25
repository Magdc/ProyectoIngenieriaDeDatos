"""Opciones y valores por defecto de la rama enriched (Lote 2.2)."""

from __future__ import annotations

import pytest
from apache_beam.options.pipeline_options import PipelineOptions

from streaming.enriched import EnrichedConfigError, resolve_enriched_config


def _cfg(argv, values=None, env=None):
    return resolve_enriched_config(PipelineOptions(argv), environ=env or {}, pipeline_values=values or {})


def test_local_directrunner_usa_lake_local_y_llave_de_desarrollo():
    cfg = _cfg([], {"output_root": "/lake/", "raw_window_seconds": 60})
    assert cfg.enabled and cfg.root == "/lake" and cfg.window_seconds == 60
    assert cfg.bq_table is None
    assert cfg.key_source.kind == "local_dev"


def test_dataflow_por_defecto_bigquery_y_secret_manager():
    cfg = _cfg(["--runner=DataflowRunner", "--project=p1"], {"raw_window_seconds": 300})
    assert cfg.root == "gs://p1-ta-lake"
    assert cfg.bq_table == "p1:ta_stream.posts_enriched"
    assert cfg.key_source.kind == "secret" and cfg.key_source.project == "p1" and cfg.key_source.secret_name == "ta-hmac-key"
    assert cfg.window_seconds == 300


def test_nombre_del_secreto_desde_el_entorno_y_proyecto_propio():
    cfg = _cfg(["--runner=DataflowRunner", "--project=p1", "--hmac_secret_project=p2"], env={"TA_HMAC_SECRET_NAME": "otra"})
    assert cfg.key_source.project == "p2" and cfg.key_source.secret_name == "otra"


def test_salida_en_gcs_exige_secret_manager_aunque_sea_directrunner():
    cfg = _cfg(["--project=p1"], {"output_root": "gs://p1-ta-lake"})
    assert cfg.key_source.kind == "secret"


def test_hmac_key_env_solo_fuera_de_dataflow():
    assert _cfg(["--hmac_key_env=MI_LLAVE"], {"output_root": "/l"}).key_source.kind == "env"
    with pytest.raises(EnrichedConfigError):
        _cfg(["--runner=DataflowRunner", "--project=p", "--hmac_key_env=MI_LLAVE"])


@pytest.mark.parametrize("value,expected", [("none", None), ("ds.t", "p1:ds.t"), ("x.ds.t", "x:ds.t"), ("x:ds.t", "x:ds.t")])
def test_tabla_explicita(value, expected):
    cfg = _cfg([f"--posts_enriched_table={value}", "--project=p1"], {"output_root": "/l"})
    assert cfg.bq_table == expected


def test_ventana_invalida_y_sin_salida():
    with pytest.raises(EnrichedConfigError):
        _cfg(["--enriched_window_seconds=7"], {"output_root": "/l"})
    with pytest.raises(EnrichedConfigError):
        _cfg(["--enriched_window_seconds=420"], {"output_root": "/l"})
    with pytest.raises(EnrichedConfigError):
        _cfg(["--runner=DataflowRunner", "--hmac_secret_project=p"], {})  # sin proyecto ni raíz: sin salida


def test_sin_salida_fuera_de_dataflow_se_apaga():
    """Pruebas del Lote 2.1 con TestStream sin --output_root: la rama no rompe nada."""
    assert not _cfg([], {}).enabled


def test_apagada():
    cfg = _cfg(["--enriched_enabled=false"], {"output_root": "/l"})
    assert not cfg.enabled


def test_enriched_root_explicito_gana():
    cfg = _cfg(["--enriched_root=/otro"], {"output_root": "/l"})
    assert cfg.root == "/otro"
