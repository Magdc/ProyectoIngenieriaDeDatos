"""Terraform del Lote 2.4 frente al código y a los contratos de nombres (§5.3, §5.5, §5.8)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from common.stream_tables import TABLES, schema_json

PROJECT = Path(__file__).resolve().parents[3]
MODULE = PROJECT / "infra" / "gcp" / "modules" / "bigquery_stream_tables"
ENV_DIR = PROJECT / "infra" / "gcp" / "environments" / "dev"
TRENDS_TF = ENV_DIR / "trends.tf"


@pytest.mark.parametrize("table", sorted(TABLES))
def test_schema_de_terraform_es_el_del_codigo(table):
    path = MODULE / "schemas" / f"{table}.json"
    assert path.read_text(encoding="utf-8") == schema_json(table), (
        f"{path} no coincide con common/stream_tables.py: regenere con "
        "'python -m common.stream_tables --write infra/gcp/modules/bigquery_stream_tables/schemas'")


def test_schemas_son_json_de_bigquery():
    for table in TABLES:
        fields = json.loads((MODULE / "schemas" / f"{table}.json").read_text(encoding="utf-8"))
        assert all(set(f) == {"name", "type", "mode", "description"} for f in fields)
        assert all(f["mode"] in ("NULLABLE", "REQUIRED") for f in fields)


def _block(text: str, kind: str, name: str) -> str:
    match = re.search(rf'resource\s+"{kind}"\s+"{name}"\s*\{{', text)
    assert match, f"no está {kind}.{name}"
    depth, i = 1, match.end()
    while depth:
        depth += {"{": 1, "}": -1}.get(text[i], 0)
        i += 1
    return text[match.start():i]


@pytest.mark.parametrize("table,field,clustering", [
    ("trend_windows", "window_start", '["brand", "source"]'),
    ("dlq_events", "received_at", '["source", "error_type"]'),
    ("dq_results", "run_ts", '["layer", "kpi"]'),
])
def test_particion_y_clustering(table, field, clustering):
    block = _block((MODULE / "main.tf").read_text(encoding="utf-8"), "google_bigquery_table", table)
    assert f'table_id            = "{table}"' in block
    assert f'field         = "{field}"' in block
    assert f"clustering          = {clustering}" in block
    assert f'schemas/{table}.json' in block


def test_vista_toma_la_version_de_mayor_n_posts():
    block = _block((MODULE / "main.tf").read_text(encoding="utf-8"), "google_bigquery_table", "trend_windows_latest")
    assert "PARTITION BY window_start, brand, source" in block
    assert "ORDER BY n_posts DESC" in block
    assert "WHERE version_rank = 1" in block


def test_trends_tf_usa_los_nombres_del_contrato():
    text = TRENDS_TF.read_text(encoding="utf-8")
    assert 'name    = "ta-dlq-df"' in text
    assert "/topics/ta-dlq\"" in text
    for source in ("reddit", "facebook", "instagram"):
        assert f'"ta-raw-{source}-df"' in text
    assert "sa-dataflow@${var.project_id}.iam.gserviceaccount.com" in text
    assert '"roles/pubsub.viewer"' in text and '"roles/pubsub.subscriber"' in text
    assert 'ttl = ""' in text


def test_sin_valores_del_proyecto_escritos_a_mano():
    for path in [*MODULE.rglob("*.tf"), *ENV_DIR.glob("*trends*.tf")]:
        text = path.read_text(encoding="utf-8")
        assert "project-0568d9ba" not in text, path
        assert "63567159332" not in text, path


def test_outputs_del_lote():
    text = (ENV_DIR / "outputs_trends.tf").read_text(encoding="utf-8")
    for name in ("trend_windows_table", "trend_windows_latest_view", "dlq_events_table", "dq_results_table",
                 "dlq_df_subscription", "dataflow_template_spec", "dataflow_launcher_image"):
        assert f'output "{name}"' in text


def test_outputs_entran_en_un_apply_con_target():
    """Un output que solo usa variables no se guarda con -target: necesita una referencia o depends_on."""
    text = (ENV_DIR / "outputs_trends.tf").read_text(encoding="utf-8")
    for name, body in re.findall(r'output\s+"([a-z0-9_]+)"\s*\{(.*?)\n\}', text, re.S):
        assert re.search(r"\b(module|google_[a-z0-9_]+)\.", body) or "depends_on" in body, name


def _declared_modules() -> set[str]:
    names = set()
    for path in ENV_DIR.glob("*.tf"):
        names.update(re.findall(r'^module\s+"([a-z0-9_]+)"', path.read_text(encoding="utf-8"), re.M))
    return names


@pytest.mark.parametrize("module", ["pubsub", "bigquery_stream"])
def test_depends_on_apunta_a_modulos_que_existen(module):
    if not (ENV_DIR / "platform.tf").exists() or not (ENV_DIR / "streaming.tf").exists():
        pytest.skip("platform.tf o streaming.tf no están: la prueba se corre en el repositorio")
    assert f"module.{module}" in TRENDS_TF.read_text(encoding="utf-8")
    assert module in _declared_modules(), (
        f'trends.tf depende de module.{module}, pero no hay un bloque module "{module}" en '
        f"{ENV_DIR}. Cambie el nombre en trends.tf por el que use platform.tf/streaming.tf.")


def test_variables_y_outputs_no_chocan_con_otros_lotes():
    mine_vars = set(re.findall(r'^variable\s+"([a-z0-9_]+)"', (ENV_DIR / "variables_trends.tf").read_text(), re.M))
    mine_outs = set(re.findall(r'^output\s+"([a-z0-9_]+)"', (ENV_DIR / "outputs_trends.tf").read_text(), re.M))
    others_vars, others_outs = set(), set()
    for path in ENV_DIR.glob("*.tf"):
        if path.name in ("variables_trends.tf", "outputs_trends.tf", "trends.tf"):
            continue
        text = path.read_text(encoding="utf-8")
        others_vars.update(re.findall(r'^variable\s+"([a-z0-9_]+)"', text, re.M))
        others_outs.update(re.findall(r'^output\s+"([a-z0-9_]+)"', text, re.M))
    assert not mine_vars & others_vars
    assert not mine_outs & others_outs
