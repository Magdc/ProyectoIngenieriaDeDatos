"""Catálogo de Glue desde contracts/athena/ (Lote 4.1): parser, tables.json al día y comparación."""

from __future__ import annotations

import json
import shutil

import pytest

from lakehouse import catalog_spec as cs

BUCKET = "ta-dev-648617469176-lake"


@pytest.fixture(scope="module")
def tables():
    return cs.load_contract_tables()


def by_name(tables):
    return {t.full_name: t for t in tables}


def test_tables_json_al_dia_con_las_ddl():
    assert cs.check() == [], "regenere con: cd project/src && python -m lakehouse.catalog_spec --write"


def test_once_tablas_en_tres_bases(tables):
    names = [t.full_name for t in tables]
    assert names[:2] == ["ta_raw.posts_raw", "ta_trusted.posts"]
    assert len(names) == 11 and len(set(names)) == 11
    assert {t.database for t in tables} == {"ta_raw", "ta_trusted", "ta_curated"}
    curated = {t.name for t in tables if t.database == "ta_curated"}
    assert curated == {"fact_posts", "dim_date", "dim_source", "dim_brand", "dim_user", "agg_emotion_by_brand_hour",
                       "agg_trend_daily", "edges_user_brand", "edges_hashtag_cooccurrence"}


def test_posts_raw_es_posts_enriched_columna_a_columna(tables):
    raw = by_name(tables)["ta_raw.posts_raw"]
    from common.enriched_row import COLUMN_NAMES

    assert tuple(raw.column_names) == tuple(COLUMN_NAMES)
    assert raw.partition_names == ["part_origin", "part_source", "part_dt", "part_hr"]
    types = {c.name: c.type for c in raw.columns}
    assert types["event_time"] == "timestamp" and types["brands"] == "array<string>" and types["payload"] == "string"


def test_trusted_es_raw_sin_source_y_con_text_clean(tables):
    raw, trusted = by_name(tables)["ta_raw.posts_raw"], by_name(tables)["ta_trusted.posts"]
    expected = [c for c in raw.column_names if c != "source"]
    expected.insert(expected.index("text") + 1, "text_clean")
    assert trusted.column_names == expected
    assert trusted.partition_names == ["source", "dt"]


def test_proyeccion_cubre_cada_particion(tables):
    problems = [p for t in tables for p in cs.projection_problems(t)]
    assert problems == []
    projected = [t.full_name for t in tables if t.projected]
    assert projected == ["ta_raw.posts_raw", "ta_trusted.posts", "ta_curated.fact_posts"]
    raw = by_name(tables)["ta_raw.posts_raw"]
    assert raw.parameters["projection.part_origin.values"] == "stream,historical"
    assert raw.parameters["projection.part_hr.digits"] == "2"


def test_ubicaciones_por_capa(tables):
    for t in tables:
        layer = t.database.split("_", 1)[1]
        assert t.location.startswith(f"s3://{cs.PLACEHOLDER}/{layer}/"), t.full_name
        assert t.location.endswith("/")


def test_comentarios_y_descripciones(tables):
    t = by_name(tables)
    dim_date = {c.name: c.comment for c in t["ta_curated.dim_date"].columns}
    assert dim_date["date_key"] == "yyyymmdd" and dim_date["year"] == ""
    assert t["ta_curated.fact_posts"].description == "Un registro por post. Grano = event_id."
    assert t["ta_raw.posts_raw"].description.startswith("ta_raw.posts_raw")


def test_parser_tolera_crlf_y_rechaza_lo_ilegible():
    ddl = ("-- Una tabla.\r\nCREATE EXTERNAL TABLE IF NOT EXISTS ta_x.t (\r\n  a string,  -- primera\r\n"
           "  b array<string>\r\n)\r\nPARTITIONED BY (dt string)\r\nSTORED AS PARQUET\r\n"
           "LOCATION 's3://{{lake_bucket}}/x/t/'\r\nTBLPROPERTIES ('projection.enabled' = 'true', "
           "'projection.dt.type' = 'date', 'storage.location.template' = 's3://{{lake_bucket}}/x/t/dt=${dt}/');\r\n")
    (spec,) = cs.parse_ddl(ddl, "x.sql")
    assert spec.full_name == "ta_x.t" and spec.description == "Una tabla."
    assert [(c.name, c.type, c.comment) for c in spec.columns] == [("a", "string", "primera"), ("b", "array<string>", "")]
    assert spec.partition_names == ["dt"] and spec.parameters["projection.dt.type"] == "date"
    with pytest.raises(cs.DdlError):
        cs.parse_ddl("CREATE EXTERNAL TABLE ta_x.t (\n  a string\n)\nSTORED AS ORC\nLOCATION 's3://x/';\n")
    with pytest.raises(cs.DdlError):
        cs.parse_ddl("CREATE EXTERNAL TABLE ta_x.t (\n  a string\n)\nSTORED AS PARQUET;\n")


def test_check_detecta_un_lado_cambiado(tmp_path):
    contracts = tmp_path / "athena"
    shutil.copytree(cs.CONTRACTS_DIR, contracts)
    out = tmp_path / "tables.json"
    assert cs.main(["--write", "--contracts-dir", str(contracts), "--tables-json", str(out)]) == 0
    assert cs.check(contracts, out) == []
    sql = (contracts / "curated.sql").read_text(encoding="utf-8").replace("  max_followers bigint", "  max_followers int")
    (contracts / "curated.sql").write_text(sql, encoding="utf-8")
    assert cs.check(contracts, out) == ["ta_curated.dim_user distinta"]
    assert cs.main(["--check", "--contracts-dir", str(contracts), "--tables-json", str(out)]) == 1
    assert "no existe" in cs.check(contracts, tmp_path / "otro.json")[0]


def test_resolve_y_comparacion_con_glue(tables):
    raw = cs.resolve(by_name(tables)["ta_raw.posts_raw"], BUCKET)
    assert raw.location == f"s3://{BUCKET}/raw/"
    assert raw.parameters["storage.location.template"].startswith(f"s3://{BUCKET}/raw/origin=${{part_origin}}/")
    glue = {
        "Name": "posts_raw", "TableType": "EXTERNAL_TABLE",
        "Parameters": {**raw.parameters, **cs.EXTRA_PARAMETERS},
        "PartitionKeys": [{"Name": c.name, "Type": c.type} for c in raw.partition_keys],
        "StorageDescriptor": {
            "Columns": [{"Name": c.name, "Type": c.type} for c in raw.columns],
            "Location": raw.location, "InputFormat": cs.PARQUET_INPUT,
            "SerdeInfo": {"SerializationLibrary": cs.PARQUET_SERDE},
        },
    }
    spec = by_name(tables)["ta_raw.posts_raw"]
    assert cs.glue_table_problems(spec, glue, BUCKET) == []
    broken = json.loads(json.dumps(glue))
    broken["StorageDescriptor"]["Columns"][0]["Type"] = "int"
    broken["Parameters"]["projection.part_hr.range"] = "0,12"
    broken["Parameters"]["projection.otra.type"] = "enum"
    problems = cs.glue_table_problems(spec, broken, BUCKET)
    assert any("columnas" in p for p in problems)
    assert any("projection.part_hr.range" in p for p in problems)
    assert any("de más" in p for p in problems)


def test_table_por_nombre(tables):
    assert cs.table("ta_trusted.posts", tables).partition_names == ["source", "dt"]
    with pytest.raises(KeyError):
        cs.table("ta_raw.no_existe", tables)


def test_cli_print(capsys):
    assert cs.main(["--print"]) == 0
    assert "ta_raw.posts_raw: 24 columnas" in capsys.readouterr().out
