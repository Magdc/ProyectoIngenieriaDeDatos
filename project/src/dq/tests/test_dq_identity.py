"""HU-032: «un test en CI falla si existen columnas ``handle`` o ``user_id``» (sin Great Expectations: corre también en
``ta-streaming``). Recorre los contratos de todas las capas: las DDL de Athena (Raw, Trusted y Curated), las tablas de
BigQuery del stream y las columnas que el código de cada capa declara."""

from __future__ import annotations

from common import stream_tables
from lakehouse import catalog_spec
from quality import layers as L

IDENTITY = {"handle", "user_id"}


def test_ninguna_tabla_del_lake_tiene_columnas_de_identidad():
    tables = catalog_spec.load_contract_tables()
    assert {t.database for t in tables} == {"ta_raw", "ta_trusted", "ta_curated"} and len(tables) == 11
    for t in tables:
        cols = set(t.column_names) | set(getattr(t, "partition_names", []) or [])
        assert not cols & IDENTITY, f"{t.database}.{t.name}: identidad en claro {cols & IDENTITY}"


def test_ninguna_tabla_de_bigquery_ni_capa_en_codigo_tiene_identidad():
    for name, cols in stream_tables.TABLES.items():
        assert not {c.name for c in cols} & IDENTITY, name
    assert not set(L.RAW_COLUMNS) & IDENTITY and not set(L.TRUSTED_COLUMNS) & IDENTITY
    assert "user_hash" in L.RAW_COLUMNS and "user_hash" in L.TRUSTED_COLUMNS


def test_la_prueba_falla_si_aparece_una_columna_de_identidad():
    # La misma comprobación sobre una tabla inventada con user_id: debe detectarla.
    fake = {"user_hash", "user_id", "text"}
    assert fake & IDENTITY == {"user_id"}
