"""Columnas del Lote 2.4 frente a ``contracts/bigquery/*.sql`` y etiquetas frente a ``common.label_map``.

Se omiten si falta ``contracts/`` o ``common.label_map`` (fuera del repositorio).
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import pytest

from common.stream_tables import TABLES, canonical_type
from streaming.trends_core import EMOTION_LABELS, SENTIMENT_LABELS, TREND_COLUMNS

PROJECT = Path(__file__).resolve().parents[3]
DDL_DIR = PROJECT / "contracts" / "bigquery"


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


def _split_top_level(body: str) -> list[str]:
    items, depth, current = [], 0, []
    for char in body:
        if char in "(<":
            depth += 1
        elif char in ")>":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    items.append("".join(current))
    return [i.strip() for i in items if i.strip()]


def parse_ddl_columns(sql: str, table: str) -> list[tuple[str, str, bool]]:
    """``[(nombre, tipo, not_null)]`` de ``CREATE TABLE …{table} ( … )``."""
    sql = _strip_comments(sql)
    match = re.search(rf"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?[^\s(]*\b{table}`?\s*\(",
                      sql, re.I)
    if not match:
        raise AssertionError(f"no encuentro CREATE TABLE … {table} en la DDL")
    depth, i = 1, match.end()
    while depth:
        depth += {"(": 1, ")": -1}.get(sql[i], 0)
        i += 1
    columns = []
    for item in _split_top_level(sql[match.end():i - 1]):
        tokens = item.replace("`", "").split()
        if not tokens or tokens[0].upper() in {"PRIMARY", "FOREIGN", "CONSTRAINT"}:
            continue
        name = tokens[0]
        type_match = re.match(r"\s*\S+\s+([A-Za-z0-9_]+(?:<[^>]*>)?)", item.replace("`", ""))
        col_type = type_match.group(1) if type_match else ""
        not_null = bool(re.search(r"\bNOT\s+NULL\b", item, re.I))
        columns.append((name, col_type, not_null))
    return columns


def test_parser_de_ddl():
    sql = """-- comentario
    CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.trend_windows` (
      window_start TIMESTAMP NOT NULL OPTIONS(description = "a, b"),
      brand STRING,
      emotion_counts JSON
    ) PARTITION BY DATE(window_start);"""
    assert parse_ddl_columns(sql, "trend_windows") == [
        ("window_start", "TIMESTAMP", True), ("brand", "STRING", False), ("emotion_counts", "JSON", False)]


@pytest.mark.parametrize("table", sorted(TABLES))
def test_columnas_coinciden_con_la_ddl(table):
    path = DDL_DIR / f"{table}.sql"
    if not path.exists():
        pytest.skip(f"no está {path}: la prueba se corre en el repositorio")
    ddl = parse_ddl_columns(path.read_text(encoding="utf-8"), table)
    code = TABLES[table]
    assert {c[0] for c in ddl} == {c.name for c in code}, "columnas distintas entre la DDL y el código"
    ddl_by_name = {name: (col_type, not_null) for name, col_type, not_null in ddl}
    for column in code:
        ddl_type, not_null = ddl_by_name[column.name]
        assert canonical_type(ddl_type) == canonical_type(column.type), (column.name, ddl_type, column.type)
        if column.mode == "REQUIRED":
            assert not_null, f"{table}.{column.name}: REQUIRED en Terraform pero nullable en la DDL"
        elif not_null:
            warnings.warn(f"{table}.{column.name}: NOT NULL en la DDL y NULLABLE en Terraform (compatible)")
    if [c[0] for c in ddl] != [c.name for c in code]:
        warnings.warn(f"{table}: el orden de columnas difiere de la DDL (no afecta las inserciones)")


def test_trend_columns_de_la_fila_son_las_de_la_tabla():
    assert TREND_COLUMNS == tuple(c.name for c in TABLES["trend_windows"])


def test_etiquetas_coinciden_con_label_map():
    label_map = pytest.importorskip("common.label_map", reason="common.label_map no está (Lote 1.1)")
    raw_emotions = ("others", "joy", "sadness", "anger", "surprise", "disgust", "fear")
    assert {label_map.map_emotion(x) for x in raw_emotions} == set(EMOTION_LABELS)
    assert {label_map.map_sentiment(x) for x in ("NEG", "NEU", "POS")} == set(SENTIMENT_LABELS)
