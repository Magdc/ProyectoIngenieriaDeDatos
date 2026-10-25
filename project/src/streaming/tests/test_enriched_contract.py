"""Rama enriched contra ``contracts/`` reales (Lote 2.2). Se omite si falta ``contracts/``.

* La DDL de ``contracts/bigquery/posts_enriched.sql`` tiene las mismas columnas y tipos que
  ``common.enriched_row`` (y que el schema de Terraform, ver ``test_enriched_row.py``).
* ``contracts/athena/posts_raw.sql`` tiene las mismas columnas (lee el Parquet de ``enriched/``).
* Los envelopes válidos de ``contracts/examples/valid/`` enriquecidos, con un NLP de ejemplo
  tomado de un enriquecido válido, cumplen ``envelope.enriched.v1.schema.json``.
"""

from __future__ import annotations

import json
import re
import warnings
from pathlib import Path

import pytest

from common.enriched_row import COLUMN_NAMES, NLP_FIELDS, POSTS_ENRICHED_COLUMNS, enrich_envelope, row_problems, to_posts_enriched_row
from streaming.enriched_testing import TEST_KEY

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
PT = "2026-10-17T14:03:13.900Z"

pytestmark = pytest.mark.skipif(not CONTRACTS.is_dir(), reason="falta contracts/")


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


def _column_block(sql: str) -> str:
    sql = _strip_comments(sql)
    m = re.search(r"\bCREATE\b[^(]*\(", sql, flags=re.I)
    assert m, "no se encontró CREATE … ("
    depth, start = 1, m.end()
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
    raise AssertionError("paréntesis sin cerrar en la DDL")


def _split_top(block: str) -> list[str]:
    parts, depth, angle, cur = [], 0, 0, []
    for ch in block:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "<":
            angle += 1
        elif ch == ">":
            angle -= 1
        if ch == "," and depth == 0 and angle == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def parse_columns(sql: str) -> dict[str, tuple[str, bool]]:
    """``{columna: (TIPO, not_null)}`` de la primera sentencia CREATE."""
    cols: dict[str, tuple[str, bool]] = {}
    for piece in _split_top(_column_block(sql)):
        if re.match(r"(PRIMARY|FOREIGN|CONSTRAINT)\b", piece, flags=re.I):
            continue
        m = re.match(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\s+([A-Za-z0-9_]+(?:\s*<[^>]*>)?)", piece)
        if not m:
            continue
        name, kind = m.group(1).lower(), re.sub(r"\s+", "", m.group(2)).upper()
        cols[name] = (kind, bool(re.search(r"\bNOT\s+NULL\b", piece, flags=re.I)))
    return cols


def _bq_type(c) -> str:
    return f"ARRAY<{c.bq_type}>" if c.repeated else c.bq_type


def test_parser_de_ddl():
    sql = """-- comentario
    CREATE TABLE IF NOT EXISTS `{{GCP_PROJECT}}.ta_stream.x` (
      a STRING NOT NULL OPTIONS(description="uno, dos"),
      b ARRAY<STRING>,
      c JSON
    ) PARTITION BY DATE(a) CLUSTER BY a;"""
    assert parse_columns(sql) == {"a": ("STRING", True), "b": ("ARRAY<STRING>", False), "c": ("JSON", False)}


def test_ddl_bigquery_posts_enriched_coincide():
    path = CONTRACTS / "bigquery" / "posts_enriched.sql"
    ddl = parse_columns(path.read_text(encoding="utf-8"))
    mine = {c.name: c for c in POSTS_ENRICHED_COLUMNS}
    assert set(ddl) == set(mine), f"columnas distintas: {sorted(set(ddl) ^ set(mine))}"
    aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "ARRAY<INT64>": "ARRAY<INT64>"}
    for name, (kind, not_null) in ddl.items():
        assert aliases.get(kind, kind) == _bq_type(mine[name]), f"{name}: DDL {kind} vs {_bq_type(mine[name])}"
        # Terraform no puede ser más estricto que el contrato (rechazaría filas válidas).
        if mine[name].required and not not_null:
            pytest.fail(f"{name} es REQUIRED en Terraform pero nullable en la DDL")
        if not_null and not mine[name].required and not mine[name].repeated:
            warnings.warn(f"{name} es NOT NULL en la DDL y NULLABLE en Terraform (compatible)")


def test_ddl_athena_posts_raw_tiene_las_mismas_columnas():
    path = CONTRACTS / "athena" / "posts_raw.sql"
    cols = parse_columns(path.read_text(encoding="utf-8"))
    assert set(cols) == set(COLUMN_NAMES), f"distintas: {sorted(set(cols) ^ set(COLUMN_NAMES))}"


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #


def _registry():
    referencing = pytest.importorskip("referencing")
    pytest.importorskip("jsonschema")
    from referencing.jsonschema import DRAFT202012

    resources = []
    schemas = {}
    for path in CONTRACTS.glob("*.schema.json"):
        doc = json.loads(path.read_text(encoding="utf-8"))
        schemas[path.name] = doc
        res = referencing.Resource.from_contents(doc, default_specification=DRAFT202012)
        resources.append((path.name, res))
        if "$id" in doc:
            resources.append((doc["$id"], res))
    return referencing.Registry().with_resources(resources), schemas


def _validator(name):
    import jsonschema

    registry, schemas = _registry()
    schema = schemas[name]
    cls = jsonschema.validators.validator_for(schema)
    return cls(schema, registry=registry)


def _examples():
    return sorted((CONTRACTS / "examples" / "valid").glob("*.json"))


def _load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_envelopes_validos_enriquecidos_cumplen_el_schema_enriquecido():
    raw_v = _validator("envelope.v1.schema.json")
    enr_v = _validator("envelope.enriched.v1.schema.json")
    docs = [_load(p) for p in _examples()]
    raws = [d for d in docs if isinstance(d, dict) and raw_v.is_valid(d)]
    enriched_examples = [d for d in docs if isinstance(d, dict) and enr_v.is_valid(d)]
    if not raws or not enriched_examples:
        pytest.skip("contracts/examples/valid/ no tiene envelopes crudos y enriquecidos")
    nlp = {k: enriched_examples[0][k] for k in NLP_FIELDS}
    for env in raws:
        out = enrich_envelope(env, TEST_KEY, PT)
        out.update(nlp)
        errors = sorted(enr_v.iter_errors(out), key=lambda e: list(e.path))
        assert not errors, f"{env['source']}/{env['payload'].get('kind')}: {errors[0].message}"
        assert row_problems(to_posts_enriched_row(out)) == []


def test_enriquecidos_validos_se_aplanan_sin_problemas():
    enr_v = _validator("envelope.enriched.v1.schema.json")
    found = 0
    for path in _examples():
        doc = _load(path)
        if isinstance(doc, dict) and enr_v.is_valid(doc):
            found += 1
            row = to_posts_enriched_row(doc)
            assert row_problems(row) == [], path.name
    if not found:
        pytest.skip("sin ejemplos de envelope enriquecido")
