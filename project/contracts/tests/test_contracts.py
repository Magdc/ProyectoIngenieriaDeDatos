"""Pruebas de los contratos (HU-001, ARCHITECTURE.md §5).

Se pueden correr de dos formas:
    pytest project/contracts/tests -q
    python project/contracts/tests/test_contracts.py        # sin pytest

Dependencias: jsonschema>=4.18 y referencing (ver contracts/requirements.txt).
"""
from __future__ import annotations

import json
import re
import sys
import uuid
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"
SIM_SEED = 7  # SIM_SEED por defecto (§5.1); los ejemplos se generaron con esta semilla

SOURCES = ("reddit", "facebook", "instagram")
# Prefijo del nombre de archivo de un ejemplo -> schema contra el que se valida
SCHEMA_BY_PREFIX = {
    "envelope": "envelope.v1.schema.json",
    "enriched": "envelope.enriched.v1.schema.json",
    "pubsub": "pubsub.attributes.schema.json",
}


# --------------------------------------------------------------------------- helpers
def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _schemas() -> dict[str, dict]:
    return {p.name: _load(p) for p in sorted(ROOT.glob("*.schema.json"))}


def _registry() -> Registry:
    resources = [(s["$id"], Resource.from_contents(s)) for s in _schemas().values()]
    return Registry().with_resources(resources)


def _validator(schema_file: str) -> Draft202012Validator:
    return Draft202012Validator(_schemas()[schema_file], registry=_registry())


def _examples(kind: str):
    """Itera (ruta, schema_file, contenido) de examples/{valid|invalid}."""
    for p in sorted((EXAMPLES / kind).glob("*.json")):
        prefix = p.name.split("__", 1)[0]
        assert prefix in SCHEMA_BY_PREFIX, f"{p.name}: prefijo desconocido '{prefix}'"
        yield p, SCHEMA_BY_PREFIX[prefix], _load(p)


def _strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _parse_ddl(path: Path) -> dict[str, dict]:
    """Parser mínimo para las DDL de este directorio (una columna por línea)."""
    sql = _strip_comments(path.read_text(encoding="utf-8"))
    tables: dict[str, dict] = {}
    for stmt in sql.split(";"):
        m = re.search(r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+IF\s+NOT\s+EXISTS\s+`?([^\s`(]+)`?\s*\((.*?)\n\)", stmt, re.S)
        if not m:
            continue
        name = m.group(1).split(".")[-1]
        columns = _columns(m.group(2))
        pm = re.search(r"PARTITIONED\s+BY\s*\((.*?)\)", stmt, re.S)
        partitions = _columns(pm.group(1)) if pm else []
        props = dict(re.findall(r"'([^']+)'\s*=\s*'([^']*)'", stmt))
        tables[name] = {"columns": columns, "partitions": partitions, "props": props, "raw": stmt}
    return tables


def _columns(block: str) -> list[tuple[str, str]]:
    cols = []
    for line in block.splitlines():
        line = line.strip().rstrip(",").strip()
        if not line:
            continue
        name, _, typ = line.partition(" ")
        cols.append((name, re.sub(r"\s+NOT\s+NULL", "", typ.strip(), flags=re.I).lower()))
    return cols


_BQ_TO_ATHENA = {"string": "string", "int64": "bigint", "float64": "double",
                 "timestamp": "timestamp", "array<string>": "array<string>", "json": "string", "bool": "boolean"}


# --------------------------------------------------------------------------- JSON Schemas
def test_all_schemas_are_valid_draft_2020_12():
    schemas = _schemas()
    expected = {"envelope.v1.schema.json", "envelope.enriched.v1.schema.json", "pubsub.attributes.schema.json",
                "payload.reddit.schema.json", "payload.facebook.schema.json", "payload.instagram.schema.json"}
    assert expected <= set(schemas), f"faltan schemas: {expected - set(schemas)}"
    for name, schema in schemas.items():
        Draft202012Validator.check_schema(schema)
        assert schema["$id"].endswith(name), f"{name}: $id no coincide con el nombre de archivo"


def test_valid_examples_pass():
    n = 0
    for path, schema_file, doc in _examples("valid"):
        errors = sorted(_validator(schema_file).iter_errors(doc), key=lambda e: list(e.path))
        assert not errors, f"{path.name} debería ser válido: " + "; ".join(
            f"{'/'.join(map(str, e.path))}: {e.message}" for e in errors[:3])
        n += 1
    assert n >= 10


def test_invalid_examples_fail():
    n = 0
    for path, schema_file, doc in _examples("invalid"):
        assert not _validator(schema_file).is_valid(doc), f"{path.name} debería ser rechazado por {schema_file}"
        n += 1
    assert n >= 20


def test_each_source_and_kind_has_a_valid_example():
    seen = set()
    for path, schema_file, doc in _examples("valid"):
        if schema_file == SCHEMA_BY_PREFIX["envelope"]:
            seen.add((doc["source"], doc["payload"]["kind"]))
    required = {("reddit", "post"), ("reddit", "comment"), ("facebook", "post"),
                ("facebook", "comment"), ("instagram", "comment")}
    assert required <= seen, f"faltan ejemplos para: {required - seen}"
    origins = {doc["origin"] for _, sf, doc in _examples("valid") if sf == SCHEMA_BY_PREFIX["envelope"]}
    assert origins == {"stream", "historical"}


def test_event_id_follows_uuid5_formula():
    """§3.6: event_id = uuid5(NAMESPACE_URL, '{origin}|{source}|{seed}|{seq}')."""
    for path, schema_file, doc in _examples("valid"):
        if schema_file == SCHEMA_BY_PREFIX["pubsub"]:
            continue
        expected = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc['origin']}|{doc['source']}|{SIM_SEED}|{doc['seq']}"))
        assert doc["event_id"] == expected, f"{path.name}: event_id no sigue §3.6"


def test_enriched_examples_are_envelope_minus_identity_plus_nlp():
    """El enriquecido conserva los campos del envelope salvo author.user_id/handle."""
    by_name = {p.name: d for p, _, d in _examples("valid")}
    checked = 0
    for name, enriched in by_name.items():
        if not name.startswith("enriched__"):
            continue
        env = by_name["envelope__" + name.split("__", 1)[1]]
        for key in env:
            if key == "author":
                assert enriched["author"]["followers"] == env["author"]["followers"]
            else:
                assert enriched[key] == env[key], f"{name}: '{key}' difiere del envelope"
        assert "handle" not in enriched["author"] and "user_id" not in enriched["author"]
        checked += 1
    assert checked == 4


def test_pubsub_attributes_match_envelope():
    by_name = {p.name: d for p, _, d in _examples("valid")}
    for name, attrs in by_name.items():
        if name.startswith("pubsub__"):
            env = by_name["envelope__" + name.split("__", 1)[1]]
            assert attrs == {k: env[k] for k in ("event_id", "source", "origin", "schema_version")}


def test_engagement_normalization_matches_section_5_2():
    for path, schema_file, doc in _examples("valid"):
        if schema_file != SCHEMA_BY_PREFIX["enriched"]:
            continue
        p = doc["payload"]
        if doc["source"] == "reddit":
            assert doc["likes"] == p["score"] and doc["shares"] is None and doc["comments"] == p["num_comments"]
        elif doc["source"] == "facebook":
            assert doc["likes"] == p["reactions_total"] and doc["shares"] == p["shares_count"] \
                and doc["comments"] == p["comments_count"]
        else:
            assert doc["likes"] == p["like_count"] and doc["shares"] is None and doc["comments"] == p["comments_count"]


# --------------------------------------------------------------------------- DDL
def test_bigquery_ddl_defines_the_four_tables():
    tables = {}
    for f in ("posts_enriched", "trend_windows", "dlq_events", "dq_results"):
        tables.update(_parse_ddl(ROOT / "bigquery" / f"{f}.sql"))
    assert set(tables) == {"posts_enriched", "trend_windows", "dlq_events", "dq_results"}
    pe = [c for c, _ in tables["posts_enriched"]["columns"]]
    assert len(pe) == len(set(pe))
    assert "handle" not in pe and "user_id" not in pe, "posts_enriched no puede tener identidad en claro"
    assert [c for c, _ in tables["trend_windows"]["columns"]] == [
        "window_start", "window_end", "brand", "source", "n_posts", "n_negative", "n_positive",
        "avg_sentiment", "emotion_counts", "zscore_volume", "zscore_sentiment", "is_alert"]
    assert [c for c, _ in tables["dlq_events"]["columns"]] == [
        "received_at", "source", "error_type", "error_detail", "raw_message"]


def test_posts_raw_matches_bigquery_posts_enriched_column_by_column():
    bq = _parse_ddl(ROOT / "bigquery" / "posts_enriched.sql")["posts_enriched"]["columns"]
    raw = _parse_ddl(ROOT / "athena" / "posts_raw.sql")["posts_raw"]["columns"]
    bq_as_athena = [(n, _BQ_TO_ATHENA[t]) for n, t in bq]
    assert raw == bq_as_athena, "posts_raw (Parquet) debe coincidir con posts_enriched de BigQuery"


def test_trusted_posts_is_raw_with_text_clean_and_source_as_partition():
    raw = _parse_ddl(ROOT / "athena" / "posts_raw.sql")["posts_raw"]["columns"]
    posts = _parse_ddl(ROOT / "athena" / "posts.sql")["posts"]
    expected = [(n, t) for n, t in raw if n != "source"]
    i = [n for n, _ in expected].index("text") + 1
    expected.insert(i, ("text_clean", "string"))
    assert posts["columns"] == expected
    assert [n for n, _ in posts["partitions"]] == ["source", "dt"]


def test_partition_projection_covers_every_partition_key():
    for f, table in (("posts_raw", "posts_raw"), ("posts", "posts")):
        t = _parse_ddl(ROOT / "athena" / f"{f}.sql")[table]
        props = t["props"]
        assert props["projection.enabled"] == "true"
        keys = [n for n, _ in t["partitions"]]
        template = props["storage.location.template"]
        for k in keys:
            assert f"projection.{k}.type" in props, f"{table}: falta projection.{k}.type"
            assert "${" + k + "}" in template, f"{table}: la plantilla no usa ${{{k}}}"
        data_cols = {n for n, _ in t["columns"]}
        assert not (set(keys) & data_cols), f"{table}: clave de partición repetida como columna de datos"


def test_raw_partition_values_match_the_envelope_enums():
    props = _parse_ddl(ROOT / "athena" / "posts_raw.sql")["posts_raw"]["props"]
    env = _schemas()["envelope.v1.schema.json"]["properties"]
    assert props["projection.part_origin.values"].split(",") == env["origin"]["enum"]
    assert props["projection.part_source.values"].split(",") == env["source"]["enum"]


def test_curated_tables_and_foreign_keys():
    t = _parse_ddl(ROOT / "athena" / "curated.sql")
    assert set(t) == {"fact_posts", "dim_date", "dim_source", "dim_brand", "dim_user",
                      "agg_emotion_by_brand_hour", "agg_trend_daily", "edges_user_brand",
                      "edges_hashtag_cooccurrence"}
    fact = {n for n, _ in t["fact_posts"]["columns"]}
    for fk, (dim, key) in {"date_key": ("dim_date", "date_key"), "source": ("dim_source", "source"),
                           "brand_primary": ("dim_brand", "brand"), "user_hash": ("dim_user", "user_hash")}.items():
        assert fk in fact and key in {n for n, _ in t[dim]["columns"]}, f"FK {fk} -> {dim}.{key} rota"
    for name, tbl in t.items():
        cols = {n for n, _ in tbl["columns"]}
        assert not (cols & {"handle", "user_id"}), f"{name}: identidad en claro en Curated"


def test_ddl_placeholders_are_only_deployment_ones():
    allowed = {"GCP_PROJECT", "lake_bucket"}
    for p in list((ROOT / "bigquery").glob("*.sql")) + list((ROOT / "athena").glob("*.sql")):
        found = set(re.findall(r"\{\{(\w+)\}\}", p.read_text(encoding="utf-8")))
        assert found <= allowed, f"{p.name}: placeholder desconocido {found - allowed}"
    for p in (ROOT / "bigquery").glob("*.sql"):
        assert "${" not in p.read_text(encoding="utf-8"), f"{p.name}: usa ${{...}} en vez de {{{{...}}}}"


def test_bigquery_columns_are_derivable_from_enriched_envelope():
    """Cada columna de posts_enriched sale del envelope enriquecido (o de un renombrado documentado)."""
    enriched_keys = set(_schemas()["envelope.enriched.v1.schema.json"]["properties"])
    derived = {"user_hash", "followers", "label_sentiment", "brand_primary"}  # author.* / sim.* / brands[0]
    bq = {n for n, _ in _parse_ddl(ROOT / "bigquery" / "posts_enriched.sql")["posts_enriched"]["columns"]}
    missing = bq - derived - enriched_keys
    assert not missing, f"columnas sin origen en el envelope enriquecido: {missing}"


# --------------------------------------------------------------------------- runner sin pytest
if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}\n      {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} pruebas OK")
    sys.exit(1 if failed else 0)
