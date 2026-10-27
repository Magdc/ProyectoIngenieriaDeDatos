"""Tablas de Glue del lake desde ``contracts/athena/*.sql`` (Lote 4.1, ARCHITECTURE.md §3.19.1).

Las DDL de ``contracts/athena/`` son la copia ejecutable de §5.7 (§5.13). Terraform no puede leer
SQL, así que este módulo las traduce a ``infra/aws/modules/glue_catalog/tables.json``, que el
módulo ``glue_catalog`` lee con ``jsondecode(file(...))``. Una prueba exige que el JSON
versionado sea exactamente el que sale de las DDL: si cambia un lado sin el otro, falla.

    python -m lakehouse.catalog_spec --check     # desde project/src: 0 si tables.json y docs/CATALOG.md están al día
    python -m lakehouse.catalog_spec --write     # los regenera
    python -m lakehouse.catalog_spec --print     # resumen de las tablas

API (otros lotes pueden usarla, p. ej. el 4.2 para el orden de columnas de ``ta_trusted.posts``):
``load_contract_tables()``, ``load_tables_json()``, ``table(name)``, ``check()``,
``resolve(spec, bucket)`` y ``glue_table_problems(spec, glue_table, bucket)``.

Lote 5.3 (catálogo, HU-034; ARCHITECTURE.md §3.25.1): las descripciones de tabla y los comentarios de columna (también
los de las claves de partición) salen de las DDL; cada comentario empieza por su clase de privacidad
(``[anonimizado]``, ``[cuasi_identificador]``, ``[texto_libre]``, ``[sin_dato_personal]``). ``catalog_problems()`` exige
que todo esté descrito, ``glue_description_problems()`` compara las descripciones con Glue y ``render_dictionary()``
genera el diccionario de datos ``docs/CATALOG.md`` (``check_dictionary()``).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

PROJECT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = PROJECT / "contracts" / "athena"
CONTRACT_FILES = ("posts_raw.sql", "posts.sql", "curated.sql")
TABLES_JSON = PROJECT / "infra" / "aws" / "modules" / "glue_catalog" / "tables.json"
DICTIONARY_MD = PROJECT.parent / "docs" / "CATALOG.md"
PLACEHOLDER = "{{lake_bucket}}"

# Catálogo (Lote 5.3): clases de privacidad de una columna, en el orden en que se explican (§3.25.1).
PRIVACY_CLASSES = ("anonimizado", "cuasi_identificador", "texto_libre", "sin_dato_personal")
ANONYMIZED = "anonimizado"
#: Las únicas columnas que identifican a una persona: el seudónimo HMAC del autor (§3.14, ADR-06).
ANONYMIZED_COLUMNS = ("user_hash",)
IDENTITY_COLUMNS = ("user_id", "handle")
# Límites de Glue: Comment de columna ≤ 255 y Description de tabla ≤ 2048 (el módulo de Terraform recorta;
# la prueba exige que no haga falta).
COMMENT_MAX = 255
DESCRIPTION_MAX = 2048
DESCRIPTION_MIN = 40
_TAG = re.compile(r"^\[([a-z_]+)\]\s*(.*)$", re.S)

# Lo que Athena escribe al crear una tabla STORED AS PARQUET (Glue lo guarda así).
PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
# Parámetros que agrega Terraform (módulo glue_catalog) además de los TBLPROPERTIES.
EXTRA_PARAMETERS = {"EXTERNAL": "TRUE", "classification": "parquet"}

_CREATE = re.compile(r"^\s*CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z0-9_]+)\.([a-z0-9_]+)\s*\(\s*$", re.I)
_COLUMN = re.compile(r"^\s*([a-z0-9_]+)\s+([a-z0-9_<>,\s]+?)\s*,?\s*$", re.I)
_PROP = re.compile(r"'([^']*)'\s*=\s*'([^']*)'")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str = ""

    def to_json(self) -> dict[str, str]:
        return {"name": self.name, "type": self.type, "comment": self.comment}

    @property
    def privacy(self) -> str:
        """Clase de privacidad (la etiqueta ``[…]`` del comentario) o ``""`` si no tiene."""
        m = _TAG.match(self.comment)
        return m.group(1) if m else ""

    @property
    def description(self) -> str:
        """El comentario sin la etiqueta de privacidad."""
        m = _TAG.match(self.comment)
        return m.group(2).strip() if m else self.comment

    @property
    def anonymized(self) -> bool:
        return self.privacy == ANONYMIZED


@dataclass(frozen=True)
class TableSpec:
    database: str
    name: str
    source: str
    description: str
    location: str
    columns: tuple[Column, ...]
    partition_keys: tuple[Column, ...] = ()
    parameters: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.name}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def partition_names(self) -> list[str]:
        return [c.name for c in self.partition_keys]

    @property
    def projected(self) -> bool:
        return self.parameters.get("projection.enabled") == "true"

    def to_json(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "name": self.name,
            "source": self.source,
            "description": self.description,
            "location": self.location,
            "columns": [c.to_json() for c in self.columns],
            "partition_keys": [c.to_json() for c in self.partition_keys],
            "parameters": dict(self.parameters),
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "TableSpec":
        cols = tuple(Column(c["name"], c["type"], c.get("comment", "")) for c in data["columns"])
        parts = tuple(Column(c["name"], c["type"], c.get("comment", "")) for c in data.get("partition_keys", []))
        return cls(data["database"], data["name"], data.get("source", ""), data.get("description", ""),
                   data["location"], cols, parts, dict(data.get("parameters", {})))


class DdlError(ValueError):
    """Una DDL de contracts/athena/ no tiene la forma esperada."""


def _split_comment(line: str) -> tuple[str, str]:
    code, _, comment = line.partition("--")
    return code.rstrip(), comment.strip()


def _norm_type(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()


def _partition_comments(pairs: list[tuple[str, str]]) -> dict[str, str]:
    """Comentario de cada clave de ``PARTITIONED BY (…)``: el de su línea (una clave por línea; en la forma de una
    sola línea, solo si hay una única clave)."""
    notes: dict[str, str] = {}
    inside = False
    for code, comment in pairs:
        if not inside:
            m = re.search(r"PARTITIONED\s+BY\s*\((.*)$", code, re.I)
            if not m:
                continue
            inline = m.group(1)
            if ")" in inline:
                items = [x for x in inline.split(")", 1)[0].split(",") if x.strip()]
                cm = _COLUMN.match(items[0]) if len(items) == 1 else None
                if cm and comment:
                    notes[cm.group(1).lower()] = comment
                return notes
            inside = True
            code = inline
        if code.strip().startswith(")"):
            return notes
        cm = _COLUMN.match(code) if code.strip() else None
        if cm and comment:
            notes[cm.group(1).lower()] = comment
    return notes


def parse_ddl(text: str, source: str = "") -> list[TableSpec]:
    """Tablas de un archivo de DDL de Athena (``CREATE EXTERNAL TABLE … STORED AS PARQUET``)."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    tables: list[TableSpec] = []
    i = 0
    while i < len(lines):
        m = _CREATE.match(lines[i])
        if not m:
            i += 1
            continue
        database, name = m.group(1).lower(), m.group(2).lower()
        # Descripción: las líneas de comentario seguidas justo encima del CREATE.
        desc: list[str] = []
        j = i - 1
        while j >= 0 and lines[j].strip().startswith("--"):
            desc.insert(0, lines[j].strip()[2:].strip())
            j -= 1
        i += 1
        columns: list[Column] = []
        while i < len(lines):
            code, comment = _split_comment(lines[i])
            if code.strip().startswith(")"):
                break
            if code.strip():
                cm = _COLUMN.match(code)
                if not cm:
                    raise DdlError(f"{source}: columna ilegible en {database}.{name}: {lines[i]!r}")
                columns.append(Column(cm.group(1).lower(), _norm_type(cm.group(2)), comment))
            i += 1
        else:
            raise DdlError(f"{source}: {database}.{name} sin cierre de columnas")
        # Resto del enunciado hasta el ';' (sin comentarios para el cuerpo; los de las particiones aparte).
        rest: list[str] = []
        first_code, first_comment = _split_comment(lines[i])
        rest.append(first_code.strip()[1:])
        pairs = [(rest[0], first_comment)]
        while ";" not in rest[-1] and i + 1 < len(lines):
            i += 1
            code, comment = _split_comment(lines[i])
            rest.append(code)
            pairs.append((code, comment))
        body = " ".join(rest)
        body = body.split(";", 1)[0]
        notes = _partition_comments(pairs)
        parts: list[Column] = []
        pm = re.search(r"PARTITIONED\s+BY\s*\((.*?)\)", body, re.I | re.S)
        if pm:
            for item in pm.group(1).split(","):
                item = item.strip()
                if not item:
                    continue
                cm = _COLUMN.match(item)
                if not cm:
                    raise DdlError(f"{source}: partición ilegible en {database}.{name}: {item!r}")
                key = cm.group(1).lower()
                parts.append(Column(key, _norm_type(cm.group(2)), notes.get(key, "")))
        if not re.search(r"STORED\s+AS\s+PARQUET", body, re.I):
            raise DdlError(f"{source}: {database}.{name} no es STORED AS PARQUET")
        lm = re.search(r"LOCATION\s+'([^']+)'", body, re.I)
        if not lm:
            raise DdlError(f"{source}: {database}.{name} sin LOCATION")
        props: dict[str, str] = {}
        tm = re.search(r"TBLPROPERTIES\s*\((.*)\)\s*$", body.strip(), re.I | re.S)
        if tm:
            props = {k: v for k, v in _PROP.findall(tm.group(1))}
        tables.append(TableSpec(database, name, source, " ".join(d for d in desc if d), lm.group(1),
                                tuple(columns), tuple(parts), props))
        i += 1
    return tables


def load_contract_tables(contracts_dir: Path = CONTRACTS_DIR, files: Iterable[str] = CONTRACT_FILES) -> list[TableSpec]:
    tables: list[TableSpec] = []
    for name in files:
        path = Path(contracts_dir) / name
        tables.extend(parse_ddl(path.read_text(encoding="utf-8"), f"contracts/athena/{name}"))
    names = [t.full_name for t in tables]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise DdlError(f"tablas repetidas en contracts/athena/: {dup}")
    return tables


def to_document(tables: Iterable[TableSpec]) -> dict[str, Any]:
    return {
        "_generated_from": "project/contracts/athena/*.sql · python -m lakehouse.catalog_spec --write (ARCHITECTURE.md §3.19.1)",
        "tables": [t.to_json() for t in tables],
    }


def render(tables: Iterable[TableSpec]) -> str:
    return json.dumps(to_document(tables), ensure_ascii=False, indent=2) + "\n"


def load_tables_json(path: Path = TABLES_JSON) -> list[TableSpec]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [TableSpec.from_json(t) for t in data["tables"]]


def table(full_name: str, tables: Iterable[TableSpec] | None = None) -> TableSpec:
    """Una tabla por nombre (``ta_trusted.posts``); por defecto, de las DDL de ``contracts/``."""
    for t in tables if tables is not None else load_contract_tables():
        if t.full_name == full_name:
            return t
    raise KeyError(full_name)


def check(contracts_dir: Path = CONTRACTS_DIR, tables_json: Path = TABLES_JSON) -> list[str]:
    """Diferencias entre ``tables.json`` y lo que sale de las DDL (vacío = al día)."""
    try:
        expected = render(load_contract_tables(contracts_dir))
    except (OSError, DdlError) as exc:
        return [f"no se pudieron leer las DDL: {exc}"]
    try:
        current = Path(tables_json).read_text(encoding="utf-8").replace("\r\n", "\n")
    except OSError:
        return [f"no existe {tables_json}"]
    if current == expected:
        return []
    try:
        cur = {t.full_name: t for t in load_tables_json(tables_json)}
    except (ValueError, KeyError) as exc:
        return [f"tables.json ilegible: {exc}"]
    exp = {t.full_name: t for t in load_contract_tables(contracts_dir)}
    out = [f"falta {n}" for n in sorted(set(exp) - set(cur))]
    out += [f"sobra {n}" for n in sorted(set(cur) - set(exp))]
    out += [f"{n} distinta" for n in sorted(set(exp) & set(cur)) if exp[n] != cur[n]]
    return out or ["tables.json distinto (formato u orden): regenérelo con --write"]


def resolve(spec: TableSpec, bucket: str) -> TableSpec:
    """La tabla con ``{{lake_bucket}}`` sustituido (lo que Terraform crea en Glue)."""
    params = {k: v.replace(PLACEHOLDER, bucket) for k, v in spec.parameters.items()}
    return TableSpec(spec.database, spec.name, spec.source, spec.description.replace(PLACEHOLDER, bucket),
                     spec.location.replace(PLACEHOLDER, bucket), spec.columns, spec.partition_keys, params)


def glue_table_problems(spec: TableSpec, glue_table: dict[str, Any], bucket: str) -> list[str]:
    """Compara una tabla de ``aws glue get-table(s)`` con su DDL de ``contracts/`` (vacío = igual)."""
    want = resolve(spec, bucket)
    sd = glue_table.get("StorageDescriptor") or {}
    problems: list[str] = []
    got_cols = [(c.get("Name"), _norm_type(c.get("Type", ""))) for c in sd.get("Columns") or []]
    exp_cols = [(c.name, c.type) for c in want.columns]
    if got_cols != exp_cols:
        problems.append(f"columnas {got_cols} ≠ {exp_cols}")
    got_parts = [(c.get("Name"), _norm_type(c.get("Type", ""))) for c in glue_table.get("PartitionKeys") or []]
    exp_parts = [(c.name, c.type) for c in want.partition_keys]
    if got_parts != exp_parts:
        problems.append(f"particiones {got_parts} ≠ {exp_parts}")
    if sd.get("Location") != want.location:
        problems.append(f"location {sd.get('Location')} ≠ {want.location}")
    if glue_table.get("TableType") != "EXTERNAL_TABLE":
        problems.append(f"TableType {glue_table.get('TableType')}")
    if sd.get("InputFormat") != PARQUET_INPUT or (sd.get("SerdeInfo") or {}).get("SerializationLibrary") != PARQUET_SERDE:
        problems.append("no es Parquet (InputFormat/SerDe)")
    params = glue_table.get("Parameters") or {}
    for key, value in want.parameters.items():
        if params.get(key) != value:
            problems.append(f"parámetro {key} = {params.get(key)!r} (esperado {value!r})")
    extra = sorted(k for k in params if k.startswith("projection.") and k not in want.parameters)
    if extra:
        problems.append(f"parámetros de proyección de más: {extra}")
    return problems


def projection_problems(spec: TableSpec) -> list[str]:
    """Cada clave de partición de una tabla proyectada tiene su ``projection.<clave>.type``."""
    if not spec.partition_keys:
        return []
    if not spec.projected:
        return [f"{spec.full_name}: particionada sin partition projection"]
    out = [f"{spec.full_name}: falta projection.{k}.type" for k in spec.partition_names
           if f"projection.{k}.type" not in spec.parameters]
    template = spec.parameters.get("storage.location.template", "")
    out += [f"{spec.full_name}: storage.location.template sin ${{{k}}}" for k in spec.partition_names
            if "${" + k + "}" not in template]
    if template and not template.startswith(spec.location):
        out.append(f"{spec.full_name}: storage.location.template fuera de LOCATION")
    return out


# --- Catálogo (Lote 5.3, HU-034) ---------------------------------------------------------------


def all_columns(spec: TableSpec) -> tuple[Column, ...]:
    """Columnas de datos y claves de partición, en el orden de Glue."""
    return spec.columns + spec.partition_keys


def catalog_problems(tables: Iterable[TableSpec]) -> list[str]:
    """Reglas del catálogo de HU-034 (vacío = cumple): cada tabla con descripción y cada columna (también las claves
    de partición) con clase de privacidad válida y descripción, dentro de los límites de Glue; ``[anonimizado]``
    exactamente en las columnas ``user_hash`` y ninguna columna de identidad en claro."""
    out: list[str] = []
    for t in tables:
        desc = t.description
        if len(desc) < DESCRIPTION_MIN:
            out.append(f"{t.full_name}: descripción de tabla vacía o corta ({len(desc)} < {DESCRIPTION_MIN} caracteres)")
        if len(desc) > DESCRIPTION_MAX:
            out.append(f"{t.full_name}: descripción de {len(desc)} caracteres (Glue admite {DESCRIPTION_MAX})")
        if PLACEHOLDER in desc:
            out.append(f"{t.full_name}: la descripción menciona {PLACEHOLDER}")
        for c in all_columns(t):
            where = f"{t.full_name}.{c.name}"
            if c.name in IDENTITY_COLUMNS:
                out.append(f"{where}: identidad en claro")
            if not c.comment:
                out.append(f"{where}: sin comentario")
                continue
            if c.privacy not in PRIVACY_CLASSES:
                out.append(f"{where}: sin clase de privacidad válida al inicio ({'[' + c.privacy + ']' if c.privacy else 'ninguna'}; "
                           f"use una de {', '.join('[' + p + ']' for p in PRIVACY_CLASSES)})")
            if len(c.description) < 3:
                out.append(f"{where}: comentario sin descripción")
            if len(c.comment) > COMMENT_MAX:
                out.append(f"{where}: comentario de {len(c.comment)} caracteres (Glue admite {COMMENT_MAX})")
            if c.anonymized != (c.name in ANONYMIZED_COLUMNS):
                out.append(f"{where}: [{c.privacy}] — [{ANONYMIZED}] va exactamente en {', '.join(ANONYMIZED_COLUMNS)}")
    return out


def glue_description_problems(spec: TableSpec, glue_table: dict[str, Any], bucket: str) -> list[str]:
    """Descripción de la tabla y comentario de cada columna y clave de partición en Glue frente a la DDL (vacío =
    igual). Aplica los mismos recortes que el módulo de Terraform (2048 y 255 caracteres)."""
    want = resolve(spec, bucket)
    problems: list[str] = []
    if (glue_table.get("Description") or "") != want.description[:DESCRIPTION_MAX]:
        problems.append(f"{spec.full_name}: descripción distinta de la DDL")
    sd = glue_table.get("StorageDescriptor") or {}
    got = {c.get("Name"): c.get("Comment") or "" for c in (sd.get("Columns") or []) + (glue_table.get("PartitionKeys") or [])}
    for c in all_columns(want):
        if got.get(c.name, "") != c.comment[:COMMENT_MAX]:
            problems.append(f"{spec.full_name}.{c.name}: comentario distinto de la DDL")
    return problems


def privacy_summary(tables: Iterable[TableSpec]) -> dict[str, Any]:
    """Conteos del catálogo: tablas, columnas (con particiones) por clase y las anonimizadas (``base.tabla.columna``)."""
    tables = list(tables)
    counts = {p: 0 for p in PRIVACY_CLASSES}
    anonymized: list[str] = []
    for t in tables:
        for c in all_columns(t):
            if c.privacy in counts:
                counts[c.privacy] += 1
            if c.anonymized:
                anonymized.append(f"{t.full_name}.{c.name}")
    return {"tables": len(tables), "columns": sum(len(all_columns(t)) for t in tables), "by_class": counts,
            "anonymized": anonymized}


def _md(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_dictionary(tables: Iterable[TableSpec]) -> str:
    """El diccionario de datos ``docs/CATALOG.md`` (lo mismo que muestra Glue), generado desde las DDL."""
    tables = list(tables)
    s = privacy_summary(tables)
    out = [
        "# TrendAnalyzer · Catálogo de datos (diccionario)",
        "",
        "> **Generado** desde `project/contracts/athena/*.sql` con `cd project/src && python -m lakehouse.catalog_spec --write`; "
        "no se edita a mano (una prueba exige que coincida con las DDL). Es lo mismo que el Glue Data Catalog muestra en "
        "`ta_raw`, `ta_trusted` y `ta_curated` (descripción de cada tabla y `Comment` de cada columna). Reglas, linaje y "
        "gobernanza: `ARCHITECTURE.md` §3.25, `LINEAGE.md` y `GOVERNANCE.md`. Datos sintéticos.",
        "",
        f"**{s['tables']} tablas y {s['columns']} columnas** (con las claves de partición). Clases de privacidad: "
        + ", ".join(f"`{p}` {n}" for p, n in s["by_class"].items()) + ".",
        "",
        "| Clase | Qué significa | ¿Dato anonimizado? |",
        "|---|---|---|",
        "| `anonimizado` | El identificador del autor, reemplazado por un seudónimo HMAC-SHA256 (`user_hash`): la única columna que "
        "identifica a una persona; sin la llave `ta-hmac-key` no se revierte | **Sí** |",
        "| `cuasi_identificador` | No identifica por sí sola, pero cruzada con otras fuentes ayudaría a reidentificar al autor "
        "(seguidores, actividad, ids y enlaces del post en `payload`) | No |",
        "| `texto_libre` | Texto escrito por el autor: en datos reales podría traer nombres, menciones o datos sensibles | No |",
        "| `sin_dato_personal` | Describe el comentario, la marca, el tiempo, la red o el modelo, no a la persona | No |",
        "",
        "| Tabla | Columnas | Particiones | Anonimizadas | Ubicación |",
        "|---|---|---|---|---|",
    ]
    for t in tables:
        anon = [c.name for c in all_columns(t) if c.anonymized]
        out.append(f"| [`{t.full_name}`](#{t.full_name.replace('.', '')}) | {len(t.columns)} | "
                   f"{', '.join(f'`{p}`' for p in t.partition_names) or '—'} | {', '.join(f'`{a}`' for a in anon) or '—'} | "
                   f"`{t.location.replace(PLACEHOLDER, '{lake}')}` |")
    for t in tables:
        out += ["", f"## {t.full_name}", "", _md(t.description), "",
                f"Ubicación `{t.location.replace(PLACEHOLDER, '{lake}')}` · Parquet + Snappy · "
                + (f"particiones {', '.join(f'`{p}`' for p in t.partition_names)} (partition projection)"
                   if t.partition_keys else "sin particiones") + f" · DDL `contracts/athena/{t.source.rsplit('/', 1)[-1]}`",
                "", "| # | Columna | Tipo | Anonimizado | Clase | Descripción |", "|---|---|---|---|---|---|"]
        for n, c in enumerate(all_columns(t), 1):
            kind = " (partición)" if c in t.partition_keys else ""
            out.append(f"| {n} | `{c.name}`{kind} | `{c.type}` | {'**sí**' if c.anonymized else 'no'} | `{c.privacy or '—'}` | "
                       f"{_md(c.description)} |")
    return "\n".join(out) + "\n"


def check_dictionary(contracts_dir: Path = CONTRACTS_DIR, path: Path = DICTIONARY_MD) -> list[str]:
    """Diferencias entre ``docs/CATALOG.md`` y lo que sale de las DDL (vacío = al día)."""
    try:
        expected = render_dictionary(load_contract_tables(contracts_dir))
    except (OSError, DdlError) as exc:
        return [f"no se pudieron leer las DDL: {exc}"]
    try:
        current = Path(path).read_text(encoding="utf-8").replace("\r\n", "\n")
    except OSError:
        return [f"no existe {path}"]
    return [] if current == expected else [f"{path.name} distinto de las DDL: regenérelo con --write"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.catalog_spec", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="0 si tables.json y docs/CATALOG.md coinciden con las DDL")
    group.add_argument("--write", action="store_true", help="regenera tables.json y docs/CATALOG.md desde las DDL")
    group.add_argument("--print", action="store_true", help="resumen de las tablas")
    parser.add_argument("--contracts-dir", type=Path, default=CONTRACTS_DIR)
    parser.add_argument("--tables-json", type=Path, default=TABLES_JSON)
    parser.add_argument("--dictionary", type=Path, default=DICTIONARY_MD, help="diccionario de datos (Markdown)")
    args = parser.parse_args(argv)
    if args.write:
        tables = load_contract_tables(args.contracts_dir)
        for path, text in ((args.tables_json, render(tables)), (args.dictionary, render_dictionary(tables))):
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
        print(f"{len(tables)} tablas → {args.tables_json} y {args.dictionary}")
        problems = catalog_problems(tables)
        if problems:
            print("catálogo incompleto (HU-034):\n" + "\n".join(problems))
        return 0
    if args.check:
        problems = check(args.contracts_dir, args.tables_json) + check_dictionary(args.contracts_dir, args.dictionary)
        try:
            problems += catalog_problems(load_contract_tables(args.contracts_dir))
        except (OSError, DdlError):
            pass  # ya lo informa check()
        print("tables.json y diccionario al día; catálogo completo" if not problems else "\n".join(problems))
        return 0 if not problems else 1
    for t in load_contract_tables(args.contracts_dir):
        parts = f" · particiones {', '.join(t.partition_names)}" if t.partition_keys else ""
        print(f"{t.full_name}: {len(t.columns)} columnas{parts} · {t.location}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
