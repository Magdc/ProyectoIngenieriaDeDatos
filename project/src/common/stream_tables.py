"""Columnas de las tablas de ``ta_stream`` que crea el Lote 2.4 (§5.5).

``trend_windows``, ``dlq_events`` y ``dq_results``. Es la misma idea que
``common.enriched_row.POSTS_ENRICHED_COLUMNS`` (Lote 2.2): el código es la
fuente de los schemas JSON de Terraform
(``infra/gcp/modules/bigquery_stream_tables/schemas/``) y una prueba exige que
coincidan. Otra prueba los compara con las DDL de ``contracts/bigquery/``.

Para regenerar los JSON después de cambiar una columna (primero en §5.5)::

    python -m common.stream_tables --write infra/gcp/modules/bigquery_stream_tables/schemas

Sin Beam ni dependencias externas.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, NamedTuple


class Column(NamedTuple):
    """Una columna de BigQuery: nombre, tipo (nombre canónico de la API), modo y descripción."""

    name: str
    type: str
    mode: str
    description: str


# Tipos con el nombre que devuelve la API de BigQuery (INTEGER, FLOAT, BOOLEAN),
# para que Terraform no muestre diferencias perpetuas con INT64, FLOAT64 o BOOL.
TREND_WINDOWS_COLUMNS: tuple[Column, ...] = (
    Column("window_start", "TIMESTAMP", "NULLABLE",
           "Inicio de la ventana deslizante, en tiempo del evento (event_time, UTC)."),
    Column("window_end", "TIMESTAMP", "NULLABLE",
           "Fin (exclusivo) de la ventana: window_start + 5 min."),
    Column("brand", "STRING", "NULLABLE",
           "Clave canónica de la marca (brand_primary del post)."),
    Column("source", "STRING", "NULLABLE",
           "Red social simulada: reddit, facebook o instagram."),
    Column("n_posts", "INTEGER", "NULLABLE",
           "Posts de la marca y la fuente con event_time dentro de la ventana."),
    Column("n_negative", "INTEGER", "NULLABLE",
           "Posts con sentiment_label = negative."),
    Column("n_positive", "INTEGER", "NULLABLE",
           "Posts con sentiment_label = positive."),
    Column("avg_sentiment", "FLOAT", "NULLABLE",
           "Media con signo en [-1, 1]: positivo +score, negativo -score, neutro 0. "
           "Nulo si ningún post de la ventana tiene sentimiento."),
    Column("emotion_counts", "JSON", "NULLABLE",
           "Conteo por emoción (las 7 clases de §3.3, siempre presentes)."),
    Column("zscore_volume", "FLOAT", "NULLABLE",
           "z-score de n_posts frente a las ventanas anteriores (baseline móvil). "
           "Nulo con menos de 2 ventanas de baseline."),
    Column("zscore_sentiment", "FLOAT", "NULLABLE",
           "z-score de avg_sentiment frente al baseline móvil. Nulo sin baseline suficiente."),
    Column("is_alert", "BOOLEAN", "NULLABLE",
           "Cambio brusco de volumen o sentimiento. Siempre false hasta tener 60 ventanas de baseline."),
)

DLQ_EVENTS_COLUMNS: tuple[Column, ...] = (
    Column("received_at", "TIMESTAMP", "NULLABLE",
           "Cuándo Dataflow rechazó el mensaje (atributo dlq_received_at, UTC)."),
    Column("source", "STRING", "NULLABLE",
           "Fuente de la suscripción donde llegó el mensaje (atributo dlq_source)."),
    Column("error_type", "STRING", "NULLABLE",
           "encoding, json, schema, attributes o routing (atributo dlq_error_type)."),
    Column("error_detail", "STRING", "NULLABLE",
           "Primer error de validación (atributo dlq_error_detail, máximo 1.024 bytes)."),
    Column("raw_message", "STRING", "NULLABLE",
           "Cuerpo del mensaje tal como llegó; los bytes que no son UTF-8 van como \\xNN."),
)

DQ_RESULTS_COLUMNS: tuple[Column, ...] = (
    Column("run_id", "STRING", "REQUIRED", "Identifica la corrida de verificación."),
    Column("run_ts", "TIMESTAMP", "REQUIRED", "Instante de la corrida (partición)."),
    Column("layer", "STRING", "REQUIRED", "Capa verificada: stream, raw, trusted o curated."),
    Column("table_name", "STRING", "REQUIRED", "Tabla verificada."),
    Column("suite", "STRING", "NULLABLE", "Suite de Great Expectations, test de dbt o verificador."),
    Column("check_name", "STRING", "REQUIRED", "Nombre de la verificación."),
    Column("kpi", "STRING", "NULLABLE",
           "completitud, unicidad, duplicados, anonimizacion, latencia, precision_nlp u otro (§5.10)."),
    Column("observed_value", "FLOAT", "NULLABLE", "Valor medido."),
    Column("threshold", "FLOAT", "NULLABLE", "Umbral del KPI."),
    Column("passed", "BOOLEAN", "REQUIRED", "Si la verificación pasó."),
    Column("details", "JSON", "NULLABLE", "Detalle libre de la verificación."),
)

TABLES: dict[str, tuple[Column, ...]] = {
    "trend_windows": TREND_WINDOWS_COLUMNS,
    "dlq_events": DLQ_EVENTS_COLUMNS,
    "dq_results": DQ_RESULTS_COLUMNS,
}

# Valores admitidos (§5.5 y §5.10). La DDL no los impone; los usan el código y los verificadores.
DQ_LAYERS = ("stream", "raw", "trusted", "curated")
DQ_KPIS = ("completitud", "unicidad", "duplicados", "anonimizacion", "latencia", "precision_nlp", "otro")
DLQ_ERROR_TYPES = ("encoding", "json", "schema", "attributes", "routing")

# Alias de tipos: BigQuery acepta los dos nombres y la DDL usa los de GoogleSQL.
TYPE_ALIASES = {
    "INT64": "INTEGER",
    "INTEGER": "INTEGER",
    "FLOAT64": "FLOAT",
    "FLOAT": "FLOAT",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "STRING": "STRING",
    "TIMESTAMP": "TIMESTAMP",
    "JSON": "JSON",
    "DATE": "DATE",
    "NUMERIC": "NUMERIC",
}


def canonical_type(type_name: str) -> str:
    """Nombre canónico de un tipo escalar de BigQuery (``INT64`` → ``INTEGER``)."""
    key = type_name.strip().upper()
    return TYPE_ALIASES.get(key, key)


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]


def bigquery_schema_fields(columns: Iterable[Column]) -> list[dict]:
    """Schema en el formato JSON de BigQuery / Terraform (``google_bigquery_table.schema``)."""
    return [
        {"name": c.name, "type": c.type, "mode": c.mode, "description": c.description}
        for c in columns
    ]


def schema_json(table: str) -> str:
    """Texto exacto del archivo de schema de Terraform (indentado a 2, con salto final)."""
    return json.dumps(bigquery_schema_fields(TABLES[table]), indent=2, ensure_ascii=False) + "\n"


def write_schema_files(directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    written = []
    for table in TABLES:
        path = directory / f"{table}.json"
        path.write_text(schema_json(table), encoding="utf-8")
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Schemas de las tablas del Lote 2.4.")
    parser.add_argument("--write", type=Path, help="Carpeta donde escribir los JSON de Terraform.")
    args = parser.parse_args(argv)
    if args.write:
        for path in write_schema_files(args.write):
            print(path)
        return 0
    for table in TABLES:
        print(table, column_names(table))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
