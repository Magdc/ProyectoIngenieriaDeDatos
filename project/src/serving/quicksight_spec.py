"""Definiciones del tablero de QuickSight (Lote 6.1, HU-037 y HU-038; ARCHITECTURE.md §3.27 y §3.27.1).

Todo lo que se despliega en QuickSight sale de aquí, sin nube: la fuente de datos Athena (workgroup ``ta-analytics``
con el rol de servicio ``ta-quicksight-role``), los cuatro datasets SPICE (una consulta de ``sql/`` cada uno, solo
sobre ``ta_curated``), los grupos y usuarios por rol IAM, los permisos y la definición del análisis y del tablero (dos
hojas: «Tendencias» para HU-037 y «Relaciones» para HU-038). ``deploy`` solo traduce esto a llamadas de boto3.

Reglas que comprueban ``dataset_problems`` y ``definition_problems`` (y las pruebas): los datasets leen solo
``ta_curated``; ninguna columna de salida es ``user_hash``, ``user_id`` ni ``handle``; las horas van en
``America/Bogota``; cada campo del tablero existe en su dataset y cada id es único.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lakehouse.catalog_spec import ANONYMIZED_COLUMNS, IDENTITY_COLUMNS

REGION = "us-east-1"
NAMESPACE = "default"
EDITION = "ENTERPRISE"
TIMEZONE = "America/Bogota"
WORKGROUP = "ta-analytics"
SERVICE_ROLE = "ta-quicksight-role"
ALLOWED_DATABASE = "ta_curated"
SQL_DIR = Path(__file__).resolve().parent / "sql"

DATA_SOURCE_ID = "ta-athena-curated"
DATA_SOURCE_NAME = "ta · Athena ta_curated"
ANALYSIS_ID = "ta-analisis-tendencias"
DASHBOARD_ID = "ta-tablero-tendencias"
DASHBOARD_NAME = "TrendAnalyzer · Tendencias y relaciones"
REFRESH_SCHEDULE_ID = "ta-cada-hora"

#: Grupos de QuickSight y el rol IAM (Terraform, módulo ``quicksight_access``) que entra a cada uno.
GROUPS = {"autores": "ta-autores", "lectores": "ta-lectores"}
ACCESS_ROLES = {"autores": "ta-quicksight-autor", "lectores": "ta-quicksight-lector"}
ACCESS_USER_ROLE = {"autores": "AUTHOR", "lectores": "READER"}
DEMO_SESSION = "demo"
#: QuickSight exige un correo al registrar un usuario IAM; los de los roles de demostración no reciben nada.
DEMO_EMAIL = "{role}@example.com"
#: Papeles Pro: activan la cuota de infraestructura de la cuenta (250 USD/mes) al terminar la prueba gratis.
PRO_ROLES = {"ADMIN_PRO": "ADMIN", "AUTHOR_PRO": "AUTHOR", "READER_PRO": "READER"}
#: Sin Pro, la cuenta pierde los 100 GB de SPICE de la prueba y el límite queda en lo consumido (v2.48): con compra
#: automática QuickSight suma la capacidad que haga falta (0,38 USD/GB al mes; hoy ~0,2 GB).
SPICE_PURCHASE_MODE = "AUTO_PURCHASE"

FORBIDDEN_COLUMNS = tuple(ANONYMIZED_COLUMNS) + tuple(IDENTITY_COLUMNS)
#: Tipo de Athena (``ResultSetMetadata``) → tipo de columna de QuickSight.
ATHENA_TO_QS = {"varchar": "STRING", "bigint": "INTEGER", "integer": "INTEGER", "double": "DECIMAL",
                "timestamp": "DATETIME", "date": "DATETIME", "boolean": "BOOLEAN"}

# --- permisos (acciones de QuickSight por recurso) -----------------------------------------------------------------

DATA_SOURCE_OWNER = ["quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions",
                     "quicksight:PassDataSource", "quicksight:UpdateDataSource", "quicksight:DeleteDataSource",
                     "quicksight:UpdateDataSourcePermissions"]
DATASET_OWNER = ["quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions", "quicksight:PassDataSet",
                 "quicksight:DescribeIngestion", "quicksight:ListIngestions", "quicksight:UpdateDataSet",
                 "quicksight:DeleteDataSet", "quicksight:CreateIngestion", "quicksight:CancelIngestion",
                 "quicksight:UpdateDataSetPermissions"]
ANALYSIS_OWNER = ["quicksight:RestoreAnalysis", "quicksight:UpdateAnalysisPermissions", "quicksight:DeleteAnalysis",
                  "quicksight:DescribeAnalysisPermissions", "quicksight:QueryAnalysis", "quicksight:DescribeAnalysis",
                  "quicksight:UpdateAnalysis"]
DASHBOARD_VIEWER = ["quicksight:DescribeDashboard", "quicksight:ListDashboardVersions", "quicksight:QueryDashboard"]
DASHBOARD_OWNER = DASHBOARD_VIEWER + ["quicksight:DescribeDashboardPermissions", "quicksight:UpdateDashboard",
                                      "quicksight:DeleteDashboard", "quicksight:UpdateDashboardPermissions",
                                      "quicksight:UpdateDashboardPublishedVersion"]


def principal_arn(account: str, kind: str, name: str) -> str:
    """ARN de un usuario (``kind = "user"``) o grupo (``"group"``) de QuickSight en el espacio ``default``."""
    return f"arn:aws:quicksight:{REGION}:{account}:{kind}/{NAMESPACE}/{name}"


def permissions(principals: list[str], actions: list[str]) -> list[dict[str, Any]]:
    return [{"Principal": p, "Actions": list(actions)} for p in principals]


# --- datasets -------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Col:
    name: str
    type: str  # STRING | INTEGER | DECIMAL | DATETIME
    description: str


@dataclass(frozen=True)
class Dataset:
    key: str  # identificador dentro de la definición del tablero
    dataset_id: str
    name: str
    sql_file: str
    hu: str
    tables: tuple[str, ...]  # tablas de ta_curated que lee
    columns: tuple[Col, ...]

    @property
    def sql(self) -> str:
        return (SQL_DIR / self.sql_file).read_text(encoding="utf-8").replace("\r\n", "\n")

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Col:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.key}.{name}")


DATASETS: tuple[Dataset, ...] = (
    Dataset(
        key="publicaciones", dataset_id="ta-ds-publicaciones", name="ta · Publicaciones (hora de Bogotá)",
        sql_file="publicaciones.sql", hu="HU-037", tables=("fact_posts", "dim_brand", "dim_source"),
        columns=(
            Col("event_id", "STRING", "Identificador del comentario (UUID del simulador); una fila por comentario"),
            Col("origen", "STRING", "Origen del dato: stream | historical"),
            Col("red_clave", "STRING", "Red social simulada (clave): reddit | facebook | instagram"),
            Col("red", "STRING", "Nombre visible de la red social"),
            Col("marca_clave", "STRING", "Marca principal (clave de brands.yaml)"),
            Col("marca", "STRING", "Nombre visible de la marca principal"),
            Col("rol_marca", "STRING", "Papel de la marca: propia | competidor"),
            Col("tipo_mencion", "STRING", "comparativa (menciona dos marcas) | una marca"),
            Col("hora_bogota", "DATETIME", "Instante del comentario en America/Bogota (se guarda en UTC)"),
            Col("fecha_bogota", "DATETIME", "Día del comentario en America/Bogota"),
            Col("hora_del_dia", "INTEGER", "Hora del día en America/Bogota (0-23)"),
            Col("dia_semana", "INTEGER", "Día de la semana en America/Bogota: 1 = lunes … 7 = domingo"),
            Col("me_gusta", "INTEGER", "Me gusta normalizados según la red (en Reddit, votos: pueden ser negativos)"),
            Col("compartidos", "INTEGER", "Veces compartido (solo Facebook; 0 en las demás)"),
            Col("respuestas", "INTEGER", "Respuestas o comentarios recibidos"),
            Col("interacciones", "INTEGER", "me_gusta + compartidos + respuestas"),
            Col("sentimiento", "STRING", "Sentimiento predicho: positivo | neutral | negativo | sin NLP"),
            Col("sentimiento_con_signo", "DECIMAL", "+confianza si positivo, −confianza si negativo, 0 si neutral; "
                                                    "nulo sin NLP (como avg_sentiment)"),
            Col("emocion", "STRING", "Emoción predicha en español (alegría, tristeza, enojo, miedo, asco, sorpresa, "
                                     "neutral, otras) | sin NLP"),
            Col("con_nlp", "INTEGER", "1 si el comentario tiene NLP; 0 en los del stream anteriores al NLP"),
        ),
    ),
    Dataset(
        key="hashtags", dataset_id="ta-ds-coocurrencia-hashtags", name="ta · Coocurrencia de hashtags",
        sql_file="coocurrencia_hashtags.sql", hu="HU-038", tables=("edges_hashtag_cooccurrence",),
        columns=(
            Col("hashtag_a", "STRING", "Primer hashtag del par (siempre menor que hashtag_b)"),
            Col("hashtag_b", "STRING", "Segundo hashtag del par"),
            Col("par", "STRING", "Los dos hashtags del par, para tablas y etiquetas"),
            Col("comentarios", "INTEGER", "Comentarios que contienen los dos hashtags (weight)"),
        ),
    ),
    Dataset(
        key="marcas", dataset_id="ta-ds-marcas-compartidas", name="ta · Autores compartidos entre marcas",
        sql_file="marcas_compartidas.sql", hu="HU-038", tables=("edges_user_brand", "dim_brand"),
        columns=(
            Col("marca_origen", "STRING", "Primera marca del par (orden alfabético)"),
            Col("marca_destino", "STRING", "Segunda marca del par"),
            Col("autores_en_comun", "INTEGER", "Autores seudonimizados que mencionan las dos marcas"),
            Col("menciones", "INTEGER", "Menciones de esos autores a las dos marcas"),
        ),
    ),
    Dataset(
        key="autores", dataset_id="ta-ds-autores-por-marcas", name="ta · Autores por combinación de marcas",
        sql_file="autores_por_marcas.sql", hu="HU-038", tables=("edges_user_brand", "dim_brand"),
        columns=(
            Col("combinacion", "STRING", "Marcas que menciona el autor, en orden alfabético"),
            Col("n_marcas", "INTEGER", "Número de marcas distintas que menciona el autor"),
            Col("autores", "INTEGER", "Autores con esa combinación de marcas"),
            Col("menciones", "INTEGER", "Menciones de esos autores (suma de weight)"),
        ),
    ),
)
DATASET_BY_KEY = {d.key: d for d in DATASETS}

_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)", re.I)
_COMMENT = re.compile(r"--[^\n]*")


def sql_tables(sql: str) -> set[str]:
    """``base.tabla`` de cada ``FROM``/``JOIN`` (sin contar los comentarios)."""
    return {f"{db}.{t}".lower() for db, t in _TABLE_REF.findall(_COMMENT.sub("", sql))}


def dataset_problems(ds: Dataset) -> list[str]:
    out: list[str] = []
    try:
        sql = ds.sql
    except OSError as exc:
        return [f"{ds.key}: no se puede leer sql/{ds.sql_file} ({exc})"]
    tables = sql_tables(sql)
    outside = sorted(t for t in tables if not t.startswith(ALLOWED_DATABASE + "."))
    if outside:
        out.append(f"{ds.key}: lee fuera de {ALLOWED_DATABASE}: {outside}")
    declared = {f"{ALLOWED_DATABASE}.{t}" for t in ds.tables}
    if tables != declared:
        out.append(f"{ds.key}: tablas leídas {sorted(tables)} ≠ declaradas {sorted(declared)}")
    names = ds.column_names
    if len(set(names)) != len(names):
        out.append(f"{ds.key}: columnas repetidas")
    bad = [n for n in names if n in FORBIDDEN_COLUMNS]
    if bad:
        out.append(f"{ds.key}: columnas prohibidas en la salida: {bad}")
    body = _COMMENT.sub("", sql)
    missing = [n for n in names if not re.search(rf"\b{re.escape(n)}\b", body)]
    if missing:
        out.append(f"{ds.key}: columnas declaradas que no aparecen en el SQL: {missing}")
    for c in ds.columns:
        if c.type not in set(ATHENA_TO_QS.values()):
            out.append(f"{ds.key}.{c.name}: tipo {c.type} no soportado")
        if not c.description.strip():
            out.append(f"{ds.key}.{c.name}: sin descripción")
    return out


def athena_column_problems(ds: Dataset, athena_columns: list[tuple[str, str]]) -> list[str]:
    """Compara las columnas que devolvió Athena para el SQL del dataset con las declaradas (nombre, orden y tipo)."""
    got = [(n, ATHENA_TO_QS.get(t.lower(), t)) for n, t in athena_columns]
    want = [(c.name, c.type) for c in ds.columns]
    out = []
    if got != want:
        out.append(f"{ds.key}: Athena devuelve {got}, declaradas {want}")
    leaked = [n for n, _ in athena_columns if n in FORBIDDEN_COLUMNS]
    if leaked:
        out.append(f"{ds.key}: Athena devuelve columnas prohibidas {leaked}")
    return out


def data_source_request(account: str, role_arn: str, owners: list[str]) -> dict[str, Any]:
    return {
        "AwsAccountId": account, "DataSourceId": DATA_SOURCE_ID, "Name": DATA_SOURCE_NAME, "Type": "ATHENA",
        "DataSourceParameters": {"AthenaParameters": {"WorkGroup": WORKGROUP, "RoleArn": role_arn}},
        "Permissions": permissions(owners, DATA_SOURCE_OWNER),
    }


def dataset_request(account: str, ds: Dataset, data_source_arn: str, owners: list[str]) -> dict[str, Any]:
    """Argumentos de ``create_data_set`` (sin ``Permissions``, ``update_data_set``): SQL propio e importación a SPICE."""
    physical = {
        "CustomSql": {
            "DataSourceArn": data_source_arn,
            "Name": ds.dataset_id,
            "SqlQuery": ds.sql,
            "Columns": [{"Name": c.name, "Type": c.type} for c in ds.columns],
        }
    }
    logical = {
        "Alias": ds.name,
        "Source": {"PhysicalTableId": "sql"},
        "DataTransforms": [
            {"TagColumnOperation": {"ColumnName": c.name, "Tags": [{"ColumnDescription": {"Text": c.description[:500]}}]}}
            for c in ds.columns
        ],
    }
    return {
        "AwsAccountId": account, "DataSetId": ds.dataset_id, "Name": ds.name, "ImportMode": "SPICE",
        "PhysicalTableMap": {"sql": physical}, "LogicalTableMap": {"tabla": logical},
        "Permissions": permissions(owners, DATASET_OWNER),
    }


def refresh_schedule() -> dict[str, Any]:
    return {"ScheduleId": REFRESH_SCHEDULE_ID, "RefreshType": "FULL_REFRESH",
            "ScheduleFrequency": {"Interval": "HOURLY", "Timezone": TIMEZONE}}


# --- piezas de la definición ---------------------------------------------------------------------------------------


def _col(ds: str, name: str) -> dict[str, str]:
    return {"DataSetIdentifier": ds, "ColumnName": name}


def dim(fid: str, ds: str, name: str) -> dict[str, Any]:
    return {"CategoricalDimensionField": {"FieldId": fid, "Column": _col(ds, name)}}


def num_dim(fid: str, ds: str, name: str) -> dict[str, Any]:
    return {"NumericalDimensionField": {"FieldId": fid, "Column": _col(ds, name)}}


def date_dim(fid: str, ds: str, name: str, granularity: str = "DAY") -> dict[str, Any]:
    return {"DateDimensionField": {"FieldId": fid, "Column": _col(ds, name), "DateGranularity": granularity}}


def count(fid: str, ds: str, name: str) -> dict[str, Any]:
    return {"CategoricalMeasureField": {"FieldId": fid, "Column": _col(ds, name), "AggregationFunction": "COUNT"}}


def _fmt(kind: str | None, decimals: int) -> dict[str, Any] | None:
    if kind == "percent":
        return {"FormatConfiguration": {"PercentageDisplayFormatConfiguration": {
            "DecimalPlacesConfiguration": {"DecimalPlaces": decimals}}}}
    if kind == "number":
        return {"FormatConfiguration": {"NumberDisplayFormatConfiguration": {
            "DecimalPlacesConfiguration": {"DecimalPlaces": decimals}}}}
    return None


def measure(fid: str, ds: str, name: str, agg: str | None = "SUM", fmt: str | None = None, decimals: int = 0) -> dict[str, Any]:
    """Medida numérica; ``agg=None`` para un campo calculado que ya agrega (``countIf``, ``sumIf``…)."""
    field: dict[str, Any] = {"FieldId": fid, "Column": _col(ds, name)}
    if agg:
        field["AggregationFunction"] = {"SimpleNumericalAggregation": agg}
    f = _fmt(fmt, decimals)
    if f:
        field["FormatConfiguration"] = f
    return {"NumericalMeasureField": field}


def _label(text: str) -> dict[str, Any]:
    return {"Visibility": "VISIBLE", "FormatText": {"PlainText": text}}


def axis(text: str) -> dict[str, Any]:
    """Etiqueta propia de un eje (en vez del nombre de la columna con su agregación)."""
    return {"Visibility": "VISIBLE", "AxisLabelOptions": [{"CustomLabel": text}]}


def legend(text: str) -> dict[str, Any]:
    return {"Visibility": "VISIBLE", "Title": {"Visibility": "VISIBLE", "CustomLabel": text}}


LOG_AXIS = {"AxisOptions": {"DataOptions": {"NumericAxisOptions": {"Scale": {"Logarithmic": {"Base": 10}}}}}}


def kpi(vid: str, title: str, value: dict[str, Any], subtitle: str) -> dict[str, Any]:
    return {"KPIVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                          "ChartConfiguration": {"FieldWells": {"Values": [value]}}}}


def line(vid: str, title: str, category: dict, value: dict, color: dict, subtitle: str, **extra: Any) -> dict[str, Any]:
    """``extra`` va a ``ChartConfiguration`` (``XAxisLabelOptions``, ``PrimaryYAxisDisplayOptions``, ``Legend``…)."""
    return {"LineChartVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                                "ChartConfiguration": {"Type": "LINE", "FieldWells": {"LineChartAggregatedFieldWells": {
                                    "Category": [category], "Values": [value], "Colors": [color]}}, **extra}}}


def bar(vid: str, title: str, category: dict, value: dict, color: dict | None, subtitle: str, *,
        arrangement: str = "CLUSTERED", orientation: str = "VERTICAL", sort_desc: bool = False, **extra: Any) -> dict[str, Any]:
    wells: dict[str, Any] = {"Category": [category], "Values": [value]}
    if color:
        wells["Colors"] = [color]
    config: dict[str, Any] = {"FieldWells": {"BarChartAggregatedFieldWells": wells}, "Orientation": orientation,
                              "BarsArrangement": arrangement, **extra}
    if sort_desc:
        fid = next(iter(value.values()))["FieldId"]
        config["SortConfiguration"] = {"CategorySort": [{"FieldSort": {"FieldId": fid, "Direction": "DESC"}}]}
    return {"BarChartVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                               "ChartConfiguration": config}}


def combo(vid: str, title: str, category: dict, bars: dict, color: dict, line_value: dict, subtitle: str,
          **extra: Any) -> dict[str, Any]:
    return {"ComboChartVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                                 "ChartConfiguration": {"BarsArrangement": "CLUSTERED", "FieldWells": {
                                     "ComboChartAggregatedFieldWells": {"Category": [category], "BarValues": [bars],
                                                                        "Colors": [color], "LineValues": [line_value]}},
                                     **extra}}}


def heat_map(vid: str, title: str, rows: dict, columns: dict, value: dict, subtitle: str, **extra: Any) -> dict[str, Any]:
    return {"HeatMapVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                              "ChartConfiguration": {"FieldWells": {"HeatMapAggregatedFieldWells": {
                                  "Rows": [rows], "Columns": [columns], "Values": [value]}}, **extra}}}


def sankey(vid: str, title: str, source: dict, destination: dict, weight: dict, subtitle: str) -> dict[str, Any]:
    return {"SankeyDiagramVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle),
                                    "ChartConfiguration": {"FieldWells": {"SankeyDiagramAggregatedFieldWells": {
                                        "Source": [source], "Destination": [destination], "Weight": [weight]}}}}}


def table(vid: str, title: str, group_by: list[dict], values: list[dict], subtitle: str,
          headers: dict[str, str] | None = None) -> dict[str, Any]:
    """``headers`` = FieldId → encabezado de la columna."""
    fid = next(iter(values[0].values()))["FieldId"]
    config: dict[str, Any] = {
        "FieldWells": {"TableAggregatedFieldWells": {"GroupBy": group_by, "Values": values}},
        "SortConfiguration": {"RowSort": [{"FieldSort": {"FieldId": fid, "Direction": "DESC"}}]}}
    if headers:
        config["FieldOptions"] = {"SelectedFieldOptions": [{"FieldId": f, "CustomLabel": h} for f, h in headers.items()]}
    return {"TableVisual": {"VisualId": vid, "Title": _label(title), "Subtitle": _label(subtitle), "ChartConfiguration": config}}


def text_box(tid: str, text: str) -> dict[str, Any]:
    safe = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return {"SheetTextBoxId": tid, "Content": f"<text-box>{safe}</text-box>"}


def grid(elements: list[tuple[str, str, int, int, int, int]]) -> dict[str, Any]:
    """``(id, tipo, columna, fila, ancho, alto)`` en una cuadrícula de 36 columnas."""
    return {"Configuration": {"GridLayout": {
        "Elements": [{"ElementId": e, "ElementType": t, "ColumnIndex": c, "RowIndex": r, "ColumnSpan": w, "RowSpan": h}
                     for e, t, c, r, w, h in elements],
        "CanvasSizeOptions": {"ScreenCanvasSizeOptions": {"ResizeOption": "RESPONSIVE"}}}}}


def _visual_id(visual: dict[str, Any]) -> str:
    return next(iter(visual.values()))["VisualId"]


# --- el tablero ----------------------------------------------------------------------------------------------------

SHEET_TRENDS = "tendencias"
SHEET_RELATIONS = "relaciones"
P, H, M, A = "publicaciones", "hashtags", "marcas", "autores"

CALCULATED_FIELDS = [
    {"DataSetIdentifier": P, "Name": "pct_negativos",
     "Expression": "countIf({event_id}, {sentimiento} = 'negativo') / countIf({event_id}, {con_nlp} = 1)"},
    {"DataSetIdentifier": A, "Name": "pct_autores_multimarca",
     "Expression": "sumIf({autores}, {n_marcas} > 1) / sum({autores})"},
]
NOTE_TRENDS = ("Horas y días en America/Bogota (los datos se guardan en UTC). Fuente: ta_curated por Athena, importado "
               "a SPICE cada hora. Sin datos de autores: solo agregados. El sentimiento medio va de −1 (negativo) a +1 "
               "(positivo); los comentarios del stream anteriores al NLP cuentan en el volumen, no en el sentimiento.")
NOTE_RELATIONS = ("Relaciones entre autores, marcas y hashtags (ta_curated.edges_user_brand y "
                  "edges_hashtag_cooccurrence). Los autores solo se cuentan: el tablero nunca muestra user_hash ni "
                  "identificadores en claro.")


def trends_visuals() -> list[dict[str, Any]]:
    return [
        kpi("kpi-comentarios", "Comentarios", count("kpi-comentarios-v", P, "event_id"), "Según los filtros"),
        kpi("kpi-sentimiento", "Sentimiento medio", measure("kpi-sentimiento-v", P, "sentimiento_con_signo", "AVERAGE",
                                                             "number", 2), "De −1 (negativo) a +1 (positivo)"),
        kpi("kpi-negativos", "Comentarios negativos", measure("kpi-negativos-v", P, "pct_negativos", None, "percent", 1),
            "Sobre los que tienen NLP"),
        kpi("kpi-interacciones", "Interacciones", measure("kpi-interacciones-v", P, "interacciones"),
            "Me gusta + compartidos + respuestas"),
        line("linea-volumen", "Comentarios por día y marca", date_dim("linea-volumen-x", P, "fecha_bogota"),
             count("linea-volumen-v", P, "event_id"), dim("linea-volumen-c", P, "marca"),
             "Día en America/Bogota · escala logarítmica (el stream trae ~100 veces más por día que el histórico)",
             XAxisLabelOptions=axis("Día (Bogotá)"), PrimaryYAxisLabelOptions=axis("Comentarios"),
             PrimaryYAxisDisplayOptions=LOG_AXIS, Legend=legend("Marca")),
        line("linea-sentimiento", "Sentimiento medio por día y marca", date_dim("linea-sentimiento-x", P, "fecha_bogota"),
             measure("linea-sentimiento-v", P, "sentimiento_con_signo", "AVERAGE", "number", 2),
             dim("linea-sentimiento-c", P, "marca"), "Solo comentarios con NLP",
             XAxisLabelOptions=axis("Día (Bogotá)"), PrimaryYAxisLabelOptions=axis("Sentimiento medio (−1 a +1)"),
             Legend=legend("Marca")),
        combo("combo-marcas", "Marca propia frente a competidores", dim("combo-marcas-x", P, "marca"),
              count("combo-marcas-b", P, "event_id"), dim("combo-marcas-c", P, "rol_marca"),
              measure("combo-marcas-l", P, "sentimiento_con_signo", "AVERAGE", "number", 2),
              "Barras: comentarios (color = propia o competidor) · línea: sentimiento medio",
              CategoryLabelOptions=axis("Marca"), PrimaryYAxisLabelOptions=axis("Comentarios"),
              SecondaryYAxisLabelOptions=axis("Sentimiento medio"), Legend=legend("Papel de la marca")),
        bar("barras-red", "Comentarios por red social y marca", dim("barras-red-x", P, "red"),
            count("barras-red-v", P, "event_id"), dim("barras-red-c", P, "marca"), "Fuente simulada",
            CategoryLabelOptions=axis("Red social"), ValueLabelOptions=axis("Comentarios"), Legend=legend("Marca")),
        bar("barras-sentimiento", "Reparto del sentimiento por marca", dim("barras-sentimiento-x", P, "marca"),
            count("barras-sentimiento-v", P, "event_id"), dim("barras-sentimiento-c", P, "sentimiento"),
            "Porcentaje de comentarios con NLP", arrangement="STACKED_PERCENT",
            CategoryLabelOptions=axis("Marca"), ValueLabelOptions=axis("% de comentarios"), Legend=legend("Sentimiento")),
        bar("barras-emociones", "Emociones por marca", dim("barras-emociones-x", P, "marca"),
            count("barras-emociones-v", P, "event_id"), dim("barras-emociones-c", P, "emocion"),
            "Porcentaje de comentarios con NLP", arrangement="STACKED_PERCENT",
            CategoryLabelOptions=axis("Marca"), ValueLabelOptions=axis("% de comentarios"), Legend=legend("Emoción")),
        heat_map("calor-hora", "Comentarios por hora del día", dim("calor-hora-f", P, "marca"),
                 num_dim("calor-hora-c", P, "hora_del_dia"), count("calor-hora-v", P, "event_id"),
                 "Hora en America/Bogota (0-23)",
                 RowLabelOptions=axis("Marca"), ColumnLabelOptions=axis("Hora del día (Bogotá)")),
    ]


def relations_visuals() -> list[dict[str, Any]]:
    return [
        kpi("kpi-multimarca", "Autores que mencionan varias marcas",
            measure("kpi-multimarca-v", A, "pct_autores_multimarca", None, "percent", 1), "Sobre todos los autores"),
        bar("barras-combinaciones", "Autores por combinación de marcas", dim("barras-combinaciones-x", A, "combinacion"),
            measure("barras-combinaciones-v", A, "autores"), None, "Cada autor cuenta una vez",
            orientation="HORIZONTAL", sort_desc=True,
            CategoryLabelOptions=axis("Marcas que menciona"), ValueLabelOptions=axis("Autores")),
        sankey("sankey-marcas", "Autores que comparten las dos marcas", dim("sankey-marcas-o", M, "marca_origen"),
               dim("sankey-marcas-d", M, "marca_destino"), measure("sankey-marcas-v", M, "autores_en_comun"),
               "Ancho = autores que mencionan las dos"),
        heat_map("calor-hashtags", "Coocurrencia de hashtags", dim("calor-hashtags-f", H, "hashtag_a"),
                 dim("calor-hashtags-c", H, "hashtag_b"), measure("calor-hashtags-v", H, "comentarios"),
                 "Comentarios que contienen los dos hashtags",
                 RowLabelOptions=axis("Hashtag"), ColumnLabelOptions=axis("Otro hashtag")),
        table("tabla-pares", "Pares de hashtags más frecuentes", [dim("tabla-pares-g", H, "par")],
              [measure("tabla-pares-v", H, "comentarios")], "Ordenados por comentarios",
              headers={"tabla-pares-g": "Par de hashtags", "tabla-pares-v": "Comentarios"}),
    ]


def _category_filter(fid: str, ds: str, name: str) -> dict[str, Any]:
    return {"CategoryFilter": {"FilterId": fid, "Column": _col(ds, name), "Configuration": {
        "FilterListConfiguration": {"MatchOperator": "CONTAINS", "SelectAllOptions": "FILTER_ALL_VALUES"}}}}


def _scope(sheet: str, visuals: list[str] | None = None) -> dict[str, Any]:
    conf: dict[str, Any] = {"SheetId": sheet, "Scope": "ALL_VISUALS" if visuals is None else "SELECTED_VISUALS"}
    if visuals is not None:
        conf["VisualIds"] = visuals
    return {"SelectedSheets": {"SheetVisualScopingConfigurations": [conf]}}


def filter_groups() -> list[dict[str, Any]]:
    def group(gid: str, flt: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
        return {"FilterGroupId": gid, "Filters": [flt], "ScopeConfiguration": scope, "Status": "ENABLED",
                "CrossDataset": "SINGLE_DATASET"}

    period = {"TimeRangeFilter": {
        "FilterId": "filtro-periodo", "Column": _col(P, "fecha_bogota"), "NullOption": "NON_NULLS_ONLY",
        "IncludeMinimum": True, "IncludeMaximum": True, "TimeGranularity": "DAY",
        "RangeMinimumValue": {"RollingDate": {"Expression": "addDateTime(-365, 'DD', truncDate('DD', now()))"}},
        "RangeMaximumValue": {"RollingDate": {"Expression": "addDateTime(1, 'DD', truncDate('DD', now()))"}}}}
    with_nlp = {"NumericEqualityFilter": {"FilterId": "filtro-con-nlp", "Column": _col(P, "con_nlp"), "Value": 1,
                                          "MatchOperator": "EQUALS", "NullOption": "NON_NULLS_ONLY"}}
    return [
        group("grupo-marca", _category_filter("filtro-marca", P, "marca"), _scope(SHEET_TRENDS)),
        group("grupo-red", _category_filter("filtro-red", P, "red"), _scope(SHEET_TRENDS)),
        group("grupo-origen", _category_filter("filtro-origen", P, "origen"), _scope(SHEET_TRENDS)),
        group("grupo-periodo", period, _scope(SHEET_TRENDS)),
        group("grupo-con-nlp", with_nlp, _scope(SHEET_TRENDS, ["barras-sentimiento", "barras-emociones"])),
    ]


FILTER_CONTROLS = [
    {"Dropdown": {"FilterControlId": "control-marca", "Title": "Marca", "SourceFilterId": "filtro-marca",
                  "Type": "MULTI_SELECT"}},
    {"Dropdown": {"FilterControlId": "control-red", "Title": "Red social", "SourceFilterId": "filtro-red",
                  "Type": "MULTI_SELECT"}},
    {"Dropdown": {"FilterControlId": "control-origen", "Title": "Origen del dato", "SourceFilterId": "filtro-origen",
                  "Type": "MULTI_SELECT"}},
    {"DateTimePicker": {"FilterControlId": "control-periodo", "Title": "Periodo (hora de Bogotá)",
                        "SourceFilterId": "filtro-periodo", "Type": "DATE_RANGE"}},
]

TRENDS_LAYOUT = [
    ("kpi-comentarios", "VISUAL", 0, 0, 9, 4), ("kpi-sentimiento", "VISUAL", 9, 0, 9, 4),
    ("kpi-negativos", "VISUAL", 18, 0, 9, 4), ("kpi-interacciones", "VISUAL", 27, 0, 9, 4),
    ("linea-volumen", "VISUAL", 0, 4, 18, 10), ("linea-sentimiento", "VISUAL", 18, 4, 18, 10),
    ("combo-marcas", "VISUAL", 0, 14, 18, 10), ("barras-red", "VISUAL", 18, 14, 18, 10),
    ("barras-sentimiento", "VISUAL", 0, 24, 18, 10), ("barras-emociones", "VISUAL", 18, 24, 18, 10),
    ("calor-hora", "VISUAL", 0, 34, 36, 8), ("nota-tendencias", "TEXT_BOX", 0, 42, 36, 3),
]
RELATIONS_LAYOUT = [
    ("kpi-multimarca", "VISUAL", 0, 0, 12, 4), ("nota-relaciones", "TEXT_BOX", 12, 0, 24, 4),
    ("barras-combinaciones", "VISUAL", 0, 4, 18, 10), ("sankey-marcas", "VISUAL", 18, 4, 18, 10),
    ("calor-hashtags", "VISUAL", 0, 14, 24, 14), ("tabla-pares", "VISUAL", 24, 14, 12, 14),
]


def definition(dataset_arns: dict[str, str]) -> dict[str, Any]:
    """La definición del análisis y del tablero (la misma para los dos); ``dataset_arns`` = clave → ARN."""
    return {
        "DataSetIdentifierDeclarations": [{"Identifier": d.key, "DataSetArn": dataset_arns[d.key]} for d in DATASETS],
        "Sheets": [
            {"SheetId": SHEET_TRENDS, "Name": "Tendencias", "ContentType": "INTERACTIVE",
             "Description": "HU-037: volumen, sentimiento y emociones por marca, red y periodo",
             "Visuals": trends_visuals(), "FilterControls": FILTER_CONTROLS,
             "TextBoxes": [text_box("nota-tendencias", NOTE_TRENDS)],
             "Layouts": [grid(TRENDS_LAYOUT)],
             "SheetControlLayouts": [{"Configuration": {"GridLayout": {"Elements": [
                 {"ElementId": c[next(iter(c))]["FilterControlId"], "ElementType": "FILTER_CONTROL",
                  "ColumnSpan": 2, "RowSpan": 1} for c in FILTER_CONTROLS]}}}]},
            {"SheetId": SHEET_RELATIONS, "Name": "Relaciones", "ContentType": "INTERACTIVE",
             "Description": "HU-038: autores, marcas y hashtags (sin user_hash)",
             "Visuals": relations_visuals(), "TextBoxes": [text_box("nota-relaciones", NOTE_RELATIONS)],
             "Layouts": [grid(RELATIONS_LAYOUT)]},
        ],
        "CalculatedFields": CALCULATED_FIELDS,
        "FilterGroups": filter_groups(),
        "AnalysisDefaults": {"DefaultNewSheetConfiguration": {
            "InteractiveLayoutConfiguration": {"Grid": {"CanvasSizeOptions": {
                "ScreenCanvasSizeOptions": {"ResizeOption": "RESPONSIVE"}}}},
            "SheetContentType": "INTERACTIVE"}},
    }


DASHBOARD_PUBLISH_OPTIONS = {
    "AdHocFilteringOption": {"AvailabilityStatus": "DISABLED"},
    "ExportToCSVOption": {"AvailabilityStatus": "ENABLED"},
    "SheetControlsOption": {"VisibilityState": "EXPANDED"},
}


# --- comprobaciones de la definición -------------------------------------------------------------------------------


def _walk(node: Any):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def definition_problems(defn: dict[str, Any]) -> list[str]:
    out: list[str] = []
    declared = {d["Identifier"] for d in defn.get("DataSetIdentifierDeclarations", [])}
    if declared != set(DATASET_BY_KEY):
        out.append(f"datasets declarados {sorted(declared)} ≠ {sorted(DATASET_BY_KEY)}")
    calc = {(c["DataSetIdentifier"], c["Name"]) for c in defn.get("CalculatedFields", [])}
    for node in _walk(defn):
        if "DataSetIdentifier" in node and "ColumnName" in node:
            ds, name = node["DataSetIdentifier"], node["ColumnName"]
            if name in FORBIDDEN_COLUMNS:
                out.append(f"usa la columna prohibida {ds}.{name}")
            elif ds not in DATASET_BY_KEY:
                out.append(f"dataset desconocido {ds} ({name})")
            elif name not in DATASET_BY_KEY[ds].column_names and (ds, name) not in calc:
                out.append(f"columna desconocida {ds}.{name}")
    for c in defn.get("CalculatedFields", []):
        for ref in re.findall(r"\{([^}]+)\}", c["Expression"]):
            if ref in FORBIDDEN_COLUMNS or ref not in DATASET_BY_KEY.get(c["DataSetIdentifier"], DATASETS[0]).column_names:
                out.append(f"campo calculado {c['Name']}: referencia inválida {{{ref}}}")
    field_ids = [n["FieldId"] for n in _walk(defn) if "FieldId" in n and "Column" in n]
    dup = sorted({f for f in field_ids if field_ids.count(f) > 1})
    if dup:
        out.append(f"FieldId repetidos: {dup}")
    sheets = defn.get("Sheets", [])
    visual_ids = [_visual_id(v) for s in sheets for v in s.get("Visuals", [])]
    dup = sorted({v for v in visual_ids if visual_ids.count(v) > 1})
    if dup:
        out.append(f"VisualId repetidos: {dup}")
    for s in sheets:
        ids = {_visual_id(v) for v in s.get("Visuals", [])} | {t["SheetTextBoxId"] for t in s.get("TextBoxes", [])}
        placed = {e["ElementId"] for lay in s.get("Layouts", []) for e in lay["Configuration"]["GridLayout"]["Elements"]}
        if ids != placed:
            out.append(f"hoja {s['SheetId']}: sin ubicar {sorted(ids - placed)}, ubicados de más {sorted(placed - ids)}")
        for lay in s.get("Layouts", []):
            for e in lay["Configuration"]["GridLayout"]["Elements"]:
                if e["ColumnIndex"] + e["ColumnSpan"] > 36:
                    out.append(f"hoja {s['SheetId']}: {e['ElementId']} se sale de las 36 columnas")
    filters = {f[next(iter(f))]["FilterId"] for g in defn.get("FilterGroups", []) for f in g["Filters"]}
    for s in sheets:
        for c in s.get("FilterControls", []):
            src = c[next(iter(c))]["SourceFilterId"]
            if src not in filters:
                out.append(f"control {c[next(iter(c))]['FilterControlId']}: filtro {src} inexistente")
    all_visuals = set(visual_ids)
    for g in defn.get("FilterGroups", []):
        for conf in g["ScopeConfiguration"]["SelectedSheets"]["SheetVisualScopingConfigurations"]:
            missing = [v for v in conf.get("VisualIds", []) if v not in all_visuals]
            if missing:
                out.append(f"filtro {g['FilterGroupId']}: visuales inexistentes {missing}")
    return out


def summary(defn: dict[str, Any] | None = None) -> dict[str, Any]:
    """Lo que se desplegaría (para ``quicksight.py plan`` y la evidencia), sin ARNs reales."""
    defn = defn or definition({d.key: f"arn:aws:quicksight:{REGION}:ACCOUNT:dataset/{d.dataset_id}" for d in DATASETS})
    return {
        "region": REGION, "edition": EDITION, "timezone": TIMEZONE,
        "data_source": {"id": DATA_SOURCE_ID, "workgroup": WORKGROUP, "role": SERVICE_ROLE},
        "datasets": [{"id": d.dataset_id, "hu": d.hu, "tables": [f"{ALLOWED_DATABASE}.{t}" for t in d.tables],
                      "columns": len(d.columns), "import_mode": "SPICE"} for d in DATASETS],
        "refresh": refresh_schedule()["ScheduleFrequency"],
        "groups": GROUPS, "access_roles": ACCESS_ROLES,
        "analysis": ANALYSIS_ID, "dashboard": DASHBOARD_ID,
        "sheets": [{"id": s["SheetId"], "name": s["Name"], "visuals": [_visual_id(v) for v in s["Visuals"]]}
                   for s in defn["Sheets"]],
        "filters": [c[next(iter(c))]["Title"] for c in FILTER_CONTROLS],
        "problems": [p for d in DATASETS for p in dataset_problems(d)] + definition_problems(defn),
    }
