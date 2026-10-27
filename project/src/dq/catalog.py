"""Catálogo del Lote 5.2: cada regla de ARCHITECTURE.md §5.10 → expectativas de Great Expectations (GX 1.x).

Sin dependencias de GX (se puede importar en ``ta-streaming``). Tres formas de llevar una regla a GX:

- ``native``: la expectativa de GX sobre la columna de datos (``ExpectColumnValuesToNotBeNull``…), con el
  ``mostly`` de la regla. Los nulos no cuentan en las de formato o rango, como en §5.10.
- ``flag``: las reglas ``custom`` por fila (predicado SQL o Python de §5.10): ``frames`` evalúa la
  comprobación del 5.1 (``quality.checks``) en cada fila y deja una columna ``dq__{id}`` (True / False /
  None = no aplica); GX la valida con ``ExpectColumnValuesToBeInSet([True])`` y el ``mostly`` de la regla.
- ``metric``: las métricas agregadas (completitud, duplicados, latencia…): ``frames`` las calcula y GX
  valida una tabla larga (``rule``, ``key``, ``value``) con ``ExpectColumnValuesToBeBetween`` y
  ``row_condition`` por regla.

Las reglas de Raw y Trusted son las de ``quality.rules.RULES`` (DQ-01…DQ-63, sin cambios); las de Curated
(DQ-64…DQ-77) son nuevas del 5.2 y están en ``CURATED_RULES`` (y en §5.10; una prueba lo exige).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from quality.checks import CHECKS_BY_ID
from quality.rules import RULES, Rule

#: Nombre clásico de la expectativa (§5.10) → clase de GX 1.x.
NATIVE = {
    "expect_column_values_to_not_be_null": "ExpectColumnValuesToNotBeNull",
    "expect_column_values_to_match_regex": "ExpectColumnValuesToMatchRegex",
    "expect_column_values_to_be_in_set": "ExpectColumnValuesToBeInSet",
    "expect_column_values_to_be_between": "ExpectColumnValuesToBeBetween",
    "expect_column_value_lengths_to_be_between": "ExpectColumnValueLengthsToBeBetween",
    "expect_column_pair_values_a_to_be_greater_than_b": "ExpectColumnPairValuesAToBeGreaterThanB",
    "expect_column_values_to_be_unique": "ExpectColumnValuesToBeUnique",
    "expect_column_proportion_of_unique_values_to_be_between": "ExpectColumnProportionOfUniqueValuesToBeBetween",
    "expect_column_unique_value_count_to_be_between": "ExpectColumnUniqueValueCountToBeBetween",
}
#: Reglas nativas con otra forma que la sugerida en §5.10 (misma semántica que la comprobación del 5.1).
NATIVE_OVERRIDES = {
    "DQ-40": ("ExpectColumnValuesToMatchRegex", {"regex": r"\S"}),  # text:not_empty = algún carácter no blanco
    "DQ-34": ("ExpectColumnProportionOfNonNullValuesToBeBetween", None),
}
#: Métricas agregadas de Raw y Trusted: (mínimo, máximo) del valor que calcula ``frames.layer_metrics``.
METRIC_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    "DQ-01": (1.0, None),   # fracción de archivos con las columnas del contrato, en orden
    "DQ-02": (1.0, None),   # anonimización
    "DQ-05": (0.95, None),  # completitud
    "DQ-15": (None, 0.01),  # duplicados (Raw)
    "DQ-48": (None, 0.01),  # duplicados (Trusted)
    "DQ-58": (0.99, 1.0),   # filas de Trusted / event_id distintos de Raw
    "DQ-59": (None, 300.0),  # latencia p95 (s)
    "DQ-60": (0.30, 0.37),  # participación de cada fuente (una fila por fuente)
    "DQ-62": (0.88, None),  # acuerdo del NLP con label_sentiment
}
TABLE_NAMES = {"raw": "ta_raw.posts_raw", "trusted": "ta_trusted.posts"}
FLAG_PREFIX = "dq__"


@dataclass(frozen=True)
class Spec:
    """Una expectativa de GX que sale de una regla."""

    rule: str
    layer: str
    table: str  # raw/trusted: posts_raw o posts; curated: la tabla
    kind: str  # native | flag | metric
    ge: str  # clase de great_expectations.expectations
    kwargs: dict = field(default_factory=dict)
    severity: str = "error"
    kpi: str = "otro"
    threshold: float | None = None
    label: str = ""  # parte del check_name (columna o tabla.columna)
    scope: str = "*"


def _threshold(rule: Rule, kwargs: dict) -> float | None:
    if rule.mostly is not None and "mostly" in kwargs:
        return float(rule.mostly)
    for key in ("min_value", "max_value"):
        if isinstance(kwargs.get(key), (int, float)):
            return float(kwargs[key])
    return 1.0


def rule_kind(rule: Rule) -> str:
    """``native``, ``flag`` o ``metric`` para una regla de Raw/Trusted."""
    if rule.id in METRIC_BOUNDS:
        return "metric"
    if rule.id in NATIVE_OVERRIDES or rule.expectation in NATIVE:
        return "native"
    if rule.check in CHECKS_BY_ID:
        return "flag"
    raise ValueError(f"{rule.id}: sin forma en GX ({rule.expectation})")


def layer_specs(layer: str) -> list[Spec]:
    """Las expectativas de una capa (``raw`` o ``trusted``), en el orden de §5.10."""
    if layer not in TABLE_NAMES:
        raise ValueError(layer)
    table = TABLE_NAMES[layer].split(".")[1]
    out = []
    for rule in RULES:
        if layer not in rule.layers:
            continue
        kind = rule_kind(rule)
        common = dict(rule=rule.id, layer=layer, table=table, kind=kind, severity=rule.severity, kpi=rule.kpi, scope=rule.scope)
        if kind == "metric":
            lo, hi = METRIC_BOUNDS[rule.id]
            kwargs = {"column": "value", "row_condition": f'rule=="{rule.id}"', "condition_parser": "pandas"}
            kwargs.update({k: v for k, v in (("min_value", lo), ("max_value", hi)) if v is not None})
            out.append(Spec(ge="ExpectColumnValuesToBeBetween", kwargs=kwargs, threshold=lo if lo is not None else hi,
                            label=rule.column, **common))
        elif kind == "flag":
            kwargs = {"column": FLAG_PREFIX + rule.id, "value_set": [True], "mostly": float(rule.mostly or 1.0)}
            out.append(Spec(ge="ExpectColumnValuesToBeInSet", kwargs=kwargs, threshold=float(rule.mostly or 1.0),
                            label=rule.column, **common))
        else:
            ge, extra = NATIVE_OVERRIDES.get(rule.id, (NATIVE.get(rule.expectation), None))
            kwargs = dict(rule.kwargs if extra is None else extra)
            if "column_A" not in kwargs:
                kwargs["column"] = rule.column
            if rule.mostly is not None and ge not in ("ExpectColumnValuesToBeUnique",) and "proportion" not in ge.lower() \
                    and "UniqueValueCount" not in ge:
                kwargs["mostly"] = float(rule.mostly)
            out.append(Spec(ge=ge, kwargs=kwargs, threshold=_threshold(rule, kwargs), label=rule.column, **common))
    return out


# --- Curated (DQ-64…DQ-77, nuevas del 5.2) ------------------------------------------------------------

_C = ("curated",)
CURATED_RULES: tuple[Rule, ...] = (
    Rule("DQ-64", _C, "*", "metric:schema", "custom: columnas de cada Parquet = contracts/athena/curated.sql", {}, None,
         "todos los archivos", "otro", "error", why="Las 9 tablas con el contrato (como D8 del 4.4)"),
    Rule("DQ-65", _C, "*", "metric:anonimizacion", "custom: sin columnas user_id/handle + user_hash hex de 64", {}, None,
         "100 %", "anonimizacion", "error", why="KPI de §5.10 en Curated (fact_posts, dim_user, edges_user_brand)"),
    Rule("DQ-66", _C, "fact_posts.event_id", "metric:unicidad", "expect_column_values_to_be_unique", {}, None, "100 %",
         "unicidad", "error", why="Grano de fact_posts = event_id"),
    Rule("DQ-67", _C, "fact_posts.*", "metric:completitud", "custom: no nulos de event_id, event_time, source, user_hash, "
         "date_key / (5 × filas)", {"min_value": 0.95}, None, "≥ 95 %", "completitud", "error"),
    Rule("DQ-68", _C, "fact_posts.*", "metric:fact_vs_trusted", "custom: filas de fact_posts / filas de Trusted",
         {"min_value": 1.0, "max_value": 1.0}, None, "= 1", "otro", "error", why="fact_posts = Trusted (ADR-26)"),
    Rule("DQ-69", _C, "fact_posts.date_key", "partition:fact_date_key", "custom: date_key = yyyymmdd(dt) y dt = DATE(event_time)",
         {}, 1.0, "100 %", "otro", "error", why="§9 #9"),
    Rule("DQ-70", _C, "fact_posts.{source,brand_primary,user_hash,date_key}", "fk:fact_posts",
         "expect_column_values_to_be_in_set (valores de la dimensión)", {}, 1.0, "100 %", "otro", "error",
         why="Llaves foráneas naturales de §5.7"),
    Rule("DQ-71", _C, "dim_date.date_key", "metric:dim_date_continuous", "custom: días entre el primero y el último = filas",
         {"min_value": 0, "max_value": 0}, None, "0 huecos", "otro", "error"),
    Rule("DQ-72", _C, "agg_trend_daily.*", "metric:recompute", "custom: = recalculado desde fact_posts",
         {"min_value": 0, "max_value": 0}, None, "0 filas distintas", "otro", "error", why="Decisión 2 del 4.3 incluida"),
    Rule("DQ-73", _C, "agg_emotion_by_brand_hour.*", "metric:recompute", "custom: = recalculado desde fact_posts",
         {"min_value": 0, "max_value": 0}, None, "0 filas distintas", "otro", "error"),
    Rule("DQ-74", _C, "dim_user.*", "metric:recompute", "custom: = recalculado desde Trusted (followers)",
         {"min_value": 0, "max_value": 0}, None, "0 filas distintas", "otro", "error"),
    Rule("DQ-75", _C, "edges_user_brand.*", "metric:recompute", "custom: = recalculado desde fact_posts (ADR-27)",
         {"min_value": 0, "max_value": 0}, None, "0 filas distintas", "otro", "error"),
    Rule("DQ-76", _C, "edges_hashtag_cooccurrence.*", "metric:recompute", "custom: = recalculado desde Trusted (ADR-27)",
         {"min_value": 0, "max_value": 0}, None, "0 filas distintas", "otro", "error"),
    Rule("DQ-77", _C, "edges_*", "edges:invariants", "expect_column_values_to_be_between / pair / compound unique", {}, 1.0,
         "100 %", "otro", "error", why="weight ≥ 1, hashtag_a < hashtag_b, (user_hash, brand) único"),
)
CURATED_RULES_BY_ID = {r.id: r for r in CURATED_RULES}
CURATED_METRICS = ("DQ-64", "DQ-65", "DQ-67", "DQ-68", "DQ-71", "DQ-72", "DQ-73", "DQ-74", "DQ-75", "DQ-76")
#: Columnas que fact_posts toma de sus dimensiones (DQ-70): columna → (dimensión, su llave).
FACT_FKS = {"source": ("dim_source", "source"), "brand_primary": ("dim_brand", "brand"),
            "user_hash": ("dim_user", "user_hash"), "date_key": ("dim_date", "date_key")}


def curated_specs() -> list[Spec]:
    """Las expectativas de Curated. Las de DQ-70 llevan ``value_set`` = ``"@dim.llave"`` (lo resuelve ``suites``)."""
    out = []

    def add(rule_id: str, table: str, kind: str, ge: str, kwargs: dict[str, Any], threshold: float | None, label: str):
        r = CURATED_RULES_BY_ID[rule_id]
        out.append(Spec(rule=rule_id, layer="curated", table=table, kind=kind, ge=ge, kwargs=kwargs, severity=r.severity,
                        kpi=r.kpi, threshold=threshold, label=label))

    for rule_id in CURATED_METRICS:
        lo, hi = {"DQ-64": (1.0, None), "DQ-65": (1.0, None), "DQ-67": (0.95, None), "DQ-68": (1.0, 1.0)}.get(rule_id, (0, 0))
        kwargs = {"column": "value", "row_condition": f'rule=="{rule_id}"', "condition_parser": "pandas"}
        kwargs.update({k: v for k, v in (("min_value", lo), ("max_value", hi)) if v is not None})
        add(rule_id, "metrics", "metric", "ExpectColumnValuesToBeBetween", kwargs, float(lo if lo is not None else hi),
            CURATED_RULES_BY_ID[rule_id].column)
    add("DQ-66", "fact_posts", "native", "ExpectColumnValuesToBeUnique", {"column": "event_id"}, 1.0, "fact_posts.event_id")
    add("DQ-69", "fact_posts", "flag", "ExpectColumnValuesToBeInSet", {"column": FLAG_PREFIX + "DQ-69", "value_set": [True]}, 1.0,
        "fact_posts.date_key")
    for col, (dim, key) in FACT_FKS.items():
        add("DQ-70", "fact_posts", "native", "ExpectColumnValuesToBeInSet", {"column": col, "value_set": f"@{dim}.{key}"}, 1.0,
            f"fact_posts.{col}")
    add("DQ-77", "edges_user_brand", "native", "ExpectColumnValuesToBeBetween", {"column": "weight", "min_value": 1}, 1.0,
        "edges_user_brand.weight")
    add("DQ-77", "edges_user_brand", "native", "ExpectCompoundColumnsToBeUnique", {"column_list": ["user_hash", "brand"]}, 1.0,
        "edges_user_brand.(user_hash,brand)")
    add("DQ-77", "edges_hashtag_cooccurrence", "native", "ExpectColumnValuesToBeBetween", {"column": "weight", "min_value": 1}, 1.0,
        "edges_hashtag_cooccurrence.weight")
    add("DQ-77", "edges_hashtag_cooccurrence", "native", "ExpectColumnPairValuesAToBeGreaterThanB",
        {"column_A": "hashtag_b", "column_B": "hashtag_a", "or_equal": False}, 1.0, "edges_hashtag_cooccurrence.hashtag_a<hashtag_b")
    return out


def all_rules() -> tuple[Rule, ...]:
    return tuple(RULES) + CURATED_RULES
