"""Reglas de calidad propuestas para el Lote 5.2 (HU-032) a partir del *profiling* (HU-031).

El catálogo ``RULES`` es fijo y es el mismo de ``ARCHITECTURE.md`` §5.10 (una prueba lo exige):
cada regla dice capa, alcance, columna, comprobación medida, expectativa de Great Expectations
sugerida, umbral (``mostly`` = fracción de filas que debe cumplir), KPI, gravedad y el predicado
SQL (Athena) que selecciona las filas que **no** cumplen. ``evaluate(report)`` le pone a cada
regla de la capa perfilada el valor observado y si hoy se cumple.

Gravedad: ``error`` = la suite falla y el KPI queda en ``passed = false``; ``warning`` = se
registra en ``dq_results`` con ``passed = false`` pero la corrida no falla.

Los nombres de expectativa son los de la API clásica de Great Expectations
(``expect_column_values_to_not_be_null``…); en GX 1.x son las clases equivalentes en CamelCase.
**A verificar** con la versión que fije el Lote 5.2. Los predicados SQL están escritos para
Athena (motor v3, Trino) y **no se han corrido** (Athena llega con el Lote 4.1).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from quality.checks import NLP_SINCE
from quality.profile_core import ALL

NLP_SINCE_TEXT = NLP_SINCE.strftime("%Y-%m-%dT%H:%M:%SZ")
BOTH = ("raw", "trusted")
RAW = ("raw",)
TRUSTED = ("trusted",)

# Umbrales derivados de lo medido en el profiling de Raw del 26-oct-2026 (§3.20, §5.10):
# texto de 24–283 caracteres (× 1,5 → 450); 99,98 % / 100 % de textos distintos (margen de 1 pp);
# acuerdo del NLP 92,0 % / 91,8 % (margen de 4 pp); fuentes al 33,3 %; label_sentiment nulo 30,1 % / 29,9 %.
TEXT_MAX_LENGTH = 450
TEXT_DISTINCT_MIN = {"stream": 0.99, "historical": 0.99}
NLP_AGREEMENT_MIN = 0.88
SOURCE_SHARE_RANGE = (0.30, 0.37)
LABEL_NULL_RANGE = (0.25, 0.35)
LATENCY_P95_MAX_S = 300.0


@dataclass(frozen=True)
class Rule:
    id: str
    layers: tuple[str, ...]
    column: str
    check: str
    expectation: str
    kwargs: dict = field(default_factory=dict)
    mostly: float | None = 1.0
    threshold: str = "100 %"
    kpi: str = "otro"
    severity: str = "error"
    scope: str = ALL  # origin: * | stream | historical
    sql_fail: str | None = None
    why: str = ""

    def public(self) -> dict[str, Any]:
        d = asdict(self)
        d["layers"] = list(self.layers)
        return d


def _nn(col: str) -> str:
    return f"{col} IS NULL"


SRC_COL = "source"  # en Trusted es la partición, con el mismo nombre (contracts/athena/posts.sql)
UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

RULES: tuple[Rule, ...] = (
    # --- tabla y anonimización ---
    Rule("DQ-01", BOTH, "*", "metric:schema", "expect_table_columns_to_match_ordered_list",
         {"column_list": "RAW_COLUMNS | TRUSTED_COLUMNS (§3.20.1)"}, None, "todos los archivos", "otro", "error",
         sql_fail=None, why="Esquema = posts_enriched (Raw) o posts (Trusted); medido 1 variante en Raw"),
    Rule("DQ-02", BOTH, "*", "metric:anonimizacion", "custom: sin columnas user_id/handle + DQ-03 + DQ-04", {}, None,
         "100 %", "anonimizacion", "error", why="KPI de §5.10; medido 100 %"),
    Rule("DQ-03", BOTH, "user_hash", "regex:user_hash", "expect_column_values_to_match_regex", {"regex": "^[0-9a-f]{64}$"},
         1.0, "100 %", "anonimizacion", "error", sql_fail="NOT regexp_like(user_hash, '^[0-9a-f]{64}$')"),
    Rule("DQ-04", BOTH, "payload", "payload:no_identity", "custom (SQL)", {}, 1.0, "100 %", "anonimizacion", "error",
         sql_fail="regexp_like(payload, '\"(user_id|handle)\"\\s*:')"),
    # --- completitud ---
    Rule("DQ-05", BOTH, "*", "metric:completitud", "custom: no nulos de event_id, event_time, source, text, user_hash / (5 × filas)",
         {"min_value": 0.95}, None, "≥ 95 %", "completitud", "error", why="KPI de §5.10; medido 100 %"),
    Rule("DQ-06", BOTH, "event_id", "not_null:event_id", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud", "error",
         sql_fail=_nn("event_id")),
    Rule("DQ-07", BOTH, "event_time", "not_null:event_time", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud", "error",
         sql_fail=_nn("event_time")),
    Rule("DQ-08", RAW, "source", "not_null:source", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud", "error",
         sql_fail=_nn("source")),
    Rule("DQ-09", BOTH, "text", "not_null:text", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud", "error",
         sql_fail=_nn("text")),
    Rule("DQ-10", BOTH, "user_hash", "not_null:user_hash", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud", "error",
         sql_fail=_nn("user_hash")),
    Rule("DQ-11", BOTH, "lang", "not_null:lang", "expect_column_values_to_not_be_null", {}, 0.98, "≥ 98 %", "completitud", "warning",
         sql_fail=_nn("lang"), why="El caos null_fields (1 %, §3.9) puede dejarlo nulo; medido 100 % sin caos"),
    Rule("DQ-12", BOTH, "followers", "not_null:followers", "expect_column_values_to_not_be_null", {}, 0.98, "≥ 98 %", "completitud", "warning",
         sql_fail=_nn("followers"), why="Igual que DQ-11"),
    Rule("DQ-13", BOTH, "nlp_model_version", "nlp:complete_since",
         "expect_column_values_to_not_be_null (× 5 columnas de NLP, filas con process_time ≥ NLP_SINCE)", {"since": NLP_SINCE_TEXT}, 0.99,
         f"≥ 99 % desde {NLP_SINCE_TEXT}", "completitud", "warning",
         sql_fail=("process_time >= from_iso8601_timestamp('" + NLP_SINCE_TEXT + "') AND (sentiment_label IS NULL OR sentiment_score IS NULL "
                   "OR emotion_label IS NULL OR emotion_score IS NULL OR nlp_model_version IS NULL)"),
         why="Criterio de verify_enriched --expect-nlp (§3.15). Medido: 65.264 filas del stream procesadas antes de "
             "NLP_SINCE (jobs sin NLP) tienen el NLP nulo; después, 0"),
    # --- unicidad y duplicados ---
    Rule("DQ-14", RAW, "event_id", "metric:unicidad", "expect_column_proportion_of_unique_values_to_be_between", {"min_value": 0.99},
         None, "≥ 99 %", "unicidad", "error", sql_fail=None, why="KPI de §5.10 en Raw (antes de deduplicar)"),
    Rule("DQ-15", RAW, "event_id", "metric:duplicados", "custom: (filas − event_id distintos) / filas", {"max_value": 0.01}, None, "< 1 %",
         "duplicados", "warning", why="En Raw solo se vigila: el KPI oficial se mide en Trusted (DQ-48)"),
    Rule("DQ-16", BOTH, "event_id", "uuid:event_id", "expect_column_values_to_match_regex", {"regex": UUID}, 1.0, "100 %", "otro", "error",
         sql_fail=f"NOT regexp_like(event_id, '{UUID}')"),
    # --- valores y enums ---
    Rule("DQ-17", BOTH, "origin", "enum:origin", "expect_column_values_to_be_in_set", {"value_set": ["stream", "historical"]}, 1.0, "100 %",
         "otro", "error", sql_fail="origin NOT IN ('stream', 'historical')"),
    Rule("DQ-18", RAW, "origin", "partition:origin", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="origin <> part_origin"),
    Rule("DQ-19", BOTH, SRC_COL, "enum:source", "expect_column_values_to_be_in_set", {"value_set": ["reddit", "facebook", "instagram"]}, 1.0,
         "100 %", "otro", "error", sql_fail="source NOT IN ('reddit', 'facebook', 'instagram')"),
    Rule("DQ-20", RAW, "source", "partition:source", "custom (SQL)", {}, 1.0, "100 %", "otro", "error", sql_fail="source <> part_source"),
    Rule("DQ-21", BOTH, "generator", "regex:generator", "expect_column_values_to_match_regex",
         {"regex": "^[a-z0-9][a-z0-9-]*@[0-9]+\\.[0-9]+(\\.[0-9]+)?$"}, 1.0, "100 %", "otro", "error",
         sql_fail="NOT regexp_like(generator, '^[a-z0-9][a-z0-9-]*@[0-9]+\\.[0-9]+(\\.[0-9]+)?$')"),
    Rule("DQ-22", BOTH, "lang", "set:lang", "expect_column_values_to_be_in_set", {"value_set": ["es"]}, 1.0, "100 %", "otro", "warning",
         sql_fail="lang <> 'es'", why="v2.2 solo emite es; otro idioma no rompe el contrato (^[a-z]{2}$) pero sería nuevo"),
    Rule("DQ-23", BOTH, "followers", "range:followers", "expect_column_values_to_be_between", {"min_value": 0}, 1.0, "100 %", "otro", "error",
         sql_fail="followers < 0"),
    Rule("DQ-24", BOTH, "likes", "engagement:likes", "custom (SQL): ≥ 0 salvo Reddit", {}, 1.0, "100 %", "otro", "error",
         sql_fail="source <> 'reddit' AND likes < 0"),
    Rule("DQ-25", BOTH, "shares", "engagement:shares", "custom (SQL) por fuente", {}, 1.0, "100 %", "otro", "error",
         sql_fail="(source IN ('reddit', 'instagram') AND shares IS NOT NULL) OR (source = 'facebook' AND (shares IS NULL OR shares < 0))"),
    Rule("DQ-26", BOTH, "comments", "engagement:comments", "custom (SQL) por fuente", {}, 1.0, "100 %", "otro", "error",
         sql_fail="(source IN ('facebook', 'instagram') AND (comments IS NULL OR comments < 0)) OR (source = 'reddit' AND comments < 0)"),
    Rule("DQ-27", BOTH, "sentiment_label", "enum:sentiment_label", "expect_column_values_to_be_in_set",
         {"value_set": ["negative", "neutral", "positive"]}, 1.0, "100 %", "otro", "error",
         sql_fail="sentiment_label NOT IN ('negative', 'neutral', 'positive')"),
    Rule("DQ-28", BOTH, "emotion_label", "enum:emotion_label", "expect_column_values_to_be_in_set",
         {"value_set": ["joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral"]}, 1.0, "100 %", "otro", "error",
         sql_fail="emotion_label NOT IN ('joy', 'sadness', 'anger', 'fear', 'disgust', 'surprise', 'neutral')"),
    Rule("DQ-29", BOTH, "sentiment_score", "range:sentiment_score", "expect_column_values_to_be_between",
         {"min_value": 0, "max_value": 1}, 1.0, "100 %", "otro", "error", sql_fail="sentiment_score NOT BETWEEN 0 AND 1"),
    Rule("DQ-30", BOTH, "emotion_score", "range:emotion_score", "expect_column_values_to_be_between",
         {"min_value": 0, "max_value": 1}, 1.0, "100 %", "otro", "error", sql_fail="emotion_score NOT BETWEEN 0 AND 1"),
    Rule("DQ-31", BOTH, "nlp_model_version", "regex:nlp_model_version", "expect_column_values_to_match_regex",
         {"regex": "^robertuito-sentiment@\\S+\\+robertuito-emotion@\\S+$"}, 1.0, "100 %", "otro", "error",
         sql_fail="NOT regexp_like(nlp_model_version, '^robertuito-sentiment@\\S+\\+robertuito-emotion@\\S+$')"),
    Rule("DQ-32", BOTH, "nlp_model_version", "metric:nlp_single_version", "expect_column_unique_value_count_to_be_between",
         {"min_value": 1, "max_value": 1}, None, "1 versión", "otro", "warning",
         why="Una sola versión en stream e histórico (§3.17); dos versiones = imágenes de NLP mezcladas"),
    Rule("DQ-33", BOTH, "label_sentiment", "enum:label_sentiment", "expect_column_values_to_be_in_set",
         {"value_set": ["negative", "neutral", "positive"]}, 1.0, "100 % (nulos permitidos)", "otro", "error",
         sql_fail="label_sentiment NOT IN ('negative', 'neutral', 'positive')"),
    Rule("DQ-34", BOTH, "label_sentiment", "metric:label_sentiment_null", "expect_column_proportion_of_non_null_values_to_be_between (o custom)",
         {"min_value": 1 - LABEL_NULL_RANGE[1], "max_value": 1 - LABEL_NULL_RANGE[0]}, None,
         f"nulos entre {LABEL_NULL_RANGE[0]:.0%} y {LABEL_NULL_RANGE[1]:.0%}", "otro", "warning",
         why="nostalgia + adiccion = 30 % por diseño (ADR-13)"),
    Rule("DQ-35", BOTH, "brands", "regex:brands", "custom (SQL): elementos [a-z0-9_]+", {}, 1.0, "100 %", "otro", "error",
         sql_fail="any_match(brands, b -> NOT regexp_like(b, '^[a-z0-9_]+$'))"),
    Rule("DQ-36", BOTH, "brands", "set:brands", "custom (SQL): elementos ∈ claves de brands.yaml", {"value_set": ["postobon", "pepsi", "sprite"]},
         1.0, "100 %", "otro", "error", sql_fail="any_match(brands, b -> b NOT IN ('postobon', 'pepsi', 'sprite'))",
         why="§5.2: claves de brands.yaml. El conjunto se lee de src/simulator/config/brands.yaml (no se fija a mano): v1 = 3 marcas"),
    Rule("DQ-37", BOTH, "brand_primary", "pair:brand_primary", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="brand_primary IS DISTINCT FROM element_at(brands, 1)"),
    Rule("DQ-38", BOTH, "hashtags", "regex:hashtags", "custom (SQL): elementos canónicos", {}, 1.0, "100 %", "otro", "error",
         sql_fail="any_match(hashtags, h -> NOT regexp_like(h, '^[a-z0-9_]{1,100}$'))"),
    Rule("DQ-39", BOTH, "hashtags", "consistency:hashtags_text", "custom (Python: common.hashtags.extract_hashtags)", {}, 1.0, "100 %", "otro",
         "warning", sql_fail=None, why="Sin equivalente simple en SQL (tildes en el texto); lo mide el profiling"),
    Rule("DQ-40", BOTH, "text", "text:not_empty", "expect_column_value_lengths_to_be_between", {"min_value": 1}, 1.0, "100 %", "otro", "error",
         sql_fail="trim(text) = ''"),
    Rule("DQ-41", BOTH, "text", "metric:text_max_length", "expect_column_value_lengths_to_be_between", {"max_value": TEXT_MAX_LENGTH},
         None, f"≤ {TEXT_MAX_LENGTH} caracteres", "otro", "warning", sql_fail=f"length(text) > {TEXT_MAX_LENGTH}",
         why="Máximo medido (283) × 1,5, redondeado a 50"),
    Rule("DQ-42", BOTH, "payload", "payload:json", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="try(CAST(json_parse(payload) AS MAP(VARCHAR, JSON))) IS NULL"),
    Rule("DQ-43", BOTH, "payload", "payload:kind", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="json_extract_scalar(payload, '$.kind') NOT IN ('post', 'comment') OR (source = 'instagram' AND json_extract_scalar(payload, '$.kind') <> 'comment')"),
    # --- tiempo y particiones ---
    Rule("DQ-44", BOTH, "ingest_time", "time:ingest_ge_event", "expect_column_pair_values_a_to_be_greater_than_b",
         {"column_A": "ingest_time", "column_B": "event_time", "or_equal": True}, 1.0, "100 %", "otro", "error",
         sql_fail="ingest_time < event_time"),
    Rule("DQ-45", BOTH, "ingest_time", "time:ingest_lag", "custom (SQL)", {}, 0.98, "≥ 98 % entre 0 y 5 s", "otro", "warning",
         sql_fail="date_diff('millisecond', event_time, ingest_time) NOT BETWEEN 0 AND 5000",
         why="Lag de 0–5 s (§3.8); el caos late (1 %) resta 2–30 min a event_time"),
    Rule("DQ-46", BOTH, "process_time", "time:process_ge_ingest", "expect_column_pair_values_a_to_be_greater_than_b",
         {"column_A": "process_time", "column_B": "ingest_time", "or_equal": True}, 1.0, "100 %", "otro", "error",
         sql_fail="process_time < ingest_time"),
    Rule("DQ-47", BOTH, "event_time", "time:not_future", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="event_time > current_timestamp + INTERVAL '5' MINUTE"),
    Rule("DQ-48", TRUSTED, "event_id", "metric:duplicados", "custom: (filas − event_id distintos) / filas", {"max_value": 0.01}, None, "< 1 %",
         "duplicados", "error", why="KPI oficial de §5.10: se mide en Trusted tras deduplicar"),
    Rule("DQ-49", TRUSTED, "event_id", "metric:unicidad", "expect_column_values_to_be_unique", {}, None, "100 %", "unicidad", "error",
         why="Trusted deduplica por event_id (§5.7); KPI ≥ 99 % y se espera 100 %"),
    Rule("DQ-50", BOTH, "dt", "partition:projection_range", "custom (listado de S3)", {}, 1.0, "100 %", "otro", "error", sql_fail=None,
         why="Athena no ve particiones fuera de projection.*.range (2026-01-01,NOW): se comprueba listando S3"),
    Rule("DQ-51", RAW, "event_time", "partition:hist_event_hour", "custom (SQL)", {}, 1.0, "100 %", "otro", "error", scope="historical",
         sql_fail="part_origin = 'historical' AND (date_format(event_time, '%Y-%m-%d') <> part_dt OR date_format(event_time, '%H') <> part_hr)"),
    Rule("DQ-52", RAW, "process_time", "partition:stream_process_hour", "custom (SQL)", {}, 1.0, "100 % entre −2 min y +65 min", "otro",
         "warning", scope="stream",
         sql_fail="part_origin = 'stream' AND date_diff('second', date_parse(part_dt || ' ' || part_hr, '%Y-%m-%d %H'), process_time) NOT BETWEEN -120 AND 3900",
         why="En el stream dt/hr es la hora de la ventana de escritura en GCS (§5.4, §5.7); medido −19,6 a 3.600 s"),
    Rule("DQ-53", RAW, "dt", "partition:file_name", "custom (Python: nombre del archivo)", {}, 1.0, "100 %", "otro", "warning", sql_fail=None),
    Rule("DQ-54", TRUSTED, "event_time", "partition:trusted_event_date", "custom (SQL)", {}, 1.0, "100 %", "otro", "error",
         sql_fail="date_format(event_time, '%Y-%m-%d') <> dt", why="dt = DATE(event_time) UTC (§5.7)"),
    Rule("DQ-55", TRUSTED, "source", "not_null:source_partition", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud",
         "error", sql_fail=_nn("source")),
    Rule("DQ-56", TRUSTED, "text_clean", "not_null:text_clean", "expect_column_values_to_not_be_null", {}, 1.0, "100 %", "completitud",
         "error", sql_fail=_nn("text_clean"), why="A confirmar con el Lote 4.2 (qué hace la limpieza)"),
    Rule("DQ-57", TRUSTED, "text_clean", "text_clean:not_longer", "custom (SQL)", {}, 1.0, "100 %", "otro", "warning",
         sql_fail="length(text_clean) > length(text)", why="A confirmar con el Lote 4.2"),
    Rule("DQ-58", TRUSTED, "*", "metric:rows_vs_raw", "custom (SQL): filas de Trusted frente a event_id distintos de Raw", {}, None,
         "entre 99 % y 100 % de los event_id distintos de Raw", "duplicados", "error",
         why="Trusted une ambos orígenes sin duplicar; puede descartar filas que no cumplan reglas (§5.7). Comparar la misma foto de Raw"),
    # --- distribución, latencia y texto (vigilancia) ---
    Rule("DQ-59", RAW, "process_time", "metric:latency_p95", "custom (SQL): approx_percentile(process_time − event_time, 0.95)",
         {"max_value": LATENCY_P95_MAX_S}, None, f"p95 < {LATENCY_P95_MAX_S:g} s", "latencia", "warning", scope="stream",
         why="KPI de §5.10; el oficial se mide en BigQuery (posts_enriched), aquí se vigila en S3"),
    Rule("DQ-60", BOTH, SRC_COL, "metric:source_share", "custom (SQL): participación de cada fuente por origen",
         {"min_value": SOURCE_SHARE_RANGE[0], "max_value": SOURCE_SHARE_RANGE[1]}, None,
         f"cada fuente entre {SOURCE_SHARE_RANGE[0]:.0%} y {SOURCE_SHARE_RANGE[1]:.0%}", "otro", "warning",
         why="Tres fuentes a ~0,5 msg/s y el histórico N/3 por fuente"),
    Rule("DQ-61", BOTH, "text", "metric:text_distinct_ratio", "expect_column_proportion_of_unique_values_to_be_between",
         {"min_value": min(TEXT_DISTINCT_MIN.values())}, None, f"≥ {min(TEXT_DISTINCT_MIN.values()):.0%} por origen", "otro", "warning",
         why="Diversidad del generador (§9, riesgo de texto repetido): avisa si las plantillas se degradan"),
    Rule("DQ-62", BOTH, "sentiment_label", "metric:nlp_agreement", "custom (SQL): acuerdo con label_sentiment",
         {"min_value": NLP_AGREEMENT_MIN}, None, f"≥ {NLP_AGREEMENT_MIN:.0%}", "precision_nlp", "warning",
         why="Vigilancia; la precisión formal (matriz de confusión) es HU-033"),
    Rule("DQ-63", BOTH, "generator", "set:generator", "expect_column_values_to_be_in_set", {"value_set": ["comment-gen@1.0"]}, 1.0,
         "100 %", "otro", "warning", sql_fail="generator <> 'comment-gen@1.0'",
         why="Medido: 6 filas de ta-verify@2.1 (centinela de verify_streaming, §3.13) en Raw stream; Trusted debería excluirlas (a confirmar con el 4.2)"),
)

RULES_BY_ID: dict[str, Rule] = {r.id: r for r in RULES}
RULE_IDS: tuple[str, ...] = tuple(r.id for r in RULES)


# --- evaluación contra un informe -------------------------------------------------------------


def _groups_for_scope(report: dict[str, Any], scope: str) -> list[dict[str, Any]]:
    """Grupos de origen (``origin != *``, ``source = *``) del alcance; ``*`` = cada origen por separado."""
    out = [g for g in report["groups"] if g["key"]["source"] == ALL and g["key"]["origin"] != ALL]
    if scope != ALL:
        out = [g for g in out if g["key"]["origin"] == scope]
    return out


def _metric(report: dict[str, Any], g: dict[str, Any], metric: str, raw_report: dict[str, Any] | None) -> tuple[Any, bool | None]:
    kpi = g.get("kpi", {})
    if metric == "completitud":
        v = kpi.get("completitud")
        return v, None if v is None else v >= 0.95
    if metric == "unicidad":
        v = kpi.get("unicidad")
        need = 1.0 if report["layer"] == "trusted" else 0.99
        return v, None if v is None else v >= need
    if metric == "duplicados":
        v = kpi.get("duplicados")
        return v, None if v is None else v < 0.01
    if metric == "anonimizacion":
        v = kpi.get("anonimizacion")
        identity = any(c in ("user_id", "handle") for var in report["files"].get("schema_variants", []) for c in var.get("extra", []))
        return v, None if v is None else (v == 1.0 and not identity)
    if metric == "schema":
        variants = report["files"].get("schema_variants", [])
        total = sum(v["files"] for v in variants)
        ok = sum(v["files"] for v in variants if v["matches_contract"])
        return (round(ok / total, 6) if total else None), (None if not total else ok == total)
    if metric == "nlp_single_version":
        v = (g["columns"].get("nlp_model_version") or {}).get("distinct")
        return v, None if v is None else v == 1
    if metric == "label_sentiment_null":
        v = (g["columns"].get("label_sentiment") or {}).get("null_ratio")
        return v, None if v is None else LABEL_NULL_RANGE[0] <= v <= LABEL_NULL_RANGE[1]
    if metric == "text_max_length":
        v = ((g["columns"].get("text") or {}).get("length") or {}).get("max")
        return v, None if v is None else v <= TEXT_MAX_LENGTH
    if metric == "latency_p95":
        v = ((g["derived"].get("latency_s") or {}).get("numeric") or {}).get("p95")
        return v, None if v is None else v < LATENCY_P95_MAX_S
    if metric == "source_share":
        origin = g["key"]["origin"]
        leaves = [x for x in report["groups"] if x["key"]["origin"] == origin and x["key"]["source"] != ALL]
        total = sum(x["rows"] for x in leaves)
        shares = {x["key"]["source"]: round(x["rows"] / total, 4) for x in leaves} if total else {}
        ok = bool(shares) and len(shares) == 3 and all(SOURCE_SHARE_RANGE[0] <= s <= SOURCE_SHARE_RANGE[1] for s in shares.values())
        return shares, (None if not shares else ok)
    if metric == "text_distinct_ratio":
        v = ((g.get("text") or {}).get("repeated") or {}).get("distinct_ratio")
        need = TEXT_DISTINCT_MIN.get(g["key"]["origin"], min(TEXT_DISTINCT_MIN.values()))
        return v, None if v is None else v >= need
    if metric == "nlp_agreement":
        v = (g.get("nlp_agreement") or {}).get("ratio")
        return v, None if v is None else v >= NLP_AGREEMENT_MIN
    if metric == "rows_vs_raw":
        if not raw_report:
            return None, None
        raw_distinct = raw_report.get("duplicates", {}).get("distinct_event_ids")
        trusted_all = next((x for x in report["groups"] if x["key"]["origin"] == ALL and x["key"]["source"] == ALL), None)
        rows = trusted_all["rows"] if trusted_all else None
        ok = None if rows is None or raw_distinct is None else (0.99 * raw_distinct <= rows <= raw_distinct)
        return {"trusted_rows": rows, "raw_distinct": raw_distinct}, ok
    return None, None


TABLE_LEVEL_METRICS = {"schema", "rows_vs_raw"}


def evaluate(report: dict[str, Any], raw_report: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Reglas de la capa del informe con el valor observado por origen y su estado.

    ``status``: ``cumple`` | ``no_cumple`` | ``sin_dato``. ``observed`` es la fracción de filas que
    cumplen (reglas por fila) o el valor de la métrica, por origen (``{"stream": …, "historical": …}``).
    """
    layer = report["layer"]
    out = []
    for rule in RULES:
        if layer not in rule.layers:
            continue
        observed: dict[str, Any] = {}
        verdicts: list[bool] = []
        if rule.check.startswith("metric:"):
            metric = rule.check.split(":", 1)[1]
            if metric in TABLE_LEVEL_METRICS:
                g_all = next((x for x in report["groups"] if x["key"]["origin"] == ALL and x["key"]["source"] == ALL), None)
                if g_all is not None:
                    v, ok = _metric(report, g_all, metric, raw_report)
                    observed[ALL] = v
                    if ok is not None:
                        verdicts.append(ok)
            else:
                for g in _groups_for_scope(report, rule.scope):
                    v, ok = _metric(report, g, metric, raw_report)
                    observed[g["key"]["origin"]] = v
                    if ok is not None:
                        verdicts.append(ok)
        else:
            for g in _groups_for_scope(report, rule.scope):
                c = g["checks"].get(rule.check)
                if not c or not c["evaluated"]:
                    continue
                ratio = c["pass_ratio"]
                observed[g["key"]["origin"]] = ratio
                verdicts.append(ratio >= (rule.mostly if rule.mostly is not None else 1.0))
        status = "sin_dato" if not verdicts else ("cumple" if all(verdicts) else "no_cumple")
        item = rule.public()
        item.update({"observed": observed, "status": status})
        out.append(item)
    return out


def dq_preview(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Filas de ``dq_results`` (§5.5) que el Lote 5.2 escribiría con los KPI de este *profiling*.

    No se escriben en ningún lado: es la forma propuesta (``suite = profiling_5_1``).
    """
    layer = report["layer"]
    table = report["table"]
    run_ts = report["generated_at"]
    run_id = f"profiling-{layer}-{run_ts.replace(':', '').replace('-', '')}"
    thresholds = {"completitud": 0.95, "unicidad": 1.0 if layer == "trusted" else 0.99, "duplicados": 0.01, "anonimizacion": 1.0}
    rows = []
    for g in _groups_for_scope(report, ALL):
        origin = g["key"]["origin"]
        for kpi, threshold in thresholds.items():
            v = g["kpi"].get(kpi)
            if v is None:
                continue
            passed = v < threshold if kpi == "duplicados" else v >= threshold
            rows.append({
                "run_id": run_id, "run_ts": run_ts, "layer": layer, "table_name": table, "suite": "profiling_5_1",
                "check_name": f"{kpi}[origin={origin}]", "kpi": kpi, "observed_value": v, "threshold": threshold,
                "passed": passed, "details": {"origin": origin, "rows": g["rows"]},
            })
    return rows
