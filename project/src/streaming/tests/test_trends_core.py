"""Lógica pura de las ventanas de tendencias (Lote 2.4, HU-023)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from streaming.trends_core import (
    EMOTION_LABELS,
    TREND_COLUMNS,
    TrendConfig,
    TrendFeature,
    WindowAggregate,
    add_feature,
    expected_windows,
    feature_from_row,
    format_ts,
    latest_versions,
    merge_accumulators,
    new_accumulator,
    row_problems,
    score_window,
    signed_sentiment,
    to_epoch_ms,
    trend_row,
    window_starts,
)

T0 = 1_790_000_000_000 - (1_790_000_000_000 % 60_000)  # ms, alineado al minuto
CFG = TrendConfig()


def _agg(start_ms: int, n: int, avg: float | None = 0.0, brand="postobon", source="reddit") -> WindowAggregate:
    sn = n if avg is not None else 0
    return WindowAggregate(brand, source, start_ms, start_ms + CFG.window_ms, n, 0, 0, n,
                           (avg or 0.0) * sn, sn, {label: 0 for label in EMOTION_LABELS})


# --------------------------------------------------------------------------- configuración y tiempo

def test_config_por_defecto_es_la_de_5_12():
    assert (CFG.window_seconds, CFG.period_seconds, CFG.allowed_lateness_seconds) == (300, 60, 1800)
    assert (CFG.baseline_windows, CFG.z_threshold) == (60, 3.0)


@pytest.mark.parametrize("kwargs", [
    {"window_seconds": 290}, {"period_seconds": 0}, {"baseline_windows": 1},
    {"z_threshold": 0}, {"allowed_lateness_seconds": -1}, {"min_posts": -1},
])
def test_config_invalida(kwargs):
    with pytest.raises(ValueError):
        TrendConfig(**kwargs)


@pytest.mark.parametrize("value,expected", [
    ("2026-10-22T19:05:00.123Z", 1792695900123),
    ("2026-10-22T19:05:00.123+00:00", 1792695900123),
    (datetime(2026, 10, 22, 19, 5, 0, 123000, tzinfo=timezone.utc), 1792695900123),
    (datetime(2026, 10, 22, 19, 5, 0, 123000), 1792695900123),
    (1792695900.123, 1792695900123),
    (1792695900123, 1792695900123),
    ("no", None), (None, None), ("", None), (True, None),
])
def test_to_epoch_ms(value, expected):
    assert to_epoch_ms(value) == expected


def test_format_ts_con_milisegundos_y_z():
    assert format_ts(1792695900123) == "2026-10-22T19:05:00.123Z"
    assert format_ts(1792695900000) == "2026-10-22T19:05:00.000Z"


def test_window_starts_igual_que_sliding_windows():
    event = T0 + 125_000  # 2 min 5 s después de T0
    starts = window_starts(event, CFG)
    assert len(starts) == 5
    assert starts == sorted(starts)
    assert all(s <= event < s + CFG.window_ms for s in starts)
    assert all(s % CFG.period_ms == 0 for s in starts)
    assert starts[-1] == T0 + 120_000


def test_evento_en_el_borde_del_minuto():
    starts = window_starts(T0, CFG)
    assert starts[-1] == T0 and starts[0] == T0 - 240_000


# --------------------------------------------------------------------------- filas de entrada

def _row(**over):
    row = {"event_id": "e1", "event_time": format_ts(T0 + 1000), "brand_primary": "postobon",
           "source": "reddit", "sentiment_label": "positive", "sentiment_score": 0.9,
           "emotion_label": "joy"}
    row.update(over)
    return row


def test_feature_de_una_fila_de_posts_enriched():
    feature, reason = feature_from_row(_row())
    assert reason is None
    assert feature == TrendFeature("postobon", "reddit", T0 + 1000, "positive", 0.9, "joy")
    assert feature.key == "postobon|reddit"


def test_feature_normaliza_etiquetas_crudas_y_descarta_scores_invalidos():
    feature, _ = feature_from_row(_row(sentiment_label="NEG", sentiment_score=1.5, emotion_label="others"))
    assert feature.sentiment_label == "negative"
    assert feature.sentiment_score is None
    assert feature.emotion_label == "neutral"


def test_feature_sin_nlp_sigue_contando():
    feature, _ = feature_from_row(_row(sentiment_label=None, sentiment_score=None, emotion_label=None))
    assert feature.sentiment_label is None and feature.emotion_label is None


@pytest.mark.parametrize("row,reason", [
    ("texto", "unrecognized"),
    ({"otra": 1}, "unrecognized"),
    (_row(event_time="mal"), "no_event_time"),
    (_row(brand_primary=None), "no_brand"),
    (_row(source=""), "no_source"),
])
def test_feature_motivos_de_descarte(row, reason):
    assert feature_from_row(row) == (None, reason)


def test_sin_brand_primary_usa_brands():
    row = _row()
    del row["brand_primary"]
    row["brands"] = ["pepsi", "postobon"]
    feature, _ = feature_from_row(row)
    assert feature.brand == "pepsi"


@pytest.mark.parametrize("label,score,expected", [
    ("positive", 0.8, 0.8), ("negative", 0.8, -0.8), ("neutral", 0.7, 0.0),
    ("neutral", None, 0.0), ("positive", None, None), (None, 0.5, None),
])
def test_sentimiento_con_signo(label, score, expected):
    assert signed_sentiment(label, score) == expected


# --------------------------------------------------------------------------- acumulador

def test_acumulador_y_merge():
    a = add_feature(new_accumulator(), TrendFeature("p", "r", T0, "positive", 0.8, "joy"))
    b = add_feature(new_accumulator(), TrendFeature("p", "r", T0, "negative", 0.4, "anger"))
    b = add_feature(b, TrendFeature("p", "r", T0, "neutral", 0.9, None))
    merged = merge_accumulators([a, b])
    agg = WindowAggregate.from_accumulator("p|r", T0, T0 + 300_000, merged)
    assert (agg.n_posts, agg.n_negative, agg.n_positive, agg.n_neutral) == (3, 1, 1, 1)
    assert agg.avg_sentiment == pytest.approx((0.8 - 0.4 + 0.0) / 3, abs=1e-6)
    assert agg.emotion_counts["joy"] == 1 and agg.emotion_counts["anger"] == 1
    assert sum(agg.emotion_counts.values()) == 2


def test_avg_nulo_sin_sentimiento():
    agg = WindowAggregate.from_accumulator("p|r", T0, T0 + 300_000,
                                           add_feature(new_accumulator(), (T0, None, None, "joy")))
    assert agg.avg_sentiment is None


def test_expected_windows_cuenta_cada_post_en_5_ventanas():
    features = [TrendFeature("postobon", "reddit", T0 + 1000, "positive", 1.0, "joy"),
                TrendFeature("postobon", "reddit", T0 + 61_000, "negative", 1.0, "anger"),
                TrendFeature("pepsi", "reddit", T0 + 1000, "neutral", 1.0, "neutral")]
    windows = expected_windows(features, CFG)
    assert (T0, "postobon", "reddit") in windows
    assert windows[(T0, "postobon", "reddit")].n_posts == 2
    assert windows[(T0 - 240_000, "postobon", "reddit")].n_posts == 1
    assert windows[(T0 + 60_000, "postobon", "reddit")].n_posts == 1
    assert len([k for k in windows if k[1] == "pepsi"]) == 5


# --------------------------------------------------------------------------- baseline y alertas

def _run(series, cfg=CFG, start=T0):
    state, scores = None, []
    for i, (n, avg) in enumerate(series):
        state, score = score_window(state, _agg(start + i * cfg.period_ms, n, avg), cfg)
        scores.append(score)
    return state, scores


def test_sin_baseline_no_hay_zscore_ni_alerta():
    _, scores = _run([(10, 0.1), (10, 0.1)])
    assert scores[0].zscore_volume is None and not scores[0].is_alert


def test_arranque_en_frio_no_alerta_antes_de_60_ventanas():
    # 5 ventanas iniciales incompletas + 59 de baseline + un pico enorme: aún no alerta.
    series = [(10, 0.1)] * 5 + [(10 + (i % 3), 0.1) for i in range(59)] + [(200, -0.9)]
    _, scores = _run(series)
    assert scores[-1].warm is False
    assert scores[-1].is_alert is False
    assert scores[-1].zscore_volume > 3


def test_alerta_por_volumen_con_baseline_completo():
    series = [(10, 0.1)] * 5 + [(10 + (i % 3), 0.1) for i in range(60)] + [(60, 0.1)]
    _, scores = _run(series)
    last = scores[-1]
    assert last.warm and last.baseline_available == 60
    assert last.zscore_volume > 3 and last.is_alert


def test_alerta_por_sentimiento():
    series = [(12, 0.2 + 0.01 * (i % 5)) for i in range(70)] + [(12, -0.8)]
    _, scores = _run(series)
    assert scores[-1].zscore_sentiment < -3 and scores[-1].is_alert


def test_ventana_incompleta_no_alerta():
    """Al drenar, las ventanas abiertas salen con datos parciales: la caída de volumen es falsa."""
    series = [(30, 0.1)] * 5 + [(30 + (i % 3), 0.1) for i in range(60)]
    state, _ = _run(series)
    start = T0 + len(series) * CFG.period_ms
    partial = _agg(start, 10, 0.1)
    _, score = score_window(state, WindowAggregate(**{**partial.__dict__, "complete": False}), CFG)
    assert score.warm and score.zscore_volume < -3 and score.is_alert is False
    _, score = score_window(state, partial, CFG)
    assert score.is_alert is True


def test_min_posts_evita_alertas_en_ventanas_chicas():
    cfg = TrendConfig(min_posts=50)
    series = [(10, 0.1)] * 5 + [(10 + (i % 3), 0.1) for i in range(60)] + [(40, 0.1)]
    _, scores = _run(series, cfg)
    assert scores[-1].zscore_volume > 3 and not scores[-1].is_alert


def test_ventanas_vacias_cuentan_como_volumen_cero():
    cfg = TrendConfig(baseline_windows=3)
    state, _ = score_window(None, _agg(T0, 10, 0.0), cfg)
    # Salto: no llegan ventanas en 10 minutos; la siguiente ve un baseline de ceros.
    later = T0 + 20 * cfg.period_ms
    state, score = score_window(state, _agg(later, 4, 0.0), cfg)
    assert score.baseline_available == 3
    assert score.zscore_volume == pytest.approx(4.0)  # (4 - 0) / piso 1.0


def test_panel_tardio_reemplaza_la_version_anterior():
    cfg = TrendConfig(baseline_windows=2)
    state, _ = score_window(None, _agg(T0, 5, 0.0), cfg)
    state, _ = score_window(state, _agg(T0, 9, 0.0), cfg)
    assert state["hist"][T0][0] == 9


def test_estado_se_poda():
    cfg = TrendConfig(baseline_windows=2, allowed_lateness_seconds=60)
    state = None
    for i in range(50):
        state, _ = score_window(state, _agg(T0 + i * 60_000, 5, 0.0), cfg)
    horizon = (2 * 60_000 + 60_000 + 300_000) // 60_000
    assert len(state["hist"]) <= horizon + 1


# --------------------------------------------------------------------------- fila de salida

def test_trend_row_tiene_las_columnas_del_contrato():
    _, score = score_window(None, _agg(T0, 3, 0.25), CFG)
    row = trend_row(_agg(T0, 3, 0.25), score)
    assert tuple(row) == TREND_COLUMNS
    assert row["window_start"] == format_ts(T0)
    assert row["window_end"] == format_ts(T0 + 300_000)
    assert json.loads(row["emotion_counts"]) == {label: 0 for label in EMOTION_LABELS}
    assert list(json.loads(row["emotion_counts"])) == list(EMOTION_LABELS)
    assert row_problems(row) == []


@pytest.mark.parametrize("change,fragment", [
    ({"window_end": format_ts(T0 + 240_000)}, "duración"),
    ({"window_start": format_ts(T0 + 1000), "window_end": format_ts(T0 + 301_000)}, "alineado"),
    ({"brand": "Postobón"}, "brand"),
    ({"source": "mastodon"}, "source"),
    ({"n_negative": 3, "n_positive": 3}, "n_negative + n_positive"),
    ({"avg_sentiment": 1.5}, "avg_sentiment"),
    ({"emotion_counts": "{}"}, "claves"),
    ({"emotion_counts": "[1]"}, "objeto JSON"),
    ({"is_alert": "no"}, "booleano"),
    ({"zscore_volume": float("nan")}, "zscore_volume"),
    ({"extra": 1}, "sobran"),
])
def test_row_problems_detecta(change, fragment):
    _, score = score_window(None, _agg(T0, 3, 0.25), CFG)
    row = dict(trend_row(_agg(T0, 3, 0.25), score))
    row.update(change)
    assert any(fragment in p for p in row_problems(row)), row_problems(row)


def test_latest_versions_toma_el_mayor_n_posts():
    _, score = score_window(None, _agg(T0, 3, 0.0), CFG)
    v1 = trend_row(_agg(T0, 3, 0.0), score)
    v2 = trend_row(_agg(T0, 5, 0.0), score)
    other = trend_row(_agg(T0, 1, 0.0, brand="pepsi"), score)
    latest = latest_versions([v2, v1, other])
    assert latest[(T0, "postobon", "reddit")]["n_posts"] == 5
    assert len(latest) == 2
