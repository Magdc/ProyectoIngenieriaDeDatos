"""Precisión del NLP (HU-033): ``sentiment_label`` (RoBERTuito) frente a ``label_sentiment`` (etiqueta del generador).

Sobre ``ta_curated.fact_posts``: solo las filas con las dos etiquetas (``label_sentiment`` nulo = ``nostalgia`` o
``adiccion``, ADR-13; sin NLP = las filas procesadas antes de ``NLP_SINCE``). ``label_sentiment`` solo se usa para
evaluar. De la emoción no hay verdad de referencia: solo su distribución. Sin dependencias de Great Expectations.
"""

from __future__ import annotations

import pandas as pd

CLASSES = ("negative", "neutral", "positive")
EMOTIONS = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
LIMITATION = ("Las métricas son optimistas: los comentarios salen de plantillas del generador (§3.2) y los modelos se "
              "entrenaron con tuits en español; `label_sentiment` es la etiqueta de la categoría de la plantilla, no una "
              "anotación humana. No hay verdad de referencia para la emoción.")


def precision_report(fact: pd.DataFrame) -> dict:
    """Matriz de confusión (filas = ``label_sentiment``, columnas = ``sentiment_label``), accuracy y métricas por clase."""
    n = len(fact)
    has_label = fact["label_sentiment"].isin(CLASSES)
    has_pred = fact["sentiment_label"].isin(CLASSES)
    ev = fact[has_label & has_pred]
    matrix = {t: {p: int(((ev["label_sentiment"] == t) & (ev["sentiment_label"] == p)).sum()) for p in CLASSES} for t in CLASSES}
    total = len(ev)
    correct = sum(matrix[c][c] for c in CLASSES)
    per_class = {}
    for c in CLASSES:
        tp = matrix[c][c]
        pred = sum(matrix[t][c] for t in CLASSES)
        true = sum(matrix[c].values())
        prec = tp / pred if pred else None
        rec = tp / true if true else None
        f1 = 2 * prec * rec / (prec + rec) if prec and rec else (0.0 if prec is not None and rec is not None else None)
        per_class[c] = {"precision": prec, "recall": rec, "f1": f1, "support": true}
    f1s = [v["f1"] for v in per_class.values() if v["f1"] is not None]
    by_origin = {}
    if "origin" in ev:
        for origin, g in ev.groupby("origin"):
            by_origin[str(origin)] = {"rows": int(len(g)), "accuracy": float((g["label_sentiment"] == g["sentiment_label"]).mean())}
    emo = fact["emotion_label"].dropna()
    emotions = {e: int((emo == e).sum()) for e in EMOTIONS}
    return {
        "rows": n,
        "evaluated": total,
        "without_label": int((~has_label).sum()),
        "without_prediction": int((has_label & ~has_pred).sum()),
        "accuracy": correct / total if total else None,
        "macro_f1": sum(f1s) / len(f1s) if f1s else None,
        "confusion": matrix,
        "per_class": per_class,
        "by_origin": by_origin,
        "emotion_distribution": {"rows": int(len(emo)), "counts": emotions,
                                 "shares": {e: (c / len(emo) if len(emo) else None) for e, c in emotions.items()}},
        "limitation": LIMITATION,
    }


def _pct(v) -> str:
    return "—" if v is None else f"{100 * v:.1f} %".replace(".", ",")


def _n(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def render_md(rep: dict, *, run_id: str, as_of: str) -> str:
    """El informe en Markdown (insumo del informe del curso; solo agregados; números con formato es-CO)."""
    lines = [f"# Precisión del NLP (HU-033) · corrida `{run_id}`", "",
             f"Datos: `ta_curated.fact_posts` ({_n(rep['rows'])} filas) al {as_of}. Evaluadas: **{_n(rep['evaluated'])}** filas "
             f"con `label_sentiment` y `sentiment_label`; sin etiqueta de referencia {_n(rep['without_label'])} (`nostalgia` y "
             f"`adiccion`, ADR-13); con etiqueta pero sin predicción {_n(rep['without_prediction'])} (filas sin NLP).", "",
             f"**Accuracy: {_pct(rep['accuracy'])}** · F1 macro: {_pct(rep['macro_f1'])}", "",
             "## Matriz de confusión (filas = `label_sentiment`, columnas = `sentiment_label`)", "",
             "| | " + " | ".join(CLASSES) + " |", "|---|" + "---|" * len(CLASSES)]
    for t in CLASSES:
        lines.append(f"| **{t}** | " + " | ".join(_n(rep["confusion"][t][p]) for p in CLASSES) + " |")
    lines += ["", "## Por clase", "", "| Clase | Precisión | Recall | F1 | Soporte |", "|---|---|---|---|---|"]
    for c in CLASSES:
        m = rep["per_class"][c]
        lines.append(f"| {c} | {_pct(m['precision'])} | {_pct(m['recall'])} | {_pct(m['f1'])} | {_n(m['support'])} |")
    if rep["by_origin"]:
        lines += ["", "## Por origen", "", "| Origen | Filas evaluadas | Accuracy |", "|---|---|---|"]
        for o, v in sorted(rep["by_origin"].items()):
            lines.append(f"| {o} | {_n(v['rows'])} | {_pct(v['accuracy'])} |")
    emo = rep["emotion_distribution"]
    lines += ["", f"## Emoción (sin verdad de referencia: solo distribución, {_n(emo['rows'])} filas)", "",
              "| Emoción | Filas | % |", "|---|---|---|"]
    for e in EMOTIONS:
        lines.append(f"| {e} | {_n(emo['counts'][e])} | {_pct(emo['shares'][e])} |")
    lines += ["", "## Limitación", "", rep["limitation"], ""]
    return "\n".join(lines)
