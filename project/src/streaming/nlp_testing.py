"""Dobles de prueba del NLP (Lote 2.3): clasificadores falsos sin torch ni modelos."""

from __future__ import annotations

from streaming.nlp import Classifier

FAKE_REVISIONS = {
    "sentiment": "0123456789abcdef0123456789abcdef01234567",
    "emotion": "fedcba9876543210fedcba9876543210fedcba98",
}


def _sentiment(texts):
    out = []
    for t in texts:
        low = t.lower()
        if "encanta" in low or "fan" in low:
            out.append({"label": "POS", "score": 0.93})
        elif "asco" in low or "odio" in low:
            out.append({"label": "NEG", "score": 0.88})
        else:
            out.append({"label": "NEU", "score": 0.61})
    return out


def _emotion(texts):
    return [{"label": "joy" if "encanta" in t.lower() else "others", "score": 0.77} for t in texts]


def fake_loader(directory: str, task: str) -> Classifier:
    fn = _sentiment if task == "sentiment" else _emotion
    return Classifier(fn, FAKE_REVISIONS[task], task)


def failing_loader(directory: str, task: str) -> Classifier:
    def boom(texts):
        raise RuntimeError("modelo caído")

    return Classifier(boom if task == "sentiment" else _emotion, FAKE_REVISIONS[task], task)


def identity(text: str) -> str:
    return text
