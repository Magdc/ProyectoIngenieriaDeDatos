import pytest

from common.label_map import (
    EMOTION_LABELS, categoria_sentimiento, categorias_para, map_emotion, map_sentiment,
)


def test_sentimiento_del_modelo():
    assert map_sentiment("NEG") == "negative"
    assert map_sentiment("NEU") == "neutral"
    assert map_sentiment("POS") == "positive"
    with pytest.raises(ValueError):
        map_sentiment("negative")      # las etiquetas del contrato no son salida cruda


def test_emocion_siete_clases():
    for raw in ["joy", "sadness", "anger", "fear", "disgust", "surprise"]:
        assert map_emotion(raw) == raw
    assert map_emotion("others") == "neutral"
    assert map_emotion("OTHERS") == "neutral"
    assert set(EMOTION_LABELS) == {"joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral"}
    with pytest.raises(ValueError):
        map_emotion("amor")


def test_adr_13_categoria_a_sentimiento():
    assert categoria_sentimiento() == {
        "gusto": "positive", "asco": "negative", "informativo": "neutral",
        "nostalgia": None, "adiccion": None,
    }


def test_categorias_para_es_el_inverso():
    assert categorias_para("positive") == ["gusto"]
    assert categorias_para("negative") == ["asco"]
    assert categorias_para("neutral") == ["informativo"]
    with pytest.raises(ValueError):
        categorias_para("positivo")
