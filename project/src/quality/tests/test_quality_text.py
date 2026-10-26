"""Diversidad léxica y texto repetido (``quality.text_stats``)."""

from __future__ import annotations

import math

from quality.layers import load_brand_aliases
from quality.text_stats import Skeletonizer, TextDiversity, normalize, tokens


def test_tokens_sin_tildes_ni_numeros_ni_emojis():
    assert tokens("¡Qué rico Postobón! 🥤 2 veces #Colombia") == ["que", "rico", "postobon", "veces", "colombia"]
    assert normalize("ÁÉÍÓÚ ñ") == "aeiou n"


def test_esqueleto_enmascara_marcas_hashtags_y_signos():
    sk = Skeletonizer(load_brand_aliases())
    a = sk("Me encanta Postobón cuando está bien fría. #Postobon")
    b = sk("me encanta PEPSI COLA cuando esta bien fria!!")
    assert a == b == "me encanta <m> cuando esta bien fria"


def test_metricas_con_valores_conocidos():
    td = TextDiversity(window=3)
    for t in ["a b c", "a b c", "c d", ""]:
        td.add(t)
    td.add(None)  # los nulos no cuentan como texto
    r = td.result()
    assert r["texts"] == 4 and r["empty"] == 1
    assert r["tokens"] == 8 and r["types"] == 4
    assert r["ttr"] == round(4 / 8, 5)
    assert r["herdan_c"] == round(math.log(4) / math.log(8), 4)
    assert r["guiraud_r"] == round(4 / math.sqrt(8), 3)
    # secuencia a b c a b c c d → ventanas de 3: abc bca cab abc bcc ccd → 3,3,3,3,2,2
    assert r["mattr"] == round((3 + 3 + 3 + 3 + 2 + 2) / 3 / 6, 4)
    # bigramas: (a,b) (b,c) (a,b) (b,c) (c,d) → 3 distintos de 5
    assert r["distinct_2"] == round(3 / 5, 4)
    assert r["hapax_ratio"] == round(1 / 4, 4)  # solo "d"
    rep = r["repeated"]
    assert rep["distinct_texts"] == 3 and rep["texts_repeated"] == 1 and rep["rows_in_repeated"] == 2
    assert rep["max_repetition"] == 2
    assert rep["frequency_histogram"]["2"] == 1 and rep["frequency_histogram"]["1"] == 2
    assert rep["top"][0] == {"preview": "a b c", "count": 2}
    assert r["top_tokens"][0]["count"] == 3


def test_vista_previa_corta_y_sin_texto_completo():
    td = TextDiversity()
    long = "palabra " * 40
    td.add(long)
    td.add(long)
    top = td.result()["repeated"]["top"][0]
    assert len(top["preview"]) == 60 and top["count"] == 2


def test_mattr_sin_ventana_completa_usa_ttr():
    td = TextDiversity(window=500)
    td.add("uno dos dos")
    assert td.result()["mattr"] == round(2 / 3, 4)
