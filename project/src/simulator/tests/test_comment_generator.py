import os
import random
import re
import string
import subprocess
import sys
from pathlib import Path

import pytest

from common.envelope import event_rng
from common.hashtags import CANONICAL_RE, extract_hashtags
from simulator.generator import GENERATOR_ID, Comentario, generate, load_brands
from simulator.generator.noise import aplicar_ruido
from simulator.generator.templates import CATEGORIAS, ENVASES

BRANDS = load_brands()
SRC = str(Path(__file__).resolve().parents[2])
CAMPOS = {"m", "m2", "envase", "comida", "lugar", "momento", "persona", "variante", "precio",
          "clima", "evento", "adjpos", "adjneg"}
CAT_SENT = {"gusto": "positive", "asco": "negative", "informativo": "neutral",
            "nostalgia": None, "adiccion": None}


def _campos(plantilla):
    return {f for _, f, _, _ in string.Formatter().parse(plantilla) if f}


def _muestras(n=300, **kw):
    return [generate(event_rng(7, "test", i), **kw) for i in range(n)]


# ---------------------------------------------------------------- pureza y determinismo
def test_generator_id_cumple_el_formato_del_envelope():
    assert GENERATOR_ID == "comment-gen@1.0"


def test_misma_semilla_mismo_comentario():
    for i in range(200):
        assert generate(event_rng(7, "reddit", i)) == generate(event_rng(7, "reddit", i))


def test_no_depende_del_orden_de_generacion():
    solo = generate(event_rng(7, "reddit", 5), "pepsi", "negative")
    for i in (9, 1, 5, 3):
        generate(event_rng(7, "reddit", i))
    assert generate(event_rng(7, "reddit", 5), "pepsi", "negative") == solo


def test_semillas_distintas_dan_comentarios_distintos():
    assert len({generate(event_rng(s, "reddit", 1)).texto for s in range(30)}) > 20


def test_no_usa_el_random_global(monkeypatch):
    def prohibido(*a, **k):
        raise AssertionError("se usó el random global")
    for nombre in ("random", "choice", "choices", "sample", "randint", "randrange", "shuffle",
                   "seed", "uniform"):
        monkeypatch.setattr(random, nombre, prohibido)
    for i in range(200):
        generate(random.Random(i))


def test_identico_entre_procesos_con_distinto_hashseed():
    codigo = ("from common.envelope import event_rng;from simulator.generator import generate;"
              "print([generate(event_rng(7,'reddit',i)).texto for i in range(40)])")
    salidas = set()
    for hs in ("1", "2", "random"):
        env = {**os.environ, "PYTHONHASHSEED": hs, "PYTHONPATH": SRC, "PYTHONIOENCODING": "utf-8"}
        salidas.add(subprocess.run([sys.executable, "-c", codigo], env=env, capture_output=True,
                           text=True, encoding="utf-8", check=True).stdout)
    assert len(salidas) == 1


def test_devuelve_comentario_inmutable():
    c = generate(random.Random(1))
    assert isinstance(c, Comentario)
    assert set(vars(c)) == {"marca", "texto", "categoria", "sentimiento"}
    with pytest.raises(Exception):
        c.marca = "otra"


# ---------------------------------------------------------------- marca (cambios #2 y #4)
def test_marca_canonica_y_presente_en_el_texto():
    for marca in BRANDS.keys:
        for c in _muestras(400, marca=marca, ruido=1.0):
            assert c.marca == marca
            assert marca in BRANDS.detect(c.texto), c.texto


def test_marca_acepta_display_y_alias():
    for nombre in ("Postobón", "POSTOBON", "Pepsi Cola"):
        c = generate(random.Random(3), marca=nombre)
        assert c.marca == BRANDS.canonical(nombre)


def test_sin_marca_se_reparten_todas():
    assert {c.marca for c in _muestras(200)} == set(BRANDS.keys)


def test_marca_desconocida_falla():
    with pytest.raises(ValueError):
        generate(random.Random(1), marca="manzana")


# ---------------------------------------------------------------- sentimiento / categoría (ADR-13)
def test_sentimiento_restringe_la_categoria():
    esperado = {"positive": {"gusto"}, "negative": {"asco"}, "neutral": {"informativo"}}
    for s, cats in esperado.items():
        muestras = _muestras(150, sentimiento=s)
        assert {c.categoria for c in muestras} == cats
        assert {c.sentimiento for c in muestras} == {s}


def test_mapeo_categoria_sentimiento_sin_filtros():
    muestras = _muestras(1500)
    assert {c.categoria for c in muestras} == set(CAT_SENT)
    for c in muestras:
        assert c.sentimiento == CAT_SENT[c.categoria]
    assert any(c.sentimiento is None for c in muestras)


def test_categoria_fija_y_coherencia():
    assert {c.categoria for c in _muestras(60, categoria="nostalgia")} == {"nostalgia"}
    assert generate(random.Random(1), sentimiento="positive", categoria="gusto").categoria == "gusto"
    with pytest.raises(ValueError):
        generate(random.Random(1), sentimiento="positive", categoria="asco")
    with pytest.raises(ValueError):
        generate(random.Random(1), categoria="inexistente")
    with pytest.raises(ValueError):
        generate(random.Random(1), sentimiento="positivo")


# ---------------------------------------------------------------- plantillas (cambio #5)
def test_minimo_de_plantillas_informativas_y_comparativas():
    inf = CATEGORIAS["informativo"]
    assert len(inf["nucleos"]) >= 15
    assert len(inf["comparativos"]) >= 15
    for p in inf["comparativos"]:
        assert "m2" in _campos(p) and _campos(p) & {"m", "envase"}, p


def test_todas_las_plantillas_usan_huecos_conocidos_y_nombran_la_marca():
    for cat, d in CATEGORIAS.items():
        for lista in ("nucleos", "comparativos", "contextos", "cierres", "reacciones"):
            for p in d[lista]:
                assert _campos(p) <= CAMPOS, (cat, p)
        for p in d["nucleos"] + d["comparativos"]:
            assert _campos(p) & {"m", "envase"}, f"{cat}: la plantilla no nombra la marca: {p}"
        for p in d["gen_compl"]:
            assert _campos(p) <= CAMPOS, (cat, p)
    for p in ENVASES:
        assert "m" in _campos(p), p


def test_comparativas_mencionan_dos_marcas_distintas():
    muestras = _muestras(400, sentimiento="neutral")
    dos = [c for c in muestras if len(BRANDS.detect(c.texto)) == 2]
    assert len(dos) >= 40
    for c in dos:
        assert c.marca in BRANDS.detect(c.texto)


def test_las_demas_categorias_nunca_mencionan_una_segunda_marca():
    for c in _muestras(600, sentimiento="positive"):
        assert BRANDS.detect(c.texto) == [c.marca]


def test_hay_variedad_lexica():
    textos = [c.texto for c in _muestras(3000)]
    assert len(set(textos)) / len(textos) > 0.97


# ---------------------------------------------------------------- hashtags y ruido (cambios #4 y #6)
def test_hashtags_del_texto_son_canonicos():
    todos = []
    for c in _muestras(800, ruido=0.5):
        tags = extract_hashtags(c.texto)
        assert len(tags) == len(set(tags))
        assert all(CANONICAL_RE.match(t) for t in tags), c.texto
        todos += tags
    assert {"colombia"} <= set(todos)


def test_el_ruido_nunca_dana_la_marca_ni_siquiera_sin_la_red_de_seguridad():
    """El enmascarado protege los alias; no depende del chequeo final de generate()."""
    datos = CATEGORIAS["gusto"]
    for marca, alias in (("postobon", "Postobón"), ("pepsi", "Pepsi"), ("sprite", "Sprite")):
        for i in range(1500):
            rng = random.Random(f"{marca}|{i}")
            base = f"Qué buena está la {alias} de lata, la {alias} es lo mejor. #{alias}"
            ruidoso = aplicar_ruido(base, datos, rng, protegidos=[alias])
            assert marca in BRANDS.detect(ruidoso), (base, ruidoso)


def test_el_ruido_se_aplica_y_se_puede_apagar():
    sin = [generate(event_rng(7, "r", i), ruido=0).texto for i in range(300)]
    con = [generate(event_rng(7, "r", i), ruido=1.0).texto for i in range(300)]
    assert sin != con
    assert not any(t.isupper() for t in sin)
    assert any(t.islower() or t.isupper() for t in con)


def test_texto_nunca_vacio_y_empieza_con_mayuscula_sin_ruido():
    for c in _muestras(500, ruido=0):
        assert c.texto.strip() and not c.texto[0].islower()
