import pytest

from simulator.generator.brands import load_brands, normalize_text


def test_v1_trae_tres_marcas_canonicas():
    b = load_brands()
    assert b.keys == ["postobon", "pepsi", "sprite"]
    assert b["postobon"].role == "propia" and b["pepsi"].role == "competidor"
    assert b["postobon"].display == "Postobón"
    assert b["sprite"].instagram_account == "sprite.sim"


def test_normalize_text():
    assert normalize_text(" Postobón ") == "postobon"
    assert normalize_text("PEPSI") == "pepsi"


def test_canonical_acepta_clave_display_y_alias():
    b = load_brands()
    for nombre in ["postobon", "Postobón", "POSTOBON", "Postobon"]:
        assert b.canonical(nombre) == "postobon"
    assert b.canonical("Pepsi Cola") == "pepsi"
    with pytest.raises(ValueError):
        b.canonical("Manzana")


def test_detect_ignora_mayusculas_y_tildes_y_ordena_por_aparicion():
    b = load_brands()
    assert b.detect("POSTOBÓN me gusta") == ["postobon"]
    assert b.detect("prefiero sprite, pero Pepsi Cola y #postobon también") == ["sprite", "pepsi", "postobon"]
    assert b.detect("sin marcas, solo gaseosa") == []
    assert b.detect("#Pepsi_Cola") == ["pepsi"]
    assert b.detect("Pepsi y Pepsi y Pepsi") == ["pepsi"]


def test_detect_no_confunde_subcadenas():
    b = load_brands()
    assert b.detect("espritesco") == []     # 'sprite' dentro de otra palabra no cuenta
