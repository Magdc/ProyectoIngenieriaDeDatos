import importlib.util
import re
from pathlib import Path

from common.hashtags import CANONICAL_RE, extract_hashtags, normalize_hashtag


def test_normalize_forma_canonica():
    assert normalize_hashtag("#Postobón") == "postobon"
    assert normalize_hashtag("Año_Nuevo") == "ano_nuevo"
    assert normalize_hashtag("#ÑANDÚ") == "nandu"
    assert normalize_hashtag("#Coca-Cola") == "cocacola"
    assert normalize_hashtag("#10/10") == "1010"


def test_normalize_sin_contenido_valido():
    assert normalize_hashtag("#") is None
    assert normalize_hashtag("###") is None
    assert normalize_hashtag("#😋") is None


def test_normalize_trunca_a_100():
    h = normalize_hashtag("#" + "a" * 150)
    assert h == "a" * 100 and CANONICAL_RE.match(h)


def test_extract_orden_sin_repetidos_y_canonico():
    texto = "Hola #Postobón y #postobon otra vez #Colombia #Año #Colombia"
    assert extract_hashtags(texto) == ["postobon", "colombia", "ano"]


def test_extract_sin_hashtags():
    assert extract_hashtags("sin etiquetas, solo texto. 10/10") == []
    assert extract_hashtags("# solo almohadilla") == []


def test_extract_cierre_tipico_del_generador():
    assert extract_hashtags("Qué rico. #Pepsi Cola #Colombia") == ["pepsi", "colombia"]


def test_coincide_con_la_referencia_de_contracts():
    """Si contracts/tests/test_contracts.py trae su normalize_hashtag(), deben coincidir."""
    ref_path = Path(__file__).resolve().parents[3] / "contracts" / "tests" / "test_contracts.py"
    if not ref_path.exists():
        import pytest
        pytest.skip("contracts/tests/test_contracts.py no está en el repositorio")
    try:
        spec = importlib.util.spec_from_file_location("_contracts_ref", ref_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        ref = mod.normalize_hashtag
    except Exception as exc:  # dependencias ausentes, nombre distinto...
        import pytest
        pytest.skip(f"no se pudo cargar la referencia: {exc!r}")
    for s in ["#Postobón", "Pepsi", "#Colombia", "#Año_Nuevo", "#Refrescante", "#TBT", "#Sprite_Zero"]:
        assert ref(s) == normalize_hashtag(s), s
