import hashlib
import hmac

import pytest

from common.hashing import is_user_hash, user_hash


def test_es_hmac_sha256_hex():
    esperado = hmac.new(b"llave", b"u_000481", hashlib.sha256).hexdigest()
    assert user_hash("u_000481", "llave") == esperado
    assert user_hash("u_000481", b"llave") == esperado
    assert is_user_hash(esperado)


def test_determinista_y_sensible_a_entrada_y_llave():
    a = user_hash("u_1", "k")
    assert a == user_hash("u_1", "k")
    assert a != user_hash("u_2", "k")
    assert a != user_hash("u_1", "otra")


def test_no_contiene_el_identificador():
    assert "u_000481" not in user_hash("u_000481", "k")


def test_entradas_vacias_se_rechazan():
    with pytest.raises(ValueError):
        user_hash("", "k")
    with pytest.raises(ValueError):
        user_hash("u_1", "")


def test_is_user_hash():
    assert not is_user_hash("abc")
    assert not is_user_hash("A" * 64)      # mayúsculas: no es la forma del contrato
    assert not is_user_hash(None)
    assert is_user_hash("a" * 64)
