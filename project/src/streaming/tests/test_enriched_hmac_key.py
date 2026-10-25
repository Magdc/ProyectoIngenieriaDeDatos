"""Llave HMAC: Secret Manager por REST, variable de entorno y llave local (Lote 2.2)."""

from __future__ import annotations

import base64

import pytest

from common import hmac_key as hk


class _Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}

    def json(self):
        return self._body


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, timeout=None):
        self.urls.append(url)
        return self.responses.pop(0)


def _ok(text: str, crc=None):
    payload = {"data": base64.b64encode(text.encode()).decode()}
    if crc is not None:
        payload["dataCrc32c"] = str(crc)
    return _Resp(200, {"payload": payload})


def test_lee_secret_manager_por_rest():
    s = _Session([_ok("a" * 64 + "\n")])
    key = hk.load_hmac_key(hk.KeySource("secret", project="p1", secret_name="ta-hmac-key"), session=s)
    assert key == "a" * 64
    assert s.urls == ["https://secretmanager.googleapis.com/v1/projects/p1/secrets/ta-hmac-key/versions/latest:access"]


def test_verifica_crc32c():
    google_crc32c = pytest.importorskip("google_crc32c")
    good = google_crc32c.value(b"b" * 32)
    assert hk.access_secret("p", "s", session=_Session([_ok("b" * 32, good)])) == b"b" * 32
    with pytest.raises(hk.HmacKeyError):
        hk.access_secret("p", "s", session=_Session([_ok("b" * 32, good + 1)]))


def test_reintenta_5xx_y_explica_403_404():
    slept = []
    s = _Session([_Resp(503), _ok("c" * 32)])
    assert hk.access_secret("p", "s", session=s, sleep=slept.append) == b"c" * 32
    assert slept == [1]
    with pytest.raises(hk.HmacKeyError, match="secretAccessor"):
        hk.access_secret("p", "s", session=_Session([_Resp(403)]))
    with pytest.raises(hk.HmacKeyError, match="hmac_key.sh init"):
        hk.access_secret("p", "s", session=_Session([_Resp(404)]))


def test_llave_corta_se_rechaza():
    with pytest.raises(hk.HmacKeyError):
        hk.load_hmac_key(hk.KeySource("secret", project="p"), session=_Session([_ok("corta")]))


def test_variable_de_entorno_y_llave_local():
    src = hk.KeySource("env", env_var="TA_TEST_KEY")
    assert hk.load_hmac_key(src, environ={"TA_TEST_KEY": "d" * 20}) == "d" * 20
    with pytest.raises(hk.HmacKeyError):
        hk.load_hmac_key(src, environ={})
    assert hk.load_hmac_key(hk.KeySource("local_dev")) == hk.LOCAL_DEV_KEY
    assert len(hk.LOCAL_DEV_KEY) >= hk.MIN_KEY_LENGTH


def test_keysource_validacion_y_descripcion_sin_valor():
    with pytest.raises(ValueError):
        hk.KeySource("secret")
    with pytest.raises(ValueError):
        hk.KeySource("env")
    with pytest.raises(ValueError):
        hk.KeySource("otro")
    assert "ta-hmac-key" in hk.KeySource("secret", project="p").describe()
