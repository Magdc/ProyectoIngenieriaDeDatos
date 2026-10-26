import io
import json
from datetime import datetime, timezone

import pytest

from exporter import identity
from exporter.identity import (
    IdentityError,
    assume_role,
    aws_condition_values,
    check_claims,
    compare_with_trust,
    decode_claims,
    fetch_gcloud_token,
    fetch_metadata_token,
    get_token,
    session_name,
    trust_conditions,
)
from exporter.testing import FakeSts, make_jwt, sa_claims, trust_policy

UID = "112233445566778899001"
EMAIL = "sa-exporter@p.iam.gserviceaccount.com"
AUD = "ta-gcp-exporter-role"


def test_decode_sin_verificar_y_valores_que_compara_aws():
    claims = sa_claims(UID, EMAIL, AUD)
    assert decode_claims(make_jwt(claims))["sub"] == UID
    values = aws_condition_values(claims)
    # Con azp presente AWS usa azp como accounts.google.com:aud y el aud del token como oaud.
    assert values == {"accounts.google.com:aud": UID, "accounts.google.com:oaud": AUD, "accounts.google.com:sub": UID}
    no_azp = sa_claims(UID, EMAIL, AUD, azp=None)
    assert aws_condition_values(no_azp)["accounts.google.com:aud"] == AUD
    with pytest.raises(IdentityError):
        decode_claims("no-es-un-jwt")


@pytest.mark.parametrize("azp", ["same", EMAIL, None])
def test_la_confianza_del_lote_acepta_azp_con_id_correo_o_ausente(azp):
    claims = sa_claims(UID, EMAIL, AUD, azp=azp)
    assert compare_with_trust(claims, trust_policy(UID, EMAIL, AUD)) == []


def test_la_confianza_rechaza_otra_cuenta_otra_audiencia_y_sin_condiciones():
    trust = trust_policy(UID, EMAIL, AUD)
    other = sa_claims("999", "otra@p.iam.gserviceaccount.com", AUD, azp="same")
    problems = compare_with_trust(other, trust)
    assert any("accounts.google.com:sub" in p for p in problems)
    assert any("accounts.google.com:aud" in p for p in problems)
    wrong_aud = sa_claims(UID, EMAIL, "sts.amazonaws.com")
    assert any("oaud" in p for p in compare_with_trust(wrong_aud, trust))
    open_trust = {"Statement": [{"Effect": "Allow", "Principal": {"Federated": "accounts.google.com"},
                                 "Action": "sts:AssumeRoleWithWebIdentity"}]}
    assert len(compare_with_trust(sa_claims(UID, EMAIL, AUD), open_trust)) == 3


def test_trust_desde_texto_url_codificado():
    doc = json.dumps(trust_policy(UID, EMAIL, AUD))
    from urllib.parse import quote

    parsed = trust_conditions(quote(doc))
    assert parsed["federated"] == {"accounts.google.com"}
    assert parsed["conditions"]["accounts.google.com:sub"]["values"] == {UID}


def test_check_claims():
    good = sa_claims(UID, EMAIL, AUD)
    assert check_claims(good, audience=AUD, unique_id=UID, email=EMAIL) == []
    bad = dict(good, iss="https://otro", aud="x", sub="1", azp="2", email="e@x")
    problems = " | ".join(check_claims(bad, audience=AUD, unique_id=UID, email=EMAIL))
    for word in ("iss", "aud", "sub", "azp", "email"):
        assert word in problems
    expired = sa_claims(UID, EMAIL, AUD, minutes=-5)
    assert "expiró" in " ".join(check_claims(expired, audience=AUD))


def test_metadata_server():
    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    seen = {}

    def opener(req, timeout):
        seen["url"] = req.full_url
        seen["flavor"] = req.headers.get("Metadata-flavor")
        return Resp(make_jwt(sa_claims()).encode())

    token = fetch_metadata_token(AUD, opener=opener)
    assert token.count(".") == 2
    assert "audience=ta-gcp-exporter-role" in seen["url"] and "format=full" in seen["url"]
    assert seen["flavor"] == "Google"
    with pytest.raises(IdentityError):
        fetch_metadata_token(AUD, opener=lambda req, timeout: Resp(b"<html>"))


def test_metadata_server_sin_format_full():
    """Si el servidor rechaza format=full, se repite sin él (sin el claim email)."""
    import urllib.error

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    urls = []

    def opener(req, timeout):
        urls.append(req.full_url)
        if "format=full" in req.full_url:
            raise urllib.error.HTTPError(req.full_url, 400, "bad format", {}, None)
        return Resp(make_jwt(sa_claims()).encode())

    assert fetch_metadata_token(AUD, opener=opener).count(".") == 2
    assert len(urls) == 2 and "format" not in urls[1]


def test_gcloud_respaldo():
    class P:
        returncode = 0
        stdout = "WARNING: algo\n" + make_jwt(sa_claims()) + "\n"
        stderr = ""

    calls = []

    def run(cmd, **kw):
        calls.append((cmd, kw))
        return P()

    assert fetch_gcloud_token(EMAIL, AUD, run=run).count(".") == 2
    cmd, kw = calls[0]
    assert f"--impersonate-service-account={EMAIL}" in cmd and f"--audiences={AUD}" in cmd and "--include-email" in cmd
    assert kw["stdin"] is not None  # sin entrada estándar: falla en vez de esperar


def test_get_token_orden_y_errores(monkeypatch):
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.setattr(identity, "fetch_impersonated_token", lambda sa, aud: (_ for _ in ()).throw(IdentityError("403 sin permiso")))
    monkeypatch.setattr(identity, "fetch_gcloud_token", lambda sa, aud: "a.b.c")
    assert get_token("auto", AUD, EMAIL) == ("a.b.c", "gcloud")
    monkeypatch.setenv("CLOUD_RUN_JOB", "ta-gcs-to-s3")
    monkeypatch.setattr(identity, "fetch_metadata_token", lambda aud: "m.m.m")
    assert get_token("auto", AUD, EMAIL) == ("m.m.m", "metadata")
    with pytest.raises(IdentityError, match="impersonate"):
        get_token("impersonate", AUD, EMAIL)
    with pytest.raises(IdentityError, match="correo"):
        get_token("gcloud", AUD, None)


def test_assume_role_sin_credenciales_de_aws():
    sts = FakeSts()
    assumed = assume_role("a.b.c", sts.role_arn, run_id="20261024T120000Z-abc", sts_client=sts, duration=1800)
    call = sts.calls[0]
    assert call["WebIdentityToken"] == "a.b.c" and call["DurationSeconds"] == 1800
    assert call["RoleSessionName"].startswith("ta-gcs-to-s3-")
    assert ":assumed-role/ta-gcp-exporter-role/" in assumed.assumed_arn
    assert "secret" not in json.dumps(assumed.public()).lower()
    with pytest.raises(IdentityError, match="AccessDenied"):
        assume_role("a.b.c", sts.role_arn, run_id="r", sts_client=FakeSts(fail="AccessDenied"))


def test_session_name_valido():
    name = session_name("2026/10/24 run:x" * 10)
    assert len(name) <= 64 and all(c.isalnum() or c in "+=,.@-_" for c in name)


def test_cliente_sts_sin_firma():
    botocore = pytest.importorskip("botocore")
    client = identity.unsigned_sts_client("us-east-1")
    assert client.meta.config.signature_version is botocore.UNSIGNED
