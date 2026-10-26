"""Adaptadores: variantes razonables de las firmas documentadas (§3.2.2, §3.10, §3.14).

Se prueban con módulos falsos instalados con los nombres reales; las pruebas contra el
código real están en ``test_hist_repo.py``.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import hashlib
import hmac
import json
import os

import pytest

from historical import adapters
from historical.adapters import AdapterError, call_flexible, extract_envelope
from historical.testing import FakeEnvelopes, fake_event_id

UTC = dt.timezone.utc


# ------------------------------------------------------------------ call_flexible

def test_call_flexible_por_nombre_y_posicion():
    def f(envelope, key, *, process_time=None, extra=3):
        return envelope, key, process_time, extra

    assert call_flexible(f, positional=[1], by_name={"key": 2, "process_time": 9}) == (1, 2, 9, 3)
    with pytest.raises(AdapterError, match="key"):
        call_flexible(f, positional=[1])


def test_call_flexible_only_if_required_y_lazy():
    calls = []

    def g(path="defecto"):
        return path

    lazy = adapters.Lazy(lambda: calls.append(1) or "calc")
    assert call_flexible(g, by_name={"path": lazy}, only_if_required=("path",)) == "defecto"
    assert calls == []

    def h(brands):
        return brands

    assert call_flexible(h, by_name={"brands": lazy}) == "calc" and calls == [1]


@pytest.mark.parametrize("make", [
    lambda env: env,
    lambda env: type("E", (), {"envelope": env})(),
    lambda env: type("E", (), {"messages": [(json.dumps(env).encode(), {"a": "b"})]})(),
    lambda env: type("E", (), {"messages": [type("M", (), {"data": json.dumps(env).encode()})()]})(),
    lambda env: {"messages": [{"data": json.dumps(env)}]},
    lambda env: (env, [b"x"]),
])
def test_extract_envelope_variantes(make):
    env = {"event_id": "abc", "text": "hola"}
    assert extract_envelope(make(env)) == env


def test_extract_envelope_desconocido():
    with pytest.raises(AdapterError, match="envelope"):
        extract_envelope(object())


# --------------------------------------------------------------------- simulador

class _Cfg:
    def __init__(self, seed, chaos=False):
        self.seed = seed
        self.chaos = type("C", (), {"enabled": chaos})()


def _sim_modules(mkmod, *, use_seed=True, builder=None, epoch=False, needs_users=False,
                 origin_param=False):
    def load_config(path=None):
        return _Cfg(int(os.environ.get("SIM_SEED", "7")),
                    os.environ.get("SIM_CHAOS_ENABLED") == "true")

    @dataclasses.dataclass
    class Emission:
        envelope: dict
        messages: list

    class EventFactory:
        def __init__(self, config, origin="stream"):
            self.seed = config.seed if use_seed else 7
            self.origin = origin
            self.inner = FakeEnvelopes(self.seed)

        def make(self, source: str, seq: int, now: dt.datetime, scenario=None):
            env = self.inner.envelope(source, seq, now)
            env["origin"] = self.origin
            env["event_id"] = fake_event_id(self.origin, source, self.seed, seq)
            return Emission(env, [(json.dumps(env).encode(), {})])

    class EpochFactory(EventFactory):
        def make(self, source: str, seq: int, now: float, scenario=None):
            assert isinstance(now, float)
            return super().make(source, seq, dt.datetime.fromtimestamp(now, UTC), scenario)

    class UsersFactory(EventFactory):
        def __init__(self, config, users):
            super().__init__(config)

    class StreamOnly(EventFactory):
        def __init__(self, config):
            super().__init__(config)

    cls = (EpochFactory if epoch else UsersFactory if needs_users
           else EventFactory if origin_param else StreamOnly)
    factory = mkmod("simulator.factory", EventFactory=cls, Emission=Emission)
    if builder:
        factory.build_factory = lambda cfg: cls(cfg)
    return {
        "simulator.sim_config": mkmod("simulator.sim_config", load_config=load_config),
        "simulator.factory": factory,
        "common.envelope": mkmod("common.envelope", event_id=fake_event_id),
    }


def test_simulador_estandar(install_modules, mkmod):
    install_modules(_sim_modules(mkmod))
    envs = adapters.SimulatorEnvelopes.build(3101)
    assert envs.native_origin == "stream"
    now = dt.datetime(2026, 5, 1, 12, tzinfo=UTC)
    env = envs.envelope("facebook", 10, now)
    assert env["origin"] == "historical"
    assert env["event_id"] == fake_event_id("historical", "facebook", 3101, 10)
    assert env["seq"] == 10 and env["sim"]["chaos"] == []


def test_simulador_con_builder_y_epoch(install_modules, mkmod):
    install_modules(_sim_modules(mkmod, builder=True, epoch=True))
    envs = adapters.SimulatorEnvelopes.build(42)
    assert envs.now_as_epoch is True
    assert envs.how == "simulator.factory.build_factory"


def test_simulador_con_origin_nativo(install_modules, mkmod):
    install_modules(_sim_modules(mkmod, origin_param=True))
    envs = adapters.SimulatorEnvelopes.build(5)
    assert envs.native_origin == "historical"


def test_simulador_que_ignora_la_semilla(install_modules, mkmod):
    install_modules(_sim_modules(mkmod, use_seed=False))
    with pytest.raises(AdapterError, match="semilla"):
        adapters.SimulatorEnvelopes.build(3101)


def test_simulador_que_pide_algo_desconocido(install_modules, mkmod):
    install_modules(_sim_modules(mkmod, needs_users=True))
    with pytest.raises(AdapterError, match="users"):
        adapters.SimulatorEnvelopes.build(3101)


def test_simulador_restaura_el_entorno(install_modules, mkmod, monkeypatch):
    monkeypatch.setenv("SIM_SEED", "7")
    install_modules(_sim_modules(mkmod))
    adapters.SimulatorEnvelopes.build(3101)
    assert os.environ["SIM_SEED"] == "7"


# ------------------------------------------------------------- anonimización

def _enriched_row_module(mkmod, variant="default"):
    def _core(envelope, key, pt):
        env = dict(envelope)
        author = env.pop("author")
        k = key if isinstance(key, bytes) else str(key).encode()
        env["author"] = {"user_hash": hmac.new(k, author["user_id"].encode(), hashlib.sha256).hexdigest(),
                         "followers": author.get("followers")}
        env["process_time"] = pt if isinstance(pt, str) else pt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return env

    if variant == "default":
        def enrich_envelope(envelope, key, process_time=None):
            return _core(envelope, key, process_time)
    elif variant == "kwonly-str":
        def enrich_envelope(envelope, *, hmac_key: bytes, now: "str | None" = None):
            assert isinstance(now, str)
            return _core(envelope, hmac_key, now)
    else:
        def enrich_envelope(envelope, hasher):
            return envelope
    return mkmod("common.enriched_row", enrich_envelope=enrich_envelope,
                 to_posts_enriched_row=lambda e: {"event_id": e["event_id"], "sentiment_label": None},
                 POSTS_ENRICHED_COLUMNS=[("event_id", "STRING", "REQUIRED"), ("x", "INT64", "NULLABLE")])


@pytest.mark.parametrize("variant", ["default", "kwonly-str"])
def test_enricher_variantes(install_modules, mkmod, variant):
    install_modules({"common.enriched_row": _enriched_row_module(mkmod, variant)})
    pt = dt.datetime(2026, 10, 23, 1, 2, 3, tzinfo=UTC)
    enrich = adapters.make_enricher("llave", pt)
    env = FakeEnvelopes(1).envelope("reddit", 1, pt)
    out = enrich(env)
    assert "user_id" not in json.dumps(out)
    assert out["process_time"].startswith("2026-10-23T01:02:03")
    assert "author" in env and "user_id" in env["author"], "no debe mutar el envelope original"


def test_enricher_firma_desconocida(install_modules, mkmod):
    install_modules({"common.enriched_row": _enriched_row_module(mkmod, "otra")})
    with pytest.raises(AdapterError, match="hasher.*parámetro de la llave"):
        adapters.make_enricher("llave", dt.datetime.now(UTC))


def test_to_row_copia_el_nlp_y_columnas(install_modules, mkmod):
    install_modules({"common.enriched_row": _enriched_row_module(mkmod)})
    row = adapters.to_row({"event_id": "e", "sentiment_label": "positive"})
    assert row["sentiment_label"] == "positive"
    assert adapters.posts_enriched_columns() == ["event_id", "x"]


# ----------------------------------------------------------------------- llave

def test_llave_env(monkeypatch):
    monkeypatch.setenv("MI_LLAVE", "abc")
    info = adapters.load_key("env", env_var="MI_LLAVE")
    assert info.value == "abc" and len(info.fingerprint()) == 16
    monkeypatch.delenv("MI_LLAVE")
    with pytest.raises(AdapterError, match="vacía"):
        adapters.load_key("env", env_var="MI_LLAVE")


def test_llave_enum_keysource(install_modules, mkmod):
    class KeySource(enum.Enum):
        SECRET = "secret"
        ENV = "env"
        LOCAL_DEV = "local_dev"

    seen = {}

    def load_hmac_key(source, *, project=None, secret_name="ta-hmac-key", env_var=None):
        seen.update(source=source, project=project, secret_name=secret_name)
        return "dev-key" if source is KeySource.LOCAL_DEV else "secret-key"

    install_modules({"common.hmac_key": mkmod("common.hmac_key", KeySource=KeySource,
                                              load_hmac_key=load_hmac_key)})
    assert adapters.load_key("local_dev").value == "dev-key"
    info = adapters.load_key("secret", gcp_project="p", secret_name="s")
    assert info.value == "secret-key" and seen == {"source": KeySource.SECRET, "project": "p",
                                                   "secret_name": "s"}


def test_llave_dataclass_keysource(install_modules, mkmod):
    @dataclasses.dataclass
    class KeySource:
        kind: str
        secret_name: str | None = None
        project: str | None = None

    def load_hmac_key(source):
        return (f"{source.kind}:{source.project}:{source.secret_name}".encode(), "ok")

    install_modules({"common.hmac_key": mkmod("common.hmac_key", KeySource=KeySource,
                                              load_hmac_key=load_hmac_key)})
    assert adapters.load_key("secret", gcp_project="p", secret_name="s").value == b"secret:p:s"


def test_llave_alternativas(install_modules, mkmod):
    def load_hmac_key(*, runner_is_dataflow):
        raise RuntimeError("no aplica")

    def access_secret(project, name, version="latest"):
        return f"{project}/{name}"

    install_modules({"common.hmac_key": mkmod("common.hmac_key", load_hmac_key=load_hmac_key,
                                              access_secret=access_secret,
                                              LOCAL_DEV_KEY="publica")})
    assert adapters.load_key("secret", gcp_project="p", secret_name="s").value == "p/s"
    assert adapters.load_key("local_dev").value == "publica"


def test_llave_origen_invalido():
    with pytest.raises(AdapterError):
        adapters.load_key("vault")


def test_llave_vacia_no_se_acepta_y_se_prueba_la_siguiente(install_modules, mkmod):
    def load_hmac_key(source, *, project=None, secret_name="ta-hmac-key"):
        return ""

    def access_secret(project, name):
        return "llave-real"

    install_modules({"common.hmac_key": mkmod("common.hmac_key", load_hmac_key=load_hmac_key,
                                              access_secret=access_secret)})
    info = adapters.load_key("secret", gcp_project="p", secret_name="s")
    assert info.value == "llave-real" and info.detail.endswith("access_secret()")
    assert info.attempts and "vacía" in info.attempts[0]


def test_llave_vacia_en_todos_los_caminos_explica(install_modules, mkmod, monkeypatch):
    import subprocess

    install_modules({"common.hmac_key": mkmod("common.hmac_key",
                                              load_hmac_key=lambda source, **kw: b"  ",
                                              access_secret=lambda project, name: None)})
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "\n", ""))
    with pytest.raises(AdapterError) as err:
        adapters.load_key("secret", gcp_project="p")
    text = str(err.value)
    assert "load_hmac_key" in text and "access_secret" in text and "salida vacía" in text


def test_enricher_encuentra_la_llave_por_nombre_parecido(install_modules, mkmod):
    seen = {}

    def enrich_envelope(envelope, *, hmac_key_hex: str = "", process_ts: "str | None" = None):
        if not hmac_key_hex:
            raise ValueError("llave HMAC vacía")
        seen.update(key=hmac_key_hex, pt=process_ts)
        return {"event_id": envelope["event_id"]}

    install_modules({"common.enriched_row": mkmod("common.enriched_row", enrich_envelope=enrich_envelope)})
    pt = dt.datetime(2026, 10, 24, 1, 2, 3, tzinfo=UTC)
    out = adapters.make_enricher("llave-real", pt)({"event_id": "e"})
    assert out == {"event_id": "e"} and seen["key"] == "llave-real"
    assert seen["pt"] == "2026-10-24T01:02:03.000Z"


def test_enricher_sin_parametro_de_llave_falla_claro(install_modules, mkmod):
    install_modules({"common.enriched_row": mkmod(
        "common.enriched_row", enrich_envelope=lambda envelope, process_time=None: envelope)})
    with pytest.raises(AdapterError, match="parámetro de la llave"):
        adapters.make_enricher("k", dt.datetime.now(UTC))


def test_enricher_rechaza_llave_vacia(install_modules, mkmod):
    install_modules({"common.enriched_row": _enriched_row_module(mkmod)})
    with pytest.raises(AdapterError, match="vacía"):
        adapters.make_enricher("", dt.datetime.now(UTC))
