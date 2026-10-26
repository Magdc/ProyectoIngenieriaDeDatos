"""CLI de punta a punta con los adaptadores reemplazados por dobles."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pyarrow")

from historical import adapters, cli, nlp_batch  # noqa: E402
from historical.testing import (COLUMNS, FakeEnvelopes, FakeNlpEngine,  # noqa: E402
                                fake_rows_to_parquet, fake_to_row, make_fake_enricher)


@pytest.fixture
def fakes(monkeypatch):
    class Envs:
        @staticmethod
        def build(seed):
            return FakeEnvelopes(seed)

    monkeypatch.setattr(adapters, "SimulatorEnvelopes", Envs)
    monkeypatch.setattr(adapters, "load_key",
                        lambda source, **kw: adapters.KeyInfo("llave", source, "prueba"))
    monkeypatch.setattr(adapters, "make_enricher", make_fake_enricher)
    monkeypatch.setattr(adapters, "to_row", fake_to_row)
    monkeypatch.setattr(adapters, "rows_to_parquet", fake_rows_to_parquet)
    monkeypatch.setattr(adapters, "posts_enriched_columns", lambda: list(COLUMNS))
    monkeypatch.setattr(nlp_batch, "BeamNlpEngine", lambda **kw: FakeNlpEngine())
    monkeypatch.delenv("HIST_SEED", raising=False)
    monkeypatch.delenv("HIST_N", raising=False)


def _run(*argv):
    return cli.main(list(argv))


BASE = ("--n", "600", "--days", "60", "--as-of", "2026-10-23", "--chunk-size", "250")


def test_plan_sin_dependencias(tmp_path, capsys):
    assert _run("plan", "--root", str(tmp_path), *BASE) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["sources"] == {"reddit": 200, "facebook": 200, "instagram": 200}


def test_flujo_completo(tmp_path, fakes, capsys):
    root = str(tmp_path / "lake")
    ev = tmp_path / "ev"
    assert _run("generate", "--root", root, *BASE, "--key-source", "local_dev",
                "--validate", "no", "--json-out", str(ev / "gen.json")) == 0
    gen = json.loads((ev / "gen.json").read_text())
    assert gen["rows"] == 600 and "seqs" not in gen
    # mismo plan: no regenera
    assert _run("generate", "--root", root, *BASE, "--key-source", "local_dev") == 0
    assert "ya generada" in capsys.readouterr().out
    # sin --as-of reutiliza el de generate.json
    assert _run("generate", "--root", root, "--n", "600", "--days", "60", "--chunk-size", "250",
                "--digest-only") == 0
    assert "igual al de generate.json" in capsys.readouterr().out
    assert _run("nlp", "--root", root) == 0
    assert _run("write", "--root", root, "--validate", "no") == 0
    assert _run("verify", "--root", root, "--allow-fake-nlp", "--json-out", str(ev / "v.json")) == 0
    report = json.loads((ev / "v.json").read_text())
    assert report["status"] in ("OK", "AVISO")
    capsys.readouterr()
    assert _run("status", "--root", root) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["write"]["rows"] == 600 and status["nlp"]["pending_files"] == 0


def test_digest_distinto_falla(tmp_path, fakes, capsys):
    root = str(tmp_path / "lake")
    assert _run("generate", "--root", root, *BASE, "--key-source", "local_dev", "--validate", "no") == 0
    assert _run("generate", "--root", root, "--n", "601", "--days", "60", "--as-of", "2026-10-23",
                "--digest-only") == 1


def test_verify_sin_motor_real_falla(tmp_path, fakes):
    root = str(tmp_path / "lake")
    _run("generate", "--root", root, *BASE, "--key-source", "local_dev", "--validate", "no")
    _run("nlp", "--root", root)
    _run("write", "--root", root, "--validate", "no")
    assert _run("verify", "--root", root) == 1


def test_write_sin_nlp_falla_con_salida_1(tmp_path, fakes, capsys):
    root = str(tmp_path / "lake")
    _run("generate", "--root", root, *BASE, "--key-source", "local_dev", "--validate", "no")
    assert _run("write", "--root", root, "--validate", "no") == 1
    assert "faltan" in capsys.readouterr().err


def test_errores_de_uso(tmp_path, fakes, capsys):
    assert _run("plan", "--root", str(tmp_path), "--as-of", "2999-01-01") == 2
    assert _run("upload", "--root", str(tmp_path)) == 2
    assert "falta --bucket" in capsys.readouterr().out


def test_semilla_por_entorno(tmp_path, fakes, monkeypatch, capsys):
    monkeypatch.setenv("HIST_SEED", "77")
    assert _run("plan", "--root", str(tmp_path), *BASE) == 0
    assert json.loads(capsys.readouterr().out)["seed"] == 77


def test_upload_con_cuenta_raiz_no_intenta_asumir_el_rol(tmp_path, fakes, monkeypatch, capsys):
    from historical import s3io

    monkeypatch.setattr(s3io, "make_session", lambda profile, region: object())
    monkeypatch.setattr(s3io, "identity", lambda session: {"arn": "arn:aws:iam::111122223333:root"})

    def boom(*a, **k):
        raise AssertionError("no debe asumir el rol con la cuenta raíz")

    monkeypatch.setattr(s3io, "assume_role_session", boom)
    assert _run("upload", "--root", str(tmp_path), "--bucket", "b", "--role-arn", "arn:r") == 2
    assert "cuenta raíz" in capsys.readouterr().out


def test_nlp_runner_fnapi_se_rechaza(tmp_path, fakes, capsys):
    assert _run("nlp", "--root", str(tmp_path), "--runner", "fnapi") == 2
    assert "FnApiRunner" in capsys.readouterr().err


def test_bench_workers():
    from historical.doctor import bench_workers

    assert bench_workers("2,4", 16) == [2, 4]
    assert bench_workers("1, 4,4,64", 8) == [2, 4, 8]
    assert bench_workers("", 16) == []
