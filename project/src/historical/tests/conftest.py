"""Fixtures de las pruebas del loader histórico (Lote 3.1). Sin red, Docker ni nube."""

from __future__ import annotations

import datetime as dt
import sys
import types
from pathlib import Path

import pytest

from historical.plan import UTC, format_ts, load_config

AS_OF = dt.datetime(2026, 10, 23, tzinfo=UTC)
PROCESS_TIME = dt.datetime(2026, 10, 23, 18, 0, 0, 123000, tzinfo=UTC)


@pytest.fixture
def as_of() -> dt.datetime:
    return AS_OF


def make_cfg(**changes):
    cfg = load_config()
    changes.setdefault("as_of", format_ts(AS_OF))
    return cfg.replace(**changes)


@pytest.fixture
def small_cfg():
    """900 filas en 60 días: la crisis (hace 40 días) cae dentro; la campaña (120) no."""
    return make_cfg(n=900, days=60, chunk_size=200)


def run_fake_pipeline(root: Path, cfg, *, nlp_fail_first: int = 0, write: bool = True,
                      envelopes=None):
    from historical.generate import generate
    from historical.nlp_batch import run_nlp_stage
    from historical.testing import (FakeEnvelopes, FakeNlpEngine, fake_rows_to_parquet,
                                    fake_to_row, make_fake_enricher)
    from historical.work import WorkDir
    from historical.writer import write_output

    work = WorkDir(root, cfg.seed)
    gen = generate(cfg, AS_OF, work, envelopes=envelopes or FakeEnvelopes(cfg.seed),
                   enrich=make_fake_enricher("llave-de-prueba", PROCESS_TIME),
                   key_info={"source": "test"}, process_time=PROCESS_TIME, progress_every=0)
    engine = FakeNlpEngine(fail_first=nlp_fail_first)
    nlp = run_nlp_stage(work, engine, to_row=fake_to_row)
    out = None
    if write:
        out = write_output(work, to_row=fake_to_row, rows_to_parquet=fake_rows_to_parquet)
    return work, gen, nlp, out


@pytest.fixture
def fake_run(tmp_path, small_cfg):
    return run_fake_pipeline(tmp_path / "lake", small_cfg)


@pytest.fixture
def install_modules(monkeypatch):
    """Instala módulos falsos con nombres reales (``simulator.factory``…) solo en la prueba.

    Pisa también el atributo del paquete padre, para que ``from paquete import mod``
    devuelva el falso aunque el real ya esté importado.
    """

    def install(mapping: dict[str, types.ModuleType]) -> None:
        for name, module in mapping.items():
            parts = name.split(".")
            for i in range(1, len(parts)):
                parent_name = ".".join(parts[:i])
                if parent_name not in sys.modules:
                    pkg = types.ModuleType(parent_name)
                    pkg.__path__ = []  # type: ignore[attr-defined]
                    pkg.__file__ = str(Path("/nonexistent") / parent_name / "__init__.py")
                    monkeypatch.setitem(sys.modules, parent_name, pkg)
            monkeypatch.setitem(sys.modules, name, module)
            if len(parts) > 1:
                monkeypatch.setattr(sys.modules[".".join(parts[:-1])], parts[-1], module,
                                    raising=False)

    return install


def module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


@pytest.fixture
def cfg_factory():
    """``cfg_factory(n=…, days=…)``: configuración por defecto con ``as_of`` fijo."""
    return make_cfg


@pytest.fixture
def pipeline():
    """``pipeline(root, cfg, nlp_fail_first=0, write=True, envelopes=None)`` con dobles."""
    return run_fake_pipeline


@pytest.fixture
def mkmod():
    """``mkmod("nombre", attr=valor…)``: módulo falso."""
    return module
