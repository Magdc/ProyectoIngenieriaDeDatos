"""
src/profiling/test_profiling.py

Pruebas unitarias del modulo de profiling (US-14).
No requiere GCS ni datos reales.
"""

import sys
import os
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone

# Agregar src/ para importar quality
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "quality"))
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import profiler as P


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def make_df(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(records)


def base_record(overrides: dict | None = None) -> dict:
    r = {
        "source":      "mastodon",
        "id":          "117156446756584402",
        "created_at":  "2026-08-25T13:45:08.445Z",
        "text":        "Cafe colombiano conquista mercados",
        "url":         "https://mastodon.social/@user/117156",
        "language":    "es",
        "tags":        ["colombia", "cafe"],
        "author_name": "usuario",
        "author_id":   "98765",
        "engagement":  {"replies": 2, "reposts": 5, "likes": 10},
        "raw_metadata": {},
    }
    if overrides:
        r.update(overrides)
    return r


# ---------------------------------------------------------------------------
# calc_null_pct
# ---------------------------------------------------------------------------
class TestCalcNullPct:
    def test_sin_nulos(self):
        df = make_df([base_record(), base_record({"id": "other_id"})])
        null_pct = P.calc_null_pct(df)
        for field in ["id", "source", "created_at", "text", "url"]:
            assert null_pct[field] == 0.0

    def test_campo_faltante_reporta_100(self):
        df = make_df([{"source": "mastodon", "id": "1"}])
        null_pct = P.calc_null_pct(df)
        assert null_pct["text"] == 100.0

    def test_mitad_nulos(self):
        df = make_df([
            base_record({"language": "es"}),
            base_record({"id": "2", "language": None}),
        ])
        null_pct = P.calc_null_pct(df)
        assert null_pct["language"] == 50.0


# ---------------------------------------------------------------------------
# calc_duplicates
# ---------------------------------------------------------------------------
class TestCalcDuplicates:
    def test_sin_duplicados(self):
        df = make_df([base_record(), base_record({"id": "2"})])
        count, pct = P.calc_duplicates(df)
        assert count == 0
        assert pct == 0.0

    def test_con_duplicados(self):
        df = make_df([
            base_record(),
            base_record(),      # id duplicado
            base_record({"id": "3"}),
        ])
        count, pct = P.calc_duplicates(df)
        assert count == 1
        assert pct == pytest_approx(33.33, abs=0.1)

    def test_sin_columna_id(self):
        df = make_df([{"source": "news"}])
        count, pct = P.calc_duplicates(df)
        assert count == 0 and pct == 0.0


# ---------------------------------------------------------------------------
# calc_date_distribution
# ---------------------------------------------------------------------------
class TestCalcDateDistribution:
    def test_agrupa_por_fecha(self):
        df = make_df([
            base_record({"created_at": "2026-08-25T10:00:00Z"}),
            base_record({"id": "2", "created_at": "2026-08-25T15:00:00Z"}),
            base_record({"id": "3", "created_at": "2026-08-26T10:00:00Z"}),
        ])
        dist = P.calc_date_distribution(df)
        assert dist["2026-08-25"] == 2
        assert dist["2026-08-26"] == 1

    def test_ignora_nulos(self):
        df = make_df([
            base_record(),
            base_record({"id": "2", "created_at": None}),
        ])
        dist = P.calc_date_distribution(df)
        assert sum(dist.values()) == 1


# ---------------------------------------------------------------------------
# calc_text_stats
# ---------------------------------------------------------------------------
class TestCalcTextStats:
    def test_longitud_correcta(self):
        df = make_df([
            base_record({"text": "abc"}),      # 3
            base_record({"id": "2", "text": "abcde"}),  # 5
        ])
        stats = P.calc_text_stats(df)
        assert stats["mean"] == 4.0
        assert stats["median"] == 4.0

    def test_vacios_contados(self):
        df = make_df([
            base_record({"text": ""}),
            base_record({"id": "2", "text": "algo"}),
        ])
        stats = P.calc_text_stats(df)
        assert stats["empty_count"] == 1


# ---------------------------------------------------------------------------
# calc_top_tags
# ---------------------------------------------------------------------------
class TestCalcTopTags:
    def test_top_tags_mas_frecuentes(self):
        df = make_df([
            base_record({"tags": ["colombia", "cafe"]}),
            base_record({"id": "2", "tags": ["colombia", "bogota"]}),
            base_record({"id": "3", "tags": ["cafe"]}),
        ])
        top = P.calc_top_tags(df, top_n=3)
        tags = [t for t, _ in top]
        freqs = {t: f for t, f in top}
        assert "colombia" in tags
        assert freqs["colombia"] == 2
        assert freqs["cafe"] == 2

    def test_sin_tags_retorna_vacio(self):
        df = make_df([{"source": "news"}])
        assert P.calc_top_tags(df) == []


# ---------------------------------------------------------------------------
# check_kpis
# ---------------------------------------------------------------------------
class TestCheckKpis:
    def test_kpis_ok(self):
        null_pct = {f: 0.0 for f in ["id", "source", "created_at", "text", "url"]}
        kpis = P.check_kpis(total=100, null_pct=null_pct, dup_count=0, dup_pct=0.0)
        assert kpis["completeness_ok"]
        assert kpis["uniqueness_ok"]
        assert kpis["duplicates_ok"]

    def test_completeness_falla(self):
        null_pct = {"id": 20.0, "source": 0, "created_at": 0, "text": 0, "url": 0}
        kpis = P.check_kpis(100, null_pct, 0, 0.0)
        assert not kpis["completeness_ok"]

    def test_duplicados_fallan(self):
        null_pct = {f: 0.0 for f in ["id", "source", "created_at", "text", "url"]}
        kpis = P.check_kpis(100, null_pct, dup_count=5, dup_pct=5.0)
        assert not kpis["duplicates_ok"]


# ---------------------------------------------------------------------------
# load_from_local
# ---------------------------------------------------------------------------
class TestLoadFromLocal:
    def test_carga_json_local(self, tmp_path):
        records = [base_record(), base_record({"id": "2"})]
        dir_ = tmp_path / "mastodon" / "2026" / "08" / "25"
        dir_.mkdir(parents=True)
        (dir_ / "raw_120000.json").write_text(
            json.dumps(records), encoding="utf-8"
        )
        dfs = P.load_from_local(str(tmp_path))
        assert "mastodon" in dfs
        assert len(dfs["mastodon"]) == 2

    def test_fuente_sin_directorio_omitida(self, tmp_path):
        dfs = P.load_from_local(str(tmp_path))
        assert len(dfs) == 0


# ---------------------------------------------------------------------------
# Helper para test_duplicates
# ---------------------------------------------------------------------------
def pytest_approx(value, abs=None):
    """Alias simple para evitar importar pytest en el modulo."""
    class Approx:
        def __eq__(self, other):
            return abs is None or __builtins__["abs"](other - value) <= abs
    return Approx()
