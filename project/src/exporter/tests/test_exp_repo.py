"""Contra el código real del repositorio (en la máquina del equipo deben correr sin omitidas)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from exporter.lake_check import DOC_COLUMNS, posts_enriched_column_names

pytestmark = pytest.mark.repo

PROJECT = Path(__file__).resolve().parents[3]


def test_columnas_de_posts_enriched_desde_el_codigo_real():
    if not (PROJECT / "src/common/enriched_row.py").exists():
        pytest.skip("falta src/common/enriched_row.py")
    names, source = posts_enriched_column_names()
    assert names is not None, source
    assert len(names) == 24 and set(names) == set(DOC_COLUMNS), sorted(set(names) ^ set(DOC_COLUMNS))


def test_cli_del_script_arranca():
    proc = subprocess.run(
        [sys.executable, str(PROJECT / "scripts/export_gcs_to_s3.py"), "--help"],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60,
    )
    assert proc.returncode == 0 and "lake-report" in proc.stdout, proc.stderr


def test_build_script_en_seco_imprime_la_imagen():
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("sin bash")
    env = dict(os.environ, GCP_PROJECT="project-0568d9ba-848f-413c-88b", GCP_REGION="us-central1")
    proc = subprocess.run(
        [bash, str(PROJECT / "scripts/build_exporter_image.sh"), "--dry-run"],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=120, env=env,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    line = [ln for ln in proc.stdout.splitlines() if ln.startswith("exporter_image = ")]
    assert line and re.fullmatch(
        r'exporter_image = "us-central1-docker\.pkg\.dev/project-0568d9ba-848f-413c-88b/ta-images/gcs-to-s3:[A-Za-z0-9._-]+-[0-9a-f]{8}"',
        line[0],
    ), line


def test_la_etiqueta_es_determinista():
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("sin bash")
    env = dict(os.environ, GCP_PROJECT="p")

    def tag() -> str:
        out = subprocess.run([bash, str(PROJECT / "scripts/build_exporter_image.sh"), "--dry-run"],
                             capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=120, env=env).stdout
        return [ln for ln in out.splitlines() if ln.startswith("exporter_image")][0].rsplit("-", 1)[1]

    assert tag() == tag()  # determinista


def test_validador_del_lote_presente():
    for rel in ("scripts/validate_3_2.py", "src/ops/validate32.py", "src/ops/tests/test_ops_validate32.py"):
        assert (PROJECT / rel).exists(), rel
