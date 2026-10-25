"""Verificador con --expect-nlp (Lote 2.3). Sin red."""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone

from common.parquet_rows import parquet_bytes_to_rows, rows_to_parquet_bytes
from streaming import nlp
from streaming.enriched_cli import run
from streaming.enriched_testing import make_envelope, write_local_lake
from streaming.nlp_testing import FAKE_REVISIONS

NOW = datetime(2026, 10, 21, 14, 40, 0, tzinfo=timezone.utc)
VERSION = nlp.nlp_model_version(FAKE_REVISIONS["sentiment"], FAKE_REVISIONS["emotion"])


def _lake(root, fill=True, bad=None):
    envs = [make_envelope(s, i, event_time=NOW - timedelta(minutes=31)) for i, s in enumerate(["reddit", "facebook", "instagram"] * 5)]
    write_local_lake(str(root), envs, now=NOW)
    for d, _x, files in os.walk(os.path.join(root, "enriched")):
        for f in files:
            path = os.path.join(d, f)
            rows = parquet_bytes_to_rows(open(path, "rb").read())
            if fill:
                for r in rows:
                    r.update(sentiment_label="positive", sentiment_score=0.9, emotion_label="joy", emotion_score=0.8, nlp_model_version=VERSION)
                    if bad:
                        r.update(bad)
            open(path, "wb").write(rows_to_parquet_bytes(rows))


def _run(argv):
    out = io.StringIO()
    code = run(argv, env={}, now=lambda: NOW, out=out)
    return code, out.getvalue()


def test_nlp_completo_pasa_y_reporta_distribucion(tmp_path):
    _lake(tmp_path)
    code, text = _run(["report", "--lake-root", str(tmp_path), "--expect-nlp"])
    assert code == 0, text
    assert "NLP presente" in text and "acuerdo con ground truth 1.0" in text


def test_sin_nlp_falla_solo_si_se_espera(tmp_path):
    _lake(tmp_path, fill=False)
    assert _run(["report", "--lake-root", str(tmp_path)])[0] == 0
    code, text = _run(["report", "--lake-root", str(tmp_path), "--expect-nlp"])
    assert code == 1 and "NLP presente" in text


def test_etiquetas_o_version_invalidas_fallan(tmp_path):
    _lake(tmp_path, bad={"sentiment_label": "POS"})
    assert _run(["report", "--lake-root", str(tmp_path), "--expect-nlp"])[0] == 1
    _lake(tmp_path, bad={"nlp_model_version": "robertuito-sentiment@x"})
    assert _run(["report", "--lake-root", str(tmp_path), "--expect-nlp"])[0] == 1


def test_raw_anterior_a_la_ventana_no_cuenta_en_la_cobertura(tmp_path):
    """raw/ de un pipeline anterior (sin rama enriched) en la misma hora no baja la cobertura."""
    import gzip
    import json as _json

    _lake(tmp_path)
    old = tmp_path / "raw" / "source=reddit" / f"dt={NOW:%Y-%m-%d}" / f"hr={NOW:%H}"
    old.mkdir(parents=True, exist_ok=True)
    start = NOW.replace(minute=0)
    viejos = [make_envelope("reddit", 900 + i) for i in range(20)]
    (old / f"part-{start:%Y%m%dT%H%M%S}Z-p000.jsonl.gz").write_bytes(
        gzip.compress("\n".join(_json.dumps(e) for e in viejos).encode()))
    code, text = _run(["report", "--lake-root", str(tmp_path), "--hours", "0.6", "--expect-nlp"])
    assert code == 0, text
