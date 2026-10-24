import csv
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "generate_comments.py"
spec = importlib.util.spec_from_file_location("generate_comments_cli", SCRIPT)
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


def _leer(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_csv_basico(tmp_path):
    out = tmp_path / "c.csv"
    assert cli.main(["--n", "200", "--seed", "7", "--out", str(out)]) == 0
    filas = _leer(out)
    assert filas[0] == ["marca", "comentario", "sentimiento"]
    assert len(filas) == 201
    assert {f[0] for f in filas[1:]} == {"postobon", "pepsi", "sprite"}
    assert {f[2] for f in filas[1:]} <= {"positive", "negative", "neutral", ""}
    assert all(f[1].strip() for f in filas[1:])
    assert not out.read_bytes().startswith(b"\xef\xbb\xbf")     # UTF-8 sin BOM por defecto


def test_reproducible_byte_a_byte(tmp_path):
    a, b, c = (tmp_path / n for n in ("a.csv", "b.csv", "c.csv"))
    cli.main(["-n", "300", "--seed", "7", "-o", str(a)])
    cli.main(["-n", "300", "--seed", "7", "-o", str(b)])
    cli.main(["-n", "300", "--seed", "8", "-o", str(c)])
    assert a.read_bytes() == b.read_bytes()
    assert a.read_bytes() != c.read_bytes()


def test_filtros_y_columna_categoria(tmp_path):
    out = tmp_path / "f.csv"
    cli.main(["-n", "100", "-o", str(out), "--marca", "Postobón", "--sentimiento", "negative",
              "--categoria"])
    filas = _leer(out)
    assert filas[0] == ["marca", "comentario", "sentimiento", "categoria"]
    assert {(f[0], f[2], f[3]) for f in filas[1:]} == {("postobon", "negative", "asco")}


def test_comentarios_con_comas_se_entrecomillan(tmp_path):
    out = tmp_path / "q.csv"
    cli.main(["-n", "300", "-o", str(out), "--ruido", "0"])
    assert any(f[1].count(",") for f in _leer(out)[1:])
    assert all(len(f) == 3 for f in _leer(out))


def test_unico_sin_duplicados(tmp_path):
    out = tmp_path / "u.csv"
    assert cli.main(["-n", "2000", "-o", str(out), "--unico"]) == 0
    filas = _leer(out)[1:]
    assert len(filas) == 2000 and len({(f[0], f[1]) for f in filas}) == 2000


def test_bom_opcional(tmp_path):
    out = tmp_path / "b.csv"
    cli.main(["-n", "5", "-o", str(out), "--bom"])
    assert out.read_bytes().startswith(b"\xef\xbb\xbf")


def test_argumentos_invalidos(tmp_path):
    out = str(tmp_path / "x.csv")
    assert cli.main(["-n", "5", "-o", out, "--marca", "manzana"]) == 2
    assert cli.main(["-n", "5", "-o", out, "--ruido", "2"]) == 2
    assert cli.main(["-n", "5", "-o", out, "--sentimiento", "positive",
                     "--solo-categoria", "asco"]) == 2
