"""Validación automática del Lote 5.1 (HU-031): ``docs/validation/validation-5-1.md``.

    python project/scripts/validate_5_1.py <parte> [opciones]      (desde la raíz del repo)

Partes: ``preflight`` (A), ``tests`` (B), ``raw`` (C: caché de S3 y *profiling* real de Raw),
``trusted`` (D: Trusted real si existe; si no, el Trusted simulado con el contrato de §5.7 y con
defectos conocidos), ``all`` (A–D seguidas) y ``clean`` (borra la caché local, pide confirmación).
Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO) y guarda su resumen en
``project/evidencia/validacion_5_1.json``.

Solo lee de la nube (``sts get-caller-identity``, ``ListObjectsV2`` y ``GetObject`` del lake con el
perfil de AWS); no escribe en S3, GCS ni BigQuery. Reutiliza ``ops.services`` (``status``) para
avisar si algo que cobra quedó encendido; no lo modifica.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

REPO = Path(__file__).resolve().parents[3]
PROJECT = REPO / "project"
SRC = PROJECT / "src"
DOCS = REPO / "docs"
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_5_1.json"
PROFILE_CLI = PROJECT / "scripts" / "profile_data.py"

RAW_JSON = "profiling_raw_5_1.json"
RAW_MD = "profiling_raw_5_1.md"
SYNC_RAW_JSON = "profiling_sync_raw_5_1.json"
TRUSTED_JSON = "profiling_trusted_5_1.json"
TRUSTED_MD = "profiling_trusted_5_1.md"
SYNTH_JSON = "profiling_trusted_synth_5_1.json"
SYNTH_MD = "profiling_trusted_synth_5_1.md"
SYNTH_DEFECTS_JSON = "profiling_trusted_defects_5_1.json"

HISTORICAL_ROWS = 50_000
HISTORICAL_OBJECTS = 12_102
DEFECT_LIMIT_FILES = 400
MAX_EVIDENCE_BYTES = 2_000_000
HEX64 = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")
USER_ID = re.compile(r"\bu_\d{6}\b")

# Pruebas esperadas (B1, B2): las cuenta docs/validation/validation-5-1.md.
B1_EXPECTED = 165
B2_EXPECTED = 24

OK, AVISO, FALLA, OMITIDO = "OK", "AVISO", "FALLA", "OMITIDO"

LOT_FILES = [
    "src/quality/__init__.py",
    "src/quality/layers.py",
    "src/quality/checks.py",
    "src/quality/text_stats.py",
    "src/quality/profile_core.py",
    "src/quality/profiler.py",
    "src/quality/anomalies.py",
    "src/quality/rules.py",
    "src/quality/report_md.py",
    "src/quality/s3sync.py",
    "src/quality/publish.py",
    "src/quality/testing.py",
    "src/quality/cli.py",
    "src/quality/requirements.txt",
    "scripts/profile_data.py",
    "scripts/validate_5_1.py",
    "src/ops/validate51.py",
    "src/ops/tests/test_ops_validate51.py",
]
PREVIOUS_FILES = [
    "src/common/enriched_row.py",
    "src/common/parquet_rows.py",
    "src/common/hashtags.py",
    "src/common/envelope.py",
    "src/simulator/config/brands.yaml",
    "contracts/athena/posts_raw.sql",
    "contracts/athena/posts.sql",
    "contracts/envelope.enriched.v1.schema.json",
    "src/ops/services.py",
    "scripts/status.py",
]


# --- ejecución de comandos ---------------------------------------------------------------------


@dataclass
class Result:
    code: int
    out: str = ""
    err: str = ""
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.code == 0

    @property
    def text(self) -> str:
        return (self.out or "") + (("\n" + self.err) if self.err else "")

    def tail(self, n: int = 12) -> str:
        lines = [ln for ln in self.text.splitlines() if ln.strip()]
        return "\n".join(lines[-n:])


class Runner:
    """Ejecuta comandos sin entrada estándar; con ``stream`` muestra la salida al vuelo."""

    def __init__(self, printer: Callable[[str], None] = print):
        self.printer = printer

    def which(self, name: str) -> str | None:
        return shutil.which(name)

    def run(self, cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None,
            timeout: float = 900, stream: bool = False) -> Result:
        start = time.monotonic()
        full_env = dict(os.environ)
        full_env.update(env or {})
        try:
            proc = subprocess.Popen(cmd, cwd=str(cwd) if cwd else None, env=full_env, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT if stream else subprocess.PIPE,
                                    text=True, encoding="utf-8", errors="replace")
        except FileNotFoundError as exc:
            return Result(127, "", f"no se encontró {cmd[0]}: {exc}")
        if not stream:
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                out, err = proc.communicate()
                return Result(124, out or "", (err or "") + f"\ntiempo agotado ({timeout:.0f} s)", time.monotonic() - start)
            return Result(proc.returncode, out or "", err or "", time.monotonic() - start)
        lines: list[str] = []

        def pump() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                lines.append(line)
                self.printer("    │ " + line.rstrip("\n"))

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            reader.join(5)
            return Result(124, "".join(lines), f"tiempo agotado ({timeout:.0f} s)", time.monotonic() - start)
        reader.join(5)
        return Result(proc.returncode, "".join(lines), "", time.monotonic() - start)


# --- pasos y evidencia ------------------------------------------------------------------------


@dataclass
class Step:
    id: str
    title: str
    status: str
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)


class Part:
    def __init__(self, ctx: "Ctx", key: str, title: str):
        self.ctx, self.key, self.title = ctx, key, title
        self.steps: list[Step] = []
        self.started = ctx.now()
        ctx.printer(f"\n== Parte {key} · {title} ==")

    def add(self, sid: str, title: str, status: str, detail: str = "", **data: Any) -> Step:
        step = Step(sid, title, status, detail, data)
        self.steps.append(step)
        first, *rest = (detail or "").splitlines() or [""]
        self.ctx.printer(f"{sid:<4} {status:<7} {title}" + (f": {first}" if first else ""))
        for line in rest[:15]:
            self.ctx.printer(f"             {line}")
        return step

    def note(self, text: str) -> None:
        self.ctx.printer(f"     … {text}")

    @property
    def worst(self) -> str:
        statuses = {s.status for s in self.steps}
        for status in (FALLA, AVISO):
            if status in statuses:
                return status
        return OK

    def finish(self) -> int:
        counts = {s: sum(1 for st in self.steps if st.status == s) for s in (OK, AVISO, FALLA, OMITIDO)}
        summary = ", ".join(f"{n} {s}" for s, n in counts.items() if n)
        self.ctx.printer(f"-- Parte {self.key}: {self.worst} ({summary})")
        self.ctx.save_part(self)
        return 1 if self.worst == FALLA else 0


@dataclass
class Ctx:
    runner: Runner
    options: argparse.Namespace
    printer: Callable[[str], None] = print
    confirm: Callable[[str], bool] | None = None
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    evidence_path: Path = EVIDENCE
    services_runner: Any = None
    cache: dict[str, Any] = field(default_factory=dict)

    def ev(self, name: str) -> Path:
        """Archivo de evidencia junto a ``validacion_5_1.json`` (``project/evidencia/`` salvo en pruebas)."""
        return self.evidence_path.parent / name

    def env(self) -> dict[str, str]:
        env = {"PYTHONPATH": str(SRC), "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}
        if getattr(self.options, "aws_profile", None):
            env["AWS_PROFILE"] = self.options.aws_profile
        if getattr(self.options, "cache", None):
            env["TA_QUALITY_CACHE"] = str(self.options.cache)
        return env

    def ask(self, question: str) -> bool:
        if getattr(self.options, "yes", False):
            return True
        if self.confirm is not None:
            return self.confirm(question)
        try:
            return input(f"{question} [s/N] ").strip().lower() in {"s", "si", "sí", "y", "yes"}
        except EOFError:
            return False

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "5.1", "hu": ["HU-031"], "parts": {}}

    def save_part(self, part: Part) -> None:
        data = self.load_evidence()
        data.setdefault("parts", {})[part.key] = {
            "title": part.title, "status": part.worst, "started_at": part.started.isoformat(),
            "finished_at": self.now().isoformat(), "steps": [asdict(s) for s in part.steps],
        }
        data["updated_at"] = self.now().isoformat()
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    def run(self, cmd: list[str], **kw: Any) -> Result:
        env = self.env()
        env.update(kw.pop("env", None) or {})
        return self.runner.run(cmd, env=env, **kw)

    def profile_cli(self, *args: str, timeout: float = 1800, stream: bool = True) -> Result:
        extra = []
        if getattr(self.options, "bucket", None) and args and args[0] in ("sync", "profile", "synth-trusted"):
            if not ("--root" in args or "--from-root" in args):
                extra = ["--bucket", self.options.bucket]
        return self.run([sys.executable, str(PROFILE_CLI), *args, *extra], cwd=REPO, timeout=timeout, stream=stream)

    def pytest(self, *args: str, timeout: float = 900) -> Result:
        return self.run([sys.executable, "-m", "pytest", *args], cwd=PROJECT, timeout=timeout)


# --- utilidades puras -------------------------------------------------------------------------


def parse_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        start = min([i for i in (text.find("{"), text.find("[")) if i >= 0], default=-1)
        if start < 0:
            return None
        try:
            return json.loads(text[start:])
        except ValueError:
            return None


def pytest_counts(text: str) -> dict[str, int]:
    tail = "\n".join((text or "").splitlines()[-5:])
    counts = {}
    for key, pattern in (("passed", "passed"), ("failed", "failed"), ("skipped", "skipped"), ("error", "errors?")):
        m = re.search(rf"(\d+) {pattern}\b", tail)
        counts[key] = int(m.group(1)) if m else 0
    return counts


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def ddl_columns(sql: str) -> tuple[list[str], list[str]]:
    """Columnas y claves de partición de una DDL de ``contracts/athena/`` (una por línea)."""
    cols, parts, mode = [], [], None
    for line in sql.splitlines():
        s = line.strip()
        if s.upper().startswith("CREATE EXTERNAL TABLE"):
            mode = "cols"
            continue
        if s.upper().startswith("PARTITIONED BY"):
            mode = "parts"
            continue
        if s.startswith(")"):
            mode = None
            continue
        m = re.match(r"^([a-z_][a-z0-9_]*)\s+[a-z]", s)
        if m and mode == "cols":
            cols.append(m.group(1))
        elif m and mode == "parts":
            parts.append(m.group(1))
    return cols, parts


def doc_rule_ids(text: str) -> list[str]:
    """IDs ``DQ-NN`` de la tabla de reglas de ``ARCHITECTURE.md`` §5.10 (desde su título hasta §5.11)."""
    start = text.find("### 5.10")
    end = text.find("### 5.11", start + 1)
    if start < 0:
        return []
    section = text[start:end if end > 0 else None]
    seen: dict[str, None] = {}
    for m in re.finditer(r"^\|\s*(DQ-\d{2})\s*\|", section, re.M):
        seen.setdefault(m.group(1), None)
    return list(seen)


def evidence_privacy(paths: list[Path]) -> tuple[list[str], dict[str, int]]:
    """Busca valores de ``user_hash`` (hex de 64) o ``user_id`` (``u_000123``) en la evidencia."""
    problems, sizes = [], {}
    for p in paths:
        if not p.exists():
            continue
        data = p.read_text(encoding="utf-8", errors="replace")
        sizes[p.name] = len(data.encode("utf-8"))
        if HEX64.search(data):
            problems.append(f"{p.name}: contiene un hex de 64 (¿user_hash?)")
        if USER_ID.search(data):
            problems.append(f"{p.name}: contiene un user_id (u_NNNNNN)")
        if sizes[p.name] > MAX_EVIDENCE_BYTES:
            problems.append(f"{p.name}: {sizes[p.name]} bytes (> {MAX_EVIDENCE_BYTES})")
    return problems, sizes


def group(report: dict[str, Any], origin: str = "*", source: str = "*") -> dict[str, Any] | None:
    for g in report.get("groups", []):
        if g["key"]["origin"] == origin and g["key"]["source"] == source:
            return g
    return None


DISTRIBUTION_KEYS = ("top", "numeric", "length", "timestamp", "list", "frequency_histogram")


def coverage_problems(report: dict[str, Any], columns: list[str], origins: tuple[str, ...]) -> list[str]:
    """Lo que pide HU-031 por columna (nulos, cardinalidad, tipo, distribución) y por origen y fuente."""
    problems = []
    for origin in origins:
        g = group(report, origin)
        if g is None:
            problems.append(f"falta el grupo {origin}/*")
            continue
        for source in ("reddit", "facebook", "instagram"):
            if group(report, origin, source) is None:
                problems.append(f"falta el grupo {origin}/{source}")
        for col in columns:
            c = g["columns"].get(col)
            if c is None:
                problems.append(f"{origin}: falta la columna {col}")
                continue
            missing = [k for k in ("kind", "nulls", "null_ratio", "distinct") if k not in c]
            if missing or not any(k in c for k in DISTRIBUTION_KEYS):
                problems.append(f"{origin}.{col}: faltan {missing or 'la distribución'}")
        t = g.get("text") or {}
        for k in ("ttr", "mattr", "repeated", "skeletons", "types", "tokens"):
            if k not in t:
                problems.append(f"{origin}: falta text.{k} (diversidad léxica)")
        if "event_id" not in g:
            problems.append(f"{origin}: faltan los duplicados por event_id")
    if not report.get("files", {}).get("physical_types"):
        problems.append("faltan los tipos físicos por columna")
    return problems


# --- Parte A · preflight -----------------------------------------------------------------------

WSL_HINT = ("corra desde WSL: cd /mnt/c/Users/frank/Documents/baul/uni/2026-2/ProyectoDatos/repo_personal "
            "&& source ~/.venvs/ta-streaming/bin/activate")


def on_windows() -> bool:
    return os.name == "nt" or sys.platform.startswith("win")


def services_status_lines(ctx: Ctx) -> tuple[list[str] | None, str]:
    """``ops.services.status()`` (lo mismo que ``status.py``) con el ejecutor de ``ctx``."""
    try:
        from ops import services
    except Exception as exc:  # noqa: BLE001
        return None, f"no se pudo importar ops.services: {exc}"
    run = ctx.services_runner or services.Runner(cwd=PROJECT, echo=lambda _: None)
    try:
        settings = services.Settings(project=services.detect_project(run), region=os.environ.get("GCP_REGION", "us-central1"),
                                     env=os.environ.get("ENV", "dev"))
    except services.OpsError:
        settings = None
    try:
        return services.status(run, settings), ""
    except Exception as exc:  # noqa: BLE001
        return None, f"{type(exc).__name__}: {exc}"


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está en Windows ({sys.platform}); {WSL_HINT}")
    py = sys.version_info
    versions, missing, soft = [], [], []
    for mod, hard in (("pyarrow", True), ("yaml", False), ("boto3", False)):
        try:
            module = __import__(mod)
            versions.append(f"{mod} {getattr(module, '__version__', '?')}")
        except Exception:  # noqa: BLE001
            (missing if hard else soft).append(mod)
    status = FALLA if missing or (py.major, py.minor) != (3, 11) else (AVISO if soft else OK)
    part.add("A1", "Python y dependencias", status,
             f"Python {py.major}.{py.minor}.{py.micro}; " + ", ".join(versions)
             + (f"; faltan: {', '.join(missing)}" if missing else "")
             + (f"; faltan (C y D las usan): {', '.join(soft)}" if soft else ""))

    absent = [f for f in LOT_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(LOT_FILES)} presentes")
    absent = [f for f in PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A3", "archivos de lotes anteriores que usa", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(PREVIOUS_FILES)} presentes")

    problems = []
    try:
        from quality import layers as L

        for name, spec_cols, spec_parts in (("posts_raw", L.RAW_COLUMNS, ("part_origin", "part_source", "part_dt", "part_hr")),
                                            ("posts", L.TRUSTED_COLUMNS, ("source", "dt"))):
            path = PROJECT / "contracts" / "athena" / f"{name}.sql"
            cols, parts = ddl_columns(path.read_text(encoding="utf-8")) if path.exists() else ([], [])
            if cols != list(spec_cols):
                problems.append(f"{name}.sql: columnas {cols} ≠ {list(spec_cols)}")
            if parts != list(spec_parts):
                problems.append(f"{name}.sql: particiones {parts} ≠ {list(spec_parts)}")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"{type(exc).__name__}: {exc}")
    part.add("A4", "capas del profiler = contracts/athena (posts_raw, posts)", FALLA if problems else OK,
             "\n".join(problems) if problems else "24 columnas de Raw y 24 de Trusted, en orden, y sus particiones")

    aws = ctx.runner.which("aws")
    part.add("A5", "herramientas", OK if aws else AVISO, "aws " + ("presente" if aws else "no está (C1 lo usa)"))

    try:
        from quality import s3sync

        cache = s3sync.resolve_cache(getattr(ctx.options, "cache", None))
        cache.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(cache).free
        part.add("A6", "caché local fuera del repo", OK if free > 1e9 else AVISO,
                 f"{cache} · libre {free / 1e9:.1f} GB" + ("" if free > 1e9 else " (C necesita ~0,3 GB y D ~0,2 GB)"), cache=str(cache))
    except Exception as exc:  # noqa: BLE001
        part.add("A6", "caché local fuera del repo", FALLA, str(exc))

    lines, err = services_status_lines(ctx)
    if lines is None:
        part.add("A7", "servicios que cobran (ops.services.status)", AVISO, err)
    else:
        charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
        part.add("A7", "servicios que cobran (ops.services.status)", AVISO if charging else OK,
                 "\n".join(lines) + ("\neste lote no los necesita: python project/scripts/off.py" if charging else ""), lines=lines)
    return part.finish()


# --- Parte B · pruebas -------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    res = ctx.pytest("src/quality", "-q", "-rs", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B1_EXPECTED and c["skipped"] == 0
    part.add("B1", "pytest src/quality", OK if ok else FALLA,
             f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {B1_EXPECTED} passed, 0 skipped)"
             + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("src/ops/tests/test_ops_validate51.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    ok = res.ok and c["passed"] == B2_EXPECTED and c["skipped"] == 0
    part.add("B2", "pytest test_ops_validate51.py", OK if ok else FALLA,
             f"{c['passed']} passed, {c['failed']} failed (esperado {B2_EXPECTED})" + ("" if ok else "\n" + res.tail(10)), **c)
    res = ctx.pytest("contracts/tests", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    part.add("B3", "pytest contracts/tests", OK if res.ok and c["passed"] == 18 else FALLA, f"{c['passed']} passed (esperado 18)", **c)
    res = ctx.pytest("src/ops/tests/test_ops_services.py", "-q", "-p", "no:cacheprovider")
    c = pytest_counts(res.out)
    part.add("B4", "regresión de ops.services (test_ops_services.py)", OK if res.ok else FALLA,
             f"{c['passed']} passed, {c['failed']} failed" + ("" if res.ok else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte C · Raw ------------------------------------------------------------------------------


def aws_account(ctx: Ctx, part: Part, sid: str = "C1") -> str | None:
    if "account" in ctx.cache:
        return ctx.cache["account"]
    res = ctx.run(["aws", "sts", "get-caller-identity", "--output", "json"], timeout=60)
    data = parse_json(res.out) or {}
    account = data.get("Account")
    arn = str(data.get("Arn", ""))
    part.add(sid, "credenciales de AWS (solo lectura)", OK if res.ok and account else FALLA,
             (f"cuenta {account} · {'cuenta raíz' if arn.endswith(':root') else 'usuario o rol'}") if account
             else "aws sts get-caller-identity falló: " + res.tail(3), account=account)
    ctx.cache["account"] = account
    return account


def lake_bucket(ctx: Ctx, account: str | None) -> str | None:
    return getattr(ctx.options, "bucket", None) or os.environ.get("TA_LAKE_BUCKET") or (
        f"ta-{os.environ.get('ENV', 'dev')}-{account}-lake" if account else None)


def part_raw(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Raw: caché de S3 y profiling real")
    account = aws_account(ctx, part)
    bucket = lake_bucket(ctx, account)
    if not bucket:
        part.add("C2", "sincronizar raw/ a la caché", FALLA, "sin bucket: --bucket o TA_LAKE_BUCKET")
        return part.finish()
    ctx.options.bucket = bucket
    part.note(f"bucket {bucket}; la primera vez baja ~180 MB (13.000 objetos, 4–5 min); luego solo lista")
    res = ctx.profile_cli("sync", "--layer", "raw", "--json-out", str(ctx.ev(SYNC_RAW_JSON)), timeout=3600)
    sync = load_json_file(ctx.ev(SYNC_RAW_JSON))
    by = sync.get("by_prefix", {})
    hist = sum(v["objects"] for k, v in by.items() if k.startswith("origin=historical"))
    stream = sum(v["objects"] for k, v in by.items() if k.startswith("origin=stream"))
    sources_ok = all(any(k == f"origin={o}/source={s}" for k in by) for o in ("historical", "stream") for s in ("reddit", "facebook", "instagram"))
    status = FALLA if not res.ok or not stream or not hist else (OK if hist == HISTORICAL_OBJECTS and sources_ok else AVISO)
    part.add("C2", "sincronizar raw/ a la caché", status,
             f"{sync.get('objects')} objetos ({(sync.get('bytes') or 0) / 1e6:.1f} MB): histórico {hist} (esperado {HISTORICAL_OBJECTS}), "
             f"stream {stream}; bajados {sync.get('downloaded')}, ya estaban {sync.get('skipped_same_size')}, fallidos {sync.get('failed')}"
             + ("" if res.ok else "\n" + res.tail(6)), objects=sync.get("objects"), historical=hist, stream=stream)
    res2 = ctx.profile_cli("sync", "--layer", "raw", "--dry-run", stream=False, timeout=600)
    m = re.search(r"por bajar (\d+)", res2.out or "")
    pending = int(m.group(1)) if m else None
    part.add("C3", "repetir la sincronización no baja nada", OK if res2.ok and pending == 0 else (FALLA if not res2.ok else AVISO),
             f"por bajar {pending}" + ("" if res2.ok else "\n" + res2.tail(4)), pending=pending)

    part.note("perfilando ~220.000 filas en 13.000 archivos (2–4 min)…")
    res = ctx.profile_cli("profile", "--layer", "raw", "--json-out", str(ctx.ev(RAW_JSON)), "--md-out", str(ctx.ev(RAW_MD)), timeout=3600)
    rep = load_json_file(ctx.ev(RAW_JSON))
    part.add("C4", "profile --layer raw", OK if res.ok and rep else FALLA,
             (f"{rep.get('summary', {}).get('files')} archivos, {group(rep)['rows'] if group(rep) else '?'} filas en "
              f"{rep.get('duration_s')} s → {ctx.ev(RAW_JSON).name}, {ctx.ev(RAW_MD).name}") if rep else res.tail(8))
    if not rep:
        return part.finish()

    from quality import layers as L

    problems = coverage_problems(rep, list(L.RAW_COLUMNS), ("historical", "stream"))
    part.add("C5", "cobertura de HU-031 (columnas, orígenes, fuentes, texto, duplicados)", FALLA if problems else OK,
             "\n".join(problems[:10]) if problems else
             "24 columnas con nulos, cardinalidad, tipo y distribución; historical y stream × 3 fuentes; diversidad léxica; event_id")

    hist_g, stream_g = group(rep, "historical"), group(rep, "stream")
    kpi = rep["summary"]["kpi"]
    anon = all((kpi.get(o) or {}).get("anonimizacion") == 1.0 for o in ("historical", "stream"))
    hist_rows = hist_g["rows"] if hist_g else None
    sev = rep["summary"]["anomalies"]
    errors = [a for a in rep.get("anomalies", []) if a["severity"] == "error"]
    status = FALLA if not anon else (AVISO if hist_rows != HISTORICAL_ROWS or errors else OK)
    part.add("C6", "resultados del profiling de Raw", status,
             f"histórico {hist_rows} filas (esperado {HISTORICAL_ROWS}), stream {stream_g['rows'] if stream_g else '?'}; "
             + "; ".join(f"{o}: completitud {v['completitud']}, unicidad {v['unicidad']}, duplicados {v['duplicados']}, anonimización {v['anonimizacion']}"
                         for o, v in kpi.items() if o != "*")
             + f"\nanomalías: {sev['error']} error, {sev['warning']} aviso, {sev['info']} info"
             + ("".join(f"\nerror: {a['kind']} {a['column']} {a['scope']}: {a['detail']}" for a in errors[:5])),
             kpi=kpi, anomalies=sev)

    from quality import rules as R

    kpis = {r.kpi for r in R.RULES}
    missing_kpi = [k for k in ("completitud", "unicidad", "duplicados", "anonimizacion") if k not in kpis]
    per_layer = {lay: sum(1 for r in R.RULES if lay in r.layers) for lay in ("raw", "trusted")}
    bad = [r.id for r in R.RULES if not r.threshold or r.severity not in ("error", "warning") or not r.kpi]
    evaluated = rep.get("rules", [])
    failing_errors = [r["id"] for r in evaluated if r["status"] == "no_cumple" and r["severity"] == "error"]
    failing_warn = [r["id"] for r in evaluated if r["status"] == "no_cumple" and r["severity"] == "warning"]
    status = FALLA if missing_kpi or bad or not evaluated else (AVISO if failing_errors else OK)
    part.add("C7", "reglas propuestas para HU-032", status,
             f"{len(R.RULES)} reglas (Raw {per_layer['raw']}, Trusted {per_layer['trusted']}); KPI de §5.10 cubiertos"
             + (f"; faltan KPI {missing_kpi}" if missing_kpi else "") + (f"; incompletas {bad}" if bad else "")
             + f"\nen Raw hoy: {sum(1 for r in evaluated if r['status'] == 'cumple')} cumplen, "
             f"no cumplen: errores {failing_errors or '—'}, avisos {failing_warn or '—'}",
             rules=len(R.RULES), failing_errors=failing_errors, failing_warnings=failing_warn)

    problems, sizes = evidence_privacy([ctx.ev(RAW_JSON), ctx.ev(RAW_MD), ctx.ev(SYNC_RAW_JSON)])
    part.add("C8", "evidencia sin datos personales ni volcados", FALLA if problems else OK,
             "\n".join(problems) if problems else "sin user_hash ni user_id; tamaños " + ", ".join(f"{k} {v / 1e3:.0f} KB" for k, v in sizes.items()))

    arch = DOCS / "ARCHITECTURE.md"
    ids = doc_rule_ids(arch.read_text(encoding="utf-8")) if arch.exists() else []
    diff_missing = [i for i in R.RULE_IDS if i not in ids]
    diff_extra = [i for i in ids if i not in R.RULE_IDS]
    part.add("C9", "ARCHITECTURE §5.10 = catálogo de reglas del código",
             AVISO if not ids else (FALLA if diff_missing or diff_extra else OK),
             "no se encontró la tabla de §5.10" if not ids else
             (f"faltan en §5.10: {diff_missing}; sobran: {diff_extra}" if diff_missing or diff_extra else f"{len(ids)} reglas iguales"))
    return part.finish()


# --- Parte D · Trusted --------------------------------------------------------------------------


def part_trusted(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Trusted: real si existe; si no, simulado con el contrato de §5.7")
    account = aws_account(ctx, part, "D0")
    bucket = lake_bucket(ctx, account)
    if bucket:
        ctx.options.bucket = bucket
    res = ctx.profile_cli("sync", "--layer", "trusted", "--dry-run", stream=False, timeout=600)
    m = re.search(r":\s*(\d+) objetos", res.out or "")
    objects = int(m.group(1)) if m else None
    if not res.ok or objects is None:
        part.add("D1", "trusted/posts/ en S3", FALLA, "no se pudo listar: " + res.tail(4))
        return part.finish()
    raw_rep = load_json_file(ctx.ev(RAW_JSON))
    if objects:
        part.add("D1", "trusted/posts/ en S3", OK, f"{objects} objetos: se perfila el Trusted real", objects=objects)
        res = ctx.profile_cli("sync", "--layer", "trusted", timeout=3600)
        part.add("D2", "sincronizar trusted/posts/", OK if res.ok else FALLA, res.tail(3))
        args = ["profile", "--layer", "trusted", "--json-out", str(ctx.ev(TRUSTED_JSON)), "--md-out", str(ctx.ev(TRUSTED_MD))]
        if raw_rep:
            args += ["--raw-profile", str(ctx.ev(RAW_JSON))]
        res = ctx.profile_cli(*args, timeout=3600)
        rep = load_json_file(ctx.ev(TRUSTED_JSON))
        part.add("D3", "profile --layer trusted (real)", OK if res.ok and rep else FALLA, res.tail(6))
        if rep:
            trusted_checks(ctx, part, rep, raw_rep, prefix="D4", real=True)
            problems, _ = evidence_privacy([ctx.ev(TRUSTED_JSON), ctx.ev(TRUSTED_MD)])
            part.add("D6", "evidencia de Trusted sin datos personales", FALLA if problems else OK, "\n".join(problems) or "ok")
        return part.finish()

    part.add("D1", "trusted/posts/ en S3", AVISO,
             "vacío: Trusted no existe todavía (lo crea el Lote 4.2). Se valida la herramienta con un Trusted simulado; "
             "el profiling real de Trusted queda pendiente", objects=0, pending="Lote 4.2")
    if not raw_rep:
        part.add("D2", "Trusted simulado", FALLA, "falta el profiling de Raw: corra antes la parte C (raw)")
        return part.finish()
    synth_info = ctx.ev("profiling_trusted_synth_build_5_1.json")
    part.note("arma el Trusted simulado desde la caché de Raw (dedup, sin source, text_clean, dt = DATE(event_time); 1–2 min)…")
    res = ctx.profile_cli("synth-trusted", "--json-out", str(synth_info), timeout=3600)
    info = load_json_file(synth_info)
    part.add("D2", "Trusted simulado desde Raw", OK if res.ok and info.get("files") else FALLA,
             f"{info.get('files')} archivos, {info.get('distinct_event_ids')} event_id distintos de {info.get('raw_rows')} filas de Raw"
             if info else res.tail(6))
    if not info:
        return part.finish()
    res = ctx.profile_cli("profile", "--layer", "trusted", "--root", info["out_root"], "--raw-profile", str(ctx.ev(RAW_JSON)),
                          "--json-out", str(ctx.ev(SYNTH_JSON)), "--md-out", str(ctx.ev(SYNTH_MD)), timeout=3600)
    rep = load_json_file(ctx.ev(SYNTH_JSON))
    part.add("D3", "profile --layer trusted sobre el simulado", OK if res.ok and rep else FALLA,
             (f"{group(rep)['rows'] if group(rep) else '?'} filas en {rep.get('duration_s')} s → {ctx.ev(SYNTH_JSON).name}") if rep else res.tail(6))
    if rep:
        trusted_checks(ctx, part, rep, raw_rep, prefix="D4", real=False)

    defects_info = ctx.ev("profiling_trusted_defects_build_5_1.json")
    res = ctx.profile_cli("synth-trusted", "--defects", "--limit-files", str(DEFECT_LIMIT_FILES), "--json-out", str(defects_info),
                          stream=False, timeout=1800)
    dinfo = load_json_file(defects_info)
    if not dinfo:
        part.add("D5", "detecta defectos conocidos", FALLA, res.tail(6))
        return part.finish()
    res = ctx.profile_cli("profile", "--layer", "trusted", "--root", dinfo["out_root"], "--json-out", str(ctx.ev(SYNTH_DEFECTS_JSON)),
                          "--quiet", stream=False, timeout=1800)
    drep = load_json_file(ctx.ev(SYNTH_DEFECTS_JSON))
    found = detected_defects(drep) if drep else {}
    missing = [d for d in dinfo.get("defects", []) if not found.get(d)]
    part.add("D5", "detecta los 4 defectos inyectados", FALLA if missing or not drep else OK,
             ", ".join(f"{d}: {'detectado' if found.get(d) else 'NO detectado'}" for d in dinfo.get("defects", [])), detected=found)
    problems, _ = evidence_privacy([ctx.ev(SYNTH_JSON), ctx.ev(SYNTH_MD), ctx.ev(SYNTH_DEFECTS_JSON)])
    part.add("D6", "evidencia de Trusted sin datos personales", FALLA if problems else OK, "\n".join(problems) or "sin user_hash ni user_id")
    return part.finish()


def trusted_checks(ctx: Ctx, part: Part, rep: dict[str, Any], raw_rep: dict[str, Any], *, prefix: str, real: bool) -> None:
    from quality import layers as L

    problems = coverage_problems(rep, list(L.TRUSTED_COLUMNS), tuple(o for o in ("historical", "stream") if group(rep, o)))
    files = rep.get("files", {})
    variants = files.get("schema_variants", [])
    if files.get("unrecognized"):
        problems.append(f"{files['unrecognized']} archivos fuera del layout trusted/posts/source=…/dt=…/")
    if not variants or not all(v["matches_contract"] for v in variants):
        problems.append("esquema distinto de ta_trusted.posts: " + "; ".join(
            f"faltan {v['missing']} sobran {v['extra']} tipos {v['kind_mismatch'] + v['type_mismatch']}" for v in variants if not v["matches_contract"]))
    g_all = group(rep) or {}
    dt_ok = (g_all.get("checks", {}).get("partition:trusted_event_date") or {}).get("failed")
    uniq = (g_all.get("kpi") or {}).get("unicidad")
    rules = {r["id"]: r for r in rep.get("rules", [])}
    dq58 = rules.get("DQ-58", {})
    text_clean = "text_clean" in g_all
    detail = (f"{g_all.get('rows')} filas; dt = DATE(event_time) en todas: {dt_ok == 0}; unicidad {uniq}; "
              f"DQ-58 filas frente a Raw: {dq58.get('observed')} → {dq58.get('status')}; text_clean perfilado: {text_clean}")
    if dt_ok:
        problems.append(f"{dt_ok} filas con dt ≠ DATE(event_time)")
    if uniq != 1.0:
        problems.append(f"unicidad {uniq} (Trusted debe deduplicar)")
    if dq58.get("status") == "no_cumple":
        problems.append("DQ-58: filas de Trusted fuera de 99–100 % de los event_id distintos de Raw")
    if not text_clean:
        problems.append("text_clean no perfilado")
    errors = [a for a in rep.get("anomalies", []) if a["severity"] == "error"]
    status = FALLA if problems and not real else (AVISO if problems or errors else OK)
    part.add(prefix, "contrato de Trusted en el profiling" + ("" if real else " (simulado)"), status,
             detail + ("\n" + "\n".join(problems[:8]) if problems else "")
             + ("".join(f"\nerror: {a['kind']} {a['column']}: {a['detail']}" for a in errors[:5]) if real else ""))


def detected_defects(rep: dict[str, Any]) -> dict[str, bool]:
    g_all = group(rep) or {}
    checks = g_all.get("checks", {})
    anomalies = rep.get("anomalies", [])
    return {
        "duplicate_event_id": rep.get("duplicates", {}).get("other_file_same_origin", 0) >= 1
                              and any(a["kind"] == "duplicado" and a["severity"] == "error" for a in anomalies),
        "dt_mismatch": (checks.get("partition:trusted_event_date") or {}).get("failed", 0) >= 1,
        "null_text_clean": (checks.get("not_null:text_clean") or {}).get("failed", 0) >= 1,
        "identity_column": any(a["kind"] == "esquema" and "user_id" in (a.get("identity") or []) for a in anomalies),
    }


# --- all y clean --------------------------------------------------------------------------------


def part_all(ctx: Ctx) -> int:
    codes = [part_preflight(ctx), part_tests(ctx), part_raw(ctx), part_trusted(ctx)]
    return 1 if any(codes) else 0


def part_clean(ctx: Ctx) -> int:
    part = Part(ctx, "clean", "Borrar la caché local (no toca S3)")
    from quality import s3sync

    try:
        cache = s3sync.resolve_cache(getattr(ctx.options, "cache", None))
    except Exception as exc:  # noqa: BLE001
        part.add("L1", "caché", FALLA, str(exc))
        return part.finish()
    if not cache.exists():
        part.add("L1", "caché", OK, f"{cache} no existe")
        return part.finish()
    size = sum(p.stat().st_size for p in cache.rglob("*") if p.is_file())
    if not ctx.ask(f"¿Borrar {cache} ({size / 1e6:.0f} MB; se vuelve a bajar con la parte C)?"):
        part.add("L1", "caché", OMITIDO, "no confirmado")
        return part.finish()
    shutil.rmtree(cache)
    part.add("L1", "caché", OK if not cache.exists() else FALLA, f"borrada {cache} ({size / 1e6:.0f} MB)")
    return part.finish()


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "raw": part_raw,
    "trusted": part_trusted,
    "all": part_all,
    "clean": part_clean,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_5_1.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación (clean)")
    parser.add_argument("--bucket", default=None, help="bucket del lake (si no: TA_LAKE_BUCKET o ta-{ENV}-{cuenta}-lake)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--cache", default=None, help="caché local (si no, TA_QUALITY_CACHE o ~/.cache/trendanalyzer/quality)")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, now: Callable[[], datetime] | None = None,
         evidence_path: Path | None = None, services_runner: Any = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              now=now or (lambda: datetime.now(timezone.utc)), evidence_path=evidence_path or EVIDENCE,
              services_runner=services_runner)
    printer(f"Lote 5.1 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido (este lote no deja nada encendido)")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
