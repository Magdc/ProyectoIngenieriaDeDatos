"""Validación automática del Lote 5.3 (HU-034 catálogo, HU-035 linaje, HU-036 gobernanza): ``docs/validation/validation-5-3.md``.

    python project/scripts/validate_5_3.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: catálogo de las DDL, diccionario, linaje y gobernanza, sin
nube), ``terraform`` (C: descripciones y comentarios de las 11 tablas de Glue; ``--apply``), ``glue`` (D: lo que quedó
en Glue y lo que ve Athena), ``controls`` (G: los controles de acceso y cifrado que cita ``docs/GOVERNANCE.md``, solo
lectura en AWS y GCP) y ``status`` (solo lectura). Cada parte imprime un paso por línea (OK / AVISO / FALLA / OMITIDO)
y guarda su resumen en ``project/evidencia/validacion_5_3.json``.

El lote no cambia datos, jobs ni el horario: actualizar la descripción de una tabla de Glue no choca con los jobs (no
leen el catálogo, §3.19.1), así que no hace falta apagar el horario. Lo único que cambia la nube es el ``apply`` de C,
que pide confirmación salvo con ``--yes``. Reutiliza de ``ops/validate41.py`` ``Ctx``, ``athena_query``,
``simulate``, ``account_id``, ``aws_outputs``, ``get_schedule`` y ``services_status``; de ``ops/validate32.py``
``Part``, ``Runner``, ``tf_plan``, ``tf_apply`` y ``pytest_counts``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from lakehouse import catalog_spec as cs
from ops import validate41 as v41
from ops.validate32 import (
    AVISO,
    FALLA,
    OK,
    OMITIDO,
    WSL_HINT,
    Part,
    Runner,
    diff_text,
    on_windows,
    parse_json,
    plan_line,
    pytest_counts,
    tf_apply,
    tf_plan,
    tfvars_value,
)

REPO = v41.REPO
PROJECT = v41.PROJECT
SRC = v41.SRC
AWS_ENV = v41.AWS_ENV
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_5_3.json"
DOCS = REPO / "docs"
LINEAGE_MD = DOCS / "LINEAGE.md"
GOVERNANCE_MD = DOCS / "GOVERNANCE.md"
DATABASES = tuple(v41.DATABASES.values())
SCHEDULE = v41.SCHEDULE
HMAC_SECRET = "ta-hmac-key"
DATAFLOW_SA = "sa-dataflow"

LOT_FILES = [
    "contracts/athena/posts_raw.sql",
    "contracts/athena/posts.sql",
    "contracts/athena/curated.sql",
    "src/lakehouse/catalog_spec.py",
    "src/lakehouse/tests/test_lake_catalog_spec.py",
    "infra/aws/modules/glue_catalog/main.tf",
    "infra/aws/modules/glue_catalog/tables.json",
    "src/ops/validate53.py",
    "src/ops/tests/test_ops_validate53.py",
    "scripts/validate_5_3.py",
]
DOC_FILES = ["docs/CATALOG.md", "docs/LINEAGE.md", "docs/GOVERNANCE.md", "docs/validation/validation-5-3.md"]
PREVIOUS_FILES = ["src/ops/validate32.py", "src/ops/validate41.py", "src/ops/services.py"]

# Terraform: solo cambian la descripción y los comentarios de las 11 tablas (ni esquemas ni ubicaciones).
TERRAFORM_TARGETS = ["module.glue_catalog"]
TABLE_ADDR = 'module.glue_catalog.aws_glue_catalog_table.this["{}"]'

# Pruebas esperadas (B): las cuenta docs/validation/validation-5-3.md.
B1_EXPECTED = 132
B2_EXPECTED = 20
B3_EXPECTED = 18
B4_EXPECTED = 3
B5_EXPECTED = 24

# Lo que dice el catálogo hoy (v2.43): si cambia una DDL, cambia aquí y en §3.25.
TODAY = {"tables": 11, "columns": 108, "anonymized": 5,
         "by_class": {"anonimizado": 5, "cuasi_identificador": 8, "texto_libre": 3, "sin_dato_personal": 92}}

#: Las 11 áreas de conocimiento de DAMA-DMBOK 2 tal como las nombra la tabla de ``docs/GOVERNANCE.md`` §2.
DAMA_AREAS = (
    "Gobierno de datos", "Arquitectura de datos", "Modelado y diseño de datos", "Almacenamiento y operaciones de datos",
    "Seguridad de datos", "Integración e interoperabilidad de datos", "Gestión de documentos y contenido",
    "Datos de referencia y maestros", "Data warehousing e inteligencia de negocio", "Metadatos", "Calidad de datos",
)
#: Lo que HU-036 exige que diga la nota (texto que debe aparecer en ``docs/GOVERNANCE.md``, sin distinguir mayúsculas).
GOVERNANCE_MUST = {
    "declaración de datos sintéticos": "todos los datos del proyecto son sintéticos",
    "limitación: plantillas con vocabulario acotado": "vocabulario acotado",
    "limitación: asociación marca → sentimiento artificial": "marca → sentimiento artificial",
    "limitación: métricas de NLP optimistas": "métricas de nlp optimistas",
    "nota de la Ley 1581": "ley 1581 de 2012",
    "anonimización": "**anonimización.**",
    "acceso restringido a raw/": "**acceso restringido a `raw/`.**",
    "minimización": "**minimización.**",
}
_SECRET_PATTERNS = {
    "correo": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9.-]*[A-Za-z]{2,}"),
    "llave de AWS": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "URL prefirmada": re.compile(r"X-Amz-Signature|X-Goog-Signature", re.I),
    "hash de 64 hex (¿un user_hash?)": re.compile(r"\b[0-9a-f]{64}\b"),
}
_ALLOWED_EMAIL = re.compile(r"^(sa-[a-z-]+@|.*@users\.noreply\.github\.com$)")


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "5.3", "hu": ["HU-034", "HU-035", "HU-036"], "parts": {}}


def tables() -> list[cs.TableSpec]:
    return cs.load_contract_tables()


def bucket_name(ctx: Ctx) -> str:
    if getattr(ctx.options, "bucket", None):
        return ctx.options.bucket
    return f"ta-dev-{v41.account_id(ctx)}-lake"


# --- utilidades puras: documentos -----------------------------------------------------------------


def lineage_columns(text: str) -> dict[str, set[str]]:
    """Columnas que ``LINEAGE.md`` §4 lista por tabla: los nombres entre comillas invertidas de la primera celda de
    cada fila de la tabla que sigue a un título ``### `base.tabla```."""
    out: dict[str, set[str]] = {}
    current = None
    for line in text.splitlines():
        m = re.match(r"^#{2,4}\s+`(ta_[a-z]+\.[a-z0-9_]+)`\s*$", line)
        if m:
            current = m.group(1)
            out.setdefault(current, set())
            continue
        if line.startswith("## "):
            current = None
            continue
        if current and line.startswith("|") and not re.match(r"^\|\s*-", line):
            first = line.split("|")[1]
            out[current] |= set(re.findall(r"`([a-z0-9_]+)`", first))
    return out


def lineage_problems(text: str, specs: list[cs.TableSpec]) -> list[str]:
    found = lineage_columns(text)
    out = []
    for t in specs:
        if t.full_name not in found:
            out.append(f"{t.full_name}: sin sección en el linaje por columna")
            continue
        missing = [c.name for c in cs.all_columns(t) if c.name not in found[t.full_name]]
        if missing:
            out.append(f"{t.full_name}: faltan {', '.join(missing)}")
    out += [f"{n}: sección de una tabla que no está en el catálogo" for n in sorted(set(found) - {t.full_name for t in specs})]
    return out


def doc_paths(text: str) -> list[str]:
    """Rutas ``project/…`` o ``docs/…`` citadas entre comillas invertidas (sin comodines ni marcadores)."""
    paths = []
    for token in re.findall(r"`((?:project|docs)/[^`\s]+)`", text):
        if any(ch in token for ch in "{}*<>…"):
            continue
        paths.append(token.rstrip("/"))
    return sorted(set(paths))


def path_problems(texts: dict[str, str], root: Path = REPO) -> tuple[list[str], int]:
    out, n = [], 0
    for name, text in texts.items():
        for p in doc_paths(text):
            n += 1
            if not (root / p).exists():
                out.append(f"{name}: no existe {p}")
    return out, n


def governance_problems(text: str) -> list[str]:
    low = text.lower()
    rows = {line.split("|")[1].strip() for line in text.splitlines() if line.startswith("|") and line.count("|") > 2}
    out = [f"DAMA-DMBOK: falta el área «{a}» en la tabla" for a in DAMA_AREAS if a not in rows]
    out += [f"falta {label}" for label, needle in GOVERNANCE_MUST.items() if needle not in low]
    return out


def leak_problems(texts: dict[str, str]) -> list[str]:
    out = []
    for name, text in texts.items():
        for label, rx in _SECRET_PATTERNS.items():
            for m in rx.finditer(text):
                if label == "correo" and _ALLOWED_EMAIL.match(m.group(0)):
                    continue
                out.append(f"{name}: {label} ({m.group(0)[:12]}…)")
    return out


# --- utilidades puras: Terraform ------------------------------------------------------------------


def _strip_docs(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _strip_docs(v) for k, v in value.items() if k not in ("description", "comment")}
    if isinstance(value, list):
        return [_strip_docs(v) for v in value]
    return value


def plan_only_descriptions(plan: dict[str, Any]) -> list[str]:
    """Recursos a cambiar cuyo cambio no es solo ``description``/``comment`` (vacío = solo documentación)."""
    out = []
    for rc in (plan or {}).get("resource_changes", []) or []:
        change = rc.get("change") or {}
        if change.get("actions") != ["update"]:
            continue
        if _strip_docs(change.get("before") or {}) != _strip_docs(change.get("after") or {}):
            out.append(rc.get("address", "?"))
    return out


# --- utilidades puras: controles --------------------------------------------------------------------


def public_access_problems(data: dict[str, Any]) -> list[str]:
    cfg = (data or {}).get("PublicAccessBlockConfiguration") or {}
    flags = ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")
    return [f"{f} = {cfg.get(f)}" for f in flags if cfg.get(f) is not True]


def encryption_problems(data: dict[str, Any]) -> list[str]:
    rules = ((data or {}).get("ServerSideEncryptionConfiguration") or {}).get("Rules") or []
    if not rules:
        return ["sin cifrado por defecto"]
    rule = rules[0]
    default = rule.get("ApplyServerSideEncryptionByDefault") or {}
    out = []
    if default.get("SSEAlgorithm") != "aws:kms":
        out.append(f"SSEAlgorithm = {default.get('SSEAlgorithm')}")
    if not default.get("KMSMasterKeyID"):
        out.append("sin llave KMS")
    if rule.get("BucketKeyEnabled") is not True:
        out.append("sin bucket key")
    return out


def denies_insecure_transport(policy_text: str) -> bool:
    try:
        policy = json.loads(policy_text)
    except (TypeError, ValueError):
        return False
    stmts = policy.get("Statement") or []
    for st in stmts if isinstance(stmts, list) else [stmts]:
        cond = (st.get("Condition") or {}).get("Bool") or {}
        if st.get("Effect") == "Deny" and str(cond.get("aws:SecureTransport")).lower() == "false":
            return True
    return False


def raw_access_cases(account: str, bucket: str) -> list[tuple[str, list[tuple[str, str, str, list, bool]]]]:
    """Quién puede leer y escribir cada capa (GOVERNANCE.md §4): (rol, casos para ``v41.simulate``)."""
    b = f"arn:aws:s3:::{bucket}"
    stream = f"{b}/raw/origin=stream/source=reddit/dt=2026-10-22/hr=10/part-20261022T100000Z-p000.parquet"
    hist = f"{b}/raw/origin=historical/source=reddit/dt=2026-05-01/hr=03/part-historical-s3101-20260501T03.parquet"
    trusted = f"{b}/trusted/posts/source=reddit/dt=2026-10-22/part-00000.parquet"
    curated = f"{b}/curated/fact_posts/dt=2026-10-22/part-00000.parquet"
    return [
        ("ta-gcp-exporter-role", [
            ("escribe raw/origin=stream/", "s3:PutObject", stream, [], True),
            ("lee raw/origin=stream/", "s3:GetObject", stream, [], False),
            ("borra raw/origin=stream/", "s3:DeleteObject", stream, [], False),
            ("escribe raw/origin=historical/", "s3:PutObject", hist, [], False),
            ("lee curated/", "s3:GetObject", curated, [], False),
        ]),
        ("ta-historical-loader", [
            ("escribe raw/origin=historical/", "s3:PutObject", hist, [], True),
            ("lee raw/origin=historical/", "s3:GetObject", hist, [], False),
            ("borra raw/origin=historical/", "s3:DeleteObject", hist, [], False),
            ("escribe raw/origin=stream/", "s3:PutObject", stream, [], False),
        ]),
        ("ta-glue-role", [
            ("lee raw/", "s3:GetObject", stream, [], True),
            ("escribe raw/", "s3:PutObject", stream, [], False),
            ("borra raw/", "s3:DeleteObject", hist, [], False),
            ("escribe trusted/", "s3:PutObject", trusted, [], True),
        ]),
        ("ta-quicksight-role", [
            ("lee curated/", "s3:GetObject", curated, [], True),
            ("lee raw/", "s3:GetObject", stream, [], False),
            ("lee trusted/", "s3:GetObject", trusted, [], False),
        ]),
    ]


def _find(data: Any, *names: str) -> Any:
    """Primer valor de una clave (recursivo; acepta los nombres de ``gcloud storage`` y los de la API JSON)."""
    if isinstance(data, dict):
        for name in names:
            if name in data:
                return data[name]
        for value in data.values():
            got = _find(value, *names)
            if got is not None:
                return got
    if isinstance(data, list):
        for value in data:
            got = _find(value, *names)
            if got is not None:
                return got
    return None


def gcs_bucket_problems(desc: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    """Acceso uniforme, prevención de acceso público y expiración de ``raw/`` y ``dlq/`` a 30 días."""
    uniform = _find(desc, "uniform_bucket_level_access", "uniformBucketLevelAccess")
    if isinstance(uniform, dict):
        uniform = uniform.get("enabled")
    pap = _find(desc, "public_access_prevention", "publicAccessPrevention")
    rules = _find(desc, "lifecycle_config", "lifecycle") or {}
    rules = rules.get("rule") if isinstance(rules, dict) else rules
    expires: dict[str, Any] = {}
    for rule in rules or []:
        action = (rule.get("action") or {}).get("type")
        cond = rule.get("condition") or {}
        for prefix in cond.get("matchesPrefix") or cond.get("matches_prefix") or []:
            if action == "Delete":
                expires[prefix] = cond.get("age")
    out = []
    if uniform is not True:
        out.append(f"acceso uniforme = {uniform}")
    if str(pap).lower() != "enforced":
        out.append(f"prevención de acceso público = {pap}")
    for prefix in ("raw/", "dlq/"):
        if expires.get(prefix) != 30:
            out.append(f"{prefix} expira a {expires.get(prefix)} días (esperado 30)")
    return out, {"uniform": uniform, "public_access_prevention": pap, "expires": expires}


def mask_member(member: str) -> str:
    """Un miembro de IAM sin datos personales: las service accounts por su nombre corto; usuarios y grupos ocultos."""
    kind, _, who = member.partition(":")
    if kind == "serviceAccount":
        return f"serviceAccount:{who.split('@', 1)[0]}"
    if kind in ("user", "group", "domain", "principal", "principalSet"):
        return f"{kind}:***"
    return member


def iam_summary(policy: dict[str, Any]) -> tuple[list[str], dict[str, list[str]]]:
    """Miembros públicos (``allUsers``/``allAuthenticatedUsers``) y rol → miembros enmascarados."""
    public, roles = [], {}
    for b in (policy or {}).get("bindings") or []:
        members = b.get("members") or []
        public += [f"{b.get('role')}: {m}" for m in members if m in ("allUsers", "allAuthenticatedUsers")]
        roles[b.get("role", "?")] = sorted(mask_member(m) for m in members)
    return public, roles


def athena_comment_problems(rows: list[dict[str, Any]], specs: list[cs.TableSpec]) -> list[str]:
    """``information_schema.columns`` de Athena frente a los comentarios de las DDL."""
    got = {(r.get("table_schema"), r.get("table_name"), r.get("column_name")): r.get("comment") or "" for r in rows}
    out = []
    for t in specs:
        for c in cs.all_columns(t):
            comment = got.get((t.database, t.name, c.name))
            if comment is None:
                out.append(f"{t.full_name}.{c.name}: no aparece en information_schema.columns")
            elif comment != c.comment[:cs.COMMENT_MAX]:
                out.append(f"{t.full_name}.{c.name}: comentario distinto en Athena")
    return out


# --- Parte A · preflight --------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código y de lo encendido")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    missing = []
    for mod in ("boto3", "pytest"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    part.add("A1", "Python y dependencias", FALLA if missing or (py.major, py.minor) != (3, 11) else OK,
             f"Python {py.major}.{py.minor}.{py.micro}" + (f"; faltan: {', '.join(missing)}" if missing else "; boto3 y pytest"))
    absent = [f for f in LOT_FILES if not (PROJECT / f).exists()] + [f for f in DOC_FILES if not (REPO / f).exists()]
    part.add("A2", f"los {len(LOT_FILES) + len(DOC_FILES)} archivos del lote", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(LOT_FILES)} de project/ y {len(DOC_FILES)} de docs/")
    absent = [f for f in PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A3", "archivos de lotes anteriores que usa", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else f"{len(PREVIOUS_FILES)} presentes")
    try:
        problems = cs.check() + cs.check_dictionary() + cs.catalog_problems(tables())
    except Exception as exc:  # noqa: BLE001
        problems = [f"{type(exc).__name__}: {exc}"]
    part.add("A4", "tables.json y docs/CATALOG.md al día; catálogo completo", FALLA if problems else OK,
             "\n".join(problems[:12]) if problems else "las 11 tablas descritas y cada columna con su clase de privacidad")
    tools = {t: ctx.runner.which(t) for t in ("terraform", "aws", "gcloud", "bash")}
    need = [t for t in tools if not tools[t]]
    part.add("A5", "herramientas", (AVISO if need == ["gcloud"] else FALLA) if need else OK,
             ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else "")
             + (" (G5–G7 quedan en AVISO)" if "gcloud" in need else ""))
    state, sched = v41.get_schedule(ctx)
    part.add("A6", f"horario {SCHEDULE}", OK if state in ("ENABLED", "DISABLED") else AVISO,
             f"{state or sched.get('error')} (ENABLED es el estado normal; este lote no necesita apagarlo: los jobs no leen el "
             "catálogo y el apply solo cambia descripciones)", schedule=state)
    lines = v41.services_status(ctx)
    charging = [ln for ln in lines if "ENCENDIDO" in ln or "(cobra)" in ln]
    part.add("A7", "GCP y local (services.status)", AVISO if charging else OK,
             "\n".join(lines) + ("\neste lote no los necesita: python project/scripts/off.py" if charging else ""))
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/lakehouse (catálogo con descripciones)", ["src/lakehouse", "-q", "-rs"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate53.py", ["src/ops/tests/test_ops_validate53.py", "-q"], B2_EXPECTED),
        ("B3", "pytest contracts/tests (las DDL con comentarios)", ["contracts/tests", "-q"], B3_EXPECTED),
        ("B4", "pytest src/dq en ta-streaming (identidad de HU-032 sobre el catálogo)", ["src/dq", "-q"], B4_EXPECTED),
        ("B5", "regresión: test_ops_validate51.py (su parser de DDL)", ["src/ops/tests/test_ops_validate51.py", "-q"], B5_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · catálogo, linaje y gobernanza en local ---------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Catálogo, diccionario, linaje y gobernanza (sin nube)")
    specs = tables()
    problems = cs.catalog_problems(specs)
    s = cs.privacy_summary(specs)
    diffs = [f"{k}: {s.get(k) if k != 'anonymized' else len(s['anonymized'])} (en v2.43: {v})" for k, v in TODAY.items()
             if (len(s["anonymized"]) if k == "anonymized" else s.get(k)) != v]
    part.add("L1", "catálogo de las DDL (HU-034)", FALLA if problems else (AVISO if diffs else OK),
             "\n".join(problems[:12]) if problems else
             f"{s['tables']} tablas con descripción; {s['columns']} columnas (con particiones) con descripción y clase: "
             + ", ".join(f"{k} {v}" for k, v in s["by_class"].items()) + f"; anonimizadas: {', '.join(s['anonymized'])}"
             + ("\ndistinto de v2.43: " + "; ".join(diffs) if diffs else ""), summary=s)
    sync = cs.check() + cs.check_dictionary()
    part.add("L2", "tables.json (Terraform) y docs/CATALOG.md = las DDL", FALLA if sync else OK,
             "\n".join(sync) + "\nregenere: cd project/src && python -m lakehouse.catalog_spec --write" if sync
             else "los dos generados al día (python -m lakehouse.catalog_spec --check)")
    try:
        lineage = LINEAGE_MD.read_text(encoding="utf-8")
        governance = GOVERNANCE_MD.read_text(encoding="utf-8")
        catalog_md = cs.DICTIONARY_MD.read_text(encoding="utf-8")
    except OSError as exc:
        part.add("L3", "documentos del lote", FALLA, str(exc))
        return part.finish()
    lp = lineage_problems(lineage, specs)
    has_diagram = "```mermaid" in lineage and "flowchart" in lineage
    part.add("L3", "linaje (HU-035): diagrama y cada columna del catálogo", FALLA if lp or not has_diagram else OK,
             "\n".join(lp[:12]) + ("" if has_diagram else "\nfalta el diagrama Mermaid") if lp or not has_diagram else
             f"diagrama Mermaid de extremo a extremo y las {s['columns']} columnas de las {s['tables']} tablas en el linaje por columna")
    texts = {"LINEAGE.md": lineage, "GOVERNANCE.md": governance, "CATALOG.md": catalog_md}
    missing, n_paths = path_problems(texts)
    part.add("L4", "rutas citadas en los documentos existen", FALLA if missing else OK,
             "\n".join(missing[:12]) if missing else f"{n_paths} rutas project/… y docs/… verificadas")
    gp = governance_problems(governance)
    part.add("L5", "gobernanza (HU-036): DAMA-DMBOK, Ley 1581 y datos sintéticos", FALLA if gp else OK,
             "\n".join(gp) if gp else f"las {len(DAMA_AREAS)} áreas de DAMA-DMBOK, anonimización, acceso restringido a raw/, "
             "minimización y la declaración de datos sintéticos con sus tres limitaciones")
    leaks = leak_problems(texts)
    part.add("L6", "documentos sin datos personales ni secretos (repo público)", FALLA if leaks else OK,
             "\n".join(leaks[:12]) if leaks else "sin correos, llaves de AWS, URLs prefirmadas ni hashes de 64 hex")
    return part.finish()


# --- Parte C · Terraform -------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (descripciones y comentarios del catálogo de Glue)")
    apply = bool(getattr(ctx.options, "apply", False))
    res = ctx.aws("sts", "get-caller-identity")
    ident = parse_json(res.out) or {}
    if not res.ok:
        part.add("C0", "identidad de AWS", FALLA, "aws sts get-caller-identity falló: " + res.tail(3))
        return part.finish()
    tf_account = tfvars_value(AWS_ENV / "terraform.tfvars", "aws_account_id")
    if tf_account and ident.get("Account") != tf_account:
        part.add("C0", "identidad de AWS", FALLA, f"el perfil apunta a {ident.get('Account')} y terraform.tfvars a {tf_account}")
        return part.finish()
    arn = ident.get("Arn", "")
    part.add("C0", "identidad de AWS", AVISO if arn.endswith(":root") else OK,
             f"{arn} (cuenta raíz: sirve para aplicar)" if arn.endswith(":root") else arn)
    missing = [n for n in ("backend.hcl", "terraform.tfvars") if not (AWS_ENV / n).exists()]
    part.add("C0b", "backend.hcl y terraform.tfvars de infra/aws", FALLA if missing else OK,
             "faltan: " + ", ".join(missing) if missing else "presentes (no se versionan)")
    if missing:
        return part.finish()
    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()
    res = ctx.tf(AWS_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
    part.add("C2a", "terraform init", OK if res.ok else FALLA, res.tail(3))
    if not res.ok:
        return part.finish()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / "tfplan_5_3"
    title = "plan (-target=module.glue_catalog)"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", title, FALLA, res.err or res.tail(8))
        return part.finish()
    expected = {TABLE_ADDR.format(t.full_name) for t in tables()}
    updates = set(summary["update"])
    extra = (updates - expected) | set(summary["create"] + summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", title, FALLA, f"{plan_line(summary)}; no aplique: cambia {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    show = ctx.tf(AWS_ENV, "show", "-json", str(plan_path), timeout=600)
    not_docs = plan_only_descriptions(parse_json(show.out) or {}) if show.ok else ["terraform show -json falló"]
    if not_docs:
        part.add("C2", title, FALLA, f"cambia algo más que descripción o comentarios en: {not_docs}" + diff_text(summary, not_docs),
                 **summary)
        return part.finish()
    part.add("C2", title, OK, plan_line(summary) + (" (ya aplicado)" if not updates else
             f" (esperado: las {len(expected)} tablas, solo descripción y comentarios; nada a crear ni a destruir)"), **summary)
    applied = not updates
    if updates:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
        elif ctx.ask(f"¿Aplicar el plan de infra/aws (0 a crear, {len(updates)} a cambiar, 0 a destruir; solo descripciones)?"):
            res = tf_apply(ctx, AWS_ENV, plan_path)
            applied = res.ok and f"Apply complete! Resources: 0 added, {len(updates)} changed, 0 destroyed." in res.out
            part.add("C3", "apply", OK if applied else FALLA, res.tail(4) if res.ok else res.err)
            if not res.ok:
                return part.finish()
        else:
            part.add("C3", "apply", AVISO, "no confirmado")
    else:
        part.add("C3", "apply", OK, "nada que aplicar")
    if applied:
        res, again = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, EVIDENCE_DIR / "tfplan_5_3_check")
        if again is None:
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA, res.err or res.tail(8))
        else:
            changes = again["create"] + again["update"] + again["delete"] + again["replace"]
            part.add("C3b", "plan tras aplicar (sin deriva)", FALLA if changes else OK,
                     (f"{plan_line(again)}: {sorted(changes)}" + diff_text(again)) if changes else "No changes", **again)
    else:
        part.add("C3b", "plan tras aplicar (sin deriva)", OMITIDO, "pendiente del apply")
    return part.finish()


# --- Parte D · Glue y Athena ------------------------------------------------------------------------


def glue_tables(ctx: Ctx) -> tuple[dict[str, dict[str, Any]], list[str]]:
    found, errors = {}, []
    for db in DATABASES:
        res = ctx.aws("glue", "get-tables", "--database-name", db, timeout=300)
        if not res.ok:
            errors.append(f"{db}: {res.tail(2)}")
            continue
        for t in (parse_json(res.out) or {}).get("TableList") or []:
            found[f"{db}.{t.get('Name')}"] = t
    return found, errors


COMMENTS_SQL = ("SELECT table_schema, table_name, column_name, comment FROM information_schema.columns "
                "WHERE table_schema IN ('ta_raw', 'ta_trusted', 'ta_curated')")


def part_glue(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Descripciones en Glue y lo que ve Athena (solo lectura)")
    specs = tables()
    bucket = bucket_name(ctx)
    found, errors = glue_tables(ctx)
    absent = [t.full_name for t in specs if t.full_name not in found]
    schema = [p for t in specs if t.full_name in found for p in cs.glue_table_problems(t, found[t.full_name], bucket)]
    part.add("D1", "las 11 tablas en Glue con el esquema de las DDL", FALLA if errors or absent or schema else OK,
             "\n".join(errors + [f"falta {a}" for a in absent] + schema[:10]) if errors or absent or schema else
             f"{len(specs)} tablas en {', '.join(DATABASES)}: columnas, tipos, particiones, ubicación y proyección = DDL")
    docs = [p for t in specs if t.full_name in found for p in cs.glue_description_problems(t, found[t.full_name], bucket)]
    n_cols = sum(len(cs.all_columns(t)) for t in specs)
    part.add("D2", "descripción de cada tabla y comentario de cada columna = DDL (HU-034)", FALLA if docs or absent else OK,
             "\n".join(docs[:12]) + (f"\n… y {len(docs) - 12} más (¿falta aplicar C?)" if len(docs) > 12 else "") if docs else
             f"{len(specs)} descripciones y {n_cols} comentarios (con clase de privacidad) iguales a las DDL", differences=len(docs))
    part.note("D3: consultando information_schema.columns en Athena (≈ 10 s, 0 bytes de datos)…")
    q = v41.athena_query(ctx, COMMENTS_SQL, "ta_raw", timeout_s=300)
    if q["state"] != "SUCCEEDED":
        part.add("D3", "comentarios visibles en Athena (information_schema.columns)", AVISO,
                 f"{q['state']}: {q.get('reason')} (D2 ya compara Glue; esta consulta es la vista del analista)",
                 query_id=q.get("id"))
    else:
        cp = athena_comment_problems(q["rows"], specs)
        tagged = sum(1 for r in q["rows"] if (r.get("comment") or "").startswith("["))
        part.add("D3", "comentarios visibles en Athena (information_schema.columns)", FALLA if cp else OK,
                 "\n".join(cp[:12]) if cp else f"{len(q['rows'])} columnas, {tagged} con comentario y clase de privacidad · "
                 f"{q['scanned_bytes']} bytes escaneados, {q['engine_ms']} ms de motor",
                 query_id=q.get("id"), rows=len(q["rows"]), scanned_bytes=q["scanned_bytes"], engine_ms=q["engine_ms"])
    return part.finish()


# --- Parte G · controles de GOVERNANCE.md ---------------------------------------------------------------


def part_controls(ctx: Ctx) -> int:
    part = Part(ctx, "G", "Controles de acceso y cifrado de GOVERNANCE.md (solo lectura en AWS y GCP)")
    bucket = bucket_name(ctx)
    res = ctx.aws("s3api", "get-public-access-block", "--bucket", bucket)
    p = public_access_problems(parse_json(res.out) or {}) if res.ok else [res.tail(2)]
    part.add("G1", f"S3 {bucket}: bloqueo total de acceso público", FALLA if p else OK,
             "; ".join(p) if p else "los 4 bloqueos activos")
    res = ctx.aws("s3api", "get-bucket-encryption", "--bucket", bucket)
    p = encryption_problems(parse_json(res.out) or {}) if res.ok else [res.tail(2)]
    part.add("G2", "S3: cifrado por defecto SSE-KMS con bucket key", FALLA if p else OK, "; ".join(p) if p else "aws:kms con llave y bucket key")
    res = ctx.aws("s3api", "get-bucket-policy", "--bucket", bucket)
    tls = res.ok and denies_insecure_transport((parse_json(res.out) or {}).get("Policy", ""))
    part.add("G3", "S3: la política niega todo sin TLS", OK if tls else FALLA,
             "Deny con aws:SecureTransport = false" if tls else (res.tail(2) if not res.ok else "no hay un Deny sin TLS"))
    account = v41.account_id(ctx)
    bad, records = [], []
    for role, cases in raw_access_cases(account, bucket):
        b, r = v41.simulate(ctx, f"arn:aws:iam::{account}:role/{role}", cases)
        bad += [f"{role} · {x}" for x in b]
        records += [{"role": role, **x} for x in r]
    part.add("G4", "IAM simulado: quién lee y escribe raw/, trusted/ y curated/", FALLA if bad else OK,
             "\n".join(bad) if bad else f"{len(records)} casos como se esperaba: raw/ lo escriben solo sus dos productores (sin leer "
             "ni borrar), Glue lo lee sin escribirlo y QuickSight solo lee curated/", cases=records)
    if not ctx.runner.which("gcloud"):
        for sid, title in (("G5", "GCS del lake"), ("G6", "GCS del lake: miembros públicos"), ("G7", f"Secret Manager {HMAC_SECRET}")):
            part.add(sid, title, AVISO, "sin gcloud en este entorno")
        return part.finish()
    gcs = f"gs://{ctx.project}-ta-lake"
    res = ctx.run(["gcloud", "storage", "buckets", "describe", gcs, "--format=json", f"--project={ctx.project}"], timeout=120)
    if res.ok:
        p, seen = gcs_bucket_problems(parse_json(res.out) or {})
        part.add("G5", f"GCS {ctx.project}-ta-lake: acceso uniforme, sin acceso público, raw/ y dlq/ a 30 días",
                 FALLA if p else OK, "; ".join(p) if p else f"uniforme, enforced, expiran {seen['expires']}", **seen)
    else:
        part.add("G5", "GCS del lake", AVISO, res.tail(3))
    res = ctx.run(["gcloud", "storage", "buckets", "get-iam-policy", gcs, "--format=json", f"--project={ctx.project}"], timeout=120)
    if res.ok:
        public, roles = iam_summary(parse_json(res.out) or {})
        part.add("G6", "GCS del lake: sin miembros públicos", FALLA if public else OK,
                 "; ".join(public) if public else "ningún allUsers ni allAuthenticatedUsers · "
                 + "; ".join(f"{r}: {', '.join(m)}" for r, m in sorted(roles.items())), roles=roles)
    else:
        part.add("G6", "GCS del lake: miembros públicos", AVISO, res.tail(3))
    res = ctx.run(["gcloud", "secrets", "get-iam-policy", HMAC_SECRET, "--format=json", f"--project={ctx.project}"], timeout=120)
    if res.ok:
        public, roles = iam_summary(parse_json(res.out) or {})
        accessors = roles.get("roles/secretmanager.secretAccessor", [])
        good = not public and accessors == [f"serviceAccount:{DATAFLOW_SA}"]
        part.add("G7", f"Secret Manager {HMAC_SECRET}: solo {DATAFLOW_SA} lo lee", OK if good else FALLA,
                 f"secretAccessor: {', '.join(accessors) or 'nadie'}; otros roles en el secreto: "
                 + (", ".join(r for r in roles if r != "roles/secretmanager.secretAccessor") or "ninguno")
                 + " (los dueños del proyecto también pueden, por herencia)", roles=roles)
    else:
        part.add("G7", f"Secret Manager {HMAC_SECRET}", AVISO, res.tail(3))
    return part.finish()


# --- status ----------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado del catálogo de Glue (solo lectura)")
    specs = tables()
    found, errors = glue_tables(ctx)
    bucket = bucket_name(ctx)
    described = [t.full_name for t in specs if t.full_name in found and not cs.glue_description_problems(t, found[t.full_name], bucket)]
    part.add("S1", "tablas de Glue con las descripciones de las DDL", OK if len(described) == len(specs) else AVISO,
             "; ".join(errors) if errors else f"{len(described)} de {len(specs)}"
             + ("" if len(described) == len(specs) else " (falta aplicar: python project/scripts/validate_5_3.py terraform --apply)"))
    state, sched = v41.get_schedule(ctx)
    part.add("S2", f"horario {SCHEDULE}", OK if state == "ENABLED" else AVISO,
             f"{state or sched.get('error')} (estado normal: ENABLED; este lote no lo toca)", schedule=state)
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------


PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "terraform": part_terraform,
    "glue": part_glue,
    "controls": part_controls,
    "status": part_status,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_5_3.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--project", default=None, help="proyecto de GCP (G y el estado de status.py)")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--bucket", default=None, help="bucket del lake (si no, ta-dev-{cuenta}-lake)")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan (pide confirmación)")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 5.3 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido (este lote no deja nada encendido)")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
