"""Validación automática del Lote 6.1 (HU-037 y HU-038, tablero de QuickSight): ``docs/validation/validation-6-1.md``.

    python project/scripts/validate_6_1.py <parte> [opciones]      (desde la raíz del repo, en WSL)

Partes: ``preflight`` (A), ``tests`` (B), ``local`` (L: definiciones y comprobaciones sin nube), ``terraform`` (C: los
roles IAM de acceso; ``--apply`` para aplicar), ``deploy`` (D: ``scripts/quicksight.py deploy``), ``checks`` (Q:
SPICE = Athena, horas de Bogotá, columnas, permisos, roles y sesión del lector), ``latency`` (E: las mediciones del
tablero en el navegador, ``--samples``, y la reducción del tiempo de informes), ``status`` y ``unsubscribe`` (U: borra
la suscripción de QuickSight; irreversible, pide ``--confirm-unsubscribe``). Cada parte imprime un paso por línea
(OK / AVISO / FALLA / OMITIDO) y guarda su resumen en ``project/evidencia/validacion_6_1.json``.

Reutiliza de ``ops/validate41.py`` ``Ctx``, ``athena_query`` y ``get_schedule``, de ``ops/validate53.py``
``leak_problems`` y de ``ops/validate32.py`` ``Part``, ``Runner``, ``tf_plan``, ``tf_apply`` y ``pytest_counts``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from ops import validate41 as v41
from ops import validate53 as v53
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
from serving import quicksight_spec as qs

REPO = v41.REPO
PROJECT = v41.PROJECT
EVIDENCE_DIR = PROJECT / "evidencia"
EVIDENCE = EVIDENCE_DIR / "validacion_6_1.json"
AWS_ENV = v41.AWS_ENV
TERRAFORM_TARGETS = ["module.quicksight_access"]
ROLE_ADDR = 'module.quicksight_access.aws_iam_role.this["{}"]'
POLICY_ADDR = 'module.quicksight_access.aws_iam_role_policy.this["{}"]'
SELF_REGISTER = {"ta-quicksight-autor": "quicksight:CreateUser", "ta-quicksight-lector": "quicksight:CreateReader"}
DEMO_USERS = {key: f"{role}/{qs.DEMO_SESSION}" for key, role in qs.ACCESS_ROLES.items()}
#: Prueba gratis de QuickSight: 30 días desde la suscripción (27-oct-2026 18:46 UTC).
SUBSCRIBED_AT = datetime(2026, 10, 27, 18, 46, tzinfo=timezone.utc)
TRIAL_DAYS = 30
#: Precio por usuario al terminar la prueba (USD/mes, página de precios de QuickSight, oct-2026).
USD_PER_USER = {"ADMIN": 24, "AUTHOR": 24, "READER": 3, "ADMIN_PRO": 40, "AUTHOR_PRO": 40, "READER_PRO": 20}
KPI_SECONDS = 3.0

#: HU-037, «reducción ≥ 50 % del tiempo de elaboración de informes»: estimación por pasos (minutos) de un informe
#: semanal de volumen, sentimiento y emociones por marca y red, antes (SQL en la consola de Athena + hoja de cálculo)
#: y con el tablero. Las consultas en Athena se midieron (E8 del 4.3: 1,0–2,1 s cada una); lo demás es trabajo humano.
REPORT_MINUTES = {
    "manual": [
        ("escribir o adaptar 5 consultas SQL en Athena (volumen, sentimiento, emociones, redes, hora del día)", 25),
        ("correrlas, revisar errores y descargar 5 CSV", 10),
        ("convertir UTC a hora de Bogotá y unir los CSV en la hoja de cálculo", 15),
        ("armar 6 gráficos (líneas por día, barras por marca y red, mapa por hora)", 30),
        ("comparar Postobón contra Pepsi y Sprite y redactar el resumen", 15),
    ],
    "tablero": [
        ("abrir el tablero y elegir periodo, marca, red y origen", 2),
        ("leer los 4 indicadores y los 7 gráficos de «Tendencias» y la hoja «Relaciones»", 8),
        ("exportar a CSV lo que haga falta y redactar el resumen", 15),
    ],
}

LOT_FILES = [
    "src/serving/__init__.py",
    "src/serving/quicksight_spec.py",
    "src/serving/deploy.py",
    "src/serving/cli.py",
    "src/serving/sql/publicaciones.sql",
    "src/serving/sql/coocurrencia_hashtags.sql",
    "src/serving/sql/marcas_compartidas.sql",
    "src/serving/sql/autores_por_marcas.sql",
    "src/serving/tests/test_quicksight.py",
    "scripts/quicksight.py",
    "infra/aws/modules/quicksight_access/main.tf",
    "infra/aws/environments/dev/serving.tf",
    "src/ops/validate61.py",
    "src/ops/tests/test_ops_validate61.py",
    "scripts/validate_6_1.py",
]
PREVIOUS_FILES = ["src/ops/validate41.py", "src/ops/validate53.py", "src/lakehouse/catalog_spec.py",
                  "infra/aws/modules/quicksight_role/main.tf"]

B1_EXPECTED = 10
B2_EXPECTED = 12
B3_EXPECTED = 132


# --- contexto ---------------------------------------------------------------------------------


@dataclass
class Ctx(v41.Ctx):
    evidence_path: Path = EVIDENCE

    def load_evidence(self) -> dict[str, Any]:
        try:
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"lote": "6.1", "hu": ["HU-037", "HU-038"], "parts": {}}

    def qs(self, *args: str, timeout: float = 180) -> dict[str, Any] | None:
        """``aws quicksight <args> --aws-account-id <cuenta>``; ``None`` si falla (el error queda en ``cache``)."""
        res = self.aws("quicksight", *args, "--aws-account-id", account(self), timeout=timeout)
        if not res.ok:
            self.cache["qs_error"] = v41.aws_error(res) or res.tail(3)
            return None
        return parse_json(res.out) or {}


def account(ctx: Ctx) -> str:
    if "account" not in ctx.cache:
        ctx.cache["account"] = v41.account_id(ctx)
    return ctx.cache["account"]


# --- utilidades puras ---------------------------------------------------------------------------


def trial_end() -> datetime:
    return SUBSCRIBED_AT + timedelta(days=TRIAL_DAYS)


def monthly_cost(users: list[dict[str, Any]]) -> int:
    return sum(USD_PER_USER.get(str(u.get("Role")), 0) for u in users)


def report_reduction() -> dict[str, Any]:
    manual = sum(m for _, m in REPORT_MINUTES["manual"])
    board = sum(m for _, m in REPORT_MINUTES["tablero"])
    return {"manual_min": manual, "tablero_min": board, "reduction_pct": round(100 * (manual - board) / manual, 1)}


def parse_samples(text: str | None) -> list[tuple[str, float]]:
    """``"restablecer=2.10,hoja_relaciones=1.65"`` → ``[("restablecer", 2.1), …]``; acepta coma decimal con ``;``."""
    out = []
    for chunk in (text or "").replace(";", ",").split(","):
        if "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        try:
            out.append((name.strip(), float(value.strip())))
        except ValueError:
            continue
    return out


def events(text: str) -> list[dict[str, Any]]:
    return [e for e in (parse_json(ln) for ln in (text or "").splitlines() if ln.strip().startswith("{")) if isinstance(e, dict)]


def last_event(evs: list[dict[str, Any]], name: str) -> dict[str, Any]:
    found = [e for e in evs if e.get("event") == name]
    return found[-1] if found else {}


def permission_problems(perms: list[dict[str, Any]], account_id: str, admins: list[str]) -> list[str]:
    """Permisos del tablero: autores y administradores dueños, lectores solo ven, nadie más."""
    want = {qs.principal_arn(account_id, "group", qs.GROUPS["autores"]): set(qs.DASHBOARD_OWNER),
            qs.principal_arn(account_id, "group", qs.GROUPS["lectores"]): set(qs.DASHBOARD_VIEWER)}
    want.update({a: set(qs.DASHBOARD_OWNER) for a in admins})
    got = {p.get("Principal"): set(p.get("Actions") or []) for p in perms}
    out = []
    for principal, actions in got.items():
        if principal not in want:
            out.append(f"principal inesperado {principal}")
        elif actions != want[principal]:
            out.append(f"{principal.rsplit('/', 1)[-1]}: sobran {sorted(actions - want[principal])}, faltan {sorted(want[principal] - actions)}")
    out += [f"falta {p.rsplit('/', 1)[-1]}" for p in want if p not in got]
    return out


def role_problems(role: dict[str, Any], policies: dict[str, Any], account_id: str, action: str) -> list[str]:
    out = []
    trust = role.get("AssumeRolePolicyDocument") or {}
    principals = []
    for st in trust.get("Statement", []):
        p = (st.get("Principal") or {}).get("AWS")
        principals += p if isinstance(p, list) else [p]
    if principals != [f"arn:aws:iam::{account_id}:root"]:
        out.append(f"confianza {principals} (esperado solo la cuenta)")
    actions = set()
    for doc in policies.values():
        for st in doc.get("Statement", []):
            a = st.get("Action")
            actions |= set(a if isinstance(a, list) else [a])
    if actions != {action}:
        out.append(f"acciones {sorted(actions)} (esperado solo {action})")
    return out


# --- Parte A · preflight --------------------------------------------------------------------------


def part_preflight(ctx: Ctx) -> int:
    part = Part(ctx, "A", "Preflight del código, de QuickSight y de lo que el lote reutiliza")
    if on_windows():
        part.add("A0", "entorno (WSL)", FALLA, f"está corriendo en Windows ({sys.platform}): {WSL_HINT}")
    py = sys.version_info
    try:
        import boto3  # noqa: F401
        missing = []
    except Exception:  # noqa: BLE001
        missing = ["boto3"]
    part.add("A1", "Python y boto3", FALLA if missing else OK, f"Python {py.major}.{py.minor}.{py.micro}"
             + (f"; falta boto3" if missing else "; boto3 presente"))
    absent = [f for f in LOT_FILES + PREVIOUS_FILES if not (PROJECT / f).exists()]
    part.add("A2", f"los {len(LOT_FILES)} archivos del lote y {len(PREVIOUS_FILES)} que reutiliza", FALLA if absent else OK,
             "faltan: " + ", ".join(absent) if absent else "presentes")
    tools = {t: ctx.runner.which(t) for t in ("aws", "terraform")}
    need = [t for t in tools if not tools[t]]
    part.add("A3", "herramientas", FALLA if need else OK, ", ".join(t for t in tools if tools[t]) + (f"; faltan: {', '.join(need)}" if need else ""))
    info = (ctx.qs("describe-account-subscription") or {}).get("AccountInfo") or {}
    good = info.get("AccountSubscriptionStatus") == "ACCOUNT_CREATED" and info.get("Edition") == qs.EDITION
    part.add("A4", f"suscripción de QuickSight ({qs.REGION})", OK if good else FALLA,
             f"{info.get('AccountSubscriptionStatus')} · {info.get('Edition')} · {info.get('AuthenticationType')} · "
             f"prueba gratis hasta ≈ {trial_end():%Y-%m-%d}" if info else
             f"sin suscripción ({ctx.cache.get('qs_error')}): se crea una vez a mano (validation-6-1.md, «Preflight»)",
             subscription=info.get("AccountSubscriptionStatus"), edition=info.get("Edition"), trial_end=trial_end().isoformat())
    res = ctx.aws("iam", "get-role", "--role-name", qs.SERVICE_ROLE)
    part.add("A5", f"rol de servicio {qs.SERVICE_ROLE} (Lote 4.1)", OK if res.ok else FALLA,
             "existe" if res.ok else res.tail(2))
    sched, data = v41.get_schedule(ctx)
    part.add("A6", f"horario {v41.SCHEDULE}", OK if sched == "ENABLED" else AVISO,
             f"{sched or data.get('error')} (estado normal: ENABLED; el refresco de SPICE lee lo que deje cada corrida)")
    return part.finish()


# --- Parte B · pruebas ------------------------------------------------------------------------------


def part_tests(ctx: Ctx) -> int:
    part = Part(ctx, "B", "Pruebas")
    plan = [
        ("B1", "pytest src/serving (QuickSight falso)", ["src/serving", "-q"], B1_EXPECTED),
        ("B2", "pytest test_ops_validate61.py", ["src/ops/tests/test_ops_validate61.py", "-q"], B2_EXPECTED),
        ("B3", "regresión: pytest src/lakehouse (catálogo, infraestructura)", ["src/lakehouse", "-q", "-W", "ignore::FutureWarning"],
         B3_EXPECTED),
    ]
    for sid, title, args, expected in plan:
        res = ctx.pytest(*args, "-p", "no:cacheprovider")
        c = pytest_counts(res.out)
        good = res.ok and c["passed"] == expected and c["skipped"] == 0 and c["failed"] == 0
        part.add(sid, title, OK if good else FALLA,
                 f"{c['passed']} passed, {c['skipped']} skipped, {c['failed']} failed (esperado {expected} passed, 0 skipped)"
                 + ("" if good else "\n" + res.tail(10)), **c)
    return part.finish()


# --- Parte L · local -------------------------------------------------------------------------------------


def part_local(ctx: Ctx) -> int:
    part = Part(ctx, "L", "Definiciones del tablero (sin nube)")
    res = ctx.run([sys.executable, "scripts/quicksight.py", "plan"], cwd=PROJECT, timeout=300)
    plan = last_event(events(res.out), "plan")
    if not plan:
        part.add("L1", "quicksight.py plan", FALLA, res.tail(8))
        return part.finish()
    part.add("L1", "quicksight.py plan sin problemas", OK if res.ok and not plan.get("problems") else FALLA,
             "\n".join(plan.get("problems") or []) or f"{len(plan['datasets'])} datasets SPICE · fuente {plan['data_source']} · "
             f"refresco {plan['refresh']}", plan=plan)
    tables = sorted({t for d in plan["datasets"] for t in d["tables"]})
    part.add("L2", "los datasets leen solo ta_curated (G4) y sin columnas de autor",
             OK if all(t.startswith("ta_curated.") for t in tables) else FALLA,
             f"tablas: {', '.join(tables)} · columnas prohibidas: {', '.join(qs.FORBIDDEN_COLUMNS)} (ninguna en la salida)")
    sheets = {s["id"]: s for s in plan["sheets"]}
    needed = {"tendencias": {"linea-volumen", "linea-sentimiento", "barras-emociones", "combo-marcas", "barras-red"},
              "relaciones": {"calor-hashtags", "sankey-marcas", "barras-combinaciones"}}
    missing = {k: sorted(v - set(sheets.get(k, {}).get("visuals", []))) for k, v in needed.items()}
    part.add("L3", "hojas de HU-037 (tendencias) y HU-038 (relaciones)", FALLA if any(missing.values()) else OK,
             f"faltan {missing}" if any(missing.values()) else
             " · ".join(f"{s['name']}: {len(s['visuals'])} visuales" for s in plan["sheets"])
             + f" · filtros: {', '.join(plan['filters'])}")
    tz = [d for d in qs.DATASETS if "America/Bogota" in d.sql]
    part.add("L4", "horas en America/Bogota (§5.12)", OK if [d.key for d in tz] == ["publicaciones"] else FALLA,
             "ta-ds-publicaciones convierte event_time con AT TIME ZONE 'America/Bogota' (hora_bogota, fecha_bogota, "
             "hora_del_dia, dia_semana); las aristas no tienen horas")
    return part.finish()


# --- Parte C · Terraform ----------------------------------------------------------------------------------


def part_terraform(ctx: Ctx) -> int:
    part = Part(ctx, "C", "Terraform (roles IAM de acceso al tablero)")
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
    res = ctx.run(["bash", "infra/validate.sh"], cwd=PROJECT, timeout=1200, stream=True)
    part.add("C1", "bash infra/validate.sh", OK if res.ok else FALLA, res.tail(6))
    if not res.ok:
        return part.finish()
    res = ctx.tf(AWS_ENV, "init", "-input=false", "-backend-config=backend.hcl", timeout=900, stream=True)
    part.add("C2a", "terraform init", OK if res.ok else FALLA, res.tail(3))
    if not res.ok:
        return part.finish()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / "tfplan_6_1"
    title = "plan (-target=module.quicksight_access)"
    res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
    if summary is None:
        part.add("C2", title, FALLA, res.err or res.tail(8))
        return part.finish()
    expected = {f.format(r) for r in SELF_REGISTER for f in (ROLE_ADDR, POLICY_ADDR)}
    creates = set(summary["create"])
    extra = (creates - expected) | set(summary["update"] + summary["delete"] + summary["replace"])
    if extra:
        part.add("C2", title, FALLA, f"{plan_line(summary)}; no aplique: toca {sorted(extra)}" + diff_text(summary, extra), **summary)
        return part.finish()
    part.add("C2", title, OK, plan_line(summary) + (" (ya aplicado)" if not creates else
             f" (esperado: {len(expected)} a crear, los 2 roles y sus políticas; nada a cambiar ni a destruir)"), **summary)
    if creates:
        if not apply:
            part.add("C3", "apply", AVISO, "sin --apply: no se aplicó (repita con --apply)")
            return part.finish()
        if not ctx.ask(f"¿Aplicar el plan de infra/aws ({len(creates)} a crear: roles ta-quicksight-autor y ta-quicksight-lector)?"):
            part.add("C3", "apply", AVISO, "no confirmado")
            return part.finish()
        res = tf_apply(ctx, AWS_ENV, plan_path)
        good = res.ok and f"Apply complete! Resources: {len(creates)} added, 0 changed, 0 destroyed." in res.out
        part.add("C3", "apply", OK if good else FALLA, res.tail(4) if res.ok else res.err)
        if not res.ok:
            return part.finish()
        res, summary = tf_plan(ctx, AWS_ENV, TERRAFORM_TARGETS, plan_path)
        clean = summary is not None and not any(summary[k] for k in ("create", "update", "delete", "replace"))
        part.add("C4", "plan después del apply", OK if clean else FALLA,
                 "sin cambios" if clean else (plan_line(summary) if summary else res.err), **(summary or {}))
    return part.finish()


# --- Parte D · despliegue ------------------------------------------------------------------------------------


def part_deploy(ctx: Ctx) -> int:
    part = Part(ctx, "D", "Despliegue en QuickSight (scripts/quicksight.py deploy)")
    if not ctx.ask("¿Crear o actualizar en QuickSight la fuente Athena, los 4 datasets (con su importación a SPICE y el "
                   "refresco cada hora), los grupos y usuarios de los roles, el análisis y el tablero?"):
        part.add("D0", "despliegue", AVISO, "no confirmado")
        return part.finish()
    cmd = [sys.executable, "scripts/quicksight.py", "deploy", "--yes"]
    if getattr(ctx.options, "aws_profile", None):
        cmd += ["--aws-profile", ctx.options.aws_profile]
    part.note("D: despliega e importa a SPICE (≈ 5 min; una línea JSON por paso)…")
    res = ctx.run(cmd, cwd=PROJECT, timeout=3600, stream=True)
    evs = events(res.out)
    err = last_event(evs, "error")
    sub = last_event(evs, "subscription")
    part.add("D1", "suscripción", OK if sub.get("status") == "ACCOUNT_CREATED" else FALLA,
             f"{sub.get('status')} · {sub.get('edition')}" if sub else (err.get("error") or res.tail(6)))
    nopro = last_event(evs, "no_pro_users")
    part.add("D2", "sin usuarios Pro (evita la cuota de 250 USD/mes al terminar la prueba)", OK if nopro else FALLA,
             (f"pasados a su papel normal: {nopro.get('changed')}" if nopro.get("changed") else "ninguno era Pro") if nopro else "no llegó")
    users = last_event(evs, "access_users").get("users") or []
    by_user = {u["user"]: u for u in users}
    good = all(by_user.get(DEMO_USERS[k], {}).get("group") == qs.GROUPS[k] for k in DEMO_USERS)
    part.add("D3", "roles IAM registrados en su grupo", OK if good else FALLA,
             "; ".join(f"{u['user']} → {u['role']} en {u['group']}" for u in users) or "no llegó", users=users)
    src = last_event(evs, "data_source")
    part.add("D4", f"fuente {qs.DATA_SOURCE_ID} (Athena {qs.WORKGROUP}, rol {qs.SERVICE_ROLE})",
             OK if str(src.get("status", "")).endswith("SUCCESSFUL") else FALLA,
             f"{src.get('action')} · {src.get('status')}" if src else "no llegó")
    ds = [e for e in evs if e.get("event") == "dataset"]
    part.add("D5", "4 datasets SPICE", OK if len(ds) == len(qs.DATASETS) else FALLA,
             "; ".join(f"{e['id']} ({e['action']})" for e in ds) or "no llegó")
    ings = [e for e in evs if e.get("event") == "ingestion"]
    good = len(ings) == len(qs.DATASETS) and all(e.get("status") == "COMPLETED" and not e.get("dropped") for e in ings)
    part.add("D6", "importación a SPICE", OK if good else FALLA,
             "; ".join(f"{e['dataset']}: {e.get('rows')} filas en {e.get('seconds')} s, {e.get('dropped')} descartadas"
                       for e in ings) or "no llegó", ingestions=ings)
    sched = last_event(evs, "refresh_schedules")
    part.add("D7", "refresco de SPICE", OK if len(sched.get("datasets") or []) == len(qs.DATASETS) else FALLA,
             f"{sched.get('schedule')} {sched.get('frequency')} en {len(sched.get('datasets') or [])} datasets" if sched else "no llegó")
    ana, dash = last_event(evs, "analysis"), last_event(evs, "dashboard")
    part.add("D8", f"análisis {qs.ANALYSIS_ID}", OK if str(ana.get("status", "")).endswith("SUCCESSFUL") and not ana.get("errors") else FALLA,
             f"{ana.get('action')} · {ana.get('status')} · hojas {ana.get('sheets')}" if ana else "no llegó")
    good = str(dash.get("status", "")).endswith("SUCCESSFUL") and not dash.get("errors") and res.ok
    part.add("D9", f"tablero {qs.DASHBOARD_ID} publicado", OK if good else FALLA,
             f"versión {dash.get('version')} · {dash.get('status')} · hojas {dash.get('sheets')}"
             + (f" · errores {dash.get('errors')}" if dash.get("errors") else "") if dash else (err.get("error") or res.tail(6)),
             version=dash.get("version"))
    return part.finish()


# --- Parte Q · comprobaciones en la nube ----------------------------------------------------------------------


def athena_columns(ctx: Ctx, qid: str) -> list[tuple[str, str]]:
    res = ctx.aws("athena", "get-query-results", "--query-execution-id", qid, "--max-results", "1")
    info = ((parse_json(res.out) or {}).get("ResultSet") or {}).get("ResultSetMetadata", {}).get("ColumnInfo", [])
    return [(c.get("Name"), c.get("Type")) for c in info]


def latest_ingestion(ctx: Ctx, dataset_id: str) -> dict[str, Any]:
    data = ctx.qs("list-ingestions", "--data-set-id", dataset_id) or {}
    done = [i for i in data.get("Ingestions", []) if i.get("IngestionStatus") == "COMPLETED"]
    return max(done, key=lambda i: str(i.get("CreatedTime"))) if done else {}


def part_checks(ctx: Ctx) -> int:
    part = Part(ctx, "Q", "Comprobaciones en QuickSight, Athena e IAM")
    acc = account(ctx)
    # Q1/Q2: cada dataset frente a Athena
    rows_ok, cols_ok, details, col_details = True, True, [], []
    for d in qs.DATASETS:
        body = v41.strip_sql_comments(d.sql)
        cnt = v41.athena_query(ctx, f"SELECT count(*) AS n FROM (\n{body}\n) t", "ta_curated")
        n = int(cnt["rows"][0]["n"]) if cnt.get("state") == "SUCCEEDED" and cnt.get("rows") else None
        ing = latest_ingestion(ctx, d.dataset_id)
        spice = (ing.get("RowInfo") or {}).get("RowsIngested")
        same = n is not None and spice == n
        rows_ok &= same
        details.append(f"{d.dataset_id}: SPICE {spice} · Athena {n}" + ("" if same else " ≠"))
        one = v41.athena_query(ctx, f"SELECT * FROM (\n{body}\n) t LIMIT 1", "ta_curated")
        probs = qs.athena_column_problems(d, athena_columns(ctx, one["id"])) if one.get("state") == "SUCCEEDED" else [one.get("reason")]
        cols_ok &= not probs
        col_details += probs
    part.add("Q1", "filas en SPICE = filas de la consulta en Athena", OK if rows_ok else AVISO,
             "\n".join(details) + ("" if rows_ok else "\n(si el horario corrió entre la importación y esta consulta: "
                                    "python project/scripts/quicksight.py refresh y repetir)"))
    part.add("Q2", "columnas que devuelve Athena = las declaradas (nombre, orden, tipo); sin columnas de autor",
             OK if cols_ok else FALLA, "\n".join(col_details) or f"{len(qs.DATASETS)} datasets coinciden")
    # Q3: hora de Bogotá
    body = v41.strip_sql_comments(qs.DATASET_BY_KEY["publicaciones"].sql)
    tz = v41.athena_query(ctx, "SELECT count(*) AS total, count_if(date_diff('second', p.hora_bogota, f.event_time) <> 18000) "
                               "AS malas, count_if(p.fecha_bogota <> CAST(CAST(p.hora_bogota AS date) AS timestamp)) AS dias_malos "
                               f"FROM (\n{body}\n) p JOIN ta_curated.fact_posts f ON f.event_id = p.event_id", "ta_curated")
    r = (tz.get("rows") or [{}])[0]
    good = tz.get("state") == "SUCCEEDED" and r.get("malas") == "0" and r.get("dias_malos") == "0"
    part.add("Q3", "hora_bogota = event_time − 5 h (UTC−5 sin horario de verano) en todas las filas", OK if good else FALLA,
             f"{r.get('total')} filas · {r.get('malas')} con otra diferencia · {r.get('dias_malos')} con otro día" if r else tz.get("reason"))
    # Q4: tablero desplegado
    dash = (ctx.qs("describe-dashboard", "--dashboard-id", qs.DASHBOARD_ID) or {}).get("Dashboard") or {}
    version = dash.get("Version") or {}
    defn = (ctx.qs("describe-dashboard-definition", "--dashboard-id", qs.DASHBOARD_ID) or {}).get("Definition") or {}
    dataset_arns = {d["Identifier"]: d["DataSetArn"] for d in defn.get("DataSetIdentifierDeclarations", [])}
    probs = qs.definition_problems(defn) if defn else ["sin definición"]
    visuals = sum(len(s.get("Visuals", [])) for s in defn.get("Sheets", []))
    good = version.get("Status", "").endswith("SUCCESSFUL") and not version.get("Errors") and not probs and visuals == 16
    part.add("Q4", "tablero publicado = definición del repositorio", OK if good else FALLA,
             f"versión {version.get('VersionNumber')} {version.get('Status')} · {len(defn.get('Sheets', []))} hojas · {visuals} "
             f"visuales · errores {version.get('Errors') or 0}" + (f"\n" + "\n".join(probs) if probs else ""),
             version=version.get("VersionNumber"))
    # Q5: datasets y fuente
    bad = []
    for d in qs.DATASETS:
        info = (ctx.qs("describe-data-set", "--data-set-id", d.dataset_id) or {}).get("DataSet") or {}
        cols = [c.get("Name") for c in info.get("OutputColumns", [])]
        if info.get("ImportMode") != "SPICE" or set(cols) & set(qs.FORBIDDEN_COLUMNS) or cols != d.column_names:
            bad.append(f"{d.dataset_id}: {info.get('ImportMode')} · {cols}")
        if dataset_arns.get(d.key) and dataset_arns[d.key] != info.get("Arn"):
            bad.append(f"{d.key}: el tablero usa {dataset_arns[d.key]}")
    src = (ctx.qs("describe-data-source", "--data-source-id", qs.DATA_SOURCE_ID) or {}).get("DataSource") or {}
    athena = (src.get("DataSourceParameters") or {}).get("AthenaParameters") or {}
    if athena != {"WorkGroup": qs.WORKGROUP, "RoleArn": f"arn:aws:iam::{acc}:role/{qs.SERVICE_ROLE}"}:
        bad.append(f"fuente: {athena}")
    part.add("Q5", "datasets SPICE sin columnas de autor; la fuente usa ta-quicksight-role", FALLA if bad else OK,
             "\n".join(bad) or f"4 datasets SPICE con las columnas declaradas · fuente {athena}")
    # Q6: permisos del tablero
    users = (ctx.qs("list-users", "--namespace", qs.NAMESPACE) or {}).get("UserList") or []
    admins = [u["Arn"] for u in users if str(u.get("Role", "")).startswith("ADMIN")]
    perms = (ctx.qs("describe-dashboard-permissions", "--dashboard-id", qs.DASHBOARD_ID) or {}).get("Permissions") or []
    probs = permission_problems(perms, acc, admins)
    part.add("Q6", "permisos del tablero: autores dueños, lectores solo ven", FALLA if probs else OK,
             "\n".join(probs) or f"ta-autores y {len(admins)} administrador(es): {len(qs.DASHBOARD_OWNER)} acciones · "
             f"ta-lectores: {', '.join(a.split(':')[1] for a in qs.DASHBOARD_VIEWER)}")
    # Q7: usuarios, grupos y costo
    members = {g: {m.get("MemberName") for m in (ctx.qs("list-group-memberships", "--group-name", g, "--namespace",
                                                          qs.NAMESPACE) or {}).get("GroupMemberList", [])}
               for g in qs.GROUPS.values()}
    roles = {u["UserName"]: u.get("Role") for u in users}
    pro = [u for u, r in roles.items() if r in qs.PRO_ROLES]
    good = (not pro and roles.get(DEMO_USERS["lectores"]) == "READER" and roles.get(DEMO_USERS["autores"]) == "AUTHOR"
            and DEMO_USERS["lectores"] in members[qs.GROUPS["lectores"]] and DEMO_USERS["autores"] in members[qs.GROUPS["autores"]]
            and DEMO_USERS["lectores"] not in members[qs.GROUPS["autores"]])
    part.add("Q7", "usuarios por rol IAM, grupos y sin Pro", OK if good else FALLA,
             f"usuarios {roles} · grupos { {g: sorted(m) for g, m in members.items()} } · Pro: {pro or 'ninguno'} · al terminar "
             f"la prueba (≈ {trial_end():%Y-%m-%d}) costarían ≈ {monthly_cost(users)} USD/mes",
             users=roles, monthly_usd_after_trial=monthly_cost(users))
    # Q8: roles IAM
    probs = []
    for role, action in SELF_REGISTER.items():
        r = ctx.aws("iam", "get-role", "--role-name", role)
        if not r.ok:
            probs.append(f"{role}: no existe (parte C con --apply)")
            continue
        names = (parse_json(ctx.aws("iam", "list-role-policies", "--role-name", role).out) or {}).get("PolicyNames", [])
        docs = {n: (parse_json(ctx.aws("iam", "get-role-policy", "--role-name", role, "--policy-name", n).out) or {}).get("PolicyDocument") or {}
                for n in names}
        attached = (parse_json(ctx.aws("iam", "list-attached-role-policies", "--role-name", role).out) or {}).get("AttachedPolicies", [])
        probs += [f"{role}: {p}" for p in role_problems((parse_json(r.out) or {}).get("Role") or {}, docs, acc, action)]
        probs += [f"{role}: política adjunta {a.get('PolicyName')}" for a in attached]
    lector = f"arn:aws:iam::{acc}:role/{qs.ACCESS_ROLES['lectores']}"
    sim = ctx.aws("iam", "simulate-principal-policy", "--policy-source-arn", lector, "--action-names", "athena:StartQueryExecution",
                  "s3:GetObject", "glue:GetTable", "quicksight:UpdateDashboard")
    decisions = {e.get("EvalActionName"): e.get("EvalDecision") for e in (parse_json(sim.out) or {}).get("EvaluationResults", [])}
    if not sim.ok or any(v == "allowed" for v in decisions.values()):
        probs.append(f"el lector no debe tocar datos: {decisions or sim.tail(2)}")
    part.add("Q8", "roles ta-quicksight-autor/lector: confianza en la cuenta y solo auto-registro", FALLA if probs else OK,
             "\n".join(probs) or f"sin acceso a datos (simulación del lector: {decisions})", simulation=decisions)
    # Q9: el lector abre el tablero
    url = ctx.qs("generate-embed-url-for-registered-user", "--session-lifetime-in-minutes", "15", "--user-arn",
                 qs.principal_arn(acc, "user", DEMO_USERS["lectores"]), "--experience-configuration",
                 json.dumps({"Dashboard": {"InitialDashboardId": qs.DASHBOARD_ID}}))
    part.add("Q9", f"sesión de {DEMO_USERS['lectores']} sobre el tablero", OK if url and url.get("EmbedUrl") else FALLA,
             "QuickSight emite la sesión (la URL no se guarda)" if url else str(ctx.cache.get("qs_error")))
    # Q10: refresco
    bad = []
    for d in qs.DATASETS:
        sch = (ctx.qs("list-refresh-schedules", "--data-set-id", d.dataset_id) or {}).get("RefreshSchedules") or []
        freq = next((s.get("ScheduleFrequency") for s in sch if s.get("ScheduleId") == qs.REFRESH_SCHEDULE_ID), None)
        if not freq or freq.get("Interval") != "HOURLY":
            bad.append(f"{d.dataset_id}: {freq}")
    part.add("Q10", "refresco de SPICE cada hora", FALLA if bad else OK, "\n".join(bad) or f"{qs.REFRESH_SCHEDULE_ID} en los 4 datasets")
    return part.finish()


# --- Parte E · tiempo de respuesta y reducción del tiempo de informes ------------------------------------------


def part_latency(ctx: Ctx) -> int:
    part = Part(ctx, "E", "Tiempo de respuesta del tablero (< 3 s) y reducción del tiempo de informes (≥ 50 %)")
    samples = parse_samples(getattr(ctx.options, "samples", None))
    if samples:
        worst = max(s for _, s in samples)
        part.add("E1", f"mediciones en el navegador (KPI < {KPI_SECONDS:.0f} s con SPICE)", OK if worst < KPI_SECONDS else FALLA,
                 "; ".join(f"{n} {s:.2f} s" for n, s in samples) + f" · peor {worst:.2f} s",
                 samples=dict(samples), worst_s=worst,
                 method=getattr(ctx.options, "method", None) or "del clic a la última mutación del DOM de la hoja")
    else:
        prev = ctx.step_data("E", "E1")
        part.add("E1", "mediciones en el navegador", OK if prev.get("worst_s", 99) < KPI_SECONDS else OMITIDO,
                 f"las de la corrida anterior: {prev.get('samples')}" if prev else
                 "sin --samples (validation-6-1.md, parte E: abrir el tablero con quicksight.py embed-url y medir)", **prev)
    times = []
    for d in qs.DATASETS:
        q = v41.athena_query(ctx, v41.strip_sql_comments(d.sql), "ta_curated")
        times.append((d.dataset_id, q.get("total_ms"), q.get("scanned_bytes"), q.get("state")))
    good = all(t[3] == "SUCCEEDED" for t in times)
    part.add("E2", "referencia: cada consulta en Athena directo (sin SPICE)", OK if good else AVISO,
             "; ".join(f"{i}: {ms} ms, {round((b or 0) / 1e6, 1)} MB" for i, ms, b, _ in times),
             athena=[{"dataset": i, "total_ms": ms, "scanned_bytes": b} for i, ms, b, _ in times])
    red = report_reduction()
    part.add("E3", "reducción del tiempo de elaboración de un informe (estimación por pasos)",
             OK if red["reduction_pct"] >= 50 else FALLA,
             f"{red['manual_min']} min a mano → {red['tablero_min']} min con el tablero: −{red['reduction_pct']} % "
             "(pasos en validation-6-1.md, parte E)", **red, steps=REPORT_MINUTES)
    texts = {EVIDENCE.name: EVIDENCE.read_text(encoding="utf-8") if EVIDENCE.exists() else ""}
    leaks = v53.leak_problems(texts)
    part.add("E4", "evidencia sin datos personales ni URLs de sesión", FALLA if leaks or "code=" in texts[EVIDENCE.name] else OK,
             "\n".join(leaks) or "sin correos, tokens ni URLs prefirmadas")
    return part.finish()


# --- status y unsubscribe ------------------------------------------------------------------------------------------


def part_status(ctx: Ctx) -> int:
    part = Part(ctx, "status", "Estado (solo lectura)")
    info = (ctx.qs("describe-account-subscription") or {}).get("AccountInfo") or {}
    left = (trial_end() - ctx.now()).days
    part.add("S1", "suscripción de QuickSight", (OK if left > 3 else AVISO) if info else OK,
             f"{info.get('AccountSubscriptionStatus')} · prueba gratis hasta ≈ {trial_end():%Y-%m-%d} ({left} días); "
             "al terminar el uso: validate_6_1.py unsubscribe --confirm-unsubscribe" if info else "sin suscripción (nada cobra)")
    if not info:
        return part.finish()
    users = (ctx.qs("list-users", "--namespace", qs.NAMESPACE) or {}).get("UserList") or []
    part.add("S2", "usuarios", AVISO if any(u.get("Role") in qs.PRO_ROLES for u in users) else OK,
             f"{ {u['UserName']: u.get('Role') for u in users} } · ≈ {monthly_cost(users)} USD/mes después de la prueba")
    rows = []
    for d in qs.DATASETS:
        ing = latest_ingestion(ctx, d.dataset_id)
        rows.append(f"{d.dataset_id}: {(ing.get('RowInfo') or {}).get('RowsIngested')} filas ({ing.get('CreatedTime')})")
    part.add("S3", "última importación a SPICE", OK, "\n".join(rows))
    dash = ((ctx.qs("describe-dashboard", "--dashboard-id", qs.DASHBOARD_ID) or {}).get("Dashboard") or {}).get("Version") or {}
    part.add("S4", "tablero", OK if dash.get("Status", "").endswith("SUCCESSFUL") else AVISO,
             f"versión {dash.get('VersionNumber')} · {dash.get('Status')}" if dash else "no existe")
    return part.finish()


def part_unsubscribe(ctx: Ctx) -> int:
    part = Part(ctx, "U", "Borrar la suscripción de QuickSight (irreversible)")
    if not getattr(ctx.options, "confirm_unsubscribe", False):
        part.add("U1", "borrar la suscripción", OMITIDO, "falta --confirm-unsubscribe: borra la cuenta de QuickSight con "
                 "sus usuarios, datasets, análisis y tablero (el código queda en el repositorio para recrearlos)")
        return part.finish()
    if not ctx.ask("¿Borrar la suscripción de QuickSight de la cuenta (irreversible)?"):
        part.add("U1", "borrar la suscripción", AVISO, "no confirmado")
        return part.finish()
    off = ctx.qs("update-account-settings", "--default-namespace", qs.NAMESPACE, "--no-termination-protection-enabled")
    gone = ctx.qs("delete-account-subscription") if off is not None else None
    part.add("U1", "borrar la suscripción", OK if gone is not None else FALLA,
             "pedida (tarda unos minutos; status la confirma)" if gone is not None else str(ctx.cache.get("qs_error")))
    return part.finish()


# --- CLI ----------------------------------------------------------------------------------------

PARTS: dict[str, Callable[[Ctx], int]] = {
    "preflight": part_preflight,
    "tests": part_tests,
    "local": part_local,
    "terraform": part_terraform,
    "deploy": part_deploy,
    "checks": part_checks,
    "latency": part_latency,
    "status": part_status,
    "unsubscribe": part_unsubscribe,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="validate_6_1.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("part", choices=list(PARTS))
    parser.add_argument("--yes", action="store_true", help="no pedir confirmación")
    parser.add_argument("--apply", action="store_true", help="C: aplicar el plan de Terraform")
    parser.add_argument("--aws-profile", default=None, help="perfil de AWS CLI (si no, AWS_PROFILE o el por defecto)")
    parser.add_argument("--samples", default=None, help="E: mediciones del navegador, p. ej. restablecer=2.10,hoja_relaciones=1.65")
    parser.add_argument("--method", default=None, help="E: cómo se midieron las muestras")
    parser.add_argument("--confirm-unsubscribe", action="store_true", help="U: confirma que se borra la suscripción")
    return parser


def main(argv: list[str] | None = None, *, runner: Runner | None = None, printer: Callable[[str], None] = print,
         confirm: Callable[[str], bool] | None = None, sleep: Callable[[float], None] | None = None,
         now: Callable[[], datetime] | None = None, evidence_path: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    ctx = Ctx(runner=runner or Runner(printer), options=args, printer=printer, confirm=confirm,
              sleep=sleep or time.sleep, now=now or (lambda: datetime.now(timezone.utc)),
              evidence_path=evidence_path or EVIDENCE)
    printer(f"Lote 6.1 · {args.part} · {ctx.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if on_windows() and args.part != "preflight":
        printer(f"error: está en Windows; {WSL_HINT}")
        return 2
    try:
        return PARTS[args.part](ctx)
    except KeyboardInterrupt:  # pragma: no cover - interacción
        printer("\ninterrumpido")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
