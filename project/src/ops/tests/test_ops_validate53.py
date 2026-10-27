"""Pruebas del validador del Lote 5.3 con AWS, Terraform y gcloud falsos (sin red)."""

from __future__ import annotations

import importlib.util
import json

import pytest

from lakehouse import catalog_spec as cs
from ops import validate41 as v41
from ops import validate53 as v

ACCOUNT = "648617469176"
BUCKET = f"ta-dev-{ACCOUNT}-lake"
SPECS = cs.load_contract_tables()
ALLOW = {
    "ta-gcp-exporter-role": [("s3:PutObject", "/raw/origin=stream/")],
    "ta-historical-loader": [("s3:PutObject", "/raw/origin=historical/")],
    "ta-glue-role": [("s3:GetObject", "/raw/"), ("s3:PutObject", "/trusted/")],
    "ta-quicksight-role": [("s3:GetObject", "/curated/")],
}


def R(code, out="", err=""):
    from ops.validate32 import Result

    return Result(code, out, err)


def glue_table(spec, *, stale=False):
    r = cs.resolve(spec, BUCKET)

    def col(c):
        return {"Name": c.name, "Type": c.type, **({} if stale else {"Comment": c.comment})}

    return {"Name": r.name, "TableType": "EXTERNAL_TABLE", "Description": "TrendAnalyzer" if stale else r.description,
            "Parameters": {**r.parameters, **cs.EXTRA_PARAMETERS}, "PartitionKeys": [col(c) for c in r.partition_keys],
            "StorageDescriptor": {"Columns": [col(c) for c in r.columns], "Location": r.location, "InputFormat": cs.PARQUET_INPUT,
                                  "SerdeInfo": {"SerializationLibrary": cs.PARQUET_SERDE}}}


def plan_json(kind):
    changes = []
    for t in SPECS:
        before = {"description": "vieja", "storage_descriptor": [{"columns": [{"name": "a", "type": "string", "comment": None}]}]}
        after = {"description": t.description, "storage_descriptor": [{"columns": [
            {"name": "a", "type": "int" if kind == "schema" else "string", "comment": "[sin_dato_personal] A"}]}]}
        changes.append({"address": v.TABLE_ADDR.format(t.full_name), "mode": "managed",
                        "change": {"actions": ["no-op"] if kind == "applied" else ["update"], "before": before,
                                   "after": before if kind == "applied" else after}})
    return {"resource_changes": changes}


class Fake:
    def __init__(self, *, stale=False, plan="docs", tls=True, public=False, extra_accessor=False, athena_ok=True, gcloud=True):
        self.stale, self.plan, self.tls, self.public, self.extra_accessor = stale, plan, tls, public, extra_accessor
        self.athena_ok, self.gcloud = athena_ok, gcloud
        self.calls = []

    def which(self, name):
        return None if name == "gcloud" and not self.gcloud else f"/usr/bin/{name}"

    def run(self, cmd, *, cwd=None, env=None, timeout=600, stream=False):
        cmd = [str(c) for c in cmd]
        self.calls.append(cmd)
        text = " ".join(cmd)
        if cmd[1:3] == ["-m", "pytest"]:
            target = next((c for c in cmd if c.startswith(("src/", "contracts/"))), "")
            counts = {"src/lakehouse": v.B1_EXPECTED, "src/ops/tests/test_ops_validate53.py": v.B2_EXPECTED,
                      "contracts/tests": v.B3_EXPECTED, "src/dq": v.B4_EXPECTED, "src/ops/tests/test_ops_validate51.py": v.B5_EXPECTED}
            return R(0, f"{counts.get(target, 1)} passed in 1.0s")
        if cmd[0] == "aws":
            return self.aws(cmd, text)
        if cmd[0] == "terraform":
            if "show" in cmd:
                return R(0, json.dumps(plan_json(self.plan)))
            if "apply" in cmd:
                self.plan = "applied"
                return R(0, f"Apply complete! Resources: 0 added, {len(SPECS)} changed, 0 destroyed.")
            return R(0, "ok")
        if cmd[0] == "bash":
            return R(0, "validate ok")
        if cmd[0] == "gcloud":
            return self.gcloud_cmd(text)
        return R(0, "")

    def aws(self, cmd, text):
        if "get-caller-identity" in text:
            return R(0, json.dumps({"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:root"}))
        if "get-schedule" in text:
            return R(0, json.dumps({"State": "ENABLED", "ScheduleExpression": "rate(1 hour)"}))
        if "get-tables" in text:
            db = cmd[cmd.index("--database-name") + 1]
            return R(0, json.dumps({"TableList": [glue_table(t, stale=self.stale) for t in SPECS if t.database == db]}))
        if "start-query-execution" in text:
            return R(0, json.dumps({"QueryExecutionId": "q1"}))
        if "get-query-execution" in text:
            state = "SUCCEEDED" if self.athena_ok else "FAILED"
            return R(0, json.dumps({"QueryExecution": {"Status": {"State": state, "StateChangeReason": "COLUMN_NOT_FOUND"},
                                                       "Statistics": {"DataScannedInBytes": 0, "EngineExecutionTimeInMillis": 900}}}))
        if "get-query-results" in text:
            cols = ["table_schema", "table_name", "column_name", "comment"]
            rows = [[t.database, t.name, c.name, "" if self.stale else c.comment] for t in SPECS for c in cs.all_columns(t)]
            data = [{"Data": [{"VarCharValue": x} for x in r]} for r in [cols] + rows]
            return R(0, json.dumps({"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": c} for c in cols]}, "Rows": data}}))
        if "get-public-access-block" in text:
            return R(0, json.dumps({"PublicAccessBlockConfiguration": {k: True for k in (
                "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}}))
        if "get-bucket-encryption" in text:
            return R(0, json.dumps({"ServerSideEncryptionConfiguration": {"Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": "arn:aws:kms:x"},
                "BucketKeyEnabled": True}]}}))
        if "get-bucket-policy" in text:
            st = {"Effect": "Deny", "Condition": {"Bool": {"aws:SecureTransport": "false" if self.tls else "true"}}}
            return R(0, json.dumps({"Policy": json.dumps({"Statement": [st]})}))
        if "simulate-principal-policy" in text:
            role = cmd[cmd.index("--policy-source-arn") + 1].rsplit("/", 1)[-1]
            action, resource = cmd[cmd.index("--action-names") + 1], cmd[cmd.index("--resource-arns") + 1]
            allowed = any(a == action and p in resource for a, p in ALLOW.get(role, []))
            return R(0, json.dumps({"EvaluationResults": [{"EvalDecision": "allowed" if allowed else "implicitDeny"}]}))
        return R(0, "{}")

    def gcloud_cmd(self, text):
        if "buckets describe" in text:
            return R(0, json.dumps({"uniform_bucket_level_access": True, "public_access_prevention": "enforced",
                                    "lifecycle_config": {"rule": [{"action": {"type": "Delete"},
                                                                    "condition": {"age": 30, "matchesPrefix": ["raw/", "dlq/"]}}]}}))
        if "buckets get-iam-policy" in text:
            members = ["serviceAccount:sa-dataflow@example-project.iam.gserviceaccount.com", "user:someone@example.com"]
            members += ["allUsers"] if self.public else []
            return R(0, json.dumps({"bindings": [{"role": "roles/storage.objectAdmin", "members": members}]}))
        if "secrets get-iam-policy" in text:
            members = ["serviceAccount:sa-dataflow@example-project.iam.gserviceaccount.com"]
            members += ["serviceAccount:sa-exporter@example-project.iam.gserviceaccount.com"] if self.extra_accessor else []
            return R(0, json.dumps({"bindings": [{"role": "roles/secretmanager.secretAccessor", "members": members}]}))
        return R(1, "", "desconocido")


KEYS = {"preflight": "A", "tests": "B", "local": "L", "terraform": "C", "glue": "D", "controls": "G", "status": "status"}


def run(part, fake, tmp_path, *extra):
    lines = []
    code = v.main([part, *extra], runner=fake, printer=lines.append, evidence_path=tmp_path / "ev.json")
    data = json.loads((tmp_path / "ev.json").read_text(encoding="utf-8"))
    assert data["lote"] == "5.3" and data["hu"] == ["HU-034", "HU-035", "HU-036"]
    return code, {s["id"]: s for s in data["parts"][KEYS[part]]["steps"]}, "\n".join(lines)


# --- utilidades puras ---------------------------------------------------------------------------


def test_linaje_real_cubre_todas_las_columnas():
    text = v.LINEAGE_MD.read_text(encoding="utf-8")
    assert v.lineage_problems(text, SPECS) == []
    cols = v.lineage_columns(text)
    assert set(cols) == {t.full_name for t in SPECS} and "text_clean" in cols["ta_trusted.posts"]


def test_linaje_detecta_columnas_y_tablas_que_faltan():
    text = ("## 4. Linaje\n\n### `ta_curated.dim_brand`\n\n| Columna | Viene de |\n|---|---|\n| `brand`, `display` | x |\n\n"
            "### `ta_curated.otra`\n\n| `a` | b |\n## 5. Otro\n| `role` | no cuenta |\n")
    dim = [t for t in SPECS if t.full_name == "ta_curated.dim_brand"]
    assert v.lineage_problems(text, dim) == ["ta_curated.dim_brand: faltan role",
                                            "ta_curated.otra: sección de una tabla que no está en el catálogo"]


def test_rutas_de_los_documentos():
    assert v.doc_paths("`project/src/dq/` `docs/LINEAGE.md` `project/x/{a}.py` `project/*.sql` `src/x`") == [
        "docs/LINEAGE.md", "project/src/dq"]
    texts = {n: (v.DOCS / n).read_text(encoding="utf-8") for n in ("LINEAGE.md", "GOVERNANCE.md", "CATALOG.md")}
    missing, n = v.path_problems(texts)
    assert missing == [] and n > 20
    assert v.path_problems({"x": "`project/no/existe.py`"})[0] == ["x: no existe project/no/existe.py"]


def test_gobernanza_real_y_lo_que_falta():
    text = v.GOVERNANCE_MD.read_text(encoding="utf-8")
    assert v.governance_problems(text) == []
    broken = text.replace("| Metadatos |", "| Meta |").replace("**Minimización.**", "Minimizacion")
    assert v.governance_problems(broken) == ["DAMA-DMBOK: falta el área «Metadatos» en la tabla", "falta minimización"]


def test_fugas_en_documentos():
    texts = {"a.md": "contacto: alguien@example.com · sa-dataflow@example-project.iam.gserviceaccount.com",
             "b.md": "AKIAABCDEFGHIJKLMNOP ?X-Amz-Signature=1 " + "ab" * 32,
             "c.md": "ta-verify@2.1 comment-gen@1.0 142058005+AlejandroSPosada@users.noreply.github.com"}
    found = v.leak_problems(texts)
    assert [f.rsplit(" (", 1)[0] for f in found] ==["a.md: correo", "b.md: llave de AWS", "b.md: URL prefirmada",
                                                 "b.md: hash de 64 hex (¿un user_hash?)"]


def test_plan_solo_descripciones():
    assert v.plan_only_descriptions(plan_json("docs")) == []
    assert len(v.plan_only_descriptions(plan_json("schema"))) == len(SPECS)
    assert v.plan_only_descriptions(plan_json("applied")) == []


def test_controles_de_s3():
    assert v.public_access_problems({"PublicAccessBlockConfiguration": {"BlockPublicAcls": True}}) == [
        "IgnorePublicAcls = None", "BlockPublicPolicy = None", "RestrictPublicBuckets = None"]
    rules = {"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}}
    assert v.encryption_problems(rules) == ["SSEAlgorithm = AES256", "sin llave KMS", "sin bucket key"]
    assert v.encryption_problems({}) == ["sin cifrado por defecto"]
    deny = {"Statement": [{"Effect": "Deny", "Condition": {"Bool": {"aws:SecureTransport": False}}}]}
    assert v.denies_insecure_transport(json.dumps(deny)) and not v.denies_insecure_transport("{}")
    assert not v.denies_insecure_transport("no es json")


def test_casos_de_acceso_a_raw():
    cases = dict(v.raw_access_cases(ACCOUNT, BUCKET))
    assert set(cases) == set(ALLOW)
    allowed = [(r, c[0]) for r, cs_ in cases.items() for c in cs_ if c[4]]
    assert allowed == [("ta-gcp-exporter-role", "escribe raw/origin=stream/"), ("ta-historical-loader", "escribe raw/origin=historical/"),
                       ("ta-glue-role", "lee raw/"), ("ta-glue-role", "escribe trusted/"), ("ta-quicksight-role", "lee curated/")]


def test_bucket_de_gcs_en_los_dos_formatos():
    api = {"iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}, "publicAccessPrevention": "enforced"},
           "lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["raw/", "dlq/"]}}]}}
    assert v.gcs_bucket_problems(api)[0] == []
    bad = {"uniform_bucket_level_access": False, "public_access_prevention": "inherited",
           "lifecycle_config": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["raw/"]}}]}}
    assert v.gcs_bucket_problems(bad)[0] == ["acceso uniforme = False", "prevención de acceso público = inherited",
                                             "raw/ expira a 7 días (esperado 30)", "dlq/ expira a None días (esperado 30)"]


def test_miembros_de_iam_sin_datos_personales():
    policy = {"bindings": [{"role": "roles/owner", "members": ["user:a@example.com", "serviceAccount:sa-x@p.iam.gserviceaccount.com",
                                                               "projectOwner:p", "allAuthenticatedUsers"]}]}
    public, roles = v.iam_summary(policy)
    assert public == ["roles/owner: allAuthenticatedUsers"]
    assert roles == {"roles/owner": ["allAuthenticatedUsers", "projectOwner:p", "serviceAccount:sa-x", "user:***"]}


def test_comentarios_en_athena():
    rows = [{"table_schema": t.database, "table_name": t.name, "column_name": c.name, "comment": c.comment}
            for t in SPECS for c in cs.all_columns(t)]
    assert v.athena_comment_problems(rows, SPECS) == []
    rows[0]["comment"] = ""
    assert v.athena_comment_problems(rows[:-1], SPECS) == [
        "ta_raw.posts_raw.event_id: comentario distinto en Athena",
        "ta_curated.edges_hashtag_cooccurrence.weight: no aparece en information_schema.columns"]


# --- partes -------------------------------------------------------------------------------------------


def test_parte_local_con_los_documentos_reales(tmp_path):
    code, steps, _ = run("local", Fake(), tmp_path)
    assert code == 0 and {k: s["status"] for k, s in steps.items()} == {f"L{i}": v.OK for i in range(1, 7)}
    assert steps["L1"]["data"]["summary"]["by_class"] == v.TODAY["by_class"]


def test_parte_tests(tmp_path):
    code, steps, _ = run("tests", Fake(), tmp_path)
    assert code == 0 and [s["status"] for s in steps.values()] == [v.OK] * 5


def test_parte_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(v41, "services_status", lambda ctx: ["simulador: apagado"])
    code, steps, _ = run("preflight", Fake(gcloud=False), tmp_path)
    assert code == 0 and steps["A4"]["status"] == v.OK and steps["A6"]["status"] == v.OK
    assert steps["A5"]["status"] == v.AVISO and "G5–G7" in steps["A5"]["detail"]


@pytest.fixture
def tf_env(tmp_path, monkeypatch):
    env = tmp_path / "env"
    env.mkdir()
    (env / "backend.hcl").write_text("bucket = \"x\"\n", encoding="utf-8")
    (env / "terraform.tfvars").write_text(f'aws_account_id = "{ACCOUNT}"\n', encoding="utf-8")
    monkeypatch.setattr(v, "AWS_ENV", env)
    monkeypatch.setattr(v, "EVIDENCE_DIR", tmp_path)
    return env


def test_parte_terraform_sin_apply(tmp_path, tf_env):
    fake = Fake()
    code, steps, _ = run("terraform", fake, tmp_path)
    assert code == 0 and steps["C2"]["status"] == v.OK and "11 a cambiar" in steps["C2"]["detail"]
    assert steps["C0"]["status"] == v.AVISO and steps["C3"]["status"] == v.AVISO and steps["C3b"]["status"] == v.OMITIDO
    assert not any("apply" in c for c in fake.calls)


def test_parte_terraform_aplica_y_no_deja_deriva(tmp_path, tf_env):
    fake = Fake()
    code, steps, _ = run("terraform", fake, tmp_path, "--apply", "--yes")
    assert code == 0 and steps["C3"]["status"] == v.OK and steps["C3b"]["status"] == v.OK
    assert sum(1 for c in fake.calls if "apply" in c) == 1


def test_parte_terraform_rechaza_un_cambio_de_esquema(tmp_path, tf_env):
    fake = Fake(plan="schema")
    code, steps, _ = run("terraform", fake, tmp_path, "--apply", "--yes")
    assert code == 1 and steps["C2"]["status"] == v.FALLA and "C3" not in steps
    assert "más que descripción" in steps["C2"]["detail"] and not any("apply" in c for c in fake.calls)


def test_parte_glue(tmp_path):
    code, steps, _ = run("glue", Fake(), tmp_path)
    assert code == 0 and [steps[k]["status"] for k in ("D1", "D2", "D3")] == [v.OK] * 3
    assert steps["D3"]["data"]["rows"] == v.TODAY["columns"]
    code, steps, _ = run("glue", Fake(stale=True, athena_ok=False), tmp_path)
    assert code == 1 and steps["D1"]["status"] == v.OK and steps["D2"]["status"] == v.FALLA
    assert steps["D3"]["status"] == v.AVISO and "¿falta aplicar C?" in steps["D2"]["detail"]


def test_parte_controls(tmp_path):
    code, steps, _ = run("controls", Fake(), tmp_path)
    assert code == 0 and [steps[f"G{i}"]["status"] for i in range(1, 8)] == [v.OK] * 7
    assert "user:***" in steps["G6"]["detail"] and "example.com" not in json.dumps(steps)
    code, steps, _ = run("controls", Fake(tls=False, public=True, extra_accessor=True), tmp_path)
    assert code == 1 and [steps[k]["status"] for k in ("G3", "G6", "G7")] == [v.FALLA] * 3


def test_parte_status_y_script(tmp_path):
    code, steps, _ = run("status", Fake(stale=True), tmp_path)
    assert code == 0 and steps["S1"]["status"] == v.AVISO and "0 de 11" in steps["S1"]["detail"]
    spec = importlib.util.spec_from_file_location("validate_5_3", v.PROJECT / "scripts" / "validate_5_3.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main is v.main
