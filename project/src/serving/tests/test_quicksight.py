"""Tablero de QuickSight (Lote 6.1): definiciones sin nube y despliegue contra un QuickSight falso en memoria."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from serving import cli
from serving import quicksight_spec as qs
from serving.deploy import Deployer, DeployError

ACCOUNT = "111122223333"
T0 = datetime(2026, 10, 27, 19, 0, 0, tzinfo=timezone.utc)


def arns() -> dict[str, str]:
    return {d.key: f"arn:aws:quicksight:{qs.REGION}:{ACCOUNT}:dataset/{d.dataset_id}" for d in qs.DATASETS}


# --- definiciones ---------------------------------------------------------------------------------------------------


def test_datasets_solo_leen_ta_curated_y_sin_columnas_de_autor():
    for d in qs.DATASETS:
        assert qs.dataset_problems(d) == [], d.key
        assert all(t.startswith("ta_curated.") for t in qs.sql_tables(d.sql))
        assert not set(d.column_names) & set(qs.FORBIDDEN_COLUMNS)
    # user_hash solo une dentro de las consultas de aristas, nunca sale
    assert "user_hash" in qs.DATASET_BY_KEY["marcas"].sql
    assert "user_hash" not in qs.DATASET_BY_KEY["marcas"].column_names


def test_dataset_problems_detecta_fugas_y_tablas_ajenas(tmp_path, monkeypatch):
    sql = tmp_path / "malo.sql"
    sql.write_text("-- FROM ta_trusted.posts en un comentario no cuenta\n"
                   "SELECT user_hash, n FROM ta_trusted.posts JOIN ta_curated.dim_brand b ON 1 = 1", encoding="utf-8")
    monkeypatch.setattr(qs, "SQL_DIR", tmp_path)
    bad = qs.Dataset("malo", "ta-ds-malo", "malo", "malo.sql", "HU-037", ("dim_brand",),
                     (qs.Col("user_hash", "STRING", "x"), qs.Col("n", "FLOAT", ""), qs.Col("falta", "STRING", "y")))
    problems = " | ".join(qs.dataset_problems(bad))
    assert "fuera de ta_curated" in problems and "ta_trusted.posts" in problems
    assert "prohibidas" in problems and "user_hash" in problems
    assert "no aparecen en el SQL: ['falta']" in problems
    assert "tipo FLOAT" in problems and "sin descripción" in problems


def test_horas_en_bogota_en_el_sql_de_publicaciones():
    sql = qs.DATASET_BY_KEY["publicaciones"].sql
    assert sql.count("AT TIME ZONE 'America/Bogota'") == 4
    for name in ("hora_bogota", "fecha_bogota", "hora_del_dia"):
        assert "Bogota" in qs.DATASET_BY_KEY["publicaciones"].column(name).description


def test_athena_column_problems_compara_nombre_orden_y_tipo():
    d = qs.DATASET_BY_KEY["hashtags"]
    ok = [("hashtag_a", "varchar"), ("hashtag_b", "varchar"), ("par", "varchar"), ("comentarios", "bigint")]
    assert qs.athena_column_problems(d, ok) == []
    assert qs.athena_column_problems(d, ok[:3] + [("comentarios", "double")])
    leak = qs.athena_column_problems(d, ok + [("user_hash", "varchar")])
    assert any("prohibidas" in p for p in leak)


def test_definicion_valida_y_completa():
    defn = qs.definition(arns())
    assert qs.definition_problems(defn) == []
    sheets = {s["SheetId"]: s for s in defn["Sheets"]}
    assert set(sheets) == {"tendencias", "relaciones"}
    assert len(sheets["tendencias"]["Visuals"]) == 11 and len(sheets["relaciones"]["Visuals"]) == 5
    used = {n["ColumnName"] for n in qs._walk(defn) if "ColumnName" in n}
    assert {"marca", "rol_marca", "red", "fecha_bogota", "emocion", "sentimiento_con_signo"} <= used  # HU-037
    rel = {n["DataSetIdentifier"] for n in qs._walk(sheets["relaciones"]) if "DataSetIdentifier" in n}
    assert rel == {"hashtags", "marcas", "autores"}  # HU-038: las dos tablas de aristas
    assert not used & set(qs.FORBIDDEN_COLUMNS)
    assert qs.summary()["problems"] == []
    json.dumps(defn)  # serializable tal cual para boto3


def test_definition_problems_detecta_errores():
    defn = qs.definition(arns())
    broken = copy.deepcopy(defn)
    vis = broken["Sheets"][0]["Visuals"]
    vis.append(copy.deepcopy(vis[0]))  # VisualId y FieldId repetidos
    kpi = broken["Sheets"][1]["Visuals"][0]["KPIVisual"]
    kpi["ChartConfiguration"]["FieldWells"]["Values"][0]["NumericalMeasureField"]["Column"]["ColumnName"] = "user_hash"
    broken["Sheets"][0]["FilterControls"][0]["Dropdown"]["SourceFilterId"] = "no-existe"
    broken["Sheets"][1]["Layouts"][0]["Configuration"]["GridLayout"]["Elements"][0]["ColumnSpan"] = 40
    broken["CalculatedFields"][0]["Expression"] = "count({handle})"
    out = " | ".join(qs.definition_problems(broken))
    for needle in ("VisualId repetidos", "FieldId repetidos", "prohibida autores.user_hash", "filtro no-existe inexistente",
                   "se sale de las 36 columnas", "referencia inválida {handle}"):
        assert needle in out, needle


def test_permisos_y_refresco():
    req = qs.dataset_request(ACCOUNT, qs.DATASETS[0], "arn:ds", ["arn:admin"])
    assert req["ImportMode"] == "SPICE"
    assert req["PhysicalTableMap"]["sql"]["CustomSql"]["DataSourceArn"] == "arn:ds"
    tags = req["LogicalTableMap"]["tabla"]["DataTransforms"]
    assert len(tags) == len(qs.DATASETS[0].columns)
    assert "quicksight:CreateIngestion" in req["Permissions"][0]["Actions"]
    src = qs.data_source_request(ACCOUNT, "arn:aws:iam::1:role/ta-quicksight-role", ["arn:admin"])
    assert src["DataSourceParameters"]["AthenaParameters"] == {"WorkGroup": "ta-analytics",
                                                               "RoleArn": "arn:aws:iam::1:role/ta-quicksight-role"}
    assert not set(qs.DASHBOARD_VIEWER) & {"quicksight:UpdateDashboard", "quicksight:DeleteDashboard"}
    assert qs.refresh_schedule()["ScheduleFrequency"] == {"Interval": "HOURLY", "Timezone": "America/Bogota"}


# --- QuickSight falso ---------------------------------------------------------------------------------------------


class ClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeQuickSight:
    """Lo mínimo de la API que usa ``Deployer``; los recursos asíncronos pasan por *_IN_PROGRESS una vez."""

    def __init__(self, *, ingestion_fails: bool = False, dashboard_errors: list | None = None):
        self.calls: list[str] = []
        self.users = {"111122223333": {"UserName": "111122223333", "Role": "ADMIN_PRO", "Email": "x@example.com",
                                       "Arn": qs.principal_arn(ACCOUNT, "user", "111122223333"), "IdentityType": "IAM"}}
        self.groups: dict[str, set] = {}
        self.data_sources: dict = {}
        self.data_sets: dict = {}
        self.ingestions: dict = {}
        self.schedules: dict = {}
        self.analyses: dict = {}
        self.dashboards: dict = {}
        self.ingestion_fails = ingestion_fails
        self.dashboard_errors = dashboard_errors or []
        self.published = None

    def __getattr__(self, name):
        raise AttributeError(name)

    def _log(self, name, **kw):
        assert kw.pop("AwsAccountId") == ACCOUNT
        self.calls.append(name)
        return kw

    def describe_account_subscription(self, **kw):
        self._log("describe_account_subscription", **kw)
        return {"AccountInfo": {"AccountSubscriptionStatus": "ACCOUNT_CREATED", "Edition": "ENTERPRISE",
                                "AccountName": "ta", "AuthenticationType": "IDENTITY_POOL"}}

    def list_users(self, **kw):
        self._log("list_users", **kw)
        return {"UserList": [dict(u) for u in self.users.values()]}

    def describe_user(self, **kw):
        kw = self._log("describe_user", **kw)
        return {"User": dict(self.users[kw["UserName"]])}

    def update_user(self, **kw):
        kw = self._log("update_user", **kw)
        assert kw["Email"]
        self.users[kw["UserName"]]["Role"] = kw["Role"]
        return {}

    def describe_group(self, **kw):
        kw = self._log("describe_group", **kw)
        if kw["GroupName"] not in self.groups:
            raise ClientError("ResourceNotFoundException")
        return {"Group": {"GroupName": kw["GroupName"]}}

    def create_group(self, **kw):
        kw = self._log("create_group", **kw)
        self.groups[kw["GroupName"]] = set()
        return {}

    def register_user(self, **kw):
        kw = self._log("register_user", **kw)
        name = f"{kw['IamArn'].rsplit('/', 1)[-1]}/{kw['SessionName']}"
        self.users[name] = {"UserName": name, "Role": kw["UserRole"], "Email": kw["Email"],
                            "Arn": qs.principal_arn(ACCOUNT, "user", name), "IdentityType": "IAM"}
        return {}

    def create_group_membership(self, **kw):
        kw = self._log("create_group_membership", **kw)
        members = self.groups[kw["GroupName"]]
        if kw["MemberName"] in members:
            raise ClientError("ResourceExistsException")
        members.add(kw["MemberName"])
        return {}

    def _async(self, store, key, ok):
        item = store[key]
        if item["Status"].endswith("IN_PROGRESS"):
            item["Status"] = ok
            return {**item, "Status": item["Status"].replace("SUCCESSFUL", "IN_PROGRESS")}
        return item

    def describe_data_source(self, **kw):
        kw = self._log("describe_data_source", **kw)
        if kw["DataSourceId"] not in self.data_sources:
            raise ClientError("ResourceNotFoundException")
        ds = self.data_sources[kw["DataSourceId"]]
        return {"DataSource": self._async(self.data_sources, kw["DataSourceId"], ds.get("_ok", "CREATION_SUCCESSFUL"))}

    def create_data_source(self, **kw):
        kw = self._log("create_data_source", **kw)
        self.data_sources[kw["DataSourceId"]] = {"Status": "CREATION_IN_PROGRESS", "Arn": f"arn:ds/{kw['DataSourceId']}",
                                                 "Params": kw["DataSourceParameters"]}
        return {}

    def update_data_source(self, **kw):
        kw = self._log("update_data_source", **kw)
        self.data_sources[kw["DataSourceId"]].update({"Status": "UPDATE_IN_PROGRESS", "_ok": "UPDATE_SUCCESSFUL"})
        return {}

    def update_data_source_permissions(self, **kw):
        kw = self._log("update_data_source_permissions", **kw)
        assert self.data_sources[kw["DataSourceId"]]["Status"] == "UPDATE_SUCCESSFUL"  # nunca durante la actualización
        return {}

    def describe_data_set(self, **kw):
        kw = self._log("describe_data_set", **kw)
        if kw["DataSetId"] not in self.data_sets:
            raise ClientError("ResourceNotFoundException")
        return {"DataSet": self.data_sets[kw["DataSetId"]]}

    def create_data_set(self, **kw):
        kw = self._log("create_data_set", **kw)
        self.data_sets[kw["DataSetId"]] = {"ImportMode": kw["ImportMode"], "ConsumedSpiceCapacityInBytes": 10,
                                           "Columns": kw["PhysicalTableMap"]["sql"]["CustomSql"]["Columns"]}
        return {"Arn": f"arn:dataset/{kw['DataSetId']}"}

    def update_data_set(self, **kw):
        kw = self._log("update_data_set", **kw)
        assert "Permissions" not in kw
        return {"Arn": f"arn:dataset/{kw['DataSetId']}"}

    def update_data_set_permissions(self, **kw):
        self._log("update_data_set_permissions", **kw)
        return {}

    def create_ingestion(self, **kw):
        kw = self._log("create_ingestion", **kw)
        self.ingestions[(kw["DataSetId"], kw["IngestionId"])] = {"n": 0, "CreatedTime": T0}
        return {}

    def describe_ingestion(self, **kw):
        kw = self._log("describe_ingestion", **kw)
        ing = self.ingestions[(kw["DataSetId"], kw["IngestionId"])]
        ing["n"] += 1
        if ing["n"] < 2:
            return {"Ingestion": {"IngestionStatus": "RUNNING"}}
        if self.ingestion_fails:
            return {"Ingestion": {"IngestionStatus": "FAILED", "ErrorInfo": {"Message": "Athena"}}}
        return {"Ingestion": {"IngestionStatus": "COMPLETED", "RowInfo": {"RowsIngested": 7, "RowsDropped": 0},
                              "IngestionTimeInSeconds": 40, "IngestionSizeInBytes": 99}}

    def list_ingestions(self, **kw):
        kw = self._log("list_ingestions", **kw)
        return {"Ingestions": [{"IngestionStatus": "COMPLETED", "RowInfo": {"RowsIngested": 7}, "CreatedTime": v["CreatedTime"]}
                               for (ds, _), v in self.ingestions.items() if ds == kw["DataSetId"]]}

    def list_refresh_schedules(self, **kw):
        kw = self._log("list_refresh_schedules", **kw)
        return {"RefreshSchedules": [s for (ds, _), s in self.schedules.items() if ds == kw["DataSetId"]]}

    def create_refresh_schedule(self, **kw):
        kw = self._log("create_refresh_schedule", **kw)
        self.schedules[(kw["DataSetId"], kw["Schedule"]["ScheduleId"])] = kw["Schedule"]
        return {}

    def update_refresh_schedule(self, **kw):
        kw = self._log("update_refresh_schedule", **kw)
        self.schedules[(kw["DataSetId"], kw["Schedule"]["ScheduleId"])] = kw["Schedule"]
        return {}

    def describe_analysis(self, **kw):
        kw = self._log("describe_analysis", **kw)
        if kw["AnalysisId"] not in self.analyses:
            raise ClientError("ResourceNotFoundException")
        a = self.analyses[kw["AnalysisId"]]
        out = self._async(self.analyses, kw["AnalysisId"], a["_ok"])
        return {"Analysis": {**out, "Sheets": [{"Name": s["Name"]} for s in a["Definition"]["Sheets"]]}}

    def create_analysis(self, **kw):
        kw = self._log("create_analysis", **kw)
        assert kw["ValidationStrategy"] == {"Mode": "STRICT"}
        self.analyses[kw["AnalysisId"]] = {"Status": "CREATION_IN_PROGRESS", "_ok": "CREATION_SUCCESSFUL",
                                          "Definition": kw["Definition"]}
        return {}

    def update_analysis(self, **kw):
        kw = self._log("update_analysis", **kw)
        self.analyses[kw["AnalysisId"]].update({"Status": "UPDATE_IN_PROGRESS", "_ok": "UPDATE_SUCCESSFUL",
                                               "Definition": kw["Definition"]})
        return {}

    def update_analysis_permissions(self, **kw):
        kw = self._log("update_analysis_permissions", **kw)
        assert self.analyses[kw["AnalysisId"]]["Status"] == "UPDATE_SUCCESSFUL"
        return {}

    def _dash_version(self, kw):
        d = self.dashboards.setdefault(kw["DashboardId"], {"versions": [], "perms": []})
        d["versions"].append({"Status": "CREATION_IN_PROGRESS", "Definition": kw["Definition"], "seen": 0})
        n = len(d["versions"])
        return {"VersionArn": f"arn:dashboard/{kw['DashboardId']}/version/{n}"}

    def describe_dashboard(self, **kw):
        kw = self._log("describe_dashboard", **kw)
        if kw["DashboardId"] not in self.dashboards:
            raise ClientError("ResourceNotFoundException")
        d = self.dashboards[kw["DashboardId"]]
        n = kw.get("VersionNumber") or self.published or len(d["versions"])
        v = d["versions"][n - 1]
        v["seen"] += 1
        status = "CREATION_IN_PROGRESS" if v["seen"] < 2 else ("CREATION_FAILED" if self.dashboard_errors else "CREATION_SUCCESSFUL")
        return {"Dashboard": {"Version": {"VersionNumber": n, "Status": status, "Errors": self.dashboard_errors,
                                          "Sheets": [{"Name": s["Name"]} for s in v["Definition"]["Sheets"]]}}}

    def create_dashboard(self, **kw):
        kw = self._log("create_dashboard", **kw)
        res = self._dash_version(kw)
        self.dashboards[kw["DashboardId"]]["perms"] = kw["Permissions"]
        return res

    def update_dashboard(self, **kw):
        kw = self._log("update_dashboard", **kw)
        assert "Permissions" not in kw
        return self._dash_version(kw)

    def update_dashboard_published_version(self, **kw):
        kw = self._log("update_dashboard_published_version", **kw)
        self.published = kw["VersionNumber"]
        return {}

    def update_dashboard_permissions(self, **kw):
        kw = self._log("update_dashboard_permissions", **kw)
        self.dashboards[kw["DashboardId"]]["perms"] = kw["GrantPermissions"]
        return {}

    def generate_embed_url_for_registered_user(self, **kw):
        kw = self._log("generate_embed_url_for_registered_user", **kw)
        return {"EmbedUrl": f"https://embed/{kw['UserArn'].rsplit('/', 1)[-1]}"}


class Clock:
    def __init__(self):
        self.t = T0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)


def deployer(fake: FakeQuickSight, events: list) -> Deployer:
    clock = Clock()
    return Deployer(fake, ACCOUNT, emit=events.append, sleep=clock.sleep, now=clock.now, poll_s=1, timeout_s=60)


ROLES = {"autores": f"arn:aws:iam::{ACCOUNT}:role/ta-quicksight-autor",
         "lectores": f"arn:aws:iam::{ACCOUNT}:role/ta-quicksight-lector"}
SERVICE = f"arn:aws:iam::{ACCOUNT}:role/ta-quicksight-role"


def test_deploy_crea_todo_y_repetirlo_no_duplica():
    fake, events = FakeQuickSight(), []
    out = deployer(fake, events).deploy(SERVICE, ROLES)
    assert out["ok"] and out["dashboard_version"] == 1 and out["ingestions"] == 4
    assert fake.users["111122223333"]["Role"] == "ADMIN"  # sin Pro: evita la cuota de 250 USD/mes
    assert fake.users["ta-quicksight-lector/demo"]["Role"] == "READER"
    assert fake.groups["ta-lectores"] == {"ta-quicksight-lector/demo"}
    assert fake.groups["ta-autores"] == {"ta-quicksight-autor/demo", "111122223333"}
    assert fake.data_sources[qs.DATA_SOURCE_ID]["Params"]["AthenaParameters"]["RoleArn"] == SERVICE
    assert set(fake.data_sets) == {d.dataset_id for d in qs.DATASETS}
    assert len(fake.schedules) == 4 and fake.published == 1
    perms = {p["Principal"]: p["Actions"] for p in fake.dashboards[qs.DASHBOARD_ID]["perms"]}
    readers = perms[qs.principal_arn(ACCOUNT, "group", "ta-lectores")]
    assert readers == qs.DASHBOARD_VIEWER
    assert "quicksight:UpdateDashboard" in perms[qs.principal_arn(ACCOUNT, "group", "ta-autores")]
    assert [e["event"] for e in events][-1] == "deploy_done"

    events2 = []
    out2 = deployer(fake, events2).deploy(SERVICE, ROLES, refresh=False)
    assert out2["dashboard_version"] == 2 and fake.published == 2
    assert fake.calls.count("create_data_set") == 4 and fake.calls.count("update_data_set") == 4
    assert fake.calls.count("register_user") == 2 and fake.calls.count("create_refresh_schedule") == 4
    assert {e["action"] for e in events2 if e["event"] in ("dataset", "analysis", "dashboard", "data_source")} == {"updated"}


def test_errores_detienen_el_despliegue():
    fake, events = FakeQuickSight(ingestion_fails=True), []
    with pytest.raises(DeployError, match="ingesta ta-ds-publicaciones FAILED"):
        deployer(fake, events).deploy(SERVICE, ROLES)
    assert "create_analysis" not in fake.calls
    fake2 = FakeQuickSight(dashboard_errors=[{"Type": "COLUMN_NOT_FOUND", "Message": "x"}])
    with pytest.raises(DeployError, match="tablero CREATION_FAILED"):
        deployer(fake2, []).deploy(SERVICE, ROLES, refresh=False)
    assert fake2.published is None  # una versión con errores no se publica


def test_status_y_cli(capsys):
    fake, events = FakeQuickSight(), []
    d = deployer(fake, events)
    d.deploy(SERVICE, ROLES)
    st = d.status()
    assert st["subscription"] == "ACCOUNT_CREATED" and st["pro_users"] == []
    assert all(x["exists"] and x["last_ingestion"]["rows"] == 7 for x in st["datasets"])
    assert st["dashboard"]["status"] == "CREATION_SUCCESSFUL"
    lines: list[str] = []
    rc = cli.main(["status"], printer=lines.append, deployer_factory=lambda args, emit: (Deployer(
        fake, ACCOUNT, emit=emit, sleep=lambda s: None), None))
    assert rc == 0 and json.loads(lines[-1])["event"] == "status"
    lines.clear()
    rc = cli.main(["embed-url", "--user", "ta-quicksight-lector/demo"], printer=lines.append,
                  deployer_factory=lambda args, emit: (Deployer(fake, ACCOUNT, emit=emit), None))
    assert rc == 0 and lines == ["https://embed/demo"]
    lines.clear()
    assert cli.main(["plan"], printer=lines.append) == 0
    assert json.loads(lines[0])["problems"] == []
    lines.clear()
    rc = cli.main(["deploy"], printer=lines.append, confirm=lambda q: False,
                  deployer_factory=lambda args, emit: (Deployer(fake, ACCOUNT, emit=emit), None))
    assert rc == 1 and "no confirmado" in lines[-1]
