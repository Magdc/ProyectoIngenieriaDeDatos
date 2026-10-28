"""Despliegue del tablero en QuickSight con boto3 (Lote 6.1; ARCHITECTURE.md §3.27.1).

``Deployer`` recibe el cliente de QuickSight (real o falso en las pruebas) y deja la cuenta como dice
``quicksight_spec``: crea lo que falta y actualiza lo que existe, así que repetir ``deploy`` no duplica nada. Cada
paso emite un evento (``dict``) con ``event`` = nombre del paso; el CLI los imprime como una línea JSON cada uno.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable

from serving import quicksight_spec as qs

Emit = Callable[[dict[str, Any]], None]


class DeployError(RuntimeError):
    pass


def error_code(exc: Exception) -> str:
    return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code") or type(exc).__name__


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


class Deployer:
    def __init__(self, client: Any, account: str, *, emit: Emit | None = None,
                 sleep: Callable[[float], None] = time.sleep, now: Callable[[], datetime] | None = None,
                 poll_s: float = 5.0, timeout_s: float = 900.0):
        self.c = client
        self.account = account
        self.emit = emit or (lambda e: None)
        self.sleep = sleep
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.poll_s = poll_s
        self.timeout_s = timeout_s

    # --- utilidades ---------------------------------------------------------------------------------------------

    def _call(self, name: str, **kw: Any) -> dict[str, Any]:
        return getattr(self.c, name)(AwsAccountId=self.account, **kw)

    def _exists(self, name: str, **kw: Any) -> dict[str, Any] | None:
        try:
            return self._call(name, **kw)
        except Exception as exc:  # noqa: BLE001 - el cliente real y el falso lanzan clases distintas
            if error_code(exc) == "ResourceNotFoundException":
                return None
            raise

    def _wait(self, what: str, probe: Callable[[], tuple[str, Any]], done: set[str], failed: set[str]) -> tuple[str, Any]:
        start = self.now()
        while True:
            status, data = probe()
            if status in done or status in failed:
                return status, data
            if (self.now() - start).total_seconds() > self.timeout_s:
                raise DeployError(f"{what}: sigue en {status} después de {self.timeout_s:.0f} s")
            self.sleep(self.poll_s)

    def owners(self) -> list[str]:
        """Los administradores de la cuenta (usuarios con papel ADMIN o ADMIN_PRO) y el grupo de autores."""
        users = self.users()
        admins = [u["Arn"] for u in users if str(u.get("Role", "")).startswith("ADMIN")]
        if not admins:
            raise DeployError("QuickSight no tiene ningún administrador (¿la suscripción existe?)")
        return admins + [qs.principal_arn(self.account, "group", qs.GROUPS["autores"])]

    def users(self) -> list[dict[str, Any]]:
        out, token = [], None
        while True:
            kw = {"Namespace": qs.NAMESPACE, **({"NextToken": token} if token else {})}
            res = self._call("list_users", **kw)
            out += res.get("UserList", [])
            token = res.get("NextToken")
            if not token:
                return out

    # --- cuenta, usuarios y grupos ------------------------------------------------------------------------------

    def subscription(self) -> dict[str, Any]:
        info = self._call("describe_account_subscription").get("AccountInfo", {})
        ev = {"event": "subscription", "status": info.get("AccountSubscriptionStatus"), "edition": info.get("Edition"),
              "name": info.get("AccountName"), "auth": info.get("AuthenticationType")}
        self.emit(ev)
        if ev["status"] != "ACCOUNT_CREATED":
            raise DeployError(f"la suscripción de QuickSight está en {ev['status']}")
        return ev

    def no_pro_users(self) -> list[dict[str, str]]:
        """Pasa a su papel normal a cada usuario Pro (la suscripción deja Pro al administrador)."""
        changed = []
        for u in self.users():
            new = qs.PRO_ROLES.get(u.get("Role", ""))
            if not new:
                continue
            email = self._call("describe_user", UserName=u["UserName"], Namespace=qs.NAMESPACE)["User"].get("Email")
            self._call("update_user", UserName=u["UserName"], Namespace=qs.NAMESPACE, Role=new, Email=email)
            changed.append({"user": u["UserName"], "from": u["Role"], "to": new})
        self.emit({"event": "no_pro_users", "changed": changed})
        return changed

    def spice_capacity(self) -> str:
        """Compra automática de SPICE: sin ella, actualizar un dataset falla con «Insufficient SPICE capacity»."""
        self._call("update_spice_capacity_configuration", PurchaseMode=qs.SPICE_PURCHASE_MODE)
        self.emit({"event": "spice_capacity", "purchase_mode": qs.SPICE_PURCHASE_MODE})
        return qs.SPICE_PURCHASE_MODE

    def groups(self) -> list[str]:
        created = []
        for key, name in qs.GROUPS.items():
            if self._exists("describe_group", GroupName=name, Namespace=qs.NAMESPACE) is None:
                self._call("create_group", GroupName=name, Namespace=qs.NAMESPACE,
                           Description=f"TrendAnalyzer · {key} del tablero (Lote 6.1)")
                created.append(name)
        self.emit({"event": "groups", "groups": list(qs.GROUPS.values()), "created": created})
        return created

    def access_users(self, role_arns: dict[str, str]) -> list[dict[str, str]]:
        """Registra cada rol IAM de acceso como usuario (sesión ``demo``) y lo pone en su grupo; los administradores
        entran a ``ta-autores``."""
        existing = {u["UserName"]: u for u in self.users()}
        rows = []
        for key, arn in role_arns.items():
            role_name = arn.rsplit("/", 1)[-1]
            user = f"{role_name}/{qs.DEMO_SESSION}"
            if user not in existing:
                self._call("register_user", Namespace=qs.NAMESPACE, IdentityType="IAM", IamArn=arn,
                           SessionName=qs.DEMO_SESSION, UserRole=qs.ACCESS_USER_ROLE[key],
                           Email=qs.DEMO_EMAIL.format(role=role_name))
            self._member(user, qs.GROUPS[key])
            rows.append({"user": user, "role": qs.ACCESS_USER_ROLE[key], "group": qs.GROUPS[key],
                         "registered": user not in existing})
        for u in existing.values():
            if str(u.get("Role", "")).startswith("ADMIN"):
                self._member(u["UserName"], qs.GROUPS["autores"])
                rows.append({"user": u["UserName"], "role": u["Role"], "group": qs.GROUPS["autores"], "registered": False})
        self.emit({"event": "access_users", "users": rows})
        return rows

    def _member(self, user: str, group: str) -> None:
        try:
            self._call("create_group_membership", MemberName=user, GroupName=group, Namespace=qs.NAMESPACE)
        except Exception as exc:  # noqa: BLE001
            if error_code(exc) != "ResourceExistsException":
                raise

    # --- fuente de datos y datasets -----------------------------------------------------------------------------

    def data_source(self, service_role_arn: str) -> str:
        req = qs.data_source_request(self.account, service_role_arn, self.owners())
        current = self._exists("describe_data_source", DataSourceId=qs.DATA_SOURCE_ID)
        if current is None:
            self._call("create_data_source", **{k: v for k, v in req.items() if k != "AwsAccountId"})
            action = "created"
        else:
            self._call("update_data_source", **{k: v for k, v in req.items() if k not in ("AwsAccountId", "Type", "Permissions")})
            action = "updated"

        def probe() -> tuple[str, Any]:
            ds = self._call("describe_data_source", DataSourceId=qs.DATA_SOURCE_ID)["DataSource"]
            return ds.get("Status", "?"), ds

        status, ds = self._wait("fuente de datos", probe, {"CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"},
                                {"CREATION_FAILED", "UPDATE_FAILED", "DELETED"})
        if action == "updated" and status.endswith("SUCCESSFUL"):
            # Después de la actualización: durante ella QuickSight responde ConflictException.
            self._call("update_data_source_permissions", DataSourceId=qs.DATA_SOURCE_ID, GrantPermissions=req["Permissions"])
        self.emit({"event": "data_source", "id": qs.DATA_SOURCE_ID, "action": action, "status": status,
                   "error": ds.get("ErrorInfo")})
        if not status.endswith("SUCCESSFUL"):
            raise DeployError(f"fuente de datos {status}: {ds.get('ErrorInfo')}")
        return ds["Arn"]

    def datasets(self, data_source_arn: str) -> dict[str, str]:
        owners = self.owners()
        arns = {}
        for d in qs.DATASETS:
            req = qs.dataset_request(self.account, d, data_source_arn, owners)
            body = {k: v for k, v in req.items() if k != "AwsAccountId"}
            if self._exists("describe_data_set", DataSetId=d.dataset_id) is None:
                res = self._call("create_data_set", **body)
                action = "created"
            else:
                res = self._call("update_data_set", **{k: v for k, v in body.items() if k != "Permissions"})
                self._call("update_data_set_permissions", DataSetId=d.dataset_id, GrantPermissions=body["Permissions"])
                action = "updated"
            arns[d.key] = res["Arn"]
            self.emit({"event": "dataset", "id": d.dataset_id, "action": action, "columns": len(d.columns)})
        return arns

    def refresh(self, keys: list[str] | None = None) -> list[dict[str, Any]]:
        """Importa a SPICE cada dataset (``FULL_REFRESH``) y espera a que termine."""
        out = []
        stamp = self.now().strftime("%Y%m%dT%H%M%SZ")
        for d in qs.DATASETS:
            if keys and d.key not in keys:
                continue
            ing_id = f"ta-{stamp}-{d.key}"
            self._call("create_ingestion", DataSetId=d.dataset_id, IngestionId=ing_id, IngestionType="FULL_REFRESH")

            def probe(ds_id: str = d.dataset_id, i: str = ing_id) -> tuple[str, Any]:
                ing = self._call("describe_ingestion", DataSetId=ds_id, IngestionId=i)["Ingestion"]
                return ing.get("IngestionStatus", "?"), ing

            status, ing = self._wait(f"ingesta de {d.dataset_id}", probe, {"COMPLETED"}, {"FAILED", "CANCELLED"})
            row = {"event": "ingestion", "dataset": d.dataset_id, "id": ing_id, "status": status,
                   "rows": (ing.get("RowInfo") or {}).get("RowsIngested"),
                   "dropped": (ing.get("RowInfo") or {}).get("RowsDropped"),
                   "seconds": ing.get("IngestionTimeInSeconds"),
                   "bytes": ing.get("IngestionSizeInBytes"), "error": ing.get("ErrorInfo")}
            self.emit(row)
            out.append(row)
            if status != "COMPLETED":
                raise DeployError(f"ingesta {d.dataset_id} {status}: {ing.get('ErrorInfo')}")
        return out

    def refresh_schedules(self) -> list[str]:
        sched = qs.refresh_schedule()
        done = []
        for d in qs.DATASETS:
            have = self._call("list_refresh_schedules", DataSetId=d.dataset_id).get("RefreshSchedules", [])
            if any(s.get("ScheduleId") == qs.REFRESH_SCHEDULE_ID for s in have):
                self._call("update_refresh_schedule", DataSetId=d.dataset_id, Schedule=sched)
            else:
                self._call("create_refresh_schedule", DataSetId=d.dataset_id, Schedule=sched)
            done.append(d.dataset_id)
        self.emit({"event": "refresh_schedules", "schedule": qs.REFRESH_SCHEDULE_ID, "frequency": sched["ScheduleFrequency"],
                   "datasets": done})
        return done

    # --- análisis y tablero -------------------------------------------------------------------------------------

    def analysis(self, dataset_arns: dict[str, str]) -> dict[str, Any]:
        defn = qs.definition(dataset_arns)
        owners = self.owners()
        body = {"AnalysisId": qs.ANALYSIS_ID, "Name": qs.DASHBOARD_NAME, "Definition": defn,
                "ValidationStrategy": {"Mode": "STRICT"}}
        if self._exists("describe_analysis", AnalysisId=qs.ANALYSIS_ID) is None:
            self._call("create_analysis", Permissions=qs.permissions(owners, qs.ANALYSIS_OWNER), **body)
            action = "created"
        else:
            self._call("update_analysis", **body)
            action = "updated"

        def probe() -> tuple[str, Any]:
            a = self._call("describe_analysis", AnalysisId=qs.ANALYSIS_ID)["Analysis"]
            return a.get("Status", "?"), a

        status, a = self._wait("análisis", probe, {"CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"},
                               {"CREATION_FAILED", "UPDATE_FAILED", "DELETED"})
        if action == "updated" and status.endswith("SUCCESSFUL"):
            self._call("update_analysis_permissions", AnalysisId=qs.ANALYSIS_ID,
                       GrantPermissions=qs.permissions(owners, qs.ANALYSIS_OWNER))
        ev = {"event": "analysis", "id": qs.ANALYSIS_ID, "action": action, "status": status,
              "errors": a.get("Errors") or [], "sheets": [s.get("Name") for s in a.get("Sheets", [])]}
        self.emit(ev)
        if not status.endswith("SUCCESSFUL") or ev["errors"]:
            raise DeployError(f"análisis {status}: {ev['errors']}")
        return ev

    def dashboard(self, dataset_arns: dict[str, str]) -> dict[str, Any]:
        defn = qs.definition(dataset_arns)
        owners = self.owners()
        readers = [qs.principal_arn(self.account, "group", qs.GROUPS["lectores"])]
        perms = qs.permissions(owners, qs.DASHBOARD_OWNER) + qs.permissions(readers, qs.DASHBOARD_VIEWER)
        body = {"DashboardId": qs.DASHBOARD_ID, "Name": qs.DASHBOARD_NAME, "Definition": defn,
                "DashboardPublishOptions": qs.DASHBOARD_PUBLISH_OPTIONS, "ValidationStrategy": {"Mode": "STRICT"},
                "VersionDescription": f"quicksight.py deploy {self.now().strftime('%Y-%m-%dT%H:%M:%SZ')}"}
        if self._exists("describe_dashboard", DashboardId=qs.DASHBOARD_ID) is None:
            res = self._call("create_dashboard", Permissions=perms, **body)
            action = "created"
        else:
            res = self._call("update_dashboard", **body)
            action = "updated"
        version = int(str(res.get("VersionArn", "")).rsplit("/", 1)[-1] or 1)

        def probe() -> tuple[str, Any]:
            d = self._call("describe_dashboard", DashboardId=qs.DASHBOARD_ID, VersionNumber=version)["Dashboard"]
            return (d.get("Version") or {}).get("Status", "?"), d

        status, d = self._wait("tablero", probe, {"CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"},
                               {"CREATION_FAILED", "UPDATE_FAILED", "DELETED"})
        errors = (d.get("Version") or {}).get("Errors") or []
        if status.endswith("SUCCESSFUL") and not errors:
            self._call("update_dashboard_published_version", DashboardId=qs.DASHBOARD_ID, VersionNumber=version)
            self._call("update_dashboard_permissions", DashboardId=qs.DASHBOARD_ID, GrantPermissions=perms)
        ev = {"event": "dashboard", "id": qs.DASHBOARD_ID, "action": action, "version": version, "status": status,
              "errors": errors, "sheets": [s.get("Name") for s in (d.get("Version") or {}).get("Sheets", [])]}
        self.emit(ev)
        if not status.endswith("SUCCESSFUL") or errors:
            raise DeployError(f"tablero {status}: {errors}")
        return ev

    # --- todo junto y estado ------------------------------------------------------------------------------------

    def deploy(self, service_role_arn: str, access_role_arns: dict[str, str], *, refresh: bool = True) -> dict[str, Any]:
        self.subscription()
        self.no_pro_users()
        self.spice_capacity()
        self.groups()
        users = self.access_users(access_role_arns)
        ds_arn = self.data_source(service_role_arn)
        arns = self.datasets(ds_arn)
        ingestions = self.refresh() if refresh else []
        self.refresh_schedules()
        self.analysis(arns)
        dash = self.dashboard(arns)
        out = {"event": "deploy_done", "ok": True, "datasets": arns, "ingestions": len(ingestions),
               "dashboard_version": dash["version"], "users": len(users)}
        self.emit(out)
        return out

    def status(self) -> dict[str, Any]:
        """Solo lectura: suscripción, usuarios, datasets con su última ingesta y versión publicada del tablero."""
        info = self._call("describe_account_subscription").get("AccountInfo", {})
        users = [{"user": u["UserName"], "role": u.get("Role"), "identity": u.get("IdentityType")} for u in self.users()]
        datasets = []
        for d in qs.DATASETS:
            ds = self._exists("describe_data_set", DataSetId=d.dataset_id)
            if ds is None:
                datasets.append({"id": d.dataset_id, "exists": False})
                continue
            ings = self._call("list_ingestions", DataSetId=d.dataset_id).get("Ingestions", [])
            last = max(ings, key=lambda i: str(_iso(i.get("CreatedTime")))) if ings else {}
            datasets.append({"id": d.dataset_id, "exists": True, "import_mode": ds["DataSet"].get("ImportMode"),
                             "spice_bytes": ds["DataSet"].get("ConsumedSpiceCapacityInBytes"),
                             "last_ingestion": {"status": last.get("IngestionStatus"),
                                                "rows": (last.get("RowInfo") or {}).get("RowsIngested"),
                                                "created": _iso(last.get("CreatedTime"))} if last else None})
        dash = self._exists("describe_dashboard", DashboardId=qs.DASHBOARD_ID)
        version = (dash or {}).get("Dashboard", {}).get("Version") or {}
        out = {"event": "status", "subscription": info.get("AccountSubscriptionStatus"), "edition": info.get("Edition"),
               "users": users, "pro_users": [u["user"] for u in users if u["role"] in qs.PRO_ROLES],
               "datasets": datasets,
               "dashboard": {"exists": dash is not None, "version": version.get("VersionNumber"),
                             "status": version.get("Status"), "errors": version.get("Errors") or []}}
        self.emit(out)
        return out

    def embed_url(self, user_name: str, minutes: int = 15) -> str:
        """URL de un solo uso para abrir el tablero como ``user_name`` (prueba de < 3 s). No se guarda en ningún lado."""
        arn = qs.principal_arn(self.account, "user", user_name)
        res = self._call("generate_embed_url_for_registered_user", SessionLifetimeInMinutes=minutes, UserArn=arn,
                         ExperienceConfiguration={"Dashboard": {"InitialDashboardId": qs.DASHBOARD_ID}})
        return res["EmbedUrl"]
