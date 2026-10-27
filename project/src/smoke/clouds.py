"""Los puertos de ``smoke.e2e`` con los clientes reales (Pub/Sub, BigQuery, GCS y Cloud Run en GCP; S3, Glue y
Athena en AWS). Credenciales: ADC de GCP y el perfil de AWS del desarrollador (§3.26.1)."""

from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from smoke.e2e import JOB_CURATED, JOB_TRUSTED, WORKFLOW, Busy

PROJECT_DIR = Path(__file__).resolve().parents[2]
GCP_TFVARS = PROJECT_DIR / "infra" / "gcp" / "environments" / "dev" / "terraform.tfvars"
REGION_GCP = "us-central1"
REGION_AWS = "us-east-1"
WORKGROUP = "ta-analytics"
EXPORTER_JOB = "ta-gcs-to-s3"
DATAFLOW_PREFIX = "ta-stream-dev"


def default_gcp_project(env: dict[str, str]) -> str | None:
    """``--gcp-project``, ``TA_GCP_PROJECT``/``GCP_PROJECT`` o ``project_id`` de ``infra/gcp/.../terraform.tfvars``."""
    for key in ("TA_GCP_PROJECT", "GCP_PROJECT"):
        if env.get(key):
            return env[key]
    try:
        m = re.search(r'^\s*project_id\s*=\s*"([^"]+)"', GCP_TFVARS.read_text(encoding="utf-8"), re.M)
    except OSError:
        return None
    return m.group(1) if m else None


class Clouds:
    def __init__(self, gcp_project: str, *, aws_profile: str | None = None, bucket: str | None = None):
        import boto3
        from google.cloud import bigquery, pubsub_v1, storage

        self.project = gcp_project
        self.session = boto3.Session(profile_name=aws_profile, region_name=REGION_AWS)
        self.s3 = self.session.client("s3")
        self.glue = self.session.client("glue")
        self.athena_client = self.session.client("athena")
        self.bucket = bucket or f"ta-dev-{self.session.client('sts').get_caller_identity()['Account']}-lake"
        self.publisher = pubsub_v1.PublisherClient()
        self.bq = bigquery.Client(project=gcp_project)
        self.gcs = storage.Client(project=gcp_project).bucket(f"{gcp_project}-ta-lake")

    # GCP
    def _gcloud(self, *args: str, timeout: float = 600) -> subprocess.CompletedProcess:
        return subprocess.run(["gcloud", *args, "--project", self.project], capture_output=True, text=True,
                              timeout=timeout, stdin=subprocess.DEVNULL)

    def dataflow_active(self) -> list[str]:
        res = self._gcloud("dataflow", "jobs", "list", "--region", REGION_GCP, "--status=active", "--format=json", timeout=120)
        if res.returncode != 0:
            return []
        return [j.get("name", "") for j in json.loads(res.stdout or "[]") if str(j.get("name", "")).startswith(DATAFLOW_PREFIX)]

    def publish(self, topic: str, data: bytes, attributes: dict[str, str]) -> str:
        path = self.publisher.topic_path(self.project, topic)
        return self.publisher.publish(path, data, **attributes).result(timeout=60)

    def bq_rows(self, event_id: str, day: str) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        sql = (f"SELECT * FROM `{self.project}.ta_stream.posts_enriched` "
               "WHERE event_id = @id AND DATE(event_time) = @d")
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", event_id),
                                                        bigquery.ScalarQueryParameter("d", "DATE", day)])
        return [dict(row.items()) for row in self.bq.query(sql, job_config=cfg).result()]

    def gcs_list(self, prefix: str) -> list[dict[str, Any]]:
        return [{"name": b.name, "created": b.time_created, "size": b.size} for b in self.gcs.client.list_blobs(self.gcs, prefix=prefix)]

    def gcs_get(self, name: str) -> bytes:
        return self.gcs.blob(name).download_as_bytes()

    def run_exporter(self) -> dict[str, Any]:
        start = time.monotonic()
        res = self._gcloud("run", "jobs", "execute", EXPORTER_JOB, "--region", REGION_GCP, "--wait", "--format=json", timeout=1200)
        return {"ok": res.returncode == 0, "seconds": round(time.monotonic() - start, 1),
                "detail": "" if res.returncode == 0 else (res.stderr or res.stdout)[-300:]}

    # AWS
    def s3_head(self, key: str) -> dict[str, Any] | None:
        from botocore.exceptions import ClientError

        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "403", "NoSuchKey", "NotFound"):
                return None
            raise

    def s3_get(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def workflow_runs(self) -> list[dict[str, Any]]:
        return self.glue.get_workflow_runs(Name=WORKFLOW, MaxResults=10).get("Runs", [])

    def start_workflow(self) -> str:
        try:
            return self.glue.start_workflow_run(Name=WORKFLOW)["RunId"]
        except self.glue.exceptions.ConcurrentRunsExceededException as exc:
            raise Busy(str(exc)) from exc

    def workflow_run(self, run_id: str) -> dict[str, Any]:
        run = self.glue.get_workflow_run(Name=WORKFLOW, RunId=run_id, IncludeGraph=True)["Run"]
        jobs: dict[str, dict[str, Any]] = {}
        for node in (run.get("Graph") or {}).get("Nodes") or []:
            for jr in (node.get("JobDetails") or {}).get("JobRuns") or []:
                if jr.get("JobName") in (JOB_TRUSTED, JOB_CURATED):
                    jobs[jr["JobName"]] = {"state": jr.get("JobRunState"), "completed": jr.get("CompletedOn"),
                                           "seconds": jr.get("ExecutionTime")}
        return {"RunId": run_id, "Status": run.get("Status"), "StartedOn": run.get("StartedOn"),
                "CompletedOn": run.get("CompletedOn"), "jobs": jobs}

    def athena(self, sql: str, database: str) -> tuple[list[str], list[dict[str, Any]]]:
        qid = self.athena_client.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": database},
                                                       WorkGroup=WORKGROUP)["QueryExecutionId"]
        for _ in range(200):
            state = self.athena_client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)
        if state["State"] != "SUCCEEDED":
            raise RuntimeError(f"Athena {state['State']}: {state.get('StateChangeReason', '')}")
        res = self.athena_client.get_query_results(QueryExecutionId=qid)
        cols = [c["Name"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = [[d.get("VarCharValue") for d in r["Data"]] for r in res["ResultSet"]["Rows"]][1:]
        return cols, [dict(zip(cols, r)) for r in rows]
