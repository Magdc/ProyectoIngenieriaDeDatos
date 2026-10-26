"""Glue job ``ta-job-trusted-to-curated`` · MARCADOR DE POSICIÓN del Lote 4.1 (ADR-23).

El Lote 4.3 reemplaza este archivo por el job real (hechos, dimensiones y agregados de Curated,
HU-027; con Glue o lanzando dbt-athena). Terraform lo sube a
``s3://{lake}/_glue/scripts/ta-job-trusted-to-curated.py`` y el job no cambia: basta con
reemplazar el archivo y aplicar (ARCHITECTURE.md §3.19.1, "Cómo reemplazar un job").

Corre en el workflow ``ta-wf-raw-to-curated`` solo si ``ta-job-raw-to-trusted`` terminó en
SUCCEEDED. Mientras tanto, para que el workflow se pueda validar sin tocar datos:

1. lee sus argumentos y, dentro del workflow, sus propiedades de corrida;
2. falla a propósito si ``ta_force_fail`` nombra a este job (su clave, su nombre o ``all``);
3. cuenta lo que hay bajo ``trusted/`` (sin las marcas) y lee 4 bytes de un Parquet si hay;
4. lee la marca que dejó ``ta-job-raw-to-trusted`` en la misma corrida
   (``trusted/_ta_placeholder/ta-job-raw-to-trusted/{corrida}.json``): prueba el orden y la
   lectura de ``trusted/``;
5. escribe su marca en ``curated/_ta_placeholder/{job}/{corrida}.json`` e imprime
   ``glue_placeholder_done`` (CloudWatch: ``/aws-glue/python-jobs/output``).

Compatible con Python 3.9 (Glue Python shell) y sin dependencias fuera de boto3.
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone

JOB_KEY = "trusted_to_curated"
JOB_NAME = "ta-job-trusted-to-curated"
DEFAULTS = {
    "ta_input_prefix": "trusted/",
    "ta_output_prefix": "curated/",
    "ta_upstream_job": "ta-job-raw-to-trusted",
    "ta_region": "",
    "ta_force_fail": "",
}
PLACEHOLDER_DIR = "_ta_placeholder/"
MAX_LISTED = 200000


def parse_args(argv):
    """``--clave valor`` o ``--clave=valor`` (como los pasa Glue) → dict sin los ``--``."""
    out = dict(DEFAULTS)
    i = 0
    while i < len(argv):
        token = argv[i]
        if token.startswith("--"):
            key = token[2:]
            if "=" in key:
                key, value = key.split("=", 1)
            elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                value = argv[i + 1]
                i += 1
            else:
                value = "true"
            out[key] = value
        i += 1
    return out


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log(event, **fields):
    record = {"time": now_iso(), "event": event, "job": JOB_NAME, "placeholder": True, "lote": "4.1"}
    record.update(fields)
    print(json.dumps(record, ensure_ascii=False, sort_keys=True, default=str), flush=True)
    return record


def wants_failure(value):
    names = {v.strip() for v in str(value or "").split(",") if v.strip()}
    return bool(names & {JOB_KEY, JOB_NAME, "all", "true", "1"})


def run_properties(glue, args):
    workflow, run_id = args.get("WORKFLOW_NAME"), args.get("WORKFLOW_RUN_ID")
    if not (glue and workflow and run_id):
        return {}
    resp = glue.get_workflow_run_properties(Name=workflow, RunId=run_id)
    return resp.get("RunProperties") or {}


def inventory(s3, bucket, prefix):
    """Objetos y bytes bajo ``prefix`` (sin las marcas), por origen si es Raw, y un Parquet."""
    paginator = s3.get_paginator("list_objects_v2")
    objects, size, by_origin, sample = 0, 0, {}, None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/") or "/" + PLACEHOLDER_DIR in "/" + key[len(prefix):]:
                continue
            objects += 1
            size += int(obj.get("Size") or 0)
            if "origin=" in key:
                origin = key.split("origin=", 1)[1].split("/", 1)[0]
                by_origin[origin] = by_origin.get(origin, 0) + 1
            if sample is None and key.endswith(".parquet"):
                sample = key
            if objects >= MAX_LISTED:
                return {"objects": objects, "bytes": size, "by_origin": by_origin, "sample": sample, "truncated": True}
    return {"objects": objects, "bytes": size, "by_origin": by_origin, "sample": sample, "truncated": False}


def read_probe(s3, bucket, key):
    if not key:
        return "sin objetos Parquet"
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-3")["Body"].read()
    if body != b"PAR1":
        raise RuntimeError("%s no empieza con PAR1 (%r)" % (key, body))
    return "PAR1"


def marker_key(prefix, job, run_id):
    return "%s%s%s/%s.json" % (prefix, PLACEHOLDER_DIR, job, run_id)


def upstream_found(s3, bucket, args, run_id):
    upstream = args.get("ta_upstream_job")
    if not upstream or not args.get("WORKFLOW_RUN_ID"):
        return None
    key = marker_key(args["ta_input_prefix"], upstream, run_id)
    try:
        json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
        return True
    except Exception as exc:  # noqa: BLE001 - NoSuchKey u otro: se informa, no se cae
        log("glue_placeholder_upstream_missing", key=key, error=type(exc).__name__)
        return False


def main(argv=None, s3=None, glue=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    bucket = args.get("lake_bucket")
    if not bucket:
        raise RuntimeError("falta --lake_bucket")
    if s3 is None or glue is None:
        import boto3

        region = args.get("ta_region") or None
        s3 = s3 or boto3.client("s3", region_name=region)
        glue = glue or boto3.client("glue", region_name=region)
    run_id = args.get("WORKFLOW_RUN_ID") or ("manual-" + uuid.uuid4().hex[:12])
    log("glue_job_start", workflow=args.get("WORKFLOW_NAME"), workflow_run_id=args.get("WORKFLOW_RUN_ID"),
        input_prefix=args["ta_input_prefix"], output_prefix=args["ta_output_prefix"])
    props = run_properties(glue, args)
    if wants_failure(args.get("ta_force_fail")) or wants_failure(props.get("ta_force_fail")):
        log("glue_job_forced_failure", workflow_run_id=args.get("WORKFLOW_RUN_ID"))
        raise RuntimeError("ta_force_fail: fallo provocado por la validación (%s)" % JOB_NAME)
    inv = inventory(s3, bucket, args["ta_input_prefix"])
    probe = read_probe(s3, bucket, inv["sample"])
    found = upstream_found(s3, bucket, args, run_id)
    summary = {
        "job": JOB_NAME,
        "job_key": JOB_KEY,
        "workflow": args.get("WORKFLOW_NAME"),
        "workflow_run_id": args.get("WORKFLOW_RUN_ID"),
        "input_prefix": args["ta_input_prefix"],
        "input_objects": inv["objects"],
        "input_bytes": inv["bytes"],
        "input_by_origin": inv["by_origin"],
        "input_truncated": inv["truncated"],
        "read_probe": probe,
        "upstream_marker_found": found,
        "finished_at": now_iso(),
    }
    key = marker_key(args["ta_output_prefix"], JOB_NAME, run_id)
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(summary, sort_keys=True).encode("utf-8"),
                  ContentType="application/json")
    summary["marker"] = key
    log("glue_placeholder_done", **{k: v for k, v in summary.items() if k != "job"})
    return summary


# Glue Python shell ejecuta el archivo como script; las pruebas lo importan sin argumentos de Glue.
if __name__ == "__main__" or "--lake_bucket" in sys.argv[1:]:
    main()
