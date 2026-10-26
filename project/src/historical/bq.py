"""Cruces con el stream en BigQuery (``posts_enriched``, §5.5), solo lectura.

* ``key_check``: antes de escribir la etapa 1, comprueba que la llave HMAC produce los
  mismos ``user_hash`` que el stream (mismo pool de usuarios, §3.10). Si la tabla tiene
  filas y ningún hash coincide, la llave no es la del stream y se aborta.
* ``stream_check`` (H13): lo mismo sobre el Parquet final, más la versión del NLP.

Consulta solo ``user_hash`` y ``nlp_model_version`` de los últimos 90 días (partición por
``event_time``): unos pocos MB.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from historical.verify import AVISO, FALLA, OK, Check

QUERY = """
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(user_hash IN UNNEST(@hashes)) AS hit_rows,
  COUNT(DISTINCT IF(user_hash IN UNNEST(@hashes), user_hash, NULL)) AS hit_users,
  COUNT(DISTINCT user_hash) AS stream_users,
  ARRAY_AGG(DISTINCT nlp_model_version IGNORE NULLS LIMIT 10) AS versions
FROM `{table}`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND origin = 'stream'
"""
MIN_ROWS_FOR_VERDICT = 100


def default_table(project: str | None = None) -> str:
    project = project or os.environ.get("GCP_PROJECT")
    if not project:
        raise ValueError("falta GCP_PROJECT para consultar BigQuery")
    return f"{project}.ta_stream.posts_enriched"


def normalize_table(table: str) -> str:
    """Acepta ``proyecto:dataset.tabla`` (output de Terraform) o ``proyecto.dataset.tabla``."""
    return table.replace(":", ".", 1)


def run_query(table: str, hashes: list[str], project: str | None = None) -> dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project or table.split(".")[0])
    job = client.query(QUERY.format(table=normalize_table(table)),
                        job_config=bigquery.QueryJobConfig(query_parameters=[
                            bigquery.ArrayQueryParameter("hashes", "STRING", hashes)]))
    row = list(job.result())[0]
    return {"n_rows": int(row["n_rows"]), "hit_rows": int(row["hit_rows"]),
            "hit_users": int(row["hit_users"]), "stream_users": int(row["stream_users"]),
            "versions": list(row["versions"] or []),
            "bytes_processed": getattr(job, "total_bytes_processed", None)}


def verdict(result: dict[str, Any], hashes: list[str]) -> tuple[str, str]:
    if result["n_rows"] < MIN_ROWS_FOR_VERDICT:
        return AVISO, (f"posts_enriched tiene {result['n_rows']} filas de stream en 90 días: "
                       "no hay con qué comparar")
    if result["hit_users"] == 0:
        return FALLA, (f"0 de {len(hashes)} user_hash del histórico aparecen en "
                       f"{result['n_rows']} filas del stream: la llave HMAC no es la del stream")
    return OK, (f"{result['hit_users']} de {len(hashes)} user_hash más activos aparecen en el "
                f"stream ({result['hit_rows']} filas de {result['n_rows']})")


def make_key_check(table: str, *, query: Callable[..., dict[str, Any]] = run_query
                   ) -> Callable[[list[str]], dict[str, Any]]:
    def check(hashes: list[str]) -> dict[str, Any]:
        try:
            result = query(table, hashes)
        except Exception as exc:  # noqa: BLE001 - sin BigQuery no se puede afirmar nada
            return {"status": AVISO, "detail": f"no se pudo consultar {table}: {exc}"[:400]}
        status, detail = verdict(result, hashes)
        return {"status": status, "detail": detail, "table": table, **result}

    return check


def make_stream_check(table: str, *, query: Callable[..., dict[str, Any]] = run_query):
    def check(hashes: list[str], versions: set[str]) -> Check:
        try:
            result = query(table, hashes)
        except Exception as exc:  # noqa: BLE001
            return Check("H13", "Cruce con el stream", AVISO,
                         f"no se pudo consultar {table}: {exc}"[:400])
        status, detail = verdict(result, hashes)
        stream_versions = set(result.get("versions") or [])
        if status != FALLA and stream_versions and versions:
            if versions & stream_versions:
                detail += "; misma nlp_model_version que el stream"
            else:
                status = AVISO if status == OK else status
                detail += (f"; versión de NLP distinta del stream ({sorted(versions)} vs "
                           f"{sorted(stream_versions)})")
        return Check("H13", "Cruce con el stream", status, detail,
                     {"table": table, **result})

    return check
