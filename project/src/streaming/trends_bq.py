"""Acceso a BigQuery y Pub/Sub del verificador del Lote 2.4. Las pruebas usan un doble.

Usa ``google-cloud-bigquery`` y ``google-cloud-pubsub``, que vienen con
``apache-beam[gcp]`` (entorno ``~/.venvs/ta-streaming``). Se autentica con
Application Default Credentials (§5.1).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping, Sequence

from streaming.trends_core import format_ts, to_epoch_ms

_IDENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_DATASET = re.compile(r"^[A-Za-z0-9_]+$")


def _norm(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_ts(to_epoch_ms(value))
    return value


class CloudBackend:
    """Interfaz que usa ``trends_cli`` (la implementa ``GoogleCloudBackend`` y los dobles de prueba)."""

    project: str
    dataset: str

    def table_columns(self, table: str) -> list[tuple[str, str]] | None: ...
    def trend_rows(self, lo_ms: int, hi_ms: int) -> list[dict]: ...
    def window_keys(self, from_ms: int) -> list[tuple[int, str, str]]: ...
    def posts(self, lo_ms: int, hi_ms: int) -> list[dict]: ...
    def latency(self, process_from_ms: int) -> dict: ...
    def dlq_rows(self, since_ms: int, marker: str) -> list[dict]: ...
    def insert_dq(self, rows: Sequence[Mapping]) -> list: ...
    def publish(self, topic: str, data: bytes, attributes: Mapping[str, str]) -> str: ...


class GoogleCloudBackend(CloudBackend):
    def __init__(self, project: str, dataset: str = "ta_stream", client=None, publisher=None):
        if not _IDENT.match(project) or not _DATASET.match(dataset):
            raise ValueError("proyecto o dataset con caracteres no válidos")
        self.project = project
        self.dataset = dataset
        if client is None:
            from google.cloud import bigquery
            client = bigquery.Client(project=project)
        self._client = client
        self._publisher = publisher

    def _table(self, name: str) -> str:
        return f"`{self.project}.{self.dataset}.{name}`"

    def _query(self, sql: str, params: Mapping[str, tuple[str, Any]]) -> list[dict]:
        from google.cloud import bigquery
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter(name, kind, value) for name, (kind, value) in params.items()])
        result = self._client.query(sql, job_config=config).result()
        return [{k: _norm(v) for k, v in row.items()} for row in result]

    @staticmethod
    def _ts(ms: int) -> tuple[str, datetime]:
        from datetime import timezone
        return "TIMESTAMP", datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

    def table_columns(self, table: str):
        from google.api_core.exceptions import NotFound
        try:
            found = self._client.get_table(f"{self.project}.{self.dataset}.{table}")
        except NotFound:
            return None
        return [(f.name, f.field_type) for f in found.schema]

    def trend_rows(self, lo_ms: int, hi_ms: int) -> list[dict]:
        sql = f"""
            SELECT window_start, window_end, brand, source, n_posts, n_negative, n_positive,
                   avg_sentiment, TO_JSON_STRING(emotion_counts) AS emotion_counts,
                   zscore_volume, zscore_sentiment, is_alert
            FROM {self._table('trend_windows')}
            WHERE window_start >= @lo AND window_start < @hi"""
        return self._query(sql, {"lo": self._ts(lo_ms), "hi": self._ts(hi_ms)})

    def window_keys(self, from_ms: int):
        sql = f"""
            SELECT DISTINCT window_start, brand, source
            FROM {self._table('trend_windows')}
            WHERE window_start >= @lo"""
        rows = self._query(sql, {"lo": self._ts(from_ms)})
        return [(to_epoch_ms(r["window_start"]), r["brand"], r["source"]) for r in rows]

    def posts(self, lo_ms: int, hi_ms: int) -> list[dict]:
        sql = f"""
            SELECT event_id, ANY_VALUE(event_time) AS event_time, ANY_VALUE(brand_primary) AS brand_primary,
                   ANY_VALUE(source) AS source, ANY_VALUE(sentiment_label) AS sentiment_label,
                   ANY_VALUE(sentiment_score) AS sentiment_score, ANY_VALUE(emotion_label) AS emotion_label
            FROM {self._table('posts_enriched')}
            WHERE event_time >= @lo AND event_time < @hi
            GROUP BY event_id"""
        return self._query(sql, {"lo": self._ts(lo_ms), "hi": self._ts(hi_ms)})

    def latency(self, process_from_ms: int) -> dict:
        sql = f"""
            WITH d AS (
              SELECT TIMESTAMP_DIFF(process_time, event_time, MILLISECOND) / 1000 AS s
              FROM {self._table('posts_enriched')}
              WHERE event_time >= TIMESTAMP_SUB(@since, INTERVAL 1 HOUR)
                AND process_time >= @since
                AND TIMESTAMP_DIFF(ingest_time, event_time, SECOND) <= 60)
            SELECT COUNT(*) AS n,
                   APPROX_QUANTILES(s, 100)[OFFSET(50)] AS p50,
                   APPROX_QUANTILES(s, 100)[OFFSET(95)] AS p95,
                   MAX(s) AS max
            FROM d"""
        rows = self._query(sql, {"since": self._ts(process_from_ms)})
        row = rows[0] if rows else {"n": 0, "p50": None, "p95": None, "max": None}
        return {"n": row["n"], "p50": row["p50"], "p95": row["p95"], "max": row["max"]}

    def dlq_rows(self, since_ms: int, marker: str) -> list[dict]:
        sql = f"""
            SELECT received_at, source, error_type, error_detail, raw_message
            FROM {self._table('dlq_events')}
            WHERE received_at >= @since AND STRPOS(raw_message, @marker) > 0"""
        return self._query(sql, {"since": self._ts(since_ms), "marker": ("STRING", marker)})

    def insert_dq(self, rows):
        return list(self._client.insert_rows_json(f"{self.project}.{self.dataset}.dq_results", list(rows)))

    def publish(self, topic: str, data: bytes, attributes):
        if self._publisher is None:
            from google.cloud import pubsub_v1
            self._publisher = pubsub_v1.PublisherClient()
        path = f"projects/{self.project}/topics/{topic}"
        return self._publisher.publish(path, data, **dict(attributes)).result(timeout=60)
