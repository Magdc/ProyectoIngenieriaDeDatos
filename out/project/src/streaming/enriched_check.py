"""Verificador de la rama enriched (Lote 2.2): ``enriched/`` en el lake y ``posts_enriched``.

No usa Beam. Lee los Parquet de ``enriched/`` y los JSONL de ``raw/`` de las últimas horas
(local o GCS) y, opcionalmente, consulta BigQuery. Comprueba:

* columnas = ``posts_enriched`` (§5.5) y ninguna columna o clave de identidad (HU-020);
* ``user_hash`` hex de 64 y **1 a 1** con ``user_id`` (cruce con ``raw/`` por ``event_id``);
* reglas de engagement por fuente y ``brand_primary = brands[0]`` (HU-021);
* sin copias de ``event_id`` en ``enriched/``;
* cobertura: los eventos de ``raw/`` (ventanas ya cerradas) aparecen en ``enriched/``;
* BigQuery: filas recientes, ids únicos, reglas básicas y cobertura frente a ``enriched/``;
* con ``--expect-nlp`` (Lote 2.3): NLP presente, etiquetas, scores, versión y concordancia con
  ``label_sentiment`` (informativa);
* latencia ``process_time − event_time`` (informativa; el KPI de p95 < 5 min es del Lote 2.4;
  con el caos encendido incluye los eventos tardíos).

Salida: 0 pasa (con o sin avisos), 1 falla, 2 error de uso o de infraestructura.
"""

from __future__ import annotations

import gzip
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Protocol

from common.enriched_row import COLUMN_NAMES, IDENTITY_KEYS, row_problems
from common.parquet_rows import parquet_bytes_to_rows, parse_ts

SOURCES = ("reddit", "facebook", "instagram")
SENTIMENTS = ("negative", "neutral", "positive")
EMOTIONS = ("joy", "sadness", "anger", "fear", "disgust", "surprise", "neutral")
NLP_VERSION_RE = re.compile(r"^robertuito-sentiment@[0-9a-f]+\+robertuito-emotion@[0-9a-f]+$")
NLP_COLUMNS = ("sentiment_label", "sentiment_score", "emotion_label", "emotion_score", "nlp_model_version")


def nlp_row_problems(row: dict) -> list[str]:
    """Reglas del NLP (Lote 2.3) sobre una fila con NLP."""
    out = []
    if row.get("sentiment_label") not in SENTIMENTS:
        out.append("sentiment_label fuera de negative|neutral|positive")
    if row.get("emotion_label") not in EMOTIONS:
        out.append("emotion_label fuera de las 7 clases")
    for k in ("sentiment_score", "emotion_score"):
        v = row.get(k)
        if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
            out.append(f"{k} fuera de [0, 1]")
    if not NLP_VERSION_RE.match(str(row.get("nlp_model_version") or "")):
        out.append("nlp_model_version con formato inválido")
    return out
_PART_RE = re.compile(r"part-(\d{8}T\d{6})Z")

OK, WARN, FAIL = "ok", "warn", "fail"


class InfraError(RuntimeError):
    """Problema de acceso (bucket inexistente, sin credenciales…): salida 2."""


# --------------------------------------------------------------------------- #
# Acceso al lake
# --------------------------------------------------------------------------- #


class Lake(Protocol):
    def list(self, prefix: str) -> list[str]: ...
    def read(self, key: str) -> bytes: ...


class LocalLake:
    def __init__(self, root: str):
        self.root = root.rstrip("/")
        if not os.path.isdir(self.root):
            raise InfraError(f"no existe la carpeta del lake: {self.root}")

    def list(self, prefix: str) -> list[str]:
        base = os.path.join(self.root, prefix)
        if not os.path.isdir(base):
            return []
        out = []
        for dirpath, _dirs, files in os.walk(base):
            for f in files:
                if f.startswith("."):
                    continue  # temporales
                full = os.path.join(dirpath, f)
                out.append(os.path.relpath(full, self.root).replace(os.sep, "/"))
        return sorted(out)

    def read(self, key: str) -> bytes:
        with open(os.path.join(self.root, key), "rb") as fh:
            return fh.read()


class GcsLake:
    def __init__(self, root: str, client: Any | None = None):
        if not root.startswith("gs://"):
            raise ValueError("GcsLake necesita gs://bucket")
        rest = root[5:].rstrip("/")
        self.bucket, _, base = rest.partition("/")
        self.base = f"{base}/" if base else ""
        if client is None:
            try:
                from google.cloud import storage
            except ImportError as exc:  # pragma: no cover
                raise InfraError("falta google-cloud-storage") from exc
            try:
                client = storage.Client()
            except Exception as exc:  # noqa: BLE001
                raise InfraError(f"sin credenciales de GCP: {exc}") from exc
        self.client = client

    def list(self, prefix: str) -> list[str]:
        try:
            blobs = self.client.list_blobs(self.bucket, prefix=self.base + prefix)
            names = [b.name for b in blobs]
        except Exception as exc:  # noqa: BLE001
            raise InfraError(f"no se pudo listar gs://{self.bucket}/{self.base}{prefix}: {exc}") from exc
        return sorted(n[len(self.base):] for n in names if not n.rsplit("/", 1)[-1].startswith("."))

    def read(self, key: str) -> bytes:
        return self.client.bucket(self.bucket).blob(self.base + key).download_as_bytes()


def open_lake(root: str, client: Any | None = None) -> Lake:
    return GcsLake(root, client) if root.startswith("gs://") else LocalLake(root)


def hour_prefixes(layer: str, source: str, since: datetime, until: datetime) -> list[str]:
    """Prefijos ``{layer}/source=…/dt=…/hr=…/`` entre dos instantes (horas completas)."""
    cur = since.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    out = []
    while cur <= until:
        out.append(f"{layer}/source={source}/dt={cur:%Y-%m-%d}/hr={cur:%H}/")
        cur += timedelta(hours=1)
    return out


def file_window_start(key: str) -> datetime | None:
    m = _PART_RE.search(key.rsplit("/", 1)[-1])
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Reporte
# --------------------------------------------------------------------------- #


@dataclass
class Check:
    name: str
    status: str
    detail: str
    source: str | None = None


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def add(self, name: str, status: str, detail: str, source: str | None = None) -> None:
        self.checks.append(Check(name, status, detail, source))

    @property
    def verdict(self) -> str:
        statuses = {c.status for c in self.checks}
        if FAIL in statuses:
            return FAIL
        return WARN if WARN in statuses else OK

    def to_json(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "checks": [c.__dict__ for c in self.checks],
            "stats": self.stats,
        }

    def to_text(self) -> str:
        lines = []
        for c in self.checks:
            tag = {OK: "OK  ", WARN: "AVISO", FAIL: "FALLA"}[c.status]
            where = f"[{c.source}] " if c.source else ""
            lines.append(f"{tag} {where}{c.name}: {c.detail}")
        lines.append(f"Veredicto: {self.verdict.upper()}")
        return "\n".join(lines)


def _percentile(values: list[int], q: float) -> int | None:
    if not values:
        return None
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[idx]


@dataclass
class Thresholds:
    min_rows: int = 1
    max_extra_copies: int = 0
    min_coverage: float = 0.95
    settle_seconds: int = 600
    max_p95_latency_s: int = 300
    max_bq_dup_ratio: float = 0.01
    max_nlp_null_ratio: float = 0.01


def read_enriched(lake: Lake, source: str, since: datetime, until: datetime) -> tuple[list[dict], list[str], list[str]]:
    """Filas, archivos leídos y archivos ilegibles de ``enriched/`` para una fuente."""
    rows: list[dict] = []
    files: list[str] = []
    bad: list[str] = []
    for prefix in hour_prefixes("enriched", source, since, until):
        for key in lake.list(prefix):
            if not key.endswith(".parquet"):
                continue
            files.append(key)
            try:
                rows.extend(parquet_bytes_to_rows(lake.read(key)))
            except Exception:  # noqa: BLE001
                bad.append(key)
    return rows, files, bad


def read_raw(lake: Lake, source: str, since: datetime, until: datetime, settle_before: datetime) -> dict[str, dict]:
    """``event_id`` → ``{"user_id", "source"}`` de ``raw/`` en ventanas que empiezan desde ``since``
    y ya cerraron antes de ``settle_before``.

    Las ventanas anteriores a ``since`` se ignoran aunque sean de la misma hora: pueden venir de
    un pipeline anterior sin la rama enriched (p. ej. antes de enganchar o de cambiar de imagen),
    y contarían como eventos sin su fila en ``enriched/``.
    """
    out: dict[str, dict] = {}
    for prefix in hour_prefixes("raw", source, since, until):
        for key in lake.list(prefix):
            if not key.endswith(".jsonl.gz"):
                continue
            start = file_window_start(key)
            if start is not None and (start >= settle_before or start < since):
                continue
            try:
                text = gzip.decompress(lake.read(key)).decode("utf-8")
            except Exception:  # noqa: BLE001
                continue
            for line in text.splitlines():
                try:
                    env = json.loads(line)
                    out[env["event_id"]] = {"user_id": (env.get("author") or {}).get("user_id"), "source": env.get("source")}
                except (ValueError, KeyError, TypeError):
                    continue
    return out


def check_lake(
    lake: Lake,
    *,
    hours: float,
    now: datetime,
    window_seconds: int = 300,
    thresholds: Thresholds | None = None,
    sources: Iterable[str] = SOURCES,
    report: Report | None = None,
    expect_nlp: bool = False,
) -> tuple[Report, dict[str, set[str]]]:
    """Revisa ``enriched/`` y su cruce con ``raw/``. Devuelve el reporte y los ids por fuente."""
    th = thresholds or Thresholds()
    rep = report or Report()
    since = now - timedelta(hours=hours)
    settle_before = now - timedelta(seconds=max(th.settle_seconds, 2 * window_seconds))
    ids_by_source: dict[str, set[str]] = {}
    user_to_hashes: dict[str, set[str]] = defaultdict(set)
    hash_to_users: dict[str, set[str]] = defaultdict(set)

    for source in sources:
        # Una hora más atrás en enriched/ para no perder eventos del borde.
        rows, files, bad = read_enriched(lake, source, since - timedelta(hours=1), now)
        recent = [r for r in rows if (parse_ts(r.get("process_time")) or now) >= since]
        counts = Counter(r["event_id"] for r in rows)
        extra = sum(c - 1 for c in counts.values() if c > 1)
        ids = set(counts)
        ids_by_source[source] = {r["event_id"] for r in recent}
        latencies = []
        problems: Counter = Counter()
        for r in rows:
            for p in row_problems(r):
                problems[p] += 1
            et, pt = parse_ts(r.get("event_time")), parse_ts(r.get("process_time"))
            if et and pt:  # incluye los tardíos si el caos está encendido
                latencies.append(int((pt - et).total_seconds() * 1000))
        rep.stats[source] = {
            "files": len(files),
            "unreadable_files": len(bad),
            "rows": len(rows),
            "rows_in_window": len(recent),
            "unique_event_ids": len(ids),
            "extra_copies": extra,
            "latency_ms_p50": _percentile(latencies, 0.5),
            "latency_ms_p95": _percentile(latencies, 0.95),
        }

        if len(recent) < th.min_rows:
            rep.add("filas recientes", FAIL, f"{len(recent)} filas en las últimas {hours:g} h (mínimo {th.min_rows})", source)
            continue
        rep.add("filas recientes", OK, f"{len(recent)} filas en {len(files)} archivos", source)
        rep.add("archivos legibles", FAIL if bad else OK, f"{len(bad)} ilegibles" if bad else "todos", source)

        cols_ok = all(tuple(r.keys()) == COLUMN_NAMES for r in rows)
        rep.add("columnas = posts_enriched", OK if cols_ok else FAIL, "coinciden" if cols_ok else "difieren", source)
        id_cols = [k for k in IDENTITY_KEYS if any(k in r for r in rows)]
        rep.add("sin identidad en claro", FAIL if id_cols or problems.get("identidad dentro de payload") else OK,
                f"columnas {id_cols}" if id_cols else "ninguna columna ni clave user_id/handle", source)
        rule_problems = {p: n for p, n in problems.items() if not p.startswith("columna") and p != "identidad dentro de payload"}
        rep.add("reglas por fila (hash, engagement, brand_primary)", FAIL if rule_problems else OK,
                json.dumps(rule_problems, ensure_ascii=False) if rule_problems else "todas cumplen", source)
        rep.add("copias de event_id", FAIL if extra > th.max_extra_copies else OK, f"{extra} copias", source)

        raw = read_raw(lake, source, since, now, settle_before)
        joined = 0
        by_id = {r["event_id"]: r for r in rows}
        for eid, info in raw.items():
            r = by_id.get(eid)
            if r is None or not info.get("user_id"):
                continue
            joined += 1
            user = str(info["user_id"])  # el hash depende solo de user_id
            user_to_hashes[user].add(r["user_hash"])
            hash_to_users[r["user_hash"]].add(user)
        if raw:
            coverage = joined / len(raw)
            status = OK if coverage >= th.min_coverage else (WARN if coverage >= th.min_coverage - 0.05 else FAIL)
            rep.add("cobertura raw → enriched", status, f"{joined}/{len(raw)} ({coverage:.1%})", source)
            rep.stats[source]["raw_events"] = len(raw)
            rep.stats[source]["joined"] = joined
        else:
            rep.add("cobertura raw → enriched", WARN, "sin archivos de raw/ cerrados en la ventana", source)

        if expect_nlp:
            _check_nlp(rep, source, recent, th)

        p95 = rep.stats[source]["latency_ms_p95"]
        if p95 is not None:
            status = OK if p95 <= th.max_p95_latency_s * 1000 else WARN
            rep.add("latencia event_time → process_time (p95)", status, f"{p95 / 1000:.1f} s", source)

    multi_hash = sum(1 for v in user_to_hashes.values() if len(v) > 1)
    collisions = sum(1 for v in hash_to_users.values() if len(v) > 1)
    if user_to_hashes:
        status = FAIL if multi_hash or collisions else OK
        rep.add(
            "user_hash determinista (1 a 1)",
            status,
            f"{len(user_to_hashes)} usuarios; {multi_hash} con más de un hash; {collisions} hashes compartidos",
        )
    else:
        rep.add("user_hash determinista (1 a 1)", WARN, "no se pudo cruzar con raw/")
    return rep, ids_by_source


def _check_nlp(rep: Report, source: str, rows: list[dict], th: Thresholds) -> None:
    """NLP del Lote 2.3: cobertura, etiquetas, scores, versión y concordancia (informativa)."""
    with_nlp = [r for r in rows if all(r.get(k) is not None for k in NLP_COLUMNS)]
    null_ratio = 1 - len(with_nlp) / len(rows) if rows else 1.0
    status = OK if null_ratio <= th.max_nlp_null_ratio else FAIL
    rep.add("NLP presente", status, f"{len(with_nlp)}/{len(rows)} filas con sentimiento, emoción y versión", source)
    problems: Counter = Counter()
    for r in with_nlp:
        for p in nlp_row_problems(r):
            problems[p] += 1
    rep.add("NLP: etiquetas, scores y versión", FAIL if problems else OK,
            json.dumps(dict(problems), ensure_ascii=False) if problems else "todas cumplen", source)
    versions = Counter(r["nlp_model_version"] for r in with_nlp)
    if len(versions) > 1:
        rep.add("NLP: una sola versión de modelos", WARN, f"{len(versions)} versiones (¿cambio de imagen?)", source)
    labelled = [r for r in with_nlp if r.get("label_sentiment") in SENTIMENTS]
    agree = sum(1 for r in labelled if r["sentiment_label"] == r["label_sentiment"])
    st = rep.stats[source]
    st["sentiment_counts"] = dict(Counter(r["sentiment_label"] for r in with_nlp))
    st["emotion_counts"] = dict(Counter(r["emotion_label"] for r in with_nlp))
    st["nlp_model_versions"] = dict(versions)
    st["sentiment_vs_ground_truth"] = {"labelled": len(labelled), "agree": agree, "accuracy": round(agree / len(labelled), 4) if labelled else None}


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #

BQ_STATS_SQL = """
SELECT
  source,
  COUNT(*) AS n,
  COUNT(DISTINCT event_id) AS ids,
  COUNTIF(user_hash IS NULL OR NOT REGEXP_CONTAINS(user_hash, r'^[0-9a-f]{{64}}$')) AS bad_hash,
  COUNTIF(source IN ('reddit', 'instagram') AND shares IS NOT NULL) AS bad_shares,
  COUNTIF(source = 'facebook' AND shares IS NULL) AS missing_shares,
  COUNTIF(likes IS NULL) AS null_likes,
  COUNTIF(IFNULL(brand_primary, '') != IFNULL(brands[SAFE_OFFSET(0)], '')) AS bad_brand_primary,
  COUNTIF(sentiment_label IS NULL OR emotion_label IS NULL OR nlp_model_version IS NULL) AS null_nlp,
  COUNTIF(sentiment_label IS NOT NULL AND sentiment_label NOT IN ('negative', 'neutral', 'positive')) AS bad_sentiment,
  COUNTIF(emotion_label IS NOT NULL AND emotion_label NOT IN ('joy', 'sadness', 'anger', 'fear', 'disgust', 'surprise', 'neutral')) AS bad_emotion
FROM `{table}`
WHERE process_time >= @since
  AND event_time >= TIMESTAMP_SUB(@since, INTERVAL 1 DAY)
GROUP BY source
"""

BQ_IDS_SQL = """
SELECT source, event_id
FROM `{table}`
WHERE process_time >= @since
  AND event_time >= TIMESTAMP_SUB(@since, INTERVAL 1 DAY)
"""

BQ_COLUMNS_SQL = """
SELECT column_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = @table
ORDER BY ordinal_position
"""


def _bq_query(client: Any, sql: str, params: dict[str, Any]) -> list[dict]:
    from google.cloud import bigquery

    qp = []
    for name, value in params.items():
        kind = "TIMESTAMP" if isinstance(value, datetime) else "STRING"
        qp.append(bigquery.ScalarQueryParameter(name, kind, value))
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    return [dict(r.items()) for r in job.result()]


def check_bigquery(
    table: str,
    *,
    since: datetime,
    lake_ids: dict[str, set[str]],
    report: Report,
    thresholds: Thresholds | None = None,
    query: Any = None,
    expect_nlp: bool = False,
) -> Report:
    """Revisa ``posts_enriched``. ``table`` = ``proyecto.dataset.tabla``.

    ``query(sql, params) -> list[dict]`` se puede inyectar en las pruebas.
    """
    th = thresholds or Thresholds()
    project, dataset, name = table.replace(":", ".").split(".")
    fq = f"{project}.{dataset}.{name}"
    if query is None:
        try:
            from google.cloud import bigquery
        except ImportError as exc:  # pragma: no cover
            raise InfraError("falta google-cloud-bigquery") from exc
        try:
            client = bigquery.Client(project=project)
        except Exception as exc:  # noqa: BLE001
            raise InfraError(f"sin credenciales de GCP: {exc}") from exc

        def query(sql, params):  # noqa: E306
            return _bq_query(client, sql, params)

    try:
        cols = [r["column_name"] for r in query(BQ_COLUMNS_SQL.format(project=project, dataset=dataset), {"table": name})]
    except Exception as exc:  # noqa: BLE001
        raise InfraError(f"no se pudo consultar {fq}: {exc}") from exc
    if not cols:
        raise InfraError(f"la tabla {fq} no existe (¿terraform apply del Lote 2.2?)")
    report.add("BigQuery: columnas", OK if tuple(cols) == COLUMN_NAMES or set(cols) == set(COLUMN_NAMES) else FAIL,
               "coinciden con posts_enriched" if set(cols) == set(COLUMN_NAMES) else f"difieren: {sorted(set(cols) ^ set(COLUMN_NAMES))}")
    leaked = [c for c in cols if c in IDENTITY_KEYS]
    report.add("BigQuery: sin identidad", FAIL if leaked else OK, f"columnas {leaked}" if leaked else "sin user_id ni handle")

    stats = {r["source"]: r for r in query(BQ_STATS_SQL.format(table=fq), {"since": since})}
    ids_rows = query(BQ_IDS_SQL.format(table=fq), {"since": since})
    bq_ids: dict[str, set[str]] = defaultdict(set)
    for r in ids_rows:
        bq_ids[r["source"]].add(r["event_id"])
    report.stats["bigquery"] = {s: dict(v) for s, v in stats.items()}

    for source, lids in lake_ids.items():
        st = stats.get(source)
        if not st or not st.get("n"):
            report.add("BigQuery: filas recientes", FAIL, "0 filas", source)
            continue
        n, ids = int(st["n"]), int(st["ids"])
        dup_ratio = (n - ids) / n if n else 0.0
        status = OK if n == ids else (WARN if dup_ratio <= th.max_bq_dup_ratio else FAIL)
        report.add("BigQuery: filas recientes", status, f"{n} filas, {ids} ids únicos", source)
        bad = {k: int(st[k]) for k in ("bad_hash", "bad_shares", "missing_shares", "null_likes", "bad_brand_primary") if int(st.get(k) or 0)}
        report.add("BigQuery: reglas", FAIL if bad else OK, json.dumps(bad) if bad else "todas cumplen", source)
        if expect_nlp:
            null_nlp = int(st.get("null_nlp") or 0)
            bad_labels = int(st.get("bad_sentiment") or 0) + int(st.get("bad_emotion") or 0)
            ratio = null_nlp / n if n else 1.0
            status = FAIL if bad_labels or ratio > th.max_nlp_null_ratio else OK
            report.add("BigQuery: NLP", status, f"{n - null_nlp}/{n} con NLP; {bad_labels} etiquetas inválidas", source)
        if lids:
            covered = len(lids & bq_ids.get(source, set()))
            cov = covered / len(lids)
            status = OK if cov >= th.min_coverage else (WARN if cov >= th.min_coverage - 0.05 else FAIL)
            report.add("cobertura enriched → BigQuery", status, f"{covered}/{len(lids)} ({cov:.1%})", source)
    return report
