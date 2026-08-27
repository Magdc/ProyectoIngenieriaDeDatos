"""
src/profiling/profiler.py

Cloud Run Job — Data Profiler del Raw Layer (US-14)
====================================================
Lee archivos JSON del Raw layer, calcula las metricas definidas en el
TODO.md y genera un reporte Markdown que guarda en GCS.

Modos de operacion:
  1. LOCAL  — lee desde un directorio local (--input DIR)
              Util para CI, desarrollo y pruebas sin credenciales GCP.
  2. GCS    — lee desde Cloud Storage (--gcs-bucket BUCKET)
              Modo usado en produccion cuando corre como Cloud Run Job.

Variables de entorno (Cloud Run Job):
  RAW_BUCKET_NAME   — bucket GCS del Raw layer
  GCP_PROJECT_ID    — ID del proyecto GCP
  ENVIRONMENT       — dev | qa | prod

Uso CLI:
  # Local con datos sinteticos
  python synthetic_data.py --output sample_data
  python profiler.py --input sample_data --output profiling_report.md

  # GCS (requiere ADC configurado)
  python profiler.py --gcs-bucket trend-dev-xxx-raw --output profiling_report.md

Metricas calculadas por fuente (segun US-14):
  1. Conteo de registros totales
  2. Distribucion por fecha (created_at)
  3. % de valores nulos por campo
  4. Conteo de duplicados (id repetido)
  5. Distribucion de idiomas (language)
  6. Longitud promedio y mediana de text
  7. Top 20 tags mas frecuentes
  + KPIs: completitud >= 95%, unicidad >= 99%, duplicados < 1%
"""

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Agregar src/ al path para importar el modulo de calidad
sys.path.insert(0, str(Path(__file__).parent.parent / "quality"))
from validator import validate_dataframe

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SOURCES = ["mastodon", "reddit", "news"]
SCHEMA_FIELDS = [
    "source", "id", "created_at", "text", "url",
    "language", "tags", "author_name", "engagement",
]
KPI_COMPLETENESS = 0.95
KPI_UNIQUENESS   = 0.99
KPI_DUPLICATES   = 0.01


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------
def load_from_local(input_dir: str) -> dict[str, pd.DataFrame]:
    """Lee todos los JSON de input_dir/<fuente>/ y retorna un dict de DataFrames."""
    dfs: dict[str, pd.DataFrame] = {}
    base = Path(input_dir)

    for source in SOURCES:
        source_path = base / source
        if not source_path.exists():
            print(f"  [WARN] No se encontro directorio para fuente '{source}': {source_path}")
            continue

        records: list[dict] = []
        for json_file in source_path.rglob("*.json"):
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)

        if not records:
            print(f"  [WARN] Sin registros para fuente '{source}'")
            continue

        dfs[source] = pd.DataFrame(records)
        print(f"  [{source}] {len(records)} registros cargados")

    return dfs


def load_from_gcs(bucket_name: str, project: str) -> dict[str, pd.DataFrame]:
    """Lee JSON desde GCS usando google-cloud-storage."""
    from google.cloud import storage
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    dfs: dict[str, pd.DataFrame] = {}

    for source in SOURCES:
        records: list[dict] = []
        blobs = bucket.list_blobs(prefix=f"{source}/")
        for blob in blobs:
            if not blob.name.endswith(".json"):
                continue
            data = json.loads(blob.download_as_text(encoding="utf-8"))
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)

        if records:
            dfs[source] = pd.DataFrame(records)
            print(f"  [{source}] {len(records)} registros desde GCS")
        else:
            print(f"  [WARN] Sin datos GCS para fuente '{source}'")

    return dfs


# ---------------------------------------------------------------------------
# Metricas
# ---------------------------------------------------------------------------
def calc_null_pct(df: pd.DataFrame) -> dict[str, float]:
    """% de nulos por campo del esquema comun."""
    result = {}
    for field in SCHEMA_FIELDS:
        if field in df.columns:
            null_count = df[field].isna().sum()
            result[field] = round(null_count / len(df) * 100, 2)
        else:
            result[field] = 100.0   # campo ausente = 100% nulo
    return result


def calc_duplicates(df: pd.DataFrame) -> tuple[int, float]:
    """Cuenta ids duplicados y retorna (count, pct)."""
    if "id" not in df.columns:
        return 0, 0.0
    dup_count = int(df["id"].duplicated().sum())
    dup_pct = round(dup_count / len(df) * 100, 2)
    return dup_count, dup_pct


def calc_date_distribution(df: pd.DataFrame) -> dict[str, int]:
    """Cuenta registros por fecha (YYYY-MM-DD) en created_at."""
    if "created_at" not in df.columns:
        return {}
    dates = df["created_at"].dropna().astype(str).str[:10]
    return dict(dates.value_counts().sort_index())


def calc_language_distribution(df: pd.DataFrame) -> dict[str, int]:
    """Distribucion de valores del campo language."""
    if "language" not in df.columns:
        return {}
    dist = df["language"].fillna("null").value_counts()
    return dict(dist)


def calc_text_stats(df: pd.DataFrame) -> dict[str, Any]:
    """Longitud promedio y mediana del campo text."""
    if "text" not in df.columns:
        return {"mean": None, "median": None, "empty_count": 0}
    lengths = df["text"].fillna("").astype(str).str.len()
    return {
        "mean":        round(float(lengths.mean()), 1),
        "median":      round(float(lengths.median()), 1),
        "empty_count": int((lengths == 0).sum()),
    }


def calc_top_tags(df: pd.DataFrame, top_n: int = 20) -> list[tuple[str, int]]:
    """Top N tags mas frecuentes."""
    if "tags" not in df.columns:
        return []
    all_tags: list[str] = []
    for val in df["tags"].dropna():
        if isinstance(val, list):
            all_tags.extend(str(t) for t in val)
        elif isinstance(val, str) and val.startswith("["):
            try:
                tags = json.loads(val.replace("'", '"'))
                all_tags.extend(str(t) for t in tags)
            except Exception:
                all_tags.append(val)
    counter = Counter(all_tags)
    return counter.most_common(top_n)


def check_kpis(
    total: int,
    null_pct: dict[str, float],
    dup_count: int,
    dup_pct: float,
) -> dict[str, bool]:
    """Verifica los KPIs del proyecto contra los umbrales definidos."""
    # Completitud: promedio de no-nulos en campos obligatorios
    required = ["id", "source", "created_at", "text", "url"]
    avg_null = sum(null_pct.get(f, 100) for f in required) / len(required)
    completeness = 1 - avg_null / 100

    return {
        "completeness_ok": completeness >= KPI_COMPLETENESS,
        "uniqueness_ok":   dup_pct / 100 <= (1 - KPI_UNIQUENESS),
        "duplicates_ok":   dup_pct / 100 < KPI_DUPLICATES,
        "completeness":    round(completeness * 100, 2),
        "uniqueness":      round((1 - dup_pct / 100) * 100, 2),
        "duplicate_pct":   dup_pct,
    }


# ---------------------------------------------------------------------------
# Generacion del reporte Markdown
# ---------------------------------------------------------------------------
def build_report(
    all_metrics: dict[str, dict],
    generated_at: str,
) -> str:
    lines = [
        "# Data Profiling Report — TrendAnalyzer Raw Layer",
        "",
        f"> Generado: {generated_at}  ",
        f"> Fuentes analizadas: {', '.join(all_metrics.keys())}",
        "",
        "---",
        "",
    ]

    for source, m in all_metrics.items():
        total = m["total"]
        kpis = m["kpis"]

        lines += [
            f"## Fuente: `{source}`",
            "",
            f"**Total de registros:** {total:,}",
            "",
            "### KPIs del proyecto",
            "",
            "| KPI | Valor | Umbral | Estado |",
            "|---|---|---|---|",
            f"| Completitud | {kpis['completeness']}% | >= 95% | {'OK' if kpis['completeness_ok'] else 'FALLO'} |",
            f"| Unicidad    | {kpis['uniqueness']}% | >= 99% | {'OK' if kpis['uniqueness_ok'] else 'FALLO'} |",
            f"| Duplicados  | {kpis['duplicate_pct']}% | < 1%   | {'OK' if kpis['duplicates_ok'] else 'FALLO'} |",
            "",
            "### % Nulos por campo",
            "",
            "| Campo | % Nulo |",
            "|---|---|",
        ]
        for field, pct in m["null_pct"].items():
            flag = " !!!" if pct > 5 and field in ["id", "source", "created_at", "text", "url"] else ""
            lines.append(f"| `{field}` | {pct}%{flag} |")

        lines += [
            "",
            f"### Texto — longitud",
            "",
            f"- Promedio: **{m['text_stats']['mean']}** caracteres",
            f"- Mediana:  **{m['text_stats']['median']}** caracteres",
            f"- Registros con texto vacio: **{m['text_stats']['empty_count']}**",
            "",
            "### Distribucion por fecha (created_at)",
            "",
            "| Fecha | Registros |",
            "|---|---|",
        ]
        for date, count in list(m["date_dist"].items())[:10]:
            lines.append(f"| {date} | {count} |")
        if len(m["date_dist"]) > 10:
            lines.append(f"| ... | ({len(m['date_dist']) - 10} fechas mas) |")

        lines += [
            "",
            "### Distribucion de idiomas",
            "",
            "| Idioma | Registros |",
            "|---|---|",
        ]
        for lang, count in m["lang_dist"].items():
            lines.append(f"| {lang} | {count} |")

        lines += [
            "",
            "### Top 20 tags",
            "",
            "| Tag | Frecuencia |",
            "|---|---|",
        ]
        for tag, freq in m["top_tags"]:
            lines.append(f"| `{tag}` | {freq} |")

        # Calidad
        qr = m["quality"]
        lines += [
            "",
            "### Validacion de calidad (reglas US-13)",
            "",
            f"- Registros validos:   **{qr.valid}** ({round(qr.valid/total*100,1)}%)",
            f"- Registros invalidos: **{qr.invalid}** ({round(qr.invalid/total*100,1)}%)",
            f"- Alerta activada:     **{'SI' if qr.alert_triggered else 'No'}**",
        ]
        if qr.errors_summary:
            lines += ["", "| Regla violada | Ocurrencias |", "|---|---|"]
            for rule, cnt in sorted(qr.errors_summary.items()):
                lines.append(f"| {rule} | {cnt} |")

        lines += ["", "---", ""]

    lines += [
        "## Hallazgos y recomendaciones",
        "",
        "*(Completar manualmente con los hallazgos especificos luego de revisar el reporte.)*",
        "",
        "- [ ] Campos con > 5% de nulos en datos obligatorios → revisar logica de mapeo en el collector",
        "- [ ] Duplicados > 1% → agregar deduplicacion por ventana de tiempo en Dataflow",
        "- [ ] Textos vacios → revisar strip_html() y casos de toots solo con imagen",
        "- [ ] Fechas no normalizadas → verificar normalize_date() en el conector RSS",
        "",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Guardar reporte en GCS
# ---------------------------------------------------------------------------
def save_report_gcs(report: str, bucket_name: str, project: str) -> str:
    from google.cloud import storage
    now = datetime.now(timezone.utc)
    blob_name = f"profiling/{now.strftime('%Y/%m/%d')}/report_{now.strftime('%H%M%S')}.md"
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_string(
        report.encode("utf-8"), content_type="text/markdown; charset=utf-8"
    )
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    print(f"Reporte guardado en GCS: {gcs_uri}")
    return gcs_uri


# ---------------------------------------------------------------------------
# Entrypoint principal
# ---------------------------------------------------------------------------
def run(input_dir: str | None, gcs_bucket: str | None, output_file: str) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()

    print("Cargando datos...")
    if gcs_bucket:
        project = os.environ.get("GCP_PROJECT_ID", "")
        dfs = load_from_gcs(gcs_bucket, project)
    else:
        dfs = load_from_local(input_dir or "sample_data")

    if not dfs:
        print("ERROR: No se encontraron datos. Verifica el directorio de entrada.")
        sys.exit(1)

    print("\nCalculando metricas...")
    all_metrics: dict[str, dict] = {}

    for source, df in dfs.items():
        print(f"  Procesando {source} ({len(df)} registros)...")
        null_pct = calc_null_pct(df)
        dup_count, dup_pct = calc_duplicates(df)
        date_dist = calc_date_distribution(df)
        lang_dist = calc_language_distribution(df)
        text_stats = calc_text_stats(df)
        top_tags = calc_top_tags(df)
        kpis = check_kpis(len(df), null_pct, dup_count, dup_pct)
        quality = validate_dataframe(df)

        all_metrics[source] = {
            "total": len(df),
            "null_pct": null_pct,
            "dup_count": dup_count,
            "dup_pct": dup_pct,
            "date_dist": date_dist,
            "lang_dist": lang_dist,
            "text_stats": text_stats,
            "top_tags": top_tags,
            "kpis": kpis,
            "quality": quality,
        }

    print("\nGenerando reporte...")
    report = build_report(all_metrics, generated_at)

    # Guardar localmente
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Reporte guardado en: {output_file}")

    # Guardar en GCS si corresponde
    if gcs_bucket:
        project = os.environ.get("GCP_PROJECT_ID", "")
        save_report_gcs(report, gcs_bucket, project)

    # Imprimir resumen de KPIs en consola
    print("\n=== RESUMEN DE KPIs ===")
    for source, m in all_metrics.items():
        k = m["kpis"]
        status = "OK" if all([k["completeness_ok"], k["uniqueness_ok"], k["duplicates_ok"]]) else "FALLO"
        print(f"  {source:<12} completitud={k['completeness']}%  unicidad={k['uniqueness']}%  dup={k['duplicate_pct']}%  [{status}]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TrendAnalyzer Raw Layer Profiler")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--input",      help="Directorio local con datos (local mode)")
    group.add_argument("--gcs-bucket", help="Nombre del bucket GCS (production mode)")
    parser.add_argument("--output", default="profiling_report.md", help="Archivo de salida")
    args = parser.parse_args()

    run(
        input_dir=args.input,
        gcs_bucket=args.gcs_bucket,
        output_file=args.output,
    )
