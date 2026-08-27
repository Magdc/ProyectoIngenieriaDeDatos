"""
src/profiling/synthetic_data.py

Generador de datos sinteticos del esquema comun de ingesta.
===========================================================
Produce archivos JSON en la misma estructura de directorios que
usan los collectors en GCS: <fuente>/YYYY/MM/DD/raw_HHMMSS.json

Uso:
    python synthetic_data.py                    # genera sample_data/ en el directorio actual
    python synthetic_data.py --output /tmp/raw  # directorio personalizado
    python synthetic_data.py --records 200      # N registros por fuente

Los datos incluyen intencionalmente anomalias para que el profiler
y las reglas de calidad tengan algo que detectar:
  - ~5% de nulos en campos opcionales (language, author_name)
  - ~2% de ids duplicados (simula reprocesamiento)
  - ~3% de registros con texto vacio (simula error de limpieza HTML)
  - ~1% de fechas con offset Colombia sin normalizar (simula bug en RSS)
"""

import argparse
import json
import os
import random
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Semilla para reproducibilidad
random.seed(42)

# ---------------------------------------------------------------------------
# Datos de muestra por fuente
# ---------------------------------------------------------------------------
SAMPLE_TAGS = {
    "mastodon": ["colombia", "cafe", "bogota", "medellin", "economia",
                 "mercado", "bebidas", "exportacion", "agro", "startup"],
    "reddit":   ["colombia", "cafe", "bogota", "medellin", "ColombiaNoticias",
                 "emprendimiento", "economia", "consumo"],
    "news":     ["Economia", "Sectores", "Empresas", "Agroindustria",
                 "Mercados", "Bebidas", "Cafe"],
}

SAMPLE_TEXTS = [
    "Drummond renuncia a su regasificadora en medio de la crisis energetica",
    "El Nino podria impulsar el precio del cafe colombiano en 2026",
    "Postobón lanza nueva linea de bebidas naturales para el mercado latinoamericano",
    "Exportaciones de cafe superan los 1000 millones de dolares en el primer semestre",
    "Bavaria y Heineken compiten por el mercado de cervezas artesanales en Bogota",
    "Jugos Hit refuerza su presencia en tiendas de barrio con nueva campaña",
    "El mercado de bebidas energeticas crece 35% en Colombia durante 2026",
    "Nescafe y Juan Valdez se disputan el segmento premium del cafe instantaneo",
    "Crisis del agua afecta produccion de refrescos en la Costa Caribe",
    "Colombia apuesta por la exportacion de agua de coco al mercado europeo",
    "La industria de lacteos colombiana busca nuevos mercados en Asia",
    "Alpina diversifica su portafolio con bebidas vegetales para el 2026",
    "Bebidas Olympica expande su red de distribucion en ciudades intermedias",
    "El mercado del te crece en Colombia impulsado por tendencias de salud",
    "Quala lanza nueva linea de jugos con bajo contenido de azucar",
]

SAMPLE_AUTHORS = [
    "economistacol", "mercados_co", "agronews", "cafetero_digital",
    "bogota_news", "tendencias_col", "consumo_masivo", "analista_bebidas",
    "mediosbiz", "finanzascol", None, None,  # ~15% nulos
]

BASE_DATE = datetime(2026, 8, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Generadores por fuente
# ---------------------------------------------------------------------------
def _random_id(length: int = 18) -> str:
    return "".join(random.choices(string.digits, k=length))


def _random_date(offset_days: int = 30) -> str:
    delta = timedelta(
        days=random.randint(0, offset_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    dt = BASE_DATE + delta
    # ~1% fechas con offset Colombia (simula bug en RSS sin normalizar)
    if random.random() < 0.01:
        from datetime import timezone as tz
        import zoneinfo
        dt_col = dt.astimezone(zoneinfo.ZoneInfo("America/Bogota"))
        return dt_col.isoformat()
    return dt.isoformat()


def _random_tags(source: str, n: int = 3) -> list[str]:
    pool = SAMPLE_TAGS[source]
    return random.sample(pool, min(n, len(pool)))


def generate_mastodon_record(duplicate_id: str | None = None) -> dict:
    rid = duplicate_id or _random_id(18)
    text = random.choice(SAMPLE_TEXTS)
    # ~3% texto vacio
    if random.random() < 0.03:
        text = ""
    return {
        "source": "mastodon",
        "id": rid,
        "created_at": _random_date(),
        "text": text,
        "url": f"https://mastodon.social/@user_{random.randint(1000,9999)}/{rid}",
        "language": random.choice(["es", "es", "es", "en", None]),
        "tags": _random_tags("mastodon"),
        "author_name": random.choice(SAMPLE_AUTHORS),
        "author_id": str(random.randint(10**15, 10**16 - 1)),
        "engagement": {
            "replies":  random.randint(0, 20),
            "reposts":  random.randint(0, 50),
            "likes":    random.randint(0, 200),
        },
        "raw_metadata": {
            "visibility": "public",
            "sensitive": False,
        },
    }


def generate_reddit_record(duplicate_id: str | None = None) -> dict:
    subreddit = random.choice(["colombia", "bogota", "cafe", "Colombia_news"])
    short_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    fullname = duplicate_id or f"t3_{short_id}"
    text = random.choice(SAMPLE_TEXTS)
    if random.random() < 0.03:
        text = ""
    return {
        "source": "reddit",
        "id": fullname,
        "created_at": _random_date(),
        "text": text,
        "url": f"https://www.reddit.com/r/{subreddit}/comments/{short_id}/",
        "language": None,
        "tags": [subreddit],
        "author_name": random.choice(SAMPLE_AUTHORS),
        "author_id": None,
        "engagement": {
            "replies": random.randint(0, 100),
            "reposts": 0,
            "likes":   random.randint(-5, 500),  # Reddit score puede ser bajo
        },
        "raw_metadata": {
            "subreddit": subreddit,
            "upvote_ratio": round(random.uniform(0.5, 1.0), 2),
        },
    }


def generate_news_record(duplicate_id: str | None = None) -> dict:
    feeds = [
        "https://www.eltiempo.com/rss/economia.xml",
        "https://www.elcolombiano.com/rss/economia.xml",
        "https://www.portafolio.co/rss/portafolio.xml",
    ]
    feed = random.choice(feeds)
    domain = feed.split("/")[2]
    slug = "-".join(random.choice(SAMPLE_TEXTS).lower().split()[:6])
    url = duplicate_id or f"https://{domain}/economia/sectores/{slug}-{random.randint(1000000,9999999)}"
    text = random.choice(SAMPLE_TEXTS)
    if random.random() < 0.03:
        text = ""
    return {
        "source": "news",
        "id": url,
        "created_at": _random_date(),
        "text": text,
        "url": url,
        "language": None,
        "tags": _random_tags("news", 2),
        "author_name": None,
        "author_id": None,
        "engagement": {
            "replies": None,
            "reposts": None,
            "likes":   None,
        },
        "raw_metadata": {
            "feed_url": feed,
        },
    }


# ---------------------------------------------------------------------------
# Generacion y escritura
# ---------------------------------------------------------------------------
GENERATORS = {
    "mastodon": generate_mastodon_record,
    "reddit":   generate_reddit_record,
    "news":     generate_news_record,
}


def generate_dataset(n_per_source: int = 100, output_dir: str = "sample_data") -> None:
    """Genera N registros por fuente y los escribe en output_dir/<fuente>/YYYY/MM/DD/."""
    base = Path(output_dir)
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S")

    for source, gen_fn in GENERATORS.items():
        records = []
        ids: list[str] = []

        for i in range(n_per_source):
            # ~2% duplicados
            dup_id = ids[random.randint(0, len(ids) - 1)] if ids and random.random() < 0.02 else None
            record = gen_fn(duplicate_id=dup_id)
            records.append(record)
            ids.append(record["id"])

        out_path = base / source / date_path / f"raw_{timestamp}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"  [{source}] {len(records)} registros -> {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera datos sinteticos del esquema comun")
    parser.add_argument("--output",  default="sample_data", help="Directorio de salida")
    parser.add_argument("--records", type=int, default=100, help="Registros por fuente")
    args = parser.parse_args()

    print(f"Generando {args.records} registros por fuente en '{args.output}/'...")
    generate_dataset(n_per_source=args.records, output_dir=args.output)
    print("Listo.")
