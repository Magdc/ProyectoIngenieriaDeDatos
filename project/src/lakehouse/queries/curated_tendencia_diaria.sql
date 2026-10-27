-- ta-curated-tendencia-diaria (Lote 4.3, ARCHITECTURE.md §3.22.1): volumen y sentimiento diario por marca en los
-- últimos 30 días con datos (agg_trend_daily, día UTC, + dim_brand). n_posts cuenta también las filas sin NLP;
-- n_negative, n_neutral, n_positive y avg_sentiment solo las que tienen sentimiento (decisión del usuario, v2.37).
SELECT
  t.full_date,
  b.display AS marca,
  b.role AS rol,
  t.n_posts,
  t.n_negative + t.n_neutral + t.n_positive AS con_sentimiento,
  round(100.0 * t.n_negative / nullif(t.n_negative + t.n_neutral + t.n_positive, 0), 1) AS pct_negativo,
  t.avg_sentiment
FROM ta_curated.agg_trend_daily t
JOIN ta_curated.dim_brand b ON b.brand = t.brand
WHERE t.full_date > (SELECT max(full_date) FROM ta_curated.agg_trend_daily) - INTERVAL '30' DAY
ORDER BY t.full_date DESC, marca
