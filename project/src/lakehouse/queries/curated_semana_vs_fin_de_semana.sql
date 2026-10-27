-- ta-curated-semana-vs-fin-de-semana (Lote 4.3, ARCHITECTURE.md §3.22.1): volumen y sentimiento medio por marca
-- entre semana y en fin de semana (fact_posts + dim_date). El filtro por dt hace que Athena lea solo esas
-- particiones (partition projection). El sentimiento medio usa la definición de avg_sentiment (sin las filas sin NLP).
SELECT
  f.brand_primary AS marca,
  d.is_weekend AS fin_de_semana,
  COUNT(*) AS posts,
  COUNT(DISTINCT d.date_key) AS dias,
  round(COUNT(*) * 1.0 / COUNT(DISTINCT d.date_key), 1) AS posts_por_dia,
  round(avg(CASE f.sentiment_label WHEN 'positive' THEN f.sentiment_score WHEN 'negative' THEN -f.sentiment_score
                                   WHEN 'neutral' THEN 0 END), 4) AS sentimiento_medio
FROM ta_curated.fact_posts f
JOIN ta_curated.dim_date d ON d.date_key = f.date_key
WHERE f.dt >= date_format(current_date - INTERVAL '90' DAY, '%Y-%m-%d')
GROUP BY f.brand_primary, d.is_weekend
ORDER BY marca, fin_de_semana
