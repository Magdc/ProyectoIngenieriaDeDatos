-- ta-curated-precision-nlp (Lote 4.3, ARCHITECTURE.md §3.22.1): matriz de confusión del NLP frente a la etiqueta del
-- generador (label_sentiment), solo con las dos etiquetas presentes (§5.10, KPI «Precisión NLP»; insumo de HU-033).
SELECT
  label_sentiment AS esperado,
  sentiment_label AS predicho,
  COUNT(*) AS posts,
  round(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY label_sentiment), 1) AS pct_del_esperado
FROM ta_curated.fact_posts
WHERE label_sentiment IS NOT NULL AND sentiment_label IS NOT NULL
GROUP BY label_sentiment, sentiment_label
ORDER BY esperado, predicho
