-- ta-curated-emociones-por-hora-bogota (Lote 4.3, ARCHITECTURE.md §3.22.1): reparto de emociones por marca y hora
-- del día en America/Bogota (se guarda en UTC y se muestra en Bogotá, §5.12), últimos 7 días con datos.
-- agg_emotion_by_brand_hour no tiene las filas sin emoción (decisión del usuario, v2.37).
WITH por_hora AS (
  SELECT
    b.display AS marca,
    hour(e.hour_start AT TIME ZONE 'America/Bogota') AS hora_bogota,
    e.emotion_label,
    SUM(e.n_posts) AS posts
  FROM ta_curated.agg_emotion_by_brand_hour e
  JOIN ta_curated.dim_brand b ON b.brand = e.brand
  WHERE e.hour_start > (SELECT max(hour_start) FROM ta_curated.agg_emotion_by_brand_hour) - INTERVAL '7' DAY
  GROUP BY 1, 2, 3
)
SELECT
  marca,
  hora_bogota,
  emotion_label,
  posts,
  round(100.0 * posts / SUM(posts) OVER (PARTITION BY marca, hora_bogota), 1) AS pct
FROM por_hora
ORDER BY marca, hora_bogota, posts DESC
