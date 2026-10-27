-- ta-curated-posts-por-origen (Lote 4.3, ARCHITECTURE.md §3.22.1): filas de fact_posts por origen y fuente.
-- Es el «Listo cuando» de la Fase 4: stream e histórico en ta_curated.fact_posts. Lee pocas columnas.
SELECT
  origin,
  source,
  COUNT(*) AS filas,
  COUNT(DISTINCT event_id) AS event_ids,
  MIN(dt) AS desde,
  MAX(dt) AS hasta
FROM ta_curated.fact_posts
GROUP BY origin, source
ORDER BY origin, source
