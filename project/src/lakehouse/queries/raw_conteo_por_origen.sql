-- ta-raw-conteo-por-origen (Lote 4.1, ARCHITECTURE.md §3.19.1): filas de Raw por origen y fuente.
-- Lee solo event_id y event_time de los Parquet (pocos MB); las particiones salen de la proyección.
SELECT
  part_origin AS origin,
  part_source AS source,
  COUNT(*) AS filas,
  COUNT(DISTINCT event_id) AS event_ids,
  MIN(event_time) AS desde,
  MAX(event_time) AS hasta
FROM ta_raw.posts_raw
GROUP BY part_origin, part_source
ORDER BY part_origin, part_source
