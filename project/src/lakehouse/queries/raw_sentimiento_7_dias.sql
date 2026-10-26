-- ta-raw-sentimiento-7-dias (Lote 4.1, ARCHITECTURE.md §3.19.1): sentimiento por marca y origen en
-- los últimos 7 días (UTC). El filtro por part_dt hace que Athena lea solo esas particiones.
SELECT
  part_origin AS origin,
  brand_primary AS brand,
  sentiment_label,
  COUNT(*) AS filas
FROM ta_raw.posts_raw
WHERE part_dt >= date_format(current_date - INTERVAL '7' DAY, '%Y-%m-%d')
GROUP BY part_origin, brand_primary, sentiment_label
ORDER BY part_origin, brand_primary, sentiment_label
