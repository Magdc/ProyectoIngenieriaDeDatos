-- ta-ds-autores-por-marcas (Lote 6.1, HU-038; ARCHITECTURE.md §3.27): cuántos autores hablan de una, dos o tres
-- marcas y de cuáles, desde ta_curated.edges_user_brand. Agrega por combinación de marcas: user_hash no sale del dataset.
WITH por_autor AS (
  SELECT
    e.user_hash,
    array_join(array_sort(array_agg(COALESCE(b.display, e.brand))), ' + ') AS combinacion,
    COUNT(*) AS n_marcas,
    SUM(e.weight) AS menciones
  FROM ta_curated.edges_user_brand e
  LEFT JOIN ta_curated.dim_brand b ON b.brand = e.brand
  GROUP BY e.user_hash
)
SELECT
  combinacion,
  n_marcas,
  COUNT(*) AS autores,
  SUM(menciones) AS menciones
FROM por_autor
GROUP BY combinacion, n_marcas
