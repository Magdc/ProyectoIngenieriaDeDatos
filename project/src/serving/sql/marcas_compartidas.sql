-- ta-ds-marcas-compartidas (Lote 6.1, HU-038; ARCHITECTURE.md §3.27): cómo se cruza la conversación entre marcas.
-- Una fila por par de marcas (marca_origen < marca_destino) con los autores que mencionan las dos, desde
-- ta_curated.edges_user_brand. user_hash solo sirve para unir dentro de la consulta: no sale del dataset.
WITH autor_marca AS (
  SELECT e.user_hash, COALESCE(b.display, e.brand) AS marca, e.weight
  FROM ta_curated.edges_user_brand e
  LEFT JOIN ta_curated.dim_brand b ON b.brand = e.brand
)
SELECT
  a.marca AS marca_origen,
  c.marca AS marca_destino,
  COUNT(*) AS autores_en_comun,
  SUM(a.weight + c.weight) AS menciones
FROM autor_marca a
JOIN autor_marca c ON c.user_hash = a.user_hash AND a.marca < c.marca
GROUP BY a.marca, c.marca
