-- ta-ds-coocurrencia-hashtags (Lote 6.1, HU-038; ARCHITECTURE.md §3.27): la red de hashtags de
-- ta_curated.edges_hashtag_cooccurrence, una arista no dirigida por fila (siempre hashtag_a < hashtag_b, §5.7).
SELECT
  e.hashtag_a,
  e.hashtag_b,
  concat('#', e.hashtag_a, ' + #', e.hashtag_b) AS par,
  e.weight AS comentarios
FROM ta_curated.edges_hashtag_cooccurrence e
