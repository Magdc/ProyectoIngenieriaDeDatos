-- ta-ds-publicaciones (Lote 6.1, HU-037; ARCHITECTURE.md §3.27): un comentario de ta_curated.fact_posts por fila, con
-- el nombre visible y el papel de su marca, su red social y el NLP en español. Las horas se guardan en UTC y aquí se
-- pasan a America/Bogota (§5.12). Sin user_hash ni otra columna de autor: el tablero solo muestra agregados.
-- En Athena, CAST(x AT TIME ZONE 'America/Bogota' AS timestamp) devuelve la hora UTC: la hora local sale de
-- format_datetime (v2.48, Q3 del validador lo comprueba fila por fila).
WITH f AS (
  SELECT
    fp.*,
    CAST(format_datetime(fp.event_time AT TIME ZONE 'America/Bogota', 'yyyy-MM-dd HH:mm:ss.SSS') AS timestamp) AS t_bogota
  FROM ta_curated.fact_posts fp
)
SELECT
  f.event_id,
  f.origin AS origen,
  f.source AS red_clave,
  COALESCE(s.display_name, f.source) AS red,
  f.brand_primary AS marca_clave,
  COALESCE(b.display, f.brand_primary) AS marca,
  COALESCE(b.role, 'sin rol') AS rol_marca,
  CASE WHEN cardinality(f.brands) > 1 THEN 'comparativa' ELSE 'una marca' END AS tipo_mencion,
  f.t_bogota AS hora_bogota,
  date_trunc('day', f.t_bogota) AS fecha_bogota,
  hour(f.t_bogota) AS hora_del_dia,
  day_of_week(f.t_bogota) AS dia_semana,
  COALESCE(f.likes, 0) AS me_gusta,
  COALESCE(f.shares, 0) AS compartidos,
  COALESCE(f.comments, 0) AS respuestas,
  COALESCE(f.likes, 0) + COALESCE(f.shares, 0) + COALESCE(f.comments, 0) AS interacciones,
  CASE f.sentiment_label
    WHEN 'positive' THEN 'positivo'
    WHEN 'neutral' THEN 'neutral'
    WHEN 'negative' THEN 'negativo'
    ELSE 'sin NLP'
  END AS sentimiento,
  CASE f.sentiment_label
    WHEN 'positive' THEN f.sentiment_score
    WHEN 'negative' THEN -f.sentiment_score
    WHEN 'neutral' THEN 0.0
  END AS sentimiento_con_signo,
  CASE f.emotion_label
    WHEN 'joy' THEN 'alegría'
    WHEN 'sadness' THEN 'tristeza'
    WHEN 'anger' THEN 'enojo'
    WHEN 'fear' THEN 'miedo'
    WHEN 'disgust' THEN 'asco'
    WHEN 'surprise' THEN 'sorpresa'
    WHEN 'neutral' THEN 'neutral'
    WHEN 'others' THEN 'otras'
    ELSE COALESCE(f.emotion_label, 'sin NLP')
  END AS emocion,
  CASE WHEN f.sentiment_label IS NULL THEN 0 ELSE 1 END AS con_nlp
FROM f
LEFT JOIN ta_curated.dim_brand b ON b.brand = f.brand_primary
LEFT JOIN ta_curated.dim_source s ON s.source = f.source
