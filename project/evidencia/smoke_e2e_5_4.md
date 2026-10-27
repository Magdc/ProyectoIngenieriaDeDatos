# Prueba de humo E2E · smoke-20261027T162356Z

- Resultado: **OK**
- Centinela: `4e2712e1-0004-56bc-8606-209103050e96` (reddit, `comment-gen@1.0`), publicado 2026-10-27T16:27:49.268Z
- Total publicación → `fact_posts`: 465.2 s
- Identidad: sin `user_id`/`handle` en bigquery, curated, gcs_enriched, s3_raw, trusted; mismo `user_hash` en 5 etapas: sí

- Calentamiento (`ta-verify@2.1`, Trusted lo descarta): 229.1 s hasta BigQuery

| Etapa | Desde | Latencia (s) | Objetivo (s) | Desde la publicación (s) | OK |
|---|---|---|---|---|---|
| bigquery | published | 19.1 | 60 | 19.1 | ✅ |
| gcs_enriched | published | 175.1 | 900 | 175.1 | ✅ |
| s3_raw | gcs_enriched | 78.6 | 300 | 253.7 | ✅ |
| trusted | s3_raw | 149.1 | 900 | 402.8 | ✅ |
| curated | trusted | 62.4 | 600 | 465.2 | ✅ |
