PROJECT_ID="tu-gcp-project-id"
REGION="us-central1"
TOPIC_RAW="trend-dev-raw-events"
SUB_DATAFLOW="trend-dev-dataflow-sub"
BUCKET_RAW="trend-dev-${PROJECT_ID}-raw"
DATASET_BQ="trend_dev_streaming"


#Cloud Run

# Obtener URL del colector de Mastodon
CLOUD_RUN_URL=$(gcloud run services describe trend-dev-mastodon-collector --region $REGION --format='value(status.url)')

# Invocar endpoint de extracción
curl -X POST "$CLOUD_RUN_URL/fetch" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json"


#Pub/Sub
TEST_PAYLOAD='{
  "id": "test-val-99999",
  "source": "mastodon",
  "content": "<p>#Colombia Probrando flujo de streaming end-to-end</p>",
  "url": "https://mastodon.social/@test/99999",
  "language": "es",
  "created_at": "2026-08-27T10:00:00Z",
  "tags": [{"name": "colombia"}],
  "account": {"id": "123", "username": "test_user", "followers_count": 10}
}'

gcloud pubsub topics publish $TOPIC_RAW --message="$TEST_PAYLOAD"


#Suscripción en Pub/Sub
gcloud pubsub subscriptions describe $SUB_DATAFLOW --format="value(numUndeliveredMessages)"


# Cloud Storage (Capa Raw)
# Dataflow procesa el elemento en 30 segundos
sleep 30

# Listar la estructura particionada en GCS
gcloud storage ls --recursive "gs://${BUCKET_RAW}/source=mastodon/"


# Validar Registro en BigQuery"
bq query --use_legacy_sql=false \
"SELECT event_id, source_name, clean_text, ingested_at 
 FROM \`${PROJECT_ID}.${DATASET_BQ}.events_processed\` 
 WHERE event_id = 'test-val-99999'"