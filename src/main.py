import argparse
import json
import logging
import re
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

TAG_PROCESSED = 'processed'
TAG_DEADLETTER = 'dead_letter'

class ProcessAndNormalizeEventFn(beam.DoFn):
    """
    DoFn para parsear eventos con Esquema Común y adaptarlos a BigQuery.
    """
    def process(self, element):
        raw_payload = element
        data = {}
        try:
            if isinstance(element, bytes):
                raw_payload = element.decode('utf-8')

            data = json.loads(raw_payload)

            # 1. Campos obligatorios
            event_id = str(data.get('id') or '').strip()
            source_name = str(data.get('source') or '').strip().lower()
            text = str(data.get('text') or '').strip()

            if not event_id or not source_name or not text:
                raise ValueError(f"Campos obligatorios faltantes (id, source, text): {data}")

            # 2. Manejo estricto de Fechas UTC (ISO-8601 con 'Z')
            now_utc = datetime.now(timezone.utc)
            ingested_at = now_utc.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'

            raw_date = data.get('created_at') or data.get('published_at')
            published_at = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
            
            if raw_date:
                try:
                    clean_date = str(raw_date).replace('Z', '+00:00')
                    dt = datetime.fromisoformat(clean_date)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    published_at = dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                except Exception:
                    pass

            # 3. Metadatos
            title = str(data.get('title') or '').strip()
            if not title:
                title = text[:80] + ('...' if len(text) > 80 else '')

            url = str(data.get('url') or '').strip()
            language = str(data.get('language') or 'unknown').strip()

            # 4. Normalización de Tags (ARRAY<STRING>)
            raw_tags = data.get('tags', [])
            tags = [str(t).strip() for t in raw_tags if t] if isinstance(raw_tags, list) else []

            # 5. Autor
            author_id = str(data.get('author_id') or '').strip()
            author_username = str(data.get('author_name') or data.get('author_username') or '').strip()

            # 6. Métricas (RECORD)
            engagement = data.get('engagement', {}) if isinstance(data.get('engagement'), dict) else {}
            metrics_data = data.get('metrics', {}) if isinstance(data.get('metrics'), dict) else {}

            metrics = {
                'likes': int(engagement.get('likes', metrics_data.get('likes', 0)) or 0),
                'shares': int(engagement.get('reblogs', metrics_data.get('shares', 0)) or 0),
                'comments': int(engagement.get('replies', metrics_data.get('comments', 0)) or 0)
            }

            # 7. Registro estructurado para BQ
            processed_record = {
                'event_id': event_id,
                'source_name': source_name,
                'title': title[:500],
                'clean_text': text,
                'url': url,
                'language': language,
                'tags': tags,
                'author_id': author_id,
                'author_username': author_username,
                'metrics': metrics,
                'published_at': published_at,
                'ingested_at': ingested_at
            }

            yield beam.pvalue.TaggedOutput(TAG_PROCESSED, processed_record)

        except Exception as e:
            logging.warning(f"Error procesando evento, enviando a DLQ: {e}")
            
            now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            dead_letter_record = {
                'event_id': str(data.get('id', 'unknown')) if isinstance(data, dict) else 'unknown',
                'source_name': str(data.get('source', 'unknown')) if isinstance(data, dict) else 'unknown',
                'raw_payload': raw_payload if isinstance(raw_payload, str) else str(raw_payload),
                'error_message': str(e),
                'failed_at': now_utc
            }
            yield beam.pvalue.TaggedOutput(TAG_DEADLETTER, dead_letter_record)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True, help='Suscripción de PubSub')
    parser.add_argument('--output_table', required=True, help='Tabla BigQuery Procesados')
    parser.add_argument('--deadletter_table', required=True, help='Tabla BigQuery DLQ')
    parser.add_argument('--raw_bucket_path', required=True, help='Prefijo GCS para datos Raw')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Lectura de Pub/Sub
        raw_messages = (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        )

        # 2. Respaldo Raw en GCS
        now = datetime.now(timezone.utc)
        path_prefix = f"{known_args.raw_bucket_path.rstrip('/')}/year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/raw_events"

        _ = (
            raw_messages
            | "DecodeToString" >> beam.Map(lambda x: x.decode('utf-8') if isinstance(x, bytes) else str(x))
            | "WriteRawToGCS" >> beam.io.WriteToText(
                path_prefix,
                file_name_suffix='.ndjson'
            )
        )

        # 3. Transformación y Validación
        results = (
            raw_messages
            | "ProcessAndNormalize" >> beam.ParDo(ProcessAndNormalizeEventFn()).with_outputs(
                TAG_DEADLETTER, TAG_PROCESSED
            )
        )

        # 4. Escritura en BigQuery (Tabla Procesados)
        _ = (
            results[TAG_PROCESSED]
            | "WriteProcessedToBQ" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

        # 5. Escritura en BigQuery (Tabla Dead Letter)
        _ = (
            results[TAG_DEADLETTER]
            | "WriteDeadLetterToBQ" >> beam.io.WriteToBigQuery(
                table=known_args.deadletter_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()