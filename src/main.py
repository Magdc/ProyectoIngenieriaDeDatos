import argparse
import json
import logging
import re
from datetime import datetime

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows


class ValidateAndNormalizeDoFn(beam.DoFn):
    OUTPUT_TAG_DEAD_LETTER = 'dead_letter'

    def clean_html(self, raw_html):
        if not raw_html:
            return ""
        return re.sub(r'<[^>]+>', '', raw_html).strip()

    def process(self, element):
        try:
            payload_str = element.decode('utf-8')
            data = json.loads(payload_str)

            source = str(data.get('source', '')).lower().strip()
            if not source:
                source = 'mastodon' if 'content' in data else 'news'

            if source == 'mastodon':
                if not data.get('id'):
                    raise ValueError("Mastodon: Campo 'id' faltante")
                tags_list = [t.get('name') for t in data.get('tags', []) if isinstance(t, dict) and t.get('name')]
                account = data.get('account', {})
                metrics = {
                    'primary_count': data.get('favourites_count', 0),
                    'secondary_count': data.get('reblogs_count', 0),
                    'replies_count': data.get('replies_count', 0),
                    'author_followers_count': account.get('followers_count', 0)
                }
                normalized_event = {
                    'event_id': str(data['id']),
                    'source_name': 'mastodon',
                    'title': None,
                    'clean_text': self.clean_html(data.get('content')),
                    'url': data.get('url'),
                    'language': data.get('language', 'es'),
                    'tags': tags_list,
                    'author_id': str(account.get('id', '')),
                    'author_username': account.get('username'),
                    'metrics': metrics,
                    'published_at': data.get('created_at'),
                    'ingested_at': datetime.utcnow().isoformat()
                }

            elif source in ['news', 'rss']:
                if not data.get('id'):
                    raise ValueError("RSS/News: Campo 'id' faltante")
                tags_list = [t.get('term') for t in data.get('tags', []) if isinstance(t, dict) and t.get('term')]
                comments_count = int(data.get('slash_comments', 0)) if str(data.get('slash_comments', '0')).isdigit() else 0
                metrics = {
                    'primary_count': 0,
                    'secondary_count': comments_count,
                    'replies_count': comments_count,
                    'author_followers_count': None
                }
                normalized_event = {
                    'event_id': str(data['id']),
                    'source_name': 'news',
                    'title': data.get('title'),
                    'clean_text': data.get('summary', '').strip() if data.get('summary') else None,
                    'url': data.get('link') or data.get('id'),
                    'language': 'es',
                    'tags': tags_list,
                    'author_id': None,
                    'author_username': None,
                    'metrics': metrics,
                    'published_at': data.get('published'),
                    'ingested_at': datetime.utcnow().isoformat()
                }
            else:
                raise ValueError(f"Fuente no soportada: {source}")

            yield normalized_event

        except Exception as e:
            logging.error(f"Error procesando mensaje: {str(e)}")
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_TAG_DEAD_LETTER,
                {
                    'raw_payload': element.decode('utf-8', errors='ignore'),
                    'error_message': str(e),
                    'failed_at': datetime.utcnow().isoformat()
                }
            )


# Función con 1 solo parámetro obligatorio 'element' (Fix TypeError)
def gcs_destination_fn(element):
    try:
        data = json.loads(element.decode('utf-8'))
        source = str(data.get('source', 'unknown')).lower().strip()
        if source not in ['mastodon', 'news', 'rss']:
            source = 'mastodon' if 'content' in data else 'news'
    except Exception:
        source = 'unknown'

    now = datetime.utcnow()
    return f"source={source}/year={now.year}/month={now.month:02d}/day={now.day:02d}"


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--subscription', required=True, help='Suscripcion PubSub de entrada')
    parser.add_argument('--output_table', required=True, help='Tabla destino BigQuery para eventos')
    parser.add_argument('--deadletter_table', required=True, help='Tabla destino BigQuery para errores')
    parser.add_argument('--raw_gcs_bucket', required=True, help='Ruta gs:// del bucket raw')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Leer de Pub/Sub (Mensaje binario crudo)
        raw_messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)

        # 2. Persistir EL DATO CRUDO en Cloud Storage Raw (Fix de arquitectura Bronze)
        (
            raw_messages
            | "WindowRaw" >> beam.WindowInto(FixedWindows(300))
            | "WriteToGCSRaw" >> fileio.WriteToFiles(
                path=f"{known_args.raw_gcs_bucket.rstrip('/')}/",
                destination=gcs_destination_fn,
                file_naming=fileio.default_file_naming(prefix="raw_events", suffix=".json")
            )
        )

        # 3. Validar y Normalizar
        processed_results = raw_messages | "ValidateAndNormalize" >> beam.ParDo(
            ValidateAndNormalizeDoFn()
        ).with_outputs(ValidateAndNormalizeDoFn.OUTPUT_TAG_DEAD_LETTER, main='valid_events')

        # 4. Escritura en BigQuery (Analítica)
        processed_results.valid_events | "WriteToBQ" >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

        # 5. Errores a BigQuery (Dead Letter Queue)
        processed_results[ValidateAndNormalizeDoFn.OUTPUT_TAG_DEAD_LETTER] | "WriteDeadLetterToBQ" >> beam.io.WriteToBigQuery(
            table=known_args.deadletter_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()