import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions
import json
import argparse
import csv
import logging
import io

BQ_SCHEMA = (
    'capital:STRING, '
    'temperature:INT64, '
    'weather_description:STRING, '
    'wind_speed:INT64, '
    'pressure:INT64, '
    'precipitation:FLOAT64, '
    'humidity:INT64, '
    'cloudcover:INT64, '
    'feelslike:INT64, '
    'uv_index:INT64, '
    'visibility:INT64, '
    'observation_time:STRING, '
    'timestamp:TIMESTAMP'
)

def parse_pubsub_message(message):
    """Parse Pub/Sub message to extract file path."""
    try:
        message_dict = json.loads(message.decode('utf-8'))
        file_path = message_dict.get("location")
        if file_path:
            logging.info(f"Received file path: {file_path}")
            return file_path
        else:
            logging.error(f"No 'location' in message: {message_dict}")
            return None
    except Exception as e:
        logging.error(f"Failed to parse message: {e}")
        return None

class ParseCsvFn(beam.DoFn):
    def __init__(self):
        self.header_processed = False

    def process(self, line):
        logging.info(f"Processing line: {line}")
        try:
            reader = csv.DictReader(io.StringIO(line), fieldnames=[
                'capital', 'temperature', 'weather_description', 'wind_speed', 'pressure', 
                'precipitation', 'humidity', 'cloudcover', 'feelslike', 'uv_index', 
                'visibility', 'observation_time', 'timestamp'
            ])

            for row in reader:
                if not self.header_processed:
                    self.header_processed = True
                    continue
                yield {
                    'capital': row['capital'],
                    'temperature': int(row['temperature']),
                    'weather_description': row['weather_description'],
                    'wind_speed': int(row['wind_speed']),
                    'pressure': int(row['pressure']),
                    'precipitation': float(row['precipitation']),
                    'humidity': int(row['humidity']),
                    'cloudcover': int(row['cloudcover']),
                    'feelslike': int(row['feelslike']),
                    'uv_index': int(row['uv_index']),
                    'visibility': int(row['visibility']),
                    'observation_time': row['observation_time'],
                    'timestamp': row['timestamp']
                }
        except Exception as e:
            logging.error(f"Error parsing CSV line: {line} | Error: {e}")
        
def run_pipeline(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bigquery_table", required=True, help=("Output bigquery table")
    )
    parser.add_argument(
        "--input_subscription", required=True, help=("Input subscription")
    )
    parser.add_argument(
        "--bigquery_dataset", required=True, help=("Output bigquery dataset")
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    

    beam_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        enable_streaming_engine=True
    )
    beam_options.view_as(SetupOptions).save_main_session = save_main_session
    beam_options.view_as(StandardOptions).streaming = True
    BQ_DATASET = known_args.bigquery_dataset
    BQ_TABLE = known_args.bigquery_table
    project_id = beam_options.view_as(GoogleCloudOptions).project
    
    with beam.Pipeline(options=beam_options) as pipeline:
        gcs_file = (
            pipeline
            | 'Read PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | 'Parse PubSub Message' >> beam.Map(parse_pubsub_message)
            | 'Filter None file paths' >> beam.Filter(lambda x: x is not None)
        )

        weather_data = (
            gcs_file
            | 'Read CSV from GCS' >> beam.io.ReadAllFromText()
            | 'Parse CSV Lines' >> beam.ParDo(ParseCsvFn())
            | 'Filter Empty Rows' >> beam.Filter(lambda record: record is not None)
        )

        weather_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=f'{project_id}:{BQ_DATASET}.{BQ_TABLE}',
            schema=BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run_pipeline()
