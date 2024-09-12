import os
import json
import functions_framework
from google.cloud import pubsub_v1, storage

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

project_id = 'planar-cistern-435110-n1'
topic_name = 'testing_topic'
topic_path = publisher.topic_path(project_id, topic_name)

curated_bucket_name = 'planar-cistern-435110-n1-curated'

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    try:
        data = cloud_event.data

        event_id = cloud_event["id"]
        event_type = cloud_event["type"]

        bucket_name = data["bucket"]
        file_name = data["name"]
        metageneration = data["metageneration"]
        time_created = data["timeCreated"]
        updated = data["updated"]

        print(f"Event ID: {event_id}")
        print(f"Event type: {event_type}")
        print(f"Bucket: {bucket_name}")
        print(f"File: {file_name}")
        print(f"Metageneration: {metageneration}")
        print(f"Created: {time_created}")
        print(f"Updated: {updated}")

        if file_name.startswith('to-process/') and file_name.endswith('.csv'):
            print(f"CSV File {file_name} is in the 'to-process/' folder. Processing file.")

            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(file_name)
            file_content = blob.download_as_bytes()

            curated_bucket = storage_client.get_bucket(curated_bucket_name)

            curated_file_name = os.path.basename(file_name)
            curated_blob = curated_bucket.blob(curated_file_name)
            curated_blob.upload_from_string(file_content)

            print(f"File {curated_file_name} uploaded to curated bucket {curated_bucket_name}.")

            payload = {
                "filename": curated_file_name,
                "bucket": curated_bucket_name,
                "location": f"gs://{curated_bucket_name}/{curated_file_name}"
            }

            message_data = json.dumps(payload).encode('utf-8')

            future = publisher.publish(topic_path, data=message_data)
            future.result()

            print(f"Published message to {topic_name}: {payload}")

        else:
            print(f"File {file_name} is not a CSV or not in the 'to-process/' folder. Ignoring.")

    except Exception as e:
        print(f"An error occurred: {e}")