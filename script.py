import requests
import time
import csv
from datetime import datetime
from google.cloud import storage
import os

countries_and_capitals = [
    {"country": "Russia", "capital": "Moscow"},
    {"country": "Canada", "capital": "Ottawa"},
    {"country": "United States", "capital": "Washington"},
    {"country": "China", "capital": "Beijing"},
    {"country": "Brazil", "capital": "Brasilia"},
    {"country": "Australia", "capital": "Canberra"},
    {"country": "India", "capital": "New Delhi"},
    {"country": "Argentina", "capital": "Buenos Aires"},
    {"country": "Kazakhstan", "capital": "Nur-Sultan"},
    {"country": "Algeria", "capital": "Algiers"}
]

API_KEY = os.environ.get("API_KEY")
BASE_URL = "http://api.weatherstack.com/current"

CSV_FILE = "weather_data.csv"

BUCKET_NAME = os.environ.get("BUCKET_NAME")
GCS_FOLDER = "to-process"

def get_weather(capital):
    try:
        params = {
            'access_key': API_KEY,
            'query': capital,
            'units': 'm',
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if 'current' in data:
            current_weather = data['current']
            return {
                'capital': capital,
                'temperature': current_weather['temperature'],
                'weather_description': current_weather['weather_descriptions'][0],
                'wind_speed': current_weather['wind_speed'],
                'pressure': current_weather['pressure'],
                'precipitation': current_weather['precip'],
                'humidity': current_weather['humidity'],
                'cloudcover': current_weather['cloudcover'],
                'feelslike': current_weather['feelslike'],
                'uv_index': current_weather['uv_index'],
                'visibility': current_weather['visibility'],
                'observation_time': current_weather['observation_time'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        else:
            print(f"Error fetching weather for {capital}: {data}")
            return None
    except Exception as e:
        print(f"Error fetching weather for {capital}: {e}")
        return None

def write_to_csv(data, file_name):
    file_exists = False
    try:
        file_exists = open(file_name, 'r')
        file_exists.close()
    except FileNotFoundError:
        pass

    with open(file_name, mode='a', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['capital', 'temperature', 'weather_description', 'wind_speed', 'pressure', 
                      'precipitation', 'humidity', 'cloudcover', 'feelslike', 'uv_index', 
                      'visibility', 'observation_time', 'timestamp']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(data)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    except Exception as e:
        print(f"Error uploading file to GCS: {e}")

def main():
    while True:
        for country_info in countries_and_capitals:
            capital = country_info['capital']
            weather_data = get_weather(capital)

            if weather_data:
                write_to_csv(weather_data, CSV_FILE)
                print(f"Weather data for {capital} saved.")
            else:
                print(f"Skipping data for {capital} due to error.")

        upload_to_gcs(BUCKET_NAME, CSV_FILE, f"{GCS_FOLDER}/weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        print("Waiting for 10 minutes before fetching data again...")
        time.sleep(600)

if __name__ == "__main__":
    main()
