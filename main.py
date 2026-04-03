import logging
import sys
import requests
from decouple import config


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer



API_KEY = config("API_KEY")
PLAYLIST_ID = config("PLAYLIST_ID")
SCHEMA_REG_URL = config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")
BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:9102")
TOPIC = "playlist.alert"



def fetch_playlist_items_page(google_api_key, playlist_id, page_token=None):
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "key": google_api_key,
        "playlistId": playlist_id,
        "part": "contentDetails",
        "maxResults": 50,
        "pageToken": page_token
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()