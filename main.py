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


def fetch_playlist_video_ids(api_key, playlist_id):
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    page_token = None