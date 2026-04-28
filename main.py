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

def fetch_playlist_items(google_api_key, playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key, playlist_id, page_token)
    
    for item in payload.get("items", []):
        yield item

    next_page_token = payload.get("nextPageToken")
    if next_page_token:
        yield from fetch_playlist_items(google_api_key, playlist_id, next_page_token)



def fetch_video_details(google_api_key, video_id):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "key": google_api_key,
        "id": video_id,
        "part": "snippet,statistics"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("items", [])


def on_delivery(err, msg):
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.info(f"Record {msg.key()} successfully produced to {msg.topic()}")


def main():
    logging.info("STARTING PIPELINE")

    sr_client = SchemaRegistryClient({"url": SCHEMA_REG_URL})
    video_value_schema = sr_client.get_latest_version("playlist-shema")

    kafka_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            sr_client,
            video_value_schema.schema.schema_str,
        )
    }
    producer = SerializingProducer(kafka_config)

    try:
        for video_item in fetch_playlist_items(API_KEY, PLAYLIST_ID):
            video_id = video_item["contentDetails"]["videoId"]
            
            video_list = fetch_video_details(API_KEY, video_id)
            
            for video in video_list:
                stats = video.get("statistics", {})
                snippet = video.get("snippet", {})
                
                value = {
                    "TITLE": snippet.get("title", "Unknown"),
                    "VIEWS": int(stats.get("viewCount", 0)),
                    "LIKES": int(stats.get("likeCount", 0)),
                    "COMMENTS": int(stats.get("commentCount", 0)),
                }

                logging.info(f"Processing: {value['TITLE']}")
                
                producer.produce(
                    topic="playlist.alert",
                    key=video_id,
                    value=value,
                    on_delivery=on_delivery
                )
        
        producer.flush()
        logging.info("FINISHED")

    except Exception as e:
        logging.error(f"Pipeline crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    main()
