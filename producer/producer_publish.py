import os
import json
import logging
from datetime import datetime, timezone
import requests
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import time

# ENV
load_dotenv(dotenv_path="../airflow/.env")
API_KEY = os.getenv("OWM_API")
LAT = os.getenv("BERLIN_LAT")
LON = os.getenv("BERLIN_LON")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")

POLL_SECONDS = 300 # streaming imitation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("air_pollution_producer")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# fetching data from OpenWeatherMap Air Pollution API
def fetch_air_pollution_current() -> dict:
    """
    Fetch the current Air Pollution data for Berlin from OpenWeatherMap API.
    Returns a dict containing timestamp, source, and full API JSON data.
    """
    params = {"lat": LAT, "lon": LON, "appid": API_KEY}
    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()  # full JSON response

    # Extract timestamp from first element in list
    dt = data["list"][0]["dt"]  # Unix timestamp
    timestamp_utc = datetime.fromtimestamp(dt, tz=timezone.utc).replace(microsecond=0).isoformat() + "Z"

    # Return a dict ready to be published to Pub/Sub
    return {
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z",
        "source": "air_pollution_current",  
        "data": data                    
    }

# publish to Pub/Sub
def publish(event: dict):
    """
    Publish the Air Pollution event to the configured Pub/Sub topic.
    """
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    msg_id = future.result()
    logger.info("Published Air Pollution event: %s %s", msg_id, event["fetched_at"])

# cycle loop to fetch and publish data
def run_loop():
    """
    Main loop to continuously fetch and publish data.
    """
    logger.info("Starting Air Pollution producer loop every %s seconds", POLL_SECONDS)
    while True:
        try:
            event = fetch_air_pollution_current()
            publish(event)
        except Exception as e:
            logger.exception("Error in producer loop: %s", e)
        time.sleep(POLL_SECONDS)

# Entry point
if __name__ == "__main__":
    run_loop()
