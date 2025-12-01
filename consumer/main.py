import base64
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("air_pollution_consumer")

# ENV
PROJECT_ID = "GCP_PROJECT_ID"
DATASET = "BQ_RAW_DATASET"        
TABLE = "aqi_raw"      

bq_client = bigquery.Client(project=PROJECT_ID)
table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# Cloud Function entry point
def pubsub_to_bq(event, context):
    """
    Triggered from a message on a Pub/Sub topic.
    Writes the full Air Pollution JSON into BigQuery raw table.
    """
    try:
        if "data" not in event:
            logger.warning("No data in event")
            return

        # Decode Pub/Sub message
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        data_json = json.loads(pubsub_message)

        # Prepare row for BQ
        row = {
            "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z",
            "source": data_json.get("source", "unknown"),
            "data": data_json  # store full JSON
        }

        # Insert into BigQuery
        errors = bq_client.insert_rows_json(table_id, [row])
        if errors:
            logger.error("BQ insert errors: %s", errors)
        else:
            logger.info("Inserted raw Air Pollution row at %s", row["fetched_at"])

    except Exception as e:
        logger.exception("Error processing Pub/Sub message: %s", e)
