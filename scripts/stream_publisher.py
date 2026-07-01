import time
import json
from datetime import datetime
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "your-gcp-project-id"  # Swap with your actual GCP project ID
TOPIC_ID = "log-ingest-topic"
LOG_FILE_PATH = "path/to/your/nasa_dataset.tsv"

def stream_logs():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Starting NASA log streaming from {LOG_FILE_PATH}...")

    try:
        with open(LOG_FILE_PATH, "r") as file:
            # 1. Read and skip the header row
            header = file.readline()
            headers = [h.strip() for h in header.split("\t")]

            # 2. Stream data rows
            for line in file:
                clean_line = line.strip()
                if not clean_line:
                    continue

                # Split row by tabs
                fields = clean_line.split("\t")

                # Handle cases where a row might have missing columns safely
                if len(fields) < len(headers):
                    continue

                # Map fields dynamically to headers
                log_data = dict(zip(headers, fields))

                # Inject a processing timestamp so Spark can handle real-time windows
                payload = {
                    "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
                    "host": log_data.get("host"),
                    "epoch_time": int(log_data.get("time", 0)),
                    "method": log_data.get("method"),
                    "url": log_data.get("url"),
                    "response_code": int(log_data.get("response", 0)),
                    "bytes": int(log_data.get("bytes") if log_data.get("bytes") != '-' else 0),
                    "useragent": log_data.get("useragent", "-")
                }

                data_bytes = json.dumps(payload).encode("utf-8")

                # Publish asynchronously
                publisher.publish(topic_path, data_bytes)

                # Stream velocity: 1ms delay (~1,000 requests/second)
                time.sleep(0.001)

    except FileNotFoundError:
        print(f"Error: NASA log file not found at {LOG_FILE_PATH}")
    except KeyboardInterrupt:
        print("\nStreaming paused.")

if __name__ == "__main__":
    stream_logs()