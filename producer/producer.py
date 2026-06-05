"""
STEP 1 — KAFKA PRODUCER
========================
Reads taxi_sample.csv line by line and publishes each row as a JSON
message to the Kafka topic 'taxi-raw'.

Windows note: no sh entrypoint — the Dockerfile calls python directly.
The startup wait is done here in Python (more reliable on Windows Docker).
"""

import csv
import json
import os
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s"
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_FILE          = os.getenv("CSV_FILE", "/data/taxi_sample.csv")
TOPIC             = os.getenv("TOPIC_NAME", "taxi-raw")
DELAY_S           = float(os.getenv("DELAY_MS", "100")) / 1000.0


def create_producer(retries: int = 15) -> KafkaProducer:
    """Retry-loop so the producer waits for Kafka to be ready."""
    log.info("Waiting 30s for Kafka to initialise...")
    time.sleep(30)                          # initial wait — replaces shell sleep
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Attempt {attempt}/{retries}: Kafka not ready. Retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def parse_row(row: dict) -> dict:
    try:
        return {
            "vendor_id":              int(row["VendorID"]),
            "pickup_datetime":        row["tpep_pickup_datetime"].strip(),
            "dropoff_datetime":       row["tpep_dropoff_datetime"].strip(),
            "passenger_count":        int(float(row["passenger_count"])),
            "trip_distance":          float(row["trip_distance"]),
            "rate_code_id":           int(float(row["RatecodeID"])),
            "store_and_fwd_flag":     row["store_and_fwd_flag"].strip(),
            "pu_location_id":         int(row["PULocationID"]),
            "do_location_id":         int(row["DOLocationID"]),
            "payment_type":           int(row["payment_type"]),
            "fare_amount":            float(row["fare_amount"]),
            "extra":                  float(row["extra"]),
            "mta_tax":                float(row["mta_tax"]),
            "tip_amount":             float(row["tip_amount"]),
            "tolls_amount":           float(row["tolls_amount"]),
            "improvement_surcharge":  float(row["improvement_surcharge"]),
            "total_amount":           float(row["total_amount"]),
            "congestion_surcharge":   float(row.get("congestion_surcharge", 0) or 0),
        }
    except (ValueError, KeyError) as e:
        log.debug(f"Skipping malformed row: {e}")
        return None


def normalize_csv_line(line: str) -> str:
    line = line.strip()
    if line.startswith('"') and line.endswith('"'):
        return line[1:-1]
    return line


def main():
    producer  = create_producer()
    sent      = 0
    skipped   = 0
    log_every = 1000

    log.info(f"Streaming '{CSV_FILE}' → Kafka topic '{TOPIC}'")

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader((normalize_csv_line(line) for line in f), delimiter="\t")
        for raw_row in reader:
            record = parse_row(raw_row)
            if record is None:
                skipped += 1
                continue
            producer.send(TOPIC, value=record)
            sent += 1
            if sent % log_every == 0:
                log.info(f"Sent {sent:,} messages | Skipped {skipped:,} bad rows")
            time.sleep(DELAY_S)

    producer.flush()
    producer.close()
    log.info(f"Done. Total sent: {sent:,} | Total skipped: {skipped:,}")


if __name__ == "__main__":
    main()
