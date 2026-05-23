"""
MongoDB Writer Service
Consuma da Kafka:
  - topic 'sensor-readings' → collection 'sensor_readings'
  - topic 'alerts'          → collection 'alerts'

Indici creati automaticamente su machine_id + timestamp
per query time-series efficienti.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure

log = logging.getLogger("mongo-writer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MONGO_URI         = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB          = os.getenv("MONGO_DB", "factory")
SENSOR_TOPIC      = os.getenv("SENSOR_TOPIC", "sensor-readings")
ALERT_TOPIC       = os.getenv("ALERT_TOPIC", "alerts")
GROUP_ID          = os.getenv("GROUP_ID", "mongo-writer-group")

# Scrivi su Mongo in batch per efficienza
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "20"))


def connect_mongo(retries=15) -> MongoClient:
    for attempt in range(retries):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info(f"Connesso a MongoDB: {MONGO_URI}")
            return client
        except ConnectionFailure as e:
            log.warning(f"MongoDB non disponibile, tentativo {attempt+1}/{retries}: {e}")
            time.sleep(4)
    raise RuntimeError("Impossibile connettersi a MongoDB")


def setup_indexes(db):
    """Crea indici per query time-series efficienti."""
    # sensor_readings: query per macchina + range temporale
    db.sensor_readings.create_index(
        [("machine_id", ASCENDING), ("timestamp", DESCENDING)],
        name="machine_time_idx"
    )
    db.sensor_readings.create_index(
        [("timestamp", DESCENDING)],
        name="time_idx"
    )
    # alerts: query per severity + macchina
    db.alerts.create_index(
        [("machine_id", ASCENDING), ("timestamp", DESCENDING)],
        name="alert_machine_time_idx"
    )
    db.alerts.create_index(
        [("severity", ASCENDING), ("timestamp", DESCENDING)],
        name="alert_severity_idx"
    )
    log.info("Indici MongoDB creati")


def connect_kafka(retries=15) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                SENSOR_TOPIC,
                ALERT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,   # commit manuale dopo scrittura
            )
            log.info(f"Connesso a Kafka: {KAFKA_BOOTSTRAP}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponibile, tentativo {attempt+1}/{retries}...")
            time.sleep(5)
    raise RuntimeError("Impossibile connettersi a Kafka")


def run():
    mongo_client = connect_mongo()
    db = mongo_client[MONGO_DB]
    setup_indexes(db)

    consumer = connect_kafka()

    sensor_batch = []
    alert_batch  = []
    msg_count    = 0

    log.info(f"In ascolto su topic '{SENSOR_TOPIC}' e '{ALERT_TOPIC}'...")

    for msg in consumer:
        doc = msg.value
        # Aggiungi timestamp di ingestion
        doc["_ingested_at"] = datetime.now(timezone.utc).isoformat()

        if msg.topic == SENSOR_TOPIC:
            sensor_batch.append(doc)
        elif msg.topic == ALERT_TOPIC:
            alert_batch.append(doc)
            log.warning(
                f"ALERT salvato [{doc.get('severity','?').upper()}] "
                f"{doc.get('machine_id')} — {doc.get('alert_type')}"
            )

        msg_count += 1

        # Flush batch su MongoDB
        if msg_count % BATCH_SIZE == 0:
            if sensor_batch:
                db.sensor_readings.insert_many(sensor_batch)
                log.info(f"Scritti {len(sensor_batch)} sensor readings su MongoDB")
                sensor_batch = []
            if alert_batch:
                db.alerts.insert_many(alert_batch)
                log.info(f"Scritti {len(alert_batch)} alert su MongoDB")
                alert_batch = []
            consumer.commit()

    # Flush finale
    if sensor_batch:
        db.sensor_readings.insert_many(sensor_batch)
    if alert_batch:
        db.alerts.insert_many(alert_batch)
    consumer.commit()


if __name__ == "__main__":
    run()
