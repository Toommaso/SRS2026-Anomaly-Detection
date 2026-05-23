"""
MQTT -> Kafka Bridge
Legge messaggi dai topic MQTT factory/+/sensors
e li pubblica sul topic Kafka 'sensor-readings'.
"""

import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
log = logging.getLogger("mqtt-kafka-bridge")

MQTT_HOST   = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")


def make_kafka_producer(retries=15) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",           # durabilità massima
                retries=3,
            )
            log.info(f"Connesso a Kafka: {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponibile, tentativo {attempt+1}/{retries}...")
            time.sleep(5)
    raise RuntimeError("Impossibile connettersi a Kafka")


def make_bridge():
    producer = make_kafka_producer()

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            machine_id = payload.get("machine_id", "unknown")
            producer.send(
                KAFKA_TOPIC,
                key=machine_id,
                value=payload,
            )
            if payload.get("status") != "normal":
                log.warning(f"[{machine_id}] {payload.get('status')} → Kafka")
        except Exception as e:
            log.error(f"Errore bridge: {e}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.subscribe("factory/+/sensors", qos=1)
            log.info("Connesso a MQTT, subscribed a factory/+/sensors")
        else:
            log.error(f"MQTT connessione fallita rc={rc}")

    client = mqtt.Client(client_id="mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message

    for attempt in range(15):
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            log.warning(f"MQTT non disponibile: {e}, tentativo {attempt+1}/15")
            time.sleep(3)

    client.loop_forever()


if __name__ == "__main__":
    make_bridge()
