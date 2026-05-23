import json
import logging
import os
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scada-consumer] %(levelname)s — %(message)s"
)
log = logging.getLogger("scada-consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCADA_TOPIC = os.getenv("SCADA_TOPIC", "scada-events")
MACHINE_ID = os.getenv("MACHINE_ID", "machine_a")
MACHINE_PARTITION = os.getenv("MACHINE_PARTITION", 0)

def connect_consumer(retries: int = 10) -> KafkaConsumer:
    #if MACHINE_ID not in MACHINE_PARTITION:
    #    raise ValueError(f"MACHINE_ID '{MACHINE_ID}' non riconosciuto. Valori validi: {list(MACHINE_PARTITION)}")
 
    tp = TopicPartition(SCADA_TOPIC, int(MACHINE_PARTITION))
 
    for attempt in range(1, retries + 1):
        try:
            # No group_id: manual assignment bypasses group coordination entirely.
            # This consumer owns its partition exclusively and won't be rebalanced.
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            consumer.assign([tp])
            log.info(
                f"Connesso a Kafka {KAFKA_BOOTSTRAP}, "
                f"topic={SCADA_TOPIC}, partition={MACHINE_PARTITION} ({MACHINE_ID})"
            )
            return consumer
        except NoBrokersAvailable as exc:
            log.warning(
                f"Kafka non disponibile (tentativo {attempt}/{retries}): {exc}"
            )
            time.sleep(3)
    raise RuntimeError("Impossibile connettersi a Kafka")



if __name__ == "__main__":
    consumer = connect_consumer()

    try:
        for msg in consumer:
            log.info(
                "SCADA event received: partition=%s offset=%s\n%s",
                msg.partition,
                msg.offset,
                json.dumps(msg.value, indent=2, ensure_ascii=False)
            )
            # Estratto il target_rpm, possiamo modificare le baseline, e finalmente il controllo della velocità è fatto.
            target_rpm = msg.value.get("target_rpm", "N/A")

            # Serve leggere e riscrivere tutto, perché in python non si può modificare una riga soltanto.
            with open("baselines_" + MACHINE_ID + ".txt", "r") as f:
                lines = f.readlines()

            lines[2] = str(target_rpm) + "\n" 

            with open("baselines_" + MACHINE_ID + ".txt", "w") as f:
                f.writelines(lines)

    except KeyboardInterrupt:
        log.info("Interrotto dall'utente")
    finally:
        consumer.close()
        log.info("Consumer chiuso")
